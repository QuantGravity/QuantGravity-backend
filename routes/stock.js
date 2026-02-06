// ===========================================================================
// [파일명] : routes/stock.js
// [대상]   : 티커 마스터 및 주가 정보 조회 (검색, 관심종목, 싸이클 분석)
// [기준]   : 
//   1. 권한 제어: 무료(FREE) 사용자의 ETF 및 상세 검색 범위를 엄격히 제한한다.
//   2. 데이터 정규화: '^'로 시작하는 지수 데이터를 우선 정렬하여 상단에 노출한다.
//   3. 중복 제거: getDailyStockData 호출 시 Map을 사용하여 날짜 기준 중복 데이터를 제거한다.
//   4. 싸이클 엔진: 주가 데이터 조회 시 상승/하락 싸이클 및 historicMax 지표를 실시간 산출한다.
// ===========================================================================
const express = require('express');
const router = express.Router();
const admin = require('firebase-admin');
const firestore = admin.firestore();
const { verifyToken } = require('../utils/authHelper');

// ============================================================
// [신규 API] 티커 검색 및 필터링 (권한 제어 포함)
// ============================================================
// [수정] verifyToken 미들웨어 추가 (req.user 사용 가능해짐)`
router.post('/ticker-search', verifyToken, async (req, res) => {
    try {
        const { type, keyword, userGrade } = req.body;
        
        // [보안 강화] 토큰에서 직접 권한 확인 (위조 방지)
        const tokenRole = req.user ? req.user.role : null;
        const isAdmin = ['admin', 'G9'].includes(tokenRole);

        // 1. [보안] 권한 체크 로직 수정
        // 관리자(isAdmin)이거나, 프론트에서 VIP라고 보냈으면 통과
        // 그 외(무료 유저)인 경우에만 제약 사항 체크
        const isFreeUser = !isAdmin && (!userGrade || userGrade === 'FREE');

        if (isFreeUser && type === '2') {
            return res.status(403).json({ 
                error: "무료 등급 회원은 ETF 리스트를 조회할 수 없습니다." 
            });
        }

        // 2. Firestore에서 전체 티커 가져오기
        const snapshot = await firestore.collection('tickers').get();
        
        const allTickers = snapshot.docs.map(doc => {
            const fullData = doc.data();
            const dataContent = fullData.metadata ? fullData.metadata : fullData;
            return {
                id: doc.id,
                ...dataContent
            };
        });

        // 3. 필터링 로직 실행
        const filteredList = allTickers.filter(item => {
            const tCode = (item.id || item.ticker || "").toUpperCase();
            const isIndex = tCode.startsWith('^'); 

            // [보안] 무료 유저는 검색(9)을 하더라도 '지수'만 보여줌
            // 관리자는 모든 종목 검색 가능
            if (isFreeUser) {
                if (!isIndex) return false; 
            }

            // 구분자별 로직
            if (type === '1') { // [지수]
                return isIndex;
            } 
            else if (type === '2') { // [지수ETF]
                return !isIndex;
            } 
            else if (type === '9') { // [검색]
                if (!keyword) return false;
                const searchKey = keyword.toUpperCase().trim();

                const kName = (item.ticker_name_kr || "").toUpperCase();
                const desc = (item.description || "").toUpperCase();
                const und = (item.underlying_ticker || "").toUpperCase();

                return tCode.includes(searchKey) ||
                       kName.includes(searchKey) ||
                       desc.includes(searchKey) ||
                       und.includes(searchKey);
            }
            
            return false;
        });

        // 4. 정렬
        filteredList.sort((a, b) => {
            const aId = (a.id || a.ticker).toUpperCase();
            const bId = (b.id || b.ticker).toUpperCase();
            return aId.localeCompare(bId);
        });

        // 실제 권한 로그 출력 (디버깅용)
        console.log(`[Ticker Search] Type:${type}, Key:${keyword}, Role:${tokenRole}, Grade:${userGrade} -> Result:${filteredList.length}건`);
        
        res.json(filteredList);

    } catch (e) {
        console.error("Search API Error:", e);
        res.status(500).json({ error: "검색 중 오류가 발생했습니다." });
    }
});

// ----------------------------------------------------------------
// 3. 주가 데이터 조회 및 분석 API
// ----------------------------------------------------------------

// 특정 티커의 싸이클 계산
router.get('/daily-stock', async (req, res) => {
// 모든 인자를 query에서 한 번에 구조 분해 할당
    const { ticker, startDate, endDate, upperRate: uR, lowerRate: lR } = req.query;
    const upperRate = parseFloat(uR) || 30; 
    const lowerRate = parseFloat(lR) || 15;

    console.log(`[조회 시작] ticker: ${ticker}, StartDate: ${startDate}`); // 디버깅용

    try {
        // 1. 쿼리 실행 (ticker 대소문자 무시 등 대비)
        const rows = await getDailyStockData(ticker, startDate, endDate);

        console.log(`[쿼리 결과] 데이터 개수: ${rows.length}건`);

        if (rows.length === 0) {
            return res.json([]); // 데이터가 없으면 빈 배열 반환 -> 프론트에서 "데이터가 없습니다" 알림 발생
        }

        let hMax = -Infinity; 
        let rMax = parseFloat(rows[0].high_price); 
        let rMin = parseFloat(rows[0].low_price);  
        let currentStatus = "-";

        const results = rows.map((row, index) => {
            try {
                const high = parseFloat(row.high_price);
                const low = parseFloat(row.low_price);
                const close = parseFloat(row.close_price);
                
                // 날짜 처리 방어 코드
                if (!row.date) return null;
                const dateObj = new Date(row.date);
                if (isNaN(dateObj.getTime())) return null; // 유효하지 않은 날짜 패스

                const currentRowDate = dateObj.toISOString().split('T')[0];

                // 1. historicMax (시작일 이후 갱신)
                if (!startDate || currentRowDate >= startDate) {
                    if (high > hMax) hMax = high;
                }

                // 2. 싸이클 판단 로직
                let judgeDrop = ((low - rMax) / rMax * 100);
                let judgeRise = ((high - rMin) / rMin * 100);

                let prevStatus = currentStatus;
                let turnToDown = "";
                let turnToUp = "";

                if (currentStatus !== "하락" && Math.abs(judgeDrop) >= lowerRate) {
                    currentStatus = "하락";
                    turnToDown = "O";
                } else if (currentStatus !== "상승" && Math.abs(judgeRise) >= upperRate) {
                    currentStatus = "상승";
                    turnToUp = "O";
                }

                // 3. 싸이클 전환 및 극값 갱신
                let renewedHigh = "";
                let renewedLow = "";

                if (prevStatus === "상승" && currentStatus === "하락") {
                    rMin = low;
                } else if (prevStatus === "하락" && currentStatus === "상승") {
                    rMax = high;
                } else {
                    if (high > rMax) { rMax = high; renewedHigh = "O"; }
                    if (low < rMin) { rMin = low; renewedLow = "O"; }
                }

                const currentHMax = hMax === -Infinity ? 0 : hMax;

                return {
                    date: row.date,
                    open_price: row.open_price,
                    high_price: row.high_price,
                    low_price: row.low_price,
                    close_price: row.close_price,
                    historicMax: currentHMax,
                    dropFromHMax: currentHMax > 0 ? ((close - currentHMax) / currentHMax * 100).toFixed(2) : "0.00",
                    runningMax: rMax,
                    closeFromRMax: ((close - rMax) / rMax * 100).toFixed(2),
                    minFromRMax: ((low - rMax) / rMax * 100).toFixed(2),
                    runningMin: rMin,
                    closeFromRMin: ((close - rMin) / rMin * 100).toFixed(2),
                    maxFromRMin: ((high - rMin) / rMin * 100).toFixed(2),
                    renewedHigh: renewedHigh,
                    renewedLow: renewedLow,
                    turnToDown: turnToDown,
                    turnToUp: turnToUp,
                    cycleStatus: currentStatus
                };
            } catch (e) {
                console.error(`Row mroutering error at index ${index}:`, e);
                return null;
            }
        }).filter(item => item !== null); // 에러 난 행 제외

        res.json(results);
    } catch (err) {
        console.error("[백엔드 에러]:", err.message);
        res.status(500).json({ error: err.message });
    }
});

// 티커 전체 조회
// [변경] Firestore에서 가져오기 (백엔드가 프록시 역할)
// 티커 전체 조회
// [변경] 지수(^) 우선 정렬 로직 및 데이터 평탄화 적용
router.get('/tickers', async (req, res) => {
    try {
        // [수정] orderBy('ticker') 대신 데이터를 가져온 후 커스텀 정렬을 수행합니다.
        const snapshot = await firestore.collection('tickers').get();
        
        const tickers = snapshot.docs.map(doc => {
            const fullData = doc.data();
            // [구조 통일] metadata 주머니가 있으면 풀어서 반환, 없으면 그대로 반환
            const dataContent = fullData.metadata ? fullData.metadata : fullData;
            
            return {
                id: doc.id,
                ...dataContent
            };
        });

        // [핵심 로직] ^로 시작하는 지수를 상단으로 보내는 정렬
        tickers.sort((a, b) => {
            const aId = (a.id || a.ticker || "").trim().toUpperCase();
            const bId = (b.id || b.ticker || "").trim().toUpperCase();
            
            const aIsIndex = aId.startsWith('^');
            const bIsIndex = bId.startsWith('^');

            if (aIsIndex === bIsIndex) {
                // numeric: true를 주면 문자열 속 숫자 정렬도 자연스러워집니다.
                return aId.localeCompare(bId, undefined, { numeric: true, sensitivity: 'base' });
            }
            
            return aIsIndex ? -1 : 1;
        });

        // 기존 프론트엔드들이 기대하는 JSON 형식 그대로 반환
        res.json(tickers); 
    } catch (err) {
        console.error("Firestore 조회 에러:", err);
        res.status(500).json({ error: "클라우드 데이터를 불러올 수 없습니다." });
    }
});

// [Backend] 사용자별 관심종목 전제 데이터 반환 API
router.get('/user/investments/:email', verifyToken, async (req, res) => {
    try {
        const { email } = req.params;
        const docRef = firestore.collection('investment_tickers').doc(email);
        const doc = await docRef.get();

        if (!doc.exists) return res.status(200).json([]);

        const data = doc.data();
        let tickerMap = {};

        // 헬퍼 함수: DB 필드(fee_rate, tax_rate)를 프론트에서 쓰는 명칭으로 매핑
        const extractItem = (item, key) => ({
            ticker: item.ticker || key,
            ticker_name_kr: item.ticker_name_kr || "",
            description: item.description || "",
            // [중요] DB 필드명을 그대로 유지하여 프론트 전달
            fee_rate: (item.fee_rate !== undefined && item.fee_rate !== null) ? item.fee_rate : 0,
            tax_rate: (item.tax_rate !== undefined && item.tax_rate !== null) ? item.tax_rate : 0,
            createdAt: item.createdAt || ""
        });

        // [사진 구조 반영] investments 객체 내부 순회
        if (data.investments && typeof data.investments === 'object') {
            Object.keys(data.investments).forEach(key => {
                const itemData = data.investments[key];
                // null 체크 (삭제 대기 데이터 등 방어 코드)
                if (itemData) {
                    tickerMap[key] = extractItem(itemData, key);
                }
            });
        }

        // Dot notation (investments.TQQQ 형태) 필드가 혼재할 경우를 대비한 방어 코드
        Object.keys(data).forEach(key => {
            if (key.startsWith('investments.') && data[key]) {
                const tickerCode = key.split('.')[1];
                tickerMap[tickerCode] = extractItem(data[key], tickerCode);
            }
        });

        const tickerArray = Object.values(tickerMap).sort((a, b) => a.ticker.localeCompare(b.ticker));
        res.status(200).json(tickerArray);
    } catch (error) {
        console.error("[Get investments Error]:", error);
        res.status(500).json({ error: "관심종목 로드 실패" });
    }
});

// ----------------------------------------------------------------
// [수정] 주가 데이터 조회 함수 (Firestore 전용)
// ----------------------------------------------------------------
async function getDailyStockData(ticker, start, end) {
    try {
        const doc = await firestore.collection('ticker_prices').doc(ticker).get();
        
        if (!doc.exists) {
            console.warn(`Firestore에 데이터 없음: ${ticker}`);
            return [];
        }

        const data = doc.data();
        const labels = data.labels || [];
        const prices = data.prices || [];

        // 1. 데이터 매핑 (날짜와 가격을 묶음)
        let rawRows = labels.map((date, index) => {
            const dDate = date.includes('T') ? date.split('T')[0] : date;
            const p = prices[index];

            return {
                date: dDate,
                close_price: p && typeof p === 'object' ? p.c : p,
                open_price:  p && typeof p === 'object' ? p.o : p,
                high_price:  p && typeof p === 'object' ? p.h : p,
                low_price:   p && typeof p === 'object' ? p.l : p
            };
        });

        // 2. [핵심 수정] 날짜(date) 기준 중복 제거 (Map 사용)
        const uniqueMap = new Map();
        rawRows.forEach(row => {
            // 날짜 범위 필터링을 여기서 미리 수행하여 불필요한 연산 감소
            if ((!start || row.date >= start) && (!end || row.date <= end)) {
                uniqueMap.set(row.date, row); // 같은 날짜가 있으면 덮어씌움 (중복 제거)
            }
        });

        // 3. 중복 제거된 데이터를 배열로 변환 후 날짜 오름차순 정렬
        const sortedRows = Array.from(uniqueMap.values()).sort((a, b) => {
            return a.date.localeCompare(b.date);
        });

        return sortedRows;

    } catch (err) {
        console.error(`Firestore 조회 에러 (${ticker}):`, err.message);
        throw err;
    }
}

module.exports = router;