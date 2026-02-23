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
const { verifyToken, verifyBatchOrAdmin } = require('../utils/authHelper');
const { getTickerData, getDailyStockData } = require('../utils/stockHelper');

// ============================================================
// [기능 1] 통합 종목 조회 (만능 API) - 리팩토링 버전
// ============================================================
router.get('/symbol-lookup', verifyToken, async (req, res) => {
    try {
        // 1. 프론트엔드에서 보낸 파라미터 수신
        const { symbol, exchange, country, justList } = req.query;

        // 2. 헬퍼 함수 호출 시 country 파라미터가 누락되지 않도록 주의!
        const results = await getTickerData({
            symbol,
            exchange,
            country, // 🛑 [핵심 수정] 이 부분이 빠져 있었어! 꼭 넣어줘야 해.
            justList: justList === 'true'
        });

        if (symbol && !results) {
            return res.json({ success: false, message: "Symbol not found" });
        }

        res.json({ 
            success: true, 
            count: Array.isArray(results) ? results.length : 1,
            [symbol ? 'data' : 'symbols']: results 
        });
    } catch (error) {
        console.error("Symbol Lookup Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// =============================================================
// [기능 2] 티커 검색 (New: meta_tickers 기반 + ETF 관리 지원)
// =============================================================
// [기능 2] 티커 검색 및 전체 로드 (로컬 필터링 지원용)
router.post('/ticker-search', verifyToken, async (req, res) => {
    try {
        const { type, keyword, userGrade } = req.body;
        
        const tokenRole = req.user ? req.user.role : null;
        const isAdmin = ['admin', 'G9'].includes(tokenRole);
        const isFreeUser = !isAdmin && (!userGrade || userGrade === 'FREE');

        // 전체 리스트 확보 (getTickerData는 이미 정렬 및 기본 정규화가 되어 있다고 가정)
        let allTickers = await getTickerData();

        // [보안] 무료 유저인 경우 지수(^)가 아닌 데이터는 필터링하여 보안 유지
        if (isFreeUser) {
            allTickers = allTickers.filter(item => (item.id || "").startsWith('^'));
        }

        // 클라이언트에서 'ALL'을 요청하면 필터링 없이 전체 반환 (로컬 캐싱용)
        if (type === 'ALL') {
            return res.json(allTickers);
        }

        // 기존 하위 호환을 위한 서버 필터링 로직 (필요시 유지)
        const filteredList = allTickers.filter(item => {
            const tCode = item.id.toUpperCase();
            const isIndex = tCode.startsWith('^');
            
            if (type === '1') return !isIndex; // 지수ETF 등
            if (type === '8') return isIndex;  // 지수
            return true;
        });

        res.json(filteredList);

    } catch (e) {
        console.error("Search API Error:", e);
        res.status(500).json({ error: "데이터 로드 중 오류가 발생했습니다." });
    }
});

// ============================================================
// [기능 3] 티커 상세 속성 일괄 조회 (Bulk Attributes)
// ============================================================
router.post('/get-attributes-bulk', verifyToken, async (req, res) => {
    try {
        const { tickers } = req.body; 
        if (!tickers || !Array.isArray(tickers) || tickers.length === 0) {
            return res.json({});
        }

        const db = admin.firestore();
        // ID를 알고 있으므로 map을 이용해 refs 생성
        const refs = tickers.map(t => db.collection('stocks').doc(t));
        
        // Firestore의 getAll을 사용하여 읽기 (비용 절감 및 속도 향상)
        const snapshots = await db.getAll(...refs);

        const resultMap = {};
        snapshots.forEach(doc => {
            if (doc.exists) {
                const d = doc.data();
                // 필요한 필드만 추출해서 반환
                resultMap[doc.id] = {
                    leverage: d.leverage_factor || '1',
                    underlying: d.underlying_ticker || '',
                    name_kr: d.ticker_name_kr || '',
                    confirm_status: d.confirm_status || 'N' // <--- 이 줄을 꼭 추가해야 해!
                };
            }
        });

        res.json(resultMap);

    } catch (e) {
        console.error("Bulk Attribute Error:", e);
        res.status(500).json({ error: e.message });
    }
});

// ============================================================
// [기능 4] 주가 데이터 조회
// ============================================================
router.get('/daily-stock', async (req, res) => {
    const { ticker, startDate, endDate, upperRate: uR, lowerRate: lR } = req.query;
    const upperRate = parseFloat(uR) || 30; 
    const lowerRate = parseFloat(lR) || 15;

    try {
        const rows = await getDailyStockData(ticker, startDate, endDate);

        if (rows.length === 0) return res.json([]);

        // 싸이클 분석 엔진 (기존 로직 유지)
        let hMax = -Infinity; 
        let rMax = parseFloat(rows[0].high_price); 
        let rMin = parseFloat(rows[0].low_price);  
        let currentStatus = "-";

        const results = rows.map((row) => {
            try {
                const high = parseFloat(row.high_price);
                const low = parseFloat(row.low_price);
                const close = parseFloat(row.close_price);
                
                if (startDate && row.date < startDate) return null;
                if (high > hMax) hMax = high;

                let judgeDrop = ((low - rMax) / rMax * 100);
                let judgeRise = ((high - rMin) / rMin * 100);
                let prevStatus = currentStatus;
                let turnToDown = "", turnToUp = "";

                if (currentStatus !== "하락" && Math.abs(judgeDrop) >= lowerRate) {
                    currentStatus = "하락"; turnToDown = "O";
                } else if (currentStatus !== "상승" && Math.abs(judgeRise) >= upperRate) {
                    currentStatus = "상승"; turnToUp = "O";
                }

                let renewedHigh = "", renewedLow = "";
                if (prevStatus === "상승" && currentStatus === "하락") rMin = low;
                else if (prevStatus === "하락" && currentStatus === "상승") rMax = high;
                else {
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
                    minFromRMax: ((low - rMax) / rMax * 100).toFixed(2),
                    runningMin: rMin,
                    maxFromRMin: ((high - rMin) / rMin * 100).toFixed(2),
                    renewedHigh, renewedLow, turnToDown, turnToUp,
                    cycleStatus: currentStatus
                };
            } catch (e) { return null; }
        }).filter(item => item !== null);

        res.json(results);
    } catch (err) {
        console.error("[Daily Stock Error]:", err.message);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================
// [기능 5] 사용자별 관심종목 조회
// ============================================================
router.get('/user/investments/:email', verifyToken, async (req, res) => {
    try {
        const { email } = req.params;
        const doc = await firestore.collection('investment_tickers').doc(email).get();
        if (!doc.exists) return res.status(200).json([]);

        const data = doc.data();
        let tickerMap = {};

        const extractItem = (item, key) => ({
            ticker: item.ticker || key,
            ticker_name_kr: item.ticker_name_kr || "",
            description: item.description || "",
            fee_rate: item.fee_rate || 0,
            tax_rate: item.tax_rate || 0,
            createdAt: item.createdAt || ""
        });

        if (data.investments) {
            Object.keys(data.investments).forEach(key => {
                if(data.investments[key]) tickerMap[key] = extractItem(data.investments[key], key);
            });
        }
        
        Object.keys(data).forEach(key => {
            if (key.startsWith('investments.') && data[key]) {
                const tickerCode = key.split('.')[1];
                tickerMap[tickerCode] = extractItem(data[key], tickerCode);
            }
        });

        res.status(200).json(Object.values(tickerMap).sort((a, b) => a.ticker.localeCompare(b.ticker)));
    } catch (error) {
        res.status(500).json({ error: "관심종목 로드 실패" });
    }
});

// [기능] 섹터/산업 마스터 데이터 조회 (한글 매핑용)
router.get('/meta-sectors', verifyToken, async (req, res) => {
    try {
        const db = admin.firestore();
        // GICS_Standard 문서 혹은 meta_sectors 컬렉션 전체를 가져옴
        const snapshot = await db.collection('meta_sectors').get();
        
        let sectorMap = {};

        // 문서 구조에 따라 다르지만, 보통 { 영문명: 한글명 } 형태의 맵을 기대함
        // 만약 DB가 계층형(Hierarchy)이라면 여기서 평탄화(Flatten)해서 내려주는 것이 프론트에서 쓰기 편함
        
        snapshot.forEach(doc => {
            const data = doc.data();
            // 예: data = { "Technology": "기술", "Software": "소프트웨어" ... }
            // 또는 data.hierarchy 구조일 경우 재귀적으로 파싱 필요
            // 여기서는 단순 병합으로 처리 (필요시 DB구조에 맞춰 수정)
            Object.assign(sectorMap, data);
            
            // 만약 hierarchy 필드에 들어있다면:
            if (data.hierarchy) {
                // hierarchy 순회하며 매핑 추출 로직 (예시)
                for (const [secEng, content] of Object.entries(data.hierarchy)) {
                    // content가 한글명 스트링이거나, 객체 내부에 한글명이 있거나
                    // DB 구조에 맞춰 매핑 추가
                }
            }
            
            // [중요] 사용자가 수기로 관리하는 'translations' 필드가 있다고 가정하거나
            // 혹은 프론트엔드에서 하드코딩된 맵을 기본으로 쓰고 DB는 보정용으로 쓸 수도 있음.
            if (data.translations) {
                Object.assign(sectorMap, data.translations);
            }
        });

        res.json(sectorMap);

    } catch (error) {
        console.error("Sector Load Error:", error);
        res.status(500).json({});
    }
});

// [stocks.js]
// 비활성 종목 리스트 조회 API
router.get('/inactive-list', verifyBatchOrAdmin, async (req, res) => {
    try {
        // 1. 쿼리 파라미터에서 국가(country) 값 추출
        const { country } = req.query; 

        const db = admin.firestore();
        
        // 2. 기본 쿼리: active가 false인 문서
        let stockQuery = db.collection('stocks').where('active', '==', false);

        // 3. 국가 값이 전달된 경우 국가 조건 추가 필터링
        // 주의: 파이어스토어 stocks 컬렉션에 국가를 저장하는 필드명이 'country'가 맞는지 확인 필요해!
        if (country) {
            stockQuery = stockQuery.where('country', '==', country);
        }

        const snapshot = await stockQuery.get();

        const inactiveList = [];
        snapshot.forEach(doc => {
            inactiveList.push({
                id: doc.id,
                symbol: doc.data().symbol,
                name_en: doc.data().name_en,
                name_ko: doc.data().name_ko
            });
        });

        res.json({ success: true, count: inactiveList.length, data: inactiveList });
    } catch (error) {
        console.error("Fetch Inactive Stocks Error:", error);
        res.status(500).json({ error: error.message });
    }
});

module.exports = router; // 기존처럼 라우터 자체를 내보냄