// ===========================================================================
// [파일명] : utils/stockHelper.js
// [설명]   : 종목 마스터 데이터 조회를 위한 통합 유틸리티
// ===========================================================================
const admin = require('firebase-admin');
const fmpClient = require('./fmpClient'); // fmpClient 경로 확인 필요

/**
 * 종목 데이터를 조회하는 만능 함수 (단건, 거래소별, 전체 통합)
 * * @param {Object} options 검색 옵션
 * @param {string} [options.symbol] 특정 종목코드 (예: 'AAPL') - 지정 시 stocks 컬렉션 직접 조회 (가장 빠름)
 * @param {string} [options.exchange] 특정 거래소 (예: 'US_NASDAQ') - 지정 시 해당 거래소 리스트만 반환
 * @param {boolean} [options.justList] true일 경우 상세 정보 대신 종목코드 배열만 반환 ['AAPL', 'TSLA'...]
 * * @returns {Promise<Array|Object>} 결과 배열 또는 단일 객체
 */
const getTickerData = async ({ symbol, exchange, justList = false } = {}) => {
    const db = admin.firestore();

    // -------------------------------------------------------
    // CASE 1: 단일 종목 조회 (Symbol이 주어진 경우)
    // 전략: meta_tickers를 뒤지는 건 비효율적이므로, stocks 컬렉션(ID=Symbol)을 바로 조회
    // -------------------------------------------------------
    if (symbol) {
        const docRef = db.collection('stocks').doc(symbol.toUpperCase());
        const doc = await docRef.get();
        
        if (!doc.exists) return null; // 없으면 null 반환
        return doc.data();
    }

    // -------------------------------------------------------
    // CASE 2: 목록 조회 (전체 또는 특정 거래소)
    // 전략: meta_tickers 하위의 chunks를 순회하며 수집
    // -------------------------------------------------------
    let targetExchanges = [];

    if (exchange) {
        // 특정 거래소만 지정된 경우
        targetExchanges.push(exchange);
    } else {
        // 전체 거래소를 가져와야 하는 경우 (meta_tickers 문서 ID 목록 확보)
        const metaSnapshot = await db.collection('meta_tickers').get();
        metaSnapshot.forEach(doc => targetExchanges.push(doc.id));
    }

    let results = [];

    // 병렬 처리로 속도 향상 (각 거래소의 chunks를 동시에 읽음)
    const promises = targetExchanges.map(async (exCode) => {
        const chunkSnapshot = await db.collection('meta_tickers').doc(exCode).collection('chunks').get();
        
        chunkSnapshot.forEach(chunkDoc => {
            const data = chunkDoc.data();
            // chunks 내부의 'list' 배열 추출
            if (data.list && Array.isArray(data.list)) {
                data.list.forEach(item => {
                    // 데이터 구조 정규화 (s: 심볼, n: 이름 등 축약어 처리 고려)
                    const tickerCode = item.symbol || item.s; 
                    
                    if (tickerCode) {
                        if (justList) {
                            results.push(tickerCode);
                        } else {
                            // 필요한 정보만 정제해서 담기
                            results.push({
                                symbol: tickerCode,
                                name: item.name || item.n || '',
                                exchange: exCode, // 어느 거래소 소속인지 명시
                                ...item
                            });
                        }
                    }
                });
            }
        });
    });

    await Promise.all(promises);

    // 정렬 (알파벳순)
    if (justList) {
        // 중복 제거 후 반환
        return [...new Set(results)].sort();
    } else {
        return results.sort((a, b) => a.symbol.localeCompare(b.symbol));
    }
};

// ===========================================================================
// [공통 함수] 주가/지수 하이브리드 수집 핵심 로직 (API/Batch 겸용)
// ===========================================================================
async function processHybridData(arg1, arg2, arg3, arg4) {
    let symbol, from, to, userEmail, res;

    // ---------------------------------------------------------
    // [1. 입력 어댑터] 호출 방식에 따라 파라미터 매핑
    // ---------------------------------------------------------
    if (arg1 && arg1.body) { 
        // Case A: API 라우터에서 호출됨 (req, res)
        const req = arg1;
        res = arg2;
        symbol = req.body.symbol;
        from = req.body.from;
        to = req.body.to;
        userEmail = req.user ? req.user.email : 'Unknown_User';
    } else { 
        // Case B: 내부 함수나 배치에서 직접 호출됨 (symbol, from, to, email)
        symbol = arg1;
        from = arg2;
        to = arg3;
        userEmail = arg4 || 'System_Batch';
    }

    // 필수값 체크
    if (!symbol) {
        const errorMsg = { error: 'Symbol required' };
        if (res) return res.status(400).json(errorMsg);
        throw new Error(errorMsg.error);
    }

    try {
        const isIndex = symbol.startsWith('^');
        console.log(`[User: ${userEmail}] Hybrid Load for [${symbol}] (Type: ${isIndex ? 'INDEX' : 'STOCK'})`);

        // --------------------------------------------------------------------------
        // [Step 1] 종목 프로필 & 스냅샷 업데이트 (1타 2피 전략)
        // --------------------------------------------------------------------------
        try {
            // FMP v3 Profile API 사용
            const profileRes = await fmpClient.get('/profile', { params: { symbol: symbol } });
            const profile = (profileRes.data && profileRes.data.length > 0) ? profileRes.data[0] : null;

            if (profile) {
                const updateData = {
                    // 1. 고정 정보
                    symbol: profile.symbol,
                    name_en: profile.companyName,
                    exchange: profile.exchangeShortName || profile.exchange,
                    sector: profile.sector,
                    industry: profile.industry,
                    ipoDate: profile.ipoDate,
                    description: profile.description,
                    website: profile.website,
                    currency: profile.currency,
                    image: profile.image,
                    ceo: profile.ceo,

                    // 2. 변동 정보 (스냅샷)
                    snapshot: {
                        price: profile.price,
                        mktCap: profile.mktCap,
                        vol: profile.volAvg,
                        beta: profile.beta,
                        div: profile.lastDiv,
                        range: profile.range,
                        lastUpdated: new Date().toISOString()
                    },

                    // 3. 시스템 정보
                    active: true,
                    last_crawled: new Date().toISOString()
                };

                await admin.firestore().collection('stocks').doc(symbol).set(updateData, { merge: true });
                console.log(`✅ [${symbol}] 프로필 및 스냅샷 업데이트 완료`);
            } else {
                console.log(`⚠️ [${symbol}] 프로필 데이터 없음 (주가 수집만 진행)`);
            }
        } catch (profileErr) {
            console.warn(`⚠️ [${symbol}] 프로필 수집 실패: ${profileErr.message}`);
        }

        // --------------------------------------------------------------------------
        // [Step 2] 과거 주가 데이터 수집 (Historical Price - v4 Stable)
        // --------------------------------------------------------------------------
        
        // 1. 날짜 범위 생성
        const startDate = from ? new Date(from) : new Date('1990-01-01');
        const endDate = to ? new Date(to) : new Date();

        if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
            throw new Error('유효하지 않은 날짜 형식입니다.');
        }

        const dateRanges = [];
        let current = new Date(startDate);

        // 15년 단위 분할 요청
        while (current <= endDate) {
            let next = new Date(current);
            next.setFullYear(current.getFullYear() + 15);
            
            if (next > endDate) next = endDate;

            dateRanges.push({
                from: current.toISOString().split('T')[0],
                to: next.toISOString().split('T')[0]
            });

            if (next >= endDate) break;
            current = new Date(next);
            current.setDate(current.getDate() + 1);
        }

        console.log(`>> [${symbol}] 주가 요청 분할: 총 ${dateRanges.length}개 구간`);

        // 2. 병렬 요청 (Stable Endpoint)
        const fetchPromises = dateRanges.map(async (range) => {
            try {
                const fmpRes = await fmpClient.get('https://financialmodelingprep.com/stable/historical-price-eod/full', { 
                    params: { 
                        symbol: symbol,
                        from: range.from,
                        to: range.to,
                        apikey: process.env.FMP_API_KEY
                    }
                });
                return Array.isArray(fmpRes.data) ? fmpRes.data : (fmpRes.data.historical || []);
            } catch (err) {
                console.warn(`>> 구간 실패 (${range.from}~${range.to}): ${err.message}`);
                return [];
            }
        });

        const results = await Promise.all(fetchPromises);
        let mergedData = results.flat(); 

        if (mergedData.length === 0) {
            const resultMsg = { success: true, message: `프로필은 확인됐으나 주가 데이터가 없습니다.`, symbol };
            if (res) return res.json(resultMsg);
            return resultMsg;
        }

        // 중복 제거 및 정렬
        const uniqueMap = new Map();
        mergedData.forEach(item => uniqueMap.set(item.date, item));
        
        const finalData = Array.from(uniqueMap.values())
            .sort((a, b) => new Date(a.date) - new Date(b.date));

        console.log(`>> 주가 수집 완료: ${finalData.length}일 데이터`);

        // 3. Firestore 저장 (증분 업데이트 최적화)
        const chunks = {};
        finalData.forEach(day => {
            const year = day.date.split('-')[0];
            if (!chunks[year]) chunks[year] = [];
            chunks[year].push(day);
        });

        const batch = admin.firestore().batch();
        const years = Object.keys(chunks);

        for (const year of years) {
            const chunkRef = admin.firestore()
                .collection('stocks').doc(symbol)
                .collection('annual_data').doc(year);

            // [최적화] 해당 연도의 1월 1일 확인
            const yearStart = new Date(`${year}-01-01`);
            const isFullYearCovered = startDate <= yearStart;
            
            let finalYearList = chunks[year];

            // 부분 업데이트인 경우에만 DB 읽기 (Cost 절약)
            if (!isFullYearCovered) {
                const docSnapshot = await chunkRef.get();
                if (docSnapshot.exists) {
                    const existingData = docSnapshot.data().data || [];
                    const dataMap = new Map();
                    // 기존 데이터 + 새 데이터 병합 (새 데이터 우선)
                    existingData.forEach(d => dataMap.set(d.date, d));
                    chunks[year].forEach(d => dataMap.set(d.date, d));
                    
                    finalYearList = Array.from(dataMap.values()).sort((a, b) => new Date(a.date) - new Date(b.date));
                }
            } else {
                // console.log(`>> [Skip Read] ${year}년도는 전체 덮어쓰기 (Optimized)`);
            }

            batch.set(chunkRef, {
                symbol: symbol,
                year: year,
                lastUpdated: new Date().toISOString(),
                data: finalYearList
            }, { merge: true });
        }

        await batch.commit();

        const successResult = {
            success: true,
            symbol: symbol,
            isIndex: isIndex,
            totalYears: years.length,
            totalDays: finalData.length,
            range: `${finalData[0].date} ~ ${finalData[finalData.length - 1].date}`,
            message: 'Updated successfully'
        };

        // ---------------------------------------------------------
        // [3. 응답 어댑터] 호출 방식에 따라 응답 처리
        // ---------------------------------------------------------
        if (res) return res.json(successResult); // API 모드
        return successResult;                    // 배치 모드

    } catch (error) {
        console.error(`Hybrid Load Error [${symbol}]:`, error.message);
        if (res) return res.status(500).json({ error: error.message });
        throw error; // 배치는 에러를 던져서 상위에서 집계하도록 함
    }
}

// 모듈 내보내기 (기존 exports 유지)

module.exports = {
    processHybridData, getTickerData };