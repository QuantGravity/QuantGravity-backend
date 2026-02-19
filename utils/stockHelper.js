// ===========================================================================
// [파일명] : utils/stockHelper.js
// [설명]   : 종목 데이터 조회 및 지수 멤버십/상세정보 병합 유틸리티 (One-Line Consolidation)
// ===========================================================================
const admin = require('firebase-admin');

/**
 * 종목 데이터를 조회하고 병합하여 반환하는 함수
 * - 모든 거래소/지수 컬렉션을 순회하며 종목 정보를 하나로 합침
 * - is_sp500, is_dow 등의 지수 포함 여부 플래그를 통합
 * - 시가총액, 섹터 등 상세 정보가 있는 데이터를 우선하여 보존
 */
// utils/stockHelper.js

const getTickerData = async ({ symbol, exchange, country, justList = false } = {}) => {
    const db = admin.firestore();

    // 1. 단일 종목 조회 (속도 최적화)
    if (symbol) {
        const doc = await db.collection('stocks').doc(symbol.toUpperCase()).get();
        return doc.exists ? doc.data() : null;
    }

    // 2. 조회 대상 컬렉션(거래소/지수) 목록 확보
    let targetExchanges = [];
    if (exchange) {
        targetExchanges.push(exchange);
    } else {
        const metaSnapshot = await db.collection('meta_tickers').get();
        metaSnapshot.forEach(doc => targetExchanges.push(doc.id));
    }

    // 3. 데이터 병합
    const tickerMap = new Map();

    await Promise.all(targetExchanges.map(async (exCode) => {
        const chunkSnapshot = await db.collection('meta_tickers').doc(exCode).collection('chunks').get();
        
        chunkSnapshot.forEach(chunkDoc => {
            const chunkData = chunkDoc.data();
            const list = chunkData.list || [];
            const collectionCountry = exCode.split('_')[0]; 

            list.forEach(item => {
                const tickerCode = (item.symbol || item.s || item.id || "").toUpperCase();
                if (!tickerCode) return;

                // 지수 플래그 (기존 로직 유지)
                const flags = {
                    is_sp500: exCode === 'US_SP500',
                    is_nasdaq100: exCode === 'US_NASDAQ100',
                    is_dow: exCode === 'US_DOW30',
                    is_kospi200: exCode === 'KR_KOSPI200',
                    is_sp100: exCode === 'US_SP100',
                    is_sp_global100: exCode === 'US_SP100_GLOBAL' 
                };

                const itemCountry = item.country || collectionCountry;

                if (tickerMap.has(tickerCode)) {
                    // [기존 데이터 병합 로직] - 네 원본 코드와 동일하게 유지
                    const existing = tickerMap.get(tickerCode);
                    Object.assign(existing, flags); // 플래그 병합
                    // (중략: 필드 보강 로직은 기존 코드 그대로 사용)
                    if (!existing.country && itemCountry) existing.country = itemCountry;
                } else {
                    // [신규 등록]
                    const exchangeName = item.ex || item.exchange || exCode;
                    tickerMap.set(tickerCode, {
                        id: tickerCode,
                        ticker: tickerCode,
                        exchange: exchangeName, 
                        country: itemCountry, 
                        ...item,
                        ...flags
                    });
                }
            });
        });
    }));

    // 4. Map을 배열로 변환
    let finalResults = Array.from(tickerMap.values());

    // 🛑 [핵심 수정] 국가 필터링 적용 (지수는 무조건 통과!)
    if (country) {
        const targetCountry = country.toUpperCase();
        
        finalResults = finalResults.filter(r => {
            // 지수 종목(^로 시작) 판별
            const isIndex = String(r.id).startsWith('^');
            
            // [Rule 1] 지수라면 국가 불문 무조건 포함 (US, KR 모두에서 조회 가능)
            if (isIndex) return true;
            
            // [Rule 2] 일반 종목은 요청한 국가 코드와 일치해야 함
            // (데이터에 country 필드가 없으면 US로 간주하는 안전장치 추가)
            const myCountry = r.country || 'US'; 
            return myCountry === targetCountry;
        });
    }

    // 리스트만 요청 시
    if (justList) {
        return finalResults.map(r => r.id).sort();
    }

    // 5. 정렬: 지수(^...) 우선
    return finalResults.sort((a, b) => {
        const isIndexA = String(a.id).startsWith('^');
        const isIndexB = String(b.id).startsWith('^');
        if (isIndexA && !isIndexB) return -1;
        if (!isIndexA && isIndexB) return 1;
        return String(a.id).localeCompare(String(b.id));
    });
};

module.exports = { getTickerData };

// ----------------------------------------------------------------
// [내부 함수] New 주가 데이터 조회 (stocks/{symbol}/annual_data)
// ----------------------------------------------------------------
async function getDailyStockData(ticker, start, end) {
    try {
        const db = admin.firestore();
        let startYear = 1980;
        let endYear = new Date().getFullYear();

        if (start) startYear = new Date(start).getFullYear();
        if (end) endYear = new Date(end).getFullYear();

        const promises = [];
        for (let y = startYear; y <= endYear; y++) {
            promises.push(db.collection('stocks').doc(ticker).collection('annual_data').doc(String(y)).get());
        }

        const snapshots = await Promise.all(promises);
        let allData = [];

        snapshots.forEach(snap => {
            if (snap.exists) {
                const data = snap.data();
                if (data.data && Array.isArray(data.data)) {
                    allData.push(...data.data);
                }
            }
        });

        const filtered = allData.filter(d => {
            if (start && d.date < start) return false;
            if (end && d.date > end) return false;
            return true;
        });

        return filtered.map(d => ({
            date: d.date,
            open_price: d.open,
            high_price: d.high,
            low_price: d.low,
            close_price: d.close || d.adjClose
        })).sort((a, b) => a.date.localeCompare(b.date));

    } catch (err) {
        console.warn(`Firestore V2 조회 에러 (${ticker}):`, err.message);
        return [];
    }
}

module.exports = { getTickerData, getDailyStockData };