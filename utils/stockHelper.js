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
// 🌟 [수정 1] 파라미터에 includeDelisted 추가 (기본값 false)
const getTickerData = async ({ symbol, exchange, country, justList = false, includeDelisted = false } = {}) => {
    const db = admin.firestore();
    
    const MAJOR_INDICES = [
        'US_SP500', 'US_NASDAQ100', 'US_DOW30', 
        'KR_KOSPI200', 'KR_MSCI', 
        'US_SP100'
    ];

    if (symbol) {
        const doc = await db.collection('stocks').doc(symbol.toUpperCase()).get();
        if (doc.exists) {
            const data = doc.data();
            const currency = (data.currency || (data.snapshot && data.snapshot.currency) || '').toUpperCase();
            const stockCountry = (data.country || '').toUpperCase(); 
            
            if (stockCountry && stockCountry !== 'US' && stockCountry !== 'KR') return null;
            if (currency && currency !== 'USD' && currency !== 'KRW') return null;
            return data;
        }
        return null;
    }

    let targetExchanges = [];
    if (exchange) {
        targetExchanges.push(exchange);
        MAJOR_INDICES.forEach(idx => {
            if (!targetExchanges.includes(idx)) targetExchanges.push(idx);
        });
    } else {
        const metaSnapshot = await db.collection('meta_tickers').get();
        metaSnapshot.forEach(doc => targetExchanges.push(doc.id));
    }

    const tickerMap = new Map();

    await Promise.all(targetExchanges.map(async (exCode) => {
        const chunkSnapshot = await db.collection('meta_tickers').doc(exCode).collection('chunks').get();
        
        chunkSnapshot.forEach(chunkDoc => {
            const chunkData = chunkDoc.data();
            const list = chunkData.list || [];
            const collectionCountry = exCode.split('_')[0]; 

            list.forEach(item => {
                const tickerCode = (item.symbol || item.s || item.id || "").trim().toUpperCase();
                if (!tickerCode || tickerCode.length < 1) return;

                const flags = {
                    is_sp500: exCode === 'US_SP500',
                    is_nasdaq100: exCode === 'US_NASDAQ100',
                    is_dow: exCode === 'US_DOW30',
                    is_kospi200: exCode === 'KR_KOSPI200',
                    is_msci_kr: exCode === 'KR_MSCI',
                    is_sp100: exCode === 'US_SP100'
                };

                const itemCountry = item.country || collectionCountry;

                if (tickerMap.has(tickerCode)) {
                    const existing = tickerMap.get(tickerCode);
                    Object.keys(flags).forEach(key => { if (flags[key]) existing[key] = true; });
                    if (!existing.country && itemCountry) existing.country = itemCountry;
                    if (!existing.name_ko && item.name_ko) existing.name_ko = item.name_ko;
                    if (!existing.name_en && (item.name_en || item.name)) existing.name_en = item.name_en || item.name;
                } else {
                    const finalExchange = item.ex || item.exchange || item.exch || exCode;
                    tickerMap.set(tickerCode, {
                        id: tickerCode, ticker: tickerCode, exchange: finalExchange, country: itemCountry, 
                        name_ko: item.name_ko || "", name_en: item.name_en || item.name || "", 
                        ...item, ...flags
                    });
                }
            });
        });
    }));

    // 🌟 [수정 2] 시뮬레이터를 위한 상장폐지 유령 주식 병합 (Bypass 로직)
    if (includeDelisted && !symbol) {
        let delistedQuery = db.collection('stocks').where('isDelisted', '==', true);
        const delistedSnap = await delistedQuery.select('name_ko', 'name_en', 'exchange', 'country', 'sector', 'industry', 'snapshot', 'currency').get();
        
        delistedSnap.forEach(doc => {
            const d = doc.data();
            const stockCountry = (d.country || '').toUpperCase();
            const currency = (d.currency || (d.snapshot && d.snapshot.currency) || '').toUpperCase();
            
            // 국가/통화 2차 방어막 통과 확인
            if (stockCountry && stockCountry !== 'US' && stockCountry !== 'KR') return;
            if (currency && currency !== 'USD' && currency !== 'KRW') return;
            
            tickerMap.set(doc.id, {
                id: doc.id,
                ticker: doc.id,
                exchange: d.exchange || 'UNKNOWN',
                country: stockCountry,
                name_ko: d.name_ko || "",
                name_en: d.name_en || "",
                sector: d.sector || "-",
                industry: d.industry || "-",
                mktCap: d.snapshot?.mktCap || 0,
                isDelisted: true, // 식별 플래그
                active: false
            });
        });
    }

    if (!justList && tickerMap.size > 0) {
        const stocksSnap = await db.collection('stocks')
            .select('sector', 'industry', 'snapshot', 'name_ko', 'name_en', 'country', 'currency')
            .get();

        stocksSnap.forEach(doc => {
            const sym = doc.id;
            if (tickerMap.has(sym)) {
                const tData = tickerMap.get(sym);
                const sData = doc.data();

                const currency = (sData.currency || (sData.snapshot && sData.snapshot.currency) || '').toUpperCase();
                const stockCountry = (sData.country || '').toUpperCase(); 
                
                if (stockCountry && stockCountry !== 'US' && stockCountry !== 'KR') { tickerMap.delete(sym); return; }
                if (currency && currency !== 'USD' && currency !== 'KRW') { tickerMap.delete(sym); return; }

                tData.sector = sData.sector || tData.sector || '-';
                tData.industry = sData.industry || tData.industry || '-';
                
                if (sData.country) tData.country = sData.country;
                if (sData.snapshot && sData.snapshot.mktCap) tData.mktCap = sData.snapshot.mktCap; 
                if (sData.name_ko) tData.name_ko = sData.name_ko;
                if (sData.name_en) tData.name_en = sData.name_en;
            }
        });
    }

    let finalResults = Array.from(tickerMap.values());

    if (exchange) {
        const isRequestingIndex = MAJOR_INDICES.includes(exchange);
        if (!isRequestingIndex) {
            finalResults = finalResults.filter(r => r.exchange === exchange || r.exchange?.includes(exchange));
        }
    }

    if (country) {
        const targetCountry = country.toUpperCase();
        finalResults = finalResults.filter(r => {
            if (String(r.id).startsWith('^')) return true;
            return (r.country || 'US') === targetCountry;
        });
    }

    if (justList) {
        return finalResults.map(r => r.id).sort();
    }

    return finalResults.sort((a, b) => {
        const isIndexA = String(a.id).startsWith('^');
        const isIndexB = String(b.id).startsWith('^');
        if (isIndexA && !isIndexB) return -1;
        if (!isIndexA && isIndexB) return 1;
        return String(a.id).localeCompare(String(b.id));
    });
};

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