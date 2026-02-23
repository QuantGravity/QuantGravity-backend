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
const getTickerData = async ({ symbol, exchange, country, justList = false } = {}) => {
    const db = admin.firestore();
    
    // [1] 주요 지수 목록 정의 (이 리스트는 항상 체크해야 함)
    const MAJOR_INDICES = [
        'US_SP500', 'US_NASDAQ100', 'US_DOW30', 
        'KR_KOSPI200', 'KR_MSCI', 
        'US_SP100'
    ];

    // 1. 단일 종목 조회 (속도 최적화) - 기존 유지
    if (symbol) {
        const doc = await db.collection('stocks').doc(symbol.toUpperCase()).get();
        return doc.exists ? doc.data() : null;
    }

    // 2. 조회 대상 컬렉션 확보 (수정됨)
    let targetExchanges = [];
    
    if (exchange) {
        targetExchanges.push(exchange);
        
        // 💡 [핵심 수정 1] 특정 거래소를 조회하더라도, 지수 플래그 계산을 위해 지수 컬렉션도 조회 목록에 강제 추가
        MAJOR_INDICES.forEach(idx => {
            // 중복 방지 체크 후 추가
            if (!targetExchanges.includes(idx)) {
                targetExchanges.push(idx);
            }
        });
    } else {
        const metaSnapshot = await db.collection('meta_tickers').get();
        metaSnapshot.forEach(doc => targetExchanges.push(doc.id));
    }

    // 3. 데이터 병합 - 기존 유지
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

                // Flags 로직 (기존 유지)
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
                    Object.keys(flags).forEach(key => {
                        if (flags[key]) existing[key] = true;
                    });
                    if (!existing.country && itemCountry) existing.country = itemCountry;
                    if (!existing.name_ko && item.name_ko) existing.name_ko = item.name_ko;
                    if (!existing.name_en && (item.name_en || item.name)) existing.name_en = item.name_en || item.name;
                } else {
                    const originalExchange = item.ex || item.exchange || item.exch;
                    const finalExchange = originalExchange || exCode;

                    tickerMap.set(tickerCode, {
                        id: tickerCode,
                        ticker: tickerCode,
                        exchange: finalExchange,
                        country: itemCountry, 
                        name_ko: item.name_ko || "", 
                        name_en: item.name_en || item.name || "", 
                        ...item, 
                        ...flags
                    });
                }
            });
        });
    }));

    // 🌟 [새로 추가된 핵심 로직] STOCKS 컬렉션에서 상세 정보(섹터, 산업, 시가총액 등) 가져와서 조인(Join)
    // justList 모드가 아닐 때만 실행하여 불필요한 DB 읽기를 방지합니다.
    if (!justList && tickerMap.size > 0) {
        // 네트워크 낭비를 막기 위해 .select()를 써서 딱 필요한 5개 필드만 가져옵니다.
        const stocksSnap = await db.collection('stocks')
            .select('sector', 'industry', 'snapshot', 'name_ko', 'name_en')
            .get();

        stocksSnap.forEach(doc => {
            const sym = doc.id;
            if (tickerMap.has(sym)) {
                const tData = tickerMap.get(sym);
                const sData = doc.data();

                // 1. 섹터 및 산업 병합
                tData.sector = sData.sector || tData.sector || '-';
                tData.industry = sData.industry || tData.industry || '-';
                
                // 2. 시가총액 병합 (snapshot 안에 있음)
                if (sData.snapshot && sData.snapshot.mktCap) {
                    tData.mktCap = sData.snapshot.mktCap; // 프론트 호환성을 위해 둘 다 세팅
                }

                // 3. 한글명/영문명이 STOCKS에 확실히 있다면 덮어쓰기
                if (sData.name_ko) tData.name_ko = sData.name_ko;
                if (sData.name_en) tData.name_en = sData.name_en;
            }
        });
    }

    // 4. Map을 배열로 변환
    let finalResults = Array.from(tickerMap.values());

    // 🛑 [핵심 수정 2] 최종 결과 필터링 보완
    if (exchange) {
         // 요청한 거래소가 지수 목록에 없는 '일반 거래소'(예: NASDAQ, NYSE)라면 필터링 필요
        const isRequestingIndex = MAJOR_INDICES.includes(exchange);
        
        if (!isRequestingIndex) {
            finalResults = finalResults.filter(r => {
                return r.exchange === exchange || r.exchange?.includes(exchange);
            });
        }
    }

    if (country) {
        const targetCountry = country.toUpperCase();
        finalResults = finalResults.filter(r => {
            const isIndex = String(r.id).startsWith('^');
            if (isIndex) return true;
            const myCountry = r.country || 'US'; 
            return myCountry === targetCountry;
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