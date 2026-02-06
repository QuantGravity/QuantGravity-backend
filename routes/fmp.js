// ===========================================================================
// [파일명] : routes/fmp.js
// [대상]   : FMP(Financial Modeling Prep) API 연동 및 기초 마스터 데이터 수집
// [기준]   : 
//   1. 데이터 정제: 불필요한 회사명(Inc, Corp 등)은 shortenName 유틸을 통해 정규화한다.
//   2. 청크 저장: 컬렉션당 문서 개수 제한을 피하기 위해 CHUNK_SIZE(600) 단위로 분할 저장한다.
//   3. 이력 관리: 과거 주가 데이터는 5000건 제한 우회를 위해 15년 단위 구간별 병렬 요청을 수행한다.
//   4. 자동화 최적화: 연도별 데이터(annual_data)는 연 단위로 파티셔닝하여 쿼리 효율을 높인다.
// ===========================================================================

const express = require('express');
const router = express.Router();
const fmpClient = require('../utils/fmpClient');
const admin = require('firebase-admin');
const { verifyToken } = require('../utils/authHelper');
const { processHybridData } = require('../utils/stockHelper'); // ★ 여기서 가져옴

// ======================================================================
// [통합 기능] 티커 마스터 동기화 (미국 시장 전용 + 600개 청크 분할 저장)
// ======================================================================
router.post('/sync-ticker-master', verifyToken, async (req, res) => {
    const { mode = 'FULL', limit = 100, exchangeCode } = req.body; 

    console.log(`[Ticker Sync] 요청 모드: ${mode} / 옵션: ${exchangeCode || 'All'} / 제한: ${limit}`);

    // [Helper] 스마트 이름 단축 함수
    const shortenName = (name) => {
        if (!name) return "";
        return name
            .replace(/,?\s*Inc\.?$/i, "")
            .replace(/,?\s*Corp\.?$/i, "")
            .replace(/,?\s*Corporation$/i, "")
            .replace(/,?\s*Ltd\.?$/i, "")
            .replace(/,?\s*Limited$/i, "")
            .replace(/,?\s*PLC$/i, "")
            .replace(/,?\s*L\.?P\.?$/i, "")
            .replace(/,?\s*L\.?L\.?C\.?$/i, "")
            .replace(/,?\s*Co\.?$/i, "")
            .replace(/,?\s*Company$/i, "")
            .trim();
    };

    try {
        let responseData = [];
        let sourceName = "";
        const endpoint = '/company-screener';
        
        let params = { apikey: process.env.FMP_API_KEY };

        if (mode === 'SAMPLE') {
            sourceName = "🧪 샘플 (Top 100)";
            params.limit = limit;
            params.exchange = 'NASDAQ,NYSE,AMEX'; 
        } else if (mode === 'EXCHANGE' && exchangeCode) {
            sourceName = `🏛️ 특정 거래소 (${exchangeCode})`;
            params.limit = 20000; 
            params.exchange = exchangeCode;
        } else {
            sourceName = "🚀 전체 동기화 (US Only)";
            params.limit = 60000; 
        }

        console.log(`>> FMP 요청 시작 (${sourceName})...`);
        const resFmp = await fmpClient.get(endpoint, { params });
        responseData = resFmp.data;
        console.log(`>> 데이터 수신 완료. 총 ${responseData.length}개.`);

        if (!responseData || responseData.length === 0) {
            return res.json({ success: false, message: "FMP 데이터 없음" });
        }

        const groupedData = {};
        let skippedCount = 0; 
        
        responseData.forEach(item => {
            if (!item.symbol || (!item.companyName && !item.name)) return;

            // [기본 필터링]
            if (!item.isActivelyTrading || item.exchangeShortName === 'OTC' || item.isFund === true) {
                skippedCount++; return; 
            }
            if ((item.marketCap || 0) < 10000000) { // 10M $
                skippedCount++; return;
            }
            if ((item.volume || 0) === 0) {
                skippedCount++; return;
            }

            // [국가 및 거래소 정제]
            let exchange = item.exchangeShortName || 'OTC';
            if (mode === 'EXCHANGE' && exchangeCode) exchange = exchangeCode; 
            exchange = exchange.replace(/\//g, '_').replace(/\./g, '');

            let country = item.country || 'US';
            if (['KSE', 'KOSDAQ', 'KOE'].includes(exchange)) country = 'KR';

            if (country !== 'US') {
                skippedCount++; return;
            }
            
            const docId = `${country}_${exchange}`;
            if (!groupedData[docId]) groupedData[docId] = [];
            
            groupedData[docId].push({
                symbol: item.symbol,
                name:   shortenName(item.companyName || item.name),
                ex:     item.exchangeShortName, 
                sec:    item.sector,
                ind:    item.industry,
                cap:    item.marketCap,         
                price:  item.price,
                vol:    item.volume,
                beta:   item.beta,
                div:    item.lastAnnualDividend,
                etf:    item.isEtf              
            });
        });

        console.log(`>> 최종 필터링: ${skippedCount}개 제외. 유효 ${responseData.length - skippedCount}개.`);

        // 3. Firestore 저장 (Batch & Chunking)
        let batch = admin.firestore().batch();
        let opCount = 0;
        let savedGroups = 0;

        const targetCollection = (mode === 'SAMPLE') ? '_debug_sample' : 'meta_tickers';
        const finalColRef = admin.firestore().collection(targetCollection);
        
        // [수정] 청크 사이즈 600으로 상향 (S&P 500 등 최적화)
        const CHUNK_SIZE = 600; 

        // SAMPLE 모드는 그냥 저장 (구조 확인용)
        if (mode === 'SAMPLE') {
            let sampleCount = 0;
            for (const docId in groupedData) {
                const list = groupedData[docId];
                for (const item of list) {
                    if (sampleCount >= limit) break;
                    batch.set(finalColRef.doc(`sample_${sampleCount}`), item); 
                    sampleCount++;
                }
            }
            await batch.commit();
            return res.json({ success: true, mode, validCount: sampleCount, message: "샘플 저장 완료" });
        }

        // FULL, EXCHANGE 모드는 청크 분할 저장
        for (const [docId, list] of Object.entries(groupedData)) {
            const mainDocRef = finalColRef.doc(docId);
            const chunkColRef = mainDocRef.collection('chunks'); // 컬렉션 참조 생성

            // [CleanUp] 기존 청크 삭제 (중복 방지)
            const existingChunks = await chunkColRef.get();
            if (!existingChunks.empty) {
                const deleteBatch = admin.firestore().batch();
                existingChunks.docs.forEach(doc => {
                    deleteBatch.delete(doc.ref);
                });
                await deleteBatch.commit();
            }

            const [c, e] = docId.split('_');
            const totalChunks = Math.ceil(list.length / CHUNK_SIZE);

            // (1) 메인 문서에는 메타데이터만 저장
            batch.set(mainDocRef, {
                country: c,
                exchange: e,
                count: list.length,
                updatedAt: new Date().toISOString(),
                isChunked: true,       
                chunkCount: totalChunks 
            }, { merge: true });
            
            opCount++;

            // (2) 서브컬렉션에 데이터 분할 저장
            for (let i = 0; i < totalChunks; i++) {
                const chunkList = list.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
                const chunkRef = mainDocRef.collection('chunks').doc(`batch_${i}`);
                
                batch.set(chunkRef, {
                    chunkIndex: i,
                    list: chunkList
                });
                
                opCount++;
                if (opCount >= 400) {
                    await batch.commit();
                    batch = admin.firestore().batch();
                    opCount = 0;
                }
            }
            savedGroups++;
        }
        if (opCount > 0) await batch.commit();

        res.json({
            success: true,
            mode: mode,
            totalTickers: responseData.length,
            validCount: responseData.length - skippedCount,
            savedGroups: savedGroups,
            targetCollection: targetCollection,
            isChunked: true // 프론트엔드에 청크 저장 알림
        });

    } catch (error) {
        console.error("Ticker Sync Error:", error.message);
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [기능 2] 주요 지수(Index) 구성종목 및 전체 지수 동기화 (완결판 + 600 청크)
// ============================================================
router.post('/sync-index-master', verifyToken, async (req, res) => {
    try {
        console.log("지수 데이터 동기화 시작 (Chunk Size: 600)...");

        // [내부 함수] 데이터를 600개씩 쪼개서 저장하는 공통 함수
        const saveToFirestoreWithChunking = async (docId, description, rawList) => {
            const CHUNK_SIZE = 600; // [수정] 600개로 상향
            const mainDocRef = admin.firestore().collection('meta_tickers').doc(docId);
            const chunkColRef = mainDocRef.collection('chunks');

            // 1. 기존 청크 삭제 (CleanUp)
            const existingChunks = await chunkColRef.get();
            if (!existingChunks.empty) {
                const deleteBatch = admin.firestore().batch();
                existingChunks.docs.forEach(doc => deleteBatch.delete(doc.ref));
                await deleteBatch.commit();
            }

            // 2. 메인 문서 저장 (메타데이터)
            const totalChunks = Math.ceil(rawList.length / CHUNK_SIZE);
            await mainDocRef.set({
                country: 'US',
                exchange: 'INDEX', 
                description: description,
                count: rawList.length,
                isChunked: true,
                chunkCount: totalChunks,
                updatedAt: new Date().toISOString()
            }, { merge: true });

            // 3. 청크 분할 저장
            let batch = admin.firestore().batch();
            let opCount = 0;

            for (let i = 0; i < totalChunks; i++) {
                const chunkList = rawList.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
                const chunkRef = chunkColRef.doc(`batch_${i}`);
                
                batch.set(chunkRef, { chunkIndex: i, list: chunkList });
                
                opCount++;
                if (opCount >= 400) {
                    await batch.commit();
                    batch = admin.firestore().batch();
                    opCount = 0;
                }
            }
            if (opCount > 0) await batch.commit();
            console.log(`>> [${docId}] 저장 완료 (${rawList.length}개)`);
        };

        // 1. S&P 500 (Simple Mode: sp500-constituent)
        try {
            const sp500Res = await fmpClient.get('/sp500-constituent');
            if (sp500Res.data && Array.isArray(sp500Res.data)) {
                const list = sp500Res.data.map(i => ({ s: i.symbol, n: i.name, sec: i.sector }));
                await saveToFirestoreWithChunking('US_SP500', 'S&P 500 Constituents', list);
            }
        } catch (e) { 
            console.warn(`S&P500 fetch failed: ${e.message}`); 
        }
        
        // 2. Nasdaq 100 (Simple Mode: nasdaq-constituent)
        try {
            const ndxRes = await fmpClient.get('/nasdaq-constituent');
            if (ndxRes.data && Array.isArray(ndxRes.data)) {
                const list = ndxRes.data.map(i => ({ s: i.symbol, n: i.name, sec: i.sector }));
                await saveToFirestoreWithChunking('US_NASDAQ100', 'NASDAQ 100 Constituents', list);
            }
        } catch (e) { 
            console.warn(`Nasdaq100 fetch failed: ${e.message}`); 
        }

        // 3. 모든 지수 목록 (Simple Mode: index-list)
        try {
            const indexRes = await fmpClient.get('/index-list');
            
            if (indexRes.data && Array.isArray(indexRes.data)) {
                console.log(`>> 전체 지수 ${indexRes.data.length}개 수신 성공! 저장 시작...`);

                const allIndices = indexRes.data.map(i => ({
                    symbol: i.symbol,
                    name: i.name || i.symbol, 
                    ex: i.exchange || 'INDEX', 
                    currency: i.currency || 'USD'
                }));
                
                if (allIndices.length > 0) {
                    await saveToFirestoreWithChunking('US_INDEX', 'All Global Indices', allIndices);
                }
            } else {
                console.warn("지수 목록 데이터가 비어있습니다.");
            }
        } catch (e) { 
            console.warn(`All Index fetch failed: ${e.message}`); 
        }

        res.json({ success: true, message: "지수 전체 동기화 작업 완료" });

    } catch (error) {
        console.error("Index Sync Error:", error.message);
        res.status(500).json({ error: error.message });
    }
});

// [기능 3] 상장폐지 종목 (거래소별 그룹핑 + 600개 청크 저장)
router.post('/sync-delisted-master', verifyToken, async (req, res) => {
    try {
        console.log("상장폐지 종목 동기화 시작 (Group by Exchange & Chunking)...");
        
        // 1. 모든 데이터 수집 (메모리에 적재)
        let allDelisted = [];
        let page = 0;
        let hasMoreData = true;
        const LIMIT = 1000; 

        // (1) FMP에서 전체 데이터 가져오기
        while (hasMoreData) {
            try {
                const response = await fmpClient.get('/delisted-companies', {
                    params: { page: page, limit: LIMIT }
                });
                const data = response.data;

                if (!data || data.length === 0) {
                    hasMoreData = false;
                    console.log(`>> 수집 종료. 총 ${allDelisted.length}개 데이터 확보.`);
                    break;
                }

                // 데이터 정제
                const cleanedData = data.map(item => ({
                    s: item.symbol,
                    n: item.companyName,
                    ex: item.exchange,
                    delDate: item.delistedDate, 
                    ipoDate: item.ipoDate
                }));

                allDelisted.push(...cleanedData);
                
                if (page % 5 === 0) console.log(`>> Page ${page} 수신 중...`);
                page++;
                
                // (안전장치)
                if (page > 100) break; 

            } catch (err) {
                console.error(`>> Page ${page} 에러:`, err.message);
                hasMoreData = false; 
            }
        }

        if (allDelisted.length === 0) {
            return res.json({ success: false, message: "수신된 데이터가 없습니다." });
        }

        // (2) 거래소별 그룹핑 (Grouping by Exchange)
        const groupedByExchange = {};
        
        allDelisted.forEach(item => {
            // 거래소 이름 정규화 (특수문자 제거 등)
            let exchange = item.ex || 'Unknown';
            exchange = exchange.replace(/\//g, '_').replace(/\./g, '').trim(); // 슬래시, 점 제거
            
            if (!groupedByExchange[exchange]) {
                groupedByExchange[exchange] = [];
            }
            groupedByExchange[exchange].push(item);
        });

        // (3) Firestore 저장 (거래소별 문서 + 청크)
        const collectionRef = admin.firestore().collection('meta_delisted');
        const CHUNK_SIZE = 600;
        let savedExchanges = 0;
        let batch = admin.firestore().batch();
        let opCount = 0;

        for (const [exchange, list] of Object.entries(groupedByExchange)) {
            const mainDocRef = collectionRef.doc(exchange);
            const chunkColRef = mainDocRef.collection('chunks');

            // [CleanUp] 기존 청크 삭제 (중복 방지)
            const existingChunks = await chunkColRef.get();
            if (!existingChunks.empty) {
                const deleteBatch = admin.firestore().batch();
                existingChunks.docs.forEach(doc => deleteBatch.delete(doc.ref));
                await deleteBatch.commit();
                console.log(`>> [CleanUp] ${exchange} 기존 청크 삭제 완료.`);
            }

            // 메인 문서 저장 (메타데이터)
            const totalChunks = Math.ceil(list.length / CHUNK_SIZE);
            batch.set(mainDocRef, {
                exchange: exchange,
                count: list.length,
                chunkCount: totalChunks,
                isChunked: true,
                updatedAt: new Date().toISOString()
            }, { merge: true }); // merge: true로 기존 필드 보존

            opCount++;

            // 청크 분할 저장
            for (let i = 0; i < totalChunks; i++) {
                const chunkList = list.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
                const chunkRef = chunkColRef.doc(`batch_${i}`);
                
                batch.set(chunkRef, {
                    chunkIndex: i,
                    list: chunkList
                });
                
                opCount++;
                if (opCount >= 400) {
                    await batch.commit();
                    batch = admin.firestore().batch();
                    opCount = 0;
                }
            }
            savedExchanges++;
        }

        if (opCount > 0) await batch.commit();

        console.log(`✅ 저장 완료! 총 ${savedExchanges}개 거래소 문서 생성.`);
        
        res.json({ 
            success: true, 
            totalCount: allDelisted.length, 
            exchanges: Object.keys(groupedByExchange),
            chunkSize: CHUNK_SIZE
        });

    } catch (error) {
        console.error("Delisted Sync Error:", error.message);
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [기능 4] 기업 이벤트 (배당, 분할) - Stocks 하위 컬렉션 저장
// ============================================================
router.post('/load-corporate-actions', verifyToken, async (req, res) => {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Symbol required' });

    try {
        const batch = admin.firestore().batch();
        let updateCount = 0;
        
        // 1. 배당 정보 (Dividend)
        try {
            // FMP: Historical Stock Dividend
            const divRes = await fmpClient.get(`/historical-price-full/stock_dividend/${symbol}`);
            
            if (divRes.data && divRes.data.historical) {
                // 최근 5년치 정도만 가져오거나 전체를 가져옴
                divRes.data.historical.forEach(d => {
                    // stocks/{symbol}/dividends/{date}
                    const docRef = admin.firestore()
                        .collection('stocks').doc(symbol)
                        .collection('dividends').doc(d.date);
                    
                    batch.set(docRef, d);
                    updateCount++;
                });
            }
        } catch (e) { 
            console.warn(`Dividend Error for ${symbol}: ${e.message}`); 
        }

        // 2. 액면분할 (Stock Split)
        try {
            const splitRes = await fmpClient.get(`/historical-price-full/stock_split/${symbol}`);
            
            if (splitRes.data && splitRes.data.historical) {
                splitRes.data.historical.forEach(s => {
                    // stocks/{symbol}/splits/{date} -> 분할은 별도 컬렉션 추천 (데이터가 적음)
                    const docRef = admin.firestore()
                        .collection('stocks').doc(symbol)
                        .collection('splits').doc(s.date);
                    
                    batch.set(docRef, { 
                        date: s.date,
                        label: s.label,
                        numerator: s.numerator,
                        denominator: s.denominator,
                        ratio: `${s.numerator}:${s.denominator}` 
                    });
                    updateCount++;
                });
            }
        } catch (e) { 
            console.warn(`Split Error for ${symbol}: ${e.message}`); 
        }

        if (updateCount > 0) await batch.commit();
        res.json({ success: true, symbol, count: updateCount });

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [기능 5] 재무제표 수집 (Income, Balance, CashFlow)
// ============================================================
router.post('/load-financials', verifyToken, async (req, res) => {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Symbol required' });

    try {
        const db = admin.firestore();
        const batch = db.batch();
        let savedTypes = [];

        // 재무제표 3종 세트
        const stmts = [
            { type: 'income-statement', url: '/income-statement' },
            { type: 'balance-sheet-statement', url: '/balance-sheet-statement' },
            { type: 'cash-flow-statement', url: '/cash-flow-statement' }
        ];

        for (const stmt of stmts) {
            try {
                // 연간(Annual) 데이터 기준, 최근 30년치 수집
                const res = await fmpClient.get(`${stmt.url}/${symbol}`, { 
                    params: { limit: 30 } 
                });

                if (res.data && res.data.length > 0) {
                    // stocks/{symbol}/financials/{type} 문서에 'history' 배열로 통째로 저장
                    // 이유: 재무제표는 보통 한 번에 로드해서 차트를 그림
                    const docRef = db.collection('stocks').doc(symbol)
                                     .collection('financials').doc(stmt.type);
                    
                    batch.set(docRef, {
                        type: stmt.type,
                        symbol: symbol,
                        updatedAt: new Date().toISOString(),
                        history: res.data // 배열 전체 저장
                    });
                    savedTypes.push(stmt.type);
                }
            } catch (e) { 
                console.warn(`${stmt.type} fail: ${e.message}`); 
            }
        }

        if (savedTypes.length > 0) await batch.commit();
        
        res.json({ 
            success: true, 
            symbol, 
            message: `${savedTypes.join(', ')} 저장 완료` 
        });

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [프리미엄] 주식/지수 하이브리드 수집 (개별)
// ============================================================
router.post('/load-hybrid-data', verifyToken, async (req, res) => {
    const { symbol, from, to } = req.body;
    const userEmail = req.user ? req.user.email : 'Unknown';

    if (!symbol) return res.status(400).json({ error: 'Symbol required' });

    try {
        // 공통 함수 호출
        const result = await processHybridData(symbol, from, to, userEmail);
        res.json(result);
    } catch (error) {
        console.error(`Hybrid Load Error [${symbol}]:`, error.message);
        res.status(500).json({ error: error.message });
    }
});

module.exports = router;