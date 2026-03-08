// ===========================================================================
// [파일명] : routes/fmp.js
// [대상]   : FMP(Financial Modeling Prep) API 연동 및 기초 마스터 데이터 수집
// [기준]   : 
//   1. 데이터 정제: 불필요한 회사명(Inc, Corp 등)은 shortenName 유틸을 통해 정규화한다.
//   2. 청크 저장: 컬렉션당 문서 개수 제한을 피하기 위해 CHUNK_SIZE(600) 단위로 분할 저장한다.
//   3. 이력 관리: 과거 주가 데이터는 5000건 제한 우회를 위해 15년 단위 구간별 병렬 요청을 수행한다.
//   4. 자동화 최적화: 연도별 데이터(annual_data)는 연 단위로 파티셔닝하여 쿼리 효율을 높인다.
// [규칙]   : 
//   1. 모든 통신은 utils/fmpClient를 통해서만 수행한다. (BaseURL: /stable, Key 자동 주입)
//   2. URL 하드코딩 금지. 엔드포인트 경로만 사용한다. (예: '/available-sectors')
//   3. API Key를 이 파일에서 직접 호출하지 않는다.
// ===========================================================================

const express = require('express');
const router = express.Router();
const fmpClient = require('../utils/fmpClient');
const admin = require('firebase-admin');
const { verifyToken, verifyBatchOrAdmin } = require('../utils/authHelper');
const { askJarvis } = require('../utils/jarvisClient'); // ⚡ [추가] 자비스 호출
const { getTickerData } = require('../utils/stockHelper');
const errorManager = require('../utils/errorManager'); // 🌟 추가!

// ---------------------------------------------------------------------------
// [Helper] 종목 유효성 검사 (Strict Whitelist - 우선주, 펀드, 장외주식 완벽 차단)
// ---------------------------------------------------------------------------
function isValidTicker(symbol) {
    if (!symbol) return false;
    const sym = symbol.toUpperCase().trim();

    // 🌟 [핵심 수정] 지수(Index) 티커는 무조건 프리패스 (거름망 면제)
    if (sym.startsWith('^')) {
        return true;
    }

    // 1. 점(.) 처리 (미국 특정 클래스만 허용, 한국 삭제)
    if (sym.includes('.')) {
        const parts = sym.split('.');
        const suffix = parts[parts.length - 1];
        
        if (suffix === 'A' || suffix === 'B' || suffix === 'C') return true;
        
        console.log(`🚫 [거름망 작동] 특수 목적 주식(.) 차단: ${sym}`);
        return false;
    }

    // 2. 대시(-) 처리 (FMP의 특수목적 주식 필터링)
    if (sym.includes('-')) {
        const parts = sym.split('-');
        const suffix = parts[parts.length - 1];
        
        if (suffix === 'A' || suffix === 'B' || suffix === 'C') return true;

        console.log(`🚫 [거름망 작동] 특수 목적 주식(-) 차단: ${sym}`);
        return false;
    }

    // 3. 5자리 특수 티커 차단 (나스닥 5번째 알파벳 규칙 적용)
    if (sym.length === 5) {
        const lastChar = sym.charAt(4);
        // X(펀드), F/Y(해외장외), P(우선주), W(워런트), R(권리), U(유닛), Q(파산), V(When-Issued 임시주식) 차단
        if (['X', 'F', 'Y', 'P', 'W', 'R', 'U', 'Q', 'V'].includes(lastChar)) {
            console.log(`🚫 [거름망 작동] 5자리 특수주식(우선주/펀드/임시 등) 차단: ${sym}`);
            return false;
        }
    }

    // 4. 특수문자가 없고 위 조건에 걸리지 않으면 순수 보통주 합격
    return true;
}

// ===========================================================================
// 동기화 에러 관리
// ===========================================================================
router.get('/sync-errors/summary', verifyToken, async (req, res) => {
    const summary = await errorManager.getSummary();
    res.json({ success: true, summary });
});

// [기존에 추가했던 상세 목록 조회 API - 경로명 살짝 정리]
router.get('/sync-errors/list', verifyToken, async (req, res) => {
    try {
        const { type } = req.query;
        if(!type) return res.status(400).json({ error: "type 파라미터가 필요합니다." });

        const snapshot = await admin.firestore().collection('sync_errors').where('type', '==', type).get();
        const symbols = [];
        snapshot.forEach(doc => symbols.push(doc.data().symbol));

        res.json({ success: true, count: symbols.length, symbols });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});


// ===========================================================================
// [1] 마스터 데이터 관리
// ===========================================================================
// [1.1] 티커 마스터 동기화 (인덱스 국가 정밀 분류 및 GLOBAL 제외, 상태 반영)
// ===========================================================================
router.post('/sync-ticker-master', verifyBatchOrAdmin, async (req, res) => {
    const { mode = 'FULL', limit = 100, exchangeCode } = req.body; 
    const crypto = require('crypto');
    console.log(`🚀 [Ticker Sync] 모드: ${mode} / 자비스: 인덱스 정밀 분류 및 동기화 시작 (US ONLY)`);

    if (mode === 'SAMPLE') {
        console.log("📢 [알림] 현재 '빠른 모드(SAMPLE)'로 동작 중입니다.");
    }

    const shortenName = (name) => {
        if (!name) return "";
        return name.replace(/,?\s*Inc\.?$/i, "").replace(/,?\s*Corp\.?$/i, "").replace(/,?\s*Corporation$/i, "")
                   .replace(/,?\s*Ltd\.?$/i, "").replace(/,?\s*Limited$/i, "").replace(/,?\s*PLC$/i, "")
                   .replace(/,?\s*Co\.?$/i, "").replace(/,?\s*Company$/i, "").trim();
    };

    try {
        const db = admin.firestore();
        let batch = db.batch();
        let opCount = 0;

        const commitBatchIfNeeded = async () => {
            if (opCount >= 400) {
                await batch.commit();
                batch = db.batch();
                opCount = 0;
            }
        };

        let params = { limit: 60000 }; 
        if (mode === 'SAMPLE') params.limit = limit;
        if (exchangeCode) params.exchange = exchangeCode;
        else params.country = 'US'; // 🌟 US 전용

        const resFmp = await fmpClient.get('/company-screener', { params });
        const responseData = resFmp.data || [];
        if (responseData.length === 0) return res.json({ success: false, message: "FMP 데이터 없음" });

        console.log(">> [Deep-Fetch] 비교를 위해 기존 stocks 데이터 로드 중...");
        const stockSnapshot = await db.collection('stocks').get();
        const existingStockMap = new Map();
        
        stockSnapshot.forEach(doc => {
            const data = doc.data();
            const stockCountry = (data.country || '').toUpperCase();
            if (stockCountry && stockCountry !== 'US') { // 🌟 US 필터
                return; 
            }
            existingStockMap.set(doc.id, data);
        });

        const groupedData = {};     
        const activeStocksList = [];      
        const inactiveStocksToProcess = []; 

        responseData.forEach(item => {
            if (!item.symbol || (!item.companyName && !item.name)) return;
            if (item.isFund === true) return; 

            const sym = item.symbol.toUpperCase();

            if (!isValidTicker(sym)) {
                return; 
            }

            const apiCountry = (item.country || '').toUpperCase();

            if (apiCountry !== 'US') { // 🌟 US 필터
                return;
            }

            const existingData = existingStockMap.get(sym);
            const rawEx = (item.exchangeShortName || '').toUpperCase();
            let cleanExchange = rawEx;

            if (['NASDAQ', 'NMS', 'NGS'].includes(rawEx)) cleanExchange = 'NASDAQ';
            else if (rawEx === 'NYSE') cleanExchange = 'NYSE';
            else if (rawEx === 'AMEX') cleanExchange = 'AMEX';

            if (rawEx === 'INDEX' || sym.startsWith('^')) {
                cleanExchange = 'INDEX';
            }

            const isActivelyTrading = item.isActivelyTrading === true;
            
            const stockData = {
                id: sym,
                symbol: item.symbol,
                name_en: item.companyName || item.name,
                name_short: shortenName(item.companyName || item.name),
                name_ko: existingData?.name_ko || "", 
                ex: cleanExchange,
                etf: item.isEtf,
                sector: item.sector || "Unknown",
                industry: item.industry || "Unknown",
                country: apiCountry
            };

            if (isActivelyTrading) {
                const docId = `${apiCountry}_${cleanExchange}`;
                if (!groupedData[docId]) groupedData[docId] = [];
                groupedData[docId].push(stockData);
                activeStocksList.push(stockData);
            } else {
                inactiveStocksToProcess.push(sym);
            }
        });

        const targetDocIds = Object.keys(groupedData);
        let isAnyMetaChanged = false;
        const metaChanges = {}; 

        for (const docId of targetDocIds) {
            const newList = groupedData[docId].sort((a, b) => a.id.localeCompare(b.id));
            const currentListHash = crypto.createHash('md5')
                                          .update(newList.map(s => s.id).join(','))
                                          .digest('hex');

            const oldMetaDoc = await db.collection('meta_tickers').doc(docId).get();
            const oldMeta = oldMetaDoc.exists ? oldMetaDoc.data() : {};

            const isChanged = oldMeta.count !== newList.length || oldMeta.listHash !== currentListHash;

            if (isChanged || mode === 'FULL') {
                isAnyMetaChanged = true;
                metaChanges[docId] = { list: newList, hash: currentListHash }; 
            }
        }

        const stocksUpdateList = [];

        if (isAnyMetaChanged) {
            console.log(">> [Write] meta_tickers 변동 감지됨. 업데이트 시작...");
            const CHUNK_SIZE = 600;

            for (const docId of Object.keys(metaChanges)) {
                const { list: newList, hash: newListHash } = metaChanges[docId];
                const countryFromHeader = docId.split('_')[0]; 
                const mainDocRef = db.collection('meta_tickers').doc(docId);
                const totalChunks = Math.ceil(newList.length / CHUNK_SIZE);

                const oldChunks = await mainDocRef.collection('chunks').get();
                for (const chunkDoc of oldChunks.docs) {
                    batch.delete(chunkDoc.ref);
                    opCount++;
                    await commitBatchIfNeeded();
                }

                batch.set(mainDocRef, { 
                    count: newList.length, country: countryFromHeader,
                    updatedAt: new Date().toISOString(), isChunked: true, 
                    chunkCount: totalChunks, listHash: newListHash 
                }, { merge: true });
                opCount++;
                await commitBatchIfNeeded();

                for (let i = 0; i < totalChunks; i++) {
                    batch.set(mainDocRef.collection('chunks').doc(`batch_${i}`), { 
                        chunkIndex: i, 
                        list: newList.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE).map(s => ({
                            id: s.id, symbol: s.symbol, name: s.name_short, 
                            name_ko: s.name_ko, ex: s.ex, etf: s.etf, 
                            sector: s.sector, industry: s.industry, country: s.country
                        })) 
                    });
                    opCount++;
                    await commitBatchIfNeeded();
                }
            }
        } else {
            console.log("✅ [Sync Skip] 변경된 마스터 정보가 없습니다.");
        }

        const fmpResponseIds = new Set([
            ...activeStocksList.map(s => s.id),
            ...inactiveStocksToProcess
        ]);

        activeStocksList.forEach(item => {
            const existing = existingStockMap.get(item.id);
            const isChanged = !existing || 
                              existing.country !== item.country || 
                              existing.name_en !== item.name_en || 
                              existing.exchange !== item.ex ||
                              existing.active !== true;

            if (isChanged || mode === 'FULL') {
                stocksUpdateList.push({
                    symbol: item.symbol, name_en: item.name_en, name_short: item.name_short,
                    name_ko: existing?.name_ko || "", sector: item.sector, industry: item.industry,
                    exchange: item.ex, country: item.country, active: true, isEtf: item.etf
                });
            }
        });

        existingStockMap.forEach((data, sym) => {
            const isTargetExchange = !exchangeCode || data.exchange === exchangeCode;
            const isIndex = data.ex === 'INDEX' || sym.startsWith('^');

            if (isTargetExchange && !isIndex && !fmpResponseIds.has(sym)) {
                if (data.active !== false) {
                    stocksUpdateList.push({ symbol: sym, active: false });
                    console.log(`>> [Deactivate] ${sym} (FMP 마스터 리스트에서 사라짐)`);
                }
            }
        });

        inactiveStocksToProcess.forEach(sym => {
            const existing = existingStockMap.get(sym);
            if (existing && existing.active !== false) {
                stocksUpdateList.push({ symbol: sym, active: false });
            }
        });

        if (stocksUpdateList.length > 0) {
            console.log(`>> [Write] stocks 업데이트 실행: ${stocksUpdateList.length}건`);
            for (const stock of stocksUpdateList) {
                batch.set(db.collection('stocks').doc(stock.symbol), stock, { merge: true });
                opCount++;
                await commitBatchIfNeeded();
            }
        }

        const hasActualChanges = isAnyMetaChanged || stocksUpdateList.length > 0;
        
        if (hasActualChanges) {
            batch.set(db.collection('meta_stats').doc('meta_sync_status'), {
                ticker_master: {
                    lastUpdated: new Date().toISOString(), version: Date.now(),
                    totalStocksUpdated: stocksUpdateList.length
                }
            }, { merge: true });
            opCount++;
            await commitBatchIfNeeded();
        } else {
            console.log("✅ [Sync Status Skip] 변경사항 없음");
        }

        if (opCount > 0) {
            await batch.commit();
        }
        
        console.log(`✅ [Sync Complete] 전체 동기화 완료: ${stocksUpdateList.length}건 갱신`);
        res.json({ success: true, updated: hasActualChanges, stocksUpdated: stocksUpdateList.length });

    } catch (error) {
        console.error("Ticker Sync Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// ---------------------------------------------------------------------------
// [2] 주가 데이터 수집 - 개별 (Single Item)
// ---------------------------------------------------------------------------
router.post('/load-stock-data', verifyToken, async (req, res) => {
    try {
        const { symbol, from, to } = req.body;
        const result = await processStockHistoryData(symbol, from, to);
        res.json(result);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===========================================================================
// [2.1] 주가 전체 업데이트 (실행 시점 현재일 전용)
// ===========================================================================
router.post('/daily-update-all', verifyBatchOrAdmin, async (req, res) => {
    // 🌟 미국 고정
    const timeZone = 'America/New_York';
    const now = new Date();
    const timeStr = now.toLocaleString("en-US", {
        timeZone: timeZone, year: "numeric", month: "2-digit", day: "2-digit"
    });
    const [m, d, y] = timeStr.split('/');
    const targetDate = `${y}-${m}-${d}`;

    const dayOfWeek = new Date(parseInt(y), parseInt(m) - 1, parseInt(d)).getDay();
    const isWeekend = (dayOfWeek === 0 || dayOfWeek === 6);

    if (isWeekend) {
        console.log(`⏭️ [Daily Batch Skip] ${targetDate} (US)은 주말이므로 주가 수집을 스킵합니다.`);
        return res.json({ 
            success: true, 
            date: targetDate,
            message: `[US] 주말이므로 데이터 수집 배치를 건너뜁니다.` 
        });
    }

    res.json({ 
        success: true, 
        date: targetDate,
        message: `[US] 현재일 데이터 수집을 백그라운드에서 시작합니다.` 
    });
    
    setImmediate(async () => {
        try {
            console.log(`\n============== [Daily Batch Start] ==============`);
            
            const snapshot = await admin.firestore().collection('stocks').select('type', 'exchange', 'country').get();
            const validSymbolsSet = new Set();

            snapshot.forEach(doc => {
                const sym = doc.id;
                const data = doc.data() || {};
                
                const country = (data.country || '').toUpperCase();
                if (country !== 'US') return; // 🌟 US 필터

                validSymbolsSet.add(sym);
            });

            if (validSymbolsSet.size === 0) return console.error("❌ 대상 종목 없음");

            await processBulkDailyDataInternal(targetDate, validSymbolsSet);

            console.log(`============== [Daily Batch End: ${targetDate}] ============== \n`);
        } catch (error) {
            console.error("💥 [Batch Critical Error]", error);
        }
    });
});

// ===========================================================================
// [2.3] 일자별 주가 업데이터 (기간 지정) 백그라운드 배치
// ===========================================================================
router.post('/batch-load-stock-data-period', verifyBatchOrAdmin, async (req, res) => {
    const { country, status, startSymbol, endSymbol, startDate, endDate } = req.body;
    
    // 프론트엔드에는 멈춤 현상이 없도록 즉시 성공 응답을 내려줌
    res.json({ success: true, message: "백그라운드에서 범위 지정 주가 수집 배치를 시작합니다." });
    
    // Node.js 백그라운드에서 브라우저 종료와 무관하게 안전하게 루프 실행
    setImmediate(async () => {
        try {
            console.log(`\n============== [Period Batch Start] ==============`);
            const db = admin.firestore();
            let query = db.collection('stocks');
            
            if (status === 'active') query = query.where('active', '==', true);
            else if (status === 'delisted') query = query.where('isDelisted', '==', true);
            
            const snapshot = await query.select('country').get();
            let tickerList = [];
            
            snapshot.forEach(doc => {
                const sym = doc.id;
                const docCountry = (doc.data().country || '').toUpperCase();
                
                if (country && country !== 'all' && docCountry !== country) return;
                if (startSymbol && sym < startSymbol) return;
                if (endSymbol && sym > endSymbol) return;
                
                tickerList.push(sym);
            });
            
            tickerList.sort();
            console.log(`📋 [Period Batch] 대상 종목: ${tickerList.length}개`);
            
            let successCount = 0, failCount = 0;
            for (const symbol of tickerList) {
                try {
                    // 기존에 만들어둔 코어 함수 재활용
                    await processStockHistoryData(symbol, startDate, endDate);
                    successCount++;
                } catch (e) {
                    console.error(`❌ [${symbol}] 수집 실패:`, e.message);
                    failCount++;
                }
                // FMP API Rate Limit 보호를 위해 100ms 대기
                await new Promise(r => setTimeout(r, 100)); 
            }
            console.log(`✅ [Period Batch End] 성공: ${successCount}, 실패: ${failCount}`);
            console.log(`============== [Period Batch End] ============== \n`);
            
        } catch (error) {
            console.error("💥 [Period Batch Critical Error]", error);
        }
    });
});

// ===========================================================================
// [2-2] 배치 및 청소 도구
// ===========================================================================
router.post('/cleanup-garbage-stocks', verifyToken, async (req, res) => {
    try {
        const db = admin.firestore();
        console.log("🧹 [Cleanup] 최종 청소 시작 (US 외 모든 데이터 가차 없이 삭제)...");

        const collectionRef = db.collection('stocks');
        
        const snapshot = await collectionRef.select('country').get();
        console.log(`🔎 전체 스캔 완료: 총 ${snapshot.size}개 문서 검열 중...`);

        let deleteTargets = [];
        let detectedSuffixes = new Set(); 
        let countryDeletedCount = 0; 

        snapshot.forEach(doc => {
            const symbol = doc.id.toUpperCase().trim();
            const data = doc.data() || {};
            const country = (data.country || '').toUpperCase();

            let shouldDelete = false;

            // 🛑 1. 국가(Country) 필터링: US가 아니면 무조건 삭제
            if (country !== 'US') {
                shouldDelete = true;
                countryDeletedCount++;
            }
            // 🛑 2. Strict Whitelist Check
            else if (!isValidTicker(symbol)) {
                shouldDelete = true;
                if (symbol.includes('.')) detectedSuffixes.add('.' + symbol.split('.').pop());
            }

            if (shouldDelete) {
                deleteTargets.push(doc.ref);
            }
        });

        console.log(`📋 삭제 예정: ${deleteTargets.length}개 (국가 제외: ${countryDeletedCount}개 / 패턴: ${Array.from(detectedSuffixes).join(', ')})`);

        if (deleteTargets.length === 0) return res.json({ success: true, message: "삭제할 데이터가 없습니다." });

        let deleteCount = 0;
        const batchSize = 50; 
        for (let i = 0; i < deleteTargets.length; i += batchSize) {
            const chunk = deleteTargets.slice(i, i + batchSize);
            await Promise.all(chunk.map(async (ref) => {
                try {
                    await db.recursiveDelete(ref); 
                    deleteCount++;
                    process.stdout.write('.');
                } catch (e) { console.error(`❌ ${ref.id} 삭제 실패:`, e.message); }
            }));
            if (i > 0 && i % 500 === 0) await new Promise(r => setTimeout(r, 500));
        }

        console.log(`\n✅ [Cleanup] 완료. 총 ${deleteCount}개 삭제.`);
        res.json({ 
            success: true, 
            deletedCount: deleteCount, 
            countryFiltered: countryDeletedCount,
            foundSuffixes: Array.from(detectedSuffixes) 
        });

    } catch (error) {
        console.error("Cleanup Error:", error);
        res.status(500).json({ error: error.message });
    }
});

router.post('/cleanup-ghost-stocks', verifyToken, async (req, res) => {
    try {
        const db = admin.firestore();
        console.log("👻 [Ghostbuster] 유령 문서 삭제 시작...");
        const docRefs = await db.collection('stocks').listDocuments();
        
        let ghostTargets = [];
        const chunkSize = 500;
        for (let i = 0; i < docRefs.length; i += chunkSize) {
            const chunk = docRefs.slice(i, i + chunkSize);
            const snapshots = await db.getAll(...chunk);
            snapshots.forEach((snap, index) => {
                if (!snap.exists) ghostTargets.push(chunk[index]);
            });
            process.stdout.write('.');
        }

        console.log(`\n👻 발견된 유령: ${ghostTargets.length}개`);
        if (ghostTargets.length === 0) return res.json({ success: true, message: "유령 문서 없음" });

        let deleteCount = 0;
        const deleteBatchSize = 50;
        for (let i = 0; i < ghostTargets.length; i += deleteBatchSize) {
            const chunk = ghostTargets.slice(i, i + deleteBatchSize);
            await Promise.all(chunk.map(async (ref) => {
                try { await db.recursiveDelete(ref); deleteCount++; } 
                catch (e) {}
            }));
            if (i > 0 && i % 500 === 0) await new Promise(r => setTimeout(r, 200));
        }
        res.json({ success: true, ghostCount: deleteCount });
    } catch (error) { res.status(500).json({ error: error.message }); }
});

// [3] 지수 및 주요지수 구성종목 마스터 동기화 (최종: 공식 Stable API 적용)
// ⚡ [수정] 프론트 화면과 배치 모두에서 호출할 수 있게 verifyBatchOrAdmin 으로 변경
router.post('/sync-index-master', verifyBatchOrAdmin, async (req, res) => {
    try {
        console.log("🚀 [Index Sync] 지수 동기화 시작 (ETF Holdings 비중 데이터 기반)...");
        const db = admin.firestore();
        const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

        const saveChunks = async (docId, desc, list, countryCode = 'US', exchangeCode = 'INDEX') => {
            const CHUNK_SIZE = 600;
            const mainRef = db.collection('meta_tickers').doc(docId);
            const totalChunks = Math.ceil(list.length / CHUNK_SIZE);
            
            const oldChunks = await mainRef.collection('chunks').get();
            if(!oldChunks.empty) {
                const delBatch = db.batch();
                oldChunks.docs.forEach(d => delBatch.delete(d.ref));
                await delBatch.commit();
            }

            await mainRef.set({ 
                country: countryCode, 
                exchange: exchangeCode, description: desc, 
                count: list.length, isChunked: true, chunkCount: totalChunks, 
                updatedAt: new Date().toISOString() 
            }, { merge: true });

            let batch = db.batch();
            let op = 0;
            for(let i=0; i<totalChunks; i++) {
                batch.set(mainRef.collection('chunks').doc(`batch_${i}`), { 
                    chunkIndex: i, list: list.slice(i*CHUNK_SIZE, (i+1)*CHUNK_SIZE) 
                });
                op++;
                if(op>=400) { await batch.commit(); batch = db.batch(); op=0; }
            }
            if(op>0) await batch.commit();
            console.log(`✅ [저장 완료] ${docId} (${list.length}개)`);
        };

        const etfTargets = [
            { id: 'US_SP500', symbol: 'SPY', desc: 'S&P 500', country: 'US', step: '1/4' },
            { id: 'US_NASDAQ100', symbol: 'QQQ', desc: 'NASDAQ 100', country: 'US', step: '2/4' },
            { id: 'US_DOW30', symbol: 'DIA', desc: 'Dow Jones 30', country: 'US', step: '3/4' },
            { id: 'US_SP100', symbol: 'OEF', desc: 'S&P 100', country: 'US', step: '4/4' }
        ];

        for (const target of etfTargets) {
            let success = false;
            let retryCount = 0;
            const maxRetries = 1;

            while (!success && retryCount <= maxRetries) {
                try {
                    console.log(`📡 [${target.step}] ${target.desc} 데이터 요청 중... (ETF: ${target.symbol})`);
                    
                    const resEtf = await fmpClient.get('/etf/holdings', {
                        params: { symbol: target.symbol } 
                    });
                    
                    if(resEtf.data && Array.isArray(resEtf.data)) {
                        const list = resEtf.data.map(i => ({ 
                            s: i.asset || i.symbol, 
                            n: i.name || '', 
                            weight: i.weightPercentage || i.weight || 0 
                        }));

                        list.sort((a, b) => b.weight - a.weight);

                        await saveChunks(target.id, `${target.desc} (via ${target.symbol} Holdings)`, list, target.country, 'INDEX');
                        success = true;
                    }
                } catch(err) { 
                    console.error(`❌ ${target.desc} 실패:`, err.message); 
                    
                    if (err.response && err.response.status === 429 && retryCount < maxRetries) {
                        console.log(`⚠️ 429 Rate Limit 감지! 5초 휴식 후 재시도...`);
                        await delay(5000); 
                        retryCount++;
                    } else {
                        break; 
                    }
                }
            }
            if (success) await delay(2000); 
        }

        console.log("⏳ Global Indices API 요청 전 3초 대기 중...");
        await delay(3000); 

        try {
            console.log("📡 [Last Step] Global Indices 목록 요청 및 Stocks 전체 등록...");
            const allIdx = await fmpClient.get('/index-list');
            
            if(allIdx.data && Array.isArray(allIdx.data)) {
                // 1. USD 통화만 필터링 🌟
                const filteredIndices = allIdx.data.filter(item => item.currency === 'USD');

                console.log(`✅ FMP 수신 완료: 전체 ${allIdx.data.length}개 지수 중, USD 대상 ${filteredIndices.length}개 필터링 완료!`);

                const usIndices = [];

                filteredIndices.forEach(item => {
                    const fmpExchange = item.exchangeShortName || item.stockExchange || 'INDEX';
                    
                    const mappedItem = {
                        symbol: item.symbol,
                        name: item.name, 
                        name_en: item.name,
                        ex: fmpExchange,         
                        currency: item.currency, 
                        country: 'US'         
                    };

                    usIndices.push(mappedItem);
                });

                if (usIndices.length > 0) {
                    await saveChunks('US_INDEX', 'US Market Indices', usIndices, 'US', 'INDEX');
                }

                console.log("💾 Stocks 컬렉션에 필터링된 지수 데이터를 동기화합니다...");
                
                let batch = db.batch();
                let opCount = 0;
                let savedCount = 0;

                for (const item of usIndices) {
                    const stockRef = db.collection('stocks').doc(item.symbol);
                    
                    batch.set(stockRef, {
                        symbol: item.symbol,
                        name_en: item.name_en,
                        exchange: item.ex,        
                        isEtf: false,
                        active: true,
                        type: 'index',
                        currency: item.currency,  
                        country: item.country,    
                        updatedAt: new Date().toISOString()
                    }, { merge: true });

                    opCount++;
                    savedCount++;

                    if (opCount >= 400) {
                        await batch.commit();
                        batch = db.batch();
                        opCount = 0;
                        console.log(`... ${savedCount} / ${usIndices.length} 저장 중`);
                        await delay(500); 
                    }
                }

                if (opCount > 0) await batch.commit();
                console.log(`✅ Stocks 컬렉션 동기화 완료! (총 ${savedCount}개)`);
            }
        } catch(err) { 
            console.error("❌ Global Indices 처리 실패:", err.message); 
            throw err; 
        }

        res.json({ success: true, message: "지수 비중 동기화 완료! (USD 분류 저장)" });
    } catch (e) { 
        res.status(500).json({ error: e.message }); 
    }
});

// [4] 상장폐지 종목 동기화 (Strict Guard & Stocks 단일 통합 저장)
router.post('/sync-delisted-master', verifyToken, async (req, res) => {
    try {
        console.log("🚀 [Delisted] 상장폐지 종목 동기화 (Single Source of Truth)..."); 
        let allDelisted = [];
        let page = 0;
        let hasMoreData = true;
        const LIMIT = 1000; 

        while (hasMoreData) {
            try {
                const response = await fmpClient.get('/delisted-companies', { params: { page: page, limit: LIMIT } });
                const data = response.data;
                if (!data || data.length === 0) { hasMoreData = false; break; }
                allDelisted.push(...data);
                page++;
                if (page > 150) break; 
            } catch (err) { hasMoreData = false; }
        }

        if (allDelisted.length === 0) return res.json({ success: false, message: "데이터 없음" });

        const db = admin.firestore();
        console.log(">> 기존 stocks 데이터 확인 중...");
        const existingDocs = await db.collection('stocks').select('active', 'isDelisted', 'delistedDate').get();
        const existingMap = new Map();
        existingDocs.forEach(doc => existingMap.set(doc.id, doc.data()));

        const stocksBatchList = []; 

        allDelisted.forEach(item => {
            if (!item.symbol) return;
            const sym = item.symbol.toUpperCase();
            
            if (!isValidTicker(sym)) return;

            const existingData = existingMap.get(sym);

            if (existingData) {
                if (existingData.active === false && existingData.isDelisted === true && existingData.delistedDate === item.delistedDate) {
                    return; 
                }

                stocksBatchList.push({
                    symbol: sym,
                    active: false,
                    isDelisted: true,
                    delistedDate: item.delistedDate || '',
                    updatedAt: new Date().toISOString()
                });
            } else {
                let rawExchange = (item.exchange || 'Unknown').toUpperCase();
                let country = null;
                let currency = null;
                let cleanExchange = rawExchange;

                // 🌟 미국(US) 거래소만 파악
                if (['NASDAQ', 'NYSE', 'AMEX', 'NMS', 'NGS', 'PNK', 'OTC'].some(u => rawExchange.includes(u))) {
                    country = 'US'; currency = 'USD';
                    if (rawExchange.includes('NASDAQ') || rawExchange.includes('NMS') || rawExchange.includes('NGS')) cleanExchange = 'NASDAQ';
                    else if (rawExchange.includes('NYSE')) cleanExchange = 'NYSE';
                    else if (rawExchange.includes('AMEX')) cleanExchange = 'AMEX';
                    else if (rawExchange.includes('PNK') || rawExchange.includes('OTC')) cleanExchange = 'OTC'; 
                    else cleanExchange = '';
                }

                if (!country || !currency) return; 

                stocksBatchList.push({
                    symbol: sym,
                    name_en: item.companyName || '',
                    exchange: cleanExchange,
                    country: country,
                    currency: currency,
                    active: false,           
                    isDelisted: true,        
                    delistedDate: item.delistedDate || '',
                    ipoDate: item.ipoDate || '',
                    updatedAt: new Date().toISOString()
                });
            }
        });

        let batch = db.batch();
        let opCount = 0;

        for (const stock of stocksBatchList) {
            const stockRef = db.collection('stocks').doc(stock.symbol);
            batch.set(stockRef, stock, { merge: true });
            opCount++;
            
            if (opCount >= 400) { 
                await batch.commit(); 
                batch = db.batch(); 
                opCount = 0; 
            }
        }

        if (opCount > 0) await batch.commit();

        res.json({ success: true, count: allDelisted.length, stocksUpdated: stocksBatchList.length });
    } catch (error) {
        console.error("Delisted Sync Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// [5] 기업 이벤트 (배당/분할) - DB 존재 종목만 저장 및 Array 통합 모델 적용
router.post('/sync-action-master', verifyToken, async (req, res) => {
    const { mode = 'DAILY', symbol, startDate, endDate, startSymbol, endSymbol } = req.body; 
    console.log(`🚀 [Action Sync] 기업 이벤트 동기화 (Mode: ${mode})...`);

    try {
        const db = admin.firestore();

        if (mode === 'SINGLE') {
            if (!symbol) return res.status(400).json({ error: "Symbol required for SINGLE mode" });

            const stockSnap = await db.collection('stocks').doc(symbol).get();
            if (stockSnap.exists) {
                const stockData = stockSnap.data();
                const stockCountry = (stockData.country || '').toUpperCase();
                if (stockCountry && stockCountry !== 'US') { // 🌟 US 필터
                    console.log(`⏭️ [Skip] ${symbol}은 US 국가가 아니므로 이벤트 동기화를 건너뜁니다.`);
                    return res.json({ success: false, message: "US 국가가 아니므로 제외됨" });
                }
            }

            try {
                const [divRes, splitRes] = await Promise.all([
                    fmpClient.get('/dividends', { params: { symbol } }),
                    fmpClient.get('/splits', { params: { symbol } })
                ]);

                const divData = Array.isArray(divRes.data) ? divRes.data : [];
                const splitData = Array.isArray(splitRes.data) ? splitRes.data : [];
                
                const filterByDate = (dateStr) => {
                    if (startDate && dateStr < startDate) return false;
                    if (endDate && dateStr > endDate) return false;
                    return true;
                };

                const filteredDivs = divData.filter(item => item.date && filterByDate(item.date));
                const filteredSplits = splitData.filter(item => item.date && filterByDate(item.date));

                const batchHandler = db.batch();

                if (filteredDivs.length > 0) {
                    const divRef = db.collection('stocks').doc(symbol).collection('actions').doc('dividends');
                    const divSnap = await divRef.get();
                    const existingDivs = divSnap.exists ? (divSnap.data().history || []) : [];
                    const mergedDivs = mergeActionData(existingDivs, filteredDivs);
                    
                    batchHandler.set(divRef, {
                        symbol: symbol, lastUpdated: new Date().toISOString(), history: mergedDivs
                    }, { merge: true });
                }

                if (filteredSplits.length > 0) {
                    const splitRef = db.collection('stocks').doc(symbol).collection('actions').doc('splits');
                    const splitSnap = await splitRef.get();
                    const existingSplits = splitSnap.exists ? (splitSnap.data().history || []) : [];
                    const mergedSplits = mergeActionData(existingSplits, filteredSplits.map(item => ({
                        date: item.date, label: item.label || '', numerator: item.numerator || 0,
                        denominator: item.denominator || 0, ratio: `${item.numerator}:${item.denominator}`
                    })));

                    batchHandler.set(splitRef, {
                        symbol: symbol, lastUpdated: new Date().toISOString(), history: mergedSplits
                    }, { merge: true });
                }

                await batchHandler.commit();
                await errorManager.resolveError('actions', symbol);

                return res.json({ success: true, mode: 'SINGLE', stats: { dividends: filteredDivs.length, splits: filteredSplits.length } });

            } catch (err) {
                console.error(`❌ [SINGLE Action Error] ${symbol}: ${err.message}`);
                await errorManager.logError('actions', symbol, err.message);
                return res.status(500).json({ error: err.message, symbol: symbol });
            }
        }

        if (mode === 'DAILY') {
            console.log("   👉 유효 종목 리스트 로딩 중...");
            
            const snapshot = await db.collection('stocks').select('country').get();
            const validSymbols = new Set();
            snapshot.forEach(doc => {
                const data = doc.data() || {};
                const country = (data.country || '').toUpperCase();
                if (country && country !== 'US') return; // 🌟 US 필터
                validSymbols.add(doc.id);
            }); 
            console.log(`   ✅ 유효 종목 ${validSymbols.size}개 로드 완료.`);

            const today = new Date();
            const past = new Date(); past.setDate(today.getDate() - 14);
            const future = new Date(); future.setDate(today.getDate() + 14);
            const from = past.toISOString().split('T')[0];
            const to = future.toISOString().split('T')[0];

            console.log(`   - [데일리] ${from} ~ ${to} 범위 시장 전체 달력 조회 중...`);
            const [divRes, splitRes] = await Promise.all([
                fmpClient.get('/dividends-calendar', { params: { from, to } }),
                fmpClient.get('/splits-calendar', { params: { from, to } })
            ]);

            const isSymbolInRange = (sym) => {
                if (!validSymbols.has(sym)) return false; 
                if (startSymbol && sym < startSymbol) return false;
                if (endSymbol && sym > endSymbol) return false;
                return true;
            };

            const divUpdates = {};
            const splitUpdates = {};

            divRes.data?.forEach(item => {
                if(item.symbol && isSymbolInRange(item.symbol)) {
                    if(!divUpdates[item.symbol]) divUpdates[item.symbol] = [];
                    divUpdates[item.symbol].push(item);
                }
            });

            splitRes.data?.forEach(item => {
                if(item.symbol && isSymbolInRange(item.symbol)) {
                    if(!splitUpdates[item.symbol]) splitUpdates[item.symbol] = [];
                    splitUpdates[item.symbol].push(item);
                }
            });

            let batchHandler = db.batch();
            let opCount = 0;
            let updateCount = 0;

            const commitBatchIfNeeded = async () => {
                if (opCount >= 400) {
                    await batchHandler.commit();
                    batchHandler = db.batch();
                    opCount = 0;
                }
            };

            for (const [sym, newDivs] of Object.entries(divUpdates)) {
                const divRef = db.collection('stocks').doc(sym).collection('actions').doc('dividends');
                const divSnap = await divRef.get();
                const existingDivs = divSnap.exists ? (divSnap.data().history || []) : [];
                
                const mergedDivs = mergeActionData(existingDivs, newDivs);
                
                batchHandler.set(divRef, {
                    symbol: sym, lastUpdated: new Date().toISOString(), history: mergedDivs
                }, { merge: true });
                
                updateCount += newDivs.length;
                opCount++;
                await commitBatchIfNeeded();
            }

            for (const [sym, newSplits] of Object.entries(splitUpdates)) {
                const splitRef = db.collection('stocks').doc(sym).collection('actions').doc('splits');
                const splitSnap = await splitRef.get();
                const existingSplits = splitSnap.exists ? (splitSnap.data().history || []) : [];

                const mappedSplits = newSplits.map(item => ({
                    date: item.date, label: item.label || '', numerator: item.numerator || 0,
                    denominator: item.denominator || 0, ratio: `${item.numerator}:${item.denominator}`
                }));

                const mergedSplits = mergeActionData(existingSplits, mappedSplits);

                batchHandler.set(splitRef, {
                    symbol: sym, lastUpdated: new Date().toISOString(), history: mergedSplits
                }, { merge: true });
                
                updateCount += newSplits.length;
                opCount++;
                await commitBatchIfNeeded();
            }

            if (opCount > 0) {
                await batchHandler.commit();
            }
            
            return res.json({ success: true, mode: 'DAILY', stats: { validUpdates: updateCount } });
        }

    } catch (error) {
        console.error("Sync Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// ---------------------------------------------------------------------------
// [6] 재무제표 필수 필드 및 매핑 정의 (JSON Key 기준)
// ---------------------------------------------------------------------------
const ESSENTIAL_FIELDS = {
    'income-statement': [
        'date', 'calendarYear', 'period',
        'revenue', 'costOfRevenue', 'grossProfit', 
        'operatingIncome', 'netIncome', 'eps', 'ebitda'
    ],
    'balance-sheet-statement': [
        'date', 'calendarYear', 'period',
        'totalAssets', 'totalLiabilities', 'totalStockholdersEquity',
        'cashAndCashEquivalents', 'shortTermDebt', 'longTermDebt', 'netDebt'
    ],
    'cash-flow-statement': [
        'date', 'calendarYear', 'period',
        'operatingCashFlow', 'capitalExpenditure', 'freeCashFlow',
        'dividendsPaid'
    ],
    'financial-ratios': [
        'date', 'calendarYear', 'period',
        'debtToEquityRatio',        // 부채비율
        'priceToEarningsRatio',     // PER
        'priceToBookRatio',         // PBR
        'priceToSalesRatio',        // PSR
        'dividendYield',            // 배당수익율 
        'returnOnEquity',           // ROE
        'returnOnAssets'            // ROA
    ],
    'key-metrics': [
        'date', 'calendarYear', 'period',
        'marketCap',                // 시가총액
        'enterpriseValue',          // EV
        'evToEBITDA',               // EV/EBITDA (대문자 주의)
        'evToSales',                // EV/Sales
        'evToOperatingCashFlow',    // EV/OCF
        'evToFreeCashFlow',         // EV/FCF
        'earningsYield',            // 이익수익률
        'freeCashFlowYield',        // FCF수익률
        'returnOnInvestedCapital',  // ROIC
        'grahamNumber',             // 그레이엄 수
        'investedCapital',          // 투하자본
        'netDebtToEBITDA'           // 순부채/EBITDA
    ]
};

// [6-1] 재무제표 수집 및 저장 코어 로직 (개별/배치/최신 공용)
async function saveFinancialsInternal(db, symbol) {
    const stockRef = db.collection('stocks').doc(symbol);

    const docSnap = await stockRef.get();
    if (!docSnap.exists) return false;

    const stockData = docSnap.data();

    const stockCountry = (stockData.country || '').toUpperCase();
    if (stockCountry && stockCountry !== 'US') { // 🌟 US 필터
        console.log(`⏩ [Skip] ${symbol} is not US (${stockCountry})`);
        return 'SKIPPED';
    }

    if (stockData.isEtf === true) {
        console.log(`⏩ [Skip] ${symbol} is ETF (No Financials)`);
        return 'SKIPPED'; 
    }

    const batch = db.batch();
    let savedTypes = [];

    const stmts = [
        { type: 'income-statement', url: '/income-statement' },
        { type: 'financial-ratios', url: '/ratios' },
    ];

    for (const stmt of stmts) {
        try {
            const res = await fmpClient.get(stmt.url, { 
                params: { symbol: symbol, limit: 30, period: 'annual' } 
            });

            if (res.data && Array.isArray(res.data) && res.data.length > 0) {
                const targetFields = ESSENTIAL_FIELDS[stmt.type];
                
                const filteredData = res.data.map(item => {
                    const cleanItem = {};
                    targetFields.forEach(field => {
                        if (item[field] !== undefined && item[field] !== null) {
                            cleanItem[field] = item[field];
                        }
                    });
                    return cleanItem;
                });

                filteredData.sort((a, b) => new Date(b.date) - new Date(a.date));

                batch.set(stockRef.collection('financials').doc(stmt.type), {
                    type: stmt.type, symbol: symbol, updatedAt: new Date().toISOString(), history: filteredData
                });
                savedTypes.push(stmt.type);
            } 
        } catch (e) { 
        }
    }

    if (savedTypes.length > 0) {
        batch.set(stockRef, { 
            last_financial_update: new Date().toISOString(), has_financials: true 
        }, { merge: true });
        
        await batch.commit();
        console.log(`✅ [Fin-Batch] ${symbol} 저장 완료 (${savedTypes.length}종)`);

        return true;
    }
    return false;
}

// [6-2] 재무제표 수집 (개별 & 최신)
router.post('/load-financials', verifyToken, async (req, res) => {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Symbol required' });

    try {
        const db = admin.firestore();
        const success = await saveFinancialsInternal(db, symbol);

        if (success) res.json({ success: true, symbol, message: "저장 완료" });
        else res.json({ success: false, symbol, message: "데이터 없음 (FMP)" });
    } catch (error) { 
        res.status(500).json({ error: error.message }); 
    }
});

// [6-3] 최신 재무제표 업데이트 (Daily Batch)
// ⚡ FMP Latest Financial Statements API 활용
router.post('/sync-latest-financials', verifyToken, async (req, res) => {
    try {
        console.log("🚀 [Latest Financials] 최신 업데이트 확인 중...");
        const db = admin.firestore();

        // 1. 최신 업데이트 목록 가져오기 (Limit 250)
        // fmpClient가 BaseURL(/stable)과 Key를 처리하므로 경로만 입력
        const latestRes = await fmpClient.get('/latest-financial-statements', {
            params: { limit: 250 } 
        });

        const latestList = latestRes.data || [];
        if (latestList.length === 0) return res.json({ success: true, message: "업데이트 내역 없음" });

        // 2. 중복 종목 제거 (동일 종목이 여러 보고서를 냈을 수 있음)
        const uniqueSymbols = [...new Set(latestList.map(item => item.symbol))];
        console.log(`📋 업데이트 대상: ${uniqueSymbols.length}개 종목`);

        // 3. 순차 업데이트 실행
        let successCount = 0;
        
        // (응답 타임아웃 방지를 위해 비동기로 돌리거나, 여기서 일부만 기다릴 수 있음. 여기선 순차 처리)
        for (const symbol of uniqueSymbols) {
            try {
                // DB에 있는 종목인지 체크 (옵션: 관리 종목만 업데이트)
                const docRef = db.collection('stocks').doc(symbol);
                const docSnap = await docRef.get();
                
                if (docSnap.exists) {
                    console.log(`  🔄 [Update] ${symbol}...`);
                    const result = await saveFinancialsInternal(db, symbol);
                    if (result) successCount++;
                    // FMP Rate Limit 고려
                    await new Promise(r => setTimeout(r, 100));
                }
            } catch (e) {
                console.error(`  ❌ [Fail] ${symbol}: ${e.message}`);
            }
        }

        res.json({ success: true, count: successCount, targets: uniqueSymbols });

    } catch (error) {
        console.error("Latest Sync Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// [6-4] 전체 재무제표 일괄 수집 (Batch Job)
router.post('/batch-financials', verifyToken, async (req, res) => {
    res.json({ success: true, status: 'STARTED', message: "전체 재무제표 수집 시작 (백그라운드)" });

    (async () => {
        try {
            console.log("🚀 [Batch Financials] 전체 수집 시작...");
            const db = admin.firestore();
            
            const snapshot = await db.collection('stocks').where('active', '==', true).select('country').get();
            const symbols = [];
            
            snapshot.docs.forEach(doc => {
                const data = doc.data() || {};
                const country = (data.country || '').toUpperCase();
                if (country && country !== 'US') return; // 🌟 US 필터
                symbols.push(doc.id);
            });
            
            console.log(`📋 대상 종목: ${symbols.length}개`);

            const CONCURRENCY = 3; 
            for (let i = 0; i < symbols.length; i += CONCURRENCY) {
                const chunk = symbols.slice(i, i + CONCURRENCY);
                await Promise.all(chunk.map(async (symbol) => {
                    try {
                        await saveFinancialsInternal(db, symbol);
                    } catch (e) { console.error(`❌ ${symbol} Fail`); }
                }));
                await new Promise(r => setTimeout(r, 200));
            }
            console.log("✅ [Batch Financials] 전체 수집 완료");
        } catch (e) {
            console.error("Batch Error:", e);
        }
    })();
});

// ===========================================================================
// [7] [설정] 섹터 고정 번역 맵 (100% 한글 저장을 위한 사전)
// ===========================================================================
const FIXED_SECTOR_MAP = {
    "Basic Materials": "기초 소재",
    "Communication Services": "커뮤니케이션 서비스",
    "Consumer Cyclical": "임의소비재",
    "Consumer Defensive": "필수소비재",
    "Energy": "에너지",
    "Financial Services": "금융",
    "Healthcare": "헬스케어",
    "Industrials": "산업재",
    "Real Estate": "부동산",
    "Technology": "기술",
    "Utilities": "유틸리티",
    "Financial": "금융",       // FMP 변형 대응
    "Services": "서비스",      // FMP 변형 대응
    "Conglomerates": "복합기업", // FMP 변형 대응
    "General": "기타"
};

// [7-1] [내부 유틸] AI 응답 파싱 (jarvis.js와 동일)
function cleanAndParseJSON(text) {
    try {
        if (!text) return {};
        // 1. 마크다운 및 불필요한 공백 제거
        let cleanText = text.replace(/```json/gi, '').replace(/```/g, '').trim();
        
        // 2. JSON 객체 범위 추출 ({...})
        const start = cleanText.indexOf('{');
        const end = cleanText.lastIndexOf('}');
        
        if (start !== -1 && end !== -1) {
            cleanText = cleanText.substring(start, end + 1);
        }
        return JSON.parse(cleanText);
    } catch (e) {
        console.error("❌ JSON Parsing Failed:", e.message);
        return {}; // 실패 시 빈 객체 반환 (영문 유지용)
    }
}

// [7-2] 섹터/산업 동기화 (오류 수정: 데이터 파싱 강화)
router.post('/sync-sector-master', verifyToken, async (req, res) => {
    try {
        const result = await syncSectorMasterInternal();
        res.json(result);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// [7-2] 섹터/산업 동기화 (오류 수정: 데이터 파싱 강화)
router.post('/sync-sector-master', verifyToken, async (req, res) => {
    try {
        const result = await syncSectorMasterInternal();
        res.json(result);
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===========================================================================
// 섹터 및 산업 정보 동기화.   대표 ETF 도 하드코딩으로 지정
// ===========================================================================
async function syncSectorMasterInternal() {
    const db = admin.firestore();
    console.log("🚀 [Master Sync] 섹터/산업 데이터 동기화 시작 (ETF 세밀 매핑 포함)...");

    // 🌟 1. 주요 11개 섹터 대표 ETF 매핑 사전
    const SECTOR_ETF_MAP = {
        "Technology": "XLK",
        "Communication Services": "XLC",
        "Consumer Cyclical": "XLY",
        "Consumer Defensive": "XLP",
        "Energy": "XLE",
        "Financial Services": "XLF",
        "Healthcare": "XLV",
        "Industrials": "XLI",
        "Basic Materials": "XLB",
        "Real Estate": "XLRE",
        "Utilities": "XLU",
        "Financial": "XLF",       
        "Services": "XLC",      
        "Conglomerates": null, 
        "General": null
    };

    // 🌟 2. 120개 중 핵심 주도 산업 대표 ETF 엄선 매핑 사전 (AUM 및 거래량 기준)
    const INDUSTRY_ETF_MAP = {
        "Semiconductors": "SOXX",
        "Software - Infrastructure": "IGV",
        "Software - Application": "IGV",
        "Computer Hardware": "QQQ", // 하드웨어는 QQQ가 압도적 대용
        "Consumer Electronics": "VGT",
        "Biotechnology": "XBI",
        "Drug Manufacturers - General": "PJP",
        "Medical Devices": "IHI",
        "Healthcare Plans": "IHF",
        "Aerospace & Defense": "ITA",
        "Airlines": "JETS",
        "Auto Manufacturers": "CARZ",
        "Banks - Regional": "KRE",
        "Banks - Diversified": "KBE",
        "Asset Management": "KCE",
        "Capital Markets": "KCE",
        "Insurance - Property & Casualty": "KIE",
        "Insurance - Life": "KIE",
        "Oil & Gas E&P": "XOP",
        "Oil & Gas Midstream": "AMLP",
        "Oil & Gas Refining & Marketing": "CRAK",
        "Oil & Gas Equipment & Services": "XES",
        "Gold": "GDX", // 금광주
        "Other Precious Metals & Mining": "GDXJ",
        "Copper": "COPX",
        "Steel": "SLX",
        "Internet Retail": "XRT",
        "Apparel Retail": "XRT",
        "Home Improvement Retail": "XHB",
        "Residential Construction": "XHB",
        "Building Products & Equipment": "XHB",
        "Restaurants": "PBJ",
        "Packaged Foods": "PBJ",
        "Beverages - Non-Alcoholic": "PBJ",
        "Household & Personal Products": "XLP",
        "REITs - Retail": "VNQ",
        "REITs - Residential": "REZ",
        "REITs - Office": "VNQ",
        "REITs - Healthcare": "VNQ",
        "Solar": "TAN",
        "Utilities - Regulated Electric": "XLU",
        "Telecom Services": "XLC",
        "Entertainment": "PEJ",
        "Broadcasting": "PBS",
        "Travel Services": "PEJ",
        "Lodging": "PEJ",
        "Trucking": "IYT",
        "Railroads": "IYT",
        "Integrated Freight & Logistics": "IYT"
    };

    try {
        // 1. 기존 데이터 로드 (비교용)
        const oldDocRef = db.collection('meta_sectors').doc('GICS_Standard');
        const oldDoc = await oldDocRef.get();
        const oldData = oldDoc.exists ? oldDoc.data() : null;

        // 2. FMP 데이터 로드
        const [secRes, indRes] = await Promise.all([
            fmpClient.get('/available-sectors'),
            fmpClient.get('/available-industries')
        ]);

        // 3. FMP 데이터 정제 (무조건 1차원 문자열 배열로 만들어서 가나다순 정렬)
        const parseFmp = (data, key) => {
            if (!Array.isArray(data)) return [];
            return data.map(item => {
                if (typeof item === 'string') return item.trim();
                if (item && item[key]) return String(item[key]).trim();
                return String(item).trim();
            }).filter(Boolean);
        };

        const fmpSectors = parseFmp(secRes.data, 'sector').sort();
        const fmpIndustries = parseFmp(indRes.data, 'industry').sort();

        // 4. 기존 DB 데이터에서 번역 정보 및 이름 추출
        const translationMap = new Map();
        let oldSectorNames = [];
        let oldIndustryNames = [];

        if (oldData) {
            if (Array.isArray(oldData.sectorList)) {
                oldData.sectorList.forEach(s => {
                    if (s.name_en) {
                        translationMap.set(s.name_en, s.name_ko);
                        oldSectorNames.push(s.name_en);
                    }
                });
            }
            if (Array.isArray(oldData.industryList)) {
                oldData.industryList.forEach(i => {
                        if (i.name_en) {
                        translationMap.set(i.name_en, i.name_ko);
                        oldIndustryNames.push(i.name_en);
                    }
                });
            }
        }
        oldSectorNames.sort();
        oldIndustryNames.sort();

        console.log(`📊 데이터 비교 -> [기존] 섹터: ${oldSectorNames.length}개 / 산업: ${oldIndustryNames.length}개`);
        console.log(`📊 데이터 비교 -> [FMP] 섹터: ${fmpSectors.length}개 / 산업: ${fmpIndustries.length}개`);

        // 5. ⚡ '진짜 신규' 산업 찾기 (기존 translationMap에 없는 영문명만 추출)
        const newIndustries = fmpIndustries.filter(ind => !translationMap.has(ind));

        // ---------------------------------------------------------
        // 6. 계층 구조 (Hierarchy) 생성 - meta_tickers (getTickerData) 기준
        // ---------------------------------------------------------
        console.log("🔍 meta_tickers 기반으로 계층 구조 스캔 중...");
        
        // 스탁헬퍼의 getTickerData 함수를 호출하여 전체 종목 정보 로드
        const allTickers = await getTickerData(); 
        const treeMap = {};
        
        allTickers.forEach(d => {
            // chunk 내부에 sector, industry 정보가 있어야 함
            if (!d.sector || !d.industry) return; 
            
            if (!treeMap[d.sector]) treeMap[d.sector] = new Set();
            treeMap[d.sector].add(d.industry);
        });

        const sortedHierarchy = {};
        Object.keys(treeMap).sort().forEach(sec => {
            sortedHierarchy[sec] = Array.from(treeMap[sec]).sort();
        });
        // ---------------------------------------------------------

        // 7. 🛑 변경점 최종 확인 (Deep Compare)
        const isSectorListSame = JSON.stringify(oldSectorNames) === JSON.stringify(fmpSectors);
        const isIndustryListSame = JSON.stringify(oldIndustryNames) === JSON.stringify(fmpIndustries);

        // 계층 구조 정밀 비교 (키 순서 무관하게 비교하기 위해 정렬된 새로운 객체 생성)
        const sortObjectKeys = (obj) => {
            if (!obj) return "{}";
            return JSON.stringify(Object.keys(obj).sort().reduce((acc, key) => {
                acc[key] = Array.isArray(obj[key]) ? obj[key].sort() : obj[key];
                return acc;
            }, {}));
        };

        const currentHierarchyStr = sortObjectKeys(sortedHierarchy);
        const oldHierarchyStr = oldData ? sortObjectKeys(oldData.hierarchy) : "{}";
        const isHierarchySame = currentHierarchyStr === oldHierarchyStr;

        // 🌟 기존 DB에 etf_ticker 필드가 없으면 최초 1회 업데이트 트리거
        const needsSectorEtfUpdate = oldData && oldData.sectorList && oldData.sectorList.length > 0 && oldData.sectorList[0].etf_ticker === undefined;
        const needsIndustryEtfUpdate = oldData && oldData.industryList && oldData.industryList.length > 0 && oldData.industryList[0].etf_ticker === undefined;

        // [중요] 변경사항 여부 판단
        const hasActualChanges = !isSectorListSame || !isIndustryListSame || !isHierarchySame || newIndustries.length > 0 || needsSectorEtfUpdate || needsIndustryEtfUpdate;

        // 모든 것이 똑같고, 번역할 신규 산업도 없다면 즉시 종료!
        if (!hasActualChanges) {
            console.log("✅ [Master Sync] 모든 데이터 및 구조가 기존과 완전히 동일합니다. (DB 업데이트 및 번역 생략)");
            return { success: true, updated: false, message: "변경사항 없음" };
        }

        console.log(`⚡ 변경 감지됨! [섹터: ${!isSectorListSame}, 산업: ${!isIndustryListSame}, 계층: ${!isHierarchySame}, 신규: ${newIndustries.length}개, ETF매핑적용: ${!!(needsSectorEtfUpdate || needsIndustryEtfUpdate)}]`);
        
        // 8. 🤖 신규 산업만 AI 번역 진행
        if (newIndustries.length > 0) {
            console.log(`🤖 [Jarvis] 신규 산업 ${newIndustries.length}개 번역 시작...`, newIndustries);
            const CHUNK_SIZE = 50;
            
            for (let i = 0; i < newIndustries.length; i += CHUNK_SIZE) {
                const chunk = newIndustries.slice(i, i + CHUNK_SIZE);
                const indPrompt = `Translate these financial industries to Korean. Return ONLY a JSON object: {"English Name": "Korean Name"}. List: ${JSON.stringify(chunk)}`;

                try {
                    if (i > 0) await new Promise(r => setTimeout(r, 1500));
                    const rawResult = await askJarvis(indPrompt);
                    const chunkResult = cleanAndParseJSON(rawResult);

                    for (const [en, ko] of Object.entries(chunkResult)) {
                        translationMap.set(en, ko);
                    }
                } catch (e) {
                    console.error("⚠️ AI 번역 실패 (영문 그대로 유지):", e.message);
                    chunk.forEach(en => translationMap.set(en, en)); 
                }
            }
        }

        // 9. 최종 데이터 조립 (ETF 티커 매핑 포함)
        const structuredSectors = fmpSectors.map(en => ({
            key: en,
            name_en: en,
            name_ko: FIXED_SECTOR_MAP[en] || translationMap.get(en) || en,
            etf_ticker: SECTOR_ETF_MAP[en] || null 
        }));

        const structuredIndustries = fmpIndustries.map(en => ({
            key: en,
            name_en: en,
            name_ko: translationMap.get(en) || en,
            etf_ticker: INDUSTRY_ETF_MAP[en] || null // 🌟 산업별 ETF 매핑 적용
        }));

        // 10. Firestore 일괄 저장 (Batch) - 변경사항이 있을 때만 실행됨
        const batch = db.batch();
        const now = new Date().toISOString();
        const newVersion = Date.now();

        // (1) 메타 섹터 저장
        batch.set(oldDocRef, {
            sectorList: structuredSectors,
            industryList: structuredIndustries,
            hierarchy: sortedHierarchy,
            updatedAt: now
        });

        // (2) 동기화 상태 문서 업데이트 (meta_stats)
        batch.set(db.collection('meta_stats').doc('meta_sync_status'), {
            sector_master: {
                lastUpdated: now,
                version: newVersion,
                sectorCount: structuredSectors.length,
                industryCount: structuredIndustries.length
            }
        }, { merge: true });

        await batch.commit();
        console.log(`✅ [Master Sync] 업데이트 완료 및 meta_sync_status 갱신 성공! (버전: ${newVersion})`);
        
        return { success: true, updated: true, lastUpdated: now };

    } catch (error) {
        console.error("❌ Master Sync Error:", error);
        throw error;
    }
}

// ===========================================================================
// [Helper] 기존 데이터와 새로운 데이터를 날짜 기준으로 병합하는 함수 (필수 필드 유지)
// ===========================================================================
function mergeStockData(existingData, newData) {
    const dataMap = new Map();

    // 1. 기존 데이터 매핑 (기존 데이터 보존)
    if (Array.isArray(existingData)) {
        existingData.forEach(item => {
            if (item.date) dataMap.set(item.date, item);
        });
    }

    // 2. 새로운 데이터로 덮어쓰기 (업데이트)
    // 필요한 필드만 엄격하게 추출하여 저장
    if (Array.isArray(newData)) {
        newData.forEach(item => {
            if (item.date) {
                dataMap.set(item.date, {
                    date: item.date,
                    open: Number(item.open || 0),
                    high: Number(item.high || 0),
                    low: Number(item.low || 0),
                    close: Number(item.close || 0),
                    volume: Number(item.volume || 0),
                    mktCap: Number(item.mktCap || 0) // 시가총액 필드 필수 포함
                });
            }
        });
    }

    // 3. 날짜 오름차순 정렬 후 반환
    return Array.from(dataMap.values()).sort((a, b) => new Date(a.date) - new Date(b.date));
}

// ===========================================================================
// [Helper] 배당/분할 데이터 병합 (날짜 기준 중복 제거 및 최신순 정렬)
// ===========================================================================
function mergeActionData(existingData, newData) {
    const dataMap = new Map();

    if (Array.isArray(existingData)) {
        existingData.forEach(item => { if (item.date) dataMap.set(item.date, item); });
    }

    if (Array.isArray(newData)) {
        newData.forEach(item => { if (item.date) dataMap.set(item.date, item); });
    }

    // 날짜 내림차순 정렬 (최신 데이터가 배열의 첫 번째로 오도록)
    return Array.from(dataMap.values()).sort((a, b) => new Date(b.date) - new Date(a.date));
}

// ===========================================================================
// 특정기간 일자별 주가데이터 Bulk Daily Data 처리 (Merge 로직 적용)
// ===========================================================================
async function processBulkDailyDataInternal(targetDate, validSymbolsSet) {
    const db = admin.firestore();
    console.log(`🚀 [Bulk Batch Stable] ${targetDate} 수집 시작 (Merge Mode)...`);

    try {
        // [1] 주가 데이터(CSV) 가져오기
        const priceRes = await fmpClient.get('/eod-bulk', { 
            params: { date: targetDate }, 
            responseType: 'text' 
        });
        
        const csvData = priceRes.data;
        if (!csvData || typeof csvData !== 'string') {
            console.error("❌ 주가 CSV 데이터가 비어있습니다.");
            return;
        }

        const rows = csvData.split('\n');
        // 🌟 [추가] 헤더(1줄)만 있거나 빈 파일인 경우 (공휴일 등 휴장일 처리)
        // rows.length가 1이거나, 2줄이더라도 두 번째 줄이 빈 공백일 때
        if (rows.length <= 1 || (rows.length === 2 && rows[1].trim() === '')) {
            console.log(`⏭️ [Skip] ${targetDate} 주가 데이터가 없습니다. (휴장일 가능성 높음)`);
            return;
        }

        const priceMap = new Map();
        const symbolsForMktCap = [];

        for (let i = 1; i < rows.length; i++) {
            const rowStr = rows[i].trim();
            if (!rowStr) continue;
            const cols = rowStr.split(',');
            let symbol = cols[0].replace(/"/g, '').trim();

            if (symbol && validSymbolsSet.has(symbol)) {
                priceMap.set(symbol, {
                    date: cols[1].replace(/"/g, '').trim(),
                    open: parseFloat(cols[2] || 0),
                    high: parseFloat(cols[3] || 0),
                    low: parseFloat(cols[4] || 0),
                    close: parseFloat(cols[5] || 0),
                    volume: parseFloat(cols[7] || 0)
                });
                symbolsForMktCap.push(symbol);
            }
        }

        // [2] Market Cap Batch 호출
        const mktCapMap = new Map();
        const CHUNK_SIZE = 100; 
        for (let i = 0; i < symbolsForMktCap.length; i += CHUNK_SIZE) {
            const chunk = symbolsForMktCap.slice(i, i + CHUNK_SIZE);
            try {
                const mktCapRes = await fmpClient.get('/market-capitalization-batch', {
                    params: { symbols: chunk.join(',') }
                });
                
                if (Array.isArray(mktCapRes.data)) {
                    mktCapRes.data.forEach(item => {
                        mktCapMap.set(item.symbol, item.marketCap);
                    });
                }
            } catch (e) {
                console.warn(`⚠️ [MCap Batch Error] Chunk ${i} 실패: ${e.message}`);
            }
        }

        // [3] DB 업데이트 (Read -> Merge -> Write)
        let batchHandler = db.batch();
        let opCount = 0;
        const year = targetDate.split('-')[0];

        // 주의: 모든 문서를 읽어야 하므로 속도가 조금 느려질 수 있지만 데이터 안전성이 우선임
        for (const [symbol, priceInfo] of priceMap) {
            const marketCap = mktCapMap.get(symbol) || 0;
            
            const newRecord = {
                date: priceInfo.date,
                open: priceInfo.open,
                high: priceInfo.high,
                low: priceInfo.low,
                close: priceInfo.close,
                volume: priceInfo.volume,
                mktCap: marketCap
            };

            const docRef = db.collection('stocks').doc(symbol).collection('annual_data').doc(year);
            const snapshotRef = db.collection('stocks').doc(symbol);

            // 1) 기존 데이터 읽어오기 (비동기 처리 주의 - for loop 내 await 허용)
            // 배치를 사용하지만, 병합을 위해 현재 데이터를 알아야 함
            const docSnap = await docRef.get();
            let existingData = [];
            if (docSnap.exists) {
                existingData = docSnap.data().data || [];
            }

            // 2) 데이터 병합 (기존 + 신규)
            const mergedData = mergeStockData(existingData, [newRecord]);

            // 3) 연도별 데이터 업데이트 (덮어쓰기가 아닌 병합된 배열 저장)
            batchHandler.set(docRef, { 
                symbol: symbol,
                year: year,
                lastUpdated: new Date().toISOString(),
                data: mergedData 
            }, { merge: true });

            // 4) 최신 스냅샷 업데이트
            batchHandler.set(snapshotRef, { 
                snapshot: { 
                    price: priceInfo.close, 
                    vol: priceInfo.volume, 
                    mktCap: marketCap, 
                    lastUpdated: new Date().toISOString() 
                }, 
                last_crawled: new Date().toISOString() 
            }, { merge: true });

            opCount += 2; 
            // 배치 제한(500) 고려하여 커밋
            if (opCount >= 400) { 
                await batchHandler.commit(); 
                batchHandler = db.batch(); 
                opCount = 0; 
                // 너무 빠른 읽기/쓰기 반복으로 인한 부하 조절을 위해 약간의 지연이 필요할 수도 있음
            }
        }
        
        if (opCount > 0) await batchHandler.commit();
        console.log(`✅ [Bulk Complete] ${targetDate} 총 ${priceMap.size}건 동기화(병합) 성공!`);

    } catch (error) {
        console.error("💥 Stable Bulk Sync Error:", error);
    }
}

// ===========================================================================
// 종목별 과거 전체 주가 및 시가총액 데이터 수집 (Merge 로직 적용)
// ===========================================================================
async function processStockHistoryData(arg1, arg2, arg3, arg4) {
    let symbol, from, to;
    if (arg1 && arg1.body) { symbol = arg1.body.symbol; from = arg1.body.from; to = arg1.body.to; } 
    else { symbol = arg1; from = arg2; to = arg3; }

    if (!symbol) throw new Error('Symbol required');

    try {
        const startDate = from ? new Date(from) : new Date('1990-01-01');
        const endDate = to ? new Date(to) : new Date();
        const fmpFrom = startDate.toISOString().split('T')[0];
        const fmpTo = endDate.toISOString().split('T')[0];

        const stockDoc = await admin.firestore().collection('stocks').doc(symbol).get();
        const existingData = stockDoc.exists ? stockDoc.data() : null;
        const isDelisted = existingData && existingData.isDelisted === true;
        
        const isIndex = symbol.startsWith('^'); 

        let targetCurrency = existingData ? existingData.currency : null;

        if (isIndex) {
            console.log(`📈 [Bypass Profile] ${symbol}은 지수이므로 프로필 조회를 건너뜁니다.`);
            targetCurrency = targetCurrency || 'USD'; 
        } else if (!isDelisted) {
            try {
                const profileRes = await fmpClient.get('/profile', { params: { symbol: symbol } });
                const profile = (profileRes.data && profileRes.data.length > 0) ? profileRes.data[0] : null;
                
                if (!profile || !profile.currency) {
                    console.warn(`⚠️ [Skip] ${symbol}: 통화 정보가 존재하지 않아 제외합니다.`);
                    return { success: false, symbol: symbol, message: '통화(currency) 정보 누락' };
                }

                targetCurrency = profile.currency.toUpperCase();
                if (targetCurrency !== 'USD') { // 🌟 USD 필터
                    console.warn(`⚠️ [Skip] ${symbol}: 지원하지 않는 통화(${targetCurrency})`);
                    return { success: false, symbol: symbol, message: `지원하지 않는 통화(${targetCurrency})` };
                }

                const safeIpoPrice = (profile.ipoPrice && profile.ipoPrice > 0) ? profile.ipoPrice : 0;
                
                const updateObj = {
                    symbol: profile.symbol || symbol,
                    name_en: profile.companyName || '',
                    sector: profile.sector || '',
                    industry: profile.industry || '',
                    ipoDate: profile.ipoDate || '',
                    ipoPrice: safeIpoPrice,
                    currency: targetCurrency, 
                    snapshot: { 
                        price: profile.price || 0, 
                        mktCap: profile.marketCap || 0,     
                        lastUpdated: new Date().toISOString() 
                    },
                    active: true,
                    last_crawled: new Date().toISOString()
                };

                if (profile.exchangeShortName) updateObj.exchange = profile.exchangeShortName;

                await admin.firestore().collection('stocks').doc(symbol).set(updateObj, { merge: true });

            } catch (e) { 
                throw new Error(`Profile Load Error: ${e.message}`); 
            }
        } else {
            console.log(`👻 [Bypass Profile] ${symbol}은 상장폐지(Delisted) 종목이므로 프로필 조회를 건너뜁니다.`);
            if (!targetCurrency || targetCurrency !== 'USD') { // 🌟 USD 필터
                return { success: false, symbol: symbol, message: '상장폐지 종목이나 유효한 통화 키가 없음' };
            }
        }

        const histMktCapMap = new Map();
        try {
            const mcapRes = await fmpClient.get('/historical-market-capitalization', {
                params: { symbol: symbol, from: fmpFrom, to: fmpTo, limit: 10000 }
            });
            if (mcapRes.data && Array.isArray(mcapRes.data)) {
                mcapRes.data.forEach(item => {
                    if (item.date && item.marketCap) histMktCapMap.set(item.date, item.marketCap);
                });
            }
        } catch (e) { console.error(`❌ [MCap Error] ${symbol}: ${e.message}`); }

        const dateRanges = [];
        let current = new Date(startDate);
        while (current <= endDate) {
            let next = new Date(current);
            next.setFullYear(current.getFullYear() + 15);
            if (next > endDate) next = endDate;
            dateRanges.push({ from: current.toISOString().split('T')[0], to: next.toISOString().split('T')[0] });
            if (next >= endDate) break;
            current = new Date(next);
            current.setDate(current.getDate() + 1);
        }

        const results = [];
        for (const range of dateRanges) {
            try {
                const fmpRes = await fmpClient.get('/historical-price-eod/full', { 
                    params: { symbol: symbol, from: range.from, to: range.to }
                });
                
                const rawData = fmpRes.data.historical ? fmpRes.data.historical : (Array.isArray(fmpRes.data) ? fmpRes.data : []);
                
                const cleanData = rawData.map(day => ({
                    date: day.date, open: day.open || 0, high: day.high || 0,
                    low: day.low || 0, close: day.close || 0, volume: day.volume || day.vol || 0, 
                    mktCap: histMktCapMap.get(day.date) || 0
                }));
                results.push(cleanData);
            } catch (err) { console.error(`Price range error:`, err.message); }
        }

        let mergedFetchedData = results.flat(); 
        if (mergedFetchedData.length === 0) return { success: true, message: `데이터 없음`, symbol };

        const chunks = {};
        mergedFetchedData.forEach(day => {
            const year = day.date.split('-')[0];
            if (!chunks[year]) chunks[year] = [];
            chunks[year].push(day);
        });

        for (const year of Object.keys(chunks)) {
            const chunkRef = admin.firestore().collection('stocks').doc(symbol).collection('annual_data').doc(year);
            const docSnap = await chunkRef.get();
            let existingChunkData = docSnap.exists ? docSnap.data().data || [] : [];

            const finalMergedData = mergeStockData(existingChunkData, chunks[year]);

            await chunkRef.set({ 
                symbol: symbol, year: parseInt(year), lastUpdated: new Date().toISOString(), data: finalMergedData 
            }, { merge: true });
        }
        
        return { success: true, symbol: symbol, message: 'Updated successfully with Merge' };

    } catch (error) {
        console.error(`ProcessStockHistoryData Error (${symbol}):`, error);
        throw error;
    }
}

module.exports = router;