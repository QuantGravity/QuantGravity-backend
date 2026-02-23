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

// ---------------------------------------------------------------------------
// [Helper] 종목 유효성 검사 (Strict Whitelist)
// ---------------------------------------------------------------------------
function isValidTicker(symbol) {
    if (!symbol) return false;
    const sym = symbol.toUpperCase().trim();

    // 1. 점(.)이 없으면 순수 미국 주식으로 간주 (Keep)
    // 예: AAPL, MSFT, BRK-B (FMP는 대시를 쓰기도 함)
    if (!sym.includes('.')) return true;

    const parts = sym.split('.');
    const suffix = parts[parts.length - 1];

    // 2. 한국 주식 (Keep)
    if (suffix === 'KS' || suffix === 'KQ') return true;

    // 3. 미국 클래스 주식 (Keep) - 오직 A와 B만 허용
    // 예: BRK.A, BRK.B
    // 주의: .T(도쿄), .L(런던), .V(캐나다) 등은 여기서 걸러짐
    if (suffix === 'A' || suffix === 'B') return true;

    // 그 외 모두 Drop
    return false;
}

// ===========================================================================
// [1] 마스터 데이터 관리
// ===========================================================================
// [1.1] 티커 마스터 동기화 (인덱스 국가 정밀 분류 및 GLOBAL 제외, 상태 반영)
// ===========================================================================
router.post('/sync-ticker-master', verifyBatchOrAdmin, async (req, res) => {
    const { mode = 'FULL', limit = 100, exchangeCode } = req.body; 
    const crypto = require('crypto');
    console.log(`🚀 [Ticker Sync] 모드: ${mode} / 자비스: 인덱스 정밀 분류 및 동기화 시작`);

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

        // [Step 1] FMP 데이터 요청
        let params = { limit: 60000 }; 
        if (mode === 'SAMPLE') params.limit = limit;
        if (exchangeCode) params.exchange = exchangeCode;
        else params.country = 'US,KR';

        const resFmp = await fmpClient.get('/company-screener', { params });
        const responseData = resFmp.data || [];
        if (responseData.length === 0) return res.json({ success: false, message: "FMP 데이터 없음" });

        // [Step 2] 정밀 비교를 위한 기존 stocks 전체 로드
        console.log(">> [Deep-Fetch] 비교를 위해 기존 stocks 데이터 로드 중...");
        const stockSnapshot = await db.collection('stocks').get();
        const existingStockMap = new Map();
        stockSnapshot.forEach(doc => existingStockMap.set(doc.id, doc.data()));

        // [Step 3] 데이터 그룹핑 및 필터링 로직
        const groupedData = {};     
        const activeStocksList = [];      
        const inactiveStocksToProcess = []; 

        responseData.forEach(item => {
            if (!item.symbol || (!item.companyName && !item.name)) return;
            if (item.isFund === true) return; 

            const sym = item.symbol.toUpperCase();
            
            // [수정 포인트 1] 여기서 미리 기존 데이터(한글명)를 조회한다!
            const existingData = existingStockMap.get(sym);

            const rawEx = (item.exchangeShortName || '').toUpperCase();
            let country = null;
            let cleanExchange = rawEx;

            // ... (국가 및 거래소 판별 로직은 기존과 동일) ...
            if (rawEx === 'KSC' || rawEx === 'KOE' || sym.endsWith('.KS') || sym.endsWith('.KQ')) {
                country = 'KR';
                cleanExchange = (rawEx === 'KSC' || sym.endsWith('.KS')) ? 'KOSPI' : 'KOSDAQ';

                // 🌟 [핵심 수정] 한국 종목인 경우 보통주(끝자리 0)만 허용
                // symbol 예: "005930.KS" -> 숫자 부분만 추출: "005930"
                const pureTicker = sym.split('.')[0]; 
                if (!pureTicker.endsWith('0')) {
                    // 끝자리가 0이 아니면 우선주(5), 신주인수권 등임 -> 스킵
                    return; 
                }
            } else if (['NASDAQ', 'NYSE', 'AMEX', 'NMS', 'NGS'].includes(rawEx)) {
                country = 'US';
                if (['NMS', 'NGS'].includes(rawEx)) cleanExchange = 'NASDAQ';
                else cleanExchange = rawEx;
            } else if (rawEx === 'INDEX' || sym.startsWith('^')) {
                cleanExchange = 'INDEX';
                if (['^KS11', '^KQ11', '^KS200', '^KRX100'].includes(sym)) country = 'KR';
                else if (['^GSPC', '^IXIC', '^DJI', '^RUT', '^VIX', '^NDX', '^W5000'].includes(sym)) country = 'US';
                else country = 'GLOBAL';
            }

            if (!country || (country !== 'US' && country !== 'KR')) return;

            const isActivelyTrading = item.isActivelyTrading === true;
            
            const stockData = {
                id: sym,
                symbol: item.symbol,
                name_en: item.companyName || item.name,
                name_short: shortenName(item.companyName || item.name),
                // [수정 포인트 1-1] 기존 stocks에 있는 한글명을 가져와서 세팅! 없으면 빈 문자열
                name_ko: existingData?.name_ko || "", 
                ex: cleanExchange,
                etf: item.isEtf,
                sector: item.sector || "Unknown",
                industry: item.industry || "Unknown",
                country: country
            };

            if (isActivelyTrading) {
                const docId = `${country}_${cleanExchange}`;
                if (!groupedData[docId]) groupedData[docId] = [];
                groupedData[docId].push(stockData);
                activeStocksList.push(stockData);
            } else {
                inactiveStocksToProcess.push(sym);
            }
        });

        // [Step 4] 거시적 변경 감지 (MD5 해시 적용)
        const targetDocIds = Object.keys(groupedData);
        let isAnyMetaChanged = false;
        const metaChanges = {}; 

        for (const docId of targetDocIds) {
            const newList = groupedData[docId].sort((a, b) => a.id.localeCompare(b.id));
            
            // 해시 생성 (한글명이 바뀌어도 감지하고 싶다면 여기서 join에 name_ko도 포함해야 하지만, 보통 리스트 구성 변경 위주로 체크함)
            const currentListHash = crypto.createHash('md5')
                                          .update(newList.map(s => s.id).join(','))
                                          .digest('hex');

            const oldMetaDoc = await db.collection('meta_tickers').doc(docId).get();
            const oldMeta = oldMetaDoc.exists ? oldMetaDoc.data() : {};

            const isChanged = oldMeta.count !== newList.length || oldMeta.listHash !== currentListHash;

            if (isChanged || mode === 'FULL') {
                isAnyMetaChanged = true;
                metaChanges[docId] = {
                    list: newList,
                    hash: currentListHash
                }; 
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

                // 기존 청크 삭제
                const oldChunks = await mainDocRef.collection('chunks').get();
                for (const chunkDoc of oldChunks.docs) {
                    batch.delete(chunkDoc.ref);
                    opCount++;
                    await commitBatchIfNeeded();
                }

                // 메타 정보 갱신
                batch.set(mainDocRef, { 
                    count: newList.length, 
                    country: countryFromHeader,
                    updatedAt: new Date().toISOString(), 
                    isChunked: true, 
                    chunkCount: totalChunks,
                    listHash: newListHash 
                }, { merge: true });
                opCount++;
                await commitBatchIfNeeded();

                // 새로운 청크 삽입
                for (let i = 0; i < totalChunks; i++) {
                    batch.set(mainDocRef.collection('chunks').doc(`batch_${i}`), { 
                        chunkIndex: i, 
                        // [수정 포인트 2] 저장할 때 name_ko 필드를 포함시킨다!
                        list: newList.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE).map(s => ({
                            id: s.id, 
                            symbol: s.symbol, 
                            name: s.name_short, 
                            name_ko: s.name_ko, // <--- 여기 추가됨
                            ex: s.ex, 
                            etf: s.etf, 
                            sector: s.sector, 
                            industry: s.industry,
                            country: s.country
                        })) 
                    });
                    opCount++;
                    await commitBatchIfNeeded();
                }
            }
        } else {
            console.log("✅ [Sync Skip] 변경된 마스터 정보가 없습니다.");
        }

        // 🌟 [추가] 이번 FMP 응답에 포함된 모든 ID 셋 생성 (비활성화 판단 기준)
        const fmpResponseIds = new Set([
            ...activeStocksList.map(s => s.id),
            ...inactiveStocksToProcess
        ]);

        // [Step 5] stocks 업데이트 리스트 생성 (기존 로직 유지)
        activeStocksList.forEach(item => {
            const existing = existingStockMap.get(item.id);
            const isChanged = !existing || 
                              existing.country !== item.country || 
                              existing.name_en !== item.name_en || 
                              existing.exchange !== item.ex ||
                              existing.active !== true;

            if (isChanged || mode === 'FULL') {
                stocksUpdateList.push({
                    symbol: item.symbol,
                    name_en: item.name_en,
                    name_short: item.name_short,
                    name_ko: existing?.name_ko || "", // 기존 한글명 유지
                    sector: item.sector, 
                    industry: item.industry,
                    exchange: item.ex, 
                    country: item.country, 
                    active: true, 
                    isEtf: item.etf
                });
            }
        });

        // 2. [핵심 수정] DB에는 있지만 이번 FMP 응답(fmpResponseIds)에 아예 없는 종목만 비활성화
        existingStockMap.forEach((data, sym) => {
            const isTargetExchange = !exchangeCode || data.exchange === exchangeCode;
            
            // 제외 조건: 지수(INDEX)는 제외, 이미 비활성인 것 제외, 이번 응답에 있는 것 제외
            const isIndex = data.ex === 'INDEX' || sym.startsWith('^');

            if (isTargetExchange && !isIndex && !fmpResponseIds.has(sym)) {
                if (data.active !== false) {
                    stocksUpdateList.push({
                        symbol: sym,
                        active: false 
                    });
                    console.log(`>> [Deactivate] ${sym} (FMP 마스터 리스트에서 사라짐)`);
                }
            }
        });

        // ... (이후 비활성 처리 및 업데이트 로직은 기존과 동일) ...
        inactiveStocksToProcess.forEach(sym => {
            const existing = existingStockMap.get(sym);
            if (existing && existing.active !== false) {
                stocksUpdateList.push({
                    symbol: sym,
                    active: false 
                });
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
                    lastUpdated: new Date().toISOString(),
                    version: Date.now(),
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
// [2.1] 주가 전체 업데이트 (진단 로그 강화 + 미국시간 + 에러 추적)
// ⚡ [수정] 아무나 호출하지 못하도록 verifyBatchOrAdmin 미들웨어 장착
// ===========================================================================
// ===========================================================================
// [2.1] 주가 전체 업데이트 (실행 시점 현재일 전용)
// ===========================================================================
// [Backend] daily-update-all 라우터
router.post('/daily-update-all', verifyBatchOrAdmin, async (req, res) => {
    const { market } = req.body;
    const targetMarket = market || 'US';

    // 국가별 현재 날짜 및 주말 판별 로직
    const getTodayInfoByMarket = (marketType) => {
        const timeZone = (marketType === 'KR') ? 'Asia/Seoul' : 'America/New_York';
        const now = new Date();
        const timeStr = now.toLocaleString("en-US", {
            timeZone: timeZone, year: "numeric", month: "2-digit", day: "2-digit"
        });
        const [m, d, y] = timeStr.split('/');
        const targetDate = `${y}-${m}-${d}`;

        // 요일 추출 (0: 일요일, 6: 토요일)
        const dayOfWeek = new Date(parseInt(y), parseInt(m) - 1, parseInt(d)).getDay();
        const isWeekend = (dayOfWeek === 0 || dayOfWeek === 6);

        return { targetDate, isWeekend };
    };

    const { targetDate, isWeekend } = getTodayInfoByMarket(targetMarket);

    // 🌟 [추가] 주말 필터링: 토, 일요일이면 배치 실행 안 함
    if (isWeekend) {
        console.log(`⏭️ [Daily Batch Skip] ${targetDate} (${targetMarket})은 주말이므로 주가 수집을 스킵합니다.`);
        return res.json({ 
            success: true, 
            date: targetDate,
            message: `[${targetMarket}] 주말이므로 데이터 수집 배치를 건너뜁니다.` 
        });
    }

    // 즉시 응답 (평일인 경우 정상 실행)
    res.json({ 
        success: true, 
        date: targetDate,
        message: `[${targetMarket}] 현재일 데이터 수집을 백그라운드에서 시작합니다.` 
    });
    
    setImmediate(async () => {
        try {
            console.log(`\n============== [Daily Batch Start] ==============`);
            
            // 1. 유효 종목 리스트 필터링
            const snapshot = await admin.firestore().collection('stocks').select('type', 'exchange').get();
            const validSymbolsSet = new Set();

            snapshot.forEach(doc => {
                const sym = doc.id;
                const data = doc.data() || {};
                let isKr = sym.startsWith('KR_') || ['KSC', 'KOE', 'KOSPI', 'KOSDAQ'].includes(data.exchange);
                if (targetMarket === 'US' && isKr) return;
                if (targetMarket === 'KR' && !isKr) return;
                validSymbolsSet.add(sym);
            });

            if (validSymbolsSet.size === 0) return console.error("❌ 대상 종목 없음");

            // 2. 내부 함수 호출
            await processBulkDailyDataInternal(targetDate, validSymbolsSet);

            console.log(`============== [Daily Batch End: ${targetDate}] ============== \n`);
        } catch (error) {
            console.error("💥 [Batch Critical Error]", error);
        }
    });
});

// ===========================================================================
// [2-2] 배치 및 청소 도구
// ===========================================================================
router.post('/cleanup-garbage-stocks', verifyToken, async (req, res) => {
    try {
        const db = admin.firestore();
        console.log("🧹 [Cleanup] 최종 청소 시작 (Whitelist 기준 삭제)...");

        const collectionRef = db.collection('stocks');
        const docRefs = await collectionRef.listDocuments();
        console.log(`🔎 전체 스캔 완료: 총 ${docRefs.length}개 문서 검열 중...`);

        let deleteTargets = [];
        let detectedSuffixes = new Set(); 

        for (const docRef of docRefs) {
            const symbol = docRef.id.toUpperCase().trim();
            // 🛑 Strict Whitelist Check
            if (isValidTicker(symbol)) continue; // 합격하면 생존

            // 불합격 시 삭제
            deleteTargets.push(docRef);
            if (symbol.includes('.')) detectedSuffixes.add('.' + symbol.split('.').pop());
        }

        console.log(`📋 삭제 예정: ${deleteTargets.length}개 (패턴: ${Array.from(detectedSuffixes).join(', ')})`);

        if (deleteTargets.length === 0) return res.json({ success: true, message: "삭제할 데이터가 없습니다." });

        let deleteCount = 0;
        const batchSize = 50; 
        for (let i = 0; i < deleteTargets.length; i += batchSize) {
            const chunk = deleteTargets.slice(i, i + batchSize);
            await Promise.all(chunk.map(async (ref) => {
                try {
                    await db.recursiveDelete(ref); // 하위 컬렉션 포함 완전 삭제
                    deleteCount++;
                    process.stdout.write('.');
                } catch (e) { console.error(`❌ ${ref.id} 삭제 실패:`, e.message); }
            }));
            if (i > 0 && i % 500 === 0) await new Promise(r => setTimeout(r, 500));
        }

        console.log(`\n✅ [Cleanup] 완료. 총 ${deleteCount}개 삭제.`);
        res.json({ success: true, deletedCount: deleteCount, foundSuffixes: Array.from(detectedSuffixes) });

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

        // 문서 청크 분할 저장 헬퍼 (기존 로직 유지)
        const saveChunks = async (docId, desc, list, countryCode = 'US') => {
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
                exchange: 'INDEX', description: desc, 
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

        // ==========================================================
        // [Step 1 & 2 통합] 대표 지수 대응 ETF 리스트 (비중 데이터 확보용)
        // ==========================================================
        const etfTargets = [
            { id: 'US_SP500', symbol: 'SPY', desc: 'S&P 500', country: 'US', step: '1/5' },
            { id: 'US_NASDAQ100', symbol: 'QQQ', desc: 'NASDAQ 100', country: 'US', step: '2/5' },
            { id: 'US_DOW30', symbol: 'DIA', desc: 'Dow Jones 30', country: 'US', step: '3/5' },
            { id: 'US_SP100', symbol: 'OEF', desc: 'S&P 100', country: 'US', step: '4/5' },
            { id: 'KR_MSCI_KOREA', symbol: 'EWY', desc: 'MSCI South Korea', country: 'KR', step: '5/5' }
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

                        // 비중 순 정렬
                        list.sort((a, b) => b.weight - a.weight);

                        await saveChunks(target.id, `${target.desc} (via ${target.symbol} Holdings)`, list, target.country);
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

        // ==========================================================
        // [Last Step] 전체 지수 리스트 수신 및 Stocks 등록 (기존 로직 유지)
        // ==========================================================
        try {
            console.log("📡 [Last Step] Global Indices 목록 요청 및 Stocks 전체 등록...");
            const allIdx = await fmpClient.get('/index-list');
            
            if(allIdx.data && Array.isArray(allIdx.data)) {
                const indexList = allIdx.data;
                console.log(`✅ FMP 수신 완료: 총 ${indexList.length}개 지수`);

                // 1. Meta Tickers에 저장
                await saveChunks('INDEX', 'Global Indices', indexList.map(i=>({symbol:i.symbol, name:i.name, ex: 'INDEX'})), 'GLOBAL');

                // 2. Stocks 컬렉션에 전체 등록
                console.log("💾 Stocks 컬렉션에 전체 지수 데이터를 동기화합니다...");
                
                let batch = db.batch();
                let opCount = 0;
                let savedCount = 0;

                for (const item of indexList) {
                    const stockRef = db.collection('stocks').doc(item.symbol);
                    
                    batch.set(stockRef, {
                        symbol: item.symbol,
                        name_en: item.name,
                        exchange: 'INDEX',
                        isEtf: false,
                        active: true,
                        type: 'index',
                        updatedAt: new Date().toISOString()
                    }, { merge: true });

                    opCount++;
                    savedCount++;

                    if (opCount >= 400) {
                        await batch.commit();
                        batch = db.batch();
                        opCount = 0;
                        console.log(`... ${savedCount} / ${indexList.length} 저장 중`);
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

        res.json({ success: true, message: "MSCI Korea 포함 지수 비중 동기화 완료!" });
    } catch (e) { 
        res.status(500).json({ error: e.message }); 
    }
});

// [4] 상장폐지 종목 동기화 (Strict Guard)
router.post('/sync-delisted-master', verifyToken, async (req, res) => {
    try {
        console.log("🚀 [Delisted] 상장폐지 종목 동기화 (Strict)...");
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

        const groupedData = {};
        allDelisted.forEach(item => {
            if (!item.symbol) return;
            const sym = item.symbol.toUpperCase();
            
            // 🛑 [Strict Guard]
            if (!isValidTicker(sym)) return;

            let rawExchange = (item.exchange || 'Unknown').toUpperCase();
            let country = null;
            let cleanExchange = rawExchange;

            if (sym.endsWith('.KS') || sym.endsWith('.KQ') || (['KOSPI', 'KOSDAQ', 'KSE', 'KOE'].some(k => rawExchange.includes(k)))) {
                country = 'KR';
                if (rawExchange.includes('KOSDAQ') || sym.endsWith('.KQ')) cleanExchange = 'KOSDAQ';
                else cleanExchange = 'KOSPI'; 
            } else if (['NASDAQ', 'NYSE', 'AMEX', 'NMS', 'NGS'].some(u => rawExchange.includes(u))) {
                 country = 'US';
                 // NMS, NGS를 포함하거나 NASDAQ인 경우 모두 'NASDAQ'으로 통일
                if (rawExchange.includes('NASDAQ') || rawExchange.includes('NMS') || rawExchange.includes('NGS')) cleanExchange = 'NASDAQ';
                 else if (rawExchange.includes('NYSE')) cleanExchange = 'NYSE';
                 else if (rawExchange.includes('AMEX')) cleanExchange = 'AMEX';
                 else cleanExchange = '';
            }

            if (!country) return; 

            const docId = `${country}_${cleanExchange}`;
            if (!groupedData[docId]) groupedData[docId] = [];
            groupedData[docId].push({
                s: item.symbol, n: item.companyName, ex: cleanExchange, 
                delDate: item.delistedDate, ipoDate: item.ipoDate
            });
        });

        // 저장 로직 (동일)
        const db = admin.firestore();
        const collectionRef = db.collection('meta_delisted');
        const CHUNK_SIZE = 600;
        let batch = db.batch();
        let opCount = 0;
        let savedGroups = 0;

        for (const [docId, list] of Object.entries(groupedData)) {
            list.sort((a, b) => a.s.localeCompare(b.s)); 
            const mainDocRef = collectionRef.doc(docId);
            const totalChunks = Math.ceil(list.length / CHUNK_SIZE);
            const [c, e] = docId.split('_'); 

            // 기존 청크 삭제 (Clean Update)
            const existing = await mainDocRef.collection('chunks').get();
            if(!existing.empty) {
                const delBatch = db.batch();
                existing.docs.forEach(d => delBatch.delete(d.ref));
                await delBatch.commit();
            }

            batch.set(mainDocRef, { country: c, exchange: e, count: list.length, chunkCount: totalChunks, isChunked: true, updatedAt: new Date().toISOString() }, { merge: true });
            opCount++;

            for (let i = 0; i < totalChunks; i++) {
                batch.set(mainDocRef.collection('chunks').doc(`batch_${i}`), { chunkIndex: i, list: list.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE) });
                opCount++;
                if (opCount >= 400) { await batch.commit(); batch = db.batch(); opCount = 0; }
            }
            savedGroups++;
        }
        if (opCount > 0) await batch.commit();

        res.json({ success: true, count: allDelisted.length, savedGroups: savedGroups, groups: Object.keys(groupedData) });
    } catch (error) {
        console.error("Delisted Sync Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// [5] 기업 이벤트 (배당/분할) - DB 존재 종목만 저장
router.post('/sync-action-master', verifyToken, async (req, res) => {
    const { mode = 'DAILY', targetYear } = req.body; 
    console.log(`🚀 [Action Sync] 기업 이벤트 동기화 (Mode: ${mode}, Valid Stocks Only)...`);

    try {
        const db = admin.firestore();
        
        // ⚡ [핵심] 유효 종목 리스트 로드 (Whitelist)
        console.log("   👉 유효 종목 리스트 로딩 중...");
        const snapshot = await db.collection('stocks').select().get();
        const validSymbols = new Set();
        snapshot.forEach(doc => validSymbols.add(doc.id)); 
        console.log(`   ✅ 유효 종목 ${validSymbols.size}개 로드 완료.`);

        let yearsToProcess = [];
        const currentYear = new Date().getFullYear();

        if (mode === 'INIT_ALL') {
            for (let y = 2000; y <= currentYear; y++) yearsToProcess.push(y);
        } else if (mode === 'YEAR' && targetYear) {
            yearsToProcess.push(parseInt(targetYear));
        } else {
            // DAILY
            yearsToProcess = null;
        }

        // [Logic A] 전체/연도별 수집
        if (yearsToProcess) {
            let totalDivs = 0;
            let totalSplits = 0;

            for (const year of yearsToProcess) {
                console.log(`   👉 ${year}년 데이터 요청 중...`);
                const from = `${year}-01-01`;
                const to = `${year}-12-31`;

                try {
                    const [divRes, splitRes] = await Promise.all([
                        fmpClient.get('/dividends-calendar', { params: { from, to } }),
                        fmpClient.get('/splits-calendar', { params: { from, to } })
                    ]);

                    const divData = divRes.data || [];
                    const splitData = splitRes.data || [];
                    const batchHandler = db.batch();
                    let opCount = 0;

                    for (const item of divData) {
                        // 🛑 DB에 없는 종목은 저장 안 함!
                        if (!item.symbol || !validSymbols.has(item.symbol)) continue; 
                        const docRef = db.collection('stocks').doc(item.symbol).collection('dividends').doc(item.date);
                        batchHandler.set(docRef, item, { merge: true });
                        opCount++;
                    }

                    for (const item of splitData) {
                        if (!item.symbol || !validSymbols.has(item.symbol)) continue;
                        const docRef = db.collection('stocks').doc(item.symbol).collection('splits').doc(item.date);
                        batchHandler.set(docRef, {
                            date: item.date, label: item.label || '', numerator: item.numerator || 0, denominator: item.denominator || 0, ratio: `${item.numerator}:${item.denominator}`
                        }, { merge: true });
                        opCount++;
                    }

                    if (opCount > 0) await batchHandler.commit();
                    totalDivs += divData.length;
                    totalSplits += splitData.length;
                    await new Promise(r => setTimeout(r, 200));

                } catch (err) { console.error(`   ❌ ${year}년 오류: ${err.message}`); }
            }
            return res.json({ success: true, message: "완료", stats: { dividends: totalDivs, splits: totalSplits } });
        }
        
        // [Logic B] 데일리 업데이트 (최근 2주)
        else {
            const today = new Date();
            const past = new Date(); past.setDate(today.getDate() - 14);
            const future = new Date(); future.setDate(today.getDate() + 14);
            const from = past.toISOString().split('T')[0];
            const to = future.toISOString().split('T')[0];

            console.log(` - [데일리] ${from} ~ ${to}`);
            const [divRes, splitRes] = await Promise.all([
                fmpClient.get('/dividends-calendar', { params: { from, to } }),
                fmpClient.get('/splits-calendar', { params: { from, to } })
            ]);

            const batch = db.batch();
            let count = 0;

            divRes.data?.forEach(item => {
                if(item.symbol && validSymbols.has(item.symbol)) {
                    batch.set(db.collection('stocks').doc(item.symbol).collection('dividends').doc(item.date), item, { merge: true });
                    count++;
                }
            });

            splitRes.data?.forEach(item => {
                if(item.symbol && validSymbols.has(item.symbol)) {
                    batch.set(db.collection('stocks').doc(item.symbol).collection('splits').doc(item.date), item, { merge: true });
                    count++;
                }
            });

            if (count > 0) await batch.commit();
            return res.json({ success: true, mode: 'DAILY', stats: { validUpdates: count } });
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

    // [Step 1] 종목 정보 확인 (존재 여부 및 ETF 여부)
    const docSnap = await stockRef.get();
    if (!docSnap.exists) return false;

    const stockData = docSnap.data();

    // 🛑 [ETF 필터링] ETF는 재무제표가 없으므로 API 호출 자체를 스킵
    if (stockData.isEtf === true) {
        console.log(`⏩ [Skip] ${symbol} is ETF (No Financials)`);
        return 'SKIPPED'; // ETF 스킵 시그널 반환
    }

    const batch = db.batch();
    let savedTypes = [];

    // 5가지 재무 데이터 엔드포인트
    const stmts = [
        { type: 'income-statement', url: '/income-statement' },
//        { type: 'balance-sheet-statement', url: '/balance-sheet-statement' },
//        { type: 'cash-flow-statement', url: '/cash-flow-statement' },
        { type: 'financial-ratios', url: '/ratios' },
//        { type: 'key-metrics', url: '/key-metrics' }
    ];

    for (const stmt of stmts) {
        try {
            // ⚡ [핵심] fmpClient 사용 (API Key 자동 주입됨)
            // Stable API: /income-statement?symbol=AAPL&limit=30&period=annual
            const res = await fmpClient.get(stmt.url, { 
                params: { 
                    symbol: symbol, 
                    limit: 30,      
                    period: 'annual' 
                } 
            });

            if (res.data && Array.isArray(res.data) && res.data.length > 0) {
                const targetFields = ESSENTIAL_FIELDS[stmt.type];
                
                // 필드 필터링
                const filteredData = res.data.map(item => {
                    const cleanItem = {};
                    targetFields.forEach(field => {
                        if (item[field] !== undefined && item[field] !== null) {
                            cleanItem[field] = item[field];
                        }
                    });
                    return cleanItem;
                });

                // 날짜 내림차순 정렬
                filteredData.sort((a, b) => new Date(b.date) - new Date(a.date));

                // 저장
                batch.set(stockRef.collection('financials').doc(stmt.type), {
                    type: stmt.type, 
                    symbol: symbol, 
                    updatedAt: new Date().toISOString(), 
                    history: filteredData
                });
                savedTypes.push(stmt.type);
            } 
        } catch (e) { 
            // 개별 실패(404 등)는 로그만 남기고 무시 (전체 트랜잭션 방해 금지)
            // console.warn(`   ⚠️ [Skip] ${symbol} ${stmt.type}`);
        }
    }

    if (savedTypes.length > 0) {
        batch.set(stockRef, { 
            last_financial_update: new Date().toISOString(),
            has_financials: true 
        }, { merge: true });
        
        await batch.commit();
        // 📢 [로그 추가] 여기가 핵심! 이걸 넣으면 Render 로그창에 뜹니다.
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
    // 타임아웃 방지: 응답 먼저 보냄
    res.json({ success: true, status: 'STARTED', message: "전체 재무제표 수집 시작 (백그라운드)" });

    // 백그라운드 실행
    (async () => {
        try {
            console.log("🚀 [Batch Financials] 전체 수집 시작...");
            const db = admin.firestore();
            
            // Active 종목만 가져오기
            const snapshot = await db.collection('stocks').where('active', '==', true).select().get();
            const symbols = snapshot.docs.map(doc => doc.id);
            console.log(`📋 대상 종목: ${symbols.length}개`);

            const CONCURRENCY = 3; // 동시 처리 수
            for (let i = 0; i < symbols.length; i += CONCURRENCY) {
                const chunk = symbols.slice(i, i + CONCURRENCY);
                await Promise.all(chunk.map(async (symbol) => {
                    try {
                        await saveFinancialsInternal(db, symbol);
                    } catch (e) { console.error(`❌ ${symbol} Fail`); }
                }));
                // 딜레이
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

async function syncSectorMasterInternal() {
    const db = admin.firestore();
    console.log("🚀 [Master Sync] 섹터/산업 데이터 동기화 시작...");

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

        // [중요] 변경사항 여부 판단
        const hasActualChanges = !isSectorListSame || !isIndustryListSame || !isHierarchySame || newIndustries.length > 0;

        // 모든 것이 똑같고, 번역할 신규 산업도 없다면 즉시 종료!
        if (!hasActualChanges) {
            console.log("✅ [Master Sync] 모든 데이터 및 구조가 기존과 완전히 동일합니다. (DB 업데이트 및 번역 생략)");
            return { success: true, updated: false, message: "변경사항 없음" };
        }

        console.log(`⚡ 변경 감지됨! [섹터변경: ${!isSectorListSame}, 산업목록변경: ${!isIndustryListSame}, 계층구조변경: ${!isHierarchySame}, 신규산업: ${newIndustries.length}개]`);
        
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

        // 9. 최종 데이터 조립
        const structuredSectors = fmpSectors.map(en => ({
            key: en,
            name_en: en,
            name_ko: FIXED_SECTOR_MAP[en] || translationMap.get(en) || en
        }));

        const structuredIndustries = fmpIndustries.map(en => ({
            key: en,
            name_en: en,
            name_ko: translationMap.get(en) || en
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

        // 1. 프로필 업데이트 (거래소 필드 보호)
        try {
            const profileRes = await fmpClient.get('/profile', { params: { symbol: symbol } });
            const profile = (profileRes.data && profileRes.data.length > 0) ? profileRes.data[0] : null;
            if (profile) {
                const safeIpoPrice = (profile.ipoPrice && profile.ipoPrice > 0) ? profile.ipoPrice : 0;
                
                // 업데이트할 객체 생성 (값이 있을 때만 필드 포함)
                const updateObj = {
                    symbol: profile.symbol || symbol,
                    name_en: profile.companyName || '',
                    sector: profile.sector || '',
                    industry: profile.industry || '',
                    ipoDate: profile.ipoDate || '',
                    ipoPrice: safeIpoPrice,
                    description: profile.description || '',
                    website: profile.website || '',
                    currency: profile.currency || 'USD',
                    image: profile.image || '',
                    ceo: profile.ceo || '',
                    snapshot: { 
                        price: profile.price || 0, 
                        mktCap: profile.marketCap || 0,     // [변경] mktCap -> marketCap
                        aveVol: profile.averageVolume || 0,    // [변경] volAvg -> averageVolume
                        beta: profile.beta || 0, 
                        div: profile.lastDividend || 0,     // [변경] lastDiv -> lastDividend
                        range: profile.range || '', 
                        lastUpdated: new Date().toISOString() 
                    },
                    active: true,
                    last_crawled: new Date().toISOString()
                };

                // [수정] 거래소 정보가 확실히 있을 때만 업데이트하여 기존 데이터 보존
                if (profile.exchangeShortName) {
                    updateObj.exchange = profile.exchangeShortName;
                }

                await admin.firestore().collection('stocks').doc(symbol).set(updateObj, { merge: true });
            }
        } catch (e) { console.error(`Profile Load Error (${symbol}):`, e.message); }

        // 2. 과거 시가총액 데이터 수집 (Map 생성)
        const histMktCapMap = new Map();
        try {
            // 시가총액은 데이터가 많을 수 있으므로 10,000개 리미트로 충분히 수집
            const mcapRes = await fmpClient.get('/historical-market-capitalization', {
                params: { symbol: symbol, from: fmpFrom, to: fmpTo, limit: 10000 }
            });
            if (mcapRes.data && Array.isArray(mcapRes.data)) {
                mcapRes.data.forEach(item => {
                    if (item.date && item.marketCap) {
                        histMktCapMap.set(item.date, item.marketCap);
                    }
                });
            }
        } catch (e) { console.error(`❌ [MCap Error] ${symbol}: ${e.message}`); }

        // 3. Historical Price 호출 (날짜 분할 호출 유지)
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
                
                // FMP 응답 구조 대응 (historical 키가 있을 수도 있고 아닐 수도 있음)
                const rawData = fmpRes.data.historical ? fmpRes.data.historical : (Array.isArray(fmpRes.data) ? fmpRes.data : []);
                
                const cleanData = rawData.map(day => ({
                    date: day.date,
                    open: day.open || 0,
                    high: day.high || 0,
                    low: day.low || 0,
                    close: day.close || 0,
                    volume: day.volume || day.vol || 0, // [수정] volume과 vol 필드 모두 대응
                    mktCap: histMktCapMap.get(day.date) || 0 // 미리 수집한 Map에서 매칭
                }));
                results.push(cleanData);
            } catch (err) { console.error(`Price range error:`, err.message); }
        }

        let mergedFetchedData = results.flat(); 
        if (mergedFetchedData.length === 0) return { success: true, message: `데이터 없음`, symbol };

        // 4. 연도별 분류
        const chunks = {};
        mergedFetchedData.forEach(day => {
            const year = day.date.split('-')[0];
            if (!chunks[year]) chunks[year] = [];
            chunks[year].push(day);
        });

        // 5. DB 저장 (Merge 로직 적용)
        for (const year of Object.keys(chunks)) {
            const chunkRef = admin.firestore().collection('stocks').doc(symbol).collection('annual_data').doc(year);
            
            // 1) 기존 데이터 읽기
            const docSnap = await chunkRef.get();
            let existingData = [];
            if (docSnap.exists) {
                existingData = docSnap.data().data || [];
            }

            // 2) 데이터 병합 (기존 함수 mergeStockData 사용)
            const finalMergedData = mergeStockData(existingData, chunks[year]);

            // 3) 연도별 데이터 저장
            await chunkRef.set({ 
                symbol: symbol, 
                year: parseInt(year), 
                lastUpdated: new Date().toISOString(), 
                data: finalMergedData 
            }, { merge: true });
        }
        
        return { success: true, symbol: symbol, message: 'Updated successfully with Merge' };

    } catch (error) {
        console.error(`ProcessStockHistoryData Error (${symbol}):`, error);
        throw error;
    }
}

module.exports = router;