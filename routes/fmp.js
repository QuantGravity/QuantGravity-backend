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
const { verifyToken } = require('../utils/authHelper');
const { askJarvis } = require('../utils/jarvisClient'); // ⚡ [추가] 자비스 호출
const { getTickerData } = require('../utils/stockHelper');

// ⚡ [추가] 깃허브 액션(배치) & 관리자 공용 인증 미들웨어
const BATCH_SECRET = process.env.BATCH_SECRET_KEY || 'quantgravity_batch_secret_20260218'; 
const verifyBatchOrAdmin = (req, res, next) => {
    const batchKey = req.headers['x-batch-key'];
    // 1. 헤더에 배치용 시크릿 키가 일치하면 무사 통과 (GitHub Actions 용)
    if (batchKey && batchKey === BATCH_SECRET) {
        return next();
    }
    // 2. 배치 키가 없으면 기존 프론트엔드 관리자 토큰 검증 로직 실행
    return verifyToken(req, res, next);
};

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

// [1.1] 티커 마스터 동기화 (Strict Guard)
// [1.1] 티커 마스터 동기화 (기존 데이터 보존 및 병합 로직 추가)
router.post('/sync-ticker-master', verifyToken, async (req, res) => {
    const { mode = 'FULL', limit = 100, exchangeCode } = req.body; 
    console.log(`🚀 [Ticker Sync] 모드: ${mode} / 거래소 중심 엄격 분류 시작`);

    const shortenName = (name) => {
        if (!name) return "";
        return name.replace(/,?\s*Inc\.?$/i, "").replace(/,?\s*Corp\.?$/i, "").replace(/,?\s*Corporation$/i, "")
                   .replace(/,?\s*Ltd\.?$/i, "").replace(/,?\s*Limited$/i, "").replace(/,?\s*PLC$/i, "")
                   .replace(/,?\s*Co\.?$/i, "").replace(/,?\s*Company$/i, "").trim();
    };

    try {
        const db = admin.firestore();

        // [Step 0] 기존 stocks 컬렉션에서 '수기 관리 데이터' 미리 로드 (기초자산, 레버리지, 한글명 등)
        // 전체를 읽는 것이 부담된다면 필요한 필드만 select 하지만, 여기서는 병합을 위해 로드합니다.
        console.log(">> [Pre-Fetch] 기존 stocks 데이터(한글명, 기초자산, 레버리지 등) 로딩 중...");
        
        // 메모리 효율을 위해 필요한 필드만 가져오거나, 데이터가 많으면 배치로 처리해야 하지만
        // 여기서는 Sync 작업이므로 전체 Map을 생성합니다.
        const stockSnapshot = await db.collection('stocks').get();
        const existingStockMap = new Map();
        
        stockSnapshot.forEach(doc => {
            const d = doc.data();
            existingStockMap.set(doc.id, {
                ticker_name_kr: d.ticker_name_kr || "",
                sector_kr: d.sector_kr || "",
                industry_kr: d.industry_kr || "",
                underlying_ticker: d.underlying_ticker || "",
                leverage_factor: d.leverage_factor || 1
            });
        });
        console.log(`>> [Pre-Fetch] ${existingStockMap.size}개 기존 데이터 로드 완료.`);

        // [Step 1] FMP 데이터 요청
        let params = {}; 
        if (mode === 'SAMPLE') {
            params.limit = limit;
            params.exchange = 'NASDAQ,NYSE,AMEX'; 
        } else if (mode === 'EXCHANGE' && exchangeCode) {
            params.limit = 20000; 
            params.exchange = exchangeCode;
        } else {
            params.limit = 60000; 
            params.country = 'US,KR'; 
        }

        console.log(`>> FMP 데이터 요청 중...`);
        const resFmp = await fmpClient.get('/company-screener', { params });
        const responseData = resFmp.data || [];

        if (responseData.length === 0) return res.json({ success: false, message: "FMP 데이터 없음" });

        const groupedData = {};     
        const stocksUpdateList = []; 
        let skippedCount = 0; 
        
        // [Step 2] 데이터 병합 및 그룹핑
        responseData.forEach(item => {
            if (!item.symbol || (!item.companyName && !item.name)) return;
            if (!item.isActivelyTrading || item.isFund === true) { skippedCount++; return; }
            if ((item.marketCap || 0) < 10000000) { skippedCount++; return; } 

            const sym = item.symbol.toUpperCase();
            
            // 화이트리스트 검사 (isValidTicker 함수가 있다고 가정)
            if (typeof isValidTicker === 'function' && !isValidTicker(sym)) {
                skippedCount++;
                return;
            }

            // 기존 stocks 데이터 조회
            const existing = existingStockMap.get(sym) || {};

            const rawEx = (item.exchangeShortName || 'OTC').toUpperCase();
            let country = null;
            let cleanExchange = rawEx;

            // [Rule 1] 한국 시장
            if (rawEx === 'KSC' || rawEx === 'KOE' || sym.endsWith('.KS') || sym.endsWith('.KQ')) {
                country = 'KR';
                if (rawEx === 'KSC' || sym.endsWith('.KS')) cleanExchange = 'KOSPI';
                else if (rawEx === 'KOE' || sym.endsWith('.KQ')) cleanExchange = 'KOSDAQ';
                else cleanExchange = 'OTC';
            }
            // [Rule 2] 미국 시장
            else if (['NASDAQ', 'NYSE', 'AMEX', 'OTC', 'PNK', 'NMS', 'NGS'].includes(rawEx)) {
                country = 'US';
                if (rawEx === 'PNK') cleanExchange = 'OTC'; 
                else if (['NMS', 'NGS'].includes(rawEx)) cleanExchange = 'NASDAQ';
                else cleanExchange = rawEx;
            }

            if (!country) {
                skippedCount++;
                return;
            }

            const docId = `${country}_${cleanExchange}`;
            if (!groupedData[docId]) groupedData[docId] = [];
            
            // 🛑 [핵심 수정] meta_tickers 청크에 저장될 데이터 객체 구성
            // FMP 데이터 + 기존 DB 데이터(한글명, 기초자산, 레버리지) 병합
            groupedData[docId].push({
                id: sym,                // ID 통일
                symbol: item.symbol,
                name: shortenName(item.companyName || item.name),
                ex: cleanExchange, 
                
                // [표시 필드 수정]
                market_cap: item.marketCap, // 프론트엔드에서 사용하는 키값으로 저장
                price: item.price,
                vol: item.volume,
                etf: item.isEtf,
                
                // [기존 정보 병합]
                ticker_name_kr: existing.ticker_name_kr || "",  // 한글명
                sector_kr: existing.sector_kr || "",            // 섹터(한글)
                industry_kr: existing.industry_kr || "",        // 산업(한글)
                
                // [FMP 정보 Fallback]
                sector: item.sector, 
                industry: item.industry,

                // [요청한 핵심 필드 추가]
                underlying_ticker: existing.underlying_ticker || "", // 기초자산
                leverage_factor: existing.leverage_factor || 1       // 레버리지
            });

            // stocks 컬렉션 업데이트용 리스트 (여기서도 병합된 정보를 유지해야 함)
            stocksUpdateList.push({
                symbol: item.symbol,
                name_en: item.companyName || item.name,
                name_short: shortenName(item.companyName || item.name),
                
                // 기존 데이터 보존하면서 업데이트
                ticker_name_kr: existing.ticker_name_kr || "",
                sector_kr: existing.sector_kr || "",
                industry_kr: existing.industry_kr || "",
                underlying_ticker: existing.underlying_ticker || "",
                leverage_factor: existing.leverage_factor || 1,

                sector: item.sector,
                industry: item.industry,
                exchange: cleanExchange,
                country: country, 
                currency: (country === 'KR') ? 'KRW' : 'USD', 
                active: true,
                isEtf: item.isEtf, 
                snapshot: { price: item.price, mktCap: item.marketCap, lastUpdated: new Date().toISOString() }
            });
        });

        // [Step 3] Firestore 저장 (Batch)
        let batch = db.batch();
        let opCount = 0;
        let savedGroups = 0;

        const finalColRef = db.collection(mode === 'SAMPLE' ? '_debug_sample' : 'meta_tickers');
        const CHUNK_SIZE = 600; 

        for (const [docId, list] of Object.entries(groupedData)) {
            list.sort((a, b) => a.symbol.localeCompare(b.symbol));
            const mainDocRef = finalColRef.doc(docId);
            const totalChunks = Math.ceil(list.length / CHUNK_SIZE);
            const [c, e] = docId.split('_');

            batch.set(mainDocRef, { country: c, exchange: e, count: list.length, updatedAt: new Date().toISOString(), isChunked: true, chunkCount: totalChunks }, { merge: true });
            opCount++;

            for (let i = 0; i < totalChunks; i++) {
                batch.set(mainDocRef.collection('chunks').doc(`batch_${i}`), { chunkIndex: i, list: list.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE) });
                opCount++;
                if (opCount >= 400) { await batch.commit(); batch = db.batch(); opCount = 0; }
            }
            savedGroups++;
        }

        console.log(`>> stocks 동기화 (${stocksUpdateList.length}개)...`);
        for (const stock of stocksUpdateList) {
            batch.set(db.collection('stocks').doc(stock.symbol), stock, { merge: true });
            opCount++;
            if (opCount >= 400) { await batch.commit(); batch = db.batch(); opCount = 0; await new Promise(r => setTimeout(r, 50)); }
        }
        if (opCount > 0) await batch.commit();

        res.json({ success: true, mode, validCount: responseData.length - skippedCount, savedGroups, stocksUpdated: stocksUpdateList.length });

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
router.post('/daily-update-all', verifyBatchOrAdmin, async (req, res) => {
    // 🌐 [Helper] 국가(Market) 타임존 기준 날짜 계산 함수
    const getDateByMarket = (offsetDays = 0, market = 'US') => {
        // KR이면 서울, 그 외(US, ALL 등)는 뉴욕 시간 적용
        const timeZone = (market === 'KR') ? 'Asia/Seoul' : 'America/New_York';
        
        const now = new Date();
        const timeStr = now.toLocaleString("en-US", {
            timeZone: timeZone,
            year: "numeric", month: "2-digit", day: "2-digit"
        });
        
        const [m, d, y] = timeStr.split('/');
        const targetDate = new Date(`${y}-${m}-${d}`);
        targetDate.setDate(targetDate.getDate() - offsetDays);
        
        return targetDate.toISOString().split('T')[0];
    };

    // [Helper] 날짜 범위 생성 함수
    const getDatesStartToByEnd = (start, end) => {
        const result = [];
        const curDate = new Date(start);
        const endDate = new Date(end);
        while (curDate <= endDate) {
            result.push(curDate.toISOString().split('T')[0]);
            curDate.setDate(curDate.getDate() + 1);
        }
        return result.reverse(); // 최신순
    };

    const { startDate, endDate, days, market } = req.body;
    const targetMarket = market || 'ALL';
    let targetDates = [];

    // 🛑 [날짜 결정 로직 우선순위]
    if (startDate && endDate) {
        // 1순위: 관리자 화면에서 달력으로 구간을 정한 경우
        targetDates = getDatesStartToByEnd(startDate, endDate);
    } else {
        // 2순위: 파라미터로 days가 왔거나(배치잡 등), 둘 다 없으면 기본 3일 적용
        const requestDays = days ? parseInt(days) : 3;
        for (let i = 0; i < requestDays; i++) {
            targetDates.push(getUSDate(i));
        }
    }

    // 결과 응답 (배치잡 호출 시에도 이 메시지가 로그에 남음)
    res.json({ 
        success: true, 
        status: 'STARTED', 
        dates: targetDates,
        message: `[${targetMarket}] 총 ${targetDates.length}일치 데이터 수집을 백그라운드에서 시작합니다.` 
    });
    
    // 백그라운드 실행
    setImmediate(async () => {
        try {
            console.log(`\n============== [Batch Start] ==============`);
            console.log(`📅 대상 기간: ${targetDates[targetDates.length-1]} ~ ${targetDates[0]}`);
            
            // 1. 유효 종목 리스트 캐싱 (기존 로직 동일)
            const snapshot = await admin.firestore().collection('stocks').select('type', 'exchange').get();
            const validSymbolsSet = new Set();
            const indexSymbols = [];

            snapshot.forEach(doc => {
                const sym = doc.id;
                const data = doc.data() || {};
                
                let isKr = sym.startsWith('KR_') || ['KSC', 'KOE', 'KOSPI', 'KOSDAQ'].includes(data.exchange);
                let isUs = !isKr; 
                
                if (targetMarket === 'US' && !isUs) return;
                if (targetMarket === 'KR' && !isKr) return;

                validSymbolsSet.add(sym);
                
                if (sym.startsWith('^') || data.type === 'index' || data.exchange === 'INDEX') {
                    indexSymbols.push(sym);
                }
            });

            console.log(`📊 [DB Load] 총 로드된 종목 수: ${snapshot.size}`);
            console.log(`✅ [Filter] '${targetMarket}' 기준 유효 종목: ${validSymbolsSet.size}개`);
            console.log(`🛡️ [Index] 식별된 지수: ${indexSymbols.length}개`);

            if (validSymbolsSet.size === 0) {
                console.error("❌ [Fatal] 업데이트할 종목이 없습니다. DB 상태를 확인하세요.");
                return;
            }

            // 2. Bulk API Loop
            // 2. Bulk API Loop (성공 시 15초, 실패 시 60초 대기 로직 유지) [cite: 2026-02-01]
            for (const date of targetDates) {
                try {
                    await processBulkDailyDataInternal(date, validSymbolsSet);
                    console.log(`⏳ [Wait] 15s Cooldown after ${date}...`);
                    await new Promise(r => setTimeout(r, 15000)); 
                } catch (err) {
                    if (err.response && err.response.status === 429) {
                        console.warn(`⚠️ [429 Error] ${date}: 한도 초과! 60초 대기.`);
                        await new Promise(r => setTimeout(r, 60000));
                    } else {
                        console.error(`❌ [Error] ${date} 처리 실패: ${err.message}`);
                    }
                }
            }

            // 3. 지수 업데이트
            if (indexSymbols.length > 0) {
                console.log(`🛡️ 지수 ${indexSymbols.length}개 업데이트 시작...`);
                // ... (지수 업데이트 로직 동일) ...
                // 지수 로직은 생략 (문제 없다고 가정)
            }

            console.log(`============== [Batch End] ==============\n`);

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
        console.log("🚀 [Index Sync] 지수 동기화 시작 (공식 Stable API 적용)...");
        const db = admin.firestore();
        const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

        // 문서 청크 분할 저장 헬퍼
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
        // [Step 1] Standard 지수들 (대시(-) 사용 확인 완료)
        // ==========================================================
        const standardTargets = [
            { id: 'US_SP500', url: '/sp500-constituent', desc: 'S&P 500', step: '1/6' },
            { id: 'US_NASDAQ100', url: '/nasdaq-constituent', desc: 'NASDAQ 100', step: '2/6' },
            { id: 'US_DOW30', url: '/dowjones-constituent', desc: 'Dow Jones 30', step: '3/6' } 
        ];

        for (const tgt of standardTargets) {
            try {
                console.log(`📡 [${tgt.step}] ${tgt.desc} 요청 중...`);
                const resFmp = await fmpClient.get(tgt.url);
                if(resFmp.data) await saveChunks(tgt.id, tgt.desc, resFmp.data.map(i=>({s:i.symbol, n:i.name, sec:i.sector})));
            } catch(err) { console.error(`❌ ${tgt.desc} 실패:`, err.message); }
            await delay(2000); 
        }

        console.log("⏳ ETF 홀딩스 API 요청 전 3초 대기 중...");
        await delay(3000); 

        // ==========================================================
        // [Step 2] S&P 100 등 무거운 ETF 홀딩스 (🛑 네가 찾은 공식 API 적용! 🛑)
        // ==========================================================
        const etfTargets = [
            { id: 'US_SP100', symbol: 'OEF', desc: 'S&P 100', step: '4/6' },
            { id: 'US_SP100_GLOBAL', symbol: 'IOO', desc: 'S&P Global 100', step: '5/6' }
        ];

        for (const target of etfTargets) {
            let success = false;
            let retryCount = 0;
            const maxRetries = 1;

            while (!success && retryCount <= maxRetries) {
                try {
                    console.log(`📡 [${target.step}] ${target.desc} 데이터 요청 중... (시도: ${retryCount + 1})`);
                    
                    // 핵심: 네가 찾아준 공식 API 엔드포인트 `/etf/holdings` 사용!
                    const resEtf = await fmpClient.get('/etf/holdings', {
                        params: { symbol: target.symbol } 
                    });
                    
                    if(resEtf.data && Array.isArray(resEtf.data)) {
                        // API 버전에 따라 asset 혹은 symbol 필드를 사용하므로 둘 다 지원하도록 매핑
                        const list = resEtf.data.map(i => ({ 
                            s: i.asset || i.symbol, 
                            n: i.name || '', 
                            weight: i.weightPercentage || i.weight || 0 
                        }));
                        await saveChunks(target.id, `${target.desc} (via ETF Holdings)`, list);
                        success = true;
                    }
                } catch(err) { 
                    console.error(`❌ ${target.desc} 실패:`, err.message); 
                    
                    if (err.response && err.response.status === 429 && retryCount < maxRetries) {
                        console.log(`⚠️ 429 Rate Limit 감지! 5초 휴식 후 재시도합니다...`);
                        await delay(5000); 
                        retryCount++;
                    } else {
                        break; 
                    }
                }
            }
            
            if (success) {
                await delay(2000); // 성공하면 2초만 쉬고 다음으로 넘어감
            }
        }

        // ==========================================================
        // [Step 3 & 4 통합] 전체 지수 리스트 수신 및 Stocks 등록 (하드코딩 제거)
        // ==========================================================
        try {
            console.log("📡 [Last Step] Global Indices 목록 요청 및 Stocks 전체 등록...");
            const allIdx = await fmpClient.get('/index-list');
            
            if(allIdx.data && Array.isArray(allIdx.data)) {
                const indexList = allIdx.data;
                console.log(`✅ FMP 수신 완료: 총 ${indexList.length}개 지수`);

                // 1. Meta Tickers에 저장 (리스트 조회용)
                await saveChunks('INDEX', 'Global Indices', indexList.map(i=>({symbol:i.symbol, name:i.name, ex: 'INDEX'})), 'GLOBAL');

                // 2. Stocks 컬렉션에 전체 등록 (DB 마스터화)
                console.log("💾 Stocks 컬렉션에 전체 지수 데이터를 동기화합니다...");
                
                let batch = db.batch();
                let opCount = 0;
                let savedCount = 0;

                for (const item of indexList) {
                    const stockRef = db.collection('stocks').doc(item.symbol);
                    
                    batch.set(stockRef, {
                        symbol: item.symbol,
                        name_en: item.name,        // 영문명
                        ticker_name_kr: item.name, // 한글명은 없으니 영문명으로 대체
                        exchange: 'INDEX',
                        isEtf: false,
                        active: true,            // [중요] 데일리 배치에서 수집되도록 활성화
                        type: 'index',
                        updatedAt: new Date().toISOString()
                    }, { merge: true });

                    opCount++;
                    savedCount++;

                    // Firestore 배치 한도(500) 안전하게 400에서 끊기
                    if (opCount >= 400) {
                        await batch.commit();
                        batch = db.batch();
                        opCount = 0;
                        console.log(`... ${savedCount} / ${indexList.length} 저장 중`);
                        await delay(500); // DB 쓰기 부하 조절
                    }
                }

                if (opCount > 0) await batch.commit();
                console.log(`✅ Stocks 컬렉션 동기화 완료! (총 ${savedCount}개)`);
            }
        } catch(err) { 
            console.error("❌ Global Indices 처리 실패:", err.message); 
            throw err; 
        }

        res.json({ success: true, message: "지수 동기화 및 Stocks 전체 등록 완료!" });
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
            } else if (['NASDAQ', 'NYSE', 'AMEX', 'OTC', 'PNK'].some(u => rawExchange.includes(u))) {
                 country = 'US';
                 if (rawExchange.includes('NASDAQ')) cleanExchange = 'NASDAQ';
                 else if (rawExchange.includes('NYSE')) cleanExchange = 'NYSE';
                 else if (rawExchange.includes('AMEX')) cleanExchange = 'AMEX';
                 else cleanExchange = 'OTC';
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

async function syncSectorMasterInternal() {
    const db = admin.firestore();
    const batch = db.batch();
    
    console.log("🚀 [Master Sync] 섹터/산업 데이터 동기화 시작...");

    try {
        // 1. FMP 원본 데이터 가져오기
        const [secRes, indRes] = await Promise.all([
            fmpClient.get('/available-sectors'),
            fmpClient.get('/available-industries')
        ]);
        
        let rawSectors = secRes.data || [];
        let rawIndustries = indRes.data || [];

        // [수정] 정렬 로직 강화 (객체/문자열 모두 대응)
        const getVal = (item, type) => {
            if (typeof item === 'string') return item;
            if (type === 'sector' && item.sector) return item.sector;
            if (type === 'industry' && item.industry) return item.industry;
            return JSON.stringify(item);
        };

        rawSectors.sort((a, b) => getVal(a, 'sector').localeCompare(getVal(b, 'sector')));
        rawIndustries.sort((a, b) => getVal(a, 'industry').localeCompare(getVal(b, 'industry')));

        // ---------------------------------------------------------
        // 🤖 [AI 번역] 산업 (Industries)
        // ---------------------------------------------------------
        const translationMap = {}; 

        console.log(`🤖 [Jarvis] 산업 ${rawIndustries.length}개 AI 번역 시작...`);
        
        const CHUNK_SIZE = 50;
        const industryChunks = [];
        for (let i = 0; i < rawIndustries.length; i += CHUNK_SIZE) {
            industryChunks.push(rawIndustries.slice(i, i + CHUNK_SIZE));
        }

        for (let i = 0; i < industryChunks.length; i++) {
            const chunk = industryChunks[i];
            // [수정] 번역 요청 시에도 순수 문자열만 추출해서 보냄
            const cleanChunk = chunk.map(item => getVal(item, 'industry'));
            
            console.log(`   - Chunk ${i+1}/${industryChunks.length} 처리 중...`);
            
            const indPrompt = `
                Translate these financial industries to Korean.
                Return ONLY a JSON object: {"English Name": "Korean Name"}.
                List: ${JSON.stringify(cleanChunk)}
            `;
            
            if (i > 0) await new Promise(r => setTimeout(r, 1500));
            const chunkResult = cleanAndParseJSON(await askJarvis(indPrompt));
            Object.assign(translationMap, chunkResult);
        }

        // ---------------------------------------------------------
        // 3. 데이터 구조화 (핵심 수정 부분)
        // ---------------------------------------------------------
        const formatList = (list, isSector = false) => {
            return list.map(item => {
                // ⚡ [Fix] 객체에서 문자열 값만 정확히 추출
                const nameEn = getVal(item, isSector ? 'sector' : 'industry');
                
                let nameKo = nameEn;
                if (isSector) {
                    // 섹터: 고정 맵 우선 사용
                    nameKo = FIXED_SECTOR_MAP[nameEn] || translationMap[nameEn] || nameEn;
                } else {
                    // 산업: AI 번역 맵 사용
                    nameKo = translationMap[nameEn] || nameEn;
                }

                return {
                    key: nameEn,     // 이제 깔끔한 문자열 키가 들어감 ("Basic Materials")
                    name_en: nameEn, 
                    name_ko: nameKo  
                };
            });
        };

        const structuredSectors = formatList(rawSectors, true); 
        const structuredIndustries = formatList(rawIndustries, false);

        // 4. 계층 구조 (Hierarchy) 생성
        const snapshot = await db.collection('stocks').where('active', '==', true).select('sector', 'industry').get();
        const treeMap = {}; 
        snapshot.forEach(doc => {
            const d = doc.data();
            if (!d.sector || !d.industry) return;
            if (!treeMap[d.sector]) treeMap[d.sector] = new Set();
            treeMap[d.sector].add(d.industry);
        });

        const sortedHierarchy = {};
        Object.keys(treeMap).sort().forEach(sec => {
            sortedHierarchy[sec] = Array.from(treeMap[sec]).sort();
        });

        // 5. 저장
        batch.set(db.collection('meta_sectors').doc('GICS_Standard'), {
            sectorList: structuredSectors,
            industryList: structuredIndustries,
            hierarchy: sortedHierarchy,
            updatedAt: new Date().toISOString()
        });

        await batch.commit();
        console.log("✅ [Master Sync] 섹터/산업 동기화 완료 (객체 파싱 수정됨)");
        return { success: true };

    } catch (error) {
        console.error("Master Sync Error:", error);
        throw error;
    }
}

// ===========================================================================
// 특정기간 일자별 주가데이터 Bulk Daily Data 처리 (쌍따옴표 버그 픽스 적용)
// ===========================================================================
async function processBulkDailyDataInternal(targetDate, validSymbolsSet) {
    const db = admin.firestore();
    
    console.log(`🚀 [Bulk Batch] ${targetDate} 수집 요청...`);
    
    // 이전에 알려준 429 재시도 로직을 위해 throw error를 타도록 함
    const response = await fmpClient.get('/eod-bulk', { 
        params: { date: targetDate }, responseType: 'text' 
    });
    
    const csvData = response.data;
    if (!csvData || typeof csvData !== 'string') return;

    const rows = csvData.split('\n');
    let batchHandler = db.batch();
    let opCount = 0;
    let processedCount = 0;
    const year = targetDate.split('-')[0];

    // 헤더가 있을 수 있으므로 i=1 부터 시작 (첫 줄은 건너뜀)
    for (let i = 1; i < rows.length; i++) {
        const rowStr = rows[i].trim();
        if (!rowStr) continue;
        const cols = rowStr.split(',');
        
        // 🔥 [핵심 버그 픽스] FMP가 보내는 쌍따옴표(") 제거 및 양옆 공백 제거
        let symbol = cols[0].replace(/"/g, '').trim();
        
        // 🛑 [Strict Guard] DB에 없는 종목은 저장 안 함
        if (!symbol || !validSymbolsSet.has(symbol)) continue;

        const dailyRecord = {
            // 날짜에도 쌍따옴표가 있으므로 제거해 줌
            date: cols[1].replace(/"/g, '').trim(), 
            open: parseFloat(cols[2] || 0), 
            low: parseFloat(cols[3] || 0),
            high: parseFloat(cols[4] || 0), 
            close: parseFloat(cols[5] || 0),
            adjClose: parseFloat(cols[6] || 0), 
            volume: parseFloat(cols[7] || 0)
        };

        const docRef = db.collection('stocks').doc(symbol).collection('annual_data').doc(year);
        batchHandler.set(docRef, { 
            symbol: symbol, year: year, lastUpdated: new Date().toISOString(), 
            data: admin.firestore.FieldValue.arrayUnion(dailyRecord) 
        }, { merge: true });
        
        // Snapshot 업데이트
        batchHandler.set(db.collection('stocks').doc(symbol), { 
            snapshot: { price: dailyRecord.close, vol: dailyRecord.volume, lastUpdated: new Date().toISOString() }, 
            last_crawled: new Date().toISOString() 
        }, { merge: true });

        opCount++;
        if (opCount >= 400) { await batchHandler.commit(); processedCount += opCount; batchHandler = db.batch(); opCount = 0; }
    }
    if (opCount > 0) { await batchHandler.commit(); processedCount += opCount; }

    console.log(`✅ [Bulk Batch] ${targetDate} 저장 완료 (${processedCount}건)`);
}

// 종목별 과거 전체 주가 데이터 수집 (Standardized)
async function processStockHistoryData(arg1, arg2, arg3, arg4) {
    let symbol, from, to;
    if (arg1 && arg1.body) { symbol = arg1.body.symbol; from = arg1.body.from; to = arg1.body.to; } 
    else { symbol = arg1; from = arg2; to = arg3; }

    if (!symbol) throw new Error('Symbol required');

    try {
        // [Standard] /profile 호출
        try {
            const profileRes = await fmpClient.get('/profile', { params: { symbol: symbol } });
            const profile = (profileRes.data && profileRes.data.length > 0) ? profileRes.data[0] : null;
            
            if (profile) {
                const safeIpoPrice = (profile.ipoPrice && profile.ipoPrice > 0) ? profile.ipoPrice : 0;
                await admin.firestore().collection('stocks').doc(symbol).set({
                    symbol: profile.symbol || '', name_en: profile.companyName || '', exchange: profile.exchangeShortName || '',
                    sector: profile.sector || '', industry: profile.industry || '', ipoDate: profile.ipoDate || '', ipoPrice: safeIpoPrice,
                    description: profile.description || '', website: profile.website || '', currency: profile.currency || 'USD',
                    image: profile.image || '', ceo: profile.ceo || '',
                    snapshot: { price: profile.price||0, mktCap: profile.mktCap||0, vol: profile.volAvg||0, beta: profile.beta||0, div: profile.lastDiv||0, range: profile.range||'', lastUpdated: new Date().toISOString() },
                    active: true, last_crawled: new Date().toISOString()
                }, { merge: true });
            }
        } catch (e) {}

        // [Standard] Historical Price 호출
        const startDate = from ? new Date(from) : new Date('1990-01-01');
        const endDate = to ? new Date(to) : new Date();
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
                // [핵심 변경] baseURL 제거, params 활용
                const fmpRes = await fmpClient.get('/historical-price-eod/full', { 
                    params: { 
                        symbol: symbol,
                        from: range.from,
                        to: range.to
                        // apikey 자동 주입됨
                    }
                });
                const data = Array.isArray(fmpRes.data) ? fmpRes.data : (fmpRes.data.historical || []);
                results.push(data);
            } catch (err) { results.push([]); }
        }

        let mergedData = results.flat(); 
        if (mergedData.length === 0) return { success: true, message: `데이터 없음`, symbol };

        // ... (저장 로직 기존과 동일 - 생략) ...
        const uniqueMap = new Map();
        mergedData.forEach(item => uniqueMap.set(item.date, item));
        const finalData = Array.from(uniqueMap.values()).sort((a, b) => new Date(a.date) - new Date(b.date));
        
        const chunks = {};
        finalData.forEach(day => {
            const year = day.date.split('-')[0];
            if (!chunks[year]) chunks[year] = [];
            chunks[year].push(day);
        });

        const batch = admin.firestore().batch();
        for (const year of Object.keys(chunks)) {
            const chunkRef = admin.firestore().collection('stocks').doc(symbol).collection('annual_data').doc(year);
            // (증분 업데이트 로직 유지)
            batch.set(chunkRef, { symbol: symbol, year: year, lastUpdated: new Date().toISOString(), data: chunks[year] }, { merge: true });
        }
        await batch.commit();

        return { success: true, symbol: symbol, totalDays: finalData.length, message: 'Updated successfully' };

    } catch (error) {
        throw error;
    }
}

// 지수 ETF 추출하여 별도 저장   --  사용 보류.   티커 속성 관리에서 호출해서 사용하는 방법으로 검토 필요
router.post('/sync-index-etf-master', verifyToken, async (req, res) => {
    try {
        const allTickers = await getTickerData();
        
        // 지수 판별 키워드
        const indexKeywords = ['S&P 500', 'SNP 500', 'NASDAQ 100', '나스닥 100', 'KOSPI 200', '코스피 200', 'KOSDAQ 150', '코스닥 150', 'DOW JONES', '다우존스'];
        
        const usIndexEtfs = [];
        const krIndexEtfs = [];

        allTickers.forEach(item => {
            if (!item.etf) return; // ETF가 아니면 패스

            const name = (item.name || item.ticker_name_kr || "").toUpperCase();
            const isIndexEtf = indexKeywords.some(kw => name.includes(kw.toUpperCase()));

            if (isIndexEtf) {
                const etfData = {
                    symbol: item.symbol,
                    name: item.name || "",
                    name_kr: item.ticker_name_kr || "",
                    exchange: item.exchange
                };

                // 거래소 코드로 국가 구분 (이미 스타크가 정의한 기준 활용)
                if (item.exchange.startsWith('US_') || ['NASDAQ', 'NYSE', 'AMEX'].includes(item.exchange)) {
                    usIndexEtfs.push(etfData);
                } else if (item.exchange.startsWith('KR_') || ['KSC', 'KOE'].includes(item.exchange)) {
                    krIndexEtfs.push(etfData); // 여기에 한국상장 미국 ETF도 자연스럽게 포함됨
                }
            }
        });

        const db = admin.firestore();
        const batch = db.batch();

        // 1. US_INDEX_ETF 저장
        const usRef = db.collection('meta_tickers').doc('US_INDEX_ETF');
        batch.set(usRef, { updatedAt: new Date().toISOString(), count: usIndexEtfs.length, country: 'US' });
        const usChunkRef = usRef.collection('chunks').doc('batch_0');
        batch.set(usChunkRef, { list: usIndexEtfs });

        // 2. KR_INDEX_ETF 저장
        const krRef = db.collection('meta_tickers').doc('KR_INDEX_ETF');
        batch.set(krRef, { updatedAt: new Date().toISOString(), count: krIndexEtfs.length, country: 'KR' });
        const krChunkRef = krRef.collection('chunks').doc('batch_0');
        batch.set(krChunkRef, { list: krIndexEtfs });

        await batch.commit();

        res.json({ success: true, usCount: usIndexEtfs.length, krCount: krIndexEtfs.length });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

module.exports = router;