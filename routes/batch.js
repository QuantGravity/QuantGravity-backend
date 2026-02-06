// ===========================================================================
// [파일명] : routes/batch.js
// [대상]   : 대량 데이터 처리 및 시스템 자동화 배치 작업
// [기준]   : 
//   1. 타임아웃 방지: 무거운 작업은 req.setTimeout(600000) 등을 적극 활용한다.
//   2. 부하 조절: Throttling(BATCH_SIZE)을 적용하여 서버 및 DB 과부하를 방지한다.
//   3. 재처리 보장: 작업 실패 시 해당 종목의 상태를 'ERROR'로 기록하여 추적 가능하게 한다.
//   4. 메모리 관리: 대량 데이터 처리 시 Promise.all보다는 순차(for-of) 처리를 권장한다.
// ===========================================================================
const express = require('express');
const router = express.Router();
const admin = require('firebase-admin');
const firestore = admin.firestore();
const fmpClient = require('../utils/fmpClient'); 
const { verifyToken } = require('../utils/authHelper');
const { logTraffic } = require('../utils/logger');
const { getDaysDiff } = require('../utils/math');
const { performAnalysisInternal } = require('../utils/analysisEngine');
const { processHybridData, getTickerData } = require('../utils/stockHelper'); 

// ============================================================
// [유틸리티] 전체 종목 코드 추출
// ============================================================
router.get('/get-all-symbols', verifyToken, async (req, res) => {
    try {
        const uniqueSymbols = await getTickerData({ justList: true });
        res.json({ success: true, count: uniqueSymbols.length, symbols: uniqueSymbols });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [Batch] 데일리 업데이트 (안전한 병렬 처리 모드 - Rate Limit 준수)
// ============================================================
router.post('/daily-update-all', async (req, res) => {
    try {
        console.log("🚀 [Safe Batch] 일괄 업데이트 시작 (속도 조절 모드)...");

        // 1. 날짜 설정 (최근 5일)
        const today = new Date();
        const toDate = today.toISOString().split('T')[0]; 
        const pastDate = new Date();
        pastDate.setDate(pastDate.getDate() - 5); 
        const fromDate = pastDate.toISOString().split('T')[0];

        // 2. 전체 종목 가져오기
        const symbols = await getTickerData({ justList: true }); 
        
        // 타임아웃 방지용 선응답
        res.status(200).json({ 
            status: 'STARTED', 
            mode: 'THROTTLED_CHUNK',
            dates: targetDates, // ★ [복구 완료] 이 줄이 빠져서 에러가 났던 거야!
            total: symbols.length,
            message: `전체 ${symbols.length}개 종목 업데이트가 안전 모드로 시작되었습니다.` 
        });

        // 3. 비동기 백그라운드 처리
        (async () => {
            let successCount = 0;
            let failCount = 0;
            
            // ★ [최종 튜닝] 429 에러 방지를 위해 더 안전하게 축소
            // 5개 * 2회 호출 = 10 request / 1.0초 = 분당 600회 (750회 한도 대비 안전)
            const CHUNK_SIZE = 5;  // 기존 10 -> 5로 변경
            
            console.log(`>> 작업 시작: ${fromDate} ~ ${toDate} (${symbols.length}개)`);

            for (let i = 0; i < symbols.length; i += CHUNK_SIZE) {
                const chunk = symbols.slice(i, i + CHUNK_SIZE);
                
                // 5개를 병렬로 실행
                const promises = chunk.map(symbol => 
                    processHybridData(symbol, fromDate, toDate, 'System_Batch')
                        .then(() => ({ status: 'ok' }))
                        .catch(err => ({ status: 'fail', symbol, err }))
                );

                const results = await Promise.all(promises);

                results.forEach(r => {
                    if (r.status === 'ok') successCount++;
                    else {
                        failCount++;
                        // 에러 로그 출력
                        console.error(`❌ [${r.symbol}] 실패: ${r.err.message}`);
                    }
                });

                // 진행률 로깅 (100개 단위로 자주 찍어서 확인)
                if ((i + CHUNK_SIZE) % 100 === 0) {
                    console.log(`... 진행률: ${Math.min(i + CHUNK_SIZE, symbols.length)}/${symbols.length} (성공 ${successCount})`);
                }

                // ★ [속도 제한] 1초 대기
                await new Promise(r => setTimeout(r, 1000));
            }

            console.log(`🏁 [Safe Batch] 작업 최종 종료 (성공: ${successCount}, 실패: ${failCount})`);
            
            await db.collection('system_logs').add({
                type: 'DAILY_BATCH_PARALLEL',
                status: 'COMPLETED',
                success: successCount,
                fail: failCount,
                date: new Date().toISOString()
            });

        })();

    } catch (error) {
        console.error("Batch Error:", error);
        if (!res.headersSent) res.status(500).json({ error: error.message });
    }
});

// 라우터 설정 (기존 express router에 추가)
// 호출 예시: POST /batch/update-stats { "tickers": ["AAPL", "TSLA"] }
router.post('/update-stats', async (req, res) => {
    const tickers = req.body.tickers || [];
    
    // 비동기 병렬 처리 (주의: 너무 많이 한꺼번에 돌리면 메모리 터질 수 있으니 5~10개씩 끊어서 처리 권장)
    for (const ticker of tickers) {
        await updateStockStats(ticker);
    }
    
    res.json({ result: 'Batch triggered successfully', count: tickers.length });
});

// ============================================================
// [Batch] 전체 종목 병렬 분석 및 Market Map 데이터 생성 (최적화 버전)
// ============================================================
router.post('/analyze-all-tickers', verifyToken, logTraffic, async (req, res) => {
    // 타임아웃 방지 (분석량이 많으므로 10분 설정)
    req.setTimeout(600000); 

    try {
        console.log("🚀 [Batch] 전체 종목 병렬 분석 시작...");

        const snapshot = await firestore.collection('tickers').get();
        const targetDocs = snapshot.docs;
        const totalDocs = targetDocs.length;
        
        console.log(`📋 분석 대상: 총 ${totalDocs}개 종목`);

        // ============================================================
        // [비즈니스 로직] 등급별 처리 속도 설정 (전략 분석 로직 참조)
        // ============================================================
        const userRole = req.user.role || 'G1';
        const isVip = ['G9', 'admin'].includes(userRole);

        // VIP: 한 번에 30개씩 (분석 부하 고려), 일반: 5개씩
        const BATCH_SIZE = isVip ? 30 : 5; 
        const DELAY_MS = isVip ? 0 : 500; 
        
        const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
        const today = new Date().toISOString().split('T')[0];
        const summaryList = []; 
        let successCount = 0;
        let failCount = 0;

        // ============================================================
        // [핵심 로직] 배치 단위 병렬 실행 (Throttling 적용)
        // ============================================================
        for (let i = 0; i < totalDocs; i += BATCH_SIZE) {
            // 현재 처리할 묶음 (Chunk)
            const chunk = targetDocs.slice(i, i + BATCH_SIZE);

            // 해당 묶음 병렬 실행
            const promises = chunk.map(async (doc) => {
                const tickerData = doc.data();
                const ticker = doc.id;

                try {
                    // 분석 함수 호출 (백엔드 공통 함수 사용)
                    const result = await performAnalysisInternal(ticker, '1990-01-01', today, 10, 5);

                    if (result.error) {
                        console.warn(`⚠️ [Skip] ${ticker}: ${result.error}`);
                        return null;
                    }

                    // [저장 1] 상세 데이터 저장 (비동기 처리)
                    firestore.collection('analysis_results').doc(ticker).set(result)
                        .catch(e => console.error(`상세 저장 실패(${ticker}):`, e));

                    // 요약 데이터 반환
                    return {
                        ticker: ticker,
                        name_kr: tickerData.ticker_name_kr || ticker,
                        sector: tickerData.sector || 'Etc',
                        period_start: result.period.start,
                        period_end: result.period.end,
                        listing_days: getDaysDiff(result.period.start, result.period.end),
                        listing_years: (getDaysDiff(result.period.start, result.period.end) / 365).toFixed(1),
                        cagr: result.periodCagrs['total'],
                        mdd: result.dd.max,
                        r10_min: result.rolling.r10.min,
                        r10_med: result.rolling.r10.med,
                        r10_max: result.rolling.r10.max,
                        recovery_max: result.recovery.max,
                        recovery_avg: result.recovery.avg,
                        updatedAt: new Date().toISOString()
                    };

                } catch (innerErr) {
                    console.error(`💥 [Error] ${ticker} 처리 중 예외 발생:`, innerErr);
                    return null;
                }
            });

            // 현재 배치 완료 대기
            const results = await Promise.all(promises);
            
            // 결과 수집
            results.forEach(res => {
                if (res) {
                    summaryList.push(res);
                    successCount++;
                } else {
                    failCount++;
                }
            });

            console.log(`.. 진행률: ${Math.min(i + BATCH_SIZE, totalDocs)}/${totalDocs} 완료 (성공: ${successCount})`);

            // VIP가 아니고 다음 배치가 있다면 지연 시간 부여 (서버 부하 방지)
            if (i + BATCH_SIZE < totalDocs && DELAY_MS > 0) {
                await sleep(DELAY_MS);
            }
        }

        // [저장 2] Market Map 스냅샷 저장 (Batch 사용)
        if (summaryList.length > 0) {
            const SNAPSHOT_CHUNK_SIZE = 500;
            const totalSnapshotChunks = Math.ceil(summaryList.length / SNAPSHOT_CHUNK_SIZE);
            const batch = firestore.batch();

            for (let i = 0; i < totalSnapshotChunks; i++) {
                const chunk = summaryList.slice(i * SNAPSHOT_CHUNK_SIZE, (i + 1) * SNAPSHOT_CHUNK_SIZE);
                const docRef = firestore.collection('market_map_snapshot').doc(`batch_${i}`);
                
                batch.set(docRef, {
                    batch_index: i,
                    total_batches: totalSnapshotChunks,
                    updated_at: admin.firestore.FieldValue.serverTimestamp(),
                    tickers: chunk
                });
            }

            await batch.commit();
            console.log("✅ [Batch] 모든 데이터 저장 완료!");

            res.json({ 
                success: true, 
                analyzed: successCount, 
                failed: failCount, 
                snapshot_chunks: totalSnapshotChunks 
            });
        } else {
            res.json({ success: false, message: "분석된 데이터가 없습니다." });
        }

    } catch (err) {
        console.error("🔥 [Batch Critical Error]", err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================
// [신규 API] 배치 작업용: 지수(^) 분석 실행 및 DB 저장
// ============================================================
router.post('/analyze-indices', async (req, res) => {
    try {
        console.log("[Batch] 지수 분석 및 캐싱 시작...");
        
        // 1. 모든 티커 가져오기
        const snapshot = await firestore.collection('tickers').get();
        // 2. '^'로 시작하는 지수만 필터링
        const indexTickers = snapshot.docs
            .map(doc => doc.id)
            .filter(id => id.startsWith('^'));

        const today = new Date().toISOString().split('T')[0];
        const results = [];

        // 3. 각 지수별 분석 실행 및 저장
        for (const ticker of indexTickers) {
            console.log(`[Batch] 분석 중: ${ticker}`);
            // 기본값: 1980년부터 오늘까지, Rolling 10년/5년
            const analysisResult = await performAnalysisInternal(ticker, '1980-01-01', today, 10, 5);

            if (!analysisResult.error) {
                // DB에 'analysis_cache' 컬렉션에 저장 (용량 절약을 위해 history는 제외할 수도 있으나, 차트를 위해 포함)
                await firestore.collection('analysis_cache').doc(ticker).set(analysisResult);
                results.push(ticker);
            }
        }

        console.log(`[Batch] 총 ${results.length}개 지수 분석 완료`);
        res.json({ success: true, count: results.length, tickers: results });

    } catch (err) {
        console.error("[Batch Error]", err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================================
// [기능 4] Market Map용 경량화 & 보안 요약 데이터 생성 (v3.0)
// ============================================================
router.post('/generate-market-map-summary', verifyToken, async (req, res) => {
    // 대량 데이터 처리라 시간이 좀 걸릴 수 있음 (10분 제한)
    req.setTimeout(600000); 

    try {
        console.log("🗺️ [Batch] Market Map 요약 데이터 생성 시작...");
        const db = admin.firestore();
        
        // 1. 대상 종목 가져오기 (상장폐지 안 된 'active' 종목만)
        const snapshot = await db.collection('stocks')
            .where('active', '==', true)
            .get();

        if (snapshot.empty) {
            return res.json({ success: false, message: "대상 종목이 없습니다." });
        }

        console.log(`📋 전체 Active 종목 수: ${snapshot.size}개`);

        const summaryItems = [];
        const currentYear = new Date().getFullYear();

        // [보안/압축 함수] 소수점을 버리고 정수로 변환 (해킹 방지 & 용량 절약)
        const compress = (val, type) => {
            if (val === undefined || val === null || isNaN(val)) return 0;
            
            // 1. 시가총액(MC): 백만달러 단위 -> 10억달러(B) 단위 정수로 변환
            // 예: 15,400M (154억불) -> 15
            if (type === 'MC') return Math.round(val / 1000); 

            // 2. MDD: 5% 단위로 퉁치기 (구간화)
            // 예: -12.5% -> -10, -18% -> -15
            if (type === 'MDD') return Math.floor(Math.round(val) / 5) * 5;

            // 3. 일반 수익률(CAGR, ROE 등): 소수점 날리고 반올림
            // 예: 15.43% -> 15
            return Math.round(val);
        };

        // 2. 데이터 정제 (Loop)
        snapshot.docs.forEach(doc => {
            const data = doc.data();
            const stats = data.stats || {}; // stats가 없으면 빈 객체

            // 필수 데이터(IPO날짜, 시총)가 없으면 제외 (노이즈 방지)
            if (!data.symbol || !data.ipoDate || !stats.market_cap) return;
            
            // 너무 작은 잡주(Penny Stock) 제외 (예: 시총 100억원 미만)
            // if (stats.market_cap < 10000000) return; 

            // 상장 경과 년수 계산
            const ipoYear = parseInt(data.ipoDate.split('-')[0]);
            const listingYears = currentYear - ipoYear;
            if (isNaN(listingYears)) return;

            // [핵심] 필드명을 알파벳 하나로 줄여서 전송량 30% 절약
            summaryItems.push({
                s: data.symbol,                                   // 티커
                n: data.name_kr || data.name_en || data.symbol,   // 이름 (툴팁용)
                ex: data.exchange,                                // 거래소 (필터링용)
                sec: data.sector || 'Etc',                        // 섹터 (색상 구분용)
                
                // --- X, Y축 후보군 (전부 정수형으로 변환됨) ---
                y: listingYears,                                  // 상장년수
                mc: compress(stats.market_cap, 'MC'),             // 시가총액 (Size)
                
                p: compress(stats.price_cagr_10y),                // 주가 CAGR (10y)
                e: compress(stats.eps_cagr_10y),                  // EPS CAGR (10y)
                r: compress(stats.rev_cagr_10y),                  // 매출 성장률 (10y)
                roe: compress(stats.avg_roe_5y),                  // ROE (5y Avg)
                mdd: compress(stats.mdd, 'MDD'),                  // MDD (Max Drawdown)
                per: compress(stats.per_current)                  // PER
            });
        });

        console.log(`✨ 유효 데이터 추출 완료: ${summaryItems.length}개`);

        // 3. 청크(Chunk) 분할 저장 
        // Firestore 문서 하나당 1MB 제한이 있으므로, 2000개씩 잘라서 저장
        const CHUNK_SIZE = 2000;
        const totalChunks = Math.ceil(summaryItems.length / CHUNK_SIZE);
        const batchHandler = db.batch();

        // (1) 메타 정보 저장 (버전 관리용)
        const metaRef = db.collection('market_map').doc('summary_v1');
        batchHandler.set(metaRef, {
            updatedAt: new Date().toISOString(),
            totalCount: summaryItems.length,
            chunkCount: totalChunks,
            version: "3.0"
        });

        // (2) 실제 데이터 청크 저장
        for (let i = 0; i < totalChunks; i++) {
            const chunk = summaryItems.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
            const chunkRef = metaRef.collection('shards').doc(`batch_${i}`);
            
            batchHandler.set(chunkRef, {
                index: i,
                items: chunk
            });
        }

        await batchHandler.commit();
        console.log(`💾 저장 완료! (총 ${totalChunks}개 청크)`);

        res.json({ 
            success: true, 
            count: summaryItems.length, 
            chunks: totalChunks,
            message: "Market Map 데이터 생성 완료"
        });

    } catch (err) {
        console.error("Market Map Summary Error:", err);
        res.status(500).json({ error: err.message });
    }
});

module.exports = router;