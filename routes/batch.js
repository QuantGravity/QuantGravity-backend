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
const { verifyToken, verifyBatchOrAdmin } = require('../utils/authHelper');
const { logTraffic } = require('../utils/logger');
const { performAnalysisInternal } = require('../utils/analysisEngine');
const { getDaysDiff } = require('../utils/math');
const StatsEngine = require('../utils/statsCalculator'); // 🌟 [추가] 공통 계산 엔진

// 날짜 배열 유틸리티 함수
function getDatesInRange(startStr, endStr) {
    const dates = [];
    let current = new Date(startStr);
    const end = new Date(endStr);
    while (current <= end) {
        dates.push(current.toISOString().split('T')[0]);
        current.setDate(current.getDate() + 1);
    }
    return dates;
}

// 🌐 미국 타임존을 적용하여 오늘 날짜(YYYY-MM-DD)를 구하는 함수 (US 전용)
function getTodayUS() {
    const timeZone = 'America/New_York';
    const formatter = new Intl.DateTimeFormat('en-CA', { 
        timeZone: timeZone, 
        year: 'numeric', month: '2-digit', day: '2-digit' 
    });
    return formatter.format(new Date());
}

// 종목 통계데이터 집계 (배치 잡 및 수동 호출 대응)
router.post('/update-stats', verifyBatchOrAdmin, async (req, res) => {
    try {
        // [수정] 배치 잡(Github Actions)에서 넘어오는 최소한의 payload 대응 보강 (US 고정)
        const { startSymbol, endSymbol, startDate, endDate } = req.body;
        let tickers = req.body.tickers || [];
        
        // 1. 대상 종목 리스트 확보
        if (tickers.length === 0) {
            console.log(`👉 [Batch] US 대상 종목 리스트 확보 중...`);
            
            // 🌟 US 국가 종목만 쿼리
            let query = firestore.collection('stocks')
                .where('active', '==', true)
                .where('country', '==', 'US');

            const snapshot = await query.select().get();
            tickers = snapshot.docs.map(doc => doc.id);
            
            // 🌟 미국 핵심 지수 강제 포함
            const coreIndices = ['^GSPC', '^IXIC', '^NDX', '^DJI', '^RUT', '^VIX', '^W5000']; 
            
            coreIndices.forEach(idx => {
                if (!tickers.includes(idx)) tickers.push(idx);
            });

            // 배열 정렬 및 심볼 범위 필터링
            tickers.sort();
            if (startSymbol) tickers = tickers.filter(t => t >= startSymbol);
            if (endSymbol) tickers = tickers.filter(t => t <= endSymbol);
        }

        // [수정] 배치 잡 구동 시 startDate가 없으면 뉴욕 타임존 '오늘'로 자동 세팅
        const todayStr = getTodayUS();
        const actualStartDate = startDate || todayStr;
        const actualEndDate = endDate || actualStartDate;
        
        const targetDates = getDatesInRange(actualStartDate, actualEndDate);

        // 배치 잡은 즉시 응답을 받고 백그라운드에서 동작해야 하므로 응답 먼저 전송
        res.json({ result: 'Batch triggered (Background)', count: tickers.length, dates: targetDates.length });

        setImmediate(async () => {
            console.log(`🚀 [Batch] 통계 업데이트 시작 (국가: US, 기간: ${targetDates[0]}~${targetDates[targetDates.length-1]})`);
            
            // 2. 데이터 준비 (테마, 산업/섹터 정보 등)
            const themeMap = {};
            const themesSnap = await firestore.collection('market_themes').get();
            themesSnap.forEach(doc => {
                const d = doc.data();
                if (d.tickers && Array.isArray(d.tickers)) {
                    d.tickers.forEach(t => {
                        if (!themeMap[t.symbol]) themeMap[t.symbol] = [];
                        themeMap[t.symbol].push(doc.id);
                    });
                }
            });

            const stockMasterInfo = {};
            const stocksSnap = await firestore.collection('stocks')
                .where('active', '==', true)
                .where('country', '==', 'US')
                .get();

            stocksSnap.forEach(doc => {
                const data = doc.data();
                stockMasterInfo[doc.id] = { 
                    industry: data.industry || '',
                    sector: data.sector || '',
                    isEtf: data.isEtf === true
                };
            });

            const periods = [5, 10, 20, 40, 60, 120, 240, 480, 'all'];

            // 3. 날짜별 루프 시작
            for (const targetDate of targetDates) {
                const dayOfWeek = new Date(targetDate).getDay(); 
                if (dayOfWeek === 0 || dayOfWeek === 6) {
                    console.log(`⏭️ [Skip] ${targetDate} : 주말입니다.`);
                    continue; 
                }

                // 휴장일 체크 로직 (미국 S&P 500 기준)
                const targetYear = parseInt(targetDate.split('-')[0]);
                const benchmarkTicker = '^GSPC'; // 🌟 미국 벤치마크 고정
                
                try {
                    const bmRef = firestore.collection('stocks').doc(benchmarkTicker)
                                           .collection('annual_data').doc(String(targetYear));
                    const bmSnap = await bmRef.get();

                    if (bmSnap.exists) {
                        const bmData = bmSnap.data().data || [];
                        const isMarketOpen = bmData.some(d => d.date === targetDate);
                        
                        if (!isMarketOpen) {
                            console.log(`⏭️ [Skip] ${targetDate} : 휴장일입니다. (${benchmarkTicker} 데이터 없음)`);
                            continue;
                        }
                    }
                } catch (bmError) {
                    console.warn(`⚠️ [Check] 휴장일 확인 중 에러 발생: ${bmError.message}`);
                }
                
                const requiredYears = [String(targetYear), String(targetYear - 1), String(targetYear - 2)];
                const docId = `${targetDate}_US`;
                const docRef = firestore.collection('meta_ticker_stats').doc(docId);
                
                let successCount = 0;
                let chunkIndex = 0;
                const WRITE_CHUNK_SIZE = 100;

                const groupDailyAgg = {}; 

                for (let i = 0; i < tickers.length; i += WRITE_CHUNK_SIZE) {
                    const chunkTickers = tickers.slice(i, i + WRITE_CHUNK_SIZE);
                    const batchData = {}; 

                    for (let j = 0; j < chunkTickers.length; j += 100) {
                        const subTickers = chunkTickers.slice(j, j + 100);
                        
                        await Promise.all(subTickers.map(async (ticker) => {
                            try {
                                const yearPromises = requiredYears.map(y => 
                                    firestore.collection('stocks').doc(ticker).collection('annual_data').doc(y).get()
                                );
                                const yearDocs = await Promise.all(yearPromises);

                                let combinedHistory = [];
                                yearDocs.forEach(doc => {
                                    if (doc.exists) {
                                        const dataList = doc.data().data || []; 
                                        if (Array.isArray(dataList)) combinedHistory = combinedHistory.concat(dataList);
                                    }
                                });

                                // 데이터 날짜 최신순 정렬 필수
                                combinedHistory.sort((a, b) => new Date(b.date) - new Date(a.date));
                                if (combinedHistory.length === 0) return;

                                const masterInfo = stockMasterInfo[ticker] || { industry: '', sector: '', isEtf: false };

                                // 🌟🌟🌟 [핵심] 공통 계산 엔진 호출 🌟🌟🌟
                                const stats = StatsEngine.calculateDailyStats(ticker, combinedHistory, targetDate, masterInfo);

                                if (!stats) return; // 계산 실패(데이터 없음 등) 시 패스

                                // 테마 정보는 공통 계산기 밖에서 주입
                                stats.themes = themeMap[ticker] || [];

                                batchData[ticker] = stats;
                                successCount++;

                                const isEtf = masterInfo.isEtf;
                                const isIndex = ticker.startsWith('^');

                                if (!isEtf && !isIndex) {
                                    const ctry = 'US'; // 🌟 US 고정
                                    const sec = masterInfo.sector;
                                    const ind = masterInfo.industry;

                                    const groupsToUpdate = [];
                                    groupsToUpdate.push({ key: ctry, type: 'COUNTRY', name: ctry, parentSectorKey: null });
                                    if (sec) groupsToUpdate.push({ key: `${ctry}_${sec}`, type: 'SECTOR', name: sec, parentSectorKey: `${ctry}_${sec}` });
                                    if (sec && ind) groupsToUpdate.push({ key: `${ctry}_${ind}`, type: 'INDUSTRY', name: ind, parentSectorKey: `${ctry}_${sec}` });

                                    groupsToUpdate.forEach(meta => {
                                        const gKey = meta.key;
                                        if (!groupDailyAgg[gKey]) {
                                            groupDailyAgg[gKey] = { total: 0, new_high: {}, new_low: {}, meta: meta };
                                            periods.forEach(p => {
                                                const pKey = p === 'all' ? 'all' : `${p}d`;
                                                groupDailyAgg[gKey].new_high[pKey] = 0;
                                                groupDailyAgg[gKey].new_low[pKey] = 0;
                                            });
                                        }
                                        
                                        groupDailyAgg[gKey].total++;
                                        periods.forEach(p => {
                                            const pKey = p === 'all' ? 'all' : `${p}d`;
                                            if (stats.is_new_high[pKey]) groupDailyAgg[gKey].new_high[pKey]++;
                                            if (stats.is_new_low[pKey]) groupDailyAgg[gKey].new_low[pKey]++;
                                        });
                                    });
                                }

                            } catch (e) {
                                console.error(`❌ ${ticker} 집계 에러:`, e.message);
                            }
                        }));
                    }

                    if (Object.keys(batchData).length > 0) {
                        try {
                            await docRef.collection('chunks').doc(`batch_${chunkIndex}`).set(batchData);
                            chunkIndex++;
                        } catch (saveError) {
                            console.error(`❌ [Batch Error] 개별 종목 저장 실패:`, saveError.message);
                        }
                    }
                }
                
                await docRef.set({
                    date: targetDate,
                    country: 'US',
                    isChunked: true,
                    chunkCount: chunkIndex,
                    totalCount: successCount,
                    updatedAt: new Date().toISOString()
                }, { merge: true });

                if (Object.keys(groupDailyAgg).length > 0) {
                    const sectorBundles = {};
                    const countryTotalBundle = {}; 

                    for (const [groupKey, groupData] of Object.entries(groupDailyAgg)) {
                        const rates = { totalCount: groupData.total, new_high_rate: {}, new_low_rate: {} };
                        periods.forEach(p => {
                            const pKey = p === 'all' ? 'all' : `${p}d`;
                            rates.new_high_rate[pKey] = groupData.total > 0 ? parseFloat(((groupData.new_high[pKey] / groupData.total) * 100).toFixed(2)) : 0;
                            rates.new_low_rate[pKey] = groupData.total > 0 ? parseFloat(((groupData.new_low[pKey] / groupData.total) * 100).toFixed(2)) : 0;
                        });
                        
                        const meta = groupData.meta;
                        const parentKey = meta.parentSectorKey;
                        const myName = meta.name;

                        if (meta.type === 'INDUSTRY') {
                            if (!sectorBundles[parentKey]) sectorBundles[parentKey] = {};
                            if (!sectorBundles[parentKey][myName]) sectorBundles[parentKey][myName] = {};
                            sectorBundles[parentKey][myName][targetDate] = rates;
                        } else if (meta.type === 'SECTOR') {
                            if (!sectorBundles[parentKey]) sectorBundles[parentKey] = {};
                            if (!sectorBundles[parentKey]['_SECTOR_TOTAL_']) sectorBundles[parentKey]['_SECTOR_TOTAL_'] = {};
                            sectorBundles[parentKey]['_SECTOR_TOTAL_'][targetDate] = rates;
                        } else if (meta.type === 'COUNTRY') {
                            countryTotalBundle[targetDate] = rates;
                        }
                    }

                    const batch = firestore.batch();

                    for (const [sectorKey, industriesData] of Object.entries(sectorBundles)) {
                        const sDocId = `${sectorKey}_${targetYear}`; 
                        const sDocRef = firestore.collection('meta_sector_stats').doc(sDocId);
                        batch.set(sDocRef, { country: 'US', year: String(targetYear), updatedAt: new Date().toISOString(), industries: industriesData }, { merge: true });
                    }

                    const countryDocRef = firestore.collection('meta_sector_stats').doc(`US_Total_${targetYear}`);
                    batch.set(countryDocRef, { country: 'US', year: String(targetYear), updatedAt: new Date().toISOString(), data: countryTotalBundle }, { merge: true });

                    await batch.commit();
                }

                console.log(`✅ [Batch] ${docId} 통계 완료 (총 ${successCount}개 종목)`);
            }
            console.log(`🏁 [Batch] 전체 작업 완료`);
        });

    } catch (error) {
        console.error("Update Stats Error:", error);
        if (!res.headersSent) res.status(500).json({ error: error.message });
    }
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
        
        // 1. 대상 종목 가져오기 (상장폐지 안 된 'active' 종목이면서 US 국가인 데이터만)
        const snapshot = await db.collection('stocks')
            .where('active', '==', true)
            .where('country', '==', 'US') // 🌟 US 필터 추가
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
            if (type === 'MC') return Math.round(val / 1000); 

            // 2. MDD: 5% 단위로 퉁치기 (구간화)
            if (type === 'MDD') return Math.floor(Math.round(val) / 5) * 5;

            // 3. 일반 수익률(CAGR, ROE 등): 소수점 날리고 반올림
            return Math.round(val);
        };

        // 2. 데이터 정제 (Loop)
        snapshot.docs.forEach(doc => {
            const data = doc.data();
            const stats = data.stats || {}; 

            // 필수 데이터(IPO날짜, 시총)가 없으면 제외
            if (!data.symbol || !data.ipoDate || !stats.market_cap) return;
            
            // 상장 경과 년수 계산
            const ipoYear = parseInt(data.ipoDate.split('-')[0]);
            const listingYears = currentYear - ipoYear;
            if (isNaN(listingYears)) return;

            // [핵심] 필드명을 알파벳 하나로 줄여서 전송량 30% 절약
            summaryItems.push({
                s: data.symbol,                                   
                n: data.name_kr || data.name_en || data.symbol,   
                ex: data.exchange,                                
                sec: data.sector || 'Etc',                        
                
                // --- X, Y축 후보군 (전부 정수형으로 변환됨) ---
                y: listingYears,                                  
                mc: compress(stats.market_cap, 'MC'),             
                
                p: compress(stats.price_cagr_10y),                
                e: compress(stats.eps_cagr_10y),                  
                r: compress(stats.rev_cagr_10y),                  
                roe: compress(stats.avg_roe_5y),                  
                mdd: compress(stats.mdd, 'MDD'),                  
                per: compress(stats.per_current)                  
            });
        });

        console.log(`✨ 유효 데이터 추출 완료: ${summaryItems.length}개`);

        // 3. 청크(Chunk) 분할 저장 
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

// ============================================================
// [신규 API] 산업별 모멘텀 랭킹 집계 (기간 지정 및 공통 엔진 사용)
// ============================================================
router.post('/update-industry-momentum', verifyBatchOrAdmin, async (req, res) => {
    const { startDate, endDate } = req.body;
    
    // 프론트엔드 대기 방지
    res.json({ success: true, message: "산업별 모멘텀 랭킹 집계 시작 (백그라운드)" });

    setImmediate(async () => {
        try {
            const targetCountry = 'US'; // 🌟 US 고정
            const todayStr = getTodayUS();
            const actualStartDate = startDate || todayStr;
            const actualEndDate = endDate || actualStartDate;
            const targetDates = getDatesInRange(actualStartDate, actualEndDate);

            console.log(`🚀 [Batch] 산업 모멘텀 집계 시작 (${actualStartDate} ~ ${actualEndDate})`);
            
            // 1. 마스터 정보 로드 (산업 Meta)
            const sectorMetaSnap = await firestore.collection('meta_sectors').doc('GICS_Standard').get();
            const sectorMeta = sectorMetaSnap.exists ? sectorMetaSnap.data() : {};
            const industryMetaMap = {};
            if (sectorMeta.industryList) {
                sectorMeta.industryList.forEach(ind => {
                    industryMetaMap[ind.key.toLowerCase()] = { name_ko: ind.name_ko || ind.name_en, etf_ticker: ind.etf_ticker || null };
                });
            }

            // 2. Active 종목 마스터 로드 (US 필터)
            const stockMasterInfoMap = {};
            const stocksSnap = await firestore.collection('stocks')
                .where('active', '==', true)
                .where('country', '==', 'US')
                .get();

            stocksSnap.forEach(doc => {
                const d = doc.data();
                stockMasterInfoMap[doc.id] = { industry: d.industry || '', sector: d.sector || '', isEtf: d.isEtf === true };
            });

            // 3. 날짜별 루프 시작
            for (const targetDate of targetDates) {
                const dayOfWeek = new Date(targetDate).getDay();
                if (dayOfWeek === 0 || dayOfWeek === 6) continue; // 주말 패스

                const targetYear = targetDate.split('-')[0];
                const requiredYears = [String(targetYear), String(targetYear - 1)]; // 60일 전 확보를 위해 2개년도 필요

                // 해당 날짜 연산을 위한 전체 히스토리 로드 (메모리 주의)
                const historyByTicker = {};
                // 서버 부하를 줄이기 위해 청크 단위로 읽기
                const CHUNK_SIZE = 100;
                const tickers = Object.keys(stockMasterInfoMap);
                
                for (let i = 0; i < tickers.length; i += CHUNK_SIZE) {
                    const chunk = tickers.slice(i, i + CHUNK_SIZE);
                    await Promise.all(chunk.map(async (ticker) => {
                        let combinedHistory = [];
                        for (const yr of requiredYears) {
                            const yDoc = await firestore.collection('stocks').doc(ticker).collection('annual_data').doc(yr).get();
                            if (yDoc.exists) combinedHistory = combinedHistory.concat(yDoc.data().data || []);
                        }
                        if (combinedHistory.length > 0) {
                            combinedHistory.sort((a, b) => new Date(b.date) - new Date(a.date)); // 내림차순 정렬 필수
                            historyByTicker[ticker] = combinedHistory;
                        }
                    }));
                }

                // 🌟 공통 엔진 호출
                const finalRankings = StatsEngine.calculateIndustryMomentum(targetDate, historyByTicker, stockMasterInfoMap, industryMetaMap);

                if (finalRankings.length > 0) {
                    // Point-in-Time 조회를 위해 날짜별 문서로 저장 (예: US_2026-03-02)
                    const docId = `US_${targetDate}`;
                    await firestore.collection('industry_momentum').doc(docId).set({
                        date: targetDate,
                        country: 'US',
                        updatedAt: new Date().toISOString(),
                        rankings: finalRankings
                    });

                    // 오늘 날짜인 경우 meta_stats 최신화
                    if (targetDate === todayStr) {
                        await firestore.collection('meta_stats').doc('meta_sync_status').set({
                            industry_momentum_rank: { lastUpdated: new Date().toISOString(), version: Date.now(), totalIndustries: finalRankings.length }
                        }, { merge: true });
                    }
                    console.log(`✅ [Batch] ${targetDate} 산업 모멘텀 집계 완료 (${finalRankings.length}개 산업)`);
                }
            }
            console.log(`🏁 [Batch] 산업 모멘텀 전체 범위 완료`);
        } catch (error) {
            console.error("💥 Update Industry Momentum Error:", error);
        }
    });
});

module.exports = router;