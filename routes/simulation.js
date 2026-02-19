// ===========================================================================
// [파일명] : routes/simulation.js
// [대상]   : 퀀트 그래비티 12단계 정밀 시뮬레이션 엔진 및 일괄 분석 API
// [기준]   : 
//   1. 로직 무결성: 매수/매도 Gap 및 Split 파라미터를 기반으로 한 12단계 엔진을 완벽히 유지한다.
//   2. 성능 최적화: 대량 분석 시 티커별 데이터를 선행 로드(Pre-loading)하여 DB 부하를 최소화한다.
//   3. 등급별 차등: 사용자 권한(VIP/일반)에 따라 BATCH_SIZE와 DELAY_MS를 동적으로 조절한다.
//   4. 응답 최적화: rType(1:상세, 2:차트, 3:최근) 플래그를 통해 프론트엔드 전송 데이터양을 제어한다.
// ===========================================================================
const express = require('express');
const router = express.Router();
const admin = require('firebase-admin');
const firestore = admin.firestore();
const { verifyToken } = require('../utils/authHelper');
const { logTraffic } = require('../utils/logger');
const { getDailyStockData } = require('../utils/stockHelper');
const { analyzeTickerPerformance } = require('../utils/analysisEngine');
const { simulateStrategyPerformance } = require('../utils/analysisEngine');

// =============================================================
// [API] 대량 종목 병렬 성능 분석 (Mass Parallel Ticker Analysis)
// =============================================================
router.post('/mass-parallel-analyze-tickers', verifyToken, async (req, res) => {
    const { tickers, startDate, endDate, rollingPeriod1, rollingPeriod2 } = req.body;
    const rp1 = parseInt(rollingPeriod1) || 10;
    const rp2 = parseInt(rollingPeriod2) || 5;

    if (!tickers || !Array.isArray(tickers) || tickers.length === 0) return res.json([]);

    try {
        // [1] 등급별 부하 조절 설정 (전략 시뮬레이션과 동일 기준 적용)
        const userRole = req.user.role || 'G1';
        const isVip = ['G9', 'admin'].includes(userRole);
        const BATCH_SIZE = isVip ? 50 : 10; // 일반 유저는 한 번에 10개씩
        const DELAY_MS = isVip ? 0 : 300; 

        const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
        const finalResults = [];

        // [2] 전체 티커를 BATCH_SIZE 단위로 분할하여 실행
        for (let i = 0; i < tickers.length; i += BATCH_SIZE) {
            const chunk = tickers.slice(i, i + BATCH_SIZE);

            // 해당 묶음(Chunk)만 병렬로 실행
            const chunkPromises = chunk.map(async (ticker) => {
                // 캐시 전략 로직 (기존과 동일)
                const isIndex = ticker.startsWith('^');
                const isDefaultRolling = (rp1 === 10 && rp2 === 5);
                const isRecent = !endDate || endDate >= new Date().toISOString().split('T')[0];

                if (isIndex && isDefaultRolling && isRecent) {
                    const cacheDoc = await firestore.collection('analysis_cache').doc(ticker).get();
                    if (cacheDoc.exists) return cacheDoc.data();
                }

                // 실시간 계산 수행
                return await analyzeTickerPerformance(ticker, startDate, endDate, rp1, rp2);
            });

            const results = await Promise.all(chunkPromises);
            finalResults.push(...results);

            // 다음 배치 실행 전 대기
            if (i + BATCH_SIZE < tickers.length && DELAY_MS > 0) {
                await sleep(DELAY_MS);
            }
        }

        res.json(finalResults);

    } catch (err) {
        console.error("Mass Analysis Error:", err);
        res.status(500).json({ error: "분석 중 오류 발생" });
    }
});

// ----------------------------------------------------------------
// [엔진] 시뮬레이션 핵심 로직 - 주가 데이터 주입(Injection) 지원
// ----------------------------------------------------------------
// [최적화] nper 함수 (try-catch 제거)
function nper_custom(rate, pv, fv) {
    if (rate === 0) return 0;
    const val = Math.abs(fv) / pv;
    if (val <= 0) return 0;
    return Math.log(val) / Math.log(1 + rate);
}

// [API] 병렬 처리 일괄 실행 (등급별 속도 제어 적용)
router.post('/mass-parallel-simulate-strategies', verifyToken, logTraffic, async (req, res) => {
    try {
        const { strategies, startDate, endDate, responseType } = req.body;
        const reqType = responseType || 1;

        if (!strategies || strategies.length === 0) {
            return res.status(400).json({ success: false, error: "전략 리스트가 없습니다." });
        }

        // ============================================================
        // [비즈니스 로직] 등급별 처리 속도 설정 (Throttling Config)
        // ============================================================
        const userRole = req.user.role || 'G1';
        const isVip = ['G9', 'admin'].includes(userRole);

        // VIP: 한 번에 50개씩 병렬 처리, 대기시간 없음
        // 일반(G1): 한 번에 5개씩 병렬 처리, 배치 사이 500ms(0.5초) 지연
        const BATCH_SIZE = isVip ? 50 : 5; 
        const DELAY_MS = isVip ? 0 : 500; 

        // ============================================================
        // [데이터 준비] 티커별 데이터 선행 로드 (기존 로직 유지)
        // ============================================================
        const uniqueTickers = [...new Set(strategies.map(s => s.ticker))];
        const priceDataMap = {};

        // 티커 데이터는 병렬로 최대한 빠르게 확보 (병목 최소화)
        await Promise.all(uniqueTickers.map(async (ticker) => {
            const data = await getDailyStockData(ticker, startDate, endDate);
            priceDataMap[ticker] = data || [];
        }));

        // ============================================================
        // [핵심 로직] 배치 단위 실행 및 지연 처리
        // ============================================================
        
        // 지연 처리를 위한 유틸 함수
        const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
        
        const finalResults = []; // 전체 결과 저장소

        // 전략 리스트를 BATCH_SIZE 만큼 잘라서 순차 처리
        for (let i = 0; i < strategies.length; i += BATCH_SIZE) {
            // 현재 처리할 묶음 (Chunk)
            const chunk = strategies.slice(i, i + BATCH_SIZE);

            // 해당 묶음 병렬 실행
            const chunkPromises = chunk.map(async (strat) => {
                const tickerData = priceDataMap[strat.ticker];
                
                // 데이터 없음 처리
                if (!tickerData || tickerData.length === 0) {
                    return { strategy_code: strat.strategy_code, success: false, message: "데이터 없음" };
                }

                // 시뮬레이션 실행
                const simResult = await simulateStrategyPerformance({ ...strat, responseType: reqType }, tickerData);

                if (simResult) {
                    const lastTicker = tickerData[tickerData.length - 1];
                    
                    const resultObj = {
                        strategy_code: strat.strategy_code,
                        success: true,
                        tickerStats: {
                            max: Math.max(...tickerData.map(d => d.close_price || 0)),
                            last: Number(lastTicker.close_price),
                            date: lastTicker.date
                        },
                        summary: simResult.lastStatus
                    };

                    // 요청 타입별 응답 데이터 구성
                    if (reqType === 1) {
                        resultObj.rows = simResult.rows;
                        resultObj.chart = simResult.chartData;
                    } else if (reqType === 2) {
                        resultObj.chart = simResult.chartArrays;
                        resultObj.recentHistory = simResult.recentHistory;
                    } else if (reqType === 3) {
                        resultObj.recentHistory = simResult.recentHistory;
                    }
                    return resultObj;
                } else {
                    return { strategy_code: strat.strategy_code, success: false, message: "시뮬레이션 실패" };
                }
            });

            // 현재 배치의 결과 기다림
            const batchResults = await Promise.all(chunkPromises);
            finalResults.push(...batchResults);

            // 마지막 배치가 아니고, 지연 시간이 설정되어 있다면 대기
            if (i + BATCH_SIZE < strategies.length && DELAY_MS > 0) {
                await sleep(DELAY_MS);
            }
        }

        res.json({ success: true, results: finalResults });

    } catch (e) {
        console.error("Batch Simulation Error:", e);
        res.status(500).json({ success: false, error: e.message });
    }
});

module.exports = router;