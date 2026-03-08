// ===========================================================================
// [파일명] : statsCalculator.js
// [설명]   : 주가 데이터 배열을 받아 통계(수익률, SMA, 신고가 등)를 계산하는 순수 함수 모음
// [호환]   : Node.js (Backend) & Browser (Frontend) 공용
// ===========================================================================

(function(root) {
    const StatsEngine = {};

    // [내부 유틸] 수익률 계산 (소수점 2자리)
    function calculateReturn(current, past) {
        if (!past || past === 0 || !current) return 0;
        const res = ((current - past) / past) * 100;
        return isFinite(res) ? parseFloat(res.toFixed(2)) : 0;
    }

    StatsEngine.calculateDailyStats = function(ticker, history, targetDate, masterInfo) {
        if (!history || history.length === 0) return null;

        const idx = history.findIndex(h => h.date === targetDate);
        if (idx === -1) return null; 

        const dayData = history[idx];
        const todayClose = dayData.close;
        const todayVolume = dayData.volume || 0;
        const mktCap = dayData.mktCap || 0; 
        const volumeAmt = Math.round(todayClose * todayVolume);

        const periods = [5, 10, 20, 40, 60, 120, 240, 480, 'all'];

        const stats = {
            close: todayClose,
            mktCap: mktCap,
            volume_amt: volumeAmt,
            industry: masterInfo.industry || '',
            perf_vs_prev: {}, perf_vs_low: {}, perf_vs_high: {},
            prev_low: {}, prev_high: {}, is_new_low: {}, is_new_high: {}, 
            sma: {}, avg_volume_amt_20d: 0, low_240d: 0, high_240d: 0
        };

        [1, 2, 3, 4, 5, 10, 20, 40, 60, 120, 240, 480].forEach(d => {
            const pastData = history[idx + d];
            const pastClose = pastData ? pastData.close : 0;
            stats.perf_vs_prev[`${d}d`] = calculateReturn(todayClose, pastClose);
        });

        periods.forEach(d => {
            const key = d === 'all' ? 'all' : `${d}d`;
            let sliceStart = idx + 1;
            let sliceEnd = d === 'all' ? history.length : idx + 1 + d;

            if (history.length > sliceStart) {
                const slice = history.slice(sliceStart, sliceEnd);
                const validPrices = slice.map(h => h.close).filter(v => v > 0);

                const prevLow = validPrices.length > 0 ? Math.min(...validPrices) : 0;
                const prevHigh = validPrices.length > 0 ? Math.max(...validPrices) : 0;

                stats.prev_low[key] = prevLow;
                stats.prev_high[key] = prevHigh;
                stats.perf_vs_low[key] = calculateReturn(todayClose, prevLow);
                stats.perf_vs_high[key] = calculateReturn(todayClose, prevHigh);

                stats.is_new_low[key] = prevLow > 0 && todayClose < prevLow;
                stats.is_new_high[key] = prevHigh > 0 && todayClose > prevHigh;
                
                if (d === 240) {
                    stats.low_240d = prevLow;
                    stats.high_240d = prevHigh;
                }
            } else {
                stats.prev_low[key] = 0; stats.prev_high[key] = 0;
                stats.perf_vs_low[key] = 0; stats.perf_vs_high[key] = 0;
                stats.is_new_low[key] = false; stats.is_new_high[key] = false;
            }
        });

        [5, 10, 20, 50, 100, 200].forEach(d => {
            if (history.length >= idx + d) {
                const slice = history.slice(idx, idx + d);
                const validPrices = slice.map(h => h.close).filter(v => v > 0);
                if (validPrices.length === d) {
                    const sum = validPrices.reduce((acc, curr) => acc + curr, 0);
                    stats.sma[`${d}d`] = parseFloat((sum / d).toFixed(2));
                } else {
                    stats.sma[`${d}d`] = 0;
                }
            } else {
                stats.sma[`${d}d`] = 0;
            }
        });

        const volAvgDays = 20;
        if (history.length >= idx + volAvgDays) {
            const volSlice = history.slice(idx, idx + volAvgDays);
            const volSum = volSlice.reduce((acc, curr) => acc + ((curr.close || 0) * (curr.volume || 0)), 0);
            stats.avg_volume_amt_20d = Math.round(volSum / volAvgDays);
        }

        return stats;
    };

    // 🌟 [추가] 산업 모멘텀 연산 (안전장치 100% 탑재)
    StatsEngine.calculateIndustryMomentum = function(targetDate, historyByTicker, masterInfoMap, industryMetaMap) {
        const industryAgg = {};

        for (const [ticker, history] of Object.entries(historyByTicker)) {
            const masterInfo = masterInfoMap[ticker] || {};
            const sector = masterInfo.sector;
            const industry = masterInfo.industry;

            if (!sector || !industry || masterInfo.isEtf || ticker.startsWith('^')) continue;

            const targetIdx = history.findIndex(h => h.date <= targetDate);
            if (targetIdx === -1 || history[targetIdx].date !== targetDate) continue;

            const dayData = history[targetIdx];
            const mktCap = dayData.mktCap || masterInfo.mktCap || 0;
            const currentPrice = dayData.close || 0;

            // 과거 데이터에 시총(0)이 누락되어도 살려주고, 값이 있으면 3억 불 컷!
            if ((mktCap > 0 && mktCap < 300000000) || currentPrice < 1) continue;

            if (history.length <= targetIdx + 20) continue; 
            const pastIdx = Math.min(targetIdx + 60, history.length - 1);
            const pastPrice = history[pastIdx].close;

            if (pastPrice <= 0) continue;

            const momentumScore = ((currentPrice / pastPrice) - 1) * 100;
            const indKey = industry.toLowerCase();

            if (!industryAgg[indKey]) {
                industryAgg[indKey] = { sector_en: sector, industry_en: industry, scores: [] };
            }
            industryAgg[indKey].scores.push(momentumScore);
        }

        const finalRankings = [];

        for (const [indKey, aggData] of Object.entries(industryAgg)) {
            const scores = aggData.scores;
            if (scores.length < 3) continue; 

            scores.sort((a, b) => a - b);
            let median = 0;
            const mid = Math.floor(scores.length / 2);
            if (scores.length % 2 === 0) {
                median = (scores[mid - 1] + scores[mid]) / 2;
            } else {
                median = scores[mid];
            }

            const metaInfo = industryMetaMap[indKey] || {};

            finalRankings.push({
                sector: aggData.sector_en,
                industry: aggData.industry_en,
                name_ko: metaInfo.name_ko || aggData.industry_en,
                etf_ticker: metaInfo.etf_ticker || null,
                median_momentum: parseFloat(median.toFixed(2)),
                stock_count: scores.length
            });
        }

        finalRankings.sort((a, b) => b.median_momentum - a.median_momentum);
        return finalRankings;
    };

    if (typeof module !== 'undefined' && module.exports) {
        module.exports = StatsEngine;
    } else {
        root.StatsEngine = StatsEngine;
    }

}(this));