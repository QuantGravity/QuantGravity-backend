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

    /**
     * 핵심 계산 함수
     * @param {string} ticker - 종목 코드
     * @param {Array} history - 날짜 내림차순 정렬된 주가 배열 ([{date, close, volume...}, ...])
     * @param {string} targetDate - 기준일자 (YYYY-MM-DD)
     * @param {Object} masterInfo - { industry, sector, isEtf ... }
     * @returns {Object} 계산된 통계 객체 (없으면 null)
     */
    StatsEngine.calculateDailyStats = function(ticker, history, targetDate, masterInfo) {
        // 1. 데이터 유효성 검사 및 인덱스 찾기
        if (!history || history.length === 0) return null;
        
        // 날짜 내림차순(최신이 위로) 보장
        // (호출하는 쪽에서 정렬해서 주는 게 성능상 좋지만, 안전을 위해 체크 가능)
        // history.sort((a, b) => new Date(b.date) - new Date(a.date));

        const idx = history.findIndex(h => h.date === targetDate);
        if (idx === -1) return null; // 해당 날짜 데이터 없음

        const dayData = history[idx];
        const todayClose = dayData.close;
        const todayVolume = dayData.volume || 0;
        const mktCap = dayData.mktCap || 0; // 로컬/서버 데이터 구조에 따라 다를 수 있음
        const volumeAmt = Math.round(todayClose * todayVolume);

        const periods = [5, 10, 20, 40, 60, 120, 240, 480, 'all'];

        // 2. 기본 구조 생성
        const stats = {
            close: todayClose,
            mktCap: mktCap,
            volume_amt: volumeAmt,
            industry: masterInfo.industry || '',
            // 테마 정보는 계산기 밖에서 주입하거나, masterInfo에 포함해서 전달
            perf_vs_prev: {}, 
            perf_vs_low: {}, 
            perf_vs_high: {},
            prev_low: {}, 
            prev_high: {}, 
            is_new_low: {}, 
            is_new_high: {}, 
            sma: {}, 
            avg_volume_amt_20d: 0, 
            low_240d: 0, 
            high_240d: 0
        };

        // 3. n일 전 대비 수익률 (Rate of Return)
        [1, 2, 3, 4, 5, 10, 20, 40, 60, 120, 240, 480].forEach(d => {
            const pastData = history[idx + d];
            const pastClose = pastData ? pastData.close : 0;
            stats.perf_vs_prev[`${d}d`] = calculateReturn(todayClose, pastClose);
        });

        // 4. 기간별 고가/저가 분석 (High/Low Analysis)
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

                // 신고가/신저가 판별 (오늘 종가가 과거 고가보다 높으면 신고가)
                stats.is_new_low[key] = prevLow > 0 && todayClose < prevLow;
                stats.is_new_high[key] = prevHigh > 0 && todayClose > prevHigh;
                
                if (d === 240) {
                    stats.low_240d = prevLow;
                    stats.high_240d = prevHigh;
                }
            } else {
                // 데이터 부족 시 초기화
                stats.prev_low[key] = 0; stats.prev_high[key] = 0;
                stats.perf_vs_low[key] = 0; stats.perf_vs_high[key] = 0;
                stats.is_new_low[key] = false; stats.is_new_high[key] = false;
            }
        });

        // 5. 이동평균선 (SMA)
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

        // 6. 거래대금 20일 평균
        const volAvgDays = 20;
        if (history.length >= idx + volAvgDays) {
            const volSlice = history.slice(idx, idx + volAvgDays);
            const volSum = volSlice.reduce((acc, curr) => acc + ((curr.close || 0) * (curr.volume || 0)), 0);
            stats.avg_volume_amt_20d = Math.round(volSum / volAvgDays);
        }

        return stats;
    };

    // [모듈 내보내기 로직]
    // 1. Node.js 환경 (Backend)
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = StatsEngine;
    } 
    // 2. Browser 환경 (Frontend)
    else {
        root.StatsEngine = StatsEngine;
    }

}(this));