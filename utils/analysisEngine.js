// ===========================================================================
// [파일명] : utils/analysisEngine.js
// [대상]   : 종목별 성과 분석(MDD, CAGR, Rolling 등) 핵심 엔진
// ===========================================================================
const admin = require('firebase-admin');

// ============================================================
// [리팩토링] 분석 핵심 로직을 내부 함수로 분리 (재사용 목적)
// ============================================================
async function analyzeTickerPerformance(ticker, startDate, endDate, rp1, rp2) {
    let prices = [];
    try {
        // 데이터 조회
        const stockData = await getDailyStockData(ticker, startDate, endDate);
        prices = stockData.map(r => ({
            date: new Date(r.date),
            dateStr: r.date,
            price: parseFloat(r.close_price)
        }));
    } catch (e) {
        console.error(`데이터 조회 실패 (${ticker}):`, e);
        return { ticker, error: "데이터 조회 오류" };
    }

    if (prices.length < 2) {
        return { ticker, error: "데이터 부족" };
    }

    const startItem = prices[0];
    const endItem = prices[prices.length - 1];
    const basePrice = startItem.price;

    // --- 통계 및 차트 데이터 계산 (기존 로직과 동일) ---
    let maxPrice = 0;
    let minDd = 0;
    let sumDd = 0;

    const thresholds = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6];
    const ddCounts = { 0.1: 0, 0.2: 0, 0.3: 0, 0.4: 0, 0.5: 0, 0.6: 0 };
    const isUnderWater = { 0.1: false, 0.2: false, 0.3: false, 0.4: false, 0.5: false, 0.6: false };

    let lastPeakDate = prices[0].date;
    let recoveryDays = [];
    // [신규] 회복일수 빈도 집계용 객체
    const recoveryDist = { under30: 0, under90: 0, under180: 0, under365: 0, over365: 0 };
    const history = [];

    prices.forEach((p) => {
        const price = p.price;

        if (price > maxPrice) {
            if (maxPrice > 0) {
                const diffDays = Math.ceil(Math.abs(p.date - lastPeakDate) / (1000 * 60 * 60 * 24));
                if (diffDays > 0) {
                    recoveryDays.push(diffDays);
                    // [신규] 구간별 빈도 계산
                    if (diffDays <= 30) recoveryDist.under30++;
                    else if (diffDays <= 90) recoveryDist.under90++;
                    else if (diffDays <= 180) recoveryDist.under180++;
                    else if (diffDays <= 365) recoveryDist.under365++;
                    else recoveryDist.over365++;
                }
            }
            maxPrice = price;
            lastPeakDate = p.date;
            thresholds.forEach(th => isUnderWater[th] = false);
        }

        const dd = (price - maxPrice) / maxPrice;
        if (dd < minDd) minDd = dd;
        sumDd += dd;

        thresholds.forEach(th => {
            if (dd <= -th && !isUnderWater[th]) {
                ddCounts[th]++;
                isUnderWater[th] = true;
            }
        });

        const currentYield = ((price - basePrice) / basePrice) * 100;
        const currentDd = dd * 100;

        history.push({
            d: p.dateStr,
            y: parseFloat(currentYield.toFixed(2)),
            m: parseFloat(currentDd.toFixed(2))
        });
    });

    const avgDd = (sumDd / prices.length) * 100;
    const finalMinDd = minDd * 100;
    const maxRecovery = recoveryDays.length ? Math.max(...recoveryDays) : 0;
    const avgRecovery = recoveryDays.length ? (recoveryDays.reduce((a, b) => a + b, 0) / recoveryDays.length) : 0;

// --- Rolling CAGR --- (수정됨)
    const rollingArr1 = [];
    const rollingArr2 = [];
    let idx1 = 0, idx2 = 0;

    for (let i = 0; i < prices.length; i++) {
        const curr = prices[i];
        
        // [중요] 화면에서 입력받은 변수(rp1, rp2)를 사용하여 기준 날짜 계산
        const d1 = new Date(curr.date); d1.setFullYear(curr.date.getFullYear() - rp1);
        const d2 = new Date(curr.date); d2.setFullYear(curr.date.getFullYear() - rp2);
        const t1 = d1.getTime();
        const t2 = d2.getTime();

        // 날짜 인덱스 조정
        while (idx1 < i && prices[idx1].date.getTime() < t1) idx1++;
        while (idx2 < i && prices[idx2].date.getTime() < t2) idx2++;

        // [오류 수정] val1, val2 변수를 여기서 미리 선언 (초기값 null)
        let val1 = null; 
        let val2 = null;

        // 1. Rolling Period 1 계산 (입력받은 rp1 사용)
        const p1 = prices[idx1];
        if (p1 && p1.date.getTime() >= t1 && (p1.date.getTime() - t1) < 86400000 * (rp1 + 1)) { 
            // 데이터 공백이 너무 크지 않은 경우만 계산 (rp1 + 1년 여유)
            val1 = calculateCAGR(p1.price, curr.price, rp1);
            rollingArr1.push(val1);
        }

        // 2. Rolling Period 2 계산 (입력받은 rp2 사용)
        const p2 = prices[idx2];
        if (p2 && p2.date.getTime() >= t2 && (p2.date.getTime() - t2) < 86400000 * (rp2 + 1)) {
            val2 = calculateCAGR(p2.price, curr.price, rp2);
            rollingArr2.push(val2);
        }

        // [데이터 병합] 위에서 계산한 val1, val2를 history 배열에 주입
        // (history 배열은 위쪽 prices.forEach에서 이미 생성됨)
        if (history[i]) {
            history[i].r1 = val1; // Rolling 1 (화면 입력값 1)
            history[i].r2 = val2; // Rolling 2 (화면 입력값 2)
        }
    }

    const statsRolling = (arr) => ({
        min: arr.length ? Math.min(...arr) : null,
        max: arr.length ? Math.max(...arr) : null,
        med: arr.length ? getMedian(arr) : null
    });

    // --- Period CAGR ---
    const periods = [
        { label: 'total', years: (endItem.date - startItem.date) / (1000 * 3600 * 24 * 365.25), refPrice: startItem.price },
        { label: '30y', years: 30 }, { label: '25y', years: 25 }, { label: '20y', years: 20 },
        { label: '15y', years: 15 }, { label: '10y', years: 10 }, { label: '7y', years: 7 },
        { label: '5y', years: 5 }, { label: '3y', years: 3 }, { label: '1y', years: 1 },
        { label: '6m', years: 0.5 }, { label: '3m', years: 0.25 }, { label: '1m', years: 1/12 }
    ];

    const periodCagrs = {};
    periods.forEach(p => {
        if (p.label === 'total') {
            periodCagrs['total'] = calculateCAGR(p.refPrice, endItem.price, p.years);
        } else {
            const targetDate = new Date(endItem.date);
            if (p.years < 1) {
                targetDate.setMonth(targetDate.getMonth() - Math.round(p.years * 12));
            } else {
                targetDate.setFullYear(targetDate.getFullYear() - p.years);
            }
            const pastItem = prices.find(item => item.date >= targetDate);
            if (pastItem && pastItem.date <= new Date(targetDate.getTime() + 86400000 * 15)) {
                periodCagrs[p.label] = calculateCAGR(pastItem.price, endItem.price, p.years);
            } else {
                periodCagrs[p.label] = null;
            }
        }
    });

    return {
        ticker,
        period: { start: startItem.date.toISOString().split('T')[0], end: endItem.date.toISOString().split('T')[0] },
        dd: { max: finalMinDd, avg: avgDd },
        ddCounts,
        recovery: { max: maxRecovery, avg: avgRecovery },
        recoveryDist: recoveryDist, // [추가] 프론트엔드 차트용 데이터
        rolling: { r10: statsRolling(rollingArr1), r5: statsRolling(rollingArr2) },
        periodCagrs,
        history,
        updatedAt: admin.firestore.FieldValue.serverTimestamp() // 캐싱 시점 기록
    };
}

// [핵심] 시뮬레이션 엔진 - 12단계 정밀 로직 100% 완전 복원 버전 (Gap/Split 분리 전용)
async function simulateStrategyPerformance(params, preLoadedPriceData = null) {
    const { 
        ticker, start, end, initCash, initStock, targetRate, upperRate, lowerRate, 
        // [수정] unitGap, split 제거 및 개별 변수 확정 사용
        gapBuy, gapSell, 
        splitBuy, splitSell, 
        alarmBuy, alarmSell, // [추가] 알람 파라미터
        withdraw, feeRate, taxRate,
        responseType // 차트 최적화를 위한 플래그 (1:상세, 2:차트용, 3:최근기록)
    } = params;

    // 1. 데이터 준비
    let priceData = preLoadedPriceData;
    if (!priceData) {
        priceData = await getDailyStockData(ticker, start, end);
    }
    
    // 데이터가 최소 2일치는 있어야 지표 산출 가능
    if (!priceData || priceData.length === 0) return null;

    // 2. 파라미터 초기화
    const initStockRate = parseFloat(initStock) / 100;
    const initCashVal = parseFloat(initCash);
    const targetYearRate = parseFloat(targetRate) / 100;
    const upperPct = parseFloat(upperRate) / 100;
    const lowerPct = parseFloat(lowerRate) / 100;
    
    const p_gapBuyPct = parseFloat(gapBuy) / 100;
    const p_gapSellPct = parseFloat(gapSell) / 100;
    const p_splitBuy = parseInt(splitBuy);
    const p_splitSell = parseInt(splitSell);

    // [추가] 알람 비율 파라미터 처리
    const p_alarmBuy = parseFloat(alarmBuy || 0) / 100;
    const p_alarmSell = parseFloat(alarmSell || 0) / 100;

    // [인출] 인출 비율 파라미터 처리
    const withdrawPct = parseFloat(withdraw || 0) / 100;

    const targetDayRate = Math.pow(1 + targetYearRate, 1 / 365) - 1;
    const fRate = parseFloat(feeRate || 0);
    const tRate = parseFloat(taxRate || 0);
    const feeMultiplier = 1 + fRate; 

    // 초기값 설정
    let vTarget = initCashVal * initStockRate;
    let mddRate = 0;
    let highAsset = initCashVal;
    
    // 첫날 데이터 기준 초기화
    const firstClose = parseFloat(priceData[0].close_price);
    let shares = Math.floor(vTarget / feeMultiplier / firstClose);
    let totalPurchaseAmt = shares * firstClose * feeMultiplier; // 평단가 계산용 총 매수금액
    let cash = initCashVal - totalPurchaseAmt;

    vTarget = shares * firstClose;
    
    // 이전 상태 추적 변수
    let last_asset = initCashVal; 
    let last_curLower = vTarget * lowerPct;
    let last_curUpper = vTarget * upperPct;
    let totalFailCount = 0;
    let totalWithdrawal = 0;   
    let totalBuyAmt = 0;
    let totalSellAmt = 0;
    let totalProfit = 0;
    let totalTax = 0;
    let totalFee = 0;

    // [추가] 통계 집계용 변수 초기화
    let totalBuyCount = 0;
    let totalSellCount = 0;
    let totalBuyAlarmCount = 0;
    let totalSellAlarmCount = 0;
    let sumStockRatio = 0;
    let sumDd = 0;
    let sumAsset = 0; // [신규] 자산 합계 (회전율 계산용)

    // [신규] 추가 통계 변수
    let maxStockRatio = -1;
    let minStockRatio = 9999;
    let totalBuyMissCount = 0;
    let totalSellMissCount = 0;
    
    // 최대회복기간 계산용
    let lastHighAssetDayIdx = 0;
    let maxRecoveryDays = 0;

    // 결과 담을 컨테이너
    const rType = responseType || 1;
    const rows = (rType === 1) ? [] : null;
    const chartData = (rType === 1) ? { labels: [], ev: [], vU: [], vB: [], vL: [] } : null;
    
    // [추가] 년도별 요약 데이터 컨테이너
    const yearlyReturns = []; 

    // rType 2 (차트용 경량 데이터)
    const chartArrays = (rType === 2) 
        ? { dates: [], closes: [], assets: [], dds: [], shares: [], lowers: [], uppers: [], ratios: [] } 
        : null;

    // rType 3 (최근 기록용)
    const recentHistory = (rType === 3) ? [] : null;
    const startRecordingIdx = Math.max(0, priceData.length - 14);

    // 3. 메인 시뮬레이션 루프
    for (let i = 0; i < priceData.length; i++) {
        const day = priceData[i];
        const open = parseFloat(day.open_price);
        const high = parseFloat(day.high_price);
        const low = parseFloat(day.low_price);
        const close = parseFloat(day.close_price);
        
        const dateStr = day.date instanceof Date ? day.date.toISOString().split('T')[0] : String(day.date).split('T')[0];
        
        // [수정] startCash는 인출 로직에 의해 변경될 수 있으므로 let으로 선언
        let startCash = cash; 
        let prevCash = cash; 
        let dailyWithdrawal = 0; // 금일 인출 금액

        const prevShares = shares;
        // 기존 로직: 첫날은 vTarget 기준, 이후는 전일 데이터(last_curLower) 기준
        const prevLower = i === 0 ? vTarget * lowerPct : last_curLower;
        const prevUpper = i === 0 ? vTarget * upperPct : last_curUpper;

        // [매매단위수량 계산] - 매수/매도 Gap 기준
        let unitQtyBuy = Math.floor(prevShares * p_gapBuyPct);
        let unitQtySell = Math.floor(prevShares * p_gapSellPct);
        if (unitQtyBuy <= 0) unitQtyBuy = 1;
        if (unitQtySell <= 0) unitQtySell = 1;

        let diffDays = 0;
        if (i > 0) {
            const prevDate = new Date(priceData[i-1].date);
            const currDate = new Date(priceData[i].date);
            diffDays = (currDate - prevDate) / (1000 * 60 * 60 * 24);
            
            // 목표가치 증가 (복리)
            vTarget *= Math.pow(1 + targetDayRate, diffDays);

            // [인출] 로직
            if (withdrawPct > 0) {
                dailyWithdrawal = last_asset * withdrawPct * (diffDays / 365);
                if (dailyWithdrawal > startCash) dailyWithdrawal = startCash;
                startCash -= dailyWithdrawal;
            }
        }

        const curUpper = vTarget * upperPct;
        const curLower = vTarget * lowerPct;

        // ----------------------------------------------------------------
        // [추가] 알람(Alarm) 계산 로직
        // ----------------------------------------------------------------
        let isBuyAlarm = 0;
        let isSellAlarm = 0;

        // 알람 계산을 위한 전일 종가 (첫날은 시가 사용)
        const prevClose = i === 0 ? open : parseFloat(priceData[i-1].close_price);

        if (prevShares > 0) {
            // 매수/매도 시작 예약가 계산 (Start Price)
            const calcBuyStart = (prevLower * (1 - p_gapBuyPct)) / prevShares;
            const calcSellStart = (prevUpper * (1 + p_gapSellPct)) / prevShares;

            // 매수 알람
            if (p_alarmBuy > 0) {
                const buyGapRatio = (prevClose - calcBuyStart) / prevClose;
                if (buyGapRatio < p_alarmBuy) {
                    isBuyAlarm = 1;
                }
            }

            // 매도 알람
            if (p_alarmSell > 0) {
                const sellGapRatio = (calcSellStart - prevClose) / prevClose;
                if (sellGapRatio < p_alarmSell) {
                    isSellAlarm = 1;
                }
            }
        }

        // ----------------------------------------------------------------
        // [수정됨] 매수/매도 로직: 구분 없이 통합 합산
        // ----------------------------------------------------------------
        
        const currentAvgPrice = prevShares > 0 ? totalPurchaseAmt / prevShares : 0;
        let dailyFailCount = 0; // 일간 실패 횟수

        // [통합 변수 선언] 시가/저가/고가 구분 변수 삭제
        let dailyBuyCount = 0;
        let dailyBuyQty = 0;
        let dailyBuyAmt = 0; // 수수료 포함 매수 금액 합계

        let dailySellCount = 0;
        let dailySellQty = 0;
        let dailySellAmt = 0; // 세금/수수료 차감 후 매도 금액 합계
        let dailyProfit = 0;  // 수익
        let dailyTax = 0;     // 세금

        // [신규] 이탈 횟수
        let dailyBuyMiss = 0;
        let dailySellMiss = 0;

        // 임시 변수 (루프 내 계산용)
        let tempCash = startCash;
        let tempShares = prevShares;

        // ----------------------------------------------------------------
        // 1. 매수 계산 (Buy Calculation) - NPER Logic 적용
        // ----------------------------------------------------------------
        if (prevShares > 0) {
            let buyStartPrice = (prevLower * (1 - p_gapBuyPct)) / prevShares;
            
            // [NPER 계산] 매수 가능 최대 횟수 계산 (Low 도달 기준)
            let maxBuyLoops = 0;
            if (low < buyStartPrice && p_gapBuyPct > 0) {
                // formula: low = start * (1 - gap)^n
                maxBuyLoops = Math.floor(Math.log(low / buyStartPrice) / Math.log(1 - p_gapBuyPct)) + 1;
            }

            // 실제 루프 횟수는 설정된 split과 계산된 max 중 큰 값 (이탈 계산을 위해)
            const loopLimitBuy = Math.max(p_splitBuy, maxBuyLoops);

            let currentTarget = buyStartPrice;
            
            // 매수 타겟 리스트 생성
            let buyTargets = [];
            for (let k = 0; k < loopLimitBuy; k++) {
                buyTargets.push({ price: currentTarget, index: k });
                currentTarget = currentTarget * (1 - p_gapBuyPct);
            }
            
            // 내림차순 정렬
            buyTargets.sort((a, b) => b.price - a.price);

            for (let item of buyTargets) {
                const price = item.price;
                const idx = item.index; // 0부터 시작

                let executed = false;
                let execPrice = 0;

                if (price >= open) {
                    executed = true;
                    execPrice = open;
                } else if (low <= price) {
                    executed = true;
                    execPrice = price;
                }

                if (executed) {
                    // 설정된 분할 횟수 이내인 경우만 실제 체결
                    if (idx < p_splitBuy) {
                        let reqAmount = execPrice * unitQtyBuy * feeMultiplier;
                        
                        if (tempCash >= reqAmount) {
                            tempCash -= reqAmount;
                            
                            dailyBuyCount++;
                            dailyBuyQty += unitQtyBuy;
                            dailyBuyAmt += reqAmount;
                        } else {
                            dailyFailCount++;
                        }
                    } else {
                        // 범위를 벗어난 체결 가능 건수 (이탈 횟수 증가)
                        dailyBuyMiss++;
                    }
                }
            }
        }

        // ----------------------------------------------------------------
        // 2. 매도 계산 (Sell Calculation) - NPER Logic 적용
        // ----------------------------------------------------------------
        if (prevShares > 0) {
            let sellStartPrice = (prevUpper * (1 + p_gapSellPct)) / prevShares;

            // [NPER 계산] 매도 가능 최대 횟수 계산 (High 도달 기준)
            let maxSellLoops = 0;
            if (high > sellStartPrice && p_gapSellPct > 0) {
                // formula: high = start * (1 + gap)^n
                maxSellLoops = Math.floor(Math.log(high / sellStartPrice) / Math.log(1 + p_gapSellPct)) + 1;
            }

            const loopLimitSell = Math.max(p_splitSell, maxSellLoops);

            let currentTarget = sellStartPrice;
            let sellTargets = [];

            for (let k = 0; k < loopLimitSell; k++) {
                sellTargets.push({ price: currentTarget, index: k });
                currentTarget = currentTarget * (1 + p_gapSellPct);
            }

            // 오름차순 정렬
            sellTargets.sort((a, b) => a.price - b.price);

            for (let item of sellTargets) {
                const price = item.price;
                const idx = item.index;

                let executed = false;
                let execPrice = 0;

                if (price <= open) {
                    executed = true;
                    execPrice = open;
                } else if (high >= price) {
                    executed = true;
                    execPrice = price;
                }

                if (executed) {
                    if (idx < p_splitSell) {
                        if (tempShares > 0) {
                            // 잔량 처리
                            let currentSellQty = unitQtySell;
                            if (tempShares < unitQtySell) {
                                currentSellQty = tempShares;
                            }
                            tempShares -= currentSellQty;

                            let profit = currentSellQty * (execPrice - currentAvgPrice);
                            let tax = profit > 0 ? profit * tRate : 0;
                            let sellAmount = (currentSellQty * execPrice) * (1 - fRate) - tax;

                            dailySellCount++;
                            dailySellQty += currentSellQty;
                            dailySellAmt += sellAmount;
                            dailyProfit += profit;
                            dailyTax += tax;
                        }
                    } else {
                        // 설정 범위를 초과하여 상승한 경우
                        dailySellMiss++;
                    }
                }
            }
        }

        // ----------------------------------------------------------------
        // 자산 및 수량 업데이트
        // ----------------------------------------------------------------
        
        // 최종 보유 수량 계산 (daily 변수 사용)
        shares = prevShares + dailyBuyQty - dailySellQty;

        // 현금 잔고 계산
        startCash = prevCash; 
        cash = startCash - dailyBuyAmt + dailySellAmt - dailyWithdrawal;

        const asset = cash + (shares * close);
        const evalAmt = shares * close;
        
        // [평단가(totalPurchaseAmt) 업데이트]
        // 매도 발생 시 평단가 금액 비례 차감
        if (dailySellQty > 0 && prevShares > 0) {
            totalPurchaseAmt -= (dailySellQty * (totalPurchaseAmt / prevShares));
        }
        // 매수 발생 시 실제 매수 금액 추가 (수수료 포함된 금액)
        if (dailyBuyQty > 0) {
            totalPurchaseAmt += dailyBuyAmt;
        }

        // [회복기간 계산]
        let currentRecoveryDays = 0;
        if (asset > highAsset) {
            highAsset = asset;
            lastHighAssetDayIdx = i;
            currentRecoveryDays = 0;
        } else {
            const recoveryDays = i - lastHighAssetDayIdx;
            if (recoveryDays > maxRecoveryDays) {
                maxRecoveryDays = recoveryDays;
            }
            currentRecoveryDays = recoveryDays;
        }

        const mdd = highAsset > 0 ? ((asset - highAsset) / highAsset * 100) : 0;
        if (mdd < mddRate) mddRate = mdd;

        // 상태 업데이트
        totalFailCount += dailyFailCount;
        totalWithdrawal += dailyWithdrawal;
        totalBuyAmt += dailyBuyAmt;
        totalSellAmt += dailySellAmt;
        totalProfit += dailyProfit;
        totalTax += dailyTax;
        totalFee += (dailyBuyAmt + dailySellAmt) / feeMultiplier;

        last_asset = asset;
        last_curLower = curLower;
        last_curUpper = curUpper;
        const stockRatio = shares > 0 ? (shares * close) / asset * 100 : 0;
        const avgPrice = shares > 0 ? totalPurchaseAmt / shares : 0;

        // [추가] 통계 누적
        totalBuyCount += dailyBuyCount;
        totalSellCount += dailySellCount;
        totalBuyAlarmCount += isBuyAlarm;
        totalSellAlarmCount += isSellAlarm;
        sumStockRatio += stockRatio;
        sumDd += dd;
        sumAsset += asset;

        // [신규] 통계 누적
        if (stockRatio > maxStockRatio) maxStockRatio = stockRatio;
        if (stockRatio < minStockRatio) minStockRatio = stockRatio;
        totalBuyMissCount += dailyBuyMiss;
        totalSellMissCount += dailySellMiss;

        // ------------------------------------------------------------------
        // [데이터 저장]
        // ------------------------------------------------------------------
        if (rType === 1) {
            rows.push({
                date: dateStr,
                asset, dd,
                stockRatio, avgPrice,
                failCount: dailyFailCount,
                
                open, high, low, close,
                startCash, 
                sAmt: dailySellAmt, 
                bAmt: dailyBuyAmt,  
                withdrawal: dailyWithdrawal,
                finalCash: cash,
                curLower, curUpper, vTarget, diffDays, unitQtyBuy, unitQtySell,
                totalPurchaseAmt, evalAmt,
                
                buyQty: dailyBuyQty,
                sellQty: dailySellQty,
                buyCount: dailyBuyCount,
                sellCount: dailySellCount,
                
                profit: dailyProfit,
                tax: dailyTax,
                
                buyAlarm: isBuyAlarm,
                sellAlarm: isSellAlarm,

                // [요청 반영] 일자별 상세 데이터에 추가
                buyMiss: dailyBuyMiss,
                sellMiss: dailySellMiss,
                recoveryDays: currentRecoveryDays
            });

            chartData.labels.push(dateStr);
            chartData.ev.push(Math.round(shares * close));
            chartData.vB.push(Math.round(vTarget)); 
            chartData.vU.push(Math.round(curUpper)); 
            chartData.vL.push(Math.round(curLower)); 
        } 
        else if (rType === 2) {
            chartArrays.dates.push(dateStr);
            chartArrays.assets.push(Math.round(asset));
            chartArrays.closes.push(close);
            chartArrays.dds.push(Math.round(dd * 100) / 100);
            chartArrays.shares.push(shares);
            chartArrays.lowers.push(Math.round(curLower));
            chartArrays.uppers.push(Math.round(curUpper));
            chartArrays.ratios.push(Math.round(stockRatio * 10) / 10);
        }
        else if (rType === 3) {
            if (i >= startRecordingIdx) {
                recentHistory.push({
                    date: dateStr,
                    close: close,
                    open: open,
                    high: high,
                    low: low,
                    asset: Math.round(asset),
                    stockRatio: Math.round(stockRatio * 10) / 10,
                    shares: shares,
                    curLower: Math.round(curLower),
                    curUpper: Math.round(curUpper)
                });
            }
        }

        // ----------------------------------------------------------------
        // [추가] 년도별 요약 데이터 계산 (Loop 마지막)
        // ----------------------------------------------------------------
        const currentYear = new Date(day.date).getFullYear();
        let isYearEnd = false;
        
        // 마지막 데이터이거나, 다음 데이터의 년도가 다를 경우
        if (i === priceData.length - 1) {
            isYearEnd = true;
        } else {
            const nextYear = new Date(priceData[i+1].date).getFullYear();
            if (nextYear !== currentYear) {
                isYearEnd = true;
            }
        }

        if (isYearEnd) {
            const diffYearsCurrent = (new Date(day.date) - new Date(priceData[0].date)) / (1000 * 60 * 60 * 24 * 365.25);
            // 최초 투자금(initCashVal) 대비 현재 자산 기준 CAGR
            const currentCagr = (diffYearsCurrent > 0 && asset > 0) 
                ? (Math.pow((asset / initCashVal), (1 / diffYearsCurrent)) - 1) * 100 
                : 0;

            const currentTotalDays = i + 1;
            const currentAvgStockRatio = currentTotalDays > 0 ? sumStockRatio / currentTotalDays : 0;
            const currentAvgDd = currentTotalDays > 0 ? sumDd / currentTotalDays : 0;
            const currentAvgAsset = currentTotalDays > 0 ? sumAsset / currentTotalDays : 0;

            // [요청 반영] 년도별 통계에도 lastStatus 항목 모두 추가
            const curCumulativeReturn = ((asset - initCashVal) / initCashVal) * 100;
            const curRiskRewardRatio = mddRate !== 0 ? Math.abs(currentCagr / mddRate) : 0;

            const curBuyFillRate = (totalBuyCount + totalBuyMissCount) > 0 
                ? totalBuyCount / (totalBuyCount + totalBuyMissCount) 
                : 0;
            
            const curSellFillRate = (totalSellCount + totalSellMissCount) > 0 
                ? totalSellCount / (totalSellCount + totalSellMissCount) 
                : 0;

            const curDailyTurnoverFreq = currentTotalDays > 0 ? (totalBuyCount + totalSellCount) / currentTotalDays : 0;
            
            const curTotalTurnoverRate = (currentAvgAsset > 0) 
                ? ((totalBuyAmt + totalSellAmt) / 2) / currentAvgAsset 
                : 0;
            
            const curBuyAlarmRate = currentTotalDays > 0 ? totalBuyAlarmCount / currentTotalDays : 0;
            const curSellAlarmRate = currentTotalDays > 0 ? totalSellAlarmCount / currentTotalDays : 0;

            yearlyReturns.push({
                date: dateStr,
                asset: asset,
                stockRatio: stockRatio,
                avgPrice: avgPrice,
                shares: shares,
                curLower: curLower,
                curUpper: curUpper,
                // 누적 통계치
                mdd_rate: mddRate, 
                final_cagr: currentCagr,
                total_fail_count: totalFailCount,
                total_Withdrawal: totalWithdrawal,
                total_BuyAmt: totalBuyAmt,
                total_SellAmt: totalSellAmt,
                total_Profit: totalProfit,
                total_Tax: totalTax, 
                total_Fee: totalFee,
                total_BuyCount: totalBuyCount,
                total_SellCount: totalSellCount,
                total_BuyAlarmCount: totalBuyAlarmCount,
                total_SellAlarmCount: totalSellAlarmCount,
                total_days: currentTotalDays,
                avg_StockRatio: currentAvgStockRatio,
                avg_dd: currentAvgDd,

                // [신규 추가 항목 - 년도별]
                cumulativeReturn: curCumulativeReturn,
                maxRecoveryDays: maxRecoveryDays,
                riskRewardRatio: curRiskRewardRatio,
                maxStockRatio: maxStockRatio,
                minStockRatio: minStockRatio,
                buyRangeMissCount: totalBuyMissCount,
                sellRangeMissCount: totalSellMissCount,
                buyFillRate: curBuyFillRate,
                sellFillRate: curSellFillRate,
                dailyTurnoverFreq: curDailyTurnoverFreq,
                totalTurnoverRate: curTotalTurnoverRate,
                buyAlarmRate: curBuyAlarmRate,
                sellAlarmRate: curSellAlarmRate
            });
        }
    }

    // 최종 요약본 생성
    // [수정] 루프 밖에서 사용할 마지막 날짜 계산
    const lastDayData = priceData[priceData.length - 1];
    const lastDateStr = lastDayData.date instanceof Date 
        ? lastDayData.date.toISOString().split('T')[0] 
        : String(lastDayData.date).split('T')[0];    
        
    const lastRow = (rows && rows.length > 0) ? rows[rows.length - 1] : {
        date: lastDateStr, // [수정] dateStr -> lastDateStr 로 변경
        asset: last_asset,
        stockRatio: (shares * priceData[priceData.length-1].close_price) / last_asset * 100,
        avgPrice: shares > 0 ? totalPurchaseAmt / shares : 0,
        shares: shares,
        curLower: last_curLower,
        curUpper: last_curUpper
    };

    const diffTotalYears = (new Date(priceData[priceData.length-1].date) - new Date(priceData[0].date)) / (1000 * 60 * 60 * 24 * 365.25);
    const finalCagr = (diffTotalYears > 0 && last_asset > 0) 
        ? (Math.pow((last_asset / initCashVal), (1 / diffTotalYears)) - 1) * 100 
        : 0;

    // [추가] 평균값 및 신규 통계 계산
    const totalDays = priceData.length;
    const avgStockRatio = totalDays > 0 ? sumStockRatio / totalDays : 0;
    const avgDd = totalDays > 0 ? sumDd / totalDays : 0;
    const avgAsset = totalDays > 0 ? sumAsset / totalDays : 0;

    // 신규 항목 계산
    const cumulativeReturn = ((last_asset - initCashVal) / initCashVal) * 100;
    const riskRewardRatio = mddRate !== 0 ? Math.abs(finalCagr / mddRate) : 0; 
    
    const buyFillRate = (totalBuyCount + totalBuyMissCount) > 0 
        ? totalBuyCount / (totalBuyCount + totalBuyMissCount) 
        : 0;
    
    const sellFillRate = (totalSellCount + totalSellMissCount) > 0 
        ? totalSellCount / (totalSellCount + totalSellMissCount) 
        : 0;
        
    const dailyTurnoverFreq = totalDays > 0 ? (totalBuyCount + totalSellCount) / totalDays : 0;
    
    // 총 회전율 = (매수 + 매도) / 2 / 평잔
    const totalTurnoverRate = (avgAsset > 0) 
        ? ((totalBuyAmt + totalSellAmt) / 2) / avgAsset 
        : 0;

    const buyAlarmRate = totalDays > 0 ? totalBuyAlarmCount / totalDays : 0;
    const sellAlarmRate = totalDays > 0 ? totalSellAlarmCount / totalDays : 0;


    return { 
        rows, 
        chartData, 
        chartArrays: (rType === 2 ? chartArrays : null),
        recentHistory: (rType === 3 ? recentHistory : null),
        yearlyReturns,
        lastStatus: { 
            ...lastRow, 
            mdd_rate: mddRate,
            final_cagr: finalCagr,
            total_fail_count: totalFailCount,
            total_Withdrawal: totalWithdrawal,
            total_BuyAmt: totalBuyAmt,
            total_SellAmt: totalSellAmt,
            total_Profit: totalProfit,
            total_Tax: totalTax, 
            total_Fee: totalFee,
            total_BuyCount: totalBuyCount,
            total_SellCount: totalSellCount,
            total_BuyAlarmCount: totalBuyAlarmCount,
            total_SellAlarmCount: totalSellAlarmCount,
            total_days: totalDays,
            avg_StockRatio: avgStockRatio,
            avg_dd: avgDd,
            
            // [요청 추가 항목]
            cumulativeReturn,       // 누적수익율
            maxRecoveryDays,        // 최대회복기간
            riskRewardRatio,        // 위험보상비율
            
            maxStockRatio,          // 최대주식비중
            minStockRatio,          // 최소주식비중
            
            buyRangeMissCount: totalBuyMissCount,   // 매수범위이탈횟수
            sellRangeMissCount: totalSellMissCount, // 매도범위이탈횟수
            
            buyFillRate,            // 매수체결률
            sellFillRate,           // 매도체결률
            dailyTurnoverFreq,      // 일평균 매매빈도
            totalTurnoverRate,      // 총 회전율
            
            buyAlarmRate,           // 매수알림률
            sellAlarmRate           // 매도알림률
        }
    };
}


