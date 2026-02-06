// ===========================================================================
// [파일명] : routes/yahoo.js
// [대상]   : Yahoo Finance API 연동 및 주가 정보 보완 업데이트
// [기준]   : 
//   1. 안정성 확보: 30초 타임아웃 및 랜덤 지연(1~2초)을 적용하여 API 차단을 방지한다.
//   2. 데이터 정밀도: 모든 수치(O, H, L, C)는 소수점 4자리 버림 처리를 통해 일관성을 유지한다.
//   3. 효율적 수집: 대량 데이터 요청 시 3년 단위의 청크(Chunking) 방식으로 나누어 수집한다.
//   4. 용량 최적화: Firestore 단일 문서 제한(1MB)을 상시 체크하며 초과 시 업로드를 차단한다.
// ===========================================================================
const express = require('express');
const router = express.Router();
const admin = require('firebase-admin');
const firestore = admin.firestore();
const { verifyToken } = require('../utils/authHelper');
const YahooFinance = require('yahoo-finance2').default;

// server.js 상단, YahooFinance 생성 부분 수정
const customFetch = async (url, options = {}) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30000); // 30초 타임아웃

    try {
        const response = await fetch(url, {
            ...options,
            headers: {
                ...options.headers,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
            },
            signal: controller.signal  // 타임아웃 시 abort
        });
        clearTimeout(timeoutId);
        return response;
    } catch (error) {
        clearTimeout(timeoutId);
        if (error.name === 'AbortError') {
            throw new Error('Request timeout after 30 seconds');
        }
        throw error;
    }
};

// 인스턴스 생성 시 커스텀 fetch 전달
const yahooFinance = new YahooFinance({ fetch: customFetch });

// ----------------------------------------------------------------
// 2. 야후 파이낸스 가격 업데이트 (Upsert 로직 적용)
// ----------------------------------------------------------------
// 1초 ~ 2초 사이 랜덤 딜레이 (1000ms ~ 2000ms)
function delay() {
    const min = 1000;  // 1초
    const max = 2000;  // 2초
    const delayTime = Math.floor(Math.random() * (max - min + 1)) + min;
    return new Promise(resolve => setTimeout(resolve, delayTime));
}

async function fetchWithChunks(ticker, start, end) {
    const chunks = [];
    let current = new Date(start);
    const endDate = new Date(end);

    while (current <= endDate) {
        let chunkEnd = new Date(current);
        chunkEnd.setFullYear(chunkEnd.getFullYear() + 3);
        if (chunkEnd > endDate) chunkEnd = endDate;

        const p1 = current.toISOString().split('T')[0];
        const p2 = chunkEnd.toISOString().split('T')[0];

        console.log(`[${ticker}] 청크 요청: ${p1} ~ ${p2}`);
        try {
            const result = await yahooFinance.chart(ticker, { period1: p1, period2: p2, interval: '1d' });
            if (result.quotes?.length > 0) {
                chunks.push(...result.quotes);
            }
        } catch (err) {
            console.error(`[${ticker}] 청크 실패 (${p1}~${p2}):`, err.message);
        }

        current = new Date(chunkEnd);
        current.setDate(current.getDate() + 1);
    }
    return chunks;
}

router.post('/update-prices', verifyToken, async (req, res) => {
    try {
        const { startDate, endDate, tickers } = req.body; 
        
        let targetTickers = [];
        if (tickers && tickers.length > 0) {
            targetTickers = tickers.map(s => ({ ticker: s }));
        } else {
            // [핵심 수정] 티커가 없으면 Firestore 'tickers' 컬렉션에서 전체 목록을 가져옴
            const snapshot = await firestore.collection('tickers').get();
            targetTickers = snapshot.docs.map(doc => ({ ticker: doc.id }));
            console.log(`[Batch] DB에서 ${targetTickers.length}개의 티커를 불러왔습니다.`);
        }

        const today = new Date();
        today.setUTCHours(0, 0, 0, 0);

        let endDateObj = endDate ? new Date(endDate) : today;
        if (endDateObj > today) {
            endDateObj = new Date(today);
            endDateObj.setDate(endDateObj.getDate() - 1);
        }
        endDateObj.setDate(endDateObj.getDate() + 1);
        const period2 = endDateObj.toISOString().split('T')[0];

        let startDateObj = startDate ? new Date(startDate) : new Date('2010-01-01');
        if (startDateObj > endDateObj) startDateObj = new Date(endDateObj);
        const period1 = startDateObj.toISOString().split('T')[0];

        console.log(`[작업 시작] 총 ${targetTickers.length}개 티커 수집 시작 (${period1} ~ ${period2})`);

        const results = [];

        for (const item of targetTickers) {
            const ticker = item.ticker.trim().toUpperCase();
            try {
                console.log(`[티커 : ${ticker} ] 야후 데이터 호출...`);
                const history = await fetchWithChunks(ticker, period1, period2);

                if (history && history.length > 0) {
                    // --- 1. Firestore 형식으로 데이터 가공 ---
                    // 날짜(labels)와 종가(values) 배열 생성
                    const labels = [];
                    const priceData = []; // values 대신 더 명확한 이름 사용

                    for (const quote of history) {
                        if (!quote.date || !quote.close) continue;
                        
                        const quoteDate = new Date(quote.date).toISOString().split('T')[0];
                        labels.push(quoteDate);

                        // 소수점 4자리 버림 처리를 적용한 객체 생성
                        const truncate = (val) => Math.floor(val * 10000) / 10000;

                        priceData.push({
                            o: truncate(quote.open || quote.close), // 시가 (없으면 종가로 대체)
                            h: truncate(quote.high || quote.close), // 고가
                            l: truncate(quote.low || quote.close),  // 저가
                            c: truncate(quote.close)                // 종가
                        });
                    }

                    // 업로드 페이로드 수정
                    const uploadPayload = {
                        ticker: ticker,
                        last_updated: new Date().toISOString(),
                        labels: labels,
                        prices: priceData // 객체 배열로 저장
                    };

                    const collectionName = "ticker_prices";
                    const docId = ticker;

                    // 용량 체크
                    const jsonString = JSON.stringify(uploadPayload);
                    const byteSize = Buffer.byteLength(jsonString, 'utf8');

                    if (byteSize > 1048576) {
                        throw new Error(`용량 초과 (${(byteSize / 1024).toFixed(2)} KB)`);
                    }

                    // Firestore에 통째로 저장 (set)
                    await firestore.collection(collectionName).doc(docId).set({
                        ...uploadPayload,
                        updatedAt: admin.firestore.FieldValue.serverTimestamp()
                    }, { merge: true });

                    console.log(`[${ticker}] Firestore 업로드 완료 (${labels.length}건, ${(byteSize / 1024).toFixed(2)} KB)`);
                    results.push({ ticker: ticker, status: 'Success', count: labels.length, size: `${(byteSize / 1024).toFixed(2)} KB` });

                } else {
                    results.push({ ticker: ticker, status: 'No Data' });
                }

                await delay(); // 티커 간 대기

            } catch (err) {
                console.error(`[${ticker}] 에러:`, err.message);
                results.push({ ticker: ticker, status: 'Failed', error: err.message });
            }
        }
        res.json({ success: true, details: results });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

/**
 * [추가] 야후 파이낸스 티커 정보 조회 API (프록시)
 * 프론트엔드에서 티커 유효성 검사 및 자동 명칭 완성을 위해 호출함
 */
router.get('/proxy/yahoo-info', verifyToken, async (req, res) => {
    const { ticker } = req.query;
    if (!ticker) return res.status(400).json({ error: "티커 코드가 필요합니다." });

    try {
        const cleanTicker = ticker.trim().toUpperCase();
        // quote는 단일 객체를 반환하지만, 에러 방지를 위해 방어적으로 처리
        const result = await yahooFinance.quote(cleanTicker);

        if (result && result.shortName) {
            res.json({
                symbol: result.symbol,
                shortName: result.shortName || result.longName || "명칭 없음",
                price: result.regularMarketPrice
            });
        } else {
            res.status(404).json({ error: "Not Found" });
        }
    } catch (error) {
        console.error(`[Yahoo Proxy Error] ${ticker}:`, error.message);
        // 야후 API가 에러를 던지면 티커가 없는 것으로 간주하여 404 반환
        res.status(404).json({ error: "유효하지 않은 티커입니다." });
    }
});

module.exports = router;