// File Name : server.js
const express = require('express');
const path = require('path');
const db = require('./db');

require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

// 정적 파일 설정 및 JSON 파싱 미들웨어
app.use(express.static(path.join(__dirname, '../front')));
app.use(express.json({
    limit: '50mb' // 최대 용량을 50MB로 설정
}));

app.use(express.urlencoded({
    limit: '50mb',
    extended: true
}));

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

// firebase 연결    ---------  최 상단에 둬야 함 - 시작
const admin = require('firebase-admin');
const serviceAccount = {
  projectId: process.env.FIREBASE_PROJECT_ID,
  clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
  // \n 문자 처리를 위해 아래와 같이 작성합니다.
  privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n')
};

// Firebase Admin SDK 초기화
if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount)
    });
}

const firestore = admin.firestore();

const nodemailer = require('nodemailer');

console.log("Firebase Admin SDK가 성공적으로 연결되었습니다.");

// 사용자 인증 확인을 위한 미들웨어 함수
const verifyToken = async (req, res, next) => {
    const idToken = req.headers.authorization?.split('Bearer ')[1];

    if (!idToken) {
        return res.status(401).json({ error: "로그인이 필요합니다." });
    }

    try {
        const decodedToken = await admin.auth().verifyIdToken(idToken);
        req.user = decodedToken; // 인증된 사용자 정보(uid 등)를 요청 객체에 담음
        next();
    } catch (error) {
        console.error("토큰 검증 에러:", error);
        res.status(403).json({ error: "유효하지 않은 토큰입니다." });
    }
};
// firebase 연결    ---------  최 상단에 둬야 함 - 끝

// 백엔드 API 예시: /api/firestore/upload
app.post('/api/upload-to-firestore', async (req, res) => {
    try {
        const { collectionName, docId, data } = req.body;

        if (!collectionName || !data) {
            return res.status(400).json({ error: "컬렉션 이름과 데이터는 필수입니다." });
        }

        const jsonString = JSON.stringify(data);
        // 로그 출력은 유지하되 보안상 필요한 정보만 남깁니다.
        console.log(`[Firestore Upload] ${collectionName}/${docId || 'new'} - Size: ${(jsonString.length / 1024).toFixed(2)} KB`);

        if (jsonString.length > 1048487) {
            return res.status(413).json({ error: "Firestore 단일 문서 용량 제한(1MB)을 초과했습니다." });
        }

        // 변수명 확인: firestoree 인지 firestore 인지 확인 필요
        const colRef = firestore.collection(collectionName);
        
        if (docId) {
            await colRef.doc(docId).set({
                ...data,
                updatedAt: admin.firestore.FieldValue.serverTimestamp()
            }, { merge: true });
            // 응답 키값을 docId로 통일
            res.status(200).json({ success: true, docId: docId });
        } else {
            const docRef = await colRef.add({
                ...data,
                createdAt: admin.firestore.FieldValue.serverTimestamp(),
                updatedAt: admin.firestore.FieldValue.serverTimestamp()
            });
            res.status(200).json({ success: true, docId: docRef.id });
        }
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// [공통] 특정 컬렉션의 문서 삭제
app.delete('/api/delete-from-firestore', async (req, res) => {
    try {
        const { collectionName, id } = req.query; // 프론트엔드 호출 규격에 맞춰 id로 받음

        if (!collectionName || !id) {
            return res.status(400).json({ error: "컬렉션 이름과 문서 ID는 필수입니다." });
        }

        // [보안] 관리자 권한 체크 로직을 여기에 추가할 수 있습니다.
        // 예: if (req.headers['x-admin-role'] !== 'admin') return res.status(403)...

        console.log(`[Firestore Delete] ${collectionName}/${id}`);

        await firestore.collection(collectionName).doc(id).delete();

        res.status(200).json({ success: true, message: "성공적으로 삭제되었습니다." });
    } catch (error) {
        console.error("Delete Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// [백엔드] 수정된 리스트 조회 API
app.get('/api/firestore/list/:collectionName', async (req, res) => {
    try {
        const { collectionName } = req.params;
        // updatedAt 필드가 없는 이전 데이터를 고려하여 정렬 조건은 선택적으로 적용하거나 제거할 수 있습니다.
        const snapshot = await firestore.collection(collectionName).get();

        if (snapshot.empty) {
            return res.json([]);
        }

        const list = snapshot.docs.map(doc => {
            const fullData = doc.data();
            
            // [핵심 수정] metadata 필드가 있으면 그것을 사용하고, 없으면 전체 데이터를 사용
            const dataContent = fullData.metadata ? fullData.metadata : fullData;
            
            return {
                id: doc.id,
                ...dataContent 
            };
        });

        res.json(list);
    } catch (error) {
        console.error("List Fetch Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// [공통] 특정 컬렉션의 상세 문서 가져오기 (전체 데이터)
app.get('/api/firestore/detail/:collectionName/:docId', async (req, res) => {
    try {
        const { collectionName, docId } = req.params;
        const doc = await firestore.collection(collectionName).doc(docId).get();

        if (!doc.exists) {
            return res.status(404).json({ error: "데이터를 찾을 수 없습니다." });
        }

        res.json(doc.data());
    } catch (error) {
        console.error("Detail Fetch Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// 관리자 이메일 목록 (본인의 이메일을 넣으세요)
const ADMIN_EMAILS = ['your-email@gmail.com', 'partner-email@gmail.com'];

app.post('/api/auth/google', async (req, res) => {
    const { token } = req.body;
    try {
        // 구글 토큰 검증 (google-auth-library 사용 권장)
        // 여기서는 검증되었다고 가정하고 이메일을 추출하는 로직으로 설명합니다.
        const payload = decodeGoogleToken(token); // 토큰 디코딩 함수 필요
        const email = payload.email;

        // 관리자 여부 확인
        const role = ADMIN_EMAILS.includes(email) ? 'admin' : 'user';

        res.json({ 
            success: true, 
            email: email,
            role: role 
        });
    } catch (error) {
        res.status(401).json({ success: false, message: '인증 실패' });
    }
});

// ----------------------------------------------------------------
// 어디서든 사용할 함수
// ----------------------------------------------------------------
// Helper: nper 로직
function nper_custom(rate, pv, fv) {
    try {
        const val = Math.abs(fv) / pv;
        if (val <= 0) return 0;
        return Math.log(val) / Math.log(1 + rate);
    } catch (e) {
        return 0;
    }
}

// [수정된 핵심 로직 함수] 권한과 모드에 따라 DB 또는 파이어스토어 선택 조회
async function getDailyStockData(role, mode, ticker, start, end) {
    // 관리자이면서 관리자 모드인 경우만 로컬 MySQL 조회
    const isFullAdmin = (role === 'admin' && mode === 'admin');

    try {
        if (isFullAdmin) {
            // --- 1. 로컬 MySQL DB 조회 경로 ---
            let query = 'SELECT date, open_price, high_price, low_price, close_price FROM trade_data_day WHERE ticker = ?';
            let params = [ticker];
            if (start && end) {
                query += ' AND date BETWEEN ? AND ?';
                params.push(start, end);
            }
            query += ' ORDER BY date ASC';

            const [rows] = await db.query(query, params);
            return rows;
        } else {
            // --- 2. 파이어스토어 조회 경로 (사용자 모드) ---
            const doc = await firestore.collection('ticker_prices').doc(ticker).get();
            
            if (!doc.exists) {
                console.warn(`파이어스토어에 데이터 없음: ${ticker}`);
                return []; // 데이터가 없으면 빈 배열 반환
            }

            const data = doc.data();
            const labels = data.labels || [];
            const values = data.values || [];

            // 파이어스토어의 배열 구조를 MySQL 결과와 동일한 객체 배열 포맷으로 변환
            const rows = labels.map((date, index) => {
                const dDate = date.includes('T') ? date.split('T')[0] : date;
                return {
                    date: dDate,
                    close_price: values[index],
                    // 파이어스토어에는 시가/고가/저가가 없을 수 있으므로 종가로 대체하거나 처리
                    open_price: values[index],
                    high_price: values[index],
                    low_price: values[index]
                };
            }).filter(row => {
                // 기간 필터링
                if (start && end) return row.date >= start && row.date <= end;
                return true;
            });

            // 날짜순 정렬
            return rows.sort((a, b) => a.date.localeCompare(b.date));
        }
    } catch (err) {
        console.error(`데이터 조회 에러 (${ticker}):`, err.message);
        throw err;
    }
}

// [백엔드 수정] 권한에 따라 로컬 DB 또는 Firestore에서 데이터를 조회하는 표준 API
// [백엔드] 권한 기반 통합 주가 조회 API
app.get('/api/stocks-bulk', async (req, res) => {
    // mode 파라미터 추가 수신
    const { role, mode, tickers, start, end } = req.query; 
    if (!tickers) return res.status(400).json({ error: "티커 코드가 없습니다." });

    const tickerList = tickers.split(',');

    // 최종 판정: role과 mode가 모두 admin일 때만 MySQL로 분기
    const isFullAdminRequest = (role === 'admin' && mode === 'admin');

    try {
        // [수정] 권한이 admin이고, 현재 페이지 모드도 admin일 때만 로컬 DB 조회
        if (isFullAdminRequest) {
            let query = 'SELECT ticker, date, close_price FROM trade_data_day WHERE ticker IN (?)';
            let params = [tickerList];

            if (start && end) {
                query += ' AND date BETWEEN ? AND ?';
                params.push(start, end);
            }
            query += ' ORDER BY date ASC';

            const [rows] = await db.query(query, params);
            return res.json(rows);
        } 
        
        // 2. 일반 사용자(user) 또는 권한 미지정: Firestore 'ticker_prices' 컬렉션 조회
        else {
            const bulkResults = [];
            
            // 병렬 처리를 통해 여러 티커 데이터를 Firestore에서 가져옴
            await Promise.all(tickerList.map(async (ticker) => {
                const doc = await firestore.collection('ticker_prices').doc(ticker).get();
                
                if (doc.exists) {
                    const data = doc.data();
                    const labels = data.labels || [];
                    const values = data.values || [];

                    // Firestore 배열 구조를 [{ticker, date, close_price}, ...] 형식으로 변환
                    labels.forEach((date, index) => {
                        const dDate = date.includes('T') ? date.split('T')[0] : date;
                        // 요청된 기간 필터링
                        if (dDate >= start && dDate <= end) {
                            bulkResults.push({
                                ticker: ticker,
                                date: dDate,
                                close_price: values[index]
                            });
                        }
                    });
                }
            }));

            // 날짜 기준 오름차순 정렬 (차트 렌더링용)
            bulkResults.sort((a, b) => a.date.localeCompare(b.date));
            return res.json(bulkResults);
        }
    } catch (err) {
        console.error(`Bulk fetch error:`, err.message);
        res.status(500).json({ error: "주가 데이터를 불러오는 중 오류가 발생했습니다." });
    }
});

// ----------------------------------------------------------------
// 티커 마스터 관리 API
// ----------------------------------------------------------------

// Helper: DB에서 심볼 데이터 가져오기

// 티커 전체 조회
// [변경] Firestore에서 가져오기 (백엔드가 프록시 역할)
// 티커 전체 조회
// [변경] 지수(^) 우선 정렬 로직 및 데이터 평탄화 적용
app.get('/api/tickers', async (req, res) => {
    try {
        // [수정] orderBy('ticker') 대신 데이터를 가져온 후 커스텀 정렬을 수행합니다.
        const snapshot = await firestore.collection('tickers').get();
        
        const tickers = snapshot.docs.map(doc => {
            const fullData = doc.data();
            // [구조 통일] metadata 주머니가 있으면 풀어서 반환, 없으면 그대로 반환
            const dataContent = fullData.metadata ? fullData.metadata : fullData;
            
            return {
                id: doc.id,
                ...dataContent
            };
        });

        // [핵심 로직] ^로 시작하는 지수를 상단으로 보내는 정렬
        tickers.sort((a, b) => {
            const aId = (a.id || a.ticker || "").trim().toUpperCase();
            const bId = (b.id || b.ticker || "").trim().toUpperCase();
            
            const aIsIndex = aId.startsWith('^');
            const bIsIndex = bId.startsWith('^');

            if (aIsIndex === bIsIndex) {
                // numeric: true를 주면 문자열 속 숫자 정렬도 자연스러워집니다.
                return aId.localeCompare(bId, undefined, { numeric: true, sensitivity: 'base' });
            }
            
            return aIsIndex ? -1 : 1;
        });

        // 기존 프론트엔드들이 기대하는 JSON 형식 그대로 반환
        res.json(tickers); 
    } catch (err) {
        console.error("Firestore 조회 에러:", err);
        res.status(500).json({ error: "클라우드 데이터를 불러올 수 없습니다." });
    }
});

// ----------------------------------------------------------------
// 2. 야후 파이낸스 가격 업데이트 (Upsert 로직 적용)
// ----------------------------------------------------------------

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

// 1초 ~ 2초 사이 랜덤 딜레이 (1000ms ~ 2000ms)
function delay() {
    const min = 1000;  // 1초
    const max = 2000;  // 2초
    const delayTime = Math.floor(Math.random() * (max - min + 1)) + min;
    return new Promise(resolve => setTimeout(resolve, delayTime));
}

app.post('/api/update-prices', async (req, res) => {
    try {
        const { startDate, endDate, tickers } = req.body; 
        
        let targetTickers = [];
        if (tickers && tickers.length > 0) {
            targetTickers = tickers.map(s => ({ ticker: s }));
        } else {
            const [rows] = await db.query('SELECT ticker FROM ticker_master');
            targetTickers = rows;
        }

        // Grok 이 추천한 코드
        const today = new Date();
        today.setUTCHours(0, 0, 0, 0); // 오늘 00:00 UTC로 맞춤

        let endDateObj = endDate ? new Date(endDate) : today;

        // 미래라면 오늘로 클립
        if (endDateObj > today) {
            console.warn(`endDate(${endDate})가 미래/오늘입니다. 오늘 이전으로 제한합니다.`);
            endDateObj = new Date(today);
            endDateObj.setDate(endDateObj.getDate() - 1); // 안전하게 어제까지
        }

        // 여기서 핵심! Yahoo는 period2를 exclusive로 취급하므로 +1일
        endDateObj.setDate(endDateObj.getDate() + 1);
        const period2 = endDateObj.toISOString().split('T')[0]; // YYYY-MM-DD 형식

        // === period1 정의 추가 (이게 빠졌어요!) ===
        let startDateObj = startDate ? new Date(startDate) : new Date('2010-01-01'); // 안전한 과거 기본값

        // 시작일이 종료일보다 미래면 안 됨
        if (startDateObj > endDateObj) {
            console.warn(`startDate(${startDate})가 endDate보다 미래입니다. endDate로 조정합니다.`);
            startDateObj = new Date(endDateObj);
        }

        const period1 = startDateObj.toISOString().split('T')[0]; // ← 이 줄이 누락됐어요!

        // ----------------------------------------------------

        // (로그 위치 수정) 여기서 ticker를 쓰면 에러가 납니다. 아래처럼 수정하세요.
        console.log(`[작업 시작] 총 ${targetTickers.length}개 티커 수집 시작`);
        console.log(`요청 기간: ${period1} ~ ${period2}`);

        const results = [];

        for (const item of targetTickers) {
            const ticker = item.ticker.trim().toUpperCase();
            try {
                console.log(`[티커 : ${ticker} ]`);

                const history = await fetchWithChunks(ticker, period1, period2);

                console.log(`[${ticker}] 수신정보 건수 : ${history.length}건`);

                if (history && history.length > 0) {
                    for (const quote of history) {
                        if (!quote.date) continue;

                        // === [추가] 0 데이터 필터링 로직 ===
                        // 시가, 고가, 저가, 종가 중 하나라도 0이거나 유효하지 않으면 저장하지 않음
                        if (!quote.open || !quote.high || !quote.low || !quote.close || 
                            quote.open === 0 || quote.high === 0 || quote.low === 0 || quote.close === 0) {
                            console.log(`[${ticker}] ${quote.date} 데이터가 0이므로 건너뜁니다.`);
                            continue; 
                        }
                        // =================================

                        const quoteDate = new Date(quote.date).toISOString().split('T')[0];
                        
                        // ON DUPLICATE KEY UPDATE: 있으면 업데이트, 없으면 삽입
                        const sql = `
                            INSERT INTO trade_data_day (ticker, date, open_price, high_price, low_price, close_price, volume)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON DUPLICATE KEY UPDATE 
                                open_price = VALUES(open_price),
                                high_price = VALUES(high_price),
                                low_price = VALUES(low_price),
                                close_price = VALUES(close_price),
                                volume = VALUES(volume)
                        `;
                        
                        await db.query(sql, [
                            ticker, quoteDate, 
                            quote.open || 0, quote.high || 0, 
                            quote.low || 0, quote.close || 0, 
                            quote.volume || 0
                        ]);
                    }
                    results.push({ ticker: ticker, status: 'Success', count: history.length });
                } else {
                    results.push({ ticker: ticker, status: 'No Data' });
                }

                // 여기 추가: 다음 티커 처리 전에 1~2초 대기
                console.log(`[${ticker}] 처리 완료. 다음 티커로 넘어가기 전 대기 중...`);
                await delay();  // ← 이 부분이 핵심!

            } catch (err) {
                results.push({ ticker: ticker, status: 'Failed', error: err.message });
            }
            console.log(`[${ticker}] 티커별 가져오기 완료!`);
        }
        res.json({ success: true, details: results });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// ----------------------------------------------------------------
// 3. 주가 데이터 조회 및 분석 API
// ----------------------------------------------------------------

// 특정 티커의 싸이클 계산
app.get('/api/daily-stock', async (req, res) => {
// 모든 인자를 query에서 한 번에 구조 분해 할당
    const { role, mode, ticker, startDate, endDate, upperRate: uR, lowerRate: lR } = req.query;
    const upperRate = parseFloat(uR) || 30; 
    const lowerRate = parseFloat(lR) || 15;

    console.log(`[조회 시작] ticker: ${ticker}, StartDate: ${startDate}`); // 디버깅용

    try {
        // 1. 쿼리 실행 (ticker 대소문자 무시 등 대비)
        const rows = await getDailyStockData(role, mode, ticker, startDate, endDate);

        console.log(`[쿼리 결과] 데이터 개수: ${rows.length}건`);

        if (rows.length === 0) {
            return res.json([]); // 데이터가 없으면 빈 배열 반환 -> 프론트에서 "데이터가 없습니다" 알림 발생
        }

        let hMax = -Infinity; 
        let rMax = parseFloat(rows[0].high_price); 
        let rMin = parseFloat(rows[0].low_price);  
        let currentStatus = "-";

        const results = rows.map((row, index) => {
            try {
                const high = parseFloat(row.high_price);
                const low = parseFloat(row.low_price);
                const close = parseFloat(row.close_price);
                
                // 날짜 처리 방어 코드
                if (!row.date) return null;
                const dateObj = new Date(row.date);
                if (isNaN(dateObj.getTime())) return null; // 유효하지 않은 날짜 패스

                const currentRowDate = dateObj.toISOString().split('T')[0];

                // 1. historicMax (시작일 이후 갱신)
                if (!startDate || currentRowDate >= startDate) {
                    if (high > hMax) hMax = high;
                }

                // 2. 싸이클 판단 로직
                let judgeDrop = ((low - rMax) / rMax * 100);
                let judgeRise = ((high - rMin) / rMin * 100);

                let prevStatus = currentStatus;
                let turnToDown = "";
                let turnToUp = "";

                if (currentStatus !== "하락" && Math.abs(judgeDrop) >= lowerRate) {
                    currentStatus = "하락";
                    turnToDown = "O";
                } else if (currentStatus !== "상승" && Math.abs(judgeRise) >= upperRate) {
                    currentStatus = "상승";
                    turnToUp = "O";
                }

                // 3. 싸이클 전환 및 극값 갱신
                let renewedHigh = "";
                let renewedLow = "";

                if (prevStatus === "상승" && currentStatus === "하락") {
                    rMin = low;
                } else if (prevStatus === "하락" && currentStatus === "상승") {
                    rMax = high;
                } else {
                    if (high > rMax) { rMax = high; renewedHigh = "O"; }
                    if (low < rMin) { rMin = low; renewedLow = "O"; }
                }

                const currentHMax = hMax === -Infinity ? 0 : hMax;

                return {
                    date: row.date,
                    open_price: row.open_price,
                    high_price: row.high_price,
                    low_price: row.low_price,
                    close_price: row.close_price,
                    historicMax: currentHMax,
                    dropFromHMax: currentHMax > 0 ? ((close - currentHMax) / currentHMax * 100).toFixed(2) : "0.00",
                    runningMax: rMax,
                    closeFromRMax: ((close - rMax) / rMax * 100).toFixed(2),
                    minFromRMax: ((low - rMax) / rMax * 100).toFixed(2),
                    runningMin: rMin,
                    closeFromRMin: ((close - rMin) / rMin * 100).toFixed(2),
                    maxFromRMin: ((high - rMin) / rMin * 100).toFixed(2),
                    renewedHigh: renewedHigh,
                    renewedLow: renewedLow,
                    turnToDown: turnToDown,
                    turnToUp: turnToUp,
                    cycleStatus: currentStatus
                };
            } catch (e) {
                console.error(`Row mapping error at index ${index}:`, e);
                return null;
            }
        }).filter(item => item !== null); // 에러 난 행 제외

        res.json(results);
    } catch (err) {
        console.error("[백엔드 에러]:", err.message);
        res.status(500).json({ error: err.message });
    }
});

// ----------------------------------------------------------------
// 목표기준 투자전략 API    -  2025.12.25 추가
// ----------------------------------------------------------------

// 전략 목록 조회
app.get('/api/strategies', async (req, res) => {
    try {
        const query = `
            SELECT 
                strategy_code, ticker,
                DATE_FORMAT(start_date, '%Y-%m-%d') as start_date, 
                init_cash, init_stock, target_rate, upper_rate, lower_rate, unit_gap,
                IFNULL(show_in_main, '') as show_in_main  -- NULL이면 빈 문자열로 처리
            FROM strategy_settings
            ORDER BY strategy_code ASC
        `;
        const [rows] = await db.query(query);
        res.json(rows);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/strategies', async (req, res) => {
    try {
        const { 
            strategy_code, ticker, start_date, init_cash, init_stock, 
            target_rate, upper_rate, lower_rate, unit_gap, show_in_main
        } = req.body;

        // 1. INSERT 부분의 ? 개수를 8개로 맞춤
        // 2. UPDATE 부분에서 strategy_code를 제외한 나머지 필드를 정확히 매칭
        const query = `
            INSERT INTO strategy_settings 
                (strategy_code, ticker, start_date, init_cash, init_stock, target_rate, upper_rate, lower_rate, unit_gap, show_in_main)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE 
                ticker = VALUES(ticker),
                start_date = VALUES(start_date),
                init_cash = VALUES(init_cash),
                init_stock = VALUES(init_stock),
                target_rate = VALUES(target_rate),
                upper_rate = VALUES(upper_rate),
                lower_rate = VALUES(lower_rate),
                unit_gap = VALUES(unit_gap),
                \`show_in_main\` = VALUES(\`show_in_main\`)
        `;

        const params = [
            strategy_code, ticker, start_date, init_cash, init_stock, 
            target_rate, upper_rate, lower_rate, unit_gap, show_in_main
        ];

        await db.query(query, params);
        res.json({ success: true });
    } catch (e) {
        console.error("전략 저장 에러:", e.message);
        res.status(500).json({ success: false, error: e.message });
    }
});

// 전략 삭제
app.delete('/api/strategies/:strategy_code', async (req, res) => {
    try {
        const { strategy_code } = req.params; // 여기서 decodeURIComponent가 자동 적용됨
        const query = "DELETE FROM strategy_settings WHERE strategy_code = ?";
        await db.query(query, [strategy_code]);
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// [수정] API 엔드포인트 부분
app.get('/api/simulation-summary', async (req, res) => {
    try {
        const { ticker, start, end, initCash, initStock, targetRate, upperRate, lowerRate, unitGap } = req.query;
        
        const [tickerData, ndxData] = await Promise.all([
            getDailyStockData('admin', 'admin', ticker, start, end),
            getDailyStockData('admin', 'admin', '^NDX', start, end)
        ]);

        if (!tickerData || tickerData.length === 0) {
            return res.status(404).json({ success: false, error: "조회된 데이터가 없습니다." });
        }

        // 새로 만든 함수 호출
        const result = await runSimulationInternal({
            ticker, start, end, initCash, initStock, targetRate, upperRate, lowerRate, unitGap
        });

        if (!result) return res.json({ success: false, message: "No data" });

        // [핵심] 시뮬레이션 결과와 별개로, DB 원본 데이터에서 직접 통계 추출
        const lastTicker = tickerData.length > 0 ? tickerData[tickerData.length - 1] : { close_price: 0, date: null };
        const lastNdx = ndxData.length > 0 ? ndxData[ndxData.length - 1] : { close_price: 0 };

        res.json({
            success: true,
            // NDX 영역: DB 원본 데이터(ndxData)를 직접 사용하여 계산
            ndx: {
                max: ndxData.length > 0 ? Math.max(...ndxData.map(d => d.close_price || 0)) : 0,
                last: lastNdx.close_price || 0
            },
            // 선택 티커 영역 (TQQQ 등): DB 원본 데이터(tickerData)를 직접 사용하여 계산
            tickerStats: {
                max: Number(tickerData.length > 0 ? Math.max(...tickerData.map(d => d.close_price || 0)) : 0),
                last: Number(tickerData.length > 0 ? tickerData[tickerData.length - 1].close_price : 0),
                date: tickerData.length > 0 ? tickerData[tickerData.length - 1].date : null
            },
            // 시뮬레이션 엔진이 계산한 결과들
            rows: result.rows,
            summary: result.lastStatus,
            chart: result.chartData,
        });
    } catch (e) {
        console.error("Simulation API Error:", e);
        res.status(500).json({ success: false, error: e.message });
    }
});

// [추가] 시뮬레이션 핵심 로직 - 대량/단일 공용 (기존 로직 완벽 복합)
async function runSimulationInternal(params) {
    const { ticker, start, end, initCash, initStock, targetRate, upperRate, lowerRate, unitGap } = params;

    // 1. 데이터 조회
    const priceData = await getDailyStockData('admin', 'admin', ticker, start, end);
    if (!priceData || priceData.length === 0) return null;

    // 2. 파라미터 초기화 (기존 변수명과 로직 준수)
    const initStockRate = parseFloat(initStock) / 100;
    const initCashVal = parseFloat(initCash);
    const targetYearRate = parseFloat(targetRate) / 100;
    const upperPct = parseFloat(upperRate) / 100;
    const lowerPct = parseFloat(lowerRate) / 100;
    const unitGapPct = parseFloat(unitGap) / 100;
    const targetDayRate = Math.pow(1 + targetYearRate, 1 / 365) - 1;

    // 초기값 설정 (기존 로직: 첫날 vBasis 설정 및 초기 매수)
    let vBasis = initCashVal * initStockRate;
    let cash = initCashVal - vBasis;
    let maxMddRate = 0;
    let highAsset = initCashVal;
    let shares = Math.floor(vBasis / parseFloat(priceData[0].close_price));
    let totalPurchaseAmt = shares * parseFloat(priceData[0].close_price);
    
    const chartData = { labels: [], ev: [], vU: [], vB: [], vL: [] };
    let rows = [];

    // 3. 루프 시작
    priceData.forEach((day, i) => {
        const open = parseFloat(day.open_price);
        const high = parseFloat(day.high_price);
        const low = parseFloat(day.low_price);
        const close = parseFloat(day.close_price);
        const startCash = cash;
        const prevShares = shares;
        const prevLower = i === 0 ? vBasis * lowerPct : rows[i-1].curLower;
        const prevUpper = i === 0 ? vBasis * upperPct : rows[i-1].curUpper;
        
        const dateStr = day.date instanceof Date ? day.date.toISOString().split('T')[0] : String(day.date).split('T')[0];

        // [매매단위수량 계산] 전일보유수량 * 단위gap (최소 1주)
        let diffDays = 0;
        let unitShares = Math.floor(prevShares * unitGapPct);

        if (unitShares <= 0) unitShares = 1;

        if (i > 0) {
            const prevDate = new Date(priceData[i-1].date);
            const currDate = new Date(priceData[i].date);
            diffDays = (currDate - prevDate) / (1000 * 60 * 60 * 24);
            vBasis *= Math.pow(1 + targetDayRate, diffDays);
        }

        const curUpper = vBasis * upperPct;
        const curLower = vBasis * lowerPct;

        // ----------------------------------------------------------------
        // [매수(Buy) 12단계 로직]
        // ----------------------------------------------------------------
        let bOpenReq = (prevShares * open < prevLower * (1 - unitGapPct)) ? (prevLower - (prevShares * open)) : 0;
        if (bOpenReq * 1.0007 > startCash) bOpenReq = startCash / 1.0007;

        let bOpenCount = (bOpenReq > 0 && prevShares > 0) ? Math.floor(nper_custom(-unitGapPct, prevLower / prevShares, -open)) : 0;
        let bOpenPrice = bOpenCount > 0 ? open : 0;
        let bOpenQty = bOpenCount * unitShares;
        let bOpenAmt = bOpenQty * bOpenPrice * 1.0007;

        let bLowReq = (prevShares * low < prevLower * (1 - unitGapPct)) ? (prevLower - (prevShares * low) - bOpenReq) : 0;
        if (bLowReq * 1.0007 > (startCash - bOpenAmt)) bLowReq = Math.max(0, (startCash - bOpenAmt) / 1.0007);

        let bLowCount = (bLowReq > 0 && prevShares > 0) ? Math.floor(nper_custom(-unitGapPct, prevLower / prevShares, -low)) - bOpenCount : 0;
        if (bLowCount < 0) bLowCount = 0;

        let bLowFirst = bLowCount > 0 ? (prevLower / prevShares) * Math.pow(1 - unitGapPct, bOpenCount + 1) : 0;
        let bLowLast = bLowCount > 0 ? (prevLower / prevShares) * Math.pow(1 - unitGapPct, bOpenCount + bLowCount) : 0;
        let bLowAvg = bLowFirst > 0 ? (bLowFirst + bLowLast) / 2 : 0;
        let bLowQty = bLowCount * unitShares;
        let bLowAmt = bLowQty * bLowAvg * 1.0007;
        let bAmt = bOpenAmt + bLowAmt;

        // ----------------------------------------------------------------
        // [매도(Sell) 12단계 로직]
        // ----------------------------------------------------------------
        let sOpenReq = (prevShares * open > prevUpper * (1 + unitGapPct)) ? (prevShares * open - prevUpper) : 0;
        
        let sOpenCount = (sOpenReq > 0 && prevShares > 0) ? Math.floor(nper_custom(unitGapPct, prevUpper / prevShares, -open)) : 0;
        let sOpenPrice = sOpenCount > 0 ? open : 0;
        let sOpenQty = sOpenCount * unitShares;
        let sOpenAmt = sOpenQty * sOpenPrice; // 매도는 현금 유입이므로 세금/수수료 생략(혹은 별도처리)

        let sHighReq = (prevShares * high > prevUpper * (1 + unitGapPct)) ? (prevShares * high - prevUpper - sOpenReq) : 0;
        
        let sHighCount = (sHighReq > 0 && prevShares > 0) ? Math.floor(nper_custom(unitGapPct, prevUpper / prevShares, -high)) - sOpenCount : 0;
        if (sHighCount < 0) sHighCount = 0;

        let sHighFirst = sHighCount > 0 ? (prevUpper / prevShares) * Math.pow(1 + unitGapPct, sOpenCount + 1) : 0;
        let sHighLast = sHighCount > 0 ? (prevUpper / prevShares) * Math.pow(1 + unitGapPct, sOpenCount + sHighCount) : 0;
        let sHighAvg = sHighFirst > 0 ? (sHighFirst + sHighLast) / 2 : 0;
        let sHighQty = sHighCount * unitShares;
        let sHighAmt = sHighQty * sHighAvg;
        let sAmt = sOpenAmt + sHighAmt;
      
        // 실제 자산 반영
        shares = prevShares + (bOpenQty + bLowQty) - (sOpenQty + sHighQty);
        cash = startCash - (bOpenAmt + bLowAmt) + (sOpenAmt + sHighAmt);

        const asset = cash + (shares * close);
        const evalAmt = shares * close;
        
        if (sOpenQty + sHighQty > 0 && prevShares > 0) {
            totalPurchaseAmt -= ((sOpenQty + sHighQty) * (totalPurchaseAmt / prevShares));
        }

        if (bOpenQty + bLowQty > 0) {
            totalPurchaseAmt += (bOpenQty * bOpenPrice) + (bLowQty * bLowAvg);
        }

        if ( asset > highAsset) { highAsset = asset };
        const mdd = highAsset > 0 ? ((asset - highAsset) / highAsset * 100) : 0;

        // 2. [추가] 전체 기간 중 가장 큰 하락폭(가장 낮은 음수)을 maxMddRate에 보관
        if (mdd < maxMddRate) { maxMddRate = mdd; }

        rows.push({
            date: dateStr,
            asset,
            mdd,
            stockRatio: shares > 0 ? (shares * close) / asset * 100 : 0,
            open, high, low, close,
            startCash, sAmt, bAmt, finalCash: cash,
            // 매수 12단계
            bOpenReq, bOpenCount, bOpenPrice, bOpenQty, bOpenAmt,
            bLowReq, bLowCount, bLowFirst, bLowLast, bLowAvg, bLowQty, bLowAmt,
            // 매도 12단계
            sOpenReq, sOpenCount, sOpenPrice, sOpenQty, sOpenAmt,
            sHighReq, sHighCount, sHighFirst, sHighLast, sHighAvg, sHighQty, sHighAmt,
            // 보유현황
            shares, 
            totalPurchaseAmt,
            avgPrice: shares > 0 ? totalPurchaseAmt / shares : 0,
            evalAmt,
            vBasis, curUpper, curLower, diffDays, unitShares
        });

        chartData.labels.push(dateStr);
        chartData.ev.push(Math.round(shares * close));
        chartData.vB.push(Math.round(vBasis));
        chartData.vU.push(Math.round(curUpper));
        chartData.vL.push(Math.round(curLower));
    });

    const lastRow = rows.length > 0 ? rows[rows.length - 1] : {};
    return { rows, chartData, lastStatus: { ...lastRow, max_mdd_rate: maxMddRate } };
}

/* [추가] 대량 시뮬레이션 및 결과 저장 API */
app.post('/api/bulk-simulation', async (req, res) => {
    const { strategy_code, ticker, bulkStart, bulkEnd, targetEnd, initCash, initStock, targetRate, upperRate, lowerRate, unitGap } = req.body;

    try {
        // 1. 기존 데이터 삭제
        await db.query(
            "DELETE FROM simulation_result WHERE strategy_code = ? AND start_date BETWEEN ? AND ?",
            [strategy_code, bulkStart, bulkEnd]
        );

        // 2. 시작일 리스트 가져오기
        const [dateRows] = await db.query(
            "SELECT DISTINCT date FROM trade_data_day WHERE ticker = ? AND date BETWEEN ? AND ? ORDER BY date ASC",
            [ticker, bulkStart, bulkEnd]
        );

        let count = 0;
        for (const row of dateRows) {
            // [에러 해결] row.date가 객체인지 문자열인지 확인 후 처리
            let currentStart;
            if (row.date instanceof Date) {
                currentStart = row.date.toISOString().split('T')[0];
            } else {
                // 이미 문자열(2025-01-01 형태)이라면 그대로 사용하거나 T 기준으로 자름
                currentStart = String(row.date).split(' ')[0].split('T')[0];
            }

            // 3. 공통 함수 호출
            const result = await runSimulationInternal({
                ticker, start: currentStart, end: targetEnd,
                initCash, initStock, targetRate, upperRate, lowerRate, unitGap
            });

            if (result && result.rows.length > 0) {
                const last = result.lastStatus;

                // 4. DB 저장
                await db.query(
                    `INSERT INTO simulation_result 
                    (strategy_code, start_date, end_date, end_asset, end_stock_rate, max_mdd_rate, average_price, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, NOW())`,
                    [strategy_code, currentStart, last.date, last.asset, last.stockRatio, last.max_mdd_rate, last.avgPrice]
                );
                count++;
            }
        }
        res.json({ success: true, count });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, error: e.message });
    }
});

// 메일 전송용 Transporter 설정
// (Gmail 기준 예시: 보안 설정에서 '앱 비밀번호'를 생성해야 합니다)
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.EMAIL_USER, // .env 파일에 정의
        pass: process.env.EMAIL_PASS  // .env 파일에 정의
    }
});

/**
 * [백엔드] 공통 메일 발송 API
 * @param {Array|String} to - 수신자 (배열 전달 시 다중 전송)
 * @param {String} subject - 제목
 * @param {String} html - 내용 (HTML 형식)
 */
async function sendCommonEmail(to, subject, html) {
    const mailOptions = {
        from: `"투자 전략 알림" <${process.env.EMAIL_USER}>`,
        to: Array.isArray(to) ? to.join(',') : to,
        subject: subject,
        html: html
    };

    try {
        const info = await transporter.sendMail(mailOptions);
        console.log('Email sent: ' + info.response);
        return { success: true, messageId: info.messageId };
    } catch (error) {
        console.error('Email send error:', error);
        throw error;
    }
}

// [백엔드] 공통 메일 발송 API 추가
app.post('/api/send-common-email', async (req, res) => {
    const { subject, html } = req.body;
    try {
        // 1. 구독자 조회
        const [customers] = await db.query("SELECT email FROM customer_master WHERE is_subscribed = 'Y'");
        const recipients = customers.map(c => c.email);

        if (recipients.length === 0) {
            return res.status(400).json({ success: false, error: "수신 대상자가 없습니다." });
        }

        // 2. 메일 발송 함수 호출
        await sendCommonEmail(recipients, subject, html);
        
        res.json({ success: true, count: recipients.length });
    } catch (e) {
        console.error("Mail Send Error:", e);
        res.status(500).json({ success: false, error: e.message });
    }
});

// [API] 전략 설정과 결과를 조인하여 데이터 반환
app.post('/api/get-analysis-data', async (req, res) => {
    const { strategyCodes, startDate, endDate } = req.body;
    try {
        const [rows] = await db.query(
            `SELECT 
                s.strategy_code, s.ticker, s.init_cash, s.init_stock, 
                s.target_rate, s.upper_rate, s.lower_rate, s.unit_gap,
                r.start_date, r.end_date, r.end_asset, r.end_stock_rate, 
                r.max_mdd_rate, r.average_price
             FROM strategy_settings s
             INNER JOIN simulation_result r ON s.strategy_code = r.strategy_code
             WHERE s.strategy_code IN (?) 
               AND r.start_date BETWEEN ? AND ?
             ORDER BY r.start_date ASC`,
            [strategyCodes, startDate, endDate]
        );

        // CAGR 계산 후 데이터 가공
        const processed = rows.map(row => {
            const start = new Date(row.start_date);
            const end = new Date(row.end_date);
            const diffDays = Math.max(1, (end - start) / (1000 * 60 * 60 * 24));
            // 연복리 수익률 공식: ((기말자산/기초자산)^(365/일수) - 1) * 100
            const cagr = (Math.pow(row.end_asset / row.init_cash, 365 / diffDays) - 1) * 100;
            
            return {
                ...row,
                cagr: parseFloat(cagr.toFixed(2)),
                startDateStr: start.toISOString().split('T')[0]
            };
        });

        res.json({ success: true, data: processed });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// ----------------------------------------------------------------
// 마지막에 둬야 하는 것
// ----------------------------------------------------------------

// 서버 실행
app.listen(port, () => {
    console.log(`서버 실행 중: http://localhost:${port}`);
});