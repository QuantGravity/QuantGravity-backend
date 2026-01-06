// File Name : server.js
const express = require('express');
const path = require('path');
const cors = require('cors'); // 추가
const cron = require('node-cron');

require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

// [추가] CORS 설정: 프론트엔드 호스트 허용
app.use(cors({
    origin: [
        'http://localhost:3000',      // React 기본 포트
        'http://127.0.0.1:3000',
        'http://localhost:5500',      
        'http://localhost:10000',     // Render 로그에 찍힌 백엔드 포트 (로컬 테스트용)
        'http://127.0.0.1:10000',
        'https://quant-navigator.web.app', // [중요] 실제 서비스 중인 Firebase 프론트엔드 주소
        'https://quant-navigator-backend.onrender.com'
    ],
    credentials: true
}));

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
        credential: admin.credential.cert(serviceAccount),
        ignoreUndefinedProperties: true, // undefined 필드는 무시하고 저장/쿼리함
    });
}

const firestore = admin.firestore();

const nodemailer = require('nodemailer');

console.log("Firebase Admin SDK가 성공적으로 연결되었습니다.");

// [공통 함수] 관리자 권한 체크 로직
async function verifyAdmin(adminEmail) {
    if (!adminEmail) return false;
    const adminDoc = await db.collection('users').doc(adminEmail).get();
    if (!adminDoc.exists) return false;
    return adminDoc.data().role === 'G9'; // 등급이 G9인지 확인
}

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

        // 1. 데이터 가공: "SERVER_TIMESTAMP" 문자열을 실제 Firebase 서버 시간 객체로 변환
        const finalData = { ...data };
        Object.keys(finalData).forEach(key => {
            if (finalData[key] === "SERVER_TIMESTAMP") {
                finalData[key] = admin.firestore.FieldValue.serverTimestamp();
            }
        });

        // 2. 용량 계산용 JSON 문자열 생성 (정의되지 않은 finaldata 대신 data 사용)
        const jsonString = JSON.stringify(data);
        const byteSize = Buffer.byteLength(jsonString, 'utf8');
        const kbSize = (byteSize / 1024).toFixed(2);

        // 3. 터미널 로그 출력
        console.log(`--------------------------------------------------`);
        console.log(`[Firestore Upload Log]`);
        console.log(`- 경로: ${collectionName}/${docId || 'auto-gen'}`);
        console.log(`- 용량: ${byteSize} bytes (${kbSize} KB)`);
        console.log(`--------------------------------------------------`);

        // 4. Firestore 용량 제한 체크 (1MB)
        if (byteSize > 1048487) {
            return res.status(413).json({ error: "Firestore 단일 문서 용량 제한(1MB)을 초과했습니다." });
        }

        // 5. DB 작업 (질문자님이 선언하신 'firestore' 변수 사용)
        const colRef = firestore.collection(collectionName);
        
        if (docId) {
            // 기존 문서 업데이트 (merge: true)
            await colRef.doc(docId).set({
                ...finalData,
                updatedAt: admin.firestore.FieldValue.serverTimestamp()
            }, { merge: true });
            
            res.status(200).json({ success: true, docId: docId });
        } else {
            // 신규 문서 생성
            const docRef = await colRef.add({
                ...finalData,
                createdAt: admin.firestore.FieldValue.serverTimestamp(),
                updatedAt: admin.firestore.FieldValue.serverTimestamp()
            });
            res.status(200).json({ success: true, docId: docRef.id });
        }
    } catch (error) {
        // catch 블록의 변수명을 error로 통일하여 참조 에러 방지
        console.error("[Firestore Update Error]:", error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// [공통] 특정 컬렉션의 문서 삭제
app.delete('/api/delete-from-firestore', async (req, res) => {
    try {
        const { collectionName, id } = req.query; // 프론트엔드 호출 규격에 맞춰 id로 받음

        if (!collectionName || !id) {
            return res.status(400).json({ error: "컬렉션 이름과 문서 ID는 필수입니다." });
        }

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

// ----------------------------------------------------------------
// [수정] 주가 데이터 조회 함수 (Firestore 전용)
// ----------------------------------------------------------------
async function getDailyStockData(ticker, start, end) {
    try {
        // 무조건 Firestore 'ticker_prices' 컬렉션 조회
        const doc = await firestore.collection('ticker_prices').doc(ticker).get();
        
        if (!doc.exists) {
            console.warn(`Firestore에 데이터 없음: ${ticker}`);
            return [];
        }

        const data = doc.data();
        const labels = data.labels || [];
        const values = data.values || [];

        // 데이터를 객체 배열 포맷으로 변환 및 필터링
        const rows = labels.map((date, index) => {
            const dDate = date.includes('T') ? date.split('T')[0] : date;
            return {
                date: dDate,
                close_price: values[index],
                open_price: values[index], // 종가로 대체
                high_price: values[index],
                low_price: values[index]
            };
        }).filter(row => {
            if (start && end) return row.date >= start && row.date <= end;
            return true;
        });

        return rows.sort((a, b) => a.date.localeCompare(b.date));
    } catch (err) {
        console.error(`Firestore 조회 에러 (${ticker}):`, err.message);
        throw err;
    }
}

// [백엔드 수정] 권한에 따라 로컬 DB 또는 Firestore에서 데이터를 조회하는 표준 API
// [백엔드] 권한 기반 통합 주가 조회 API
app.get('/api/stocks-bulk', async (req, res) => {
    // mode 파라미터 추가 수신
    const { tickers, start, end } = req.query; 
    if (!tickers) return res.status(400).json({ error: "티커 코드가 없습니다." });

    const tickerList = tickers.split(',');

    try {
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
    catch (err) {
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

app.post('/api/update-prices', async (req, res) => {
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
                    const values = [];

                    for (const quote of history) {
                        if (!quote.date || !quote.close || quote.close === 0) continue;
                        
                        const quoteDate = new Date(quote.date).toISOString().split('T')[0];
                        labels.push(quoteDate);
                        values.push(quote.close);
                    }

                    // --- 2. Firestore 업로드 실행 (백엔드 내부 함수 직접 호출과 유사) ---
                    const uploadPayload = {
                        ticker: ticker,
                        last_updated: new Date().toISOString(),
                        labels: labels, // 날짜 배열
                        values: values  // 종가 배열
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

// ----------------------------------------------------------------
// 3. 주가 데이터 조회 및 분석 API
// ----------------------------------------------------------------

// 특정 티커의 싸이클 계산
app.get('/api/daily-stock', async (req, res) => {
// 모든 인자를 query에서 한 번에 구조 분해 할당
    const { ticker, startDate, endDate, upperRate: uR, lowerRate: lR } = req.query;
    const upperRate = parseFloat(uR) || 30; 
    const lowerRate = parseFloat(lR) || 15;

    console.log(`[조회 시작] ticker: ${ticker}, StartDate: ${startDate}`); // 디버깅용

    try {
        // 1. 쿼리 실행 (ticker 대소문자 무시 등 대비)
        const rows = await getDailyStockData(ticker, startDate, endDate);

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
// [수정] 전략(Strategy) 관리 API (Firestore로 전환)
// ----------------------------------------------------------------
app.get('/api/strategies', async (req, res) => {
    try {
        const snapshot = await firestore.collection('strategies').get();
        const list = snapshot.docs.map(doc => ({ strategy_code: doc.id, ...doc.data() }));
        res.json(list);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/strategies', async (req, res) => {
    try {
        const { strategy_code, ...rest } = req.body;
        // upperRate, lowerRate 변수명 사용 준수
        await firestore.collection('strategies').doc(strategy_code).set({
            ...rest,
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// [수정] 전략 단일 삭제 API
app.delete('/api/strategies/:strategy_code', async (req, res) => {
    try {
        const { strategy_code } = req.params;

        if (!strategy_code) {
            return res.status(400).json({ error: "전략 코드가 필요합니다." });
        }

        console.log(`[Firestore Delete] Strategy: ${strategy_code}`);

        // 1. 해당 전략 문서 삭제
        await firestore.collection('strategies').doc(strategy_code).delete();

        // 2. (선택 사항) 해당 전략과 연결된 시뮬레이션 결과들도 함께 삭제하고 싶다면 아래 로직 추가
        const resultsRef = firestore.collection('simulation_results');
        const batch = firestore.batch();
        const snapshot = await resultsRef.where('strategy_code', '==', strategy_code).get();
        snapshot.forEach(doc => batch.delete(doc.ref));
        await batch.commit();

        res.status(200).json({ success: true, message: "전략이 성공적으로 삭제되었습니다." });
    } catch (e) {
        console.error("Strategy Delete Error:", e);
        res.status(500).json({ error: e.message });
    }
});

// [수정] API 엔드포인트 부분
app.get('/api/simulation-summary', async (req, res) => {
    try {
        const { ticker, start, end, initCash, initStock, targetRate, upperRate, lowerRate, unitGap } = req.query;
        
        const [tickerData, ndxData] = await Promise.all([
            getDailyStockData(ticker, start, end),
            getDailyStockData('^NDX', start, end)
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
    const priceData = await getDailyStockData(ticker, start, end);
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

/* [수정] 대량 시뮬레이션 및 결과 저장 API (Firestore) */
app.post('/api/bulk-simulation', async (req, res) => {
    const { 
        strategy_code, ticker, bulkStart, bulkEnd, targetEnd, 
        initCash, initStock, targetRate, upperRate, lowerRate, unitGap 
    } = req.body;

    try {
        console.log(`[Bulk Simulation] Start: ${strategy_code} (${bulkStart} ~ ${bulkEnd})`);

        // 1. 기존 데이터 삭제 (해당 전략코드 및 기간 내 결과물 삭제)
        const resultsRef = firestore.collection('simulation_results');
        const oldDocs = await resultsRef
            .where('strategy_code', '==', strategy_code)
            .where('start_date', '>=', bulkStart)
            .where('start_date', '<=', bulkEnd)
            .get();

        if (!oldDocs.empty) {
            const deleteBatch = firestore.batch();
            oldDocs.forEach(doc => deleteBatch.delete(doc.ref));
            await deleteBatch.commit();
            console.log(`[Bulk Simulation] 기존 데이터 ${oldDocs.size}건 삭제 완료`);
        }

        // 2. 시작일 리스트 추출 (Firestore ticker_prices 문서 활용)
        const tickerDoc = await firestore.collection('ticker_prices').doc(ticker).get();
        if (!tickerDoc.exists) {
            return res.status(404).json({ success: false, error: "주가 데이터가 없습니다." });
        }

        const tickerData = tickerDoc.data();
        const labels = tickerData.labels || []; // ['2025-01-01', ...]
        
        // 필터링된 시작일 리스트 생성
        const startDateList = labels.filter(date => {
            const d = date.includes('T') ? date.split('T')[0] : date;
            return d >= bulkStart && d <= bulkEnd;
        }).sort();

        // 3. 루프 돌며 시뮬레이션 실행 및 배치 저장 준비
        let count = 0;
        const saveBatch = firestore.batch();

        for (const currentStart of startDateList) {
            // 공통 함수 호출 (이 함수 내부도 Firestore를 보도록 수정되어 있어야 함)
            const result = await runSimulationInternal({
                ticker, start: currentStart, end: targetEnd,
                initCash, initStock, targetRate, upperRate, lowerRate, unitGap
            });

            if (result && result.rows.length > 0) {
                const last = result.lastStatus;
                
                // Firestore 문서 ID 생성 (전략코드_시작일)
                const docId = `${strategy_code}_${currentStart}`;
                const docRef = resultsRef.doc(docId);

                saveBatch.set(docRef, {
                    strategy_code,
                    ticker,
                    start_date: currentStart,
                    end_date: last.date,
                    end_asset: last.asset,
                    end_stock_rate: last.stockRatio,
                    max_mdd_rate: last.max_mdd_rate,
                    average_price: last.avgPrice,
                    upperRate, // 저장 시 사용자 변수명 준수
                    lowerRate,
                    created_at: admin.firestore.FieldValue.serverTimestamp()
                });
                
                count++;

                // Firestore 배치 제한(500개) 고려 - 만약 400개가 넘으면 중간 커밋
                if (count % 400 === 0) {
                    await saveBatch.commit();
                }
            }
        }

        // 남은 배치 커밋
        if (count % 400 !== 0) {
            await saveBatch.commit();
        }

        console.log(`[Bulk Simulation] 완료: 총 ${count}건 저장`);
        res.json({ success: true, count });

    } catch (e) {
        console.error("[Bulk Simulation Error]:", e);
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

// [최종 수정] 공통 메일 발송 API (컬렉션 명: users)
app.post('/api/send-common-email', async (req, res) => {
    const { subject, html } = req.body;
    
    try {
        console.log(`[Mail Service] 구독자 메일 발송 시작: ${subject}`);

        // 1. 파이어스토어 'users' 컬렉션에서 구독 중('Y')인 사용자 조회
        const usersRef = firestore.collection('users');
        const snapshot = await usersRef
            .where('is_subscribed', '==', 'Y')
            .get();

        if (snapshot.empty) {
            return res.status(400).json({ success: false, error: "수신 대상자가 없습니다." });
        }

        // 2. 이메일 목록 추출
        const recipients = [];
        snapshot.forEach(doc => {
            const data = doc.data();
            // 필드명이 email인지 userEmail인지 확인 필요 (일반적으로 email 사용)
            if (data.email) {
                recipients.push(data.email);
            }
        });

        if (recipients.length === 0) {
            return res.status(400).json({ success: false, error: "유효한 이메일 주소가 없습니다." });
        }

        // 3. 메일 발송 함수 호출
        await sendCommonEmail(recipients, subject, html);
        
        console.log(`[Mail Service] 성공: ${recipients.length}명에게 발송 완료`);
        res.json({ success: true, count: recipients.length });

    } catch (e) {
        console.error("[Mail Send Error]:", e);
        res.status(500).json({ success: false, error: e.message });
    }
});

// [수정] 전략 설정과 결과를 통합하여 데이터 반환 (Firestore)
app.post('/api/get-analysis-data', async (req, res) => {
    const { strategyCodes, startDate, endDate } = req.body;

    try {
        console.log(`[Analysis Data] Fetching for strategies: ${strategyCodes}`);

        // 1. 전략 설정(strategies) 정보 가져오기
        // WHERE IN 쿼리는 Firestore에서 FieldPath.documentId()와 'in' 연산자로 처리 가능 (최대 30개 제한)
        const strategiesSnapshot = await firestore.collection('strategies')
            .where(admin.firestore.FieldPath.documentId(), 'in', strategyCodes)
            .get();

        if (strategiesSnapshot.empty) {
            return res.json({ success: true, data: [] });
        }

        // 전략 정보를 맵(Map) 형태로 저장 (빠른 조인을 위함)
        const strategyMap = {};
        strategiesSnapshot.forEach(doc => {
            strategyMap[doc.id] = doc.data();
        });

        // 2. 시뮬레이션 결과(simulation_results) 가져오기
        const resultsRef = firestore.collection('simulation_results');
        const resultsSnapshot = await resultsRef
            .where('strategy_code', 'in', strategyCodes)
            .where('start_date', '>=', startDate)
            .where('start_date', '<=', endDate)
            .get();

        // 3. 데이터 조인 및 CAGR 계산
        const processed = resultsSnapshot.docs.map(doc => {
            const resultData = doc.data();
            const strategyData = strategyMap[resultData.strategy_code] || {};

            // 날짜 계산 (CAGR용)
            const start = new Date(resultData.start_date);
            const end = new Date(resultData.end_date);
            const diffDays = Math.max(1, (end - start) / (1000 * 60 * 60 * 24));

            // 연복리 수익률(CAGR) 공식 적용
            // 기초자산(init_cash)은 전략 설정 정보에 있음
            const initCash = strategyData.init_cash || 1; 
            const cagr = (Math.pow(resultData.end_asset / initCash, 365 / diffDays) - 1) * 100;

            return {
                // 결과 데이터와 전략 설정을 하나로 합침 (MySQL JOIN 효과)
                strategy_code: resultData.strategy_code,
                ticker: strategyData.ticker,
                init_cash: strategyData.init_cash,
                init_stock: strategyData.init_stock,
                target_rate: strategyData.target_rate,
                upperRate: strategyData.upperRate, // 사용자 변수명 준수
                lowerRate: strategyData.lowerRate, // 사용자 변수명 준수
                unit_gap: strategyData.unit_gap,
                
                start_date: resultData.start_date,
                end_date: resultData.end_date,
                end_asset: resultData.end_asset,
                end_stock_rate: resultData.end_stock_rate,
                max_mdd_rate: resultData.max_mdd_rate,
                average_price: resultData.average_price,
                
                cagr: parseFloat(cagr.toFixed(2)),
                startDateStr: resultData.start_date // 이미 YYYY-MM-DD 형태라면 그대로 사용
            };
        });

        // 시작일 기준 오름차순 정렬
        processed.sort((a, b) => a.start_date.localeCompare(b.start_date));

        res.json({ success: true, data: processed });

    } catch (e) {
        console.error("[Analysis Data Error]:", e);
        res.status(500).json({ success: false, error: e.message });
    }
});

// [테스트] 1분마다 콘솔에 로그 찍기
//cron.schedule('* * * * *', () => {
//  console.log('--- [Cron] 1분마다 배치 작업이 실행됩니다. ---');
//});

// [로컬 테스트용] 매일 새벽 6시에 실행되도록 설정
// (분 시 일 월 요일)
//cron.schedule('30 14 * * *', async () => {
//    console.log('--- [Cron Test] 한국 시간 오후 2시 30분 배치 실행 ---');
//        
//    try {
//        // 테스트용: 나스닥 지수(^IXIC) 하나만 수집해보기
//        const ticker = '^IXIC';
//        const today = new Date().toISOString().split('T')[0];
        
//        console.log(`[${ticker}] ${today} 데이터 수집 시도 중...`);
        
        // 1. 여기에 기존에 만든 주가 수집 로직(fetchWithChunks 등) 호출
        // 2. Firestore에 set 하는 로직 실행
        
//        console.log(`[Cron Test] 작업이 완료되었습니다.`);
//    } catch (err) {
//        console.error('배치 테스트 에러:', err);
//    }
//}, {
//    scheduled: true,
//    timezone: "Asia/Seoul" // PC가 한국 시간이니 이 설정이 정확합니다.
//});

// ----------------------------------------------------------------
// 마지막에 둬야 하는 것
// ----------------------------------------------------------------

// 서버 실행
app.listen(port, () => {
    console.log(`서버 실행 중: http://localhost:${port}`);
});