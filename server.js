// File Name : server.js
const express = require('express');
const path = require('path');
const cors = require('cors'); // 추가
const cron = require('node-cron');
const fmpClient = require('./utils/fmpClient');
const YahooFinance = require('yahoo-finance2').default;

require('dotenv').config();
// firebase 연결    ----------------------------------  최 상단에 둬야 함 - 시작
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

// firebase 연결    ----------------------------------  최 상단에 둬야 함 - 끝

const app = express();
const port = process.env.PORT || 3000;

// // [추가] 보안 모듈 가져오기 - server.js 상단 
const { generateToken, verifyToken } = require('./auth');
const { logTraffic } = require('./logger'); // [추가] 로거 가져오기

// ============================================================
// 각 컬렉션 별로 사용자권한 점검 - 수작업으로 관리해줘야 함.
// ============================================================
const COLLECTION_RULES = {
    // 1. [관리자 영역] 티커, 주가정보 등은 관리자나 G9만 수정 가능
    'tickers': (user, docId, data) => ['admin', 'G9'].includes(user.role),
    'ticker_prices': (user, docId, data) => ['admin'].includes(user.role),
    'notices': (user, docId, data) => ['admin'].includes(user.role),
    // [추가] 메뉴 설정도 관리자만 수정 가능
    'menu_settings': (user, docId, data) => ['admin'].includes(user.role), 

    // 2. [개인 데이터 영역] 내 문서는 '나(docId)'만 수정 가능
    'user_strategies': (user, docId, data) => user.email === docId,
    'investment_tickers': (user, docId, data) => user.email === docId, 
    
    // ▼▼▼ [누락된 항목 추가] ▼▼▼
    'favorite_groups': (user, docId, data) => user.email === docId,     // 관심종목 그룹
    'favorite_tickers': (user, docId, data) => user.email === docId,    // 관심종목 상세
    'user_analysis_jobs': (user, docId, data) => user.email === docId,  // [중요] 작업번호 관리
    // [추가] 고객 문의 권한 설정
    'customer_inquiries': (user, docId, data) => {
        // 관리자나 G9 등급은 모든 권한 허용
        if (['admin', 'G9'].includes(user.role)) return true;
        
        // 일반 사용자는 본인이 작성한 문서(userId가 본인 이메일)인 경우에만 허용
        if (data && data.userId === user.email) return true;
        
        // 상세 조회 시 docId가 user.email과 연관이 없으므로, 
        // 리스트 조회나 신규 등록 시에는 기본적으로 true를 반환하고 세부 필터링은 API에서 처리
        return true; 
    },
    // ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

    // 3. [위험 구역] 회원 정보는 공통 API로 수정 금지
    // [보강] 사용자 정보 조회 권한
    'users': (user, docId, data) => {
        // 관리자라면 모든 유저 정보 조회 가능
        if (['admin', 'G9'].includes(user.role)) return true;
        // 일반 유저는 본인 정보만 조회 가능
        return user.email === docId;
    },

    // 4. [시스템 영역] 로그 등은 API로 임의 수정 절대 불가
    'traffic_logs': (user, docId, data) => false,
    'limit_logs': (user, docId, data) => false
};

// 1. [공개 구역] 인증 없이 접근 가능한 API들 (순서 중요! 맨 위에 배치)



// ============================================================
// 2. [검문소 설치] 이 아래에 있는 모든 API는 자동으로 verifyToken이 적용됨
// ============================================================


// 3. [보안 구역] 여기부터는 verifyToken을 일일이 안 써도 됨!
// 이미 2번에서 걸러졌기 때문.


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



// [추가] null 값을 파이어스토어 삭제 명령(FieldValue.delete())으로 변환하는 재귀 함수
function convertNullToDelete(obj) {
    for (const key in obj) {
        if (obj[key] === null) {
            // 값이 null이면 삭제 명령(FieldValue.delete())으로 교체
            obj[key] = admin.firestore.FieldValue.delete();
        } else if (typeof obj[key] === 'object' && obj[key] !== null) {
            // 객체 안에 또 객체가 있으면 안쪽까지 검사 (재귀 호출)
            convertNullToDelete(obj[key]);
        }
    }
    return obj;
}

// 1. 로그인 처리 API (판사 역할: 백앤드)
app.post('/api/login', async (req, res) => {
    const { token, provider } = req.body; // 프론트에서 보낸 구글/네이버 토큰

    let email = "";
    let uid = "";

    try {
        // [구글 로그인 검증]
        if (provider === 'GOOGLE') {
            const decodedToken = await admin.auth().verifyIdToken(token);
            email = decodedToken.email;
            uid = decodedToken.uid;
        } 
        // [네이버 로그인 검증] (일단 이메일 신뢰 방식으로 약식 구현, 추후 네이버 API 검증 추가 권장)
        else if (provider === 'NAVER') {
            email = req.body.email; // 프론트에서 받은 이메일 사용
            uid = email; // 네이버는 이메일을 ID처럼 사용
        }

        // 1. DB에서 유저 조회
        const userDoc = await firestore.collection('users').doc(email).get();

        // 2. 유저가 없으면 -> "회원가입 필요" 응답
        if (!userDoc.exists) {
            return res.json({ 
                status: 'NEED_REGISTER', 
                email: email,
                provider: provider
            });
        }

        // 3. 유저가 있으면 -> 정보 가져와서 JWT 발급 (여기가 핵심 보안)
        const userData = userDoc.data();
        
        // (멤버십 만료 체크 로직 등도 여기서 수행)
        let role = userData.role || 'G1';
        
        // [보안] 백앤드 전용 인증키(JWT) 발급 (auth.js의 함수 사용)
        const systemToken = generateToken({ 
            uid: uid, 
            email: email, 
            role: role 
        });

        // 4. 프론트로 결과 전송
        res.json({ 
            status: 'SUCCESS',
            token: systemToken, // 이제 이 토큰 없이는 아무것도 못함
            user: { 
                email: email, 
                displayName: userData.name, 
                role: role 
            }
        });

    } catch (error) {
        console.error("Login Error:", error);
        res.status(401).json({ error: "인증 실패" });
    }
});

// 2. 회원가입 처리 API (DB 쓰기 권한을 백앤드만 가짐)
app.post('/api/register', async (req, res) => {
    try {
        const { email, name, phone, country, gender, birthyear, provider } = req.body;

        // 필수값 검증 (백앤드에서 한 번 더 체크)
        if (!email || !name) return res.status(400).json({ error: "필수 정보 누락" });

        const newUser = {
            email,
            name,
            phone,
            country,
            gender,
            birthyear,
            role: 'G1', // 신규 가입은 무조건 G1 고정 (해킹 방지)
            membershipLevel: 'FREE',
            status: 'ACTIVE',
            provider: provider || 'SOCIAL',
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            lastLogin: admin.firestore.FieldValue.serverTimestamp()
        };

        // DB 저장
        await firestore.collection('users').doc(email).set(newUser);

        // 가입 완료 후 바로 로그인 처리 토큰 발급
        const systemToken = generateToken({ 
            uid: email, 
            email: email, 
            role: 'G1' 
        });

        res.json({ 
            success: true, 
            token: systemToken,
            user: { email, displayName: name, role: 'G1' }
        });

    } catch (error) {
        console.error("Register Error:", error);
        res.status(500).json({ error: "회원가입 처리 실패" });
    }
});

// 백엔드 API 예시: /api/firestore/upload
app.post('/api/upload-to-firestore', verifyToken, async (req, res) => {
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
            // [추가] 필드 삭제 처리
            if (finalData[key] === "DELETE_FIELD") {
                finalData[key] = admin.firestore.FieldValue.delete();
            }
        });

        // ============================================================
        // [추가된 코드 2] 위에서 만든 재귀 함수 실행
        // finalData 안에 있는 모든 중첩된 null 값을 찾아서 삭제 명령으로 바꿉니다.
        // ============================================================
        convertNullToDelete(finalData);

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
app.delete('/api/delete-from-firestore', verifyToken, async (req, res) => {
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
app.get('/api/firestore/list/:collectionName', verifyToken, async (req, res) => {
    try {
        const { collectionName } = req.params;
        
        // 보안 로직 (기존 유지)
        if (collectionName === 'users') {
            if (!['admin', 'G9'].includes(req.user?.role)) {
                return res.status(403).json({ error: "권한 없음" });
            }
        }

        const snapshot = await firestore.collection(collectionName).get();
        if (snapshot.empty) return res.json([]);

        const list = snapshot.docs.map(doc => {
            const fullData = doc.data();
            
            // [중요 로직 유지] metadata 필드가 있으면 그것을 사용하고, 없으면 전체 데이터를 사용
            let dataContent = fullData.metadata ? { ...fullData.metadata } : { ...fullData };

            // [날짜 변환 추가] 데이터 안의 Timestamp 객체를 문자열로 변환 (에러 방지 핵심)
            if (dataContent.createdAt && typeof dataContent.createdAt.toDate === 'function') {
                dataContent.createdAt = dataContent.createdAt.toDate().toISOString();
            }
            if (dataContent.updatedAt && typeof dataContent.updatedAt.toDate === 'function') {
                dataContent.updatedAt = dataContent.updatedAt.toDate().toISOString();
            }

            return {
                id: doc.id, // 문서 ID 포함
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
// [server.js] 상세 조회 API 수정 (디버깅 로그 추가)
app.get('/api/firestore/detail/:collectionName/:docId', verifyToken, async (req, res) => {
    try {
        const { collectionName, docId } = req.params;
        
        // [1] 요청 로그 출력 (서버 콘솔 확인용)
        console.log(`========================================`);
        console.log(`[DEBUG] 상세 조회 요청: ${collectionName} / ${docId}`);

        const doc = await firestore.collection(collectionName).doc(docId).get();

        // [2] 데이터 존재 여부 로그
        console.log(`[DEBUG] Firestore 존재 여부: ${doc.exists}`);

        if (!doc.exists) {
            console.log(`[DEBUG] ❌ 문서가 없어 404 반환`);
            return res.status(404).json({ error: "데이터를 찾을 수 없습니다." });
        }

        console.log(`[DEBUG] ✅ 데이터 반환 성공`);
        res.json(doc.data());

    } catch (error) {
        console.error("[Detail Fetch Error]:", error);
        res.status(500).json({ error: error.message });
    }
});

// 관리자 이메일 목록 (본인의 이메일을 넣으세요)
const ADMIN_EMAILS = ['your-email@gmail.com', 'partner-email@gmail.com'];

// [백엔드] app.js 또는 라우터에 추가해야 할 코드
// 필요한 라이브러리: mathjs (중앙값 계산용) 또는 직접 구현 가능
// npm install mathjs (선택사항, 아래는 내장 함수로 구현함)

const calculateCAGR = (startPrice, endPrice, years) => {
    if (years <= 0 || startPrice <= 0 || endPrice <= 0) return 0;
    return (Math.pow(endPrice / startPrice, 1 / years) - 1) * 100;
};

const getMedian = (values) => {
    if (values.length === 0) return 0;
    values.sort((a, b) => a - b);
    const half = Math.floor(values.length / 2);
    if (values.length % 2) return values[half];
    return (values[half - 1] + values[half]) / 2.0;
};
// [server.js] 기존 calculateCAGR, getMedian 함수는 그대로 유지

// ============================================================
// [리팩토링] 분석 핵심 로직을 내부 함수로 분리 (재사용 목적)
// ============================================================
async function performAnalysisInternal(ticker, startDate, endDate, rp1, rp2) {
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
    let minMdd = 0;
    let sumMdd = 0;

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
        if (dd < minMdd) minMdd = dd;
        sumMdd += dd;

        thresholds.forEach(th => {
            if (dd <= -th && !isUnderWater[th]) {
                ddCounts[th]++;
                isUnderWater[th] = true;
            }
        });

        const currentYield = ((price - basePrice) / basePrice) * 100;
        const currentMdd = dd * 100;

        history.push({
            d: p.dateStr,
            y: parseFloat(currentYield.toFixed(2)),
            m: parseFloat(currentMdd.toFixed(2))
        });
    });

    const avgMdd = (sumMdd / prices.length) * 100;
    const finalMinMdd = minMdd * 100;
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
        mdd: { max: finalMinMdd, avg: avgMdd },
        ddCounts,
        recovery: { max: maxRecovery, avg: avgRecovery },
        recoveryDist: recoveryDist, // [추가] 프론트엔드 차트용 데이터
        rolling: { r10: statsRolling(rollingArr1), r5: statsRolling(rollingArr2) },
        periodCagrs,
        history,
        updatedAt: admin.firestore.FieldValue.serverTimestamp() // 캐싱 시점 기록
    };
}

// ============================================================
// [신규 API] 배치 작업용: 지수(^) 분석 실행 및 DB 저장
// ============================================================
app.post('/api/batch/analyze-indices', async (req, res) => {
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
// [수정 API] 프론트엔드 호출용: 캐시 우선 조회 (스마트 라우터)
// ============================================================
app.post('/api/analyze-ticker-performance', verifyToken, async (req, res) => {
    const { tickers, startDate, endDate, rollingPeriod1, rollingPeriod2 } = req.body;
    const rp1 = parseInt(rollingPeriod1) || 10;
    const rp2 = parseInt(rollingPeriod2) || 5;

    if (!tickers || !Array.isArray(tickers) || tickers.length === 0) return res.json([]);

    try {
        const analysisPromises = tickers.map(async (ticker) => {
            // [캐시 전략] 
            // 1. 지수(^)이고 
            // 2. Rolling 기간이 기본값(10, 5)이며
            // 3. 종료일이 오늘(또는 미지정)인 경우 -> DB 캐시 확인
            const isIndex = ticker.startsWith('^');
            const isDefaultRolling = (rp1 === 10 && rp2 === 5);
            const isRecent = !endDate || endDate >= new Date().toISOString().split('T')[0];

            if (isIndex && isDefaultRolling && isRecent) {
                const cacheDoc = await firestore.collection('analysis_cache').doc(ticker).get();
                if (cacheDoc.exists) {
                    console.log(`[Cache Hit] ${ticker} - 캐시된 데이터 반환`);
                    return cacheDoc.data(); // 저장된 데이터 바로 반환
                }
            }

            // 캐시가 없거나 조건이 안 맞으면 실시간 계산
            console.log(`[Realtime Calc] ${ticker} - 실시간 계산 수행`);
            return await performAnalysisInternal(ticker, startDate, endDate, rp1, rp2);
        });

        const results = await Promise.all(analysisPromises);
        res.json(results);

    } catch (err) {
        console.error("Analysis Error:", err);
        res.status(500).json({ error: "분석 중 오류 발생" });
    }
});

// ============================================================
// [Batch] 전체 종목 병렬 분석 및 Market Map 데이터 생성 (최적화 버전)
// ============================================================
app.post('/api/batch/analyze-all-tickers', verifyToken, logTraffic, async (req, res) => {
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
                        mdd: result.mdd.max,
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

// [보조 함수] 날짜 차이 계산
function getDaysDiff(startStr, endStr) {
    const s = new Date(startStr);
    const e = new Date(endStr);
    return Math.floor((e - s) / (1000 * 60 * 60 * 24));
}

// ----------------------------------------------------------------
// [수정] 주가 데이터 조회 함수 (Firestore 전용)
// ----------------------------------------------------------------
async function getDailyStockData(ticker, start, end) {
    try {
        const doc = await firestore.collection('ticker_prices').doc(ticker).get();
        
        if (!doc.exists) {
            console.warn(`Firestore에 데이터 없음: ${ticker}`);
            return [];
        }

        const data = doc.data();
        const labels = data.labels || [];
        const prices = data.prices || [];

        // 1. 데이터 매핑 (날짜와 가격을 묶음)
        let rawRows = labels.map((date, index) => {
            const dDate = date.includes('T') ? date.split('T')[0] : date;
            const p = prices[index];

            return {
                date: dDate,
                close_price: p && typeof p === 'object' ? p.c : p,
                open_price:  p && typeof p === 'object' ? p.o : p,
                high_price:  p && typeof p === 'object' ? p.h : p,
                low_price:   p && typeof p === 'object' ? p.l : p
            };
        });

        // 2. [핵심 수정] 날짜(date) 기준 중복 제거 (Map 사용)
        const uniqueMap = new Map();
        rawRows.forEach(row => {
            // 날짜 범위 필터링을 여기서 미리 수행하여 불필요한 연산 감소
            if ((!start || row.date >= start) && (!end || row.date <= end)) {
                uniqueMap.set(row.date, row); // 같은 날짜가 있으면 덮어씌움 (중복 제거)
            }
        });

        // 3. 중복 제거된 데이터를 배열로 변환 후 날짜 오름차순 정렬
        const sortedRows = Array.from(uniqueMap.values()).sort((a, b) => {
            return a.date.localeCompare(b.date);
        });

        return sortedRows;

    } catch (err) {
        console.error(`Firestore 조회 에러 (${ticker}):`, err.message);
        throw err;
    }
}

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

// [Backend] 사용자별 관심종목 전제 데이터 반환 API
// [server.js] 기존 기능을 유지하며 확장된 로직
// [server.js] 기존 로직을 유지하되 데이터 추출 부분을 더 정교하게 수정
app.get('/api/user/investments/:email', verifyToken, async (req, res) => {
    try {
        const { email } = req.params;
        const docRef = firestore.collection('investment_tickers').doc(email);
        const doc = await docRef.get();

        if (!doc.exists) return res.status(200).json([]);

        const data = doc.data();
        let tickerMap = {};

        // 헬퍼 함수: DB 필드(fee_rate, tax_rate)를 프론트에서 쓰는 명칭으로 매핑
        const extractItem = (item, key) => ({
            ticker: item.ticker || key,
            ticker_name_kr: item.ticker_name_kr || "",
            description: item.description || "",
            // [중요] DB 필드명을 그대로 유지하여 프론트 전달
            fee_rate: (item.fee_rate !== undefined && item.fee_rate !== null) ? item.fee_rate : 0,
            tax_rate: (item.tax_rate !== undefined && item.tax_rate !== null) ? item.tax_rate : 0,
            createdAt: item.createdAt || ""
        });

        // [사진 구조 반영] investments 객체 내부 순회
        if (data.investments && typeof data.investments === 'object') {
            Object.keys(data.investments).forEach(key => {
                const itemData = data.investments[key];
                // null 체크 (삭제 대기 데이터 등 방어 코드)
                if (itemData) {
                    tickerMap[key] = extractItem(itemData, key);
                }
            });
        }

        // Dot notation (investments.TQQQ 형태) 필드가 혼재할 경우를 대비한 방어 코드
        Object.keys(data).forEach(key => {
            if (key.startsWith('investments.') && data[key]) {
                const tickerCode = key.split('.')[1];
                tickerMap[tickerCode] = extractItem(data[key], tickerCode);
            }
        });

        const tickerArray = Object.values(tickerMap).sort((a, b) => a.ticker.localeCompare(b.ticker));
        res.status(200).json(tickerArray);
    } catch (error) {
        console.error("[Get investments Error]:", error);
        res.status(500).json({ error: "관심종목 로드 실패" });
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
// [엔진] 시뮬레이션 핵심 로직 - 주가 데이터 주입(Injection) 지원
// ----------------------------------------------------------------
// [최적화] nper 함수 (try-catch 제거)
function nper_custom(rate, pv, fv) {
    if (rate === 0) return 0;
    const val = Math.abs(fv) / pv;
    if (val <= 0) return 0;
    return Math.log(val) / Math.log(1 + rate);
}

// [핵심] 시뮬레이션 엔진 - 12단계 정밀 로직 100% 완전 복원 버전 (Gap/Split 분리 전용)
async function runSimulationInternal(params, preLoadedPriceData = null) {
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
    let maxMddRate = 0;
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
    let sumMDD = 0;
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
        ? { dates: [], closes: [], assets: [], mdds: [], shares: [], lowers: [], uppers: [], ratios: [] } 
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
        if (mdd < maxMddRate) maxMddRate = mdd;

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
        sumMDD += mdd;
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
                asset, mdd,
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
            chartArrays.mdds.push(Math.round(mdd * 100) / 100);
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
            const currentAvgMDD = currentTotalDays > 0 ? sumMDD / currentTotalDays : 0;
            const currentAvgAsset = currentTotalDays > 0 ? sumAsset / currentTotalDays : 0;

            // [요청 반영] 년도별 통계에도 lastStatus 항목 모두 추가
            const curCumulativeReturn = ((asset - initCashVal) / initCashVal) * 100;
            const curRiskRewardRatio = maxMddRate !== 0 ? Math.abs(currentCagr / maxMddRate) : 0;

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
                max_mdd_rate: maxMddRate, 
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
                avg_MDD: currentAvgMDD,

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
    const avgMDD = totalDays > 0 ? sumMDD / totalDays : 0;
    const avgAsset = totalDays > 0 ? sumAsset / totalDays : 0;

    // 신규 항목 계산
    const cumulativeReturn = ((last_asset - initCashVal) / initCashVal) * 100;
    const riskRewardRatio = maxMddRate !== 0 ? Math.abs(finalCagr / maxMddRate) : 0; 
    
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
            max_mdd_rate: maxMddRate,
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
            avg_MDD: avgMDD,
            
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

// [API] 병렬 처리 일괄 실행 (등급별 속도 제어 적용)
app.post('/api/simulation-compare-batch', verifyToken, logTraffic, async (req, res) => {
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
                const simResult = await runSimulationInternal({ ...strat, responseType: reqType }, tickerData);

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

// ============================================================
// [신규 API] 티커 검색 및 필터링 (권한 제어 포함)
// ============================================================
// [수정] verifyToken 미들웨어 추가 (req.user 사용 가능해짐)
app.post('/api/ticker/search', verifyToken, async (req, res) => {
    try {
        const { type, keyword, userGrade } = req.body;
        
        // [보안 강화] 토큰에서 직접 권한 확인 (위조 방지)
        const tokenRole = req.user ? req.user.role : null;
        const isAdmin = ['admin', 'G9'].includes(tokenRole);

        // 1. [보안] 권한 체크 로직 수정
        // 관리자(isAdmin)이거나, 프론트에서 VIP라고 보냈으면 통과
        // 그 외(무료 유저)인 경우에만 제약 사항 체크
        const isFreeUser = !isAdmin && (!userGrade || userGrade === 'FREE');

        if (isFreeUser && type === '2') {
            return res.status(403).json({ 
                error: "무료 등급 회원은 ETF 리스트를 조회할 수 없습니다." 
            });
        }

        // 2. Firestore에서 전체 티커 가져오기
        const snapshot = await firestore.collection('tickers').get();
        
        const allTickers = snapshot.docs.map(doc => {
            const fullData = doc.data();
            const dataContent = fullData.metadata ? fullData.metadata : fullData;
            return {
                id: doc.id,
                ...dataContent
            };
        });

        // 3. 필터링 로직 실행
        const filteredList = allTickers.filter(item => {
            const tCode = (item.id || item.ticker || "").toUpperCase();
            const isIndex = tCode.startsWith('^'); 

            // [보안] 무료 유저는 검색(9)을 하더라도 '지수'만 보여줌
            // 관리자는 모든 종목 검색 가능
            if (isFreeUser) {
                if (!isIndex) return false; 
            }

            // 구분자별 로직
            if (type === '1') { // [지수]
                return isIndex;
            } 
            else if (type === '2') { // [지수ETF]
                return !isIndex;
            } 
            else if (type === '9') { // [검색]
                if (!keyword) return false;
                const searchKey = keyword.toUpperCase().trim();

                const kName = (item.ticker_name_kr || "").toUpperCase();
                const desc = (item.description || "").toUpperCase();
                const und = (item.underlying_ticker || "").toUpperCase();

                return tCode.includes(searchKey) ||
                       kName.includes(searchKey) ||
                       desc.includes(searchKey) ||
                       und.includes(searchKey);
            }
            
            return false;
        });

        // 4. 정렬
        filteredList.sort((a, b) => {
            const aId = (a.id || a.ticker).toUpperCase();
            const bId = (b.id || b.ticker).toUpperCase();
            return aId.localeCompare(bId);
        });

        // 실제 권한 로그 출력 (디버깅용)
        console.log(`[Ticker Search] Type:${type}, Key:${keyword}, Role:${tokenRole}, Grade:${userGrade} -> Result:${filteredList.length}건`);
        
        res.json(filteredList);

    } catch (e) {
        console.error("Search API Error:", e);
        res.status(500).json({ error: "검색 중 오류가 발생했습니다." });
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

/**
 * [추가] 야후 파이낸스 티커 정보 조회 API (프록시)
 * 프론트엔드에서 티커 유효성 검사 및 자동 명칭 완성을 위해 호출함
 */
app.get('/api/proxy/yahoo-info', verifyToken, async (req, res) => {
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

// [하이브리드] FMP(최신 30년) + Yahoo(그 이전) 합쳐서 저장하기
// 핵심 수정: 'verifyToken' 추가 (이게 있어야 req.user.email을 읽을 수 있음)
app.post('/api/load-hybrid-data', verifyToken, async (req, res) => {
  const { symbol } = req.body; 

  // [안전장치] 토큰이 있어도 혹시 유저 정보가 없을 때를 대비
  const userEmail = req.user ? req.user.email : 'Unknown_User';
  console.log(`[User: ${userEmail}] 종목 [${symbol}] 데이터 수집 요청`);
  
  if (!symbol) {
    return res.status(400).json({ error: 'Symbol이 필요합니다.' });
  }

  try {
    let finalData = [];
    
    // -------------------------------------------------------
    // 1. FMP 데이터 가져오기 (Main Source)
    // -------------------------------------------------------
    console.log(`1. FMP에서 [${symbol}] 데이터 요청 중...`);
    
    let fmpRes;
    try {
        fmpRes = await fmpClient.get(`/historical-price-full/${symbol}`);
    } catch (fmpErr) {
        // FMP 에러가 나도 서버가 죽지 않게 잡아서 로그를 찍고 500 에러를 던짐
        const status = fmpErr.response ? fmpErr.response.status : 'Unknown';
        console.error(`FMP 호출 에러 (${status}):`, fmpErr.message);
        throw new Error(`FMP 데이터 수신 실패 (Status: ${status}). 구독 상태를 확인하세요.`);
    }
    
    // 데이터가 비어있을 경우 처리
    if (!fmpRes.data.historical || fmpRes.data.historical.length === 0) {
      throw new Error(`FMP에 [${symbol}] 데이터가 존재하지 않습니다.`);
    }

    // 최신순 -> 과거순 정렬
    const fmpData = fmpRes.data.historical.reverse();
    const fmpStartDate = fmpData[0].date; 

    console.log(`>> FMP 데이터 확보: ${fmpStartDate} ~ 현재 (${fmpData.length}일)`);

    // -------------------------------------------------------
    // 2. Yahoo 데이터 가져오기 (Gap Filling)
    // -------------------------------------------------------
    try {
      console.log(`2. Yahoo에서 [${fmpStartDate}] 이전 데이터 요청 중...`);
      
      const yahooResult = await yahooFinance.historical(symbol, {
        period1: '1900-01-01', 
        period2: fmpStartDate, 
        interval: '1d'
      });

      if (yahooResult && yahooResult.length > 0) {
        const yahooMapped = yahooResult.map(item => ({
          date: item.date.toISOString().split('T')[0],
          open: item.open,
          high: item.high,
          low: item.low,
          close: item.close,
          adjClose: item.adjClose,
          volume: item.volume,
          source: 'yahoo' 
        }));
        
        console.log(`>> Yahoo 데이터 확보: ${yahooMapped.length}일 추가 성공`);
        finalData = [...yahooMapped, ...fmpData];
      } else {
        console.log('>> Yahoo 데이터가 없거나 가져올 필요가 없습니다.');
        finalData = fmpData;
      }
    } catch (yahooError) {
      console.warn('>> Yahoo 연결 실패 (FMP 데이터만 저장합니다):', yahooError.message);
      finalData = fmpData; 
    }

    // -------------------------------------------------------
    // 3. Firestore에 저장 (Batch 처리)
    // -------------------------------------------------------
    console.log(`3. 총 ${finalData.length}건 데이터 저장 시작...`);
    
    const batchSize = 400; 
    let batch = admin.firestore().batch();
    let count = 0;
    let totalSaved = 0;

    for (const dayData of finalData) {
        const docRef = admin.firestore()
            .collection('stocks')
            .doc(symbol)
            .collection('history')
            .doc(dayData.date); 

        // undefined 값 제거 (JSON 직렬화/역직렬화 꼼수 사용)
        const safeData = JSON.parse(JSON.stringify(dayData));
        batch.set(docRef, safeData);

        count++;
        if (count >= batchSize) {
            await batch.commit();
            totalSaved += count;
            console.log(`>> 저장 중... (${totalSaved}/${finalData.length})`);
            batch = admin.firestore().batch();
            count = 0;
        }
    }

    if (count > 0) {
        await batch.commit();
        totalSaved += count;
    }

    // 성공 응답
    res.json({
      message: '데이터 수집 완료',
      symbol: symbol,
      totalDays: totalSaved,
      range: `${finalData[0].date} ~ ${finalData[finalData.length-1].date}`
    });

  } catch (error) {
    console.error(`[최종 에러] ${symbol} 수집 실패:`, error.message);
    res.status(500).json({ error: error.message });
  }
});

// ======================================================================
// FMP API -   시작
// ======================================================================

// ======================================================================
// [핵심 로직] FMP 전체 종목 리스트 가져와서 '거래소별'로 묶어 저장하기 (안전 버전)
// ======================================================================
app.post('/api/sync-ticker-master', verifyToken, async (req, res) => {
  try {
    console.log('1. FMP에서 전체 종목 리스트 다운로드 중... (시간이 좀 걸립니다)');
    
    // FMP: 거래 가능한 모든 티커 (약 6만 개 내외)
    const response = await fmpClient.get('/available-traded/list');
    const allTickers = response.data;
    
    console.log(`>> 데이터 수신 완료! 총 ${allTickers.length}개의 종목을 정리합니다.`);

    // 2. 거래소별 그룹핑 (Grouping)
    const groupedData = {};

    allTickers.forEach(item => {
        // 필수 정보 없는 쓰레기 데이터 제외
        if (!item.symbol || !item.name) return;

        // 거래소 이름 확인 (없으면 OTC)
        let exchange = item.exchangeShortName || 'OTC';
        
        // 특수문자 제거 (Firestore 문서 ID 규칙 준수)
        // 예: "TSX-V" -> "TSX_V" 등
        exchange = exchange.replace(/\//g, '_').replace(/\./g, '');

        // 국가 코드 매핑 (기본 US, 한국은 KR)
        let country = 'US'; 
        if (['KSE', 'KOSDAQ', 'KOE'].includes(exchange)) country = 'KR';
        if (['HKSE', 'HKG'].includes(exchange)) country = 'HK'; // 홍콩 등 필요시 추가
        if (['SHA', 'SHZ'].includes(exchange)) country = 'CN'; // 중국

        const docId = `${country}_${exchange}`;

        if (!groupedData[docId]) {
            groupedData[docId] = [];
        }

        // 최소 정보만 저장 (symbol, name)
        groupedData[docId].push({
            s: item.symbol, 
            n: item.name    
        });
    });

    console.log(`2. 그룹핑 완료. 총 ${Object.keys(groupedData).length}개의 거래소 그룹이 생성됨.`);

    // 3. Firestore 저장 (Batch 500개 제한 준수)
    const collectionRef = admin.firestore().collection('meta_tickers');
    let batch = admin.firestore().batch();
    let operationCount = 0;
    let totalSavedGroups = 0;
    
    for (const [docId, tickerList] of Object.entries(groupedData)) {
        const docRef = collectionRef.doc(docId);
        
        // 한 문서당 용량 제한(1MB) 고려하여 최대 8000개까지만 저장
        // (미국 주요 거래소 외에는 8000개 넘는 경우가 거의 없음)
        const safeList = tickerList.slice(0, 8000); 

        batch.set(docRef, {
            country: docId.split('_')[0],
            exchange: docId.split('_')[1],
            count: safeList.length,
            updatedAt: new Date().toISOString(),
            list: safeList
        });
        
        operationCount++;
        totalSavedGroups++;

        // 500개 차면 저장하고 비우기 (Firestore 제한)
        if (operationCount >= 400) { // 여유 있게 400에서 끊음
            await batch.commit();
            console.log(`>> 중간 저장 완료... (${totalSavedGroups}개 그룹)`);
            batch = admin.firestore().batch();
            operationCount = 0;
        }
    }

    // 남은 잔여 데이터 최종 저장
    if (operationCount > 0) {
        await batch.commit();
    }

    console.log(`3. 최종 완료! 총 ${totalSavedGroups}개의 거래소 문서가 저장되었습니다.`);
    
    res.json({
        message: '티커 마스터 동기화 성공',
        totalExchanges: totalSavedGroups,
        totalTickers: allTickers.length,
        example: Object.keys(groupedData).slice(0, 5)
    });

  } catch (error) {
    console.error('티커 동기화 실패:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// ============================================================
// [기능 2] 주요 지수(S&P500, Nasdaq100) 및 ETF 마스터 동기화
// ============================================================
app.post('/api/sync-index-master', verifyToken, async (req, res) => {
    try {
        console.log(`[Master Sync] 주요 지수 구성종목 및 ETF 업데이트 시작... (User: ${req.user.email})`);

        const collectionRef = admin.firestore().collection('meta_tickers');
        const batch = admin.firestore().batch();
        let logMsg = [];

        // -------------------------------------------------
        // 1. S&P 500 구성종목 가져오기
        // -------------------------------------------------
        try {
            const sp500Res = await fmpClient.get('/sp500_constituent');
            const sp500Data = sp500Res.data.map(item => ({ s: item.symbol, n: item.name, sec: item.sector }));
            
            batch.set(collectionRef.doc('US_SP500'), {
                country: 'US',
                exchange: 'SP500',
                description: 'S&P 500 구성종목',
                count: sp500Data.length,
                updatedAt: new Date().toISOString(),
                list: sp500Data
            });
            logMsg.push(`S&P500 (${sp500Data.length}개)`);
        } catch (e) {
            console.error("S&P500 실패:", e.message);
        }

        // -------------------------------------------------
        // 2. NASDAQ 100 구성종목 가져오기
        // -------------------------------------------------
        try {
            const ndxRes = await fmpClient.get('/nasdaq_constituent');
            const ndxData = ndxRes.data.map(item => ({ s: item.symbol, n: item.name, sec: item.sector }));

            batch.set(collectionRef.doc('US_NASDAQ100'), {
                country: 'US',
                exchange: 'NASDAQ100',
                description: 'NASDAQ 100 구성종목',
                count: ndxData.length,
                updatedAt: new Date().toISOString(),
                list: ndxData
            });
            logMsg.push(`NASDAQ100 (${ndxData.length}개)`);
        } catch (e) {
            console.error("NASDAQ100 실패:", e.message);
        }

        // -------------------------------------------------
        // 3. [업그레이드] 핵심 ETF 리스트 가져오기 (Smart Filtering)
        // -------------------------------------------------
        try {
            console.log("[Master Sync] 유동성 풍부한 ETF 선별 중...");
            
            // [변경] 단순 리스트 대신 '스크리너' 사용
            // 조건: ETF이면서 + 거래량(volume)이 50만 주 이상 + 주요 거래소
            const etfRes = await fmpClient.get('/stock-screener', {
                params: {
                    isEtf: true,
                    volumeMoreThan: 200000, // 일일 거래량 50만 주 이상 (유동성 필터)
                    exchange: 'NASDAQ,NYSE,AMEX' // 주요 거래소만
                }
            });
            
            // 필요한 필드만 추출
            const etfData = etfRes.data.map(item => ({ 
                s: item.symbol, 
                n: item.companyName, // screener는 'companyName' 필드를 씁니다
                sec: item.sector,    // 섹터 정보도 챙기면 좋음 (Technology 등)
                ind: item.industry   // 산업군 정보
            }));

            // 혹시 모르니 심볼 기준 정렬
            etfData.sort((a, b) => a.s.localeCompare(b.s));

            // Firestore 저장 (문서 ID: US_ETF_MAJOR 로 변경 추천)
            batch.set(collectionRef.doc('US_ETF'), {
                country: 'US',
                exchange: 'ETF',
                description: '미국 주요 유동성 ETF (Vol > 500k)',
                count: etfData.length,
                updatedAt: new Date().toISOString(),
                list: etfData
            });
            logMsg.push(`핵심ETF (${etfData.length}개)`);
        } catch (e) {
            console.error("ETF 필터링 실패:", e.message);
            // 실패 시 비상용으로 기존 단순 리스트 로직을 태우거나 에러 처리
        }

        // DB 저장 실행
        await batch.commit();

        console.log(`[Master Sync] 완료: ${logMsg.join(', ')}`);
        res.json({ success: true, message: `동기화 완료: ${logMsg.join(', ')}` });

    } catch (error) {
        console.error("Index Sync Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [기능 3] 상장폐지 종목 마스터 동기화 (Delisted Companies)
// ============================================================
app.post('/api/sync-delisted-master', verifyToken, async (req, res) => {
    try {
        console.log(`[Delisted Sync] 상장폐지 종목 리스트 요청... (User: ${req.user.email})`);
        
        // FMP API 호출 (페이징이 있는 경우도 있으나, 전체 리스트 엔드포인트 사용)
        const response = await fmpClient.get('/delisted-companies');
        const delistedData = response.data;

        console.log(`>> 수신 완료: 총 ${delistedData.length}개 종목`);

        const batch = admin.firestore().batch();
        const collectionRef = admin.firestore().collection('meta_delisted');
        
        // 데이터가 많으므로(수만 건) 최신 5,000개 또는 주요 종목만 저장하거나
        // 배치 처리를 여러 번 나눠서 해야 하지만, 여기선 최신 2000개만 샘플로 저장 (안전장치)
        const limitData = delistedData.slice(0, 2000); 

        // 기존 데이터 덮어쓰기 위해 문서 ID는 Symbol 사용
        limitData.forEach(item => {
            if (!item.symbol) return;
            const docRef = collectionRef.doc(item.symbol);
            batch.set(docRef, {
                symbol: item.symbol,
                name: item.companyName,
                exchange: item.exchange,
                delistedDate: item.delistedDate,
                ipoDate: item.ipoDate,
                updatedAt: new Date().toISOString()
            });
        });

        await batch.commit();
        res.json({ success: true, count: limitData.length });

    } catch (error) {
        console.error("Delisted Sync Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [기능 4] 기업 이벤트 (배당, 분할) 수집
// ============================================================
app.post('/api/load-corporate-actions', verifyToken, async (req, res) => {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Symbol required' });

    try {
        console.log(`[Actions] ${symbol} 배당 및 액면분할 정보 수집 중...`);
        const batch = admin.firestore().batch();
        let actionCount = 0;

        // 1. 배당 정보 (Historical Dividends)
        try {
            const divRes = await fmpClient.get(`/historical-price-full/stock_dividend/${symbol}`);
            if (divRes.data && divRes.data.historical) {
                const dividends = divRes.data.historical.slice(0, 100); // 최근 100건만
                
                // stocks/{symbol}/dividends 컬렉션에 저장
                dividends.forEach(d => {
                    const docRef = admin.firestore()
                        .collection('stocks').doc(symbol)
                        .collection('dividends').doc(d.date); // 날짜를 ID로
                    
                    batch.set(docRef, {
                        date: d.date,
                        dividend: d.dividend,
                        adjDividend: d.adjDividend,
                        recordDate: d.recordDate,
                        paymentDate: d.paymentDate,
                        declarationDate: d.declarationDate
                    });
                    actionCount++;
                });
            }
        } catch (e) { console.warn(`${symbol} 배당 정보 없음 또는 에러`); }

        // 2. 액면분할 (Stock Splits)
        try {
            const splitRes = await fmpClient.get(`/historical-price-full/stock_split/${symbol}`);
            if (splitRes.data && splitRes.data.historical) {
                const splits = splitRes.data.historical;
                
                splits.forEach(s => {
                    const docRef = admin.firestore()
                        .collection('stocks').doc(symbol)
                        .collection('splits').doc(s.date);
                    
                    batch.set(docRef, {
                        date: s.date,
                        numerator: s.numerator,
                        denominator: s.denominator,
                        ratio: `${s.numerator}:${s.denominator}`
                    });
                    actionCount++;
                });
            }
        } catch (e) { console.warn(`${symbol} 분할 정보 없음`); }

        if (actionCount > 0) await batch.commit();

        res.json({ success: true, symbol, count: actionCount });

    } catch (error) {
        console.error(`[Actions Error] ${symbol}:`, error.message);
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [기능 5] 재무제표 (Income, Balance, CashFlow) 및 프로필 수집
// ============================================================
app.post('/api/load-financials', verifyToken, async (req, res) => {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Symbol required' });

    try {
        console.log(`[Financials] ${symbol} 재무제표 및 상세정보 수집 중...`);
        const db = admin.firestore();
        const batch = db.batch();

        // 1. 기업 프로필 (시총, 산업, 기업개요 등)
        try {
            const profileRes = await fmpClient.get(`/profile/${symbol}`);
            if (profileRes.data && profileRes.data.length > 0) {
                const profile = profileRes.data[0];
                const docRef = db.collection('stocks').doc(symbol).collection('info').doc('profile');
                batch.set(docRef, profile);
            }
        } catch(e) { console.warn('Profile fetch fail'); }

        // 2. 재무제표 (연간 기준 최근 10년치)
        const statements = ['income-statement', 'balance-sheet-statement', 'cash-flow-statement'];
        
        for (const stmt of statements) {
            try {
                // limit=10: 최근 10년치
                const res = await fmpClient.get(`/${stmt}/${symbol}?limit=10`);
                if (res.data && res.data.length > 0) {
                    const docRef = db.collection('stocks').doc(symbol).collection('financials').doc(stmt);
                    // 배열 전체를 하나의 문서에 저장 (검색 효율성 위함)
                    batch.set(docRef, { 
                        type: stmt,
                        updatedAt: new Date().toISOString(),
                        history: res.data 
                    });
                }
            } catch (e) {
                console.warn(`${stmt} fetch fail for ${symbol}`);
            }
        }

        await batch.commit();
        res.json({ success: true, symbol, message: "재무 데이터 저장 완료" });

    } catch (error) {
        console.error(`[Financials Error] ${symbol}:`, error.message);
        res.status(500).json({ error: error.message });
    }
});


// [테스트용] API 키 생존 확인 (무료 엔드포인트)
app.get('/api/test-alive', async (req, res) => {
    try {
        // 애플(AAPL)의 기본 프로필 정보는 무료입니다.
        const response = await fmpClient.get('/profile/AAPL');
        res.json({ 
            status: 'ALIVE', 
            message: 'API 키는 살아있습니다. 프리미엄 기능만 막힌 것 같습니다.',
            data: response.data 
        });
    } catch (error) {
        res.status(error.response ? error.response.status : 500).json({ 
            status: 'DEAD', 
            message: 'API 키 자체가 정지되었습니다.',
            error: error.message 
        });
    }
});

// ======================================================================
// FMP API -   끝
// ======================================================================

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