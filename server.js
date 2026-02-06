// ===========================================================================
// File Name : server.js
// 포함할 대상 : Firestore 관련한 공통 함수
// ===========================================================================

// [1] 필수 모듈 로드 (express, admin, dotenv 등)
// [2] Firebase 초기화 (firestore 선언)
// [3] 앱 설정 (app = express())
// [4] 미들웨어 설정 (CORS, JSON 파싱 한 번만!)
// [5] 라우터 로드 및 연결 (auth, firestore, simulation, batch, stock, jarvis, billing, fmp)
// [6] 보안 미들웨어 및 로거 로드 (verifyToken, logTraffic)
// [7] 정적 파일 설정 (express.static)
// [8] 서버 실행 (app.listen)

// [1] 필수 모듈 로드
const express = require('express');
const axios = require('axios');
const path = require('path');
const cors = require('cors');
const cron = require('node-cron');
const admin = require('firebase-admin');
require('dotenv').config();

// [2] Firebase 초기화 (최상단 유지)
const serviceAccount = {
  projectId: process.env.FIREBASE_PROJECT_ID,
  clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
  privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n')
};
if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        ignoreUndefinedProperties: true, // undefined 필드는 무시하고 저장/쿼리함
    });
}
const firestore = admin.firestore();
console.log("Firebase Admin SDK가 성공적으로 연결되었습니다.");

// [3] 앱 설정
const app = express(); // ★ 반드시 라우터 연결보다 위에 있어야 함!
const port = process.env.PORT || 3000;

// [4] 미들웨어 설정
app.use(cors({
    origin: [
        'http://localhost:3000', 'http://127.0.0.1:3000', 'http://localhost:5500',
        'https://quant-navigator.web.app', 'https://quant-navigator-backend.onrender.com'
    ],
    credentials: true
}));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// [5] 라우터 로드 및 연결
const authRoutes = require('./routes/auth');
const firestoreRoutes = require('./routes/firestore');
const simulationRoutes = require('./routes/simulation');
const batchRoutes = require('./routes/batch');
const stockRoutes = require('./routes/stock');
const jarvisRoutes = require('./routes/jarvis');
const billingRoutes = require('./routes/billing');
const fmpRoutes = require('./routes/fmp'); // fmp 추가

app.use('/api/auth', authRoutes);
app.use('/api/firestore', firestoreRoutes);
app.use('/api/simulation', simulationRoutes);
app.use('/api/batch', batchRoutes);
app.use('/api/stock', stockRoutes);
app.use('/api/jarvis', jarvisRoutes);
app.use('/api/billing', billingRoutes);
app.use('/api/fmp', fmpRoutes); // 경로 통일

require('dotenv').config();

// // [추가] 보안 모듈 가져오기 - server.js 상단 
const { generateToken, verifyToken } = require('./utils/authHelper');
const { logTraffic } = require('./utils/logger'); // [추가] 로거 가져오기

app.use(express.static(path.join(__dirname, '../front')));

// ----------------------------------------------------------------
// 마지막에 둬야 하는 것
// ----------------------------------------------------------------
// 서버 실행
app.listen(port, () => {
    console.log(`서버 실행 중: http://localhost:${port}`);
});
