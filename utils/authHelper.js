// ===========================================================================
// [파일명] : utils/authHelper.js
// [대상]   : JWT(JSON Web Token) 기반 사용자 인증 및 보안 미들웨어
// [기준]   : 
//   1. 보안 유효성: 토큰의 유효 기간은 6시간(6h)으로 설정하여 세션 탈취 위험을 최소화한다.
//   2. 데이터 무결성: 토큰 페이로드에는 uid, email, role 등 최소한의 필수 정보만 담는다.
//   3. 로깅 강화: 토큰 누락이나 위조 시도 시 IP와 경로를 로그로 남겨 보안 위협에 대응한다.
//   4. 미들웨어 활용: 모든 보호된 API 라우터 상단에 verifyToken을 배치하여 검증을 의무화한다.
// ===========================================================================
const jwt = require('jsonwebtoken');
require('dotenv').config();

const SECRET_KEY = process.env.JWT_SECRET;

// 1. 인증키(Token) 발급 함수
const generateToken = (userProfile) => {
    const payload = {
        uid: userProfile.uid,
        email: userProfile.email,
        role: userProfile.role // 예: 'G1', 'G9', 'admin'
    };
    return jwt.sign(payload, SECRET_KEY, { expiresIn: '6h' });
};

// 2. [신규] 배치잡 또는 관리자 권한 통합 검증 미들웨어
const verifyBatchOrAdmin = (req, res, next) => {
    const batchKey = req.headers['x-batch-key']; // 배치잡용 헤더
    const authHeader = req.headers['authorization']; // 관리자용 헤더
    const token = authHeader && authHeader.split(' ')[1];

    // Case A: 배치 키 검증 (가장 우선순위 높음)
    // [수정됨] BATCH_SECRET_KEY를 process.env.BATCH_SECRET_KEY로 변경하여 ReferenceError 해결
    if (batchKey && batchKey === process.env.BATCH_SECRET_KEY) {
        console.log(`🤖 [Auth] Batch Key 인증 성공 - Path: ${req.originalUrl}`);
        req.isBatch = true; // 배치 호출임을 표시
        return next();
    }

    // Case B: 관리자 토큰 검증 (배치 키가 없을 경우)
    if (!token) {
        return res.status(401).json({ error: "인증 정보(Batch Key 또는 Token)가 없습니다." });
    }

    jwt.verify(token, SECRET_KEY, (err, decodedUser) => {
        if (err) {
            return res.status(403).json({ error: "유효하지 않은 인증키입니다." });
        }

        // 관리자 권한 체크 ('admin' 또는 'G9')
        if (!['admin', 'G9'].includes(decodedUser.role)) {
            console.warn(`[Forbidden] 권한 부족 - User: ${decodedUser.email}`);
            return res.status(403).json({ error: "관리자 권한이 필요합니다." });
        }

        req.user = decodedUser;
        next();
    });
};

// 2. 로그인 여부 검증 미들웨어
const verifyToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (!token) {
        const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
        console.warn(`[Security Alert] 토큰 없음 - IP: ${ip}, Path: ${req.originalUrl}`);
        return res.status(401).json({ error: "인증키가 없습니다." });
    }

    jwt.verify(token, SECRET_KEY, (err, decodedUser) => {
        if (err) {
            const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
            console.warn(`[Security Alert] 토큰 위조/만료 - IP: ${ip}`);
            return res.status(403).json({ error: "유효하지 않은 인증키입니다." });
        }
        
        req.user = decodedUser; // 다음 미들웨어에서 유저 정보를 쓸 수 있게 저장
        next(); 
    });
};

module.exports = { generateToken, verifyToken, verifyBatchOrAdmin };