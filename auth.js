// File Name : auth.js
const jwt = require('jsonwebtoken');
require('dotenv').config();

const SECRET_KEY = process.env.JWT_SECRET;

// 1. [핵심] 인증키(Token) 발급 함수
// 로그인 성공 시 백앤드가 이 함수를 통해 '출입증'을 찍어줍니다.
const generateToken = (userProfile) => {
    // 토큰에 담을 정보 (payload)
    const payload = {
        uid: userProfile.uid,
        email: userProfile.email,
        role: userProfile.role // 예: 'admin' 또는 'user'
    };

    // 토큰 생성 (유효기간 6시간 설정)
    return jwt.sign(payload, SECRET_KEY, { expiresIn: '6h' });
};

// 2. [보안 검문소] 미들웨어 함수
// 프론트엔드가 요청을 보낼 때마다 이 함수가 먼저 가로채서 검사합니다.
const verifyToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (!token) {
        // [보안 로그] 토큰 없이 접근 시도한 IP 기록
        const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
        console.warn(`[Security Alert] 토큰 없음 - IP: ${ip}, Path: ${req.originalUrl}`);
        
        return res.status(401).json({ error: "인증키가 없습니다." });
    }

    jwt.verify(token, SECRET_KEY, (err, decodedUser) => {
        if (err) {
            // [보안 로그] 위조된 토큰 접근 시도
            const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
            console.warn(`[Security Alert] 토큰 위조/만료 - IP: ${ip}`);
            
            return res.status(403).json({ error: "유효하지 않은 인증키입니다." });
        }
        
        req.user = decodedUser;
        next(); 
    });
};

module.exports = { generateToken, verifyToken };