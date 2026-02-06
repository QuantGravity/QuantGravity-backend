// ===========================================================================
// [파일명] : utils/logger.js
// [대상]   : 사용자 트래픽 및 관심사(Context) 추적 로거
// [기준]   : 
//   1. 데이터 정제: originalUrl에서 쿼리 스트링을 제거하여 순수 API 경로만 기록한다.
//   2. 관심사 추출: GET/POST 요청의 body와 query를 분석하여 핵심 키워드(ticker 등)를 target 필드에 담는다.
//   3. 자동 분류: 요청 메서드에 따라 ACCESS(조회)와 ACTION(수행) 타입으로 자동 구분한다.
//   4. 성능 우선: 로깅 작업은 백그라운드(비동기)로 수행하며, 실패하더라도 메인 API 로직에 영향을 주지 않는다.
// ===========================================================================

const admin = require('firebase-admin');
const firestore = admin.firestore();

const logTraffic = async (req, res, next) => {
    try {
        const userEmail = req.user ? req.user.email : 'guest';
        
        const logData = {
            email: userEmail,
            method: req.method,
            path: req.originalUrl.split('?')[0],
            ip: req.headers['x-forwarded-for'] || req.connection.remoteAddress,
            timestamp: admin.firestore.FieldValue.serverTimestamp()
        };

        let interestKeyword = null;

        if (req.method === 'GET') {
            interestKeyword = req.query.ticker || req.query.code || req.query.id; 
        } else {
            // [추가] 일반 티커 배열(tickers) 처리 로직 추가
            if (req.body.ticker) {
                interestKeyword = req.body.ticker;
            } else if (req.body.tickers && req.body.tickers.length > 0) {
                const count = req.body.tickers.length;
                interestKeyword = count > 1 ? `${req.body.tickers[0]} 외 ${count-1}건` : req.body.tickers[0];
            } else if (req.body.strategies && req.body.strategies.length > 0) {
                const firstTicker = req.body.strategies[0].ticker;
                const count = req.body.strategies.length;
                interestKeyword = count > 1 ? `${firstTicker} 외 ${count-1}건` : firstTicker;
            }
        }

        if (interestKeyword) logData.target = interestKeyword; 
        logData.type = (req.method === 'GET') ? 'ACCESS' : 'ACTION';

        // 서버 성능을 위해 .add() 이후에 .then()이나 await을 기다리지 않고 즉시 next() 실행
        firestore.collection('traffic_logs').add(logData)
            .catch(err => console.error("로깅 저장 실패:", err));

    } catch (criticalErr) {
        // 로거 실패가 서비스 장애로 이어지지 않도록 예외 처리
        console.error("Logger Middleware Critical Error:", criticalErr);
    }

    next();
};

module.exports = { logTraffic };