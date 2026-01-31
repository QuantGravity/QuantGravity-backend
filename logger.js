// logger.js 수정

const admin = require('firebase-admin');
const firestore = admin.firestore();

const logTraffic = async (req, res, next) => {
    const userEmail = req.user ? req.user.email : 'guest';
    
    // 1. [기본] Path와 Method 기록
    const logData = {
        email: userEmail,
        method: req.method,
        path: req.originalUrl.split('?')[0], // 물음표 뒤 지저분한 파라미터 떼고 순수 경로만 저장
        ip: req.headers['x-forwarded-for'] || req.connection.remoteAddress,
        timestamp: admin.firestore.FieldValue.serverTimestamp()
    };

    // 2. [핵심 추가] 사용자가 '무엇'에 관심 있는지 추출 (Context Logging)
    // 요청에 담긴 데이터(body나 query)에서 ticker나 strategy_code 같은 핵심 단어만 추출합니다.
    let interestKeyword = null;

    if (req.method === 'GET') {
        // GET 요청은 주소창 뒤에 붙은 값(query) 확인
        // 예: /api/daily-stock?ticker=AAPL
        interestKeyword = req.query.ticker || req.query.code || req.query.id; 
    } else {
        // POST 요청은 본문(body) 확인
        // 예: 시뮬레이션 돌릴 때 strategies 배열 안의 ticker
        if (req.body.ticker) interestKeyword = req.body.ticker;
        else if (req.body.strategies && req.body.strategies.length > 0) {
            // 여러 개 돌릴 땐 첫 번째 종목만이라도 기록 (예: TQQQ 외 2건)
            const firstTicker = req.body.strategies[0].ticker;
            const count = req.body.strategies.length;
            interestKeyword = count > 1 ? `${firstTicker} 외 ${count-1}건` : firstTicker;
        }
    }

    // 관심 키워드가 있으면 로그에 추가 (없으면 저장 안 함)
    if (interestKeyword) {
        logData.target = interestKeyword; 
    }

    // 3. 타입 구분 (기존 로직)
    if (req.method === 'GET') {
        logData.type = 'ACCESS';
    } else {
        logData.type = 'ACTION';
    }

    // 4. 저장
    firestore.collection('traffic_logs').add(logData)
        .catch(err => console.error("로깅 실패:", err));

    next();
};

module.exports = { logTraffic };