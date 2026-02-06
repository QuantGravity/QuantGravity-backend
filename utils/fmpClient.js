// ===========================================================================
// [파일명] : utils/fmpClient.js
// [대상]   : FMP(Financial Modeling Prep) API 통신 전용 Axios 인스턴스
// [기준]   : 
//   1. 버전 관리: 최신 데이터 일관성을 위해 'stable' 엔드포인트를 기본 URL로 사용한다.
//   2. 보안 관리: API Key는 소스에 노출하지 않고 환경변수(process.env)를 통해 주입한다.
//   3. 편의성 제공: 모든 요청에 apikey 파라미터를 자동 포함하여 반복 코드를 방지한다.
//   4. 모듈화: 다른 라우터(fmp.js 등)에서 간편하게 불러와 사용할 수 있도록 인스턴스를 내보낸다.
// ===========================================================================

const axios = require('axios');
require('dotenv').config(); 

// FMP API 전용 통신 도구 생성
const fmpClient = axios.create({
  baseURL: 'https://financialmodelingprep.com/stable', 
  timeout: 10000, // [추가] 10초 이내에 응답 없으면 중단
  params: {
    apikey: process.env.FMP_API_KEY 
  }
});

module.exports = fmpClient;