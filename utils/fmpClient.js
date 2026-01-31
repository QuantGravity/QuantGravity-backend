// backend/utils/fmpClient.js
const axios = require('axios');
require('dotenv').config(); // .env에서 API 키를 가져오기 위해 필요

// FMP API 전용 통신 도구 생성
const fmpClient = axios.create({
  baseURL: 'https://financialmodelingprep.com/api/v3',
  params: {
    apikey: process.env.FMP_API_KEY // .env에 저장한 키 자동 적용
  }
});

module.exports = fmpClient;