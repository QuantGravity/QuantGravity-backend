// ===========================================================================
// [파일명] : utils/db.js (또는 config/db.js)
// [대상]   : Firebase Admin SDK 및 Firestore 공통 데이터 처리 엔진
// [기준]   : 
//   1. 중앙 관리: 모든 DB 읽기/쓰기는 개별 라우터가 아닌 이 파일의 공통 함수를 통해 처리한다.
//   2. 무결성 보장: 쓰기(Upload) 작업 시 항상 serverTimestamp를 활용하여 데이터 생성 시간을 기록한다.
//   3. 에러 핸들링: DB 연결 실패나 쿼리 오류 시 상세 로그를 남기고 클라이언트에 정제된 에러를 반환한다.
//   4. 보안 원칙: 백엔드에서만 Admin SDK를 사용하며, API 키 등 인증 정보는 .env를 통해 주입받는다.
// ===========================================================================
const mysql = require('mysql2');
require('dotenv').config(); // 이 줄이 db.js 안에도 있는지 꼭 확인하세요!

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
// 타임존 연산을 무시하고 DB 값을 문자열 그대로 읽어옵니다.
  dateStrings: true,

  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// 연결 테스트용 로그 (서버 켤 때 터미널에 찍힙니다)
console.log('DB 연결 시도 중:', process.env.DB_USER); 

module.exports = pool.promise();