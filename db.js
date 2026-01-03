// db.js
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