// ===========================================================================
// [파일명] : routes/mail.js
// [대상]   : 이메일 발송 서비스 및 알림 API
// [기준]   : 
//   1. 보안 준수: 계정 정보는 절대 소스에 노출하지 않고 환경변수(EMAIL_USER, EMAIL_PASS)를 사용한다.
//   2. 타겟팅: 메일 발송 전 반드시 Firestore 'users' 컬렉션의 구독 여부('is_subscribed' == 'Y')를 검증한다.
//   3. 전송 효율: 다수 수신자 발생 시 배열을 쉼표로 결합하여 대량 발송(Bulk Send) 구조를 지원한다.
//   4. 에러 추적: 메일 전송 실패 시 상세 사유를 로그에 남기고 클라이언트에 오류 상태를 명확히 반환한다.
// ===========================================================================

// =========================================================
// 메일 전송용 Transporter 설정
// (Gmail 기준 예시: 보안 설정에서 '앱 비밀번호'를 생성해야 합니다)
// =========================================================
const express = require('express');
const router = express.Router();
const admin = require('firebase-admin');
const firestore = admin.firestore();
const { verifyToken, verifyBatchOrAdmin } = require('../utils/authHelper');
const nodemailer = require('nodemailer');
require('dotenv').config();

// 관리자 이메일 목록 (본인의 이메일을 넣으세요)
const ADMIN_EMAILS = ['your-email@gmail.com', 'partner-email@gmail.com'];

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
router.post('/send-common-email', async (req, res) => {
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