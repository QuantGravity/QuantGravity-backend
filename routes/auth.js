// ===========================================================================
// [파일명] : routes/auth.js
// [대상]   : 사용자 인증 및 계정 관리 (로그인, 회원가입, 토큰 검증 등)
// [참조]   : Front-end의 login.html, register.html과 연동
// ===========================================================================

// [상단에 추가] 필수 모듈 불러오기
const express = require('express');
const router = express.Router(); // app 대신 router 사용
const admin = require('firebase-admin');
const axios = require('axios');
const firestore = admin.firestore();
const { generateToken } = require('../utils/authHelper'); // 토큰 생성기 연결

// 1. 로그인 처리 API (판사 역할: 백앤드)
router.post('/login', async (req, res) => {
    const { token, provider } = req.body;

    let email = "";
    let uid = "";
    
    // [추가] 가입창에 채워줄 정보들을 담을 변수들
    let name = "";
    let mobile = "";
    let gender = "";
    let birthyear = "";

    try {
        // [구글 로그인 검증]
        if (provider === 'GOOGLE') {
            const decodedToken = await admin.auth().verifyIdToken(token);
            email = decodedToken.email;
            uid = decodedToken.uid;
            name = decodedToken.name || ""; // 구글 이름 가져오기
            // 구글은 기본적으로 휴대폰/성별 정보를 주지 않습니다.
        } 
        // [네이버 로그인 검증]
        else if (provider === 'NAVER') {
            if (!token) return res.status(400).json({ error: "네이버 토큰 누락" });

            try {
                const naverResponse = await axios.get('https://openapi.naver.com/v1/nid/me', {
                    headers: { 'Authorization': `Bearer ${token}` }
                });

                const naverUser = naverResponse.data.response;

                // ★ [디버깅 추가] 서버 터미널에서 이 로그를 확인하세요!
                console.log("🔥 [DEBUG] 네이버가 준 전체 원본 데이터:", JSON.stringify(naverUser, null, 2));

                email = naverUser.email;
                uid = naverUser.id;
                
                // [수정] 이름과 닉네임을 각각 분리해서 확보합니다.
                // name: 실명, nickname: 별명
                const realName = naverUser.name || "";
                const nickName = naverUser.nickname || "";
                
                // 프론트엔드로 보낼 'name' 변수에는 
                // 1순위: 닉네임, 2순위: 실명 순서로 담습니다.
                name = nickName || realName || "";
                gender = naverUser.gender || ""; // "M", "F" 등
                birthyear = naverUser.birthyear || ""; // "1990"

            } catch (naverError) {
                console.error("네이버 API 에러:", naverError.message);
                return res.status(401).json({ error: "네이버 인증 실패" });
            }
        }

        // 1. 유저 조회
        const userDoc = await firestore.collection('users').doc(email).get();

        // 2. 가입되지 않은 유저 -> [수정] 정보를 꽉 채워서 보냄
        if (!userDoc.exists) {
            return res.json({ 
                status: 'NEED_REGISTER', 
                email: email,
                provider: provider,
                uid: uid,
                // [추가된 정보들]
                name: name,
                mobile: mobile,
                gender: gender,
                birthyear: birthyear
            });
        }

        // 3. (로그인 성공 로직은 그대로 유지...)
        const userData = userDoc.data();
        const systemToken = generateToken({ uid, email, role: userData.role || 'G1' });
        
        res.json({ 
            status: 'SUCCESS',
            token: systemToken,
            user: { email, displayName: userData.name, role: userData.role || 'G1' }
        });

    } catch (error) {
        console.error("Login Error:", error);
        res.status(401).json({ error: "인증 실패" });
    }
});

// 2. 회원가입 처리 API (DB 쓰기 권한을 백앤드만 가짐)
router.post('/register', async (req, res) => {
    try {
        // req.body에 'uid'를 추가로 받습니다. (구글인 경우 프론트에서 보내줌)
        const { email, name, country, gender, birthyear, provider, uid } = req.body;

        // 필수값 검증 (백앤드에서 한 번 더 체크)
        if (!email || !name) return res.status(400).json({ error: "필수 정보 누락" });

        // [핵심 수정] 
        // 아까 로그인 단계에서 찾아낸 '진짜 ID(uid)'가 넘어왔을 테니, 그걸 그대로 사용합니다.
        // 만약(혹시라도) uid가 없다면 안전장치로 email을 사용합니다.
        const finalUid = uid ? uid : email;

        const newUser = {
            uid: finalUid, // ★ DB에 명시적으로 저장 (나중에 찾기 편함)
            email,
            name,
            country,
            gender,
            birthyear,
            role: 'G1', // 신규 가입은 무조건 G1 고정 (해킹 방지)
            membershipLevel: 'FREE',
            status: 'ACTIVE',
            provider: provider || 'SOCIAL',
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            lastLogin: admin.firestore.FieldValue.serverTimestamp()
        };

        // DB 저장
        await firestore.collection('users').doc(email).set(newUser);

        // 가입 완료 후 바로 로그인 처리 토큰 발급
        const systemToken = generateToken({ 
            uid: finalUid,
            email: email, 
            role: 'G1' 
        });

        res.json({ 
            success: true, 
            token: systemToken,
            user: { email, displayName: name, role: 'G1', uid: finalUid }
        });

    } catch (error) {
        console.error("Register Error:", error);
        res.status(500).json({ error: "회원가입 처리 실패" });
    }
});

module.exports = router;