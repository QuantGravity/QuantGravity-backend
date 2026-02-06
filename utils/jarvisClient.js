// ===========================================================================
// [파일명] : utils/jarvisClient.js
// [대상]   : Google AI Studio (Gemini 2.5) API 연동 핵심 엔진
// [기준]   : 
//   1. 모델 최신화: 최신 성능을 위해 'gemini-2.5-flash' 모델을 기본 엔진으로 채택한다.
//   2. 보안 철저: API Key는 소스에 노출하지 않고 환경변수(GEMINI_API_KEY)로 엄격히 관리한다.
//   3. 에러 가시성: 통신 실패 시 상태 코드와 상세 데이터를 로그로 출력하여 빠른 디버깅을 지원한다.
//   4. 무결성 검증: 응답 데이터의 후보군(candidates) 존재 여부를 확인하여 런타임 에러를 방지한다.
// ===========================================================================
const axios = require('axios');
require('dotenv').config();

/**
 * 자비스(Gemini)에게 질문을 던지고 응답 텍스트를 받는 핵심 함수
 * @param {string} prompt - AI에게 전달할 지령
 * @returns {Promise<string>} - AI가 생성한 텍스트 응답
 */

async function askJarvis(prompt) {
    try {
        console.log("🤖 자비스 2.5 엔진 가동 중...");

        const API_KEY = process.env.GEMINI_API_KEY;
        if (!API_KEY) throw new Error("GEMINI_API_KEY가 환경변수에 설정되지 않았습니다.");

        const modelName = "gemini-2.5-flash"; 
        const url = `https://generativelanguage.googleapis.com/v1beta/models/${modelName}:generateContent?key=${API_KEY}`;
        
        const payload = {
            contents: [{
                parts: [{ text: prompt }]
            }]
        };

        const response = await axios.post(url, payload);
        const data = response.data;

        // [추가] Safety Filter 및 차단 사유 디버깅 로그
        if (data.promptFeedback) {
            console.log("🛡️ [Safety Check] 질문 피드백:", JSON.stringify(data.promptFeedback, null, 2));
        }

        if (data.candidates && data.candidates[0].content) {
            const text = data.candidates[0].content.parts[0].text;
            console.log("✅ 자비스 응답 성공!");
            return text;
        } 
        
        // [수정] 응답이 차단된 경우 상세 사유 출력
        const finishReason = data.candidates && data.candidates[0].finishReason;
        if (finishReason === 'SAFETY') {
            console.error("🚫 [Blocked] 안전 정책에 의해 응답이 차단되었습니다. (사유: SAFETY)");
            throw new Error("자비스가 안전 정책상 답변하기 어려운 질문입니다.");
        }

        throw new Error("AI 응답 형식이 올바르지 않거나 차단되었습니다.");
        
    } catch (error) {
        console.error("❌ 자비스 통신 에러:");
        if (error.response) {
            console.error("Status:", error.response.status);
            console.error("Data:", JSON.stringify(error.response.data, null, 2));
        } else {
            console.error("Error Message:", error.message);
        }
        throw error;
    }
}

module.exports = { askJarvis };