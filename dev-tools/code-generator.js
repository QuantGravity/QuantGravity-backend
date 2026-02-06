// ===========================================================================
// [파일명] : dev-tools/code-generator.js
// [대상]   : 자비스 기반 소스 코드 자동 생성 및 파일 빌드 도구 (CLI 전용)
// [기준]   : 
//   1. 개발 전용: 실제 서버 런타임에는 사용되지 않으며, 로컬 개발 환경에서만 실행한다.
//   2. 경로 제어: BACK 폴더를 루트로 인식하며, 인자로 받은 경로에 따라 파일을 생성/덮어쓴다.
//   3. 프롬프트: '퀀트 그래비티'의 수석 개발자 페르소나를 유지하며 순수 코드만 반환받는다.
//   4. 안전주의: 실행 전 목표 파일 경로를 콘솔에 명시하여 의도치 않은 덮어쓰기를 방지한다.
// ===========================================================================
const axios = require('axios');
const fs = require('fs');
const path = require('path');
require('dotenv').config(); // BACK 폴더의 .env를 사용

// Gemini 설정
const API_KEY = process.env.GEMINI_API_KEY;
const MODEL_NAME = "gemini-2.5-flash"; // 최신 모델 사용

async function generateCode(requirement, filePath) {
    // 1. 경로 설정 (BACK 폴더 기준)
    // 예: "routes/test.js" -> BACK/routes/test.js
    // 예: "../FRONT/js/test.js" -> FRONT/js/test.js
    const rootDir = path.resolve(__dirname, '..'); // dev-tools의 상위 폴더(=BACK)
    const absolutePath = path.resolve(rootDir, filePath);
    
    console.log(`\n🤖 [자비스] 작업 시작...`);
    console.log(`📂 목표 파일: ${absolutePath}`);
    console.log(`📝 요청 내용: ${requirement}\n`);

    // 2. 자비스에게 줄 지시사항 (프롬프트 엔지니어링)
    const systemInstruction = `
        너는 '퀀트 그래비티' 프로젝트의 수석 개발자야.
        
        [프로젝트 구조]
        - 백앤드: Node.js (Express), Firebase Admin SDK
        - 프론트엔드: HTML, Vanilla JS, CSS (별도 빌드 과정 없음)
        - DB: Firestore (컬렉션: billing_plans, users, billing_keys 등)
        
        [임무]
        사용자의 요구사항을 듣고 해당 파일(${path.basename(filePath)})에 들어갈 **완벽한 소스 코드**를 작성해.
        
        [제약사항]
        1. 마크다운(\`\`\`)이나 설명 텍스트를 절대 포함하지 마. 오직 코드만 반환해.
        2. 기존 코드가 있다면 덮어쓰기되므로, 필요한 모든 의존성(require 등)을 포함해.
        3. 코드는 프로덕션 레벨로, 에러 처리와 주석을 꼼꼼히 작성해.
    `;

    const prompt = `${systemInstruction}\n\n[USER REQUEST]: ${requirement}`;

    try {
        const url = `https://generativelanguage.googleapis.com/v1beta/models/${MODEL_NAME}:generateContent?key=${API_KEY}`;
        const response = await axios.post(url, {
            contents: [{ parts: [{ text: prompt }] }]
        });

        let code = response.data.candidates[0].content.parts[0].text;
        
        // 혹시 모를 마크다운 잔재 제거
        code = code.replace(/^```\w*\n?/, '').replace(/```$/, '').trim();

        // 3. 파일 저장
        const dir = path.dirname(absolutePath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

        fs.writeFileSync(absolutePath, code, 'utf8');
        console.log(`✅ [성공] 파일이 생성되었습니다!`);
        console.log(`👉 확인: ${absolutePath}`);

    } catch (error) {
        console.error("❌ [실패] 코드 생성 중 오류 발생:");
        console.error(error.response?.data?.error?.message || error.message);
    }
}

// CLI 실행 인수 처리
const task = process.argv[2];
const targetFile = process.argv[3];

if (!task || !targetFile) {
    console.log("⚠️ 사용법: node dev-tools/code-generator.js \"요청사항\" \"파일경로\"");
    console.log("예시: node dev-tools/code-generator.js \"결제 라우터\" \"routes/billing.js\"");
} else {
    generateCode(task, targetFile);
}