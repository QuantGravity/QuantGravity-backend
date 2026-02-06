// ===========================================================================
// [파일명] : routes/jarvis.js
// [대상]   : Google AI Studio(Gemini 2.5) 연동 및 코드 자동 빌더 API
// [기준]   : 
//   1. 모델 최신화: 'gemini-2.5-flash' 모델을 기본으로 사용하며 가용 모델을 상시 체크한다.
//   2. 스타일 가이드: 코드 생성 시 '퀀트 그래비티' 프로젝트의 6대 코딩 원칙을 엄격히 준수한다.
//   3. 안전한 쓰기: 파일 생성 전 디렉토리 존재 여부를 확인하고 필요 시 자동으로 생성(recursive)한다.
//   4. 무결성 보장: 수정 요청 시 기존 소스 코드 전체를 자비스에게 전달하여 로직 유실을 방지한다.
// ===========================================================================
const express = require('express');
const router = express.Router();
const path = require('path');
const fs = require('fs');
const { askJarvis } = require('../utils/jarvisClient'); // 유틸리티 연결
const admin = require('firebase-admin'); // [추가] DB 사용을 위해 필요
const db = admin.firestore();            // [추가] Firestore 인스턴스

// ---------------------------------------------------------------------------
// [Util] AI 응답에서 JSON만 깔끔하게 추출하는 함수
// ---------------------------------------------------------------------------
function cleanAndParseJSON(text) {
    try {
        // 마크다운 코드 블록 제거 (```json ... ```)
        let cleanText = text.replace(/```json/g, '').replace(/```/g, '').trim();
        // 혹시 모를 앞뒤 잡동사니 제거 ( [ 로 시작해서 ] 로 끝나는 부분만 추출)
        const firstBracket = cleanText.indexOf('[');
        const lastBracket = cleanText.lastIndexOf(']');
        if (firstBracket !== -1 && lastBracket !== -1) {
            cleanText = cleanText.substring(firstBracket, lastBracket + 1);
        }
        return JSON.parse(cleanText);
    } catch (e) {
        console.error("JSON 파싱 실패:", text);
        throw new Error("AI 응답을 JSON으로 변환하는데 실패했습니다.");
    }
}

// ---------------------------------------------------------------------------
// [API 1] 신규 테마 리서치 (중복 방지 + 종목 15개 수집)
// 요청 URL: /api/admin/research-new-themes
// ---------------------------------------------------------------------------
// ===========================================================================
// [API 1] 신규 테마 리서치 (종목 선정 사유 포함 업그레이드)
// 요청 URL: /api/jarvis/research-new-themes
// ===========================================================================
router.post('/research-new-themes', async (req, res) => {
    console.log("🤖 [자비스] 신규 테마 리서치 시작 (선정 사유 포함)...");
    req.setTimeout(90000); // 90초 타임아웃

    try {
        // [Step 1] 기존 테마 목록 로딩 (중복 방지)
        const snapshot = await db.collection('market_themes').get();
        let existingThemes = "None";
        if (!snapshot.empty) {
            existingThemes = snapshot.docs.map(doc => doc.data().name_en).join(', ');
        }

        // [Step 2] AI 요청 (프롬프트 강화)
        const prompt = `
            너는 월스트리트의 수석 퀀트 분석가야.
            현재 미국 주식 시장을 주도하고 있는 '투자 테마(Investment Themes)' 10가지를 발굴해줘.

            [중복 방지]
            이미 보유 중인 다음 테마들은 제외해: ${existingThemes}

            [핵심 요구사항]
            1. 각 테마별로 **상위 15개 주도주(Key Stocks)**를 포함할 것.
            2. 종목별로 **'선정 사유(reason)'를 한글 15~30자 내외로 간결하게** 작성할 것. (핵심만 요약)
            3. 관련성 점수(relevance_score)를 100점 만점으로 평가할 것.

            [JSON 포맷 예시]
            [
                {
                    "id": "ai_robotics",
                    "name_en": "AI Robotics",
                    "name_ko": "AI 로봇 공학",
                    "description": "휴머노이드 및 산업 자동화 로봇 기술",
                    "relevance_score": 95,
                    "tickers": [
                        { 
                            "symbol": "TSLA", 
                            "name": "Tesla", 
                            "relevance_score": 98, 
                            "reason": "옵티머스 로봇 개발 및 AI 자율주행 선두" 
                        },
                        ...
                    ]
                }
            ]
        `;

        const responseText = await askJarvis(prompt);
        const themes = cleanAndParseJSON(responseText);

        // [Step 3] Firestore 저장
        const batch = db.batch();
        themes.forEach(theme => {
            const docRef = db.collection('market_themes').doc(theme.id);
            const tickerList = theme.tickers || [];
            
            batch.set(docRef, {
                ...theme,
                tickers: tickerList,
                ticker_count: tickerList.length,
                updated_at: new Date().toISOString()
            }, { merge: true });
        });

        await batch.commit();
        console.log(`✅ [자비스] ${themes.length}개 테마 (사유 포함) 저장 완료`);
        res.json({ success: true, count: themes.length });

    } catch (error) {
        console.error("리서치 에러:", error);
        res.status(500).json({ error: error.message });
    }
});

// ===========================================================================
// [API 2] 기존 테마 업데이트 (선정 사유 포함 업그레이드)
// 요청 URL: /api/jarvis/update-theme-tickers
// ===========================================================================
router.post('/update-theme-tickers', async (req, res) => {
    const { themeId } = req.body;
    console.log(`🤖 [자비스] 테마 종목 업데이트: ${themeId}`);

    try {
        const themeDoc = await db.collection('market_themes').doc(themeId).get();
        if (!themeDoc.exists) throw new Error("존재하지 않는 테마입니다.");
        
        const themeName = themeDoc.data().name_en;

        const prompt = `
            투자 테마 '${themeName}'의 **상위 15개 핵심 종목**을 다시 분석해줘.
            
            [요구사항]
            1. 각 종목의 **'선정 사유(reason)'를 한글 20자 내외로** 작성해줘. (예: "해당 분야 시장 점유율 1위")
            2. 대형주와 핵심 중소형주를 포함하고, 관련성 점수(100점 만점)를 매겨줘.
            3. JSON 배열 포맷 준수.

            [JSON 예시]
            [
                { "symbol": "NVDA", "name": "NVIDIA", "relevance_score": 99, "reason": "AI GPU 시장 독점적 지위" }
            ]
        `;

        const responseText = await askJarvis(prompt);
        const tickers = cleanAndParseJSON(responseText);

        await db.collection('market_themes').doc(themeId).update({
            tickers: tickers,
            ticker_count: tickers.length,
            updated_at: new Date().toISOString()
        });

        console.log(`✅ [자비스] ${themeName} 종목 업데이트 완료`);
        res.json({ success: true, count: tickers.length });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: error.message });
    }
});

// ===========================================================================
// [API 4] 테마 지수(Index) 산출 (DB 데이터 전용 - 디버깅 강화판)
// ===========================================================================
router.post('/calculate-theme-index', async (req, res) => {
    const { themeId } = req.body;
    
    // DB 작업만 하므로 타임아웃은 적당히 설정
    req.setTimeout(60000); 

    console.log(`📊 [자비스] 테마 지수 산출 시작 (DB Only): ${themeId}`);

    try {
        // 1. 테마 정보 가져오기
        const themeDoc = await db.collection('market_themes').doc(themeId).get();
        if (!themeDoc.exists) throw new Error("테마를 찾을 수 없습니다.");
        
        const themeData = themeDoc.data();
        const tickers = themeData.tickers || [];
        
        if (tickers.length === 0) throw new Error("구성 종목이 없습니다.");

        console.log(`.. 대상 종목: ${tickers.length}개`);

        // 2. 종목별 데이터 로딩 (병렬 처리)
        const stockDataPromises = tickers.map(async (t) => {
            const symbol = t.symbol;
            const stockRef = db.collection('stocks').doc(symbol);
            const stockSnap = await stockRef.get();

            // [진단 1] 종목 문서(상위)가 아예 없는 경우
            if (!stockSnap.exists) {
                console.warn(`❌ [${symbol}] 종목 마스터 문서(stocks/${symbol})가 없음 -> 스킵`);
                return null;
            }

            const profile = stockSnap.data();
            
            // [진단 2] 시가총액(snapshot.mktCap) 정보 확인
            // 지수 산출 공식: (과거 주가 * 발행주식수)
            // 발행주식수 = 현재 시가총액 / 현재 주가
            let sharesOutstanding = 0;

            if (profile.snapshot && profile.snapshot.mktCap && profile.snapshot.price) {
                sharesOutstanding = profile.snapshot.mktCap / profile.snapshot.price;
            } else {
                console.warn(`⚠️ [${symbol}] 프로필/시총 데이터 부족 (snapshot.mktCap 없음) -> 스킵`);
                // 팁: 만약 프로필 데이터가 없어도 강제로 차트를 그리고 싶다면,
                // 아래 줄 주석을 풀고 임시로 주식수를 1로 설정하세요. (단, 단순 평균 방식이 됨)
                // sharesOutstanding = 1; 
                return null; 
            }

            // [진단 3] 과거 주가 데이터 가져오기
            const targetYears = ['2023', '2024', '2025', '2026'];
            let historyMap = {}; 
            let hasData = false;

            for (const year of targetYears) {
                const yearDoc = await stockRef.collection('annual_data').doc(year).get();
                if (yearDoc.exists) {
                    const dailyList = yearDoc.data().data || [];
                    if (dailyList.length > 0) hasData = true;
                    
                    dailyList.forEach(day => {
                        // 수정주가 우선, 없으면 종가 사용
                        const price = day.adjClose || day.close;
                        if(price) historyMap[day.date] = price;
                    });
                }
            }

            if (!hasData) {
                console.warn(`⚠️ [${symbol}] 연도별 주가 데이터(annual_data)가 없음 -> 스킵`);
                return null;
            }

            return { symbol, shares: sharesOutstanding, history: historyMap };
        });

        const stocks = (await Promise.all(stockDataPromises)).filter(s => s !== null);

        // 유효한 종목이 하나도 없으면 중단
        if (stocks.length === 0) {
            console.error("🔥 유효한 데이터가 있는 종목이 0개입니다. (프로필 또는 주가 데이터 확인 필요)");
            return res.json({ success: false, message: "지수 산출 실패: 유효한 종목 데이터가 없습니다." });
        }

        console.log(`✅ 데이터 로딩 성공: ${stocks.length}/${tickers.length}개 종목 합산 시작`);

        // 3. 지수 합산 (Aggregation)
        const dailyThemeStats = {}; 

        stocks.forEach(stock => {
            Object.keys(stock.history).forEach(date => {
                const price = stock.history[date];
                const marketCap = price * stock.shares; // 시가총액 환산

                if (!dailyThemeStats[date]) dailyThemeStats[date] = 0;
                dailyThemeStats[date] += marketCap;
            });
        });

        // 4. 저장 (연도별 분할)
        const statsByYear = {};
        const dates = Object.keys(dailyThemeStats).sort();
        
        dates.forEach(date => {
            const year = date.split('-')[0];
            const marketCapSum = dailyThemeStats[date];

            if (!statsByYear[year]) statsByYear[year] = {};
            statsByYear[year][date] = { mc: Math.round(marketCapSum) }; 
        });

        const batch = db.batch();
        const statsRef = db.collection('market_themes_stats');

        for (const year of Object.keys(statsByYear)) {
            const docId = `${themeId}_${year}`;
            batch.set(statsRef.doc(docId), {
                themeId: themeId,
                year: year,
                updatedAt: new Date().toISOString(),
                daily_data: statsByYear[year]
            }, { merge: true });
        }

        await batch.commit();

        console.log(`🎉 [완료] 총 ${dates.length}일치 지수 데이터 저장됨.`);
        
        res.json({ 
            success: true, 
            days_calculated: dates.length,
            valid_tickers: stocks.length,
            message: `지수 생성 완료 (데이터: ${dates.length}일, 종목: ${stocks.length}개)`
        });

    } catch (error) {
        console.error("지수 산출 오류:", error);
        res.status(500).json({ error: error.message });
    }
});

// [자비스 빌더 API] - 파일 생성 및 수정 전용
router.post('/build-file', async (req, res) => {
    try {
        const { requirement, filePath } = req.body;
        const absolutePath = path.resolve(__dirname, '../../', filePath); // 경로 계산 주의
        
        console.log(`🤖 [자비스 빌더] 요청: ${filePath}`);

        let existingCode = "";
        let mode = "NEW_CREATION";

        if (fs.existsSync(absolutePath)) {
            existingCode = fs.readFileSync(absolutePath, 'utf8');
            mode = "MODIFICATION";
            console.log(`📖 기존 파일 수정 모드 (${existingCode.length} bytes)`);
        }

        const myCodingStyle = `
            [프로젝트 원칙]
            1. 모든 버튼은 클래스 '.btn-base', '.btn-primary' 등을 사용한다.
            2. 입력 필드나 선택 박스의 폰트 크기는 반드시 '13px'로 설정한다.
            3. 테이블이 있는 경우 'table-focus-container'를 사용하고 왼쪽 정렬한다.
            4. DB 읽기/쓰기는 반드시 백앤드 API('/api/firestore/...')를 호출한다.
            5. 전체적인 레이아웃은 'div' 기반이며 칼럼 순서를 유지한다.
            6. 마크다운(\`\`\`) 없이 오직 소스코드만 반환한다.
        `;

        const systemInstruction = `너는 '퀀트 그래비티'의 수석 개발자다. ${myCodingStyle}`;

        let fullPrompt = (mode === "MODIFICATION") 
            ? `${systemInstruction}\n[기존 소스]\n${existingCode}\n[수정 요청]\n${requirement}`
            : `${systemInstruction}\n[새 파일 생성 요청]\n${requirement}`;
        
        // 유틸리티 함수 호출
        let generatedCode = await askJarvis(fullPrompt);

        // 마크다운 제거 로직
        generatedCode = generatedCode.replace(/^```\w*\n?/, '').replace(/```$/, '').trim();

        // 디렉토리 자동 생성 및 저장
        const dir = path.dirname(absolutePath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(absolutePath, generatedCode, 'utf8');

        res.json({ success: true, path: filePath, mode: mode });

    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// [추가됨] 사용 가능한 모델 리스트 확인용 진단 API
// 브라우저에서 http://localhost:3000/api/jarvis/check-models 로 접속해서 확인
router.get('/check-models', async (req, res) => {
    try {
        const API_KEY = process.env.GEMINI_API_KEY;
        // 모델 목록을 가져오는 API 호출
        const url = `https://generativelanguage.googleapis.com/v1beta/models?key=${API_KEY}`;
        const response = await axios.get(url);
        
        // 보기 편하게 이름만 추출해서 보여줌
        const modelNames = response.data.models.map(m => m.name);
        res.json({ 
            message: "현재 사용 가능한 모델 리스트입니다.", 
            available_models: modelNames 
        });
    } catch (error) {
        res.status(500).json({ error: error.response?.data || error.message });
    }
});

module.exports = router;