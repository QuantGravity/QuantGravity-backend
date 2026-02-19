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
const { askJarvis } = require('../utils/jarvisClient'); 
const admin = require('firebase-admin'); 
const db = admin.firestore();            
const { verifyToken } = require('../utils/authHelper');
const axios = require('axios');

// ---------------------------------------------------------------------------
// [Util] AI 응답에서 JSON만 깔끔하게 추출하는 함수 (만능 버전)
// ---------------------------------------------------------------------------
function cleanAndParseJSON(text) {
    try {
        // 1. 마크다운 코드 블록 제거 (```json ... ```)
        // gi 플래그를 써서 대소문자 구분 없이 제거
        let cleanText = text.replace(/```json/gi, '').replace(/```/g, '').trim();
        
        // 2. JSON의 시작( { 또는 [ )과 끝( } 또는 ] )을 찾아서 추출
        const firstCurly = cleanText.indexOf('{');
        const firstSquare = cleanText.indexOf('[');
        
        let start = -1;
        // 둘 중 먼저 나오는 것을 시작점으로 잡음
        if (firstCurly !== -1 && firstSquare !== -1) {
            start = Math.min(firstCurly, firstSquare);
        } else if (firstCurly !== -1) {
            start = firstCurly;
        } else if (firstSquare !== -1) {
            start = firstSquare;
        }

        const lastCurly = cleanText.lastIndexOf('}');
        const lastSquare = cleanText.lastIndexOf(']');
        
        // 둘 중 나중에 나오는 것을 끝점으로 잡음
        let end = Math.max(lastCurly, lastSquare);

        if (start !== -1 && end !== -1) {
            cleanText = cleanText.substring(start, end + 1);
        }

        return JSON.parse(cleanText);
    } catch (e) {
        console.error("JSON 파싱 실패 Raw:", text);
        throw new Error("AI 응답을 JSON으로 변환하는데 실패했습니다.");
    }
}

// ===========================================================================
// [API] ETF 속성 일괄 분석 (로컬 키워드 매칭 + AI 정밀 분석 통합)
// ===========================================================================
router.post('/analyze-etf-bulk', verifyToken, async (req, res) => {
    const { items } = req.body; 
    if (!items || !Array.isArray(items) || items.length === 0) {
        return res.status(400).json({ error: "분석할 데이터(items)가 필요합니다." });
    }

    console.log(`🤖 [자비스] ETF 지능형 복합 분석 시작: ${items.length}건`);

    // 1. [로컬 로직] 지수 판별 키워드 및 매칭 맵핑
    const indexMapping = [
        { kw: ['S&P 500', 'SNP 500', 'SP500'], ticker: '^SPX' },
        { kw: ['NASDAQ 100', '나스닥 100', 'NDX100'], ticker: '^NDX' },
        { kw: ['KOSPI 200', '코스피 200', 'K200'], ticker: '^KS200' }, // 키워드 추가
        { kw: ['KOSDAQ 150', '코스닥 150'], ticker: '^KQ150' },
        { kw: ['DOW JONES', '다우존스'], ticker: '^DJI' },
        { kw: ['SOX', '필라델피아 반도체'], ticker: '^SOX' },
        { kw: ['NVDA', '엔비디아'], ticker: 'NVDA' },
        { kw: ['TSLA', '테슬라'], ticker: 'TSLA' }
    ];

    // 2. AI에게 전달하기 전, 로컬에서 먼저 기초자산 추측
    const enrichedItems = items.map(item => {
        const nameUpper = (item.ticker_name_kr || item.description || "").toUpperCase();
        let guessedTicker = "";

        for (const mapping of indexMapping) {
            if (mapping.kw.some(k => nameUpper.includes(k.toUpperCase()))) {
                guessedTicker = mapping.ticker;
                break;
            }
        }

        return {
            ...item,
            guessed_underlying: guessedTicker // AI에게 참고용으로 전달
        };
    });

    try {
        // 3. AI 프롬프트 구성 (로컬에서 찾은 guessed_underlying 활용)
        const prompt = `
            Analyze the following ETF list. We provided 'guessed_underlying' based on keywords.
            Your job is to verify it and complete the missing data.

            List: ${JSON.stringify(enrichedItems)}

            [Extraction Rules]
            1. **underlying_ticker (CRITICAL)**: 
               - If 'guessed_underlying' is provided and correct, USE IT.
               - Otherwise, find the correct Yahoo Finance style ticker.
               - Gold('GC=F'), Silver('SI=F'), Bitcoin('BTC-USD'), Treasury('TLT', 'IEF').
            2. **leverage_factor**: 
               - Inverse/Short/Bear: Negative (-1, -2, -3)
               - Bull/Long/2x/3x: Positive (1, 2, 3)
            3. **ticker_name_kr**: Format as "[Asset] [Leverage] [Direction]" (e.g., "나스닥 100 3배 레버리지")
            4. **asset_class**: [Equity, Commodity, Crypto, Fixed_Income, Volatility]

            [Return Format]
            - Return ONLY a JSON Object: { "TICKER": { "underlying_ticker": "...", "leverage_factor": 1, ... } }
        `;

        const responseText = await askJarvis(prompt);
        // AI 결과를 aiResults 변수에 저장
        const aiResults = cleanAndParseJSON(responseText);

        // [추가] 내부 표준화를 위한 티커 변환 맵핑
        const tickerNormalizationMap = {
            "^KOSPI200": "^KS200",
            "^KOSPI": "^KS11",
            "^KOSDAQ": "^KQ11",
            // 향후 다른 혼동하기 쉬운 티커들도 여기에 추가 가능
        };

        // 4. [검증 로직] 순수하게 데이터만 정제 (정의되지 않은 aiData 대신 aiResults 사용)
        const sanitizedResults = {};
        Object.keys(aiResults).forEach(ticker => {
            const info = aiResults[ticker];
            // 1. AI 응답 티커 추출 및 대문자 변환
            let rawUnderlying = (info.underlying_ticker || "SPY").toUpperCase();
            
            // 2. [핵심] 표준화 맵핑 적용 (없으면 그대로 유지)
            const normalizedUnderlying = tickerNormalizationMap[rawUnderlying] || rawUnderlying;

            sanitizedResults[ticker] = {
                underlying_ticker: normalizedUnderlying,
                leverage_factor: parseFloat(info.leverage_factor) || 1,
                ticker_name_kr: info.ticker_name_kr || `${ticker} ETF`,
                asset_class: info.asset_class || "Equity",
                updated_at: new Date().toISOString()
            };
        });

        console.log(`✅ [자비스] 지능형 분석 완료 (${Object.keys(sanitizedResults).length}건)`);
        
        // 정제된 데이터를 프론트엔드로 전송
        res.json({ success: true, data: sanitizedResults });

    } catch (e) {
        console.error("ETF Intelligent Analysis Error:", e);
        res.status(500).json({ error: e.message });
    }
});

// ===========================================================================
// [API] 신규 테마 리서치
// ===========================================================================
router.post('/research-new-themes', async (req, res) => {
    console.log("🤖 [자비스] 신규 테마 리서치 시작...");
    req.setTimeout(90000); 

    try {
        const snapshot = await db.collection('market_themes').get();
        let existingThemes = "None";
        if (!snapshot.empty) {
            existingThemes = snapshot.docs.map(doc => doc.data().name_en).join(', ');
        }

        const prompt = `
            너는 월스트리트의 수석 퀀트 분석가야.
            현재 미국 주식 시장을 주도하고 있는 '투자 테마' 10가지를 발굴해줘.
            [제외 테마]: ${existingThemes}
            
            각 테마별로 상위 15개 주도주와 선정 사유(reason)를 한글로 포함해줘.
            JSON 포맷 예시:
            [ { "id": "ai", "name_en": "AI", "name_ko": "인공지능", "tickers": [...] } ]
        `;

        const responseText = await askJarvis(prompt);
        const themes = cleanAndParseJSON(responseText);

        const batch = db.batch();
        themes.forEach(theme => {
            const docRef = db.collection('market_themes').doc(theme.id);
            batch.set(docRef, {
                ...theme,
                ticker_count: theme.tickers ? theme.tickers.length : 0,
                updated_at: new Date().toISOString()
            }, { merge: true });
        });

        await batch.commit();
        console.log(`✅ [자비스] ${themes.length}개 테마 저장 완료`);
        res.json({ success: true, count: themes.length });

    } catch (error) {
        console.error("리서치 에러:", error);
        res.status(500).json({ error: error.message });
    }
});

// ===========================================================================
// [API] 기존 테마 업데이트
// ===========================================================================
router.post('/update-theme-tickers', async (req, res) => {
    const { themeId } = req.body;
    try {
        const themeDoc = await db.collection('market_themes').doc(themeId).get();
        if (!themeDoc.exists) throw new Error("존재하지 않는 테마입니다.");
        
        const themeName = themeDoc.data().name_en;
        const prompt = `투자 테마 '${themeName}'의 상위 15개 핵심 종목과 선정 사유(reason)를 JSON으로 분석해줘.`;

        const responseText = await askJarvis(prompt);
        const tickers = cleanAndParseJSON(responseText);

        await db.collection('market_themes').doc(themeId).update({
            tickers: tickers,
            ticker_count: tickers.length,
            updated_at: new Date().toISOString()
        });

        res.json({ success: true, count: tickers.length });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ===========================================================================
// [API] 테마 지수 산출 (DB Only)
// ===========================================================================
router.post('/calculate-theme-index', async (req, res) => {
    const { themeId } = req.body;
    req.setTimeout(60000); 

    try {
        const themeDoc = await db.collection('market_themes').doc(themeId).get();
        if (!themeDoc.exists) throw new Error("테마를 찾을 수 없습니다.");
        
        const tickers = themeDoc.data().tickers || [];
        if (tickers.length === 0) throw new Error("구성 종목이 없습니다.");

        console.log(`📊 [자비스] 지수 산출 시작 (${tickers.length}종목)`);

        const stockDataPromises = tickers.map(async (t) => {
            const stockSnap = await db.collection('stocks').doc(t.symbol).get();
            if (!stockSnap.exists) return null;

            const profile = stockSnap.data();
            const price = profile.snapshot?.price;
            const mktCap = profile.snapshot?.mktCap;
            
            if (!price || !mktCap) return null;
            const shares = mktCap / price;

            let historyMap = {};
            for (const year of ['2023', '2024', '2025', '2026']) {
                const yearDoc = await stockSnap.ref.collection('annual_data').doc(year).get();
                if (yearDoc.exists) {
                    yearDoc.data().data.forEach(d => {
                        if(d.close) historyMap[d.date] = d.close;
                    });
                }
            }
            if (Object.keys(historyMap).length === 0) return null;

            return { symbol: t.symbol, shares, history: historyMap };
        });

        const stocks = (await Promise.all(stockDataPromises)).filter(s => s !== null);
        
        if (stocks.length === 0) return res.json({ success: false, message: "유효 데이터 없음" });

        const dailyStats = {};
        stocks.forEach(s => {
            Object.keys(s.history).forEach(date => {
                if (!dailyStats[date]) dailyStats[date] = 0;
                dailyStats[date] += s.history[date] * s.shares;
            });
        });

        const batch = db.batch();
        const dates = Object.keys(dailyStats).sort();
        const statsByYear = {};

        dates.forEach(d => {
            const y = d.split('-')[0];
            if (!statsByYear[y]) statsByYear[y] = {};
            statsByYear[y][d] = { mc: Math.round(dailyStats[d]) };
        });

        for (const y of Object.keys(statsByYear)) {
            batch.set(db.collection('market_themes_stats').doc(`${themeId}_${y}`), {
                themeId, year: y, daily_data: statsByYear[y], updatedAt: new Date().toISOString()
            }, { merge: true });
        }

        await batch.commit();
        res.json({ success: true, days: dates.length });

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// [자비스 빌더 API] - 파일 생성 및 수정 전용
router.post('/build-file', async (req, res) => {
    try {
        const { requirement, filePath } = req.body;
        const absolutePath = path.resolve(__dirname, '../../', filePath);
        
        console.log(`🤖 [자비스 빌더] 요청: ${filePath}`);

        let existingCode = "";
        let mode = "NEW_CREATION";

        if (fs.existsSync(absolutePath)) {
            existingCode = fs.readFileSync(absolutePath, 'utf8');
            mode = "MODIFICATION";
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
        
        let generatedCode = await askJarvis(fullPrompt);
        generatedCode = generatedCode.replace(/^```\w*\n?/, '').replace(/```$/, '').trim();

        const dir = path.dirname(absolutePath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(absolutePath, generatedCode, 'utf8');

        res.json({ success: true, path: filePath, mode: mode });

    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// [추가됨] 사용 가능한 모델 리스트 확인용 진단 API
router.get('/check-models', async (req, res) => {
    try {
        const API_KEY = process.env.GEMINI_API_KEY;
        const url = `https://generativelanguage.googleapis.com/v1beta/models?key=${API_KEY}`;
        const response = await axios.get(url);
        const modelNames = response.data.models.map(m => m.name);
        res.json({ available_models: modelNames });
    } catch (error) {
        res.status(500).json({ error: error.response?.data || error.message });
    }
});

module.exports = router;