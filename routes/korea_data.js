// ===========================================================================
// [파일명] : routes/korea_data.js
// [대상]   : 1. 금융위원회_주식발행정보 (API)
//           2. KOSPI 200 수동 업로드 (File Processing)
// ===========================================================================

const express = require('express');
const router = express.Router();
const axios = require('axios');
const admin = require('firebase-admin');
const https = require('https'); // ⚡ [추가] SSL 옵션 제어용
const { verifyToken } = require('../utils/authHelper');

// 📦 엑셀 및 파일 업로드 처리 라이브러리
const multer = require('multer');
const xlsx = require('xlsx');
const upload = multer({ storage: multer.memoryStorage() });

// 🔑 공공데이터포털 서비스 키
const SERVICE_KEY = process.env.KOREA_DATA_API_KEY;

// [Helper] 주식발행정보 API 호출 (페이징 처리로 전체 데이터 수집)
async function fetchKoreaIssuanceInfo() {
    const baseUrl = 'https://apis.data.go.kr/1160100/service/GetStocIssuInfoService_V2/getItemBasiInfo_V2';
    const rowsPerPage = 5000; // 안전하게 5000개씩 끊어서 호출
    let currentPage = 1;
    let allItems = [];
    let totalCount = 0;

    const agent = new https.Agent({ rejectUnauthorized: false });

    try {
        do {
            const queryParams = [
                `serviceKey=${SERVICE_KEY}`,
                `numOfRows=${rowsPerPage}`,
                `pageNo=${currentPage}`,
                `resultType=json`
            ].join('&');

            console.log(`📡 [API 요청] ${currentPage}페이지 수집 중...`);
            const response = await axios.get(`${baseUrl}?${queryParams}`, { timeout: 60000, httpsAgent: agent });
            const data = response.data;

            if (data?.response?.body) {
                totalCount = parseInt(data.response.body.totalCount || 0);
                const items = data.response.body.items?.item;
                
                if (items) {
                    const pageItems = Array.isArray(items) ? items : [items];
                    allItems = allItems.concat(pageItems);
                    console.log(`✅ [진행] ${allItems.length} / ${totalCount} 수집됨`);
                } else {
                    break; // 더 이상 아이템이 없으면 중단
                }
            } else {
                break;
            }

            currentPage++;
            // 무한 루프 방지 (공공데이터 포털 데이터가 보통 10만 건 안쪽이므로 20페이지면 충분)
            if (currentPage > 30) break; 

        } while (allItems.length < totalCount);

        console.log(`✨ [수집 완료] 총 ${allItems.length}개 데이터를 메모리에 로드함`);
        return allItems;

    } catch (error) {
        console.error(`⚠️ [API 에러] 전체 수집 중 실패: ${error.message}`);
        return allItems; // 실패 전까지 수집된 데이터라도 반환
    }
}

// [추가] 실제 상장된 종목의 시세 정보를 가져와서 "상장 여부"를 확인하는 함수
async function fetchActiveStockCodes() {
    // 1. 가이드에 명시된 기본 URL [cite: 18, 24]
    const baseUrl = 'https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo';
    const agent = new https.Agent({ rejectUnauthorized: false });
    
    try {
        console.log(`📡 [상장 점검] 현재 거래 중인 종목 리스트 조회 중...`);
        
        // 2. ⚠️ 중요: axios params 대신 직접 쿼리 스트링 구성 (인코딩 중복 방지)
        // SERVICE_KEY는 반드시 'Decoding' 키를 사용해야 하며, 수동으로 encodeURIComponent 처리
        const queryParams = [
            `serviceKey=${encodeURIComponent(SERVICE_KEY)}`,
            `numOfRows=4000`,
            `pageNo=1`,
            `resultType=json`
        ].join('&');

        const fullUrl = `${baseUrl}?${queryParams}`;
        console.log(`🔗 [요청 URL 확인]: ${fullUrl.split('serviceKey=')[0]}serviceKey=***`);

        const response = await axios.get(fullUrl, { 
            timeout: 60000, 
            httpsAgent: agent 
        });

        // 3. 응답 구조 체크 [cite: 29]
        const data = response.data;
        const header = data?.response?.header;

        if (header?.resultCode !== '00') {
            console.error(`❌ [API 결과 에러] 코드: ${header?.resultCode}, 메시지: ${header?.resultMsg}`);
            return new Set();
        }

        const items = data?.response?.body?.items?.item;
        if (!items) {
            console.log("⚠️ [상장 점검] 데이터가 없습니다.");
            return new Set();
        }

        const activeCodes = new Set();
        const itemList = Array.isArray(items) ? items : [items];
        
        itemList.forEach(item => {
            if (item.srtnCd) {
                // 가이드 문서상 srtnCd는 단축코드(6자리) [cite: 29]
                activeCodes.add(item.srtnCd.trim());
            }
        });

        console.log(`✅ [상장 점검] 현재 거래 중인 ${activeCodes.size}개 종목 확인 완료.`);
        return activeCodes;

    } catch (error) {
        if (error.response) {
            // 네 로그에 찍힌 403 Forbidden 상세 확인용
            console.error(`❌ [서버 응답 에러] 상태코드: ${error.response.status}`);
            console.error(`상세 내용:`, error.response.data);
        } else {
            console.error(`⚠️ [상장 점검 에러] ${error.message}`);
        }
        return new Set();
    }
}

// [기능 1] 한국 종목 마스터 보정 (필터링 강화 & 전체 필드 저장)
// [기능 1] 한국 종목 마스터 보정 (시세 정보 대조 로직 추가)
router.post('/sync-issuance-info', verifyToken, async (req, res) => {
    const { mode = 'FULL', symbol } = req.body; 
    const db = admin.firestore();

    try {
        // 1. 실제 상장되어 거래 중인 코드 리스트 확보
        const activeStockCodes = await fetchActiveStockCodes();
        
        if (activeStockCodes.size === 0 && mode === 'FULL') {
            return res.status(500).json({ success: false, message: "상장 종목 시세 정보 조회 실패로 중단합니다." });
        }

        // 2. 발행 정보 가져오기 (기존 함수)
        const items = await fetchKoreaIssuanceInfo();
        if (!items || items.length === 0) {
            return res.status(500).json({ success: false, message: "주식발행정보 수신 실패" });
        }

        const snapshot = await db.collection('stocks').select().get();
        const existingStockSet = new Set(snapshot.docs.map(doc => doc.id));

        const infoMap = {};
        const newDiscoveries = [];
        let filteredCount = 0; 

        items.forEach((item) => {
            const rawCode = item.itmsShrtnCd; 
            const name = item.stckIssuCmpyNm || "";

            // 🛑 [기본 필터링]
            if (!rawCode || rawCode.length !== 6 || !rawCode.endsWith('0')) {
                filteredCount++; return;
            }

            // 🛑 [추가 필터링] 시세 정보 API(activeStockCodes)에 없는 코드는 비상장으로 간주
            if (!activeStockCodes.has(rawCode)) {
                filteredCount++; return;
            }

            // 🛑 [기존 필터링 유지]
            if (rawCode.startsWith('5') || rawCode.startsWith('7') || rawCode.startsWith('9')) {
                filteredCount++; return;
            }
            if (!item.lstgDt || item.lstgDt.trim() === '') {
                filteredCount++; return;
            }
            if (item.lstgAbolDt && item.lstgAbolDt.trim() !== '') {
                filteredCount++; return;
            }
            if (/(채권|선물|옵션|ELW|ETN|신주인수권|스팩)/.test(name)) {
                filteredCount++; return;
            }

            // --- 통과된 실제 상장 보통주 ---
            const code = rawCode.trim();
            const fullSymbol = `${code}.KS`; 

            infoMap[code] = {
                name_ko: name, 
                ipoDate: item.lstgDt,         
                delDate: item.lstgAbolDt      
            };

            if (!existingStockSet.has(fullSymbol) && !existingStockSet.has(`${code}.KQ`)) {
                if (!newDiscoveries.some(d => d.symbol === fullSymbol)) {
                    newDiscoveries.push({
                        symbol: fullSymbol,
                        name: name,
                        foundAt: new Date().toISOString(),
                        ...item 
                    });
                }
            }
        });

        console.log(`🧹 [필터링] 총 ${filteredCount}개의 (우선주/외국/비상장/상폐/파생) 종목을 제외했습니다.`);

        // 4. 신규 종목 별도 저장 (청크 분할 + 기존 삭제)
        if (newDiscoveries.length > 0) {
            console.log(`✨ [신규 발견] 순수 보통주 ${newDiscoveries.length}개 발견! 저장 시작...`);
            
            const today = new Date().toISOString().split('T')[0];
            const discoveryRef = db.collection('discovered_stocks').doc(today);
            
            // 기존 데이터 삭제 (Clean Slate)
            const oldChunks = await discoveryRef.collection('chunks').get();
            if (!oldChunks.empty) {
                const delBatch = db.batch();
                oldChunks.docs.forEach(doc => delBatch.delete(doc.ref));
                await delBatch.commit();
            }

            const CHUNK_SIZE = 600;
            const totalChunks = Math.ceil(newDiscoveries.length / CHUNK_SIZE);

            // 메타 데이터 저장
            await discoveryRef.set({
                date: today,
                count: newDiscoveries.length,
                filteredCount: filteredCount,
                description: "Filtered Common Stocks (Ends with '0')",
                isChunked: true,
                chunkCount: totalChunks,
                updatedAt: new Date().toISOString()
            }, { merge: true });

            // 청크 분할 저장
            let batch = db.batch();
            let opCount = 0;

            for (let i = 0; i < totalChunks; i++) {
                const chunkList = newDiscoveries.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
                const chunkRef = discoveryRef.collection('chunks').doc(`batch_${i}`);
                batch.set(chunkRef, { list: chunkList });
                opCount++;

                if (opCount >= 400) {
                    await batch.commit();
                    batch = db.batch();
                    opCount = 0;
                }
            }
            if (opCount > 0) await batch.commit();
            
            console.log(`✅ [저장 완료] 정제된 신규 종목 ${newDiscoveries.length}개 저장 완료.`);

        } else {
            console.log("✨ [신규 발견] 새로운 종목이 없습니다.");
        }

        // [A] 단건 테스트 로직 (기존 유지)
        if (mode === 'SINGLE') {
            const targetCode = searchCode;
            const info = infoMap[targetCode];
            
            if (!info) return res.json({ success: false, message: `[${targetCode}]는 필터링 대상(우선주/상폐 등)이거나 데이터가 없습니다.` });

            const fullSymbol = symbol.includes('.') ? symbol : `${symbol}.KS`;
            await db.collection('stocks').doc(fullSymbol).set({
                name_ko: info.name_ko,
                ipoDate: info.ipoDate,
                ...(info.delDate && { delistedDate: info.delDate })
            }, { merge: true });

            return res.json({ success: true, mode: 'SINGLE', data: { symbol: fullSymbol, ...info }, message: "업데이트 성공" });
        }

        // [B] 전체 동기화 로직 (기존 유지)
        let updateCount = 0;
        const targetCollections = ['KR_KOSPI', 'KR_KOSDAQ'];
        let batch = db.batch();
        let opCount = 0;

        for (const colId of targetCollections) {
            const metaDoc = await db.collection('meta_tickers').doc(colId).get();
            if(!metaDoc.exists) continue;
            const chunks = await db.collection('meta_tickers').doc(colId).collection('chunks').get();
            for (const chunk of chunks.docs) {
                const list = chunk.data().list || [];
                for (const stock of list) {
                    const rawCode = stock.symbol.split('.')[0];
                    const info = infoMap[rawCode];
                    if (info) {
                        const stockRef = db.collection('stocks').doc(stock.symbol);
                        batch.set(stockRef, { 
                            name_ko: info.name_ko, 
                            ipoDate: info.ipoDate,
                            ...(info.delDate && { delistedDate: info.delDate })
                        }, { merge: true });
                        opCount++;
                        updateCount++;
                    }
                }
                if (opCount >= 400) { await batch.commit(); batch = db.batch(); opCount = 0; }
            }
        }
        if (opCount > 0) await batch.commit();

        res.json({ 
            success: true, 
            count: updateCount, 
            newlyDiscovered: newDiscoveries.length,
            filteredCount: filteredCount,
            message: `업데이트 완료. (신규: ${newDiscoveries.length}, 필터링 제외: ${filteredCount}건)` 
        });

    } catch (error) {
        console.error("Sync Error:", error);
        res.status(500).json({ success: false, error: error.message });
    }
});

module.exports = router;
// 1. 라우터 주소 변경 (upload-kospi200-excel -> upload-kospi200-json)
// 2. upload.single 미들웨어 제거 (파일 안 받으니까)
router.post('/upload-kospi200-json', verifyToken, async (req, res) => {
    console.log("🚀 [KR Data] KOSPI 200 JSON 데이터 수신...");
    
    const rawData = req.body.data; 

    if (!rawData || !Array.isArray(rawData) || rawData.length === 0) {
        return res.status(400).json({ success: false, message: "전송된 데이터가 없습니다." });
    }

    const db = admin.firestore();

    try {
        const finalList = [];
        const skippedItems = []; // ⚡ [수정] 제외된 항목을 담을 배열

        for (const row of rawData) {
            let rawCode = (row['종목코드'] || row['Code'] || Object.values(row)[0]).toString().trim();
            const stockName = row['종목명'] || 'Unknown';

            // 1. 숫자만 남기고 6자리로 맞춤 (예: "066970")
            rawCode = rawCode.replace(/[^0-9]/g, '').padStart(6, '0');
            
            // 2. [수정] KS(코스피)로 먼저 찾고, 없으면 KQ(코스닥)으로 재시도
            let symbol = `${rawCode}.KS`;
            let docSnap = await db.collection('stocks').doc(symbol).get();

            if (!docSnap.exists) {
                // KS가 없으면 KQ로 한 번 더 조회 (이전 상장 종목 등 대비)
                const symbolKQ = `${rawCode}.KQ`;
                const docSnapKQ = await db.collection('stocks').doc(symbolKQ).get();
                
                if (docSnapKQ.exists) {
                    symbol = symbolKQ; // KQ로 심볼 변경
                    docSnap = docSnapKQ; // 데이터 교체
                }
            }

            // 3. 둘 다 없으면 진짜 없는 거임
            if (!docSnap.exists) { 
                skippedItems.push(`${rawCode} (${stockName})`);
                continue; 
            }

            const stockData = docSnap.data();
            finalList.push({
                s: symbol, // 찾은 심볼(KS 혹은 KQ) 그대로 저장
                n: stockData.name_ko || stockName || stockData.name_en, 
                sec: stockData.sector || 'Unknown'
            });
        }
        
        if (finalList.length < 50) {
            return res.status(400).json({ success: false, message: `유효한 종목이 너무 적습니다 (${finalList.length}개).` });
        }

        // --- (저장 로직은 기존과 동일하므로 생략, 아래 res.json 부분만 변경됨) ---
        const docId = 'KR_KOSPI200';
        const CHUNK_SIZE = 600;
        const totalChunks = Math.ceil(finalList.length / CHUNK_SIZE);
        const mainRef = db.collection('meta_tickers').doc(docId);
        
        // ... (기존 저장 로직 유지) ...
        const oldChunks = await mainRef.collection('chunks').get();
        const delBatch = db.batch();
        oldChunks.docs.forEach(doc => delBatch.delete(doc.ref));
        await delBatch.commit();

        await mainRef.set({
            country: 'KR', exchange: 'INDEX', description: 'KOSPI 200 (Uploaded)',
            count: finalList.length, isChunked: true, chunkCount: totalChunks, updatedAt: new Date().toISOString()
        });

        let batch = db.batch();
        for (let i = 0; i < totalChunks; i++) {
            batch.set(mainRef.collection('chunks').doc(`batch_${i}`), { 
                chunkIndex: i, 
                list: finalList.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE) 
            });
        }
        await batch.commit();

        // ⚡ [수정] 응답에 skippedItems 배열 포함
        res.json({ 
            success: true, 
            count: finalList.length, 
            skippedCount: skippedItems.length, 
            skippedItems: skippedItems, 
            message: "업데이트 완료" 
        });

    } catch (e) {
        console.error("JSON Process Error:", e);
        res.status(500).json({ success: false, error: e.message });
    }
});

module.exports = router;