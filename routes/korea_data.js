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
const { verifyToken, verifyBatchOrAdmin } = require('../utils/authHelper');

// 📦 엑셀 및 파일 업로드 처리 라이브러리
const multer = require('multer');
const xlsx = require('xlsx');
const upload = multer({ storage: multer.memoryStorage() });

// 🔑 공공데이터포털 서비스 키
const SERVICE_KEY = process.env.KOREA_DATA_API_KEY;

// [Helper] 주식발행정보 API 호출 (페이징 처리로 전체 데이터 수집)
// [Helper] 주식발행정보 API 호출 (targetSymbol이 있으면 단건만 조회)
async function fetchKoreaIssuanceInfo(targetSymbol = null) {
    const baseUrl = 'https://apis.data.go.kr/1160100/service/GetStocIssuInfoService_V2/getItemBasiInfo_V2';
    // 단건 조회면 10개만 요청해도 충분, 전체면 5000개
    const rowsPerPage = targetSymbol ? 10 : 5000; 
    let currentPage = 1;
    let allItems = [];
    let totalCount = 0;

    const agent = new https.Agent({ rejectUnauthorized: false });

    try {
        do {
            let queryParams = [
                `serviceKey=${SERVICE_KEY}`,
                `numOfRows=${rowsPerPage}`,
                `pageNo=${currentPage}`,
                `resultType=json`
            ];

            // ⚡ [핵심 수정] 단건 조회를 위한 파라미터 추가
            if (targetSymbol) {
                // 입력받은 코드에서 숫자 6자리만 추출 (예: "000080.KS" -> "000080")
                const shortCode = targetSymbol.replace(/[^0-9]/g, '');
                queryParams.push(`itmsShrtnCd=${shortCode}`); // 공공데이터포털 단축코드 검색 파라미터
            }

            const queryString = queryParams.join('&');
            
            if (!targetSymbol) {
                console.log(`📡 [API 요청] ${currentPage}페이지 수집 중...`);
            } else {
                console.log(`📡 [API 요청] 단건 조회 요청: ${targetSymbol}`);
            }

            const response = await axios.get(`${baseUrl}?${queryString}`, { timeout: 60000, httpsAgent: agent });
            const data = response.data;

            if (data?.response?.body) {
                totalCount = parseInt(data.response.body.totalCount || 0);
                const items = data.response.body.items?.item;
                
                if (items) {
                    const pageItems = Array.isArray(items) ? items : [items];
                    allItems = allItems.concat(pageItems);
                    
                    // 단건 조회면 1페이지에서 끝냄
                    if (targetSymbol) break; 

                    console.log(`✅ [진행] ${allItems.length} / ${totalCount} 수집됨`);
                } else {
                    break; 
                }
            } else {
                break;
            }

            currentPage++;
            if (currentPage > 30) break; 

        } while (allItems.length < totalCount);

        console.log(`✨ [수집 완료] 총 ${allItems.length}개 데이터를 메모리에 로드함`);
        return allItems;

    } catch (error) {
        console.error(`⚠️ [API 에러] 수집 중 실패: ${error.message}`);
        return allItems; 
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

// [기능 1] 한국 종목 마스터 보정 (시세 정보 대조 및 기존 데이터 비교 업데이트 로직 추가)
// [기능 1] 한국 종목 마스터 보정 (시세 정보 대조 및 기존 데이터 비교 업데이트 로직 추가)
router.post('/sync-issuance-info', verifyToken, async (req, res) => {
    const { mode = 'FULL', symbol } = req.body; 
    const db = admin.firestore();

    try {
        // 1. 실제 상장되어 거래 중인 코드 리스트 확보
        const activeStockCodes = await fetchActiveStockCodes();
        
        if (activeStockCodes.size === 0) {
            if (mode === 'FULL') {
                return res.status(500).json({ success: false, message: "상장 종목 시세 정보 조회 실패로 중단합니다." });
            } else {
                console.warn("⚠️ [경고] 시세 정보를 가져오지 못했지만, 단건 조회이므로 진행합니다.");
            }
        }

        const targetSymbolForApi = (mode === 'SINGLE' && symbol) ? symbol : null;
        const items = await fetchKoreaIssuanceInfo(targetSymbolForApi);

        if (!items || items.length === 0) {
            return res.status(500).json({ success: false, message: "주식발행정보 수신 실패 (데이터 없음)" });
        }

        // ⚡ [핵심 수정 1] 비교를 위해 기존 stocks 데이터의 3개 필드(한글명, 상장일, 상폐일)를 함께 가져옴
        const snapshot = await db.collection('stocks').select('name_ko', 'ipoDate', 'delistedDate').get();
        const existingStocks = {};
        snapshot.docs.forEach(doc => {
            existingStocks[doc.id] = doc.data();
        });
        const existingStockSet = new Set(Object.keys(existingStocks));

        const infoMap = {};
        const newDiscoveries = [];
        let filteredCount = 0; 

        items.forEach((item) => {
            const rawCode = item.itmsShrtnCd; 
            const name = item.stckIssuCmpyNm || "";
            
            // 🌟 [핵심] 현재 거래 중인 종목인지 확인!
            const isActive = activeStockCodes.has(rawCode);

            const logReason = (reason) => {
                if (mode === 'SINGLE') console.log(`🛑 [필터링] ${rawCode} (${name}) 제외됨: ${reason}`);
            };

            // 🛑 [기본 필터링 1] 자리수 체크 (0으로 끝나는 조건은 아래로 분리)
            if (!rawCode || rawCode.length !== 6) {
                logReason("코드 형식(6자리) 불일치");
                filteredCount++; return;
            }

            // 🌟 [요청 사항 반영] 변수들을 위로 끌어올려서 먼저 판별
            const code = rawCode.trim();
            const fullSymbol = `${code}.KS`; 
            
            const isExisting = existingStockSet.has(fullSymbol) || existingStockSet.has(`${code}.KQ`);
            const isCommonStock = code.endsWith('0');

            // 🌟 [요청 사항 반영 1] 끝자리가 0이 아닌데 기존 STOCKS에도 없으면 제외
            if (!isCommonStock && !isExisting) {
                logReason("보통주가 아니며(끝자리 0 아님) 기존 STOCKS에도 없음");
                filteredCount++; return;
            }

            // [추가 필터링] SINGLE 모드가 아닐 때만 Active List 체크
            if (mode !== 'SINGLE' && activeStockCodes.size > 0 && !isActive) {
                logReason("현재 거래 중인 종목 리스트(Active List)에 없음");
                filteredCount++; return;
            }

            if (code.startsWith('5') || code.startsWith('7') || code.startsWith('9')) {
                logReason("코드 앞자리 5,7,9 (비정상)");
                filteredCount++; return;
            }
            if (!item.lstgDt || item.lstgDt.trim() === '') {
                logReason("상장일(lstgDt) 없음");
                filteredCount++; return;
            }
            
            // 🌟 [핵심 수정 2] 상폐일 체크 로직 예외 처리
            if (item.lstgAbolDt && item.lstgAbolDt.trim() !== '') {
                if (isActive) {
                    if (mode === 'SINGLE') console.log(`💡 [예외 적용] ${name}(${code}): 과거 상폐일(${item.lstgAbolDt})이 존재하나, 현재 시세가 있는 활성 종목이므로 정상 처리합니다.`);
                    item.lstgAbolDt = ''; // DB에 상폐일이 기록되지 않도록 강제 초기화
                } else if (mode === 'SINGLE') {
                    console.log(`⚠️ [단건 강제통과] ${name}(${code}): 상장폐지(${item.lstgAbolDt}) 종목이지만 조회 요청에 의해 통과합니다.`);
                } else {
                    logReason(`상장폐지됨 (폐지일: ${item.lstgAbolDt})`);
                    filteredCount++; return;
                }
            }

            if (/(채권|선물|옵션|ELW|ETN|신주인수권|스팩)/.test(name)) {
                logReason("제외 키워드 포함 (스팩/채권 등)");
                filteredCount++; return;
            }

            // --- 통과된 종목 데이터 매핑 ---
            if (mode === 'SINGLE') console.log(`✅ [통과] ${code} (${name}) 데이터 확보 완료!`);

            infoMap[code] = {
                name_ko: name, 
                ipoDate: item.lstgDt,        
                delDate: item.lstgAbolDt      
            };

            // 🌟 [요청 사항 반영 2] 신규 발견 로직 (기존 DB에 없고 + 보통주(0)인 경우만 추가)
            if (!isExisting && isCommonStock) {
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

        // 4. 신규 종목 별도 저장 로직 (기존과 동일)
        if (newDiscoveries.length > 0) {
            console.log(`✨ [신규 발견] 순수 보통주 ${newDiscoveries.length}개 발견! 저장 시작...`);
            
            const today = new Date().toISOString().split('T')[0];
            const discoveryRef = db.collection('discovered_stocks').doc(today);
            
            const oldChunks = await discoveryRef.collection('chunks').get();
            if (!oldChunks.empty) {
                const delBatch = db.batch();
                oldChunks.docs.forEach(doc => delBatch.delete(doc.ref));
                await delBatch.commit();
            }

            const CHUNK_SIZE = 600;
            const totalChunks = Math.ceil(newDiscoveries.length / CHUNK_SIZE);

            await discoveryRef.set({
                date: today, count: newDiscoveries.length, filteredCount: filteredCount,
                description: "Filtered Common Stocks (Ends with '0')",
                isChunked: true, chunkCount: totalChunks, updatedAt: new Date().toISOString()
            }, { merge: true });

            let batch = db.batch();
            let opCount = 0;

            for (let i = 0; i < totalChunks; i++) {
                const chunkList = newDiscoveries.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
                const chunkRef = discoveryRef.collection('chunks').doc(`batch_${i}`);
                batch.set(chunkRef, { list: chunkList });
                opCount++;

                if (opCount >= 400) {
                    await batch.commit(); batch = db.batch(); opCount = 0;
                }
            }
            if (opCount > 0) await batch.commit();
            console.log(`✅ [저장 완료] 정제된 신규 종목 ${newDiscoveries.length}개 저장 완료.`);
        }

        // [A] 단건 테스트 로직
        if (mode === 'SINGLE') {
            const targetCode = symbol.replace(/[^0-9]/g, ''); 
            const info = infoMap[targetCode];
            
            if (!info) {
                return res.json({ 
                    success: false, 
                    message: `[${targetCode}] 데이터를 가져왔으나 필터링(우선주/비상장/상폐 등) 되었거나 결과가 없습니다.` 
                });
            }

            const fullSymbol = symbol.includes('.') ? symbol : `${symbol}.KS`;
            
            const updatePayload = {
                name_ko: info.name_ko,
                ipoDate: info.ipoDate
            };
            
            // 살려낸 활성 종목은 상폐일을 삭제(초기화) 처리함
            if (info.delDate) {
                updatePayload.delistedDate = info.delDate;
            } else {
                updatePayload.delistedDate = admin.firestore.FieldValue.delete();
            }

            await db.collection('stocks').doc(fullSymbol).set(updatePayload, { merge: true });

            return res.json({ 
                success: true, 
                mode: 'SINGLE', 
                data: { symbol: fullSymbol, ...info }, 
                message: `[${fullSymbol}] 업데이트 성공: ${info.name_ko}` 
            });
        }

        // ⚡ [핵심 수정 3] [B] 전체 동기화 로직 (meta_tickers를 뒤지지 않고 stocks와 직접 비교하여 업데이트)
        let updateCount = 0;
        let batch = db.batch();
        let opCount = 0;

        for (const [code, info] of Object.entries(infoMap)) {
            // 해당 코드가 KS 또는 KQ로 기존 stocks에 존재하는지 확인
            let targetSymbol = null;
            if (existingStocks[`${code}.KS`]) targetSymbol = `${code}.KS`;
            else if (existingStocks[`${code}.KQ`]) targetSymbol = `${code}.KQ`;

            if (targetSymbol) {
                const existingData = existingStocks[targetSymbol];
                
                // 기존 데이터와 하나라도 다르면 업데이트 대상!
                const isNameDifferent = (existingData.name_ko || "") !== (info.name_ko || "");
                const isIpoDifferent = (existingData.ipoDate || "") !== (info.ipoDate || "");
                const isDelDifferent = (existingData.delistedDate || "") !== (info.delDate || "");

                if (isNameDifferent || isIpoDifferent || isDelDifferent) {
                    const stockRef = db.collection('stocks').doc(targetSymbol);
                    
                    const updatePayload = {
                        name_ko: info.name_ko,
                        ipoDate: info.ipoDate
                    };
                    
                    // 상폐일이 있으면 업데이트하고, 없으면 기존 필드를 깨끗하게 삭제
                    if (info.delDate) {
                        updatePayload.delistedDate = info.delDate;
                    } else {
                        updatePayload.delistedDate = admin.firestore.FieldValue.delete();
                    }

                    batch.set(stockRef, updatePayload, { merge: true });

                    opCount++;
                    updateCount++;

                    // 400건마다 배치 쓰기
                    if (opCount >= 400) { 
                        await batch.commit(); 
                        batch = db.batch(); 
                        opCount = 0; 
                    }
                }
            }
        }
        if (opCount > 0) await batch.commit();

        res.json({ 
            success: true, 
            count: updateCount, 
            newlyDiscovered: newDiscoveries.length,
            filteredCount: filteredCount,
            message: `업데이트 완료. (변경사항 반영: ${updateCount}건, 신규: ${newDiscoveries.length}건, 필터링 제외: ${filteredCount}건)` 
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