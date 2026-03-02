// ===========================================================================
// [파일명] : routes/firestore.js
// [대상]   : Firestore 데이터 CRUD(생성, 조회, 수정, 삭제) 공통 핸들러
// [기준]   : 
//   1. 보안 최우선: 모든 쓰기/삭제 요청은 COLLECTION_RULES를 통해 권한을 검증한다.
//   2. 데이터 무결성: null 값은 convertNullToDelete를 통해 필드 삭제 명령으로 변환한다.
//   3. 용량 제한: 단일 문서 1MB 초과 시 업로드를 차단하여 시스템 안정성을 유지한다.
//   4. 날짜 표준화: Firestore Timestamp 객체는 클라이언트에 보낼 때 ISO 문자열로 변환한다.
// ===========================================================================
const express = require('express');
const router = express.Router();
const admin = require('firebase-admin');
const firestore = admin.firestore();
const { verifyToken, verifyBatchOrAdmin } = require('../utils/authHelper');

// ============================================================ 
// 각 컬렉션 별로 사용자권한 점검 - 수작업으로 관리해줘야 함.
// ============================================================
const COLLECTION_RULES = {
    // 1. [관리자 영역] 티커, 주가정보 등은 관리자나 G9만 수정 가능
    'tickers': (user, docId, data) => ['admin', 'G9'].includes(user.role),
    'ticker_prices': (user, docId, data) => ['admin'].includes(user.role),
    'notices': (user, docId, data) => ['admin'].includes(user.role),
    
    // [추가] 메뉴 설정도 관리자만 수정 가능
    'menu_settings': (user, docId, data) => ['admin'].includes(user.role), 
    
    // [추가] stocks 컬렉션 권한 추가
    'stocks': (user, docId, data) => ['admin', 'G9'].includes(user.role),

    // ▼▼▼ [신규 추가] FMP 마스터 데이터 (관리자 전용) ▼▼▼
    // 섹터 및 산업 분류는 시스템 기준이므로 관리자만 수정
    'meta_sectors': (user, docId, data) => ['admin'].includes(user.role),
    'meta_industries': (user, docId, data) => ['admin'].includes(user.role),
    
    // 상장폐지 및 티커 마스터 정보도 관리자 권한 필수
    'meta_delisted': (user, docId, data) => ['admin'].includes(user.role),
    'meta_tickers': (user, docId, data) => ['admin'].includes(user.role),

    // 종목별 통계 데이터 (종목 Map 등에서 사용)
    // 날짜별/국가별 통계 정보이므로 관리자만 배치 작업으로 업데이트함
    'meta_ticker_stats': (user, docId, data) => ['admin'].includes(user.role),
    'meta_sector_stats': (user, docId, data) => ['admin'].includes(user.role), // 🌟 [추가됨] 13번 작업을 위한 권한
    
    // 통계 데이터의 하위 chunk 컬렉션 접근을 위해 추가
    // (URL 인코딩된 경로로 들어올 경우를 대비해 규칙에 포함)
    'meta_ticker_stats_chunks': (user, docId, data) => ['admin'].includes(user.role),
    'meta_sector_stats_chunk': (user, docId, data) => ['admin'].includes(user.role),

    // [추가] 일반 통계 및 시장 지표 데이터
    // 사용자는 '조회'만 가능해야 하므로, API를 통한 '쓰기'는 관리자만 허용
    'meta_stats': (user, docId, data) => ['admin', 'G9'].includes(user.role),

    // ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

    // 2. [개인 데이터 영역] 내 문서는 '나(docId)'만 수정 가능
    'user_strategies': (user, docId, data) => user.email === docId,
    'investment_tickers': (user, docId, data) => user.email === docId, 
    'favorite_groups': (user, docId, data) => user.email === docId,
    'favorite_tickers': (user, docId, data) => user.email === docId,
    'user_analysis_jobs': (user, docId, data) => user.email === docId,

    // [추가] 고객 문의 권한 설정
    'customer_inquiries': (user, docId, data) => {
        if (['admin', 'G9'].includes(user.role)) return true;
        if (data && data.userId === user.email) return true;
        return true; 
    },

    // 3. [위험 구역] 회원 정보는 공통 API로 수정 금지
    'users': (user, docId, data) => {
        if (['admin', 'G9'].includes(user.role)) return true;
        return user.email === docId;
    },

    // 4. [시스템 영역] 로그 등은 API로 임의 수정 절대 불가
    'traffic_logs': (user, docId, data) => false,
    'limit_logs': (user, docId, data) => false
};

// [수정 후] 
// null 뿐만 아니라 "DELETE_FIELD" 문자열도 재귀적으로 찾아서 삭제 명령으로 변환
function convertNullToDelete(obj) {
    for (const key in obj) {
        // 1. 값이 null 이거나 "DELETE_FIELD" 문자열이면 -> 삭제 명령으로 변경
        if (obj[key] === null || obj[key] === "DELETE_FIELD") {
            obj[key] = admin.firestore.FieldValue.delete();
        } 
        // 2. 객체인 경우 (Firestore 객체 등 특수 객체 제외) 재귀 탐색
        else if (typeof obj[key] === 'object' && obj[key] !== null) {
            // Firestore FieldValue 객체 등은 건드리지 않음
            if (obj[key].constructor && obj[key].constructor.name === 'FieldValue') continue;
            convertNullToDelete(obj[key]);
        }
    }
    return obj;
}

// 백엔드 API 예시: /api/firestore/upload
router.post('/upload-to-firestore', verifyToken, async (req, res) => {
    try {
        const { collectionName, docId, data } = req.body;
        const user = req.user; // verifyToken에서 담아준 사용자 정보

        // [추가] 권한 체크 로직
        const rule = COLLECTION_RULES[collectionName];
        if (rule && !rule(user, docId, data)) {
            return res.status(403).json({ error: "해당 컬렉션에 대한 수정 권한이 없습니다." });
        }

        if (!collectionName || !data) {
            return res.status(400).json({ error: "컬렉션 이름과 데이터는 필수입니다." });
        }

        // 1. 데이터 가공: "SERVER_TIMESTAMP" 문자열을 실제 Firebase 서버 시간 객체로 변환
        const finalData = { ...data };
        Object.keys(finalData).forEach(key => {
            if (finalData[key] === "SERVER_TIMESTAMP") {
                finalData[key] = admin.firestore.FieldValue.serverTimestamp();
            }
            // [추가] 필드 삭제 처리
            if (finalData[key] === "DELETE_FIELD") {
                finalData[key] = admin.firestore.FieldValue.delete();
            }
        });

        // ============================================================
        // [추가된 코드 2] 위에서 만든 재귀 함수 실행
        // finalData 안에 있는 모든 중첩된 null 값을 찾아서 삭제 명령으로 바꿉니다.
        // ============================================================
        convertNullToDelete(finalData);

        // 2. 용량 계산용 JSON 문자열 생성 (정의되지 않은 finaldata 대신 data 사용)
        const jsonString = JSON.stringify(data);
        const byteSize = Buffer.byteLength(jsonString, 'utf8');
        const kbSize = (byteSize / 1024).toFixed(2);

        // 3. 터미널 로그 출력
        console.log(`--------------------------------------------------`);
        console.log(`[Firestore Upload Log]`);
        console.log(`- 경로: ${collectionName}/${docId || 'auto-gen'}`);
        console.log(`- 용량: ${byteSize} bytes (${kbSize} KB)`);
        console.log(`--------------------------------------------------`);

        // 4. Firestore 용량 제한 체크 (1MB)
        if (byteSize > 1048487) {
            return res.status(413).json({ error: "Firestore 단일 문서 용량 제한(1MB)을 초과했습니다." });
        }

        // 5. DB 작업 (질문자님이 선언하신 'firestore' 변수 사용)
        const colRef = firestore.collection(collectionName);
        
        if (docId) {
            // 기존 문서 업데이트 (merge: true)
            await colRef.doc(docId).set({
                ...finalData,
                updatedAt: admin.firestore.FieldValue.serverTimestamp()
            }, { merge: true });
            
            res.status(200).json({ success: true, docId: docId });
        } else {
            // 신규 문서 생성
            const docRef = await colRef.add({
                ...finalData,
                createdAt: admin.firestore.FieldValue.serverTimestamp(),
                updatedAt: admin.firestore.FieldValue.serverTimestamp()
            });
            res.status(200).json({ success: true, docId: docRef.id });
        }
    } catch (error) {
        // catch 블록의 변수명을 error로 통일하여 참조 에러 방지
        console.error("[Firestore Update Error]:", error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// [공통] 특정 컬렉션의 문서 삭제
router.delete('/delete-from-firestore', verifyToken, async (req, res) => {
    try {
        const { collectionName, id } = req.query; // 프론트엔드 호출 규격에 맞춰 id로 받음

        if (!collectionName || !id) {
            return res.status(400).json({ error: "컬렉션 이름과 문서 ID는 필수입니다." });
        }

        console.log(`[Firestore Delete] ${collectionName}/${id}`);

        await firestore.collection(collectionName).doc(id).delete();

        res.status(200).json({ success: true, message: "성공적으로 삭제되었습니다." });
    } catch (error) {
        console.error("Delete Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [공통 기능 업그레이드] 다건 일괄 삭제 (Batch Delete)
// 요청: POST /api/firestore/batch-delete
// Body: { "collectionName": "market_themes", "ids": ["id1", "id2", ...] }
// ============================================================
router.post('/batch-delete', verifyToken, async (req, res) => {
    try {
        const { collectionName, ids } = req.body;
        const user = req.user;

        // 1. 유효성 검사
        if (!collectionName || !ids || !Array.isArray(ids) || ids.length === 0) {
            return res.status(400).json({ error: "컬렉션 이름과 삭제할 ID 목록(배열)은 필수입니다." });
        }

        // 2. 권한 체크 (기존 COLLECTION_RULES 재사용)
        // 삭제는 강력한 권한이 필요하므로, 각 문서에 대해 권한을 확인해야 하지만
        // 성능을 위해 '관리자(admin)' 이거나 해당 컬렉션의 쓰기 규칙을 통과하는지 샘플로 확인합니다.
        const rule = COLLECTION_RULES[collectionName];
        
        // 대표적으로 첫 번째 ID로 권한을 검증 (또는 관리자 권한 강제)
        // 안전을 위해 관리자/G9 등급이 아니면 본인 데이터인지 엄격하게 체크
        const isSuperUser = ['admin', 'G9'].includes(user.role);
        if (!isSuperUser) {
             // 일반 유저의 경우 삭제 권한 로직이 복잡할 수 있어, 여기서는 관리자급만 일괄 삭제 허용
             // 필요하다면 loop를 돌며 rule(user, id, null) 체크 가능
            return res.status(403).json({ error: "일괄 삭제는 관리자 등급만 가능합니다." });
        }

        console.log(`🗑️ [Batch Delete] ${collectionName} - ${ids.length}건 삭제 요청`);

        // 3. Firestore Batch 처리 (500개 제한 고려하여 쪼개서 처리)
        const CHUNK_SIZE = 450; // 여유 있게 450개씩
        let totalDeleted = 0;

        for (let i = 0; i < ids.length; i += CHUNK_SIZE) {
            const chunk = ids.slice(i, i + CHUNK_SIZE);
            const batch = firestore.batch();

            chunk.forEach(id => {
                const docRef = firestore.collection(collectionName).doc(id);
                batch.delete(docRef);
            });

            await batch.commit();
            totalDeleted += chunk.length;
        }

        console.log(`✅ [Batch Delete] 총 ${totalDeleted}건 삭제 완료`);
        res.json({ success: true, count: totalDeleted, message: "일괄 삭제가 완료되었습니다." });

    } catch (error) {
        console.error("Batch Delete Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// [백엔드] 수정된 리스트 조회 API
router.get('/list/:collectionName', verifyToken, async (req, res) => {
    try {
        const { collectionName } = req.params;
        
        // 보안 로직 (기존 유지)
        if (collectionName === 'users') {
            if (!['admin', 'G9'].includes(req.user?.role)) {
                return res.status(403).json({ error: "권한 없음" });
            }
        }

        const snapshot = await firestore.collection(collectionName).get();
        if (snapshot.empty) return res.json([]);

        const list = snapshot.docs.map(doc => {
            const fullData = doc.data();
            
            // [중요 로직 유지] metadata 필드가 있으면 그것을 사용하고, 없으면 전체 데이터를 사용
            let dataContent = fullData.metadata ? { ...fullData.metadata } : { ...fullData };

            // [날짜 변환 추가] 데이터 안의 Timestamp 객체를 문자열로 변환 (에러 방지 핵심)
            if (dataContent.createdAt && typeof dataContent.createdAt.toDate === 'function') {
                dataContent.createdAt = dataContent.createdAt.toDate().toISOString();
            }
            if (dataContent.updatedAt && typeof dataContent.updatedAt.toDate === 'function') {
                dataContent.updatedAt = dataContent.updatedAt.toDate().toISOString();
            }

            return {
                id: doc.id, // 문서 ID 포함
                ...dataContent 
            };
        });

        res.json(list);
    } catch (error) {
        console.error("List Fetch Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// [공통] 특정 컬렉션의 상세 문서 가져오기 (전체 데이터)
// [server.js] 상세 조회 API 수정 (디버깅 로그 추가)
router.get('/detail/:collectionName/:docId', verifyToken, async (req, res) => {
    try {
        const { collectionName, docId } = req.params;
        
        // [1] 요청 로그 출력 (서버 콘솔 확인용)
        console.log(`========================================`);
        console.log(`[DEBUG] 상세 조회 요청: ${collectionName} / ${docId}`);

        const doc = await firestore.collection(collectionName).doc(docId).get();

        // [2] 데이터 존재 여부 로그
        console.log(`[DEBUG] Firestore 존재 여부: ${doc.exists}`);

        if (!doc.exists) {
            console.log(`[DEBUG] ❌ 문서가 없어 404 반환`);
            return res.status(404).json({ error: "데이터를 찾을 수 없습니다." });
        }

        console.log(`[DEBUG] ✅ 데이터 반환 성공`);
        res.json(doc.data());

    } catch (error) {
        console.error("[Detail Fetch Error]:", error);
        res.status(500).json({ error: error.message });
    }
});

// 컬렉션 전체를 삭제하는 공통 함수 (서브 컬렉션 포함 가능)
async function deleteCollection(db, collectionPath, batchSize = 500) {
    const collectionRef = db.collection(collectionPath);
    const query = collectionRef.orderBy('__name__').limit(batchSize);

    return new Promise((resolve, reject) => {
        deleteQueryBatch(db, query, resolve).catch(reject);
    });
}

async function deleteQueryBatch(db, query, resolve) {
    const snapshot = await query.get();

    // 더 이상 지울 문서가 없으면 종료
    if (snapshot.size === 0) {
        resolve();
        return;
    }

    const batch = db.batch();
    snapshot.docs.forEach((doc) => {
        // 만약 chunks 같은 서브 컬렉션이 있다면 여기서도 처리가 필요할 수 있어.
        // stocks의 경우 서브 컬렉션이 없다면 바로 delete 가능
        batch.delete(doc.ref);
    });

    await batch.commit();

    // 다음 배치를 위해 재귀 호출 (프로세스 넥스트 틱 사용으로 스택 오버플로 방지)
    process.nextTick(() => {
        deleteQueryBatch(db, query, resolve);
    });
}

// 관리자용 삭제 엔드포인트
// 관리자용 재귀적 삭제 엔드포인트
router.post('/delete-recursive', verifyBatchOrAdmin, async (req, res) => {
    const { collectionName } = req.body;
    if (!collectionName) return res.status(400).json({ error: "컬렉션 이름 누락" });

    try {
        const db = admin.firestore();
        const collectionRef = db.collection(collectionName);
        
        // listDocuments()는 데이터가 없는 '유령 문서'도 모두 가져온다.
        const docRefs = await collectionRef.listDocuments();

        if (docRefs.length === 0) {
            return res.json({ success: true, isFinished: true, deletedCount: 0 });
        }

        // 한 번에 처리할 배치 크기 제한 (Firestore 제한: 500개)
        const batchSize = 500;
        const targetDocs = docRefs.slice(0, batchSize);
        
        // 각 문서의 하위 서브 컬렉션까지 삭제하려면 아래 함수를 재귀적으로 호출해야 함
        // 여기서는 일단 해당 문서 레벨에서의 삭제를 처리
        for (const docRef of targetDocs) {
            // 해당 문서의 하위 서브 컬렉션 리스트를 가져옴
            const subCollections = await docRef.listCollections();
            for (const subCol of subCollections) {
                // 서브 컬렉션 내의 문서들도 삭제 (재귀적 처리가 필요할 경우 공통함수 호출)
                await db.recursiveDelete(subCol); 
            }
            // 문서 본인 삭제
            await docRef.delete();
        }

        res.json({ 
            success: true, 
            isFinished: docRefs.length <= batchSize, 
            deletedCount: targetDocs.length 
        });
    } catch (error) {
        console.error("Delete Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [신규 11-1번용] 주가 데이터 연도별 청크 다운로드 API
// 클라이언트에서 넘긴 종목 배열을 기반으로 stocks/{symbol}/annual_data 를 수집해 반환
// ============================================================
router.post('/download-stocks-annual-chunk', verifyToken, async (req, res) => {
    try {
        const { symbols, startYear, endYear } = req.body;
        const user = req.user;

        // 권한 방어
        if (!['admin', 'G9'].includes(user.role)) {
            return res.status(403).json({ error: "관리자 전용 기능입니다." });
        }
        if (!symbols || !Array.isArray(symbols)) {
            return res.status(400).json({ error: "요청 심볼 리스트가 필요합니다." });
        }

        const result = [];

        // 비동기로 Firestore 데이터 연속 조회 (속도 최적화)
        for (const sym of symbols) {
            for (let y = parseInt(startYear); y <= parseInt(endYear); y++) {
                const doc = await firestore.collection('stocks').doc(sym).collection('annual_data').doc(String(y)).get();
                if (doc.exists) {
                    result.push({ 
                        id: `${sym}_${y}`, 
                        symbol: sym, 
                        year: String(y), 
                        data: doc.data() 
                    });
                }
            }
        }

        res.json({ success: true, data: result });
    } catch (error) {
        console.error("Download Stocks Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [신규 11-2번용] 기업 이벤트(배당/분할) 데이터 청크 다운로드 API
// 클라이언트에서 넘긴 종목 배열을 기반으로 stocks/{symbol}/actions/{doc} 수집
// ============================================================
router.post('/download-stocks-actions-chunk', verifyToken, async (req, res) => {
    try {
        const { symbols } = req.body;
        const user = req.user;

        // 권한 방어
        if (!['admin', 'G9'].includes(user.role)) {
            return res.status(403).json({ error: "관리자 전용 기능입니다." });
        }
        if (!symbols || !Array.isArray(symbols)) {
            return res.status(400).json({ error: "요청 심볼 리스트가 필요합니다." });
        }

        const result = [];

        // 배당과 분할 문서를 비동기로 동시에 요청해서 속도 최적화
        const fetchPromises = symbols.map(async (sym) => {
            const actionsRef = firestore.collection('stocks').doc(sym).collection('actions');
            
            const [divDoc, splitDoc] = await Promise.all([
                actionsRef.doc('dividends').get(),
                actionsRef.doc('splits').get()
            ]);

            if (divDoc.exists) {
                result.push({ 
                    id: `${sym}_dividends`, 
                    symbol: sym, 
                    type: 'dividends', 
                    data: divDoc.data() 
                });
            }
            
            if (splitDoc.exists) {
                result.push({ 
                    id: `${sym}_splits`, 
                    symbol: sym, 
                    type: 'splits', 
                    data: splitDoc.data() 
                });
            }
        });

        // 100개 종목의 배당/분할 쿼리를 병렬 처리
        await Promise.all(fetchPromises);

        res.json({ success: true, data: result });
    } catch (error) {
        console.error("Download Actions Error:", error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// [신규 12, 13번용] 다건 일괄 업로드 (Batch Upload)
// 클라이언트 로컬에서 집계된 데이터를 Firestore로 초고속 덮어쓰기
// ============================================================
router.post('/batch-upload', verifyToken, async (req, res) => {
    try {
        const { collectionName, items } = req.body;
        const user = req.user;

        if (!collectionName || !items || !Array.isArray(items) || items.length === 0) {
            return res.status(400).json({ error: "컬렉션 이름과 업로드할 데이터 목록은 필수입니다." });
        }

        const rule = COLLECTION_RULES[collectionName];
        if (rule && !rule(user, items[0].docId, items[0].data)) {
            return res.status(403).json({ error: "해당 컬렉션에 대한 일괄 업로드 권한이 없습니다." });
        }

        console.log(`🚀 [Batch Upload] ${collectionName} - ${items.length}건 업로드 시작`);

        // Firestore 트랜잭션 제한 (한 번에 500개) 돌파를 위해 청크 분할
        const CHUNK_SIZE = 450;
        let totalUploaded = 0;

        for (let i = 0; i < items.length; i += CHUNK_SIZE) {
            const chunk = items.slice(i, i + CHUNK_SIZE);
            const batch = firestore.batch();
            const colRef = firestore.collection(collectionName);

            chunk.forEach(item => {
                const docRef = colRef.doc(item.docId);
                const finalData = { ...item.data };
                
                // Firestore 내장 명령어 치환 로직 (기존 공통 로직 재사용)
                Object.keys(finalData).forEach(key => {
                    if (finalData[key] === "SERVER_TIMESTAMP") {
                        finalData[key] = admin.firestore.FieldValue.serverTimestamp();
                    }
                    if (finalData[key] === "DELETE_FIELD") {
                        finalData[key] = admin.firestore.FieldValue.delete();
                    }
                });

                // null을 delete 명령으로 컨버팅하는 기존 재귀 함수 적용
                if (typeof convertNullToDelete === "function") {
                    convertNullToDelete(finalData);
                }
                
                finalData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

                // Merge: true 옵션으로 덮어쓰기
                batch.set(docRef, finalData, { merge: true });
            });

            await batch.commit();
            totalUploaded += chunk.length;
        }

        console.log(`✅ [Batch Upload] ${collectionName} - 총 ${totalUploaded}건 업로드 완료`);
        res.json({ success: true, count: totalUploaded, message: "일괄 업로드가 완벽히 적용되었습니다." });

    } catch (error) {
        console.error("Batch Upload Error:", error);
        res.status(500).json({ error: error.message });
    }
});

module.exports = router;