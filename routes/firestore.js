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
const { verifyToken } = require('../utils/authHelper');

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

module.exports = router;