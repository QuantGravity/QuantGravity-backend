// ===========================================================================
// [파일명] : utils/errorManager.js
// [대상]   : 시스템 배치 작업 및 API 동기화 실패 로그 공통 관리
// ===========================================================================
const admin = require('firebase-admin');

const errorManager = {
    // 1. 에러 기록 (Upsert)
    async logError(type, symbol, errorMessage) {
        try {
            const db = admin.firestore();
            await db.collection('sync_errors').doc(`${type}_${symbol}`).set({
                type: type,
                symbol: symbol,
                errorMessage: errorMessage,
                updatedAt: new Date().toISOString()
            }, { merge: true });
        } catch (e) {
            console.error(`[ErrorManager] 로깅 실패:`, e);
        }
    },

    // 2. 에러 해결 시 삭제 (Resolve)
    async resolveError(type, symbol) {
        try {
            const db = admin.firestore();
            await db.collection('sync_errors').doc(`${type}_${symbol}`).delete();
        } catch (e) {
            // 이미 지워졌거나 오류나도 무시
        }
    },

    // 3. 타입별 에러 요약 통계 (프론트엔드 뱃지 표시용)
    async getSummary() {
        try {
            const db = admin.firestore();
            const snapshot = await db.collection('sync_errors').get();
            const summary = {};
            
            snapshot.forEach(doc => {
                const type = doc.data().type;
                if (!summary[type]) summary[type] = 0;
                summary[type]++;
            });
            return summary;
        } catch (e) {
            return {};
        }
    }
};

module.exports = errorManager;