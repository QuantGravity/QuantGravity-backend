const express = require('express');
const admin = require('firebase-admin'); // Firebase Admin SDK 사용

// Firebase Admin SDK 초기화 (이 코드는 서버 시작 시 한 번만 실행되어야 합니다)
// 실제 환경에서는 서비스 계정 키를 통해 초기화합니다.
// if (!admin.apps.length) {
//     admin.initializeApp({
//         credential: admin.credential.cert(require('/path/to/your/serviceAccountKey.json'))
//     });
// }

const db = admin.firestore();
const router = express.Router();

// 1. GET /api/billing/plans : 전체 플랜 목록 조회 (tier_level 오름차순 정렬)
router.get('/plans', async (req, res) => {
    try {
        const plansSnapshot = await db.collection('billing_plans').orderBy('tier_level', 'asc').get();
        const plans = plansSnapshot.docs.map(doc => ({
            id: doc.id,
            ...doc.data()
        }));
        return res.status(200).json(plans);
    } catch (error) {
        console.error('Error fetching billing plans:', error);
        return res.status(500).json({ message: 'Failed to retrieve billing plans.', error: error.message });
    }
});

// 2. POST /api/billing/plans : 신규 플랜 생성 (관리자 권한 체크 TODO 주석 처리)
router.post('/plans', async (req, res) => {
    // TODO: 관리자 권한 체크 로직 추가
    // if (!req.user || !req.user.isAdmin) {
    //     return res.status(403).json({ message: 'Authorization required for this action.' });
    // }

    const { id, name, price, tier_level, features, is_active } = req.body;

    if (!id || !name || price === undefined || tier_level === undefined || !features || is_active === undefined) {
        return res.status(400).json({ message: 'Missing required fields for new billing plan.' });
    }

    try {
        const planRef = db.collection('billing_plans').doc(id);
        const doc = await planRef.get();

        if (doc.exists) {
            return res.status(409).json({ message: `Billing plan with ID '${id}' already exists.` });
        }

        const newPlan = {
            name,
            price: Number(price),
            tier_level: Number(tier_level),
            features,
            is_active: Boolean(is_active),
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        };

        await planRef.set(newPlan);
        return res.status(201).json({ message: 'Billing plan created successfully.', id, plan: newPlan });
    } catch (error) {
        console.error('Error creating billing plan:', error);
        return res.status(500).json({ message: 'Failed to create billing plan.', error: error.message });
    }
});

// 3. PUT /api/billing/plans/:id : 플랜 정보 수정
router.put('/plans/:id', async (req, res) => {
    // TODO: 관리자 권한 체크 로직 추가
    // if (!req.user || !req.user.isAdmin) {
    //     return res.status(403).json({ message: 'Authorization required for this action.' });
    // }

    const planId = req.params.id;
    const updateData = req.body;

    if (Object.keys(updateData).length === 0) {
        return res.status(400).json({ message: 'No update data provided.' });
    }

    try {
        const planRef = db.collection('billing_plans').doc(planId);
        const doc = await planRef.get();

        if (!doc.exists) {
            return res.status(404).json({ message: `Billing plan with ID '${planId}' not found.` });
        }

        // 숫자 타입으로 변환이 필요한 필드 처리
        if (updateData.price !== undefined) {
            updateData.price = Number(updateData.price);
        }
        if (updateData.tier_level !== undefined) {
            updateData.tier_level = Number(updateData.tier_level);
        }
        if (updateData.is_active !== undefined) {
            updateData.is_active = Boolean(updateData.is_active);
        }

        updateData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

        await planRef.update(updateData);
        return res.status(200).json({ message: 'Billing plan updated successfully.', id: planId, updatedFields: updateData });
    } catch (error) {
        console.error(`Error updating billing plan with ID '${planId}':`, error);
        return res.status(500).json({ message: 'Failed to update billing plan.', error: error.message });
    }
});

// 4. DELETE /api/billing/plans/:id : 플랜 삭제 (실제 삭제 대신 is_active를 false로 변경하는 soft delete 권장)
router.delete('/plans/:id', async (req, res) => {
    // TODO: 관리자 권한 체크 로직 추가
    // if (!req.user || !req.user.isAdmin) {
    //     return res.status(403).json({ message: 'Authorization required for this action.' });
    // }

    const planId = req.params.id;

    try {
        const planRef = db.collection('billing_plans').doc(planId);
        const doc = await planRef.get();

        if (!doc.exists) {
            return res.status(404).json({ message: `Billing plan with ID '${planId}' not found.` });
        }

        // Soft delete: is_active 필드를 false로 변경
        await planRef.update({
            is_active: false,
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        });
        return res.status(200).json({ message: `Billing plan with ID '${planId}' soft-deleted (set to inactive).` });
    } catch (error) {
        console.error(`Error soft-deleting billing plan with ID '${planId}':`, error);
        return res.status(500).json({ message: 'Failed to soft-delete billing plan.', error: error.message });
    }
});

module.exports = router;