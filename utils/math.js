// ===========================================================================
// [파일명] : utils/math.js
// [대상]   : 퀀트 분석 및 시뮬레이션용 공통 수학/통계 함수
// [기준]   : 
//   1. 정확성: CAGR 계산 시 복리 공식을 엄격히 준수하며, 비정상 데이터(0 이하 등)는 0을 반환한다.
//   2. 일관성: 중앙값(Median) 등 통계 계산 시 원본 배열의 변형을 방지하기 위해 정렬 전 복사본을 사용한다.
//   3. 범용성: 날짜 차이(DaysDiff) 등 여러 모듈에서 공통으로 쓰이는 보조 함수를 통합 관리한다.
//   4. 무상태성: 외부 변수에 의존하지 않고 오직 입력값에 의해서만 결과가 결정되는 순수 함수로 작성한다.
// ===========================================================================

/**
 * 연평균 성장률(CAGR) 계산
 */
const calculateCAGR = (startPrice, endPrice, years) => {
    if (years <= 0 || startPrice <= 0 || endPrice <= 0) return 0;
    return (Math.pow(endPrice / startPrice, 1 / years) - 1) * 100;
};

/**
 * 배열의 중앙값(Median) 계산
 */
const getMedian = (values) => {
    if (!values || values.length === 0) return 0;
    
    // 원본 배열 보호를 위해 복사 후 정렬
    const sorted = [...values].sort((a, b) => a - b);
    const half = Math.floor(sorted.length / 2);
    
    if (sorted.length % 2) return sorted[half];
    return (sorted[half - 1] + sorted[half]) / 2.0;
};

/**
 * 두 날짜 사이의 일수 차이 계산
 */
function getDaysDiff(startStr, endStr) {
    if (!startStr || !endStr) return 0;
    const s = new Date(startStr);
    const e = new Date(endStr);
    return Math.floor((e - s) / (1000 * 60 * 60 * 24));
}

// 외부에서 사용할 수 있도록 내보내기
module.exports = {
    calculateCAGR,
    getMedian,
    getDaysDiff
};