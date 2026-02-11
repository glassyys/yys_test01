-- ================================================================
-- DELETE 쿼리: 데이터 삭제 및 정리
-- Spark/Hive SQL - 고객 관리 시스템
-- ================================================================

-- 1. 기본 DELETE: 특정 고객 삭제
DELETE FROM customer_master
WHERE customer_id = 'CUST001';

-- 2. 조건부 DELETE: 오래된 주문 삭제 (1년 이상 이전)
DELETE FROM raw_order
WHERE order_date < DATE_SUB(CURRENT_DATE, INTERVAL 365 DAY)
AND order_status IN ('CANCELLED', 'FAILED');

-- 3. DELETE with JOIN: 고객이 없는 주문 삭제
DELETE FROM raw_order
WHERE customer_id NOT IN (
    SELECT DISTINCT customer_id FROM customer_master
);

-- 4. DELETE: 중복된 고객 주소 삭제 (각 고객당 최신 주소 1개만 유지)
DELETE FROM raw_address
WHERE address_id NOT IN (
    SELECT MAX(address_id) 
    FROM raw_address 
    GROUP BY customer_id, address_type
)
AND is_primary = FALSE;

-- 5. DELETE: 테스트 데이터 삭제
DELETE FROM raw_customer
WHERE customer_name LIKE '%TEST%'
OR email LIKE '%test@%';

-- 6. DELETE with EXISTS: 주문이 없는 고객 삭제 (선택사항)
DELETE FROM customer_master
WHERE NOT EXISTS (
    SELECT 1 FROM raw_order 
    WHERE raw_order.customer_id = customer_master.customer_id
)
AND DATEDIFF(CURRENT_DATE, registration_date) > 730;  -- 2년 이상 비활성

-- 7. DELETE: 취소된 주문 관련 항목 삭제
DELETE FROM raw_order_item
WHERE order_id IN (
    SELECT order_id FROM raw_order 
    WHERE order_status = 'CANCELLED'
    AND order_date < DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
);

-- 8. DELETE: 스테이징 테이블 정리 (처리 완료 후)
DELETE FROM customer_staging
WHERE customer_id IN (
    SELECT customer_id FROM customer_master 
    WHERE updated_at >= CURRENT_DATE
)
AND is_deleted = FALSE;

-- 9. DELETE: 품질 검사 실패 레코드 삭제
DELETE FROM data_quality_log
WHERE result = 'FAILED'
AND checked_at < DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY);

-- 10. TRUNCATE: 스테이징 테이블 완전 삭제 (구조만 남김)
TRUNCATE TABLE order_staging;

-- 11. DELETE: 주소 정보 삭제 (고객 삭제 시 연계 삭제)
DELETE FROM raw_address
WHERE customer_id IN (
    SELECT customer_id FROM customer_master 
    WHERE is_deleted = TRUE OR country = 'UNKNOWN'
);

-- 12. 논리적 삭제: DELETE 플래그로 표시 (물리적 삭제 대신)
UPDATE raw_order
SET order_status = 'DELETED'
WHERE order_date < DATE_SUB(CURRENT_DATE, INTERVAL 1825 DAY)  -- 5년 이상 이전
AND order_status NOT IN ('COMPLETED', 'SHIPPED');

-- 13. DELETE: 부정 주문 삭제
DELETE FROM raw_order
WHERE total_amount <= 0
OR customer_id IS NULL
OR order_date IS NULL;

-- 14. DELETE: 보관 기간 지난 임시 테이블 삭제
DROP TABLE IF EXISTS customer_staging_temp;
DROP TABLE IF EXISTS order_staging_temp;

-- 15. 파티션 기반 DELETE (대용량 데이터 효율적 처리)
ALTER TABLE raw_order DROP IF EXISTS PARTITION (order_date < '2020-01-01');

-- 16. DELETE: 중복 레코드 제거 (ROW_NUMBER 사용)
DELETE FROM customer_master
WHERE customer_id IN (
    SELECT customer_id 
    FROM (
        SELECT customer_id,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) as rn
        FROM customer_master
    ) t
    WHERE rn > 1
);

-- 17. DELETE: 데이터 품질 기준 미만 레코드 삭제
DELETE FROM raw_order_item
WHERE (quantity IS NULL OR quantity <= 0)
OR (unit_price IS NULL OR unit_price < 0)
OR (item_total IS NULL OR item_total < 0)
OR order_id NOT IN (SELECT order_id FROM raw_order);

-- 18. DELETE: 비활성 고객 정리 (보관 후 삭제)
DELETE FROM customer_master
WHERE DATEDIFF(CURRENT_DATE, last_order_date) > 1095  -- 3년 비활성
AND DATEDIFF(CURRENT_DATE, updated_at) > 180;  -- 6개월 미갱신

-- 19. DELETE: 배치 작업 관련 로그 삭제
DELETE FROM data_quality_log
WHERE checked_at < DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
AND result IN ('SKIPPED', 'DEPRECATED');

-- 20. MERGE를 이용한 안전한 DELETE (soft delete)
MERGE INTO customer_master t
USING (
    SELECT customer_id FROM customer_staging WHERE is_deleted = TRUE
) s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN
    UPDATE SET
        t.customer_name = '[DELETED]',
        t.email = NULL,
        t.phone = NULL,
        t.updated_at = CURRENT_TIMESTAMP;
