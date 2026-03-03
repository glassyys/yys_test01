-- ================================================================
-- UPDATE 쿼리: 데이터 수정 및 병합
-- Spark/Hive SQL - 고객 관리 시스템
-- ================================================================

-- 1. 간단한 UPDATE: 고객 정보 수정
UPDATE customer_master
SET 
    customer_name = 'Updated Name',
    email = 'newemail@example.com',
    phone = '010-1234-5678',
    updated_at = CURRENT_TIMESTAMP
WHERE 
    customer_id = 'CUST001';

-- 2. 조건부 UPDATE: 고객 총 주문수 업데이트
UPDATE customer_master c
SET 
    c.total_orders = (
        SELECT COUNT(DISTINCT order_id) 
        FROM raw_order 
        WHERE customer_id = c.customer_id 
        AND order_status IN ('COMPLETED', 'PENDING')
    ),
    c.total_amount = (
        SELECT COALESCE(SUM(total_amount), 0)
        FROM raw_order
        WHERE customer_id = c.customer_id
        AND order_status = 'COMPLETED'
    ),
    c.last_order_date = (
        SELECT MAX(order_date)
        FROM raw_order
        WHERE customer_id = c.customer_id
    ),
    c.updated_at = CURRENT_TIMESTAMP
WHERE 
    c.customer_id IN (
        SELECT DISTINCT customer_id FROM raw_order
    );

-- 3. MERGE INTO: 고객 마스터 병합 (INSERT/UPDATE)
MERGE INTO customer_master t
USING customer_staging s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.is_deleted = FALSE THEN
    UPDATE SET
        t.customer_name = s.customer_name,
        t.email = s.email,
        t.phone = s.phone,
        t.registration_date = s.registration_date,
        t.country = s.country,
        t.updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED AND s.is_deleted = FALSE THEN
    INSERT (customer_id, customer_name, email, phone, registration_date, country, created_at, updated_at)
    VALUES (s.customer_id, s.customer_name, s.email, s.phone, s.registration_date, s.country, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- 4. MERGE INTO: 주문 마스터 병합 (INSERT/UPDATE/DELETE)
MERGE INTO raw_order t
USING order_staging s
ON t.order_id = s.order_id
WHEN MATCHED AND s.is_deleted = FALSE THEN
    UPDATE SET
        t.order_status = s.order_status,
        t.total_amount = s.total_amount,
        t.payment_method = s.payment_method,
        t.created_at = CURRENT_TIMESTAMP
WHEN MATCHED AND s.is_deleted = TRUE THEN
    DELETE
WHEN NOT MATCHED AND s.is_deleted = FALSE THEN
    INSERT (order_id, customer_id, order_date, order_status, total_amount, payment_method, created_at)
    VALUES (s.order_id, s.customer_id, s.order_date, s.order_status, s.total_amount, s.payment_method, CURRENT_TIMESTAMP);

-- 5. UPDATE with CASE: 주문 상태 배치 업데이트
UPDATE raw_order
SET 
    order_status = CASE 
        WHEN DATEDIFF(CURRENT_DATE, order_date) > 7 AND order_status = 'PENDING' THEN 'EXPIRED'
        WHEN order_status = 'PROCESSING' AND total_amount >= 100000 THEN 'PRIORITY'
        ELSE order_status
    END,
    created_at = CURRENT_TIMESTAMP
WHERE 
    order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
    AND order_status IN ('PENDING', 'PROCESSING');

-- 6. 배치 UPDATE: 고객 세그먼트 업데이트
UPDATE customer_master
SET 
    total_orders = (
        SELECT COUNT(*) FROM raw_order WHERE customer_id = customer_master.customer_id
    ),
    updated_at = CURRENT_TIMESTAMP
WHERE 
    customer_id IN (
        SELECT DISTINCT customer_id FROM raw_order
        WHERE order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY)
    );

-- 7. UPDATE with EXISTS: 최근 주문 있는 고객 업데이트
UPDATE customer_master
SET 
    last_order_date = (
        SELECT MAX(order_date) FROM raw_order 
        WHERE customer_id = customer_master.customer_id
    ),
    updated_at = CURRENT_TIMESTAMP
WHERE 
    EXISTS (
        SELECT 1 FROM raw_order 
        WHERE customer_id = customer_master.customer_id
        AND order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
    );

-- 8. 주소 정보 업데이트
UPDATE raw_address
SET 
    is_primary = FALSE
WHERE 
    customer_id IN (
        SELECT customer_id FROM raw_address WHERE is_primary = TRUE GROUP BY customer_id HAVING COUNT(*) > 1
    )
    AND address_id != (
        SELECT address_id FROM raw_address ra2 
        WHERE ra2.customer_id = raw_address.customer_id 
        AND is_primary = TRUE LIMIT 1
    );

-- 9. 데이터 품질 로그 업데이트
UPDATE data_quality_log
SET 
    result = 'UPDATED',
    checked_at = CURRENT_TIMESTAMP
WHERE 
    table_name = 'customer_master'
    AND result = 'PENDING';

-- 10. 대량 INSERT를 통한 업데이트 (CTAS - CREATE TABLE AS SELECT)
INSERT OVERWRITE TABLE customer_master
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone,
    c.registration_date,
    c.country,
    MAX(o.order_date) as last_order_date,
    COUNT(DISTINCT o.order_id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as total_amount,
    c.created_at,
    CURRENT_TIMESTAMP as updated_at
FROM 
    customer_staging c
    LEFT JOIN raw_order o ON c.customer_id = o.customer_id AND o.order_status = 'COMPLETED'
WHERE 
    c.is_deleted = FALSE
GROUP BY 
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone,
    c.registration_date,
    c.country,
    c.created_at;

-- 11. 증분 업데이트 (Delta Lake 방식)
MERGE INTO customer_master t
USING (
    SELECT DISTINCT
        cs.customer_id,
        cs.customer_name,
        cs.email,
        cs.phone,
        cs.registration_date,
        cs.country,
        MAX(o.order_date) as last_order_date,
        COUNT(o.order_id) as total_orders,
        SUM(o.total_amount) as total_amount
    FROM customer_staging cs
    LEFT JOIN raw_order o ON cs.customer_id = o.customer_id
    WHERE cs.is_deleted = FALSE
    GROUP BY cs.customer_id, cs.customer_name, cs.email, cs.phone, cs.registration_date, cs.country
) s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN
    UPDATE SET
        t.customer_name = s.customer_name,
        t.email = s.email,
        t.phone = s.phone,
        t.last_order_date = s.last_order_date,
        t.total_orders = s.total_orders,
        t.total_amount = s.total_amount,
        t.updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, email, phone, registration_date, country, 
            last_order_date, total_orders, total_amount, created_at, updated_at)
    VALUES (s.customer_id, s.customer_name, s.email, s.phone, s.registration_date, s.country,
            s.last_order_date, s.total_orders, s.total_amount, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
