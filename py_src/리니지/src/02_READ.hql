-- ================================================================
-- READ 쿼리: 데이터 조회 및 분석
-- Spark/Hive SQL - 고객 관리 시스템
-- ================================================================

-- 1. 기본 고객 정보 조회
SELECT 
    customer_id,
    customer_name,
    email,
    phone,
    registration_date,
    country
FROM 
    customer_master
WHERE 
    customer_id IS NOT NULL
LIMIT 100;

-- 2. 고객별 주문 이력 조회
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    MAX(o.order_date) as last_order_date,
    MIN(o.order_date) as first_order_date
FROM 
    customer_master c
    LEFT JOIN raw_order o ON c.customer_id = o.customer_id
WHERE 
    c.country = 'KR'
GROUP BY 
    c.customer_id,
    c.customer_name,
    c.email
ORDER BY 
    total_spent DESC;

-- 3. 일일 매출 분석
SELECT 
    sales_date,
    total_orders,
    unique_customers,
    daily_revenue,
    avg_order_amount,
    min_order_amount,
    max_order_amount,
    ROUND(daily_revenue / unique_customers, 2) as revenue_per_customer
FROM 
    daily_sales_report
WHERE 
    sales_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE, 30), 'yyyy-MM-dd')
ORDER BY 
    sales_date DESC;

-- 4. 판매 분석 상세 뷰
SELECT 
    customer_id,
    customer_name,
    email,
    order_id,
    order_date,
    total_amount,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price as calculated_total,
    purchase_rank
FROM 
    sales_analysis
WHERE 
    purchase_rank <= 5
ORDER BY 
    customer_id,
    purchase_rank;

-- 5. 고객 매출 집계 조회
SELECT 
    customer_id,
    customer_name,
    order_count,
    total_revenue,
    avg_order_value,
    last_purchase_date,
    first_purchase_date,
    customer_lifetime_days,
    CASE 
        WHEN total_revenue >= 100000 THEN 'VIP'
        WHEN total_revenue >= 50000 THEN 'PREMIUM'
        WHEN total_revenue >= 10000 THEN 'REGULAR'
        ELSE 'NEW'
    END as customer_segment
FROM 
    customer_revenue
WHERE 
    order_count > 0
ORDER BY 
    total_revenue DESC;

-- 6. 상품별 판매 현황
SELECT 
    oi.product_id,
    COUNT(DISTINCT oi.order_id) as total_sales,
    SUM(oi.quantity) as total_quantity,
    SUM(oi.item_total) as total_revenue,
    AVG(oi.unit_price) as avg_price,
    MIN(oi.unit_price) as min_price,
    MAX(oi.unit_price) as max_price
FROM 
    raw_order_item oi
    INNER JOIN raw_order o ON oi.order_id = o.order_id
WHERE 
    o.order_status = 'COMPLETED'
GROUP BY 
    oi.product_id
ORDER BY 
    total_revenue DESC;

-- 7. 지역별 매출 분석
SELECT 
    c.country,
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    ROUND(SUM(o.total_amount) / COUNT(DISTINCT c.customer_id), 2) as revenue_per_customer
FROM 
    customer_master c
    LEFT JOIN raw_order o ON c.customer_id = o.customer_id
GROUP BY 
    c.country
ORDER BY 
    total_revenue DESC;

-- 8. 결제 수단별 분석
SELECT 
    payment_method,
    COUNT(DISTINCT order_id) as transaction_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_amount) as total_amount,
    AVG(total_amount) as avg_amount,
    MIN(total_amount) as min_amount,
    MAX(total_amount) as max_amount
FROM 
    raw_order
WHERE 
    payment_method IS NOT NULL
    AND order_status IN ('COMPLETED', 'PENDING')
GROUP BY 
    payment_method
ORDER BY 
    transaction_count DESC;

-- 9. 고객 세분화 분석
SELECT 
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, registration_date) <= 30 THEN 'NEW'
        WHEN DATEDIFF(CURRENT_DATE, registration_date) <= 90 THEN 'ACTIVE'
        WHEN DATEDIFF(CURRENT_DATE, registration_date) <= 365 THEN 'LOYAL'
        ELSE 'DORMANT'
    END as customer_type,
    COUNT(*) as customer_count,
    ROUND(AVG(total_orders), 2) as avg_orders,
    ROUND(AVG(total_amount), 2) as avg_amount
FROM 
    customer_master
GROUP BY 
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, registration_date) <= 30 THEN 'NEW'
        WHEN DATEDIFF(CURRENT_DATE, registration_date) <= 90 THEN 'ACTIVE'
        WHEN DATEDIFF(CURRENT_DATE, registration_date) <= 365 THEN 'LOYAL'
        ELSE 'DORMANT'
    END;

-- 10. 주문 상태별 통계
SELECT 
    order_status,
    COUNT(*) as order_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_amount) as total_amount,
    ROUND(AVG(total_amount), 2) as avg_amount,
    MIN(order_date) as earliest_date,
    MAX(order_date) as latest_date
FROM 
    raw_order
GROUP BY 
    order_status
ORDER BY 
    order_count DESC;

-- 11. 데이터 품질 검증 조회
SELECT 
    table_name,
    check_type,
    check_description,
    result,
    record_count,
    error_count,
    ROUND(((record_count - error_count) / record_count) * 100, 2) as quality_percentage,
    checked_at
FROM 
    data_quality_log
WHERE 
    checked_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 7 DAY)
ORDER BY 
    checked_at DESC;
