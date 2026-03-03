-- ================================================================
-- CREATE 쿼리: 테이블 및 뷰 생성
-- Spark/Hive SQL - 고객 관리 시스템
-- ================================================================

-- 1. 원본 테이블 생성
CREATE TABLE IF NOT EXISTS raw_customer (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone STRING,
    registration_date TIMESTAMP,
    country STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
USING PARQUET
PARTITIONED BY (country)
CLUSTERED BY (customer_id) INTO 10 BUCKETS;

-- 2. 주소 테이블 생성
CREATE TABLE IF NOT EXISTS raw_address (
    address_id STRING,
    customer_id STRING,
    street_address STRING,
    city STRING,
    state STRING,
    postal_code STRING,
    country STRING,
    is_primary BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
USING PARQUET
PARTITIONED BY (country);

-- 3. 주문 테이블 생성
CREATE TABLE IF NOT EXISTS raw_order (
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    order_status STRING,
    total_amount DECIMAL(10, 2),
    payment_method STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
USING PARQUET
PARTITIONED BY (order_date);

-- 4. 주문 항목 테이블 생성
CREATE TABLE IF NOT EXISTS raw_order_item (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DECIMAL(10, 2),
    item_total DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
USING PARQUET;

-- 5. 고객 마스터 테이블 생성 (병합 대상)
CREATE TABLE IF NOT EXISTS customer_master (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone STRING,
    registration_date TIMESTAMP,
    country STRING,
    last_order_date TIMESTAMP,
    total_orders INT,
    total_amount DECIMAL(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
USING PARQUET
PARTITIONED BY (country)
TBLPROPERTIES ('orc.stripe.size'='67108864', 'orc.compress'='SNAPPY');

-- 6. 판매 분석 뷰 생성
CREATE OR REPLACE VIEW sales_analysis AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    o.order_id,
    o.order_date,
    o.total_amount,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.order_date DESC) as purchase_rank
FROM 
    customer_master c
    LEFT JOIN raw_order o ON c.customer_id = o.customer_id
    LEFT JOIN raw_order_item oi ON o.order_id = oi.order_id
WHERE 
    o.order_status IN ('COMPLETED', 'PENDING');

-- 7. 일일 매출 리포트 뷰 생성
CREATE OR REPLACE VIEW daily_sales_report AS
SELECT 
    DATE_FORMAT(order_date, 'yyyy-MM-dd') as sales_date,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_amount) as daily_revenue,
    AVG(total_amount) as avg_order_amount,
    MIN(total_amount) as min_order_amount,
    MAX(total_amount) as max_order_amount
FROM 
    raw_order
WHERE 
    order_status = 'COMPLETED'
GROUP BY 
    DATE_FORMAT(order_date, 'yyyy-MM-dd');

-- 8. 고객 매출 집계 뷰 생성
CREATE OR REPLACE VIEW customer_revenue AS
SELECT 
    customer_id,
    customer_name,
    COUNT(DISTINCT order_id) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    MAX(order_date) as last_purchase_date,
    MIN(order_date) as first_purchase_date,
    DATEDIFF(MAX(order_date), MIN(order_date)) as customer_lifetime_days
FROM 
    sales_analysis
GROUP BY 
    customer_id,
    customer_name;

-- 9. 스테이징 테이블 생성 (MERGE용)
CREATE TABLE IF NOT EXISTS customer_staging (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone STRING,
    registration_date TIMESTAMP,
    country STRING,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING PARQUET;

CREATE TABLE IF NOT EXISTS order_staging (
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    order_status STRING,
    total_amount DECIMAL(10, 2),
    payment_method STRING,
    is_deleted BOOLEAN DEFAULT FALSE
)
USING PARQUET;

-- 10. 검증 테이블 생성
CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id STRING,
    table_name STRING,
    check_type STRING,
    check_description STRING,
    result STRING,
    record_count INT,
    error_count INT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
USING PARQUET
PARTITIONED BY (table_name);

-- 테이블 목록 확인
SHOW TABLES;

-- 테이블 상세 정보 확인
DESCRIBE FORMATTED customer_master;
DESCRIBE FORMATTED sales_analysis;
