# Spark/Hive QL CRUD 쿼리 샘플

## 개요
이 디렉토리는 Spark SQL 및 Hive QL에서 사용 가능한 CRUD(Create, Read, Update, Delete) 작업의 샘플 쿼리들을 포함합니다.

## 파일 구성

### 1. **01_CREATE.hql** - 테이블 및 뷰 생성
데이터 웨어하우스 테이블 구조를 설정하는 DDL(Data Definition Language) 쿼리
- 원본 테이블 생성 (고객, 주소, 주문, 주문항목)
- 마스터 테이블 생성 (병합 대상)
- 분석 뷰 생성 (판매분석, 일일매출, 고객매출 등)
- 스테이징 테이블 생성 (데이터 로딩용)
- 데이터 품질 로그 테이블

### 2. **02_READ.hql** - 데이터 조회 및 분석
SELECT 기반의 데이터 분석 쿼리
- 고객 기본정보 조회
- 고객별 주문 이력 분석
- 일일 매출 통계
- 판매 분석 상세 뷰
- 고객 세분화 분석
- 상품별 판매 현황
- 지역별 매출 분석
- 결제 수단별 분석

### 3. **03_UPDATE.hql** - 데이터 수정 및 병합
UPDATE 및 MERGE 기반의 데이터 갱신 쿼리
- 기본 UPDATE: 고객 정보 수정
- MERGE INTO: 고객 마스터 병합 (INSERT/UPDATE)
- MERGE INTO: 주문 마스터 병합 (INSERT/UPDATE/DELETE)
- CASE 문을 이용한 조건부 UPDATE
- 배치 UPDATE: 고객 통계 업데이트
- 증분 UPDATE (Delta Lake 방식)

### 4. **04_DELETE.hql** - 데이터 삭제 및 정리
DELETE 기반의 데이터 제거 쿼리
- 기본 DELETE: 특정 레코드 삭제
- 조건부 DELETE: 오래된 데이터 삭제
- 관계 기반 DELETE: 고아 레코드 정리
- 중복 레코드 제거
- TRUNCATE: 테이블 완전 초기화
- Soft Delete: 논리적 삭제

## 데이터 모델

### 테이블 관계도
```
raw_customer ──┐
              ├─→ customer_master ──→ sales_analysis ──→ customer_revenue
raw_address ──┘                            ↓
                                     daily_sales_report
raw_order ───────────┐
                    ├─→ raw_order_item
                    │
                    └──→ order_staging (병합용)

customer_staging (병합용)

data_quality_log (모니터링용)
```

## 주요 기능

### CREATE (생성)
- **USING PARQUET**: 컬럼 지향 데이터 포맷으로 성능 최적화
- **PARTITIONED BY**: 파티션을 통한 쿼리 성능 개선
- **CLUSTERED BY**: 버킷팅을 통한 조인 성능 최적화
- **VIEW**: 복잡한 쿼리를 재사용 가능한 뷰로 정의

### READ (조회)
- **집계 함수**: COUNT, SUM, AVG, MIN, MAX 활용
- **윈도우 함수**: ROW_NUMBER() OVER를 이용한 순위 지정
- **조인**: INNER JOIN, LEFT JOIN 등 다양한 조인 방식
- **서브쿼리**: 복잡한 조건의 필터링

### UPDATE (수정)
- **간단한 UPDATE**: 특정 컬럼만 수정
- **조건부 UPDATE**: CASE 문을 이용한 조건부 수정
- **MERGE INTO**: 중복 제거 + 데이터 동기화
  - WHEN MATCHED: 기존 레코드 수정
  - WHEN NOT MATCHED: 새 레코드 삽입
  - WHEN MATCHED AND DELETE: 레코드 삭제

### DELETE (삭제)
- **기본 DELETE**: 조건에 맞는 레코드 삭제
- **관계 기반 DELETE**: 참조 무결성 확보
- **Soft Delete**: 실제 삭제 대신 플래그 업데이트
- **TRUNCATE**: 빠른 테이블 초기화

## 사용 방법

### Spark에서 실행
```bash
spark-sql -f 01_CREATE.hql
spark-sql -f 02_READ.hql
spark-sql -f 03_UPDATE.hql
spark-sql -f 04_DELETE.hql
```

### Hive에서 실행
```bash
hive -f 01_CREATE.hql
hive -f 02_READ.hql
hive -f 03_UPDATE.hql
hive -f 04_DELETE.hql
```

### PySpark에서 실행
```python
spark = SparkSession.builder.appName("CRUD_Sample").enableHiveSupport().getOrCreate()

# SQL 파일 읽기 및 실행
with open("01_CREATE.hql", "r") as f:
    queries = f.read().split(";")
    for query in queries:
        if query.strip():
            spark.sql(query)
```

## 중요 포인트

1. **파티션**: 대용량 데이터의 쿼리 성능 향상
2. **MERGE INTO**: 데이터 동기화 및 중복 처리의 효율적 방법
3. **윈도우 함수**: 복잡한 데이터 분석에 필수
4. **Soft Delete**: 데이터 복구 가능성 유지
5. **데이터 품질 모니터링**: 로그 테이블로 추적

## 주의사항

- 프로덕션 환경에서는 DELETE 전 반드시 백업 실행
- TRUNCATE는 복구 불가능하므로 신중히 사용
- MERGE INTO 사용 시 ON 조건이 정확한지 확인
- 대용량 데이터 처리 시 파티션 프루닝 확인

## 확장 가능성

이 샘플 쿼리들을 다음과 같이 확장할 수 있습니다:
- 복잡한 비즈니스 로직 추가
- 데이터 검증 및 정제 프로세스 포함
- 에러 처리 및 로깅 메커니즘 구현
- 성능 최적화 (인덱싱, 통계 수집)
- 증분 로드 (Incremental Load) 구현

---
**작성일**: 2024년 2월
**버전**: 1.0
