## python3 sql_est_srctgt_002.py /home/p190872/chksrc/SIDHUB
## python3 sql_est_srctgt_002.py /NAS/MIDP/DBMSVC/MIDP/SID/SRC/SIDHUB
## python3 sql_est_srctgt_002.py /NAS/MIDP/DBMSVC/MIDP/SID
## python3 sql_est_srctgt_002.py /NAS/MIDP/DBMSVC/MIDP/TMT
## python3 sql_est_srctgt_002.py /NAS/MIDP/DBMSVC/MIDP/TDIA
## python3 sql_est_srctgt_002.py /NAS/MIDP/DBMSVC/MIDP/TDM
## 파이썬으로 쿼리추출 소스코드 요청
## 1.추출 대상 확장자
## .sh
## .hql
## .sql
## .uld
## .ld
## 2. 추출 대상 쿼리 유형
## CREATE
## CREATE VIEW
## CREATE TEMPORARY VIEW
## ALTER VIEW
## REPLACE VIEW
## MERGE
## UPSERT
## INSERT
## UPDATE
## DELETE
## SELECT
## CTAS (CREATE TABLE AS)
## -세미콜론(;) 기준으로 완전 문장 추출
## 
## 3. dat 생성 규칙
## 저장 위치:
## 현재실행디렉토리/out/
## 파일명:
## 프로그램명_소스마지막디렉토리_YYYYMMDD_HHMMSS.dat
## 예:
## sql_extractor_projectA_20260215_091530.dat
## 
## 4. dat 컬럼 레이아웃 및 구분자
## 구분자 "|"로path는 절대경로로 아래와 같은 레이아웃으로 생서
## file_path|file_name|path_with_file|query_text
## 
## -파일경로/파일명은 쿼리문 행개수만큼 중복 생성됨
## 
## 파일생성시 아래쿼리의 경우 
## select col_1
##       ,col_2
##  from tb_1
## where a='1'
## 아래와 같은으로 생성하도록 쿼리 보완 전체코드 요청
## file_path|file_name|path_with_file|select col_1
## file_path|file_name|path_with_file|      ,col_2
## file_path|file_name|path_with_file| from tb_1
## file_path|file_name|path_with_file|where a='1'
## 
## 쿼리 유형 컬럼 추가 (C/R/U/D 자동분류)하고 파일 건수 결과도 출력하도록 수정한 전체소스
## 
## 아래 단계로 확장요청
## WITH 절 포함 강화 정규식으로 보완하고
## 파일의 전체행건수도 추가로 출력하는 소스코드 전체 요청
## 
## 아래 단계로 확장요청
## WITH 절 포함 강화 정규식으로 보완하고
## 파일의 전체행건수도 추가로 출력하는 소스코드 전체 요청
## 
## 소스유지하고 구분자만 "^"으로 변경한 전체소스
##
## 기존소스 유지하고 세미콜론(;) 기준 완전 문장 추출할때 ; 도 쿼리문생성시 포함해서 추출한 전체소스
##
## 기존소스 유지하고 쿼리문의 ";"도 "query_text" 항목에 추가하여 파일생성하도록 수정한 전체소스
##
## 기존소스 유지하고 파일명만 아래기준으로 수정한 전체소스
## dat 생성 규칙
## 저장 위치:
## 현재실행디렉토리/out/
## 파일명:
## 프로그램명_소스마지막디렉토리_YYYYMMDD_HHMMSS.dat
## 예:
## sql_extractor_projectA_20260215_091530.dat
## 
## 파일명은 예시처럼이 아닌 
## 프로그램명_소스마지막디렉토리_YYYYMMDD_HHMMSS.dat
## 의 형태인 실행프로그램명과 쿼리소스의마지막디렉토리명으로 변경한 전체소스 다시 생성
## 
## 기존소스 유지하고 아래내용 반영한 전체소스
## WITH 다중 CTE 완전 강화 버전
## 주석 제거 후 추출
## 
## 기존소스 유지하고 아래내용 반영한 전체소스
## WITH 다중 CTE 완전 강화 버전
## 주석 제거 후 추출
## 
## 기존소스 유지하고 아래 문장이 들어간 쿼리는 대문자 포함하여 모두 제거한 수정한 전체소스
## "insert into sidtest.ad1901_rgb_ac190212_svc(svc_mgmt_num)"
## "dual" 
## 
## ■요청 : 기존 소스 유지하고 아래 내용 반영한 전체소스
## 1. MAIN_QUERY_START 에 아래 명령시작 쿼리구문 추가추출
## ALTER, DROP, TRUNCATE, RENAME, DECLARE, BEGINE, EXECUTE, COMMIT, ENT
## 2./* 과 */ 사이의 주석은 제외
## 3. 첫번째 단어가 "#" 인 행 제외
## 
## 추가 
## 기존 소스 유지하고 첫단어가 "/*" 으로 시작하고 "*/"으로 끝나는 주석제외한 로직 반영한 전체소스
## DBMS_OUTPUT로 시작되는 라인 모두제외한 전체소스
## 
## 아래 원본소스 로직 유지하고 (참고 : D:\파이썬\리니지\쿼리추출_007.txt)
## 아래 쿼리 형식도 쿼리로 추출되도록 소스 보완한 전체소스
## merge /* hint */
##     into
## using
## on
## 
## update
## 
## ;
## 
## 
## merge구문 처음과 끝부분 첨부합니다. 이 쿼리가 안 잡혀요 기존로직유지후 보완수정된 전체소스
## commit 키워드로 시작되는 쿼리는 제외한 전체소스
## end if 는 제외 end 키워드는 유지하도록 수정한 전체소스
## 방금 생성한 전체소스에 "alter" 제외 "alter table" 키워드는 포함하는 전체 수정 소스
## 
## sql_est_008.py 소스에서 1,2번요청 관련 로직만 수정하고 나머지는 로직 유지하고 수정한 전체소스
## 1. MAIN_QUERY_START 에서 "ALTER" 은 제외하고 "ALTER TABLE" 형식을 추가하여 적용
## 2. 쿼리추출시 "DUAL" 이 쿼리에 있는 것은 모두제외 했는데 이 로직을 "FROM DUAL"만 있는 경우 제외하는 로직으로 변경
## FROM DUAL 단독 쿼리만 제외하는 로직으로 변경한 전체소스 단,기존 나머지로직은 유지
## RENAME 키워드 수동제외
## 
## #소스타겟추출
## 아래 원본소스에서 파일생성전 
## 추출된 queries 의 queries.append(query) 으로 처리된 최종 쿼리의
## MAIN_QUERY_START값의 키워드에서  ";"까지의 내용에서 아래 로직 반영하여 
## 소스와 타겟테이블정보를 추출하는 로직을 기존 로직 최대한 출력문 구조도 유지해서 수정하여 
## 구분자는 ","인 csv 파일로 생성하는 전체소스
## 
## crud_type,abs_path,file,full_path 항목은 동일하게 생성하면서 소스타겟테이블 정보를 추출
## 
## 로직구현시 내용 적용하여 수정요청
## 1. 주요목적은 키워드 기준 queries(queries.append(query)) 내용에 대해서 SQL구조를 분석하여
## MAIN_QUERY_START의 값은 sql_typ항목과 source_table, target_table 구분해서 .csv파일을 생성하는 로직으로 변경하는것 
## 
## 2. target_table에 대한 source_table은 여러행이 생성될수 있는 구조로 처리하여 target_table은 중복해서 생성되어도 됨
## 
## 3. 최종 csv레이아웃
## crud_type,abs_path,file,full_path,sql_typ,source_table,target_table
## 
## 4. queries(queries.append(query)) 내용에서 소스와 타겟테이블을 추출할때 아래 1) 에서 8) 까지 내용 기준적용
## 1)구문 타입,추출 위치 (Target Table),비고
## INSERT,INSERT INTO 또는 INSERT OVERWRITE 또는 INSERT OVERWRITE TABLE 뒤에 위치
## CREATE,CREATE TABLE 또는 CREATE VIEW 뒤,CTAS의 경우 소스는 FROM 뒤에 위치
## MERGE,MERGE INTO 바로 뒤,"Oracle, Teradata 등에서 사용"
## REPLACE,REPLACE INTO 뒤,MySQL 등에서 사용
## 
## 2) 구문 타입,추출 위치 (Source Table),비고
## SELECT,FROM 뒤 또는 JOIN 뒤,"콤마(,)로 구분된 다중 테이블 주의"
## USING,USING 뒤,MERGE 구문이나 일부 JOIN에서 발생
## SUBQUERY,괄호 ( 안의 FROM 뒤,계층적 구조 분석 필요
## 
## 3) 구문 타입,추출 위치,구분
## UPDATE,UPDATE 키워드 바로 뒤,Target 테이블
## UPDATE FROM,FROM 또는 JOIN 뒤,Source 테이블 (참조용)
## ALTER,ALTER TABLE 뒤,Target (스키마 구조 변경)
## 
## 4) 구문 타입,추출 위치,구분
## DELETE,DELETE FROM 뒤,Target 테이블
## TRUNCATE,TRUNCATE TABLE 뒤,Target 테이블
## DROP,DROP TABLE 뒤,Target 테이블
## 
## 5) 파싱 시 주의해야 할 특수 케이스
## 테이블 별칭 (Alias)
## FROM user_table a 에서 a는 테이블이 아닙니다. 
## 
## 6) 공백이나 AS 키워드 뒤의 단어는 무시해야 합니다.
## 7) WITH 절 (CTE)
##    WITH tmp AS (...) 에서 tmp는 물리 테이블이 아닌 임시 뷰입니다. 소스 테이블 목록에서 제외하는 로직이 필요합니다.
## 8) 함수 및 서브쿼리
##   FROM TABLE(my_func()) 처럼 함수가 위치하거나 FROM (SELECT ...) 처럼 서브쿼리가 오는 경우 테이블 추출 대상에서 제외하거나 재귀적으로 분석해야 합니다.
## 
## 5.소스타켓테이블 추출시 "END", "COMMIT"이나 행은 제외
## 
## 6. 테이블이 될 수 없는 아래 예약어를 소스 타겟에 사용하지 말고 로직 구현
##     "SET", "WHERE", "AND", "OR", "ON", "WHEN",
##     "THEN", "ELSE", "VALUES", "SELECT",
##     "UPDATE", "INSERT", "DELETE", "MERGE",
##     "USING", "FROM", "JOIN", "INTO",
##     "GROUP", "ORDER", "BY", "HAVING"
## 7. "from dual" 단독구분 소스 타겟 추출시 제외
## 추가
## substring(a.col from 1 for 8) 처럼
## 함수 괄호 내부의 from 1 이
## source_table 로 추출되지 않도록 보완
#!/usr/bin/env python3
# 실행방법:
# python3 프로그램명.py 절대경로포함소스디렉토리

#!/usr/bin/env python3

import os
import re
import sys
import csv
from datetime import datetime

# ==============================
# 1. 파라미터 체크
# ==============================

if len(sys.argv) != 2:
    print("사용법: python3 프로그램명.py 절대경로포함소스디렉토리")
    sys.exit(1)

SOURCE_DIR = os.path.abspath(sys.argv[1])

if not os.path.isdir(SOURCE_DIR):
    print("오류: 유효한 디렉토리가 아닙니다.")
    sys.exit(1)

# ==============================
# 2. 설정
# ==============================

TARGET_EXTENSIONS = ('.sh', '.hql', '.sql', '.uld', '.ld')
DELIMITER = ","
EXCLUDE_TABLE = "sidtest.ad1901_rgb_ac190212_svc"

RESERVED_WORDS = {
    "SET","WHERE","AND","OR","ON","WHEN","THEN","ELSE",
    "VALUES","SELECT","UPDATE","INSERT","DELETE","MERGE",
    "USING","FROM","JOIN","INTO","GROUP","ORDER","BY",
    "HAVING","TABLE","OVERWRITE","POSITION","SUBSTRING",
    "CAST","TRIM","COUNT","SUM","MAX","MIN","AVG",
    "SESSION"
}

ONLY_FROM_DUAL_PATTERN = re.compile(
    r'^\s*SELECT\s+.*?\s+FROM\s+DUAL\s*;?\s*$',
    re.IGNORECASE | re.DOTALL
)

MAIN_QUERY_START = re.compile(
    r'\b(CREATE|INSERT|UPDATE|DELETE|MERGE|REPLACE|ALTER|DROP|TRUNCATE|SELECT|WITH)\b',
    re.IGNORECASE
)

# ==============================
# 3. 전처리
# ==============================

def preprocess(content):

    content = "\n".join(
        line for line in content.splitlines()
        if not line.lstrip().startswith("#")
    )

    content = re.sub(r'--.*', '', content)
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

    return content


# ==============================
# 4. depth 기반 쿼리 추출
# ==============================

def extract_queries_from_file(file_path):

    queries = []
    total_lines = 0

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            raw = f.read()

        total_lines = raw.count("\n") + 1
        content = preprocess(raw)

        pos = 0
        length = len(content)

        while pos < length:

            match = MAIN_QUERY_START.search(content, pos)
            if not match:
                break

            start = match.start()
            end = start
            depth = 0
            in_string = False
            quote_char = None

            while end < length:
                ch = content[end]

                if ch in ("'", '"'):
                    if not in_string:
                        in_string = True
                        quote_char = ch
                    elif quote_char == ch:
                        in_string = False

                elif not in_string:
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth = max(depth - 1, 0)
                    elif ch == ";" and depth == 0:
                        end += 1
                        break

                end += 1

            query = content[start:end].strip()

            if query:
                if EXCLUDE_TABLE.lower() in query.lower():
                    pos = end
                    continue
                if ONLY_FROM_DUAL_PATTERN.match(query):
                    pos = end
                    continue
                queries.append(query)

            pos = end

    except Exception:
        pass

    return queries, total_lines


# ==============================
# 5. SQL TYPE (WITH 내부 DML 감지)
# ==============================

def detect_real_sql_type(query):

    q = query.strip().upper()

    if not q.startswith("WITH"):
        return q.split()[0] if q.split() else "UNKNOWN"

    if re.search(r'\bINSERT\b', q):
        return "INSERT"
    if re.search(r'\bUPDATE\b', q):
        return "UPDATE"
    if re.search(r'\bDELETE\b', q):
        return "DELETE"
    if re.search(r'\bMERGE\b', q):
        return "MERGE"

    return "SELECT"


def classify_sql_type(sql_typ):

    if sql_typ in ("CREATE","INSERT","MERGE","REPLACE"):
        return "C"
    elif sql_typ == "SELECT":
        return "R"
    elif sql_typ in ("UPDATE","ALTER"):
        return "U"
    elif sql_typ in ("DELETE","DROP","TRUNCATE"):
        return "D"
    return "R"


# ==============================
# 6. 테이블 정제
# ==============================

def clean_table(name):

    if not name:
        return None

    name = name.strip()
    name = re.split(r'\s+', name)[0]
    name = name.rstrip(";,")
    name = name.replace("(", "").replace(")", "")

    upper = name.upper()

    if (not name or
        upper in RESERVED_WORDS or
        upper == "DUAL" or
        name.isdigit() or
        re.match(r'^\d', name)):
        return None

    return name


# ==============================
# 7. CTE 분석
# ==============================

def extract_cte_map(query):

    cte_map = {}

    if not query.strip().upper().startswith("WITH"):
        return cte_map

    pattern = re.compile(
        r'(\w+)\s+AS\s*\((.*?)\)\s*(,|SELECT)',
        re.IGNORECASE | re.DOTALL
    )

    for match in pattern.finditer(query):
        alias = match.group(1)
        subquery = match.group(2)
        sources = extract_sources_recursive(subquery)
        cte_map[alias.upper()] = sources

    return cte_map


# ==============================
# 8. 소스 추출 (인라인뷰 포함 재귀)
# ==============================

def extract_sources_recursive(query):

    sources = set()

    # FROM/JOIN 대상
    pattern = re.compile(r'\b(FROM|JOIN|USING)\s+([^\s,(]+)', re.IGNORECASE)

    for match in pattern.finditer(query):

        token = match.group(2)

        if token.startswith("("):
            continue

        tbl = clean_table(token)
        if tbl:
            sources.add(tbl)

    return sources


# ==============================
# 9. 타겟 추출
# ==============================

def extract_target_tables(query):

    targets = set()

    patterns = [
        r'INSERT\s+OVERWRITE\s+TABLE\s+([^\s(]+)',
        r'INSERT\s+INTO\s+([^\s(]+)',
        r'CREATE\s+(?:TABLE|VIEW)\s+([^\s(]+)',
        r'UPDATE\s+([^\s(]+)',
        r'DELETE\s+FROM\s+([^\s(]+)',
        r'MERGE\s+INTO\s+([^\s(]+)',
        r'ALTER\s+TABLE\s+([^\s(]+)',
        r'DROP\s+TABLE\s+([^\s(]+)',
        r'TRUNCATE\s+TABLE\s+([^\s(]+)'
    ]

    for pat in patterns:
        for match in re.finditer(pat, query, re.IGNORECASE):
            tbl = clean_table(match.group(1))
            if tbl:
                targets.add(tbl)

    return targets


# ==============================
# 10. 메인
# ==============================

def main():

    total_files = 0
    total_queries = 0
    total_output_rows = 0
    total_file_lines = 0

    program_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    source_last_dir = os.path.basename(SOURCE_DIR.rstrip(os.sep))

    out_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(out_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{program_name}_{source_last_dir}_{timestamp}.csv"
    output_path = os.path.join(out_dir, output_filename)

    rows_buffer = []

    for root, _, files in os.walk(SOURCE_DIR):

        for file in files:
            if file.lower().endswith(TARGET_EXTENSIONS):

                total_files += 1

                full_path = os.path.join(root, file)
                abs_path = os.path.abspath(root)

                queries, file_lines = extract_queries_from_file(full_path)

                total_file_lines += file_lines
                total_queries += len(queries)

                for query in queries:

                    sql_typ = detect_real_sql_type(query)
                    crud_type = classify_sql_type(sql_typ)

                    cte_map = extract_cte_map(query)
                    sources = extract_sources_recursive(query)
                    targets = extract_target_tables(query)

                    real_sources = set()

                    for s in sources:
                        if s and s.upper() in cte_map:
                            real_sources.update(cte_map[s.upper()])
                        else:
                            if s:
                                real_sources.add(s)

                    if not real_sources and not targets:
                        continue

                    if not real_sources:
                        real_sources = {None}
                    if not targets:
                        targets = {None}

                    for tgt in targets:
                        for src in real_sources:
                            rows_buffer.append([
                                crud_type,
                                abs_path,
                                file,
                                full_path,
                                sql_typ,
                                src,
                                tgt
                            ])
                            total_output_rows += 1

    # 🔥 소스/타겟 없는 파일은 생성 제외
    if total_output_rows == 0:
        print("====================================")
        print(" WITH 내부 DML 감지 포함 SQL 추출 완료")
        print("====================================")
        print("CSV 파일 생성 대상이 없습니다.")
        print("====================================")
        return

    with open(output_path, 'w', newline='', encoding='utf-8') as out_file:
        writer = csv.writer(out_file, delimiter=DELIMITER)
        writer.writerow([
            "crud_type","abs_path","file","full_path",
            "sql_typ","source_table","target_table"
        ])
        writer.writerows(rows_buffer)

    print("====================================")
    print(" WITH 내부 DML 감지 포함 SQL 추출 완료")
    print("====================================")
    print(f"CSV 파일 위치        : {output_path}")
    print(f"처리 파일 건수        : {total_files}")
    print(f"추출 쿼리 건수        : {total_queries}")
    print(f"생성 행 건수          : {total_output_rows}")
    print(f"전체 파일 총 행 건수  : {total_file_lines}")
    print("====================================")


if __name__ == "__main__":
    main()