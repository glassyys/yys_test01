## python3 sql_est_srctgt_003.py /home/p190872/chksrc/SIDHUB
## python3 sql_est_srctgt_003.py /NAS/MIDP/DBMSVC/MIDP/SID/SRC/SIDHUB
## python3 sql_est_srctgt_003.py /NAS/MIDP/DBMSVC/MIDP/SID
## python3 sql_est_srctgt_003.py /NAS/MIDP/DBMSVC/MIDP/TMT
## python3 sql_est_srctgt_003.py /NAS/MIDP/DBMSVC/MIDP/TDIA
## python3 sql_est_srctgt_003.py /NAS/MIDP/DBMSVC/MIDP/TDM
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
## crud_type,abs_path,file,full_path 항목은 동일하게 생성하면서
## 추출된 queries 의 queries.append(query) 으로 처리된 최종 쿼리의 
## MAIN_QUERY_START값의 키워드에서  ";"까지의 내용에서 아래 로직 반영하여 
## 소스와 타겟테이블정보를 추출하는 로직으로 구현해서 csv 파일로 생성하는 전체소스
## 1. 키워드 기준 queries(queries.append(query)) 내용에 대해서 SQL구조를 분석하여
## MAIN_QUERY_START의 값은 sql_typ항목과 source_table, target_table 구분해서 csv파일을 생성하는 로직으로 변경
## 2. target_table에 대한 source_table은 여러행이 생성될수 있는 구조로 처리하여 target_table은 중복해서 생성되어도 됨
## 3. 최종 csv레이아웃
## crud_type,abs_path,file,full_path,sql_typ,source_table,target_table
## 4.csv파일 구분자는 ","로 생성
## 5. 키워드 기준 queries(queries.append(query)) 내용 분석시 아래내용 반영
## 1) 서브쿼리 내부 테이블 추출 강화
## 2) WITH 절 분석
## 3) CTAS 구조 완전 분석
## 4) MERGE USING 서브쿼리 완전 파싱
## 6. 기존 출력부분은 출력가능한 부분 유지
## 기존 구조(전처리 / MAIN_QUERY_START / queries.append(query) 흐름)는 그대로 유지하면서,
## 
## queries.append(query) 로 완성된 최종 쿼리 단위를 분석하여
## sql_typ, source_table, target_table 을 추출하고
## CSV 레이아웃
## crud_type,abs_path,file,full_path,sql_typ,source_table,target_table
## 형태로 생성하도록 전체 소스 재구성했습니다.
## 요구사항 반영 내용:
## 서브쿼리 내부 테이블 추출 강화
## WITH 절 분석 (CTE 내부 FROM/JOIN 포함)
## CTAS 완전 분석
## MERGE USING 서브쿼리 완전 파싱
## target 기준 source 다건 생성 허용
## 예약어는 절대 테이블로 사용 안 함
## "END", "COMMIT" 제외
## 구분자 "," (CSV)
## 기존 출력부 유지
## 전체 수정 소스
## 
## 기존로직 유지하며 추출된 테이블명에서 스키마명(예: DBNAME.TABLE)을 분리하여 별도 컬럼으로 생성하고
## 특정 데이터베이스(Oracle, Hive, MySQL 등)의 특수한 구문 파싱을 더 보강해서 전체쿼리생성
## 결과소스 로직유지하고 추가적으로 
## "FROM DUAL"으로 구성된 단독쿼리와 "ALTER"키워드로 시작되는 경우는 테이블이 없으면 소스테이블 및 타켓추출은 안해도 되고
## "ALTER TABLE" 와 "TRUNCATE TABLE" 다음에 있는 테이블은 타겟테이블로 추출되어야 하는 로직으로 
## "DELETE
## FROM 테이블" 구조에서 from절에 있는 테이블도 추출되도록 수정한 전체소스 구현
## "insert into" 와 "insert overwrite" 의 경우 타겟테이블이 "into"와 "overwrite"로 추출되는데 "insert into"이나 "insert overwrite" 다음에 있는 단어가 타켓테이블로 추출되어야 하고
## "with (select col from table_1 ) update set a=b from table_2 where c=d " 의 경우
## update 다음의 단어가 타겟테이블이과 select 절의 from table_1 이나 set 다음 from table_2 에서 from 다음단어가 소스테이블로 인식하도록 보완수정
## "drop index" 인경우 추출에서 제외하도록 수정한 전체소스로 나머지 로직은 유지
## 
## 2가지 보완
## "select
## from aa_tb a,
## bb_tb b"
## 쿼리에서 aa_tb 만 추출되는데 bb_tb 도 추출되도록 소스 보완
## "delete
## from tb_1 a"
## 구분에서 tb_1 로 테이블로 추출되도록 소스 보완
## 위 두가지 내용 보완한 전체소스
## 보완 내용 정리
## 1. FROM aa_tb a, bb_tb b 인 경우
## 기존: aa_tb 만 추출됨
## 개선: FROM 절 이후 구간을 잘라서 "," 기준으로 분리하여
## aa_tb, bb_tb 모두 추출되도록 수정
## 단, SELECT 절의 컬럼 , 구분은 절대 추출되지 않도록
## → FROM ~ (WHERE/GROUP/ORDER/...) 까지만 분석
## 2.DELETE FROM tb_1 a
## 기존: DELETE 문이 query로 잡히지만 파일 생성 안됨
## 개선: DELETE FROM 패턴을 명확히 타겟으로 추출
## tb_1 이 target_table 로 정상 기록되도록 수정
##
## 지난번에도 그랬는데 이 로직 보완요청하면 
## select 쿼리내 a.col_1 인경우 a 가 소스스키마 col_1을 테이블로 인지해서 많은행이 추가되는 현상이 발생하는데 
## 이부분 보완한 전체로직

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
# 5. SQL TYPE
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
# 7. FROM 절만 안전 추출 (핵심 수정)
# ==============================

def extract_sources_recursive(query):

    sources = set()

    # FROM ~ 다음 구간만 추출
    from_blocks = re.findall(
        r'\bFROM\b(.*?)(WHERE|GROUP|ORDER|HAVING|UNION|ON|WHEN|$)',
        query,
        re.IGNORECASE | re.DOTALL
    )

    for block, _ in from_blocks:

        # 인라인뷰 제거
        block = re.sub(r'\(.*?\)', ' ', block, flags=re.DOTALL)

        # JOIN 분리
        tokens = re.split(r'\bJOIN\b|,', block, flags=re.IGNORECASE)

        for token in tokens:
            tbl = clean_table(token)
            if tbl:
                sources.add(tbl)

    return sources


# ==============================
# 8. 타겟 추출
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
# 9. 메인
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

                    sources = extract_sources_recursive(query)
                    targets = extract_target_tables(query)

                    if not sources and not targets:
                        continue

                    if not sources:
                        sources = {None}
                    if not targets:
                        targets = {None}

                    for tgt in targets:
                        for src in sources:
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

    if total_output_rows == 0:
        print("====================================")
        print(" SQL 추출 완료 (이미지 오류 수정판)")
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
    print(" SQL 추출 완료 (이미지 오류 수정판)")
    print("====================================")
    print(f"출력 파일 위치    : {output_path}")
    print(f"처리 파일 건수    : {total_files}")
    print(f"추출 쿼리 건수    : {total_queries}")
    print(f"생성 행 건수      : {total_output_rows}")
    print(f"전체 파일 라인 수 : {total_file_lines}")
    print("====================================")


if __name__ == "__main__":
    main()