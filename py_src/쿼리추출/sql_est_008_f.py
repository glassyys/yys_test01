## python3 sql_est_008_f01.py /home/p190872/chksrc/SIDHUB/sid/msid_cntr_d_rgn_s_01.sql
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
## 변경 요구사항
## 기존 로직 100% 유지
## 파라미터로 디렉토리 대신 "절대경로+파일명" 입력
## 해당 파일 1개만 기준으로 쿼리 추출
## 생성 파일명은 입력파일명.dat
## 저장 위치는 기존과 동일:
## 현재실행디렉토리/out/
##
#!/usr/bin/env python3
# 실행방법:
# python3 프로그램명.py 절대경로포함파일명

import os
import re
import sys
from datetime import datetime

# ==============================
# 1. 파라미터 체크
# ==============================

if len(sys.argv) != 2:
    print("사용법: python3 프로그램명.py 절대경로포함파일명")
    sys.exit(1)

SOURCE_FILE = os.path.abspath(sys.argv[1])

if not os.path.isfile(SOURCE_FILE):
    print("오류: 유효한 파일이 아닙니다.")
    sys.exit(1)

# ==============================
# 2. 설정
# ==============================

TARGET_EXTENSIONS = ('.sh', '.hql', '.sql', '.uld', '.ld')
DELIMITER = "^"

EXCLUDE_PATTERNS = [
    "insert into sidtest.ad1901_rgb_ac190212_svc(svc_mgmt_num)",
    "dual"
]

# 확장자 체크
if not SOURCE_FILE.lower().endswith(TARGET_EXTENSIONS):
    print("오류: 대상 확장자가 아닙니다.")
    sys.exit(1)

# ==============================
# 3. 전처리
# ==============================

def preprocess(content):

    content = "\n".join(
        line for line in content.splitlines()
        if not line.lstrip().startswith("#")
    )

    content = "\n".join(
        line for line in content.splitlines()
        if not re.match(r'(?i)^\s*DBMS_OUTPUT', line)
    )

    content = "\n".join(
        line for line in content.splitlines()
        if not (line.strip().startswith("/*") and line.strip().endswith("*/"))
    )

    pattern = re.compile(
        r"""
        ('([^']|'')*') |
        ("([^"]|"")*") |
        (--[^\n]*$) |
        (/\*.*?\*/)
        """,
        re.MULTILINE | re.DOTALL | re.VERBOSE
    )

    def replacer(match):
        if match.group(1) or match.group(3):
            return match.group(0)
        return ""

    return pattern.sub(replacer, content)

# ==============================
# 4. MAIN QUERY 시작
# ==============================

MAIN_QUERY_START = re.compile(
    r"""
    \b(
        CREATE\s+TEMPORARY\s+VIEW|
        CREATE\s+VIEW|
        CREATE\s+TABLE|
        ALTER|
        DROP|
        TRUNCATE|
        RENAME|
        DECLARE|
        BEGIN|
        EXECUTE|
        COMMIT|
        END|
        REPLACE\s+VIEW|
        MERGE|
        UPSERT|
        INSERT|
        UPDATE|
        DELETE|
        SELECT
    )\b
    """,
    re.IGNORECASE | re.VERBOSE
)

END_IF_PATTERN = re.compile(r'^\s*END\s+IF\b', re.IGNORECASE)
ALTER_TABLE_PATTERN = re.compile(r'^\s*ALTER\s+TABLE\b', re.IGNORECASE)

# ==============================
# 5. 쿼리 추출
# ==============================

def extract_queries_from_file(file_path):

    queries = []

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        content = preprocess(content)

        pos = 0
        length = len(content)

        while pos < length:

            match = MAIN_QUERY_START.search(content, pos)
            if not match:
                break

            start = match.start()
            keyword = match.group(1).upper()

            # END IF 제외
            if keyword == "END":
                line_start = content.rfind("\n", 0, start) + 1
                line_end = content.find("\n", start)
                if line_end == -1:
                    line_end = length

                line_text = content[line_start:line_end]

                if END_IF_PATTERN.match(line_text):
                    pos = line_end
                    continue

            # COMMIT 제외
            if keyword == "COMMIT":
                end = start
                while end < length and content[end] != ";":
                    end += 1
                pos = end + 1
                continue

            # ALTER 제외 (ALTER TABLE만 포함)
            if keyword == "ALTER":
                line_start = content.rfind("\n", 0, start) + 1
                line_end = content.find("\n", start)
                if line_end == -1:
                    line_end = length

                line_text = content[line_start:line_end]

                if not ALTER_TABLE_PATTERN.match(line_text):
                    pos = line_end
                    continue

            # MERGE 블록 잠금
            end = start
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

                if ch == ";" and not in_string:
                    end += 1
                    break

                end += 1

            query = content[start:end].strip()

            if query:
                lower_query = query.lower()
                if not any(p.lower() in lower_query for p in EXCLUDE_PATTERNS):
                    queries.append(query)

            pos = end

    except Exception as e:
        print(f"[ERROR] 파일 읽기 실패: {file_path} ({e})")

    return queries

# ==============================
# 6. CRUD 분류
# ==============================

def classify_query(query):
    q = query.strip().upper()

    if q.startswith(("CREATE", "INSERT", "MERGE", "UPSERT",
                     "DECLARE", "BEGIN", "EXECUTE", "END")):
        return "C"
    elif q.startswith("SELECT"):
        return "R"
    elif q.startswith("UPDATE"):
        return "U"
    elif q.startswith("DELETE"):
        return "D"
    elif q.startswith(("ALTER", "REPLACE", "DROP", "TRUNCATE", "RENAME")):
        return "U"
    else:
        return "R"

# ==============================
# 7. 메인
# ==============================

def main():

    file_name = os.path.basename(SOURCE_FILE)
    file_name_only = os.path.splitext(file_name)[0]
    abs_path = os.path.dirname(SOURCE_FILE)

    out_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(out_dir, exist_ok=True)

    output_filename = f"{file_name_only}.dat"
    output_path = os.path.join(out_dir, output_filename)

    total_files = 1
    total_queries = 0
    total_output_rows = 0
    total_file_lines = 0

    try:
        with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
            total_file_lines = sum(1 for _ in f)
    except:
        pass

    queries = extract_queries_from_file(SOURCE_FILE)

    with open(output_path, 'w', encoding='utf-8') as out_file:

        for query in queries:
            total_queries += 1
            crud_type = classify_query(query)

            for line in query.splitlines():
                if line.strip():
                    out_file.write(
                        f"{crud_type}{DELIMITER}"
                        f"{abs_path}{DELIMITER}"
                        f"{file_name}{DELIMITER}"
                        f"{SOURCE_FILE}{DELIMITER}"
                        f"{line.rstrip()}\n"
                    )
                    total_output_rows += 1

    print("====================================")
    print(" SQL 추출 완료 (파일 1건 기준)")
    print("====================================")
    print(f"DAT 파일 위치        : {output_path}")
    print(f"처리 파일 건수        : {total_files}")
    print(f"추출 쿼리 건수        : {total_queries}")
    print(f"생성 행 건수          : {total_output_rows}")
    print(f"전체 파일 총 행 건수  : {total_file_lines}")
    print("====================================")


if __name__ == "__main__":
    main()