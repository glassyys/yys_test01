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
#!/usr/bin/env python3
# 실행방법:
# python3 프로그램명.py 절대경로포함소스디렉토리

import os
import re
import sys
from datetime import datetime

# ==============================
# 1. 파라미터 체크
# ==============================

if len(sys.argv) != 2:
    print("사용법: python3 프로그램명.py 절대경로포함소스디렉토리")
    sys.exit(1)

SOURCE_DIR = sys.argv[1]

if not os.path.isdir(SOURCE_DIR):
    print("오류: 유효한 디렉토리가 아닙니다.")
    sys.exit(1)

SOURCE_DIR = os.path.abspath(SOURCE_DIR)

# ==============================
# 2. 추출 대상 설정
# ==============================

TARGET_EXTENSIONS = ('.sh', '.hql', '.sql', '.uld', '.ld')

QUERY_PATTERN = re.compile(
    r'''
    (
        (WITH\s+.*?\)\s*)?
        (
            CREATE\s+TEMPORARY\s+VIEW |
            CREATE\s+VIEW |
            CREATE\s+TABLE |
            ALTER\s+VIEW |
            REPLACE\s+VIEW |
            MERGE |
            UPSERT |
            INSERT |
            UPDATE |
            DELETE |
            SELECT
        )
        .*?
        ;
    )
    ''',
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

DELIMITER = "^"

# ==============================
# 3. CRUD 자동 분류
# ==============================

def classify_query(query):
    q = query.strip().upper()

    if q.startswith("WITH"):
        if "INSERT" in q:
            return "C"
        elif "UPDATE" in q:
            return "U"
        elif "DELETE" in q:
            return "D"
        else:
            return "R"

    if q.startswith(("CREATE", "INSERT", "MERGE", "UPSERT")):
        return "C"
    elif q.startswith("SELECT"):
        return "R"
    elif q.startswith("UPDATE"):
        return "U"
    elif q.startswith("DELETE"):
        return "D"
    elif q.startswith(("ALTER", "REPLACE")):
        return "U"
    else:
        return "R"

# ==============================
# 4. 쿼리 추출
# ==============================

def extract_queries_from_file(file_path):
    queries = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        matches = QUERY_PATTERN.findall(content)
        for match in matches:
            queries.append(match[0].strip())

    except Exception as e:
        print(f"[ERROR] 파일 읽기 실패: {file_path} ({e})")

    return queries

# ==============================
# 5. 메인 처리
# ==============================

def main():

    # 실행 프로그램명 추출 (확장자 제거)
    program_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]

    # 소스 마지막 디렉토리명 추출
    source_last_dir = os.path.basename(SOURCE_DIR.rstrip(os.sep))

    # 출력 디렉토리 (현재 실행 디렉토리/out)
    exec_dir = os.getcwd()
    out_dir = os.path.join(exec_dir, "out")

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # 파일명 생성 규칙
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{program_name}_{source_last_dir}_{timestamp}.dat"
    output_path = os.path.join(out_dir, output_filename)

    total_files = 0
    total_queries = 0
    total_output_rows = 0
    total_file_lines = 0

    with open(output_path, 'w', encoding='utf-8') as out_file:

        for root, dirs, files in os.walk(SOURCE_DIR):

            for file in files:
                if file.lower().endswith(TARGET_EXTENSIONS):

                    total_files += 1
                    full_path = os.path.join(root, file)
                    abs_path = os.path.abspath(root)

                    # 파일 전체 행 수 카운트
                    try:
                        with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                            file_line_count = sum(1 for _ in f)
                            total_file_lines += file_line_count
                    except:
                        pass

                    queries = extract_queries_from_file(full_path)

                    for query in queries:
                        total_queries += 1
                        crud_type = classify_query(query)

                        lines = query.splitlines()

                        for line in lines:
                            if line.strip():
                                output_line = (
                                    f"{crud_type}{DELIMITER}"
                                    f"{abs_path}{DELIMITER}"
                                    f"{file}{DELIMITER}"
                                    f"{full_path}{DELIMITER}"
                                    f"{line.rstrip()}"
                                )
                                out_file.write(output_line + "\n")
                                total_output_rows += 1

    # ==============================
    # 6. 결과 출력
    # ==============================

    print("====================================")
    print(" SQL 추출 완료")
    print("====================================")
    print(f"DAT 파일 위치        : {output_path}")
    print(f"처리 파일 건수        : {total_files}")
    print(f"추출 쿼리 건수        : {total_queries}")
    print(f"생성 행 건수          : {total_output_rows}")
    print(f"전체 파일 총 행 건수  : {total_file_lines}")
    print("====================================")


if __name__ == "__main__":
    main()