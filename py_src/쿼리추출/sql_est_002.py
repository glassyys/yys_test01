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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ==========================================================
# 실행방법
# python3 절대경로포함프로그램명 절대경로포함소스디렉토리
#
# 예:
# python3 /home/user/sql_extractor.py /app/source/projectA
# ==========================================================

import os
import sys
import re
from datetime import datetime

# ==========================================================
# 1. 대상 확장자
# ==========================================================
TARGET_EXTENSIONS = {".sh", ".hql", ".sql", ".uld", ".ld"}

# ==========================================================
# 2. SQL 패턴 (세미콜론 기준 완전 문장)
# ==========================================================
SQL_PATTERN = re.compile(
    r"""
    (
        \b(
            CREATE\s+TEMPORARY\s+VIEW|
            CREATE\s+VIEW|
            CREATE\s+TABLE\s+AS|
            ALTER\s+VIEW|
            REPLACE\s+VIEW|
            MERGE|
            UPSERT|
            INSERT|
            UPDATE|
            DELETE|
            SELECT|
            CREATE
        )\b
        .*?
        ;
    )
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

# ==========================================================
# SQL 유형 자동 분류
# ==========================================================
def classify_query(sql: str) -> str:
    upper_sql = sql.strip().upper()

    if upper_sql.startswith("SELECT"):
        return "R"
    elif upper_sql.startswith("DELETE"):
        return "D"
    elif upper_sql.startswith(("UPDATE", "MERGE", "UPSERT")):
        return "U"
    elif upper_sql.startswith(("CREATE", "ALTER", "REPLACE", "INSERT")):
        return "C"
    else:
        return "UNKNOWN"

def extract_sql_statements(content: str):
    matches = SQL_PATTERN.findall(content)
    return [m[0] for m in matches]

# ==========================================================
# 메인
# ==========================================================
def main():

    if len(sys.argv) != 2:
        print("사용법: python3 절대경로포함프로그램명 절대경로포함소스디렉토리")
        sys.exit(1)

    source_root = sys.argv[1]

    if not os.path.isdir(source_root):
        print("유효하지 않은 디렉토리입니다.")
        sys.exit(1)

    program_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    last_dir_name = os.path.basename(os.path.normpath(source_root))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    out_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(out_dir, exist_ok=True)

    output_file = os.path.join(
        out_dir,
        f"{program_name}_{last_dir_name}_{timestamp}.dat"
    )

    print(f"[INFO] DAT 생성 경로: {output_file}")

    total_files = 0
    sql_file_count = 0
    total_queries = 0

    with open(output_file, "w", encoding="utf-8") as out_f:

        for root, dirs, files in os.walk(source_root):
            for file in files:
                ext = os.path.splitext(file)[1].lower()

                if ext not in TARGET_EXTENSIONS:
                    continue

                total_files += 1
                file_path = os.path.join(root, file)

                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                except Exception as e:
                    print(f"[ERROR] 파일 읽기 실패: {file_path} - {e}")
                    continue

                sql_statements = extract_sql_statements(content)

                if sql_statements:
                    sql_file_count += 1

                for sql in sql_statements:

                    query_type = classify_query(sql)
                    total_queries += 1

                    sql = sql.rstrip().rstrip(";")
                    lines = sql.splitlines()

                    for line in lines:
                        if line.strip() == "":
                            continue

                        out_line = (
                            f"{root}|"
                            f"{file}|"
                            f"{file_path}|"
                            f"{query_type}|"
                            f"{line}\n"
                        )
                        out_f.write(out_line)

    print("\n========= 처리 결과 =========")
    print(f"대상 파일 수            : {total_files}")
    print(f"SQL 포함 파일 수        : {sql_file_count}")
    print(f"총 추출 쿼리 수         : {total_queries}")
    print("================================")
    print("[DONE] 작업 완료")


if __name__ == "__main__":
    main()