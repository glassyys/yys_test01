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
import csv
from datetime import datetime

# ==========================================================
# 설정
# ==========================================================

TARGET_EXTENSIONS = {".sh", ".hql", ".sql", ".uld", ".ld"}

# 쿼리 유형 패턴 (대소문자 무시, 세미콜론 기준)
SQL_PATTERN = re.compile(
    r"""
    (
        \b(
            CREATE\s+VIEW|
            CREATE\s+TEMPORARY\s+VIEW|
            ALTER\s+VIEW|
            REPLACE\s+VIEW|
            MERGE|
            UPSERT|
            INSERT|
            UPDATE|
            DELETE|
            SELECT|
            CREATE\s+TABLE\s+AS|
            CREATE|
            ALTER
        )\b
        .*?
        ;
    )
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

# ==========================================================
# SQL 추출 함수
# ==========================================================

def extract_sql_statements(content):
    matches = SQL_PATTERN.findall(content)
    return [m[0].strip() for m in matches]

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

    # out 디렉토리 생성
    out_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(out_dir, exist_ok=True)

    output_file = os.path.join(
        out_dir,
        f"{program_name}_{last_dir_name}_{timestamp}.csv"
    )

    print(f"[INFO] CSV 생성 경로: {output_file}")

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        # CSV 헤더 (영문 컬럼명)
        writer.writerow([
            "file_absolute_path",
            "file_name",
            "absolute_path_with_file",
            "query_text"
        ])

        # 재귀 탐색
        for root, dirs, files in os.walk(source_root):
            for file in files:
                ext = os.path.splitext(file)[1].lower()

                if ext not in TARGET_EXTENSIONS:
                    continue

                file_path = os.path.join(root, file)

                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                except Exception as e:
                    print(f"[ERROR] 파일 읽기 실패: {file_path} - {e}")
                    continue

                sql_statements = extract_sql_statements(content)

                for sql in sql_statements:
                    writer.writerow([
                        root,
                        file,
                        file_path,
                        sql
                    ])

    print("[DONE] 작업 완료")


if __name__ == "__main__":
    main()