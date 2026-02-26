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

SOURCE_DIR = os.path.abspath(sys.argv[1])

if not os.path.isdir(SOURCE_DIR):
    print("오류: 유효한 디렉토리가 아닙니다.")
    sys.exit(1)

# ==============================
# 2. 설정
# ==============================

TARGET_EXTENSIONS = ('.sh', '.hql', '.sql', '.uld', '.ld')
DELIMITER = "^"

EXCLUDE_PATTERNS = [
    "insert into sidtest.ad1901_rgb_ac190212_svc(svc_mgmt_num)"
]

# 🔥 FROM DUAL 단독 쿼리만 제외
ONLY_FROM_DUAL_PATTERN = re.compile(
    r'^\s*SELECT\s+.*?\s+FROM\s+DUAL\s*;?\s*$',
    re.IGNORECASE | re.DOTALL
)

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
        ALTER\s+TABLE|
        DROP|
        TRUNCATE|
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

            # 세미콜론까지 추출
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

                if any(p.lower() in lower_query for p in EXCLUDE_PATTERNS):
                    pos = end
                    continue

                # 🔥 FROM DUAL 단독 SELECT만 제외
                if ONLY_FROM_DUAL_PATTERN.match(query.strip()):
                    pos = end
                    continue

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
    elif q.startswith(("ALTER", "REPLACE", "DROP", "TRUNCATE")):
        return "U"
    else:
        return "R"

# ==============================
# 7. 메인
# ==============================

def main():

    program_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    source_last_dir = os.path.basename(SOURCE_DIR.rstrip(os.sep))

    out_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(out_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{program_name}_{source_last_dir}_{timestamp}.dat"
    output_path = os.path.join(out_dir, output_filename)

    total_files = 0
    total_queries = 0
    total_output_rows = 0
    total_file_lines = 0

    with open(output_path, 'w', encoding='utf-8') as out_file:

        for root, _, files in os.walk(SOURCE_DIR):

            for file in files:
                if file.lower().endswith(TARGET_EXTENSIONS):

                    total_files += 1
                    full_path = os.path.join(root, file)
                    abs_path = os.path.abspath(root)

                    try:
                        with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                            total_file_lines += sum(1 for _ in f)
                    except:
                        pass

                    queries = extract_queries_from_file(full_path)

                    for query in queries:
                        total_queries += 1
                        crud_type = classify_query(query)

                        for line in query.splitlines():
                            if line.strip():
                                out_file.write(
                                    f"{crud_type}{DELIMITER}"
                                    f"{abs_path}{DELIMITER}"
                                    f"{file}{DELIMITER}"
                                    f"{full_path}{DELIMITER}"
                                    f"{line.rstrip()}\n"
                                )
                                total_output_rows += 1

    print("====================================")
    print(" SQL 추출 완료 (ALTER TABLE 전용 + FROM DUAL 단독만 제외)")
    print("====================================")
    print(f"DAT 파일 위치        : {output_path}")
    print(f"처리 파일 건수        : {total_files}")
    print(f"추출 쿼리 건수        : {total_queries}")
    print(f"생성 행 건수          : {total_output_rows}")
    print(f"전체 파일 총 행 건수  : {total_file_lines}")
    print("====================================")

if __name__ == "__main__":
    main()