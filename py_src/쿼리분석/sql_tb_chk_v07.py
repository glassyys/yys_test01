# CTE 내부 SOURCE / TARGET 분리
# VIEW → 논리 TARGET 분리 하되 with문과 create 문의 경우 SCHEMA_PREFIXES 적용하지 말고 view명이나 with문에서 사용한 임시테이블로 표시하도록 소스 수정 전체코드 부탁
# 아래소스에 
# merge into
# using
# update 구문도 분석에 추가해서 부탁
# merge 로직 최종 제외--구현 못함

import os
import sys
import re
import csv

# ===============================
# 설정
# ===============================

TARGET_EXTENSIONS = {".sql", ".hql", ".uld", ".ld", ".sh"}
ENV_FILE = "db_schema.env"

SCHEMA_PREFIXES = (
    "T", "TDIA", "KEY_MST", "RSQLUSER",
    "EDW_RAW", "ATS", "DI_CPM", "SDD_APP_QM"
)

# ===============================
# 정규식
# ===============================

SINGLE_LINE_COMMENT = re.compile(r"--.*?$", re.MULTILINE)
MULTI_LINE_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)

FROM_JOIN_PATTERN = re.compile(r"\b(FROM|JOIN)\b", re.IGNORECASE)

WRITE_PATTERN = re.compile(
    r"\b(INSERT\s+OVERWRITE|INSERT\s+INTO|UPDATE|DELETE)\b",
    re.IGNORECASE
)

CREATE_VIEW_PATTERN = re.compile(
    r"\bCREATE\s+VIEW\s+(\w+)\b",
    re.IGNORECASE
)

WITH_PATTERN = re.compile(
    r"\bWITH\s+(\w+)\s+AS\s*\(",
    re.IGNORECASE
)

TABLE_PATTERN = re.compile(
    rf"\$\{{((?:{'|'.join(SCHEMA_PREFIXES)})[^}}]+)\}}\.(\w+)",
    re.IGNORECASE
)

PLAIN_TABLE_PATTERN = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*)\b"
)

# ===============================
# ENV 로딩
# ===============================

def load_env_schema():
    mapping = {}
    env_path = os.path.join(os.getcwd(), ENV_FILE)
    if not os.path.isfile(env_path):
        print("[WARN] ENV 파일 없음:", env_path)
        return mapping

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            mapping[k.strip()] = v.strip().strip('"').strip("'")
    return mapping

# ===============================
# SQL 정리
# ===============================

def clean_sql(text):
    text = MULTI_LINE_COMMENT.sub("", text)
    text = SINGLE_LINE_COMMENT.sub("", text)
    return text

def split_sql_blocks(sql):
    blocks, buf = [], []
    for line in sql.splitlines():
        if not line.strip():
            continue
        buf.append(line)
        if ";" in line:
            blocks.append("\n".join(buf))
            buf = []
    if buf:
        blocks.append("\n".join(buf))
    return blocks

# ===============================
# SQL TYPE
# ===============================

def detect_sql_type(sql):
    u = sql.upper()
    if "INSERT OVERWRITE" in u:
        return "INSERT_OVERWRITE"
    if "INSERT INTO" in u:
        return "INSERT_INTO"
    if "UPDATE" in u:
        return "UPDATE"
    if "DELETE" in u:
        return "DELETE"
    if "CREATE VIEW" in u:
        return "CREATE_VIEW"
    if "SELECT" in u:
        return "SELECT"
    return None

# ===============================
# 파일 분석
# ===============================

def analyze_file(file_path, schema_map):
    results = []

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        raw = f.read()

    sql = clean_sql(raw)
    blocks = split_sql_blocks(sql)

    base_directory = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    dir_file = file_path

    for block in blocks:
        # MERGE 완전 제외
        if "MERGE INTO" in block.upper():
            continue

        sql_type = detect_sql_type(block)
        sources = set()
        target = None

        # WITH (CTE)
        with_tables = {w.upper() for w in WITH_PATTERN.findall(block)}

        # WRITE 계열 → target
        write_match = WRITE_PATTERN.search(block.upper())
        if write_match:
            write_sql = block[write_match.start():]
            for schema_code, table in TABLE_PATTERN.findall(write_sql):
                schema = schema_map.get(schema_code, schema_code)
                target = (schema, table)
                break

        # CREATE VIEW → target
        view_match = CREATE_VIEW_PATTERN.search(block)
        if view_match:
            view_name = view_match.group(1)
            if view_name.upper() not in with_tables:
                target = ("VIEW", view_name)

        # SOURCE 수집
        for m in FROM_JOIN_PATTERN.finditer(block):
            read_sql = block[m.start():]

            for schema_code, table in TABLE_PATTERN.findall(read_sql):
                schema = schema_map.get(schema_code, schema_code)
                sources.add((schema, table))

            for t in PLAIN_TABLE_PATTERN.findall(read_sql):
                if t.upper() in with_tables:
                    sources.add(("CTE", t))

        # SELECT 는 target 제거
        if sql_type == "SELECT":
            target = None

        # lineage 생성
        if sources and target:
            for s in sources:
                results.append({
                    "base_directory": base_directory,
                    "file_name": file_name,
                    "dir_file": dir_file,
                    "sql_type": sql_type,
                    "source_schema": s[0],
                    "source_table": s[1],
                    "target_schema": target[0],
                    "target_table": target[1],
                })

    return results

# ===============================
# 디렉토리 분석
# ===============================

def analyze_directory(target_dir, schema_map):
    all_results = []
    for root, _, files in os.walk(os.path.abspath(target_dir)):
        for f in files:
            if os.path.splitext(f)[1].lower() not in TARGET_EXTENSIONS:
                continue
            all_results.extend(
                analyze_file(os.path.join(root, f), schema_map)
            )
    return all_results

# ===============================
# CSV 저장
# ===============================

def save_csv(results, target_dir):
    if not results:
        return None

    last_dir = os.path.basename(os.path.normpath(target_dir))
    csv_name = (
        os.path.splitext(os.path.basename(sys.argv[0]))[0]
        + f"_{last_dir}.csv"
    )

    with open(csv_name, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    return csv_name

# ===============================
# MAIN
# ===============================

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("사용법: python filenm.py <분석대상_디렉토리>")
        sys.exit(1)

    target_directory = sys.argv[1]
    if not os.path.isdir(target_directory):
        print("유효한 디렉토리가 아닙니다.")
        sys.exit(1)

    schema_map = load_env_schema()
    results = analyze_directory(target_directory, schema_map)

    if not results:
        print("분석 결과 없음")
        sys.exit(0)

    csv_name = save_csv(results, target_directory)

    print("\n===== LINEAGE SAMPLE (TOP 10) =====")
    headers = list(results[0].keys())
    print(" | ".join(headers))
    print("-" * 200)

    for row in results[:10]:
        print(" | ".join(row[h] or "" for h in headers))

    print(f"\n분석 완료: {len(results)} 건")
    print(f"CSV 파일: {csv_name}")
