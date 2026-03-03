# python3 sql_lineage_scanner.py /absolute/path/to/source target_table
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# GPT 아래 요청했으나 수정 잘안되어서 소스 유지
# 소스수정요청
# FROM tb_1 b,
#      tb_2 c,
#      tb_3 d
# → tb_1
# → tb_2
# → tb_3
# 이런부분 source_table 로 처리되도록 로직보완한 전체소스 부탁
# 단 기존 로직은 유지해서 처리해야 함
#  출력부분 같은 경우가 없어지는 경우가 많음

import os
import sys
import re
import csv
import datetime
# import pymysql

# ===============================
# 설정
# ===============================
TARGET_EXTENSIONS = {".sql", ".hql", ".uld", ".ld", ".sh"}

EXCLUDE_TABLES = {
    "DUAL", "SELECT", "WHERE", "GROUP", "ORDER", "HAVING",
    "ON", "USING", "WHEN", "THEN", "ELSE", "END",
    "AND", "OR", "AS", "NULL", "TABLE", "SET"
}

# ✅ WIND 계열(${WIND_CAL}, ${WIND_FDM}, ${WIND_...})을 SCHEMA_PREFIXES에 포함 (요청사항)
# - TABLE_PATTERN은 "prefix로 시작하는 변수명"을 매칭하므로
#   SCHEMA_PREFIXES에 'WIND'만 추가하면 ${WIND_CAL}, ${WIND_FDM} 모두 커버됨
SCHEMA_PREFIXES = (
    "T", "TDIA", "KEY_MST", "RSQLUSER",
    "EDW_RAW", "ATS", "DI_CPM", "SDD_APP_QM",
    "WIND"
)

TABLE_PATTERN = re.compile(
    rf"\$\{{((?:{'|'.join(map(re.escape, SCHEMA_PREFIXES))})[^}}]*)\}}\.(\w+)",
    re.IGNORECASE
)

# ===============================
# ENV 로딩
# ===============================
def load_env():
    env_map = {}
    env_path = os.path.join(os.getcwd(), "db_schema.env")
    if not os.path.exists(env_path):
        return env_map

    with open(env_path, encoding="utf-8") as f:
        for line in f:
            if "=" not in line:
                continue
            k, v = line.strip().split("=", 1)
            env_map[k.strip()] = v.strip().replace('"', "").replace("'", "")
    return env_map

ENV_MAP = load_env()

# ===============================
# SQL 정규식
# ===============================
SINGLE_LINE_COMMENT = re.compile(r"--.*?$", re.MULTILINE)
MULTI_LINE_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)

WITH_PATTERN = re.compile(r"\bWITH\s+(\w+)\s+AS\s*\(", re.IGNORECASE)

CTAS_PATTERN = re.compile(r"\bCREATE\s+TABLE\s+([^\s(]+)\s+AS\s+SELECT", re.IGNORECASE)
VIEW_PATTERN = re.compile(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+)?VIEW\s+([^\s(]+)", re.IGNORECASE)
INSERT_PATTERN = re.compile(r"\bINSERT\s+(?:OVERWRITE|INTO)\s+(?:TABLE\s+)?([^\s(]+)", re.IGNORECASE)
UPDATE_PATTERN = re.compile(r"\bUPDATE\s+([^\s(]+)", re.IGNORECASE)
DELETE_PATTERN = re.compile(r"\bDELETE\s+FROM\s+([^\s(]+)", re.IGNORECASE)
MERGE_PATTERN = re.compile(r"\bMERGE\s+INTO\s+([^\s(]+)", re.IGNORECASE)

# ✅ MERGE 전용 source 추출: "USING <table>" 형태 지원 (요청사항)
MERGE_USING_PATTERN = re.compile(r"\bUSING\s+([^\s(,]+)", re.IGNORECASE)

# 🔥 UNION ALL 포함 모든 FROM 직접 탐색
FROM_DIRECT_PATTERN = re.compile(r"\bFROM\s+([^\s(,]+)", re.IGNORECASE)
JOIN_PATTERN = re.compile(r"\bJOIN\s+([^\s(,]+)", re.IGNORECASE)

# ===============================
# SQL 정리
# ===============================
def remove_commented_tablepattern_lines(text: str) -> str:
    """
    '#' 또는 '--' 로 주석 처리된 '라인' 중 TABLE_PATTERN(${...}.TABLE) 이 포함된 라인은
    source/target 리니지 조사 대상에서 제외한다.
    (기존 소스 내용은 최대한 유지하고, 해당 라인만 제거)
    """
    kept = []
    for line in text.splitlines(True):  # keepends=True
        s = line.lstrip()
        if (s.startswith("#") or s.startswith("--")) and TABLE_PATTERN.search(line):
            continue
        kept.append(line)
    return "".join(kept)

def clean_sql(text: str) -> str:
    text = MULTI_LINE_COMMENT.sub("", text)
    text = SINGLE_LINE_COMMENT.sub("", text)
    return text

def split_sql_blocks(sql: str):
    return [b.strip() for b in sql.split(";") if b.strip()]

# ===============================
# SQL TYPE
# ===============================
def detect_sql_type(sql: str):
    u = sql.upper()
    if "MERGE INTO" in u:
        return "MERGE"
    if "INSERT OVERWRITE" in u:
        return "INSERT_OVERWRITE"
    if "INSERT INTO" in u:
        return "INSERT_INTO"
    if "CREATE TABLE" in u and "AS SELECT" in u:
        return "CTAS"
    if "CREATE VIEW" in u:
        return "CREATE_VIEW"
    if "UPDATE" in u:
        return "UPDATE"
    if "DELETE FROM" in u:
        return "DELETE"
    if u.strip().startswith("SELECT"):
        return "SELECT"
    return None

# ===============================
# 테이블 해석
# ===============================
def resolve_table(token):
    if not token:
        return None

    token = token.strip().replace("`", "")
    token = token.split()[0]

    if token.upper() in EXCLUDE_TABLES:
        return None

    if "(" in token or ")" in token:
        return None

    # ${T_XXX}.TABLE / ${WIND_CAL}.TABLE / ${WIND_FDM}.TABLE / ${WIND_...}.TABLE
    m = TABLE_PATTERN.match(token)
    if m:
        var_name = m.group(1)
        table_name = m.group(2)
        schema = ENV_MAP.get(var_name, var_name)
        return (schema, table_name)

    # 일반 schema.table (컬럼참조 제외)
    if "." in token:
        s, t = token.split(".", 1)
        if len(s) == 1:
            return None
        return (s, t)

    # 스키마 없는 테이블
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", token):
        return ("", token)

    return None

# ===============================
# SOURCE 추출 (🔥 완전보강 + MERGE USING 지원)
# ===============================
def extract_sources(block, cte_targets, sql_type=None):
    sources = set()

    # ✅ MERGE는 FROM/JOIN 없이 USING만 있는 경우가 흔함: USING 테이블도 source로 추출
    if sql_type == "MERGE":
        for m in MERGE_USING_PATTERN.finditer(block):
            token = m.group(1)
            r = resolve_table(token)
            if not r:
                continue
            if r[1] in cte_targets:
                continue
            sources.add(r)

    # 🔥 모든 FROM 직접 추출 (UNION ALL 포함)
    for m in FROM_DIRECT_PATTERN.finditer(block):
        token = m.group(1)
        r = resolve_table(token)
        if not r:
            continue
        if r[1] in cte_targets:
            continue
        sources.add(r)

    # JOIN
    for m in JOIN_PATTERN.finditer(block):
        token = m.group(1)
        r = resolve_table(token)
        if r:
            sources.add(r)

    return sources

# ===============================
# 파일 분석
# ===============================
def analyze_file(file_path, source_dir):
    results = []

    with open(file_path, encoding="utf-8", errors="ignore") as f:
        raw_text = f.read()

    # ✅ '#' 또는 '--' 로 주석처리된 라인 중 TABLE_PATTERN 포함 라인은 리니지 조사에서 제외
    raw_text = remove_commented_tablepattern_lines(raw_text)

    # 기존 로직 유지: 일반 주석 제거 후 파싱
    sql = clean_sql(raw_text)

    blocks = split_sql_blocks(sql)
    file_name = os.path.basename(file_path)

    for block in blocks:
        sql_type = detect_sql_type(block)
        if not sql_type:
            continue

        cte_targets = set(WITH_PATTERN.findall(block))
        sources = extract_sources(block, cte_targets, sql_type=sql_type)
        targets = set()

        for pattern in [
            CTAS_PATTERN,
            VIEW_PATTERN,
            INSERT_PATTERN,
            UPDATE_PATTERN,
            DELETE_PATTERN,
            MERGE_PATTERN
        ]:
            for m in pattern.finditer(block):
                r = resolve_table(m.group(1))
                if r:
                    targets.add(r)

        if sql_type == "SELECT":
            for s in sources:
                results.append(
                    build_row(
                        source_dir, file_path, file_name,
                        "READ", sql_type, s, None
                    )
                )
            continue

        for t in targets:
            for s in sources:
                results.append(
                    build_row(
                        source_dir, file_path, file_name,
                        "WRITE", sql_type, s, t
                    )
                )

    return results

def build_row(source_dir, file_path, file_name,
              rw, sql_type, source, target):
    return {
        "source_dir": source_dir,
        "file_path": file_path,
        "file_name": file_name,
        "read_write": rw,
        "sql_type": sql_type,
        "source_schema": source[0] if source else "",
        "source_table": source[1] if source else "",
        "target_schema": target[0] if target else "",
        "target_table": target[1] if target else "",
    }

# ===============================
# 디렉토리 분석
# ===============================
def analyze_directory(target_dir):
    all_results = []
    abs_dir = os.path.abspath(target_dir)

    for root, _, files in os.walk(abs_dir):
        for f in files:
            if os.path.splitext(f)[1].lower() in TARGET_EXTENSIONS:
                full_path = os.path.join(root, f)
                all_results.extend(analyze_file(full_path, abs_dir))

    return all_results

# ===============================
# CSV 저장
# ===============================
def save_csv(results, source_dir):
    if not results:
        return None

    out_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(out_dir, exist_ok=True)

    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    program_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    last_dir = os.path.basename(os.path.abspath(source_dir))

    csv_name = os.path.join(
        out_dir,
        f"{program_name}_{last_dir}_{ts}.csv"
    )

    fieldnames = list(results[0].keys())

    with open(csv_name, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    return csv_name

# ===============================
# MAIN
# ===============================
if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("python3 program.py <source_dir>")
        sys.exit(1)

    source_dir = sys.argv[1]

    results = analyze_directory(source_dir)
    csv_file = save_csv(results, source_dir)

    print("=================================")
    print(f"총 건수: {len(results)}")
    print(f"생성 파일: {csv_file}")
    print("=================================")
