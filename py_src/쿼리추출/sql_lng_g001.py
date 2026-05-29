# python3 sql_lineage_scanner.py /absolute/path/to/source target_table
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 제미나이적용 SIDTEST 소스제외후 적용

import os
import sys
import re
import csv
import datetime

# ===============================
# 설정
# ===============================
TARGET_EXTENSIONS = {".sql", ".hql", ".uld", ".ld", ".sh"}

EXCLUDE_TABLES = {
    "DUAL", "SELECT", "WHERE", "GROUP", "ORDER", "HAVING",
    "ON", "USING", "WHEN", "THEN", "ELSE", "END",
    "AND", "OR", "AS", "NULL", "TABLE", "SET"
}

# 제외할 스키마 설정
EXCLUDE_SCHEMA_NAME = "SIDTEST"

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

MERGE_USING_PATTERN = re.compile(r"\bUSING\s+([^\s(,]+)", re.IGNORECASE)
FROM_BLOCK_PATTERN = re.compile(r"\bFROM\s+(.+?)(?=\bWHERE\b|\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|\bUNION\b|\bSELECT\b|;|$)", re.IGNORECASE | re.DOTALL)
JOIN_PATTERN = re.compile(r"\bJOIN\s+([^\s(,]+)", re.IGNORECASE)

# ===============================
# SQL 정리 및 필터링
# ===============================
def remove_commented_tablepattern_lines(text: str) -> str:
    kept = []
    for line in text.splitlines(True):
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
    if "MERGE INTO" in u: return "MERGE"
    if "INSERT OVERWRITE" in u: return "INSERT_OVERWRITE"
    if "INSERT INTO" in u: return "INSERT_INTO"
    if "CREATE TABLE" in u and "AS SELECT" in u: return "CTAS"
    if "CREATE VIEW" in u: return "CREATE_VIEW"
    if "UPDATE" in u: return "UPDATE"
    if "DELETE FROM" in u: return "DELETE"
    if u.strip().startswith("SELECT"): return "SELECT"
    return None

# ===============================
# 테이블 해석
# ===============================
def resolve_table(token):
    if not token: return None
    token = token.strip().replace("`", "").replace(",", "")
    if not token: return None
    
    token = token.split()[0]
    if token.upper() in EXCLUDE_TABLES: return None
    if "(" in token or ")" in token: return None

    m = TABLE_PATTERN.match(token)
    if m:
        var_name = m.group(1)
        table_name = m.group(2)
        schema = ENV_MAP.get(var_name, var_name)
        return (schema, table_name)

    if "." in token:
        parts = token.split(".", 1)
        if len(parts[0]) == 1: return None
        return (parts[0], parts[1])

    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", token):
        return ("", token)
    return None

# ===============================
# SOURCE 추출
# ===============================
def extract_sources(block, cte_targets, sql_type=None):
    sources = set()
    for m in FROM_BLOCK_PATTERN.finditer(block):
        from_content = m.group(1)
        table_tokens = from_content.split(',')
        for token in table_tokens:
            r = resolve_table(token.strip())
            if r and r[1] not in cte_targets:
                sources.add(r)

    if sql_type == "MERGE":
        for m in MERGE_USING_PATTERN.finditer(block):
            r = resolve_table(m.group(1))
            if r and r[1] not in cte_targets:
                sources.add(r)

    for m in JOIN_PATTERN.finditer(block):
        r = resolve_table(m.group(1))
        if r and r[1] not in cte_targets:
            sources.add(r)
    return sources

# ===============================
# 파일 분석 (SIDTEST 제외 로직 추가)
# ===============================
def analyze_file(file_path, source_dir):
    results = []
    try:
        with open(file_path, encoding="utf-8", errors="ignore") as f:
            raw_text = f.read()
    except Exception:
        return []

    raw_text = remove_commented_tablepattern_lines(raw_text)
    sql = clean_sql(raw_text)
    blocks = split_sql_blocks(sql)
    file_name = os.path.basename(file_path)

    for block in blocks:
        # ✅ SIDTEST 스키마 사용 여부 체크 (대소문자 무시)
        # 1) SIDTEST.TABLE 형태 2) ${SIDTEST}.TABLE 형태 모두 감지
        if re.search(rf"\b{EXCLUDE_SCHEMA_NAME}\.", block, re.IGNORECASE) or \
           re.search(rf"\$\{{{EXCLUDE_SCHEMA_NAME}(?:_[^}}]*)?\}}", block, re.IGNORECASE):
            continue # 해당 쿼리 블록 건너뜀

        sql_type = detect_sql_type(block)
        if not sql_type: continue

        cte_targets = set(WITH_PATTERN.findall(block))
        sources = extract_sources(block, cte_targets, sql_type=sql_type)
        targets = set()

        for pattern in [CTAS_PATTERN, VIEW_PATTERN, INSERT_PATTERN, UPDATE_PATTERN, DELETE_PATTERN, MERGE_PATTERN]:
            for m in pattern.finditer(block):
                r = resolve_table(m.group(1))
                if r: targets.add(r)

        if sql_type == "SELECT":
            for s in sources:
                results.append(build_row(source_dir, file_path, file_name, "READ", sql_type, s, None))
        else:
            for t in targets:
                for s in sources:
                    results.append(build_row(source_dir, file_path, file_name, "WRITE", sql_type, s, t))
                if not sources:
                    results.append(build_row(source_dir, file_path, file_name, "WRITE", sql_type, None, t))
    return results

def build_row(source_dir, file_path, file_name, rw, sql_type, source, target):
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

def analyze_directory(target_dir):
    all_results = []
    abs_dir = os.path.abspath(target_dir)
    for root, _, files in os.walk(abs_dir):
        for f in files:
            if os.path.splitext(f)[1].lower() in TARGET_EXTENSIONS:
                all_results.extend(analyze_file(os.path.join(root, f), abs_dir))
    return all_results

def save_csv(results, source_dir):
    if not results: return None
    out_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    program_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    last_dir = os.path.basename(os.path.abspath(source_dir))
    csv_name = os.path.join(out_dir, f"{program_name}_{last_dir}_{ts}.csv")
    
    with open(csv_name, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    return csv_name

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("python3 program.py <source_dir>")
        sys.exit(1)
    
    s_dir = sys.argv[1]
    res = analyze_directory(s_dir)
    path = save_csv(res, s_dir)
    print(f"=================================\n총 건수: {len(res)}\n생성 파일: {path}\n=================================")