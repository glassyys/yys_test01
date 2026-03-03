# python3 sql_lineage_scanner.py /absolute/path/to/source target_table
#!/usr/bin/env python3
import os
import re
import sys
import csv
import datetime
# import pymysql   # MySQL 연결은 주석 처리 (추후 해제 가능)

# MySQL 설정 (주석 처리 상태)
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "your_user",
    "password": "your_password",
    "database": "your_database",
    "charset": "utf8mb4",
    "autocommit": False,
}
MYSQL_TABLE = "sql_lineage"

# 조사할 파일 확장자
TARGET_EXT = [".sh", ".hql", ".sql", ".uld", ".ld"]

# 쿼리 유형 패턴 (예시 일부, 필요시 확장 가능)
PATTERNS = {
    "CREATE_VIEW_UNIFIED": re.compile(
        r"""
        \bCREATE\s+
        (?:OR\s+REPLACE\s+)?          
        (?:TEMPORARY\s+|TEMP\s+)?     
        VIEW\s+
        (?:IF\s+NOT\s+EXISTS\s+)?     
        (
            (?:[^ ]+|"[^"]+"|\w+)
            (?:\.(?:[^ ]+|"[^"]+"|\w+))?
        )
        """,
        re.IGNORECASE | re.VERBOSE
    ),
    "INSERT": re.compile(r"\bINSERT\s+(?:INTO|OVERWRITE)\s+", re.IGNORECASE),
    "UPDATE": re.compile(r"\bUPDATE\s+", re.IGNORECASE),
    "DELETE": re.compile(r"\bDELETE\s+FROM\s+", re.IGNORECASE),
    "JOIN": re.compile(r"\b(INNER|LEFT|RIGHT|FULL|SELF)\s+JOIN\b", re.IGNORECASE),
    "CTAS": re.compile(r"\bCREATE\s+TABLE\s+\w+\s+AS\s+SELECT\b", re.IGNORECASE),
    "MERGE": re.compile(r"\bMERGE\s+INTO\b", re.IGNORECASE),
    "WITH": re.compile(r"\bWITH\s+\w+\s+AS\s*\(", re.IGNORECASE),
}

# 스키마 매핑 로드
def load_schema_map(exec_dir):
    schema_file = os.path.join(exec_dir, "db_schema.env")
    schema_map = {}
    if os.path.exists(schema_file):
        with open(schema_file, "r") as f:
            for line in f:
                if "=" in line:
                    key, val = line.strip().split("=", 1)
                    schema_map[key.strip()] = val.strip().strip('"')
    return schema_map

# 파일 조사
def scan_files(src_dir):
    file_list = []
    for root, _, files in os.walk(src_dir):
        for f in files:
            if any(f.endswith(ext) for ext in TARGET_EXT):
                file_list.append(os.path.join(root, f))
    return file_list

# 쿼리 분석
def analyze_sql(content, schema_map):
    results = []
    for name, pattern in PATTERNS.items():
        for match in pattern.finditer(content):
            results.append({
                "sql_type": name,
                "matched": match.group(0),
                "source_schema": extract_schema(match.group(0), schema_map),
                "source_table": extract_table(match.group(0)),
                "target_schema": None,
                "target_table": None,
            })
    return results

def extract_schema(line, schema_map):
    for key in schema_map.keys():
        if f"${{{key}}}" in line:
            return schema_map[key]
    return None

def extract_table(line):
    m = re.search(r"\b(\w+)\b", line)
    return m.group(1) if m else None

# CSV 저장
def save_csv(rows, prog_name, src_dir):
    out_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(out_dir, exist_ok=True)
    last_dir = os.path.basename(src_dir.rstrip("/"))
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = os.path.join(out_dir, f"{prog_name}_{last_dir}_{ts}.csv")

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "file_abs_path", "file_name", "full_file_name",
            "read_write", "sql_type", "source_schema", "source_table",
            "target_schema", "target_table"
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"CSV generated: {out_file}")

# 메인 실행
def main():
    if len(sys.argv) < 3:
        print("Usage: python3 program.py <src_dir> <target_table>")
        sys.exit(1)

    prog_name = os.path.basename(sys.argv[0]).replace(".py", "")
    src_dir = sys.argv[1]
    target_table = sys.argv[2]
    schema_map = load_schema_map(os.getcwd())

    rows = []
    for file in scan_files(src_dir):
        with open(file, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        sql_results = analyze_sql(content, schema_map)
        for r in sql_results:
            rows.append({
                "file_abs_path": file,
                "file_name": os.path.basename(file),
                "full_file_name": file,
                "read_write": "WRITE" if r["sql_type"] in ["INSERT","UPDATE","DELETE","MERGE","CTAS"] else "READ",
                "sql_type": r["sql_type"],
                "source_schema": r["source_schema"],
                "source_table": r["source_table"],
                "target_schema": r["target_schema"],
                "target_table": target_table
            })

    save_csv(rows, prog_name, src_dir)

    # MySQL 저장 (주석 처리)
    """
    conn = pymysql.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {MYSQL_TABLE}")
    for row in rows:
        cur.execute(
            f"INSERT INTO {MYSQL_TABLE} (file_abs_path, file_name, full_file_name, read_write, sql_type, source_schema, source_table, target_schema, target_table) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (row["file_abs_path"], row["file_name"], row["full_file_name"], row["read_write"], row["sql_type"], row["source_schema"], row["source_table"], row["target_schema"], row["target_table"])
        )
    conn.commit()
    conn.close()
    """

if __name__ == "__main__":
    main()