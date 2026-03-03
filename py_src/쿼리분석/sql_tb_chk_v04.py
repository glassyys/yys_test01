import os
import sys
import re
import csv

# ===============================
# 설정
# ===============================

TARGET_EXTENSIONS = {".sql", ".hql", ".uld", ".ld", ".sh"}
ENV_FILE = "db_schema.env"

# ===============================
# 정규식
# ===============================

SINGLE_LINE_COMMENT = re.compile(r"--.*?$", re.MULTILINE)
MULTI_LINE_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)

SQL_TYPE_PATTERN = re.compile(
    r"\b("
    r"INSERT\s+OVERWRITE|"
    r"INSERT\s+INTO|"
    r"CREATE\s+VIEW|"
    r"UPDATE|DELETE|SELECT"
    r")\b",
    re.IGNORECASE
)

FROM_JOIN_PATTERN = re.compile(r"\b(FROM|JOIN)\b", re.IGNORECASE)
WRITE_PATTERN = re.compile(
    r"\b(INSERT\s+OVERWRITE|INSERT\s+INTO|UPDATE|DELETE)\b",
    re.IGNORECASE
)

TABLE_PATTERN = re.compile(
    r"\$\{((?:T|TDIA)[^}]+)\}\.([A-Za-z0-9_]+)",
    re.IGNORECASE
)

# ===============================
# ENV 로딩 (실행 디렉토리 기준)
# ===============================

def load_env_schema():
    mapping = {}
    env_path = os.path.join(os.getcwd(), ENV_FILE)

    print(f"\n[ENV FILE PATH] {env_path}")

    if not os.path.isfile(env_path):
        print("[WARN] ENV 파일 없음")
        return mapping

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, val = line.split("=", 1)
            mapping[key.strip()] = val.strip().strip('"').strip("'")

    return mapping

# ===============================
# SQL 정리 / 블록 분리
# ===============================

def clean_sql(text):
    text = MULTI_LINE_COMMENT.sub("", text)
    text = SINGLE_LINE_COMMENT.sub("", text)
    return text


def split_sql_blocks(sql):
    blocks = []
    buf = []

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
# 파일 분석 (Lineage)
# ===============================

def analyze_file(file_path, schema_map):
    results = []

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            raw = f.read()
    except Exception:
        return results

    sql = clean_sql(raw)
    blocks = split_sql_blocks(sql)

    # 🔥 경로 구조 정확히 분리
    base_directory = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    dir_file = os.path.join(base_directory, file_name)

    for block in blocks:
        upper_block = block.upper()

        sql_type_match = SQL_TYPE_PATTERN.search(upper_block)
        sql_type = sql_type_match.group(1).upper() if sql_type_match else None

        sources = set()
        target = None

        for schema_code, table in TABLE_PATTERN.findall(block):
            schema = schema_map.get(schema_code, schema_code)

            if FROM_JOIN_PATTERN.search(upper_block):
                sources.add((schema, table))

            if WRITE_PATTERN.search(upper_block):
                target = (schema, table)

        # multi-source → single-target
        if target:
            results.append({
                "base_directory": base_directory,
                "file_name": file_name,
                "dir_file": dir_file,
                "sql_type": sql_type,
                "source_schema": ",".join(sorted(s[0] for s in sources)),
                "source_table": ",".join(sorted(s[1] for s in sources)),
                "target_schema": target[0],
                "target_table": target[1],
            })

    return results

# ===============================
# 디렉토리 분석
# ===============================

def analyze_directory(target_dir, schema_map):
    all_results = []
    base_dir = os.path.abspath(target_dir)

    for root, _, files in os.walk(base_dir):
        for file in files:
            if os.path.splitext(file)[1].lower() not in TARGET_EXTENSIONS:
                continue

            file_path = os.path.join(root, file)
            all_results.extend(analyze_file(file_path, schema_map))

    return all_results

# ===============================
# CSV 저장
# ===============================

def save_csv(results, filename):
    if not results:
        return

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

# ===============================
# 메인
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

    csv_name = os.path.splitext(os.path.basename(sys.argv[0]))[0] + ".csv"
    save_csv(results, csv_name)

    print("\n===== LINEAGE SAMPLE (TOP 10) =====")
    headers = list(results[0].keys())
    print(" | ".join(headers))
    print("-" * 200)

    for row in results[:10]:
        print(" | ".join(row[h] if row[h] else "" for h in headers))

    print("\n분석 완료:", len(results), "건")
    print("CSV 파일:", csv_name)