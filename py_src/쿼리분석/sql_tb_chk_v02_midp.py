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
# 정규식 패턴
# ===============================

SINGLE_LINE_COMMENT = re.compile(r"--.*?$", re.MULTILINE)
MULTI_LINE_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)

SQL_TYPE_PATTERN = re.compile(
    r"\b("
    r"INSERT\s+OVERWRITE|"
    r"INSERT\s+INTO|"
    r"CREATE\s+VIEW|"
    r"SELECT|UPDATE|DELETE"
    r")\b",
    re.IGNORECASE
)

OPERATION_PATTERN = re.compile(
    r"\b(FROM|JOIN|INTO|OVERWRITE|UPDATE|DELETE|VIEW)\b",
    re.IGNORECASE
)

TABLE_PATTERN = re.compile(
    r"(\$\{(T|TDIA)[^}]+\})\.([A-Za-z0-9_]+)",
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
            key = key.strip()
            val = val.strip().strip('"').strip("'")

            mapping[key] = val

    return mapping

# ===============================
# READ / WRITE 판단
# ===============================

def detect_rw_type(operation, sql_type):
    op = operation.upper()
    sql_type = (sql_type or "").upper()

    if op in {"FROM", "JOIN"}:
        return "READ"

    if op in {"INTO", "OVERWRITE", "UPDATE", "DELETE"}:
        return "WRITE"

    if sql_type.startswith("INSERT"):
        return "WRITE"

    return "UNKNOWN"

# ===============================
# 파일 분석
# ===============================

def analyze_file(file_path, schema_map):
    results = []

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except Exception:
        return results

    current_sql_type = None

    for line in lines:
        line = MULTI_LINE_COMMENT.sub("", line)
        line = SINGLE_LINE_COMMENT.sub("", line)

        if not line.strip():
            continue

        upper_line = line.upper()

        type_match = SQL_TYPE_PATTERN.search(upper_line)
        if type_match:
            current_sql_type = type_match.group(1).upper().replace(" ", "_")

        if "${T" not in line:
            continue

        operations = OPERATION_PATTERN.findall(upper_line)
        if not operations:
            continue

        for match in TABLE_PATTERN.finditer(line):
            schema_token = match.group(1)      # ${T_TMT}
            schema_code = schema_token[2:-1]  # T_TMT
            table = match.group(3)

            # 🔥 핵심: ENV 치환
            schema = schema_map.get(schema_code, schema_code)

            for op in operations:
                results.append({
                    "directory": os.path.abspath(os.path.dirname(file_path)),
                    "file": os.path.basename(file_path),
                    "sql_type": current_sql_type,
                    "rw_type": detect_rw_type(op, current_sql_type),
                    "operation": op.upper(),
                    "schema_code": schema_code,
                    "schema": schema,
                    "table": table,
                    "raw_sql": line.strip()
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

    # 🔥 ENV 로딩 (실행 디렉토리 기준)
    schema_map = load_env_schema()

    print("\n[ENV LOAD SAMPLE TOP 10]")
    print("schema_code | schema")
    print("-" * 40)
    for i, (k, v) in enumerate(schema_map.items()):
        if i >= 10:
            break
        print(f"{k} | {v}")

    results = analyze_directory(target_directory, schema_map)

    if not results:
        print("분석 결과 없음")
        sys.exit(0)

    script_name = os.path.basename(sys.argv[0])
    csv_name = os.path.splitext(script_name)[0] + ".csv"
    save_csv(results, csv_name)

    print("\n===== SAMPLE OUTPUT (TOP 10) =====")
    headers = list(results[0].keys())
    print(" | ".join(headers))
    print("-" * 150)

    for row in results[:10]:
        print(" | ".join(str(row[h]) for h in headers))

    print("\n분석 완료:", len(results), "건")
    print("CSV 파일:", csv_name)