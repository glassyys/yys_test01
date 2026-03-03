# 아래소스에
# 1. "${T" 와 "${KEY_MST}" ,"${RSQLUSER}" ,"${EDW_RAW}" ,"${ATS}" ,"${DI_CPM}" ,"${SDD_APP_QM}" 이 있는패턴 모두 적용
# 2. SORCE와 TARGET SQL기준으로 한행에 나온것을 TARGET은 중복되어도 상관없으니 SORCE를 여러행으로 처리하고 해당 TARGET을 동일하게 생성하도록 변경부탁
# 4️⃣ 전체 수정 소스 (요청하신 완성본)
# 
# 아래 코드는 지금까지 요청하신 모든 조건을 반영한 최종 안정 버전입니다.
# 
# ✔ WITH + INSERT → INSERT
# ✔ READ → SOURCE / WRITE → TARGET
# ✔ multi-source → single-target
# ✔ source 중복 제거
# ✔ schema env 정상 치환
# ✔ 실제 하위 디렉토리 유지
# ✔ 파일/디렉토리 분리
# ✔ 지정한 모든 ${XXX} 패턴 인식
# 
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

TABLE_PATTERN = re.compile(
    rf"\$\{{((?:{'|'.join(SCHEMA_PREFIXES)})[^}}]+)\}}\.(\w+)",
    re.IGNORECASE
)

# ===============================
# ENV 로딩 (실행 디렉토리 기준)
# ===============================

def load_env_schema():
    mapping = {}
    env_path = os.path.join(os.getcwd(), ENV_FILE)

    print("[ENV FILE]", env_path)

    if not os.path.isfile(env_path):
        print("[WARN] ENV 파일 없음")
        return mapping

    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            mapping[k.strip()] = v.strip().strip('"').strip("'")

    print("[ENV SAMPLE]")
    for i, (k, v) in enumerate(mapping.items()):
        if i >= 10:
            break
        print(f"  {k} = {v}")

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
# SQL TYPE (WITH + INSERT 보정)
# ===============================

def detect_sql_type(sql):
    u = sql.upper()

    if re.search(r"\bINSERT\s+OVERWRITE\b", u):
        return "INSERT_OVERWRITE"
    if re.search(r"\bINSERT\s+INTO\b", u):
        return "INSERT_INTO"
    if re.search(r"\bUPDATE\b", u):
        return "UPDATE"
    if re.search(r"\bDELETE\b", u):
        return "DELETE"
    if re.search(r"\bCREATE\s+VIEW\b", u):
        return "CREATE_VIEW"
    if re.search(r"\bSELECT\b", u):
        return "SELECT"

    return None

# ===============================
# WITH 분리
# ===============================

def split_cte_and_main(sql):
    if not sql.strip().lower().startswith("with"):
        return None, sql

    m = re.search(r"\)\s*(insert|update|delete|create)", sql, re.IGNORECASE)
    if not m:
        return None, sql

    return sql[:m.start()], sql[m.start():]

# ===============================
# 파일 분석 (Lineage)
# ===============================

def analyze_file(file_path, schema_map):
    results = []
    dedup = set()

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            raw = f.read()
    except Exception:
        return results

    sql = clean_sql(raw)
    blocks = split_sql_blocks(sql)

    base_directory = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    dir_file = file_path

    for block in blocks:
        sql_type = detect_sql_type(block)

        cte_sql, main_sql = split_cte_and_main(block)

        sources = set()
        target = None

        # SOURCE : CTE + MAIN SELECT
        for sql_part in filter(None, [cte_sql, main_sql]):
            upper = sql_part.upper()
            for m in FROM_JOIN_PATTERN.finditer(upper):
                read_sql = sql_part[m.start():]
                for schema_code, table in TABLE_PATTERN.findall(read_sql):
                    schema = schema_map.get(schema_code, schema_code)
                    sources.add((schema, table))

        # TARGET : WRITE ONLY
        upper_main = main_sql.upper()
        wm = WRITE_PATTERN.search(upper_main)
        if wm:
            write_sql = main_sql[wm.start():]
            for schema_code, table in TABLE_PATTERN.findall(write_sql):
                schema = schema_map.get(schema_code, schema_code)
                target = (schema, table)
                break

        if not target or not sources:
            continue

        for s in sources:
            key = (s[0], s[1], target[0], target[1], dir_file)
            if key in dedup:
                continue
            dedup.add(key)

            results.append({
                "base_directory": base_directory,
                "file_name": dir_file,
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

def save_csv(results, filename):
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

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

    csv_name = os.path.splitext(os.path.basename(sys.argv[0]))[0] + ".csv"
    save_csv(results, csv_name)

    print("\n===== LINEAGE SAMPLE (TOP 10) =====")
    headers = list(results[0].keys())
    print(" | ".join(headers))
    print("-" * 200)

    for row in results[:10]:
        print(" | ".join(row[h] or "" for h in headers))

    print("\n분석 완료:", len(results), "건")
    print("CSV 파일:", csv_name)