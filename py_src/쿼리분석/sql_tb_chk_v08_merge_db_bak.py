## python3 /home/p190872/chksrc/sql_tb_chk_v08_merge_db.py /home/p190872/chksrc/test
## python3 /home/p190872/chksrc/sql_tb_chk_v08_merge_db.py /home/p190872/chksrc/SIDHUB
## python3 /home/p190872/chksrc/sql_tb_chk_v08_merge_db.py /NAS/MIDP/DBMSVC/MIDP/SID/SRC/SIDHUB
## python3 /home/p190872/chksrc/sql_tb_chk_v08_merge_db.py /NAS/MIDP/DBMSVC/MIDP/SID/SRC/TMT
## python3 /home/p190872/chksrc/sql_tb_chk_v08_merge_db.py /NAS/MIDP/DBMSVC/MIDP/SID/SRC/TDIA
## 
## 1~4번 내용대로 처리하여 수정한 파이썬 전체소스
## 
## 1. mysql DB연결
## "[mysql.conf 파일 예시]"처럼 mysql.conf 파일을 생성후
## 읽는데 table명은 선택옵션으로 사용하지 않고
## 
## "mysql.conf" 정보 기준으로 mysql DB연결하고 
## 
## [mysql.conf 파일생성 예시]
## [mysql]
## host     = localhost
## port     = 3306
## user     = root
## password = secret
## database = lineage_db
## table    = sql_lineage
## charset  = utf8mb4
## 
## 2. 파일명기준 테이블 생성
## database.csv파일명 기준으로 미존재시 
## "[테이블 스크립트 예시]" 처럼 생성후
## 
## [테이블 스크립트 예시]
## create table midp_project.csv파일명 (
## base_directory varchar(500) not null,
## file_name varchar(500) not null,
## dir_file varchar(1000) not null,
## sql_type varchar(50) not null,
## source_schema varchar(100) null,
## source_table varchar(500) null,
## target_schema varchar(100) null,
## target_table varchar(500) null
## )
## 
## 3. 파일생성내용기준 테이블 등록
## TRUNCATE 후 INSERT_INTO 처리하도록 소스 개선
## 
## 4. 출력문 보완
## 출력문을 파일생성시 조사한 파일수 결과 행수와
## DB적재정보도 출력하도록 수정
## 
## 추가로 파일생성은 실행디렉토리 하으 out 디렉토리에 생성하도록만 수정한 전체소스

# ===============================================================
# sql_tb_chk_v08_merge_db.py
# 실행예시: python3 /home/p190872/chksrc/sql_tb_chk_v08_merge_db.py /home/p190872/chksrc/test
# ===============================================================

import os
import sys
import re
import csv
import configparser
import pymysql

# ===============================
# 설정
# ===============================

TARGET_EXTENSIONS = {".sql", ".hql", ".uld", ".ld", ".sh"}
ENV_FILE          = "db_schema.env"
MYSQL_CONF_FILE   = "mysql.conf"

SCHEMA_PREFIXES = (
    "T", "TDIA", "KEY_MST", "RSQLUSER",
    "EDW_RAW", "ATS", "DI_CPM", "SDD_APP_QM"
)

# ===============================
# 정규식
# ===============================

SINGLE_LINE_COMMENT  = re.compile(r"--.*?$", re.MULTILINE)
MULTI_LINE_COMMENT   = re.compile(r"/\*.*?\*/", re.DOTALL)
FROM_JOIN_PATTERN    = re.compile(r"\b(FROM|JOIN)\b", re.IGNORECASE)

WRITE_PATTERN = re.compile(
    r"\b(INSERT\s+OVERWRITE|INSERT\s+INTO|UPDATE|DELETE)\b",
    re.IGNORECASE
)
MERGE_PATTERN      = re.compile(r"\bMERGE\s+INTO\b",            re.IGNORECASE)
CREATE_VIEW_PATTERN = re.compile(r"\bCREATE\s+VIEW\s+(\w+)\b",  re.IGNORECASE)
WITH_PATTERN       = re.compile(r"\bWITH\s+(\w+)\s+AS\s*\(",    re.IGNORECASE)

TABLE_PATTERN = re.compile(
    rf"\$\{{((?:{'|'.join(SCHEMA_PREFIXES)})[^}}]+)\}}\.(\w+)",
    re.IGNORECASE
)
PLAIN_TABLE_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")

# ===============================================================
# 1. mysql.conf 생성 / 읽기
# ===============================================================

def create_default_conf(conf_path: str) -> None:
    """mysql.conf 파일이 없을 때 템플릿을 생성한다."""
    template = (
        "[mysql]\n"
        "host     = localhost\n"
        "port     = 3306\n"
        "user     = root\n"
        "password = secret\n"
        "database = lineage_db\n"
        "table    = sql_lineage\n"
        "charset  = utf8mb4\n"
    )
    with open(conf_path, "w", encoding="utf-8") as f:
        f.write(template)
    print(f"[INFO] mysql.conf 파일을 생성했습니다: {conf_path}")
    print("[INFO] DB 접속 정보를 입력 후 다시 실행하세요.")
    sys.exit(0)


def load_mysql_conf() -> dict:
    """
    mysql.conf 를 읽어 접속 정보 dict 를 반환한다.
    - table 항목은 선택 옵션이므로 접속 정보에서 제외한다.
    """
    conf_path = os.path.join(os.getcwd(), MYSQL_CONF_FILE)

    # 파일이 없으면 템플릿 생성 후 종료
    if not os.path.isfile(conf_path):
        create_default_conf(conf_path)

    parser = configparser.ConfigParser()
    parser.read(conf_path, encoding="utf-8")

    if "mysql" not in parser:
        print(f"[ERROR] {MYSQL_CONF_FILE} 에 [mysql] 섹션이 없습니다.")
        sys.exit(1)

    sec = parser["mysql"]

    # table 은 접속 정보에 포함하지 않음
    config = {
        "host":     sec.get("host",     "localhost"),
        "port":     int(sec.get("port", 3306)),
        "user":     sec.get("user",     "root"),
        "password": sec.get("password", ""),
        "database": sec.get("database", ""),
        "charset":  sec.get("charset",  "utf8mb4"),
        "autocommit": False,
    }
    return config


# ===============================================================
# 2. 파일명 기준 테이블 생성 (미존재 시)
# ===============================================================

def ensure_table(conn, database: str, table_name: str) -> None:
    """
    {database}.{table_name} 테이블이 없으면 CREATE TABLE 을 실행한다.
    """
    create_ddl = f"""
CREATE TABLE IF NOT EXISTS `{database}`.`{table_name}` (
    base_directory  VARCHAR(500)  NOT NULL,
    file_name       VARCHAR(500)  NOT NULL,
    dir_file        VARCHAR(1000) NOT NULL,
    sql_type        VARCHAR(50)   NOT NULL,
    source_schema   VARCHAR(100)  NULL,
    source_table    VARCHAR(500)  NULL,
    target_schema   VARCHAR(100)  NULL,
    target_table    VARCHAR(500)  NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
    with conn.cursor() as cur:
        cur.execute(create_ddl)
    conn.commit()
    print(f"[INFO] 테이블 확인/생성 완료: `{database}`.`{table_name}`")


# ===============================================================
# ENV 로딩
# ===============================================================

def load_env_schema() -> dict:
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

def clean_sql(text: str) -> str:
    text = MULTI_LINE_COMMENT.sub("", text)
    text = SINGLE_LINE_COMMENT.sub("", text)
    return text


def split_sql_blocks(sql: str) -> list:
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

def detect_sql_type(sql: str):
    u = sql.upper()
    if "MERGE INTO"        in u: return "MERGE"
    if "INSERT OVERWRITE"  in u: return "INSERT_OVERWRITE"
    if "INSERT INTO"       in u: return "INSERT_INTO"
    if "UPDATE"            in u: return "UPDATE"
    if "DELETE"            in u: return "DELETE"
    if "CREATE VIEW"       in u: return "CREATE_VIEW"
    if "SELECT"            in u: return "SELECT"
    return None


# ===============================
# 파일 분석
# ===============================

def analyze_file(file_path: str, schema_map: dict) -> list:
    results = []

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        raw = f.read()

    sql    = clean_sql(raw)
    blocks = split_sql_blocks(sql)

    base_directory = os.path.dirname(file_path)
    file_name      = os.path.basename(file_path)
    dir_file       = file_path

    for block in blocks:
        sql_type = detect_sql_type(block)
        sources  = set()
        target   = None

        with_tables = {w.upper() for w in WITH_PATTERN.findall(block)}

        # MERGE target
        if sql_type == "MERGE":
            for schema_code, table in TABLE_PATTERN.findall(block):
                schema = schema_map.get(schema_code, schema_code)
                target = (schema, table)
                break

        # WRITE target
        write_match = WRITE_PATTERN.search(block.upper())
        if write_match:
            write_sql = block[write_match.start():]
            for schema_code, table in TABLE_PATTERN.findall(write_sql):
                schema = schema_map.get(schema_code, schema_code)
                target = (schema, table)
                break

        # CREATE VIEW
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

        if sql_type == "SELECT":
            target = None

        if sources and target:
            for s in sources:
                results.append({
                    "base_directory": base_directory,
                    "file_name":      file_name,
                    "dir_file":       dir_file,
                    "sql_type":       sql_type,
                    "source_schema":  s[0],
                    "source_table":   s[1],
                    "target_schema":  target[0],
                    "target_table":   target[1],
                })

    return results


# ===============================
# 디렉토리 분석
# ===============================

def analyze_directory(target_dir: str, schema_map: dict) -> tuple:
    """
    Returns:
        all_results (list): 분석 결과 레코드 목록
        file_count  (int):  분석 대상 파일 수
    """
    all_results = []
    file_count  = 0

    for root, _, files in os.walk(os.path.abspath(target_dir)):
        for f in files:
            ext = os.path.splitext(f)[1].lower()
            if ext not in TARGET_EXTENSIONS:
                continue
            file_path = os.path.join(root, f)
            file_count += 1
            all_results.extend(analyze_file(file_path, schema_map))

    return all_results, file_count


# ===============================
# CSV 저장
# ===============================

def save_csv(results: list, target_dir: str) -> str:
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


# ===============================================================
# 3. TRUNCATE → INSERT INTO  /  2. 테이블 자동 생성 포함
# ===============================================================

def load_to_mysql(results: list, csv_name: str, mysql_cfg: dict) -> dict:
    """
    1) mysql.conf 기준으로 DB 연결
    2) CSV 파일명 기준 테이블 미존재 시 자동 생성
    3) TRUNCATE 후 INSERT INTO 처리
    Returns: 적재 결과 정보 dict
    """
    database   = mysql_cfg["database"]
    table_name = os.path.splitext(os.path.basename(csv_name))[0]   # CSV 파일명 (확장자 제외)
    full_table = f"`{database}`.`{table_name}`"

    conn = pymysql.connect(**mysql_cfg)
    try:
        # ② 테이블 미존재 시 생성
        ensure_table(conn, database, table_name)

        with conn.cursor() as cur:
            # ③ TRUNCATE
            cur.execute(f"TRUNCATE TABLE {full_table}")

            # ③ INSERT INTO
            insert_sql = f"""
                INSERT INTO {full_table} (
                    base_directory, file_name, dir_file, sql_type,
                    source_schema, source_table, target_schema, target_table
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            data = [
                (
                    r["base_directory"], r["file_name"], r["dir_file"],
                    r["sql_type"],       r["source_schema"], r["source_table"],
                    r["target_schema"],  r["target_table"],
                )
                for r in results
            ]
            cur.executemany(insert_sql, data)

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

    return {
        "host":       mysql_cfg["host"],
        "port":       mysql_cfg["port"],
        "database":   database,
        "table":      table_name,
        "full_table": full_table,
        "rows":       len(results),
    }


# ===============================================================
# MAIN
# ===============================================================

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("사용법: python3 sql_tb_chk_v08_merge_db.py <분석대상_디렉토리>")
        sys.exit(1)

    target_directory = sys.argv[1]
    if not os.path.isdir(target_directory):
        print(f"[ERROR] 유효한 디렉토리가 아닙니다: {target_directory}")
        sys.exit(1)

    # ── mysql.conf 읽기 (없으면 템플릿 생성 후 종료)
    mysql_cfg  = load_mysql_conf()
    schema_map = load_env_schema()

    print(f"\n[INFO] 분석 시작: {os.path.abspath(target_directory)}")
    print("-" * 60)

    # ── 디렉토리 분석
    results, file_count = analyze_directory(target_directory, schema_map)

    # ── 4. 파일 조사 결과 출력
    print(f"  조사 파일 수   : {file_count:>8,} 개  "
          f"(확장자: {', '.join(sorted(TARGET_EXTENSIONS))})")
    print(f"  분석 결과 행 수: {len(results):>8,} 건")

    if not results:
        print("\n[WARN] 분석 결과가 없어 CSV/DB 저장을 건너뜁니다.")
        sys.exit(0)

    # ── CSV 저장
    csv_name = save_csv(results, target_directory)
    print(f"  저장 CSV 파일  : {csv_name}")

    # ── MySQL TRUNCATE → INSERT
    print("\n[INFO] MySQL 적재 시작 ...")
    db_info = load_to_mysql(results, csv_name, mysql_cfg)

    # ── 4. DB 적재 결과 출력
    print("-" * 60)
    print("[ DB 적재 결과 ]")
    print(f"  접속 호스트    : {db_info['host']}:{db_info['port']}")
    print(f"  데이터베이스   : {db_info['database']}")
    print(f"  테이블         : {db_info['table']}")
    print(f"  처리 방식      : TRUNCATE → INSERT INTO")
    print(f"  적재 완료 건수 : {db_info['rows']:>8,} 건")
    print("-" * 60)
    print("[INFO] 모든 처리가 완료되었습니다.\n")
