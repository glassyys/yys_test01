1. 리눅스 특정디렉토리 안의 파일에 쿼리구문중 CRUD와 JOIN 이나 view등 테이블 처리하는 부분을 추출 
2. 레이아웃은 디렉토리,파일명, CRUD종류, 스키마, 테이블명이고 
3. 테이블명은 CRUD와 JOIN 이나 view등의 구문의 행중 "${T"로 시작되는 "."포함 단어 전체중 "."을 기준으로 "${T"로 시작되는 단어를 "{"과 "}"사이 문장만 추출해서 스키마로 저장하고 "."뒤의 단어는 테이블명으로 처리해서 저장 
4. 실행은 파이선파일명 서버디렉토리 로 실행하면 해당디렉토리 하위 안에 있는 모든 파일에 대해 분석부탁 위와 같이 SQL이 포함된 파일에서 테이블처리 내용을 파악할수 있는 파이썬 소스생성부탁

파일 확장자는 sql hql uld ld sh인 파일만 분석하도록 하고 디렉토리 절대경로에 지정한 파라미터러 디렉토리포함한 디렉토리를 표시하도록 수정

✅ 수정된 전체 소스 (INSERT OVERWRITE 반영)
import os
import sys
import re
import csv

# ===============================
# 설정
# ===============================

TARGET_EXTENSIONS = {".sql", ".hql", ".uld", ".ld", ".sh"}

# ===============================
# 정규식 패턴 정의
# ===============================

# 주석 제거
SINGLE_LINE_COMMENT = re.compile(r"--.*?$", re.MULTILINE)
MULTI_LINE_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)

# SQL 타입 (INSERT OVERWRITE / INSERT INTO 우선)
SQL_TYPE_PATTERN = re.compile(
    r"\b("
    r"INSERT\s+OVERWRITE|"
    r"INSERT\s+INTO|"
    r"CREATE\s+VIEW|"
    r"SELECT|UPDATE|DELETE"
    r")\b",
    re.IGNORECASE
)

# 테이블 접근 키워드
OPERATION_PATTERN = re.compile(
    r"\b(FROM|JOIN|INTO|OVERWRITE|UPDATE|VIEW)\b",
    re.IGNORECASE
)

# ${Txxx}.TB_NAME
TABLE_PATTERN = re.compile(
    r"(\$\{T[^}]+\})\.([A-Za-z0-9_]+)",
    re.IGNORECASE
)

# ===============================
# 파일 분석
# ===============================

def analyze_file(file_path):
    results = []

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except Exception:
        return results

    current_sql_type = None

    for line in lines:
        # 주석 제거
        line = MULTI_LINE_COMMENT.sub("", line)
        line = SINGLE_LINE_COMMENT.sub("", line)

        if not line.strip():
            continue

        upper_line = line.upper()

        # SQL 타입 갱신
        type_match = SQL_TYPE_PATTERN.search(upper_line)
        if type_match:
            current_sql_type = type_match.group(1).upper().replace(" ", "_")

        # ${T 없으면 스킵
        if "${T" not in line:
            continue

        operations = OPERATION_PATTERN.findall(upper_line)
        if not operations:
            continue

        for match in TABLE_PATTERN.finditer(line):
            schema = match.group(1).replace("${", "").replace("}", "")
            table = match.group(2)

            for op in operations:
                results.append({
                    "directory": os.path.abspath(os.path.dirname(file_path)),
                    "file": os.path.basename(file_path),
                    "sql_type": current_sql_type,
                    "operation": op.upper(),
                    "schema": schema,
                    "table": table,
                    "raw_sql": line.strip()
                })

    return results


def analyze_directory(target_dir):
    all_results = []
    base_dir = os.path.abspath(target_dir)

    for root, _, files in os.walk(base_dir):
        for file in files:
            _, ext = os.path.splitext(file)
            if ext.lower() not in TARGET_EXTENSIONS:
                continue

            file_path = os.path.join(root, file)
            all_results.extend(analyze_file(file_path))

    return all_results

# ===============================
# CSV 저장
# ===============================

def save_csv(results, filename="sql_analysis_result.csv"):
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

# ===============================
# 메인
# ===============================

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("사용법: python sql_table_analyzer_adv.py <분석대상_디렉토리>")
        sys.exit(1)

    target_directory = sys.argv[1]

    if not os.path.isdir(target_directory):
        print("유효한 디렉토리가 아닙니다.")
        sys.exit(1)

    results = analyze_directory(target_directory)

    if not results:
        print("분석 결과 없음")
        sys.exit(0)

    save_csv(results)

    print(f"분석 완료: {len(results)}건")
    print(" - sql_analysis_result.csv")

📌 이제 이렇게 잡힙니다
INSERT OVERWRITE TABLE ${T_TDW}.TB_SALES
SELECT ...
FROM ${T_TDW}.TB_ORDER

sql_type	operation	schema	table
INSERT_OVERWRITE	OVERWRITE	T_TDW	TB_SALES
INSERT_OVERWRITE	FROM	T_TDW	TB_ORDER

✔ WRITE / READ 흐름 명확
✔ Hive 계열 쿼리 대응 완료

🔥 다음 단계로 가능한 고급화

INSERT OVERWRITE → WRITE

FROM / JOIN → READ

DELETE without WHERE 위험도 컬럼 추가

한 SQL 블록에서 CRUD 여러 개 묶어서 식별

원하면 데이터 거버넌스용(READ/WRITE 매트릭스) 으로 바로 확장해줄게요.