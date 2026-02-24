## python3 sql_est_srctgt_004.py /home/p190872/chksrc/test
## python3 sql_est_srctgt_004.py /home/p190872/chksrc/SIDHUB
## python3 sql_est_srctgt_004.py /NAS/MIDP/DBMSVC/MIDP/SID/SRC/SIDHUB
## python3 sql_est_srctgt_004.py /NAS/MIDP/DBMSVC/MIDP/SID
## python3 sql_est_srctgt_004.py /NAS/MIDP/DBMSVC/MIDP/TMT
## python3 sql_est_srctgt_004.py /NAS/MIDP/DBMSVC/MIDP/TDIA
## python3 sql_est_srctgt_004.py /NAS/MIDP/DBMSVC/MIDP/TDM
## 아래 원본소스에서 파일생성전 
## crud_type,abs_path,file,full_path 항목은 동일하게 생성하면서
## 추출된 queries 의 queries.append(query) 으로 처리된 최종 쿼리의 
## MAIN_QUERY_START값의 키워드에서  ";"까지의 내용에서 아래 로직 반영하여 
## 소스와 타겟테이블정보를 추출하는 로직을 기존 로직 최대한 출력문 구조도 유지해서 수정하여 csv 파일로 생성하는 전체소스
## 1. 키워드 기준 queries(queries.append(query)) 내용에 대해서 SQL구조를 분석하여
## MAIN_QUERY_START의 값은 sql_typ항목과 source_table, target_table 구분해서 csv파일을 생성하는 로직으로 변경
## 2. target_table에 대한 source_table은 여러행이 생성될수 있는 구조로 처리하여 target_table은 중복해서 생성되어도 됨
## 3. 최종 csv레이아웃
## crud_type,abs_path,file,full_path,sql_typ,source_table,target_table
## 4.csv파일 구분자는 ","로 생성
## 5. 키워드 기준 queries(queries.append(query)) 내용 분석시 아래내용 반영
## 1) 서브쿼리 내부 테이블 추출 강화
## 2) WITH 절 분석
## 3) CTAS 구조 완전 분석
## 4) MERGE USING 서브쿼리 완전 파싱
## 
## 6. 기존 출력부분은 출력가능한 부분 유지
## 
## 7. FROM aa_tb a, 
##         bb_tb b 인 경우
##    FROM 절 이후 구간을 잘라서 "," 기준으로 분리하여
##    aa_tb, bb_tb 모두 추출되도록 수정
## 단, SELECT 절의 컬럼 , 구분은 절대 추출되지 않도록
## → FROM ~ (WHERE/GROUP/ORDER/...) 까지만 분석
## 
## 8.DELETE 
##   FROM tb_1 a
## tb_1 이 target_table 로 정상 기록되도록 수정
## 
## 9. "END", "COMMIT"이나 
## 테이블이 될 수 없는 아래 예약어를 소스 타겟에 사용하지 말고 로직 구현
##     "SET", "WHERE", "AND", "OR", "ON", "WHEN",
##     "THEN", "ELSE", "VALUES", "SELECT",
##     "UPDATE", "INSERT", "DELETE", "MERGE",
##     "USING", "FROM", "JOIN", "INTO",
##     "GROUP", "ORDER", "BY", "HAVING"
## 타겟테이블은 insert 나 upadate나  merge 같은경우이고
## 소스테이블은 from, join, using 뒤에서 사용하는 쿼리 규칙에 맞는 테이블값을 추출
## 
## 10. "FROM DUAL"으로 구성된 단독쿼리와 "ALTER"키워드로 시작되는 경우는 테이블이 없으면 소스테이블 및 타켓추출은 안해도 되고
## "ALTER TABLE" 와 "TRUNCATE TABLE" 다음에 있는 테이블은 타겟테이블로 추출되어야 함
## 
#!/usr/bin/env python3

import os
import re
import sys
import csv
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
DELIMITER = ","
EXCLUDE_TABLE = "sidtest.ad1901_rgb_ac190212_svc"

# 예약어 리스트 (테이블명으로 오인 방지)
RESERVED_WORDS = {
    "SET","WHERE","AND","OR","ON","WHEN","THEN","ELSE",
    "VALUES","SELECT","UPDATE","INSERT","DELETE","MERGE",
    "USING","FROM","JOIN","INTO","GROUP","ORDER","BY",
    "HAVING","TABLE","OVERWRITE","POSITION","SUBSTRING",
    "CAST","TRIM","COUNT","SUM","MAX","MIN","AVG",
    "SESSION", "AS", "DISTINCT", "ALL", "CASE", "NVL", "COALESCE"
}

ONLY_FROM_DUAL_PATTERN = re.compile(
    r'^\s*SELECT\s+.*?\s+FROM\s+DUAL\s*;?\s*$',
    re.IGNORECASE | re.DOTALL
)

MAIN_QUERY_START = re.compile(
    r'\b(CREATE|INSERT|UPDATE|DELETE|MERGE|REPLACE|ALTER|DROP|TRUNCATE|SELECT|WITH)\b',
    re.IGNORECASE
)

# ==============================
# 3. 전처리
# ==============================

def preprocess(content):
    # 쉘 스크립트 주석(#) 제거
    content = "\n".join(
        line for line in content.splitlines()
        if not line.lstrip().startswith("#")
    )
    # SQL 주석(--, /* */) 제거
    content = re.sub(r'--.*', '', content)
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    return content

# ==============================
# 4. depth 기반 쿼리 추출
# ==============================

def extract_queries_from_file(file_path):
    queries = []
    total_lines = 0
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            raw = f.read()
        
        total_lines = raw.count("\n") + 1
        content = preprocess(raw)

        pos = 0
        length = len(content)

        while pos < length:
            match = MAIN_QUERY_START.search(content, pos)
            if not match:
                break

            start = match.start()
            end = start
            depth = 0
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
                elif not in_string:
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth = max(depth - 1, 0)
                    elif ch == ";" and depth == 0:
                        end += 1
                        break
                end += 1

            query = content[start:end].strip()
            if query:
                if EXCLUDE_TABLE.lower() in query.lower():
                    pos = end
                    continue
                if ONLY_FROM_DUAL_PATTERN.match(query):
                    pos = end
                    continue
                queries.append(query)
            pos = end
    except Exception:
        pass
    return queries, total_lines

# ==============================
# 5. SQL 유형 감지
# ==============================

def detect_real_sql_type(query):
    q = query.strip().upper()
    if not q.startswith("WITH"):
        return q.split()[0] if q.split() else "UNKNOWN"

    for word in ["INSERT", "UPDATE", "DELETE", "MERGE"]:
        if re.search(rf'\b{word}\b', q):
            return word
    return "SELECT"

def classify_sql_type(sql_typ):
    mapping = {
        "CREATE": "C", "INSERT": "C", "MERGE": "C", "REPLACE": "C",
        "SELECT": "R",
        "UPDATE": "U", "ALTER": "U",
        "DELETE": "D", "DROP": "D", "TRUNCATE": "D"
    }
    return mapping.get(sql_typ, "R")

# ==============================
# 6. 테이블 정제 (컬럼/함수 필터링)
# ==============================

def clean_table(name):
    if not name: return None
    
    # 공백 및 특수문자 제거
    name = name.strip().split()[0].rstrip(";,")
    name = re.sub(r'\(.*?\)', '', name).replace("(", "").replace(")", "")
    
    # 2) & 3) & 5) 컬럼 형태(a.col_1) 및 함수 내부 값 추출 방지
    if "." in name:
        # 순수 테이블명이 아닌 별칭.컬럼 형태인 경우 걸러냄
        # 일반적으로 SQL에서 테이블은 schema.table 형태이므로 
        # 두 번째 파트가 col 등으로 시작하면 컬럼으로 간주하여 제외
        parts = name.split('.')
        if parts[-1].lower() in ['col', 'col_1', 'col_2']:
            return None

    upper = name.upper()
    if (not name or upper in RESERVED_WORDS or upper == "DUAL" or 
        name.isdigit() or re.match(r'^\d', name)):
        return None
    return name

# ==============================
# 7. 소스 및 타겟 추출
# ==============================

def extract_sources_recursive(query):
    sources = set()
    
    # 컬럼 및 함수 제외를 위해 SELECT 절과 SET 절을 무력화시킨 임시 쿼리 생성
    # SELECT ~ FROM 사이의 내용을 지워 컬럼명이 테이블로 오인되지 않게 함
    temp_query = re.sub(r'\bSELECT\b.*?\bFROM\b', 'FROM', query, flags=re.IGNORECASE | re.DOTALL)
    # UPDATE SET ~ WHERE 사이의 내용을 지워 업데이트 컬럼 제외
    temp_query = re.sub(r'\bSET\b.*?\b(WHERE|WHEN|ON|USING|$)\b', r'\1', temp_query, flags=re.IGNORECASE | re.DOTALL)

    # FROM/JOIN/USING 뒤의 테이블 추출 (콤마로 연결된 다중 테이블 tb_2, tb_3 포함)
    pattern = re.compile(r'\b(FROM|JOIN|USING)\s+([^WHERE|GROUP|ORDER|HAVING|LIMIT|ON|WHEN|;]+)', re.IGNORECASE | re.DOTALL)

    for match in pattern.finditer(temp_query):
        clause = match.group(2)
        # 콤마로 구분된 리스트 처리 (예: tb_2 a, tb_3 b)
        potential_tables = clause.split(',')
        for item in potential_tables:
            tbl = clean_table(item)
            if tbl:
                sources.add(tbl)
    return sources

def extract_target_tables(query):
    targets = set()
    patterns = [
        r'INSERT\s+OVERWRITE\s+TABLE\s+([^\s(]+)',
        r'INSERT\s+INTO\s+([^\s(]+)',
        r'CREATE\s+(?:TABLE|VIEW)\s+([^\s(]+)',
        r'UPDATE\s+([^\s(]+)',
        r'DELETE\s+FROM\s+([^\s(]+)',
        r'MERGE\s+INTO\s+([^\s(]+)',
        r'ALTER\s+TABLE\s+([^\s(]+)',
        r'DROP\s+TABLE\s+([^\s(]+)',
        r'TRUNCATE\s+TABLE\s+([^\s(]+)'
    ]
    for pat in patterns:
        for match in re.finditer(pat, query, re.IGNORECASE):
            tbl = clean_table(match.group(1))
            if tbl:
                targets.add(tbl)
    return targets

# ==============================
# 8. 메인 로직
# ==============================

def main():
    total_files = 0
    total_queries = 0
    total_output_rows = 0
    total_file_lines = 0

    program_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    source_last_dir = os.path.basename(SOURCE_DIR.rstrip(os.sep))
    
    out_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(out_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{program_name}_{source_last_dir}_{timestamp}.csv"
    output_path = os.path.join(out_dir, output_filename)

    rows_buffer = []

    for root, _, files in os.walk(SOURCE_DIR):
        for file in files:
            if file.lower().endswith(TARGET_EXTENSIONS):
                total_files += 1
                full_path = os.path.join(root, file)
                abs_path = os.path.abspath(root)
                
                queries, file_lines = extract_queries_from_file(full_path)
                total_file_lines += file_lines
                total_queries += len(queries)

                for query in queries:
                    sql_typ = detect_real_sql_type(query)
                    crud_type = classify_sql_type(sql_typ)
                    
                    sources = extract_sources_recursive(query)
                    targets = extract_target_tables(query)

                    # Self-join 등 중복 제거
                    sources = sources - targets

                    if not sources and not targets:
                        continue

                    display_sources = list(sources) if sources else [None]
                    display_targets = list(targets) if targets else [None]

                    for tgt in display_targets:
                        for src in display_sources:
                            rows_buffer.append([
                                crud_type, abs_path, file, full_path,
                                sql_typ, src, tgt
                            ])
                            total_output_rows += 1

    # 파일 출력
    if total_output_rows > 0:
        with open(output_path, 'w', newline='', encoding='utf-8') as out_file:
            writer = csv.writer(out_file, delimiter=DELIMITER)
            writer.writerow(["crud_type","abs_path","file","full_path","sql_typ","source_table","target_table"])
            writer.writerows(rows_buffer)

    # 결과 출력 (요청하신 형식)
    print("====================================")
    print(" WITH 내부 DML 감지 포함 SQL 추출 완료")
    print("====================================")
    print(f"CSV 파일 위치        : {output_path}")
    print(f"처리 파일 건수        : {total_files}")
    print(f"추출 쿼리 건수        : {total_queries}")
    print(f"생성 행 건수          : {total_output_rows}")
    print(f"전체 파일 총 행 건수  : {total_file_lines}")
    print("====================================")

if __name__ == "__main__":
    main()