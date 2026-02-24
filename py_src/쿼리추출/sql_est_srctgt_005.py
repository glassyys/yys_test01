## python3 sql_est_srctgt_005.py /home/p190872/chksrc/test
## python3 sql_est_srctgt_005.py /home/p190872/chksrc/SIDHUB
## python3 sql_est_srctgt_005.py /NAS/MIDP/DBMSVC/MIDP/SID/SRC/SIDHUB
## python3 sql_est_srctgt_005.py /NAS/MIDP/DBMSVC/MIDP/SID
## python3 sql_est_srctgt_005.py /NAS/MIDP/DBMSVC/MIDP/TMT
## python3 sql_est_srctgt_005.py /NAS/MIDP/DBMSVC/MIDP/TDIA
## python3 sql_est_srctgt_005.py /NAS/MIDP/DBMSVC/MIDP/TDM
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

RESERVED_WORDS = {
    "SET","WHERE","AND","OR","ON","WHEN","THEN","ELSE",
    "VALUES","SELECT","UPDATE","INSERT","DELETE","MERGE",
    "USING","FROM","JOIN","INTO","GROUP","ORDER","BY",
    "HAVING","TABLE","OVERWRITE","POSITION","SUBSTRING",
    "CAST","TRIM","COUNT","SUM","MAX","MIN","AVG",
    "SESSION"
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

    content = "\n".join(
        line for line in content.splitlines()
        if not line.lstrip().startswith("#")
    )

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
# 5. SQL TYPE (WITH 내부 DML 감지)
# ==============================

def detect_real_sql_type(query):

    q = query.strip().upper()

    if not q.startswith("WITH"):
        return q.split()[0] if q.split() else "UNKNOWN"

    if re.search(r'\bINSERT\b', q):
        return "INSERT"
    if re.search(r'\bUPDATE\b', q):
        return "UPDATE"
    if re.search(r'\bDELETE\b', q):
        return "DELETE"
    if re.search(r'\bMERGE\b', q):
        return "MERGE"

    return "SELECT"


def classify_sql_type(sql_typ):

    if sql_typ in ("CREATE","INSERT","MERGE","REPLACE"):
        return "C"
    elif sql_typ == "SELECT":
        return "R"
    elif sql_typ in ("UPDATE","ALTER"):
        return "U"
    elif sql_typ in ("DELETE","DROP","TRUNCATE"):
        return "D"
    return "R"


# ==============================
# 6. 테이블 정제
# ==============================

def clean_table(name):

    if not name:
        return None

    name = name.strip()
    name = re.split(r'\s+', name)[0]
    name = name.rstrip(";,")
    name = name.replace("(", "").replace(")", "")

    upper = name.upper()

    if (not name or
        upper in RESERVED_WORDS or
        upper == "DUAL" or
        name.isdigit() or
        re.match(r'^\d', name)):
        return None

    return name


# ==============================
# 7. 문자열 제거 (파싱 전처리)
# ==============================

def remove_string_literals(sql):
    """싱글/더블 쿼트 문자열 리터럴 제거"""
    result = re.sub(r"'[^']*'", "''", sql)
    result = re.sub(r'"[^"]*"', '""', result)
    return result


# ==============================
# 8. 괄호 내부 추출 (재귀 지원)
# ==============================

def extract_paren_content(sql, start):
    """start 위치 '(' 에서 대응 ')' 까지 내용 반환"""
    depth = 0
    i = start
    while i < len(sql):
        if sql[i] == '(':
            depth += 1
        elif sql[i] == ')':
            depth -= 1
            if depth == 0:
                return sql[start+1:i], i
        i += 1
    return sql[start+1:], len(sql)-1


# ==============================
# 9. SELECT 컬럼절 제거
#    FROM 이전의 SELECT 목록을 제거하여 컬럼명이 테이블로 인식되지 않도록 함
# ==============================

def strip_select_columns(sql):
    """
    SELECT ... FROM 구조에서 SELECT 직후 컬럼 목록을 제거.
    괄호 depth=0 기준으로 첫 FROM 키워드 이전을 'SELECT __COLS__' 로 대체.
    재귀적으로 서브쿼리에도 적용되지 않아도 됨 - FROM 이후만 파싱하므로 충분.
    """
    result = []
    i = 0
    sql_up = sql.upper()
    length = len(sql)

    while i < length:
        # SELECT 키워드 탐색
        m = re.search(r'\bSELECT\b', sql_up[i:], re.IGNORECASE)
        if not m:
            result.append(sql[i:])
            break

        sel_pos = i + m.start()
        result.append(sql[i:sel_pos])
        result.append('SELECT ')

        # SELECT 이후부터 depth=0인 첫 FROM 찾기
        j = sel_pos + len('SELECT')
        depth = 0
        found_from = False

        while j < length:
            ch = sql[j]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth < 0:
                    # 상위 괄호로 나감 - 여기서 FROM을 못 찾은 것
                    break
            elif depth == 0:
                # FROM 키워드 체크
                fm = re.match(r'\bFROM\b', sql_up[j:], re.IGNORECASE)
                if fm:
                    # 컬럼 목록 스킵, FROM부터 이어서 처리
                    result.append('__COLS__ ')
                    i = j
                    found_from = True
                    break
                # 세미콜론이나 닫는 괄호에서 SELECT만 있고 FROM 없는 경우
                if ch == ';':
                    result.append(sql[j:j+1])
                    i = j + 1
                    found_from = True
                    break
            j += 1

        if not found_from:
            result.append(sql[j:])
            break

    return ''.join(result)


# ==============================
# 10. UPDATE SET 절 제거
#     SET 이후 WHERE / ; 이전까지 제거
# ==============================

def strip_update_set(sql):
    """
    UPDATE ... SET col=val, col2=val2 WHERE ...
    SET 절의 col=val 부분을 제거하여 컬럼명이 테이블로 인식되지 않도록 함.
    MERGE ... WHEN MATCHED THEN UPDATE SET ... 도 처리.
    """
    # SET ... (WHERE | WHEN | ;) 사이를 제거
    # depth=0 기준
    result = []
    i = 0
    sql_up = sql.upper()
    length = len(sql)

    while i < length:
        m = re.search(r'\bSET\b', sql_up[i:])
        if not m:
            result.append(sql[i:])
            break

        set_pos = i + m.start()
        result.append(sql[i:set_pos])
        result.append('SET ')

        j = set_pos + len('SET')
        depth = 0

        while j < length:
            ch = sql[j]
            if ch == '(':
                depth += 1
            elif ch == ')':
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0:
                up_remain = sql_up[j:]
                if (re.match(r'\bWHERE\b', up_remain) or
                    re.match(r'\bWHEN\b', up_remain) or
                    re.match(r'\bON\b', up_remain)):
                    break
                if ch == ';':
                    break
            j += 1

        result.append('__SET__ ')
        i = j

    return ''.join(result)


# ==============================
# 11. 함수 호출 내부 제거
#     함수명( ... ) 에서 내부 인수를 제거
#     단, USING( 은 제거하지 않음 (소스 테이블 추출 대상)
# ==============================

def strip_function_args(sql):
    """
    SQL 내 함수 호출 패턴: WORD( ... ) 에서 괄호 내부 제거.
    FROM/JOIN/USING 바로 뒤 서브쿼리 괄호는 유지해야 하므로,
    FROM/JOIN/USING 뒤 첫 괄호는 제거하지 않음.
    일반 식별자( 형태만 제거.
    """
    # 함수: 영문자/숫자/_ 로 끝나고 바로 ( 가 오는 패턴
    # FROM/JOIN/USING 뒤 ( 는 보존
    result = []
    i = 0
    length = len(sql)
    sql_up = sql.upper()

    while i < length:
        # 함수 호출 패턴 탐색: word(
        m = re.search(r'(\b\w+)\s*\(', sql_up[i:])
        if not m:
            result.append(sql[i:])
            break

        fn_start = i + m.start()
        fn_name = m.group(1).upper()
        paren_start = i + m.end() - 1  # '(' 위치

        # FROM/JOIN/USING/WITH/ON 뒤 괄호는 보존 (서브쿼리)
        if fn_name in ('FROM', 'JOIN', 'USING', 'WITH', 'ON',
                       'SELECT', 'WHERE', 'HAVING', 'SET',
                       'INSERT', 'UPDATE', 'DELETE', 'MERGE',
                       'CREATE', 'TABLE', 'VIEW', 'INTO',
                       'WHEN', 'THEN', 'ELSE', 'AND', 'OR', 'NOT'):
            result.append(sql[i:paren_start+1])
            i = paren_start + 1
            continue

        # 함수 내부 제거
        result.append(sql[i:fn_start + len(m.group(1))])
        inner, end_pos = extract_paren_content(sql, paren_start)
        result.append('(__FUNC_ARGS__)')
        i = end_pos + 1

    return ''.join(result)


# ==============================
# 12. CTE 분석
# ==============================

def extract_cte_map(query):

    cte_map = {}

    if not query.strip().upper().startswith("WITH"):
        return cte_map

    pattern = re.compile(
        r'(\w+)\s+AS\s*\((.*?)\)\s*(,|SELECT)',
        re.IGNORECASE | re.DOTALL
    )

    for match in pattern.finditer(query):
        alias = match.group(1)
        subquery = match.group(2)
        sources = extract_sources_recursive(subquery)
        cte_map[alias.upper()] = sources

    return cte_map


# ==============================
# 13. 소스 추출 (인라인뷰 포함 재귀)
#     SELECT 컬럼절, SET 절, 함수 인수 제거 후 FROM/JOIN/USING 파싱
# ==============================

def extract_sources_recursive(query):

    sources = set()

    # 문자열 리터럴 제거
    q = remove_string_literals(query)

    # SELECT 컬럼 목록 제거 (FROM 이전)
    q = strip_select_columns(q)

    # UPDATE SET 절 제거
    q = strip_update_set(q)

    # 함수 인수 내부 제거
    q = strip_function_args(q)

    q_up = q.upper()
    length = len(q)

    # FROM/JOIN/USING 키워드 탐색
    kw_pattern = re.compile(r'\b(FROM|JOIN|USING)\b', re.IGNORECASE)

    # FROM 절 종료 예약어
    CLAUSE_END = {
        'WHERE', 'ON', 'WHEN', 'SET', 'HAVING', 'GROUP', 'ORDER',
        'UNION', 'INTERSECT', 'EXCEPT', 'LIMIT', 'SELECT',
        'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'WITH',
        'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS',
        'JOIN', 'FROM', 'USING'
    }

    for kw_match in kw_pattern.finditer(q):
        j = kw_match.end()

        # 공백 스킵
        while j < length and q[j] in ' \t\n\r':
            j += 1

        if j >= length:
            continue

        # 괄호 서브쿼리
        if q[j] == '(':
            inner, end_pos = extract_paren_content(q, j)
            sub_sources = extract_sources_recursive(inner)
            sources.update(sub_sources)
            continue

        # 콤마 구분 테이블 목록 처리
        # FROM tb1 a, tb2 b, ... 형태
        while j < length:
            # 공백 스킵
            while j < length and q[j] in ' \t\n\r':
                j += 1

            if j >= length or q[j] in (';', ')'):
                break

            # 괄호 서브쿼리인 경우
            if q[j] == '(':
                inner, end_pos = extract_paren_content(q, j)
                sub_sources = extract_sources_recursive(inner)
                sources.update(sub_sources)
                j = end_pos + 1
                # alias 스킵
                alias_m = re.match(r'[\s]+(\w+)', q[j:])
                if alias_m and alias_m.group(1).upper() not in CLAUSE_END:
                    j += alias_m.end()
            else:
                # 테이블명 토큰
                tok_m = re.match(r'([^\s,;()\n]+)', q[j:])
                if not tok_m:
                    break
                token = tok_m.group(1)
                token_up = token.upper().rstrip(',;')

                # 예약어이면 FROM 절 종료
                if token_up in CLAUSE_END:
                    break

                tbl = clean_table(token)
                if tbl:
                    sources.add(tbl)

                j += tok_m.end()

                # alias 스킵 (다음 토큰이 예약어/콤마/괄호가 아닌 경우)
                alias_m = re.match(r'[\s]+([^\s,;()\n]+)', q[j:])
                if alias_m:
                    alias_word = alias_m.group(1).upper()
                    if alias_word not in CLAUSE_END and not alias_word.startswith(','):
                        j += alias_m.end()

            # 공백 스킵
            while j < length and q[j] in ' \t\n\r':
                j += 1

            # 콤마이면 다음 테이블 계속
            if j < length and q[j] == ',':
                j += 1
            else:
                break  # 콤마 없으면 FROM 절 종료

    return sources


# ==============================
# 14. 타겟 추출
# ==============================

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
# 15. 메인
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

                    cte_map = extract_cte_map(query)
                    sources = extract_sources_recursive(query)
                    targets = extract_target_tables(query)

                    real_sources = set()

                    for s in sources:
                        if s and s.upper() in cte_map:
                            real_sources.update(cte_map[s.upper()])
                        else:
                            if s:
                                real_sources.add(s)

                    if not real_sources and not targets:
                        continue

                    if not real_sources:
                        real_sources = {None}
                    if not targets:
                        targets = {None}

                    for tgt in targets:
                        for src in real_sources:
                            rows_buffer.append([
                                crud_type,
                                abs_path,
                                file,
                                full_path,
                                sql_typ,
                                src,
                                tgt
                            ])
                            total_output_rows += 1

    # 🔥 소스/타겟 없는 파일은 생성 제외
    if total_output_rows == 0:
        print("====================================")
        print(" WITH 내부 DML 감지 포함 SQL 추출 완료")
        print("====================================")
        print("CSV 파일 생성 대상이 없습니다.")
        print("====================================")
        return

    with open(output_path, 'w', newline='', encoding='utf-8') as out_file:
        writer = csv.writer(out_file, delimiter=DELIMITER)
        writer.writerow([
            "crud_type","abs_path","file","full_path",
            "sql_typ","source_table","target_table"
        ])
        writer.writerows(rows_buffer)

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