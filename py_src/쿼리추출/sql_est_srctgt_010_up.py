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
# 4. EXECUTE IMMEDIATE 내부 SQL 추출
# ==============================

def extract_execute_immediate(content):
    """EXECUTE IMMEDIATE '...' 내부 SQL 문자열 추출"""
    results = []
    pattern = re.compile(r'\bEXECUTE\s+IMMEDIATE\s+\'(.*?)\'', re.IGNORECASE | re.DOTALL)
    for m in pattern.finditer(content):
        inner_sql = m.group(1).strip()
        if inner_sql:
            results.append(inner_sql)
    return results


# ==============================
# 5. depth 기반 쿼리 추출
# ==============================

def extract_queries_from_file(file_path):

    queries = []
    total_lines = 0

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            raw = f.read()

        total_lines = raw.count("\n") + 1
        content = preprocess(raw)

        # EXECUTE IMMEDIATE 내부 SQL 먼저 수집
        ei_queries = extract_execute_immediate(content)

        # EXECUTE IMMEDIATE '...' 부분을 마스킹하여 중복 추출 방지
        masked = re.sub(
            r'\bEXECUTE\s+IMMEDIATE\s+\'.*?\'',
            'EXECUTE_IMMEDIATE_MASKED',
            content,
            flags=re.IGNORECASE | re.DOTALL
        )

        pos = 0
        length = len(masked)

        while pos < length:

            match = MAIN_QUERY_START.search(masked, pos)
            if not match:
                break

            keyword = match.group(1).upper()
            start = match.start()
            end = start
            depth = 0
            in_string = False
            quote_char = None

            while end < length:
                ch = masked[end]

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

            query = masked[start:end].strip()

            if query:
                if EXCLUDE_TABLE.lower() in query.lower():
                    pos = end
                    continue
                if ONLY_FROM_DUAL_PATTERN.match(query):
                    pos = end
                    continue

                # ALTER SESSION 등 ALTER TABLE/VIEW 아닌 ALTER는 스킵
                if keyword == 'ALTER':
                    if not re.match(r'ALTER\s+(TABLE|VIEW)\b', query, re.IGNORECASE):
                        pos = end
                        continue

                queries.append(query)

            pos = end

        # EXECUTE IMMEDIATE 내부 SQL 추가
        queries.extend(ei_queries)

    except Exception:
        pass

    return queries, total_lines


# ==============================
# 6. SQL TYPE
# ==============================

def detect_real_sql_type(query):

    q = query.strip().upper()
    first_word = q.split()[0] if q.split() else "UNKNOWN"

    # INSERT INTO ... WITH cte AS (...) SELECT ... 패턴 처리
    # 첫 키워드가 INSERT이면 바로 INSERT 반환
    if first_word not in ("WITH",):
        return first_word

    # WITH 시작인 경우 → 실질 DML 탐색
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
# 7. 테이블 정제
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
# 8. 문자열 제거 (파싱 전처리)
# ==============================

def remove_string_literals(sql):
    result = re.sub(r"'[^']*'", "''", sql)
    result = re.sub(r'"[^"]*"', '""', result)
    return result


# ==============================
# 9. 괄호 내부 추출
# ==============================

def extract_paren_content(sql, start):
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
# 10. SELECT 컬럼절 제거
# ==============================

def strip_select_columns(sql):
    result = []
    i = 0
    sql_up = sql.upper()
    length = len(sql)

    while i < length:
        m = re.search(r'\bSELECT\b', sql_up[i:], re.IGNORECASE)
        if not m:
            result.append(sql[i:])
            break

        sel_pos = i + m.start()
        result.append(sql[i:sel_pos])
        result.append('SELECT ')

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
                    break
            elif depth == 0:
                fm = re.match(r'\bFROM\b', sql_up[j:], re.IGNORECASE)
                if fm:
                    result.append('__COLS__ ')
                    i = j
                    found_from = True
                    break
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
# 11. UPDATE SET 절 제거
# ==============================

def strip_update_set(sql):
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
                    re.match(r'\bON\b', up_remain) or
                    re.match(r'\bFROM\b', up_remain)):
                    break
                if ch == ';':
                    break
            j += 1

        result.append('__SET__ ')
        i = j

    return ''.join(result)


# ==============================
# 12. 함수 호출 내부 제거
# ==============================

def strip_function_args(sql):
    result = []
    i = 0
    length = len(sql)
    sql_up = sql.upper()

    while i < length:
        m = re.search(r'(\b\w+)\s*\(', sql_up[i:])
        if not m:
            result.append(sql[i:])
            break

        fn_start = i + m.start()
        fn_name = m.group(1).upper()
        paren_start = i + m.end() - 1

        if fn_name in ('FROM', 'JOIN', 'USING', 'WITH', 'ON',
                       'SELECT', 'WHERE', 'HAVING', 'SET',
                       'INSERT', 'UPDATE', 'DELETE', 'MERGE',
                       'CREATE', 'TABLE', 'VIEW', 'INTO',
                       'WHEN', 'THEN', 'ELSE', 'AND', 'OR', 'NOT'):
            result.append(sql[i:paren_start+1])
            i = paren_start + 1
            continue

        result.append(sql[i:fn_start + len(m.group(1))])
        inner, end_pos = extract_paren_content(sql, paren_start)
        result.append('(__FUNC_ARGS__)')
        i = end_pos + 1

    return ''.join(result)


# ==============================
# 13. CTE 분석
#     INSERT INTO ... WITH cte AS (...) SELECT 패턴도 처리
# ==============================

def extract_cte_map(query):
    """
    WITH cte1 AS (...), cte2 AS (...) [SELECT|INSERT|UPDATE|DELETE|MERGE] 패턴에서
    CTE 이름과 내부 소스를 추출.
    괄호 depth 기반으로 파싱하여 UNION ALL 등 중첩 구조도 정확히 처리.
    """
    cte_map = {}

    if 'WITH' not in query.upper():
        return cte_map

    # WITH 키워드 위치 찾기 (INSERT ... WITH ... 도 처리)
    with_match = re.search(r'\bWITH\b', query, re.IGNORECASE)
    if not with_match:
        return cte_map

    pos = with_match.end()
    length = len(query)
    q_up = query.upper()

    # DML 종료 키워드 (CTE 목록 이후 실제 DML)
    DML_KEYWORDS = {'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'}

    while pos < length:
        # 공백 스킵
        while pos < length and query[pos] in ' \t\n\r':
            pos += 1

        if pos >= length:
            break

        # CTE alias 이름 읽기
        alias_m = re.match(r'(\w+)', query[pos:])
        if not alias_m:
            break

        alias = alias_m.group(1)
        alias_up = alias.upper()

        # alias가 DML 키워드이면 CTE 목록 종료
        if alias_up in DML_KEYWORDS:
            break

        pos += alias_m.end()

        # 공백 스킵 후 AS 확인
        while pos < length and query[pos] in ' \t\n\r':
            pos += 1

        if pos >= length:
            break

        as_m = re.match(r'\bAS\b', q_up[pos:], re.IGNORECASE)
        if not as_m:
            # AS가 없으면 CTE 구조 아님 - 종료
            break

        pos += as_m.end()

        # 공백 스킵 후 '(' 확인
        while pos < length and query[pos] in ' \t\n\r':
            pos += 1

        if pos >= length or query[pos] != '(':
            break

        # 괄호 depth 기반으로 CTE 내용 추출
        inner, end_pos = extract_paren_content(query, pos)
        pos = end_pos + 1

        # CTE 내부 소스 추출
        sources = extract_sources_recursive(inner)
        cte_map[alias_up] = sources

        # 공백 스킵 후 콤마 또는 DML 키워드 확인
        while pos < length and query[pos] in ' \t\n\r':
            pos += 1

        if pos >= length:
            break

        if query[pos] == ',':
            pos += 1  # 콤마 건너뛰고 다음 CTE
            continue

        # 콤마 없으면 DML 또는 종료
        break

    return cte_map


# ==============================
# 13.5 SET 절 서브쿼리 소스 추출
#      UPDATE ... SET (cols) = (SELECT ... FROM ...) 패턴에서
#      SET 절 내부 괄호 서브쿼리의 소스 테이블 추출
# ==============================

def _extract_sources_from_set_subqueries(sql):
    """
    SET 이후 depth>0 괄호 안에 SELECT가 포함된 경우
    해당 서브쿼리에서 소스 테이블 추출.
    예: SET (col1, col2) = (SELECT ... FROM tb1, tb2 WHERE ...)
    """
    sources = set()
    sql_up = sql.upper()
    length = len(sql)

    # SET 키워드 찾기
    set_matches = list(re.finditer(r'\bSET\b', sql_up))

    for set_m in set_matches:
        j = set_m.end()
        depth = 0

        while j < length:
            ch = sql[j]

            if ch == '(':
                depth += 1
                if depth == 1:
                    # 첫 번째 괄호 시작 - 내부 확인
                    inner, end_pos = extract_paren_content(sql, j)
                    inner_up = inner.upper()
                    # 내부에 SELECT ... FROM 패턴이 있으면 서브쿼리로 판단
                    if re.search(r'\bSELECT\b', inner_up) and re.search(r'\bFROM\b', inner_up):
                        sub_sources = extract_sources_recursive(inner)
                        sources.update(sub_sources)
                    j = end_pos + 1
                    depth = 0
                    continue
                else:
                    j += 1
                    continue
            elif ch == ')':
                depth = max(depth - 1, 0)
            elif depth == 0:
                up_remain = sql_up[j:]
                # WHERE/FROM/; 만나면 SET 절 종료
                if (re.match(r'\bWHERE\b', up_remain) or
                    re.match(r'\bFROM\b', up_remain) or
                    re.match(r'\bWHEN\b', up_remain) or
                    ch == ';'):
                    break

            j += 1

    return sources


# ==============================
# 14. 소스 추출 (인라인뷰 포함 재귀)
# ==============================

def extract_sources_recursive(query):

    sources = set()

    q = remove_string_literals(query)
    q = strip_select_columns(q)

    # ★ SET 절 내부에 서브쿼리가 있는 경우 (UPDATE ... SET (cols) = (SELECT ... FROM ...))
    # strip_update_set 전에 SET 절 내 괄호 서브쿼리에서 소스 추출
    set_sources = _extract_sources_from_set_subqueries(q)
    sources.update(set_sources)

    q = strip_update_set(q)
    q = strip_function_args(q)

    length = len(q)

    kw_pattern = re.compile(r'\b(FROM|JOIN|USING)\b', re.IGNORECASE)

    CLAUSE_END = {
        'WHERE', 'ON', 'WHEN', 'SET', 'HAVING', 'GROUP', 'ORDER',
        'UNION', 'INTERSECT', 'EXCEPT', 'LIMIT', 'SELECT',
        'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'WITH',
        'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS',
        'JOIN', 'FROM', 'USING'
    }

    for kw_match in kw_pattern.finditer(q):
        j = kw_match.end()

        while j < length and q[j] in ' \t\n\r':
            j += 1

        if j >= length:
            continue

        if q[j] == '(':
            inner, end_pos = extract_paren_content(q, j)
            sub_sources = extract_sources_recursive(inner)
            sources.update(sub_sources)
            j = end_pos + 1
            # alias 스킵
            alias_m = re.match(r'[\s]+(\w+)', q[j:])
            if alias_m and alias_m.group(1).upper() not in CLAUSE_END:
                j += alias_m.end()
            # 콤마가 없으면 다음 키워드로
            while j < length and q[j] in ' \t\n\r':
                j += 1
            if j >= length or q[j] != ',':
                continue
            j += 1  # 콤마 건너뛰고 while 루프 진입

        while j < length:
            while j < length and q[j] in ' \t\n\r':
                j += 1

            if j >= length or q[j] in (';', ')'):
                break

            if q[j] == '(':
                inner, end_pos = extract_paren_content(q, j)
                sub_sources = extract_sources_recursive(inner)
                sources.update(sub_sources)
                j = end_pos + 1
                alias_m = re.match(r'[\s]+(\w+)', q[j:])
                if alias_m and alias_m.group(1).upper() not in CLAUSE_END:
                    j += alias_m.end()
            else:
                tok_m = re.match(r'([^\s,;()\n]+)', q[j:])
                if not tok_m:
                    break
                token = tok_m.group(1)
                token_up = token.upper().rstrip(',;')

                if token_up in CLAUSE_END:
                    break

                tbl = clean_table(token)
                if tbl:
                    sources.add(tbl)

                j += tok_m.end()

                alias_m = re.match(r'[\s]+([^\s,;()\n]+)', q[j:])
                if alias_m:
                    alias_word = alias_m.group(1).upper()
                    if alias_word not in CLAUSE_END and not alias_word.startswith(','):
                        j += alias_m.end()

            while j < length and q[j] in ' \t\n\r':
                j += 1

            if j < length and q[j] == ',':
                j += 1
            else:
                break

    return sources


# ==============================
# 15. 타겟 추출
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
# 16. 메인
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

                    # CTE 이름 자체는 real_sources에서 제거
                    cte_names = set(cte_map.keys())
                    real_sources = {s for s in real_sources
                                    if s and s.upper() not in cte_names}

                    if not real_sources and not targets:
                        continue

                    if not real_sources:
                        real_sources = {None}
                    if not targets:
                        targets = {None}

                    # 중복 제거: (src, tgt) 조합 유일하게
                    # ★ real_sources에 실제 값이 있으면 src=None 행은 제외
                    has_real_src = any(s is not None for s in real_sources)
                    seen_pairs = set()
                    for tgt in sorted(targets, key=lambda x: x or ''):
                        for src in sorted(real_sources, key=lambda x: x or ''):
                            # 실제 소스가 있는데 src=None인 행은 출력 제외
                            if has_real_src and src is None:
                                continue
                            pair = (src, tgt)
                            if pair in seen_pairs:
                                continue
                            seen_pairs.add(pair)
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