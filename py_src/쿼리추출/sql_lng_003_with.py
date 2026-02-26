## python3 sql_lng_003_with.py /home/p190872/chksrc/test --mode SIMPLE
## python3 sql_lng_003_with.py /home/p190872/chksrc/test --mode DETAIL
## python3 sql_lng_003_with.py /home/p190872/chksrc/SIDHUB
## python3 sql_lng_003_with.py /NAS/MIDP/DBMSVC/MIDP/SID/SRC/SIDHUB
## python3 sql_lng_003_with.py /NAS/MIDP/DBMSVC/MIDP/SID
## python3 sql_lng_003_with.py /NAS/MIDP/DBMSVC/MIDP/TMT
## python3 sql_lng_003_with.py /NAS/MIDP/DBMSVC/MIDP/TDIA
## python3 sql_lng_003_with.py /NAS/MIDP/DBMSVC/MIDP/TDM

#!/usr/bin/env python3
# sql_lng_003_with.py
#
# 실행방법:
#   python3 sql_lng_003_with.py 절대경로포함소스디렉토리 [--mode SIMPLE|DETAIL]
#
# --mode SIMPLE (기본값)
#   CTE 를 투명하게 처리: 물리 소스 테이블 → 최종 타겟 테이블 만 출력
#   (sql_lng_002.py 기존 동작과 동일)
#
# --mode DETAIL
#   WITH 절 CTE 흐름까지 포함:
#   ① 물리소스  → 최종타겟   (SIMPLE 행과 동일)
#   ② 물리소스  → CTE명      (각 CTE 내부 소스 → CTE 이름)
#   ③ CTE명     → 최종타겟   (최종 FROM/JOIN 에서 참조된 CTE → 타겟)

import os
import re
import sys
import csv
from datetime import datetime

# ==============================
# 1. 파라미터 체크
# ==============================

def parse_args():
    """
    위치인수: 소스 디렉토리 (필수)
    옵션:     --mode SIMPLE|DETAIL  (기본 SIMPLE)
    """
    args     = sys.argv[1:]
    src_dir  = None
    mode     = "SIMPLE"

    i = 0
    while i < len(args):
        if args[i] == "--mode":
            if i + 1 < len(args):
                mode = args[i + 1].upper()
                if mode not in ("SIMPLE", "DETAIL"):
                    print(f"오류: --mode 값은 SIMPLE 또는 DETAIL 이어야 합니다. (입력값: {args[i+1]})")
                    sys.exit(1)
                i += 2
            else:
                print("오류: --mode 다음에 SIMPLE 또는 DETAIL 을 지정하세요.")
                sys.exit(1)
        else:
            if src_dir is None:
                src_dir = args[i]
            i += 1

    if src_dir is None:
        print("사용법: python3 sql_lng_003_with.py 절대경로포함소스디렉토리 [--mode SIMPLE|DETAIL]")
        sys.exit(1)

    src_dir = os.path.abspath(src_dir)
    if not os.path.isdir(src_dir):
        print("오류: 유효한 디렉토리가 아닙니다.")
        sys.exit(1)

    return src_dir, mode


SOURCE_DIR, MODE = parse_args()

# ==============================
# 2. 설정
# ==============================

TARGET_EXTENSIONS = ('.sh', '.hql', '.sql', '.uld', '.ld')
DELIMITER = ","

# 제외할 패턴 목록
EXCLUDE_PATTERNS = [
    "insert into sidtest.ad1901_rgb_ac190212_svc(svc_mgmt_num)",
    "sidtest.ad1901_rgb_ac190212_svc",
]

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

# ==============================
# 3. MAIN QUERY START 패턴
#    DECLARE / BEGIN / COMMIT 제외 (PL/SQL 블록 래퍼는 별도 처리)
# ==============================

MAIN_QUERY_START = re.compile(
    r"""
    \b(
        CREATE\s+OR\s+REPLACE\s+(?:GLOBAL\s+)?(?:TEMPORARY\s+|TEMP\s+)?(?:TABLE|VIEW)|
        CREATE\s+(?:GLOBAL\s+)?(?:TEMPORARY\s+|TEMP\s+)?(?:TABLE|VIEW)|
        CREATE\s+TABLE|
        CREATE\s+VIEW|
        ALTER\s+TABLE|
        ALTER\s+VIEW|
        DROP\s+TABLE|
        DROP\s+VIEW|
        TRUNCATE\s+TABLE|
        REPLACE\s+VIEW|
        MERGE\s+INTO|
        MERGE|
        UPSERT|
        INSERT|
        UPDATE|
        DELETE|
        SELECT|
        WITH|
        EXECUTE
    )\b
    """,
    re.IGNORECASE | re.VERBOSE
)

END_IF_PATTERN = re.compile(r'^\s*END\s+IF\b', re.IGNORECASE)

# DECLARE / BEGIN 블록 감지 패턴 (EXECUTE IMMEDIATE 내부 등 직접 전달 케이스 보정용)
DECLARE_BEGIN_BLOCK_RE = re.compile(r'^\s*(DECLARE|BEGIN)\b', re.IGNORECASE)

# 블록 내부 첫 번째 실질 DML 키워드
INNER_DML_RE = re.compile(
    r'\b(SELECT|INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|TRUNCATE|REPLACE|ALTER)\b',
    re.IGNORECASE
)

# ==============================
# 4. 전처리
#    문자열 리터럴을 보존하면서 주석만 안전하게 제거
# ==============================

def preprocess(content):
    # (1) # 시작 줄 제거 (쉘 스크립트 주석)
    content = "\n".join(
        line for line in content.splitlines()
        if not line.lstrip().startswith("#")
    )
    # (2) DBMS_OUTPUT 줄 제거
    content = "\n".join(
        line for line in content.splitlines()
        if not re.match(r'(?i)^\s*DBMS_OUTPUT', line)
    )
    # (3) 단일행 /* ... */ 블록주석 줄 제거
    content = "\n".join(
        line for line in content.splitlines()
        if not (line.strip().startswith("/*") and line.strip().endswith("*/"))
    )
    # (4) 문자열 리터럴(' " )은 보존하고 -- 주석과 /* */ 블록주석만 제거
    pattern = re.compile(
        r"""
        ('(?:[^']|'')*') |
        ("(?:[^"]|"")*") |
        (--[^\n]*$)      |
        (/\*.*?\*/)
        """,
        re.MULTILINE | re.DOTALL | re.VERBOSE
    )
    def replacer(match):
        if match.group(1) or match.group(2):   # 문자열 리터럴 → 유지
            return match.group(0)
        return ""                               # 주석 → 제거
    return pattern.sub(replacer, content)


# ==============================
# 5. EXECUTE IMMEDIATE 내부 SQL 추출
# ==============================

def extract_execute_immediate(content):
    results = []
    pattern = re.compile(
        r'\bEXECUTE\s+IMMEDIATE\s+\'(.*?)\'',
        re.IGNORECASE | re.DOTALL
    )
    for m in pattern.finditer(content):
        inner_sql = m.group(1).strip()
        if inner_sql:
            results.append(inner_sql)
    return results


# ==============================
# 6. depth 기반 쿼리 추출
# ==============================

def extract_queries_from_file(file_path):

    queries     = []
    total_lines = 0

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            raw = f.read()

        total_lines = raw.count("\n") + 1
        content     = preprocess(raw)

        # EXECUTE IMMEDIATE 내부 SQL 먼저 수집
        ei_queries = extract_execute_immediate(content)

        # EXECUTE IMMEDIATE 부분을 마스킹하여 중복 추출 방지
        masked = re.sub(
            r'\bEXECUTE\s+IMMEDIATE\s+\'.*?\'',
            'EXECUTE_IMMEDIATE_MASKED',
            content,
            flags=re.IGNORECASE | re.DOTALL
        )

        pos    = 0
        length = len(masked)

        while pos < length:

            match = MAIN_QUERY_START.search(masked, pos)
            if not match:
                break

            keyword = match.group(1).upper()
            start   = match.start()

            # ── END IF 제외 ──────────────────────────────────────────
            if keyword.startswith("END"):
                line_start = masked.rfind("\n", 0, start) + 1
                line_end   = masked.find("\n", start)
                if line_end == -1:
                    line_end = length
                if END_IF_PATTERN.match(masked[line_start:line_end]):
                    pos = line_end
                    continue

            # ── depth 기반으로 세미콜론까지 추출 ─────────────────────
            end        = start
            depth      = 0
            in_string  = False
            quote_char = None

            while end < length:
                ch = masked[end]
                if ch in ("'", '"'):
                    if not in_string:
                        in_string  = True
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
                lower_query = query.lower()

                # 제외 패턴 체크
                if any(p.lower() in lower_query for p in EXCLUDE_PATTERNS):
                    pos = end
                    continue

                # FROM DUAL 단독 SELECT 제외
                if ONLY_FROM_DUAL_PATTERN.match(query):
                    pos = end
                    continue

                # ALTER TABLE/VIEW 이외의 ALTER 제외
                if keyword.upper().startswith("ALTER") and \
                   not re.match(r'ALTER\s+(TABLE|VIEW)\b', query, re.IGNORECASE):
                    pos = end
                    continue

                queries.append(query)

            pos = end

        queries.extend(ei_queries)

    except Exception:
        pass

    return queries, total_lines


# ==============================
# 7. SQL TYPE 감지
# ==============================

def detect_real_sql_type(query):
    """
    쿼리의 실질 DML 타입 반환.
    - DECLARE/BEGIN 시작: 내부 첫 번째 실질 DML 반환
    - WITH 시작: 내부 DML 키워드 탐색
    - 그 외: 첫 번째 키워드 반환
    """
    q          = query.strip().upper()
    words      = q.split()
    first_word = words[0] if words else "UNKNOWN"

    # DECLARE / BEGIN 블록 래퍼
    if first_word in ("DECLARE", "BEGIN"):
        m = INNER_DML_RE.search(query)
        return m.group(1).upper() if m else "UNKNOWN"

    # WITH 시작
    if first_word == "WITH":
        if re.search(r'\bINSERT\b', q): return "INSERT"
        if re.search(r'\bUPDATE\b', q): return "UPDATE"
        if re.search(r'\bDELETE\b', q): return "DELETE"
        if re.search(r'\bMERGE\b',  q): return "MERGE"
        return "SELECT"

    # CREATE 계열 통일
    if first_word == "CREATE":
        return "CREATE"

    return first_word


def classify_sql_type(sql_typ):
    sql_up = sql_typ.upper()
    if sql_up in ("CREATE", "INSERT", "MERGE", "REPLACE", "UPSERT", "EXECUTE"):
        return "C"
    elif sql_up == "SELECT":
        return "R"
    elif sql_up in ("UPDATE", "ALTER"):
        return "U"
    elif sql_up in ("DELETE", "DROP", "TRUNCATE"):
        return "D"
    return "R"


# ==============================
# 8. 테이블명 정제
# ==============================

def clean_table(name):
    if not name:
        return None
    name  = name.strip()
    name  = re.split(r'\s+', name)[0]
    name  = name.rstrip(";,")
    name  = name.replace("(", "").replace(")", "")
    upper = name.upper()
    if (not name
            or upper in RESERVED_WORDS
            or upper == "DUAL"
            or name.isdigit()
            or re.match(r'^\d', name)):
        return None
    return name


# ==============================
# 9. 파싱 전처리 헬퍼
# ==============================

def strip_inline_comments(sql):
    sql = re.sub(r'--[^\n]*', '', sql)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql


def remove_string_literals(sql):
    sql = re.sub(r"'[^']*'", "''", sql)
    sql = re.sub(r'"[^"]*"', '""', sql)
    return sql


def extract_paren_content(sql, start):
    depth = 0
    i     = start
    while i < len(sql):
        if sql[i] == '(':
            depth += 1
        elif sql[i] == ')':
            depth -= 1
            if depth == 0:
                return sql[start + 1:i], i
        i += 1
    return sql[start + 1:], len(sql) - 1


def strip_select_columns(sql):
    result = []
    i      = 0
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
                if re.match(r'\bFROM\b', sql_up[j:], re.IGNORECASE):
                    result.append('__COLS__ ')
                    i = j
                    found_from = True
                    break
                if ch == ';':
                    result.append(sql[j:j + 1])
                    i = j + 1
                    found_from = True
                    break
            j += 1
        if not found_from:
            result.append(sql[j:])
            break

    return ''.join(result)


def strip_update_set(sql):
    result = []
    i      = 0
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
        j     = set_pos + len('SET')
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
                up_r = sql_up[j:]
                if (re.match(r'\bWHERE\b', up_r) or re.match(r'\bWHEN\b', up_r)
                        or re.match(r'\bON\b', up_r) or re.match(r'\bFROM\b', up_r)):
                    break
                if ch == ';':
                    break
            j += 1
        result.append('__SET__ ')
        i = j

    return ''.join(result)


def strip_insert_col_list(sql):
    pattern = re.compile(
        r'(INSERT\s+(?:OVERWRITE\s+)?(?:TABLE\s+)?[\w${}.\-]+)'
        r'(\s*\([^)]*\))'
        r'(\s*(?:WITH|SELECT|VALUES)\b)',
        re.IGNORECASE | re.DOTALL
    )
    return pattern.sub(r'\1 __INSERT_COLS__\3', sql)


def strip_function_args(sql):
    SKIP_NAMES = {
        'FROM','JOIN','USING','WITH','ON','AS',
        'SELECT','WHERE','HAVING','SET',
        'INSERT','UPDATE','DELETE','MERGE',
        'CREATE','TABLE','VIEW','INTO',
        'WHEN','THEN','ELSE','AND','OR','NOT',
        'EXISTS','IN','ANY','ALL','CASE'
    }
    result = []
    i      = 0
    length = len(sql)
    sql_up = sql.upper()

    while i < length:
        m = re.search(r'(\b\w+)\s*\(', sql_up[i:])
        if not m:
            result.append(sql[i:])
            break
        fn_start    = i + m.start()
        fn_name     = m.group(1).upper()
        paren_start = i + m.end() - 1

        if fn_name in SKIP_NAMES:
            result.append(sql[i:paren_start + 1])
            i = paren_start + 1
            continue

        if fn_start > 0 and sql[fn_start - 1] in ('.', '}', '$'):
            result.append(sql[i:paren_start + 1])
            i = paren_start + 1
            continue

        result.append(sql[i:fn_start + len(m.group(1))])
        inner, end_pos = extract_paren_content(sql, paren_start)
        result.append('(__FUNC_ARGS__)')
        i = end_pos + 1

    return ''.join(result)


# ==============================
# 10. CTE 분석
# ==============================

def extract_cte_map(query):
    """
    WITH cte1 AS (...), cte2 AS (...) 에서 CTE 이름 → 내부 소스 집합 반환.
    INSERT ... WITH ... 패턴도 처리.
    반환값: { 'CTE명(대문자)': set(소스테이블명, ...) }
    """
    cte_map = {}
    query   = strip_inline_comments(query)

    if 'WITH' not in query.upper():
        return cte_map

    with_match = re.search(r'\bWITH\b', query, re.IGNORECASE)
    if not with_match:
        return cte_map

    pos         = with_match.end()
    length      = len(query)
    q_up        = query.upper()
    DML_KEYWORDS = {'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'}

    while pos < length:
        while pos < length and query[pos] in ' \t\n\r':
            pos += 1
        if pos >= length:
            break

        alias_m = re.match(r'(\w+)', query[pos:])
        if not alias_m:
            break
        alias    = alias_m.group(1)
        alias_up = alias.upper()
        if alias_up in DML_KEYWORDS:
            break
        pos += alias_m.end()

        while pos < length and query[pos] in ' \t\n\r':
            pos += 1
        if pos >= length:
            break

        as_m = re.match(r'\bAS\b', q_up[pos:], re.IGNORECASE)
        if not as_m:
            break
        pos += as_m.end()

        while pos < length and query[pos] in ' \t\n\r':
            pos += 1
        if pos >= length or query[pos] != '(':
            break

        inner, end_pos = extract_paren_content(query, pos)
        pos = end_pos + 1

        cte_map[alias_up] = extract_sources_recursive(inner)

        while pos < length and query[pos] in ' \t\n\r':
            pos += 1
        if pos >= length:
            break
        if query[pos] == ',':
            pos += 1
            continue
        break

    return cte_map


# ==============================
# 11. SET 절 서브쿼리 소스 추출
# ==============================

def _extract_sources_from_set_subqueries(sql):
    sources = set()
    sql_up  = sql.upper()
    length  = len(sql)

    for set_m in re.finditer(r'\bSET\b', sql_up):
        j     = set_m.end()
        depth = 0
        while j < length:
            ch = sql[j]
            if ch == '(':
                depth += 1
                if depth == 1:
                    inner, end_pos = extract_paren_content(sql, j)
                    inner_up = inner.upper()
                    if re.search(r'\bSELECT\b', inner_up) and re.search(r'\bFROM\b', inner_up):
                        sources.update(extract_sources_recursive(inner))
                    j     = end_pos + 1
                    depth = 0
                    continue
                else:
                    j += 1
                    continue
            elif ch == ')':
                depth = max(depth - 1, 0)
            elif depth == 0:
                up_r = sql_up[j:]
                if (re.match(r'\bWHERE\b', up_r) or re.match(r'\bFROM\b', up_r)
                        or re.match(r'\bWHEN\b', up_r) or ch == ';'):
                    break
            j += 1

    return sources


# ==============================
# 12. 소스 테이블 추출 (재귀, 인라인뷰 포함)
# ==============================

def extract_sources_recursive(query):

    sources = set()
    query   = strip_inline_comments(query)
    q       = remove_string_literals(query)
    q       = strip_select_columns(q)

    sources.update(_extract_sources_from_set_subqueries(q))

    q = strip_update_set(q)
    q = strip_insert_col_list(q)
    q = strip_function_args(q)

    length     = len(q)
    kw_pattern = re.compile(r'\b(FROM|JOIN|USING)\b', re.IGNORECASE)

    CLAUSE_END = {
        'WHERE','ON','WHEN','SET','HAVING','GROUP','ORDER',
        'UNION','INTERSECT','EXCEPT','LIMIT','SELECT',
        'INSERT','UPDATE','DELETE','MERGE','WITH',
        'INNER','LEFT','RIGHT','FULL','CROSS',
        'JOIN','FROM','USING'
    }

    for kw_match in kw_pattern.finditer(q):
        j = kw_match.end()

        while j < length and q[j] in ' \t\n\r':
            j += 1
        if j >= length:
            continue

        if q[j] == '(':
            inner, end_pos = extract_paren_content(q, j)
            sources.update(extract_sources_recursive(inner))
            j = end_pos + 1
            alias_m = re.match(r'[\s]+(\w+)', q[j:])
            if alias_m and alias_m.group(1).upper() not in CLAUSE_END:
                j += alias_m.end()
            while j < length and q[j] in ' \t\n\r':
                j += 1
            if j >= length or q[j] != ',':
                continue
            j += 1

        while j < length:
            while j < length and q[j] in ' \t\n\r':
                j += 1
            if j >= length or q[j] in (';', ')'):
                break

            if q[j] == '(':
                inner, end_pos = extract_paren_content(q, j)
                sources.update(extract_sources_recursive(inner))
                j = end_pos + 1
                alias_m = re.match(r'[\s]+(\w+)', q[j:])
                if alias_m and alias_m.group(1).upper() not in CLAUSE_END:
                    j += alias_m.end()
            else:
                tok_m = re.match(r'([^\s,;()\n]+)', q[j:])
                if not tok_m:
                    break
                token    = tok_m.group(1)
                token_up = token.upper().rstrip(',;')
                if token_up in CLAUSE_END:
                    break
                tbl = clean_table(token)
                if tbl:
                    sources.add(tbl)
                j += tok_m.end()
                alias_m = re.match(r'[\s]+([^\s,;()\n]+)', q[j:])
                if alias_m:
                    aw = alias_m.group(1).upper()
                    if aw not in CLAUSE_END and not aw.startswith(','):
                        j += alias_m.end()

            while j < length and q[j] in ' \t\n\r':
                j += 1
            if j < length and q[j] == ',':
                j += 1
            else:
                break

    return sources


# ==============================
# 13. 타겟 테이블 추출
# ==============================

def extract_target_tables(query):
    targets = set()

    patterns = [
        r'\bINSERT\s+OVERWRITE\s+TABLE\s+([^\s(]+)',
        r'\bINSERT\s+OVERWRITE\s+(?!TABLE\b)([^\s(]+)',
        r'\bINSERT\s+INTO\s+TABLE\s+([^\s(]+)',
        r'\bINSERT\s+INTO\s+(?!TABLE\b)([^\s(]+)',
        r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+)?(?:TEMPORARY\s+|TEMP\s+)?'
        r'(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)',
        r'(?<![_\w])\bUPDATE\s+([^\s(]+)',
        r'\bDELETE\s+FROM\s+([^\s(]+)',
        r'\bMERGE\s+INTO\s+([^\s(]+)',
        r'\bMERGE\s+(?!INTO\b)([^\s(]+)',
        r'\bALTER\s+TABLE\s+([^\s(]+)',
        r'\bDROP\s+TABLE\s+([^\s(]+)',
        r'\bTRUNCATE\s+TABLE\s+([^\s(]+)',
    ]

    POST_TABLE_KEYWORDS = {
        'PARTITION','CLUSTER','STORED','LOCATION','ROW','FORMAT',
        'FIELDS','LINES','TERMINATED','WITH','SELECT','AS','SET',
        'WHERE','VALUES','ON','USING','IF'
    }

    for pat in patterns:
        for match in re.finditer(pat, query, re.IGNORECASE):
            raw = match.group(1).strip().rstrip(';,')
            if not raw or raw.upper() in POST_TABLE_KEYWORDS:
                continue
            tbl = clean_table(raw)
            if tbl:
                targets.add(tbl)

    return targets


# ==============================
# 14. 출력 행 생성 (SIMPLE / DETAIL 분기)
# ==============================

def build_rows(cte_map, sources_raw, targets, crud_type, sql_typ,
               abs_path, file, full_path, mode):
    """
    mode='SIMPLE' : 물리소스 → 최종타겟  행만 생성
    mode='DETAIL' : SIMPLE 행 + CTE 흐름 행(물리→CTE, CTE→타겟) 추가 생성
    """
    rows      = []
    cte_names = set(cte_map.keys())   # 대문자 집합

    # ── ① SIMPLE 행: 물리소스 → 최종타겟 ────────────────────────────
    real_sources = set()
    for s in sources_raw:
        if s and s.upper() in cte_names:
            # CTE 이름이면 → CTE 내부 소스로 치환
            real_sources.update(cte_map[s.upper()])
        elif s:
            real_sources.add(s)

    # CTE 이름 자체는 real_sources 에서 제거
    real_sources = {s for s in real_sources if s and s.upper() not in cte_names}

    tgt_set  = targets      if targets      else {None}
    src_set  = real_sources if real_sources else {None}
    has_src  = any(s is not None for s in src_set)
    seen     = set()

    for tgt in sorted(tgt_set, key=lambda x: x or ''):
        for src in sorted(src_set, key=lambda x: x or ''):
            if has_src and src is None:
                continue
            pair = (src, tgt)
            if pair not in seen:
                seen.add(pair)
                rows.append([crud_type, abs_path, file, full_path, sql_typ, src, tgt])

    # ── ② DETAIL 추가 행 (CTE 가 있을 때만) ─────────────────────────
    if mode == "DETAIL" and cte_map and targets:

        # (a) 물리소스 → CTE명
        for cte_name, cte_srcs in cte_map.items():
            for src in sorted(cte_srcs, key=lambda x: x or ''):
                if src and src.upper() not in cte_names:
                    pair = (src, cte_name)
                    if pair not in seen:
                        seen.add(pair)
                        rows.append([crud_type, abs_path, file, full_path,
                                     sql_typ, src, cte_name])

        # (b) CTE명 → 최종타겟
        # 최종 SELECT/FROM 에서 직접 참조된 CTE 이름 우선 사용
        cte_refs = {s for s in sources_raw if s and s.upper() in cte_names}
        if not cte_refs:
            # 직접 참조 없으면 모든 CTE 를 타겟에 연결
            cte_refs = set(cte_map.keys())

        for cte_name in sorted(cte_refs, key=lambda x: x or ''):
            for tgt in sorted(targets, key=lambda x: x or ''):
                pair = (cte_name, tgt)
                if pair not in seen:
                    seen.add(pair)
                    rows.append([crud_type, abs_path, file, full_path,
                                 sql_typ, cte_name, tgt])

    return rows


# ==============================
# 15. 메인
# ==============================

def main():

    total_files       = 0
    total_queries     = 0
    total_output_rows = 0
    total_file_lines  = 0

    program_name    = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    source_last_dir = os.path.basename(SOURCE_DIR.rstrip(os.sep))

    out_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(out_dir, exist_ok=True)

    timestamp       = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode_suffix     = MODE.lower()                           # simple / detail
    output_filename = f"{program_name}_{source_last_dir}_{mode_suffix}_{timestamp}.csv"
    output_path     = os.path.join(out_dir, output_filename)

    rows_buffer = []

    for root, _, files in os.walk(SOURCE_DIR):
        for file in files:
            if not file.lower().endswith(TARGET_EXTENSIONS):
                continue

            total_files += 1
            full_path    = os.path.join(root, file)
            abs_path     = os.path.abspath(root)

            queries, file_lines = extract_queries_from_file(full_path)

            total_file_lines += file_lines
            total_queries    += len(queries)

            for query in queries:

                sql_typ   = detect_real_sql_type(query)
                crud_type = classify_sql_type(sql_typ)

                cte_map     = extract_cte_map(query)
                sources_raw = extract_sources_recursive(query)
                targets     = extract_target_tables(query)

                # 소스도 타겟도 없으면 스킵
                has_anything = bool(sources_raw or targets)
                if not has_anything:
                    continue

                rows = build_rows(
                    cte_map, sources_raw, targets,
                    crud_type, sql_typ,
                    abs_path, file, full_path,
                    MODE
                )

                rows_buffer.extend(rows)
                total_output_rows += len(rows)

    # 출력 행 없으면 파일 미생성
    if total_output_rows == 0:
        print("====================================")
        print(f" SQL 소스/타겟 테이블 추출 완료 [{MODE}]")
        print("====================================")
        print("CSV 파일 생성 대상이 없습니다.")
        print("====================================")
        return

    with open(output_path, 'w', newline='', encoding='utf-8') as out_file:
        writer = csv.writer(out_file, delimiter=DELIMITER)
        writer.writerow([
            "crud_type", "abs_path", "file", "full_path",
            "sql_typ", "source_table", "target_table",
        ])
        writer.writerows(rows_buffer)

    print("====================================")
    print(f" SQL 소스/타겟 테이블 추출 완료 [{MODE}]")
    print("====================================")
    print(f"실행 모드             : {MODE}")
    print(f"CSV 파일 위치        : {output_path}")
    print(f"처리 파일 건수        : {total_files}")
    print(f"추출 쿼리 건수        : {total_queries}")
    print(f"생성 행 건수          : {total_output_rows}")
    print(f"전체 파일 총 행 건수  : {total_file_lines}")
    print("====================================")


if __name__ == "__main__":
    main()