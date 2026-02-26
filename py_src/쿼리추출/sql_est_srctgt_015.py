## python3 sql_est_srctgt_015.py /home/p190872/chksrc/test
## python3 sql_est_srctgt_015.py /home/p190872/chksrc/SIDHUB
## python3 sql_est_srctgt_015.py /NAS/MIDP/DBMSVC/MIDP/SID/SRC/SIDHUB
## python3 sql_est_srctgt_015.py /NAS/MIDP/DBMSVC/MIDP/SID
## python3 sql_est_srctgt_015.py /NAS/MIDP/DBMSVC/MIDP/TMT
## python3 sql_est_srctgt_015.py /NAS/MIDP/DBMSVC/MIDP/TDIA
## python3 sql_est_srctgt_015.py /NAS/MIDP/DBMSVC/MIDP/TDM
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
## 아래 원본소스 기준 아래내용적용 수정한 전체소스
## WITH 내부까지 포함 분석 버전
## 서브쿼리 내부 테이블도 추출하는 고급 분석 버전
## insert into sidtest.ad1901_rgb_ac190212_svc 포함쿼리구문 제외
## 
## 최종소스 실행시
## "insert overwrite table tb_1
## select * from tb_2;"
## 쿼리는 정상적으로 추출되는데
## 
## "insert overwrite tb_1
## select * from tb_2;"
## 의 경우는 소스테이블만 tb_2 값이 추출되고 타겟테이블이 빈값으로 추출됩니다.
## 
## 최종소스 로직 최대한 유지한 상태에서 "insert overwrite tb_1 select * from tb_2"
## 의 경우에서도 "tb_1" 이 타켓테이블로 추출되도록 원본 최대한 유지하면서 수정보완한 전체소스
## 
## --sql_est_srctgt_012.py
## 최종소스를 실행하면 "[쿼리예제]" 의 경우 분석결과
## target_table로 "${T_A}.TB_0"만 파일생성
## source_table 빈값이 되고 있습니다.
## 
## [쿼리예제]
## "insert overwrite table ${T_A}.TB_0
## (
## col_1,col_2
## )
## WITH T_W1 AS (
## SELECT col_1,col_2
##   FROM TB_1
##  where 1=1
##     and not exists (select 1 from ${T_A}.tb_2 )
## )
## , T_W2 AS ( select col_1,col_2 from tb_3)
## select col_1,col_2 from tb_4
## ;"
## 
## 요청은 "[쿼리예제]" 분석시
## source_table 이 TB_1, ${T_A}.tb_2, tb_3,tb_4 으로 추출되고
## target_table 은 ${T_A}.TB_0 으로 추출되도록 
## 기존로직 유지하면서 보완한 전체소스
## 첨부한 이미지의 sql_1.jpg와 sql_2.jpg의 내부텍스트를 병합한 쿼리 분석 결과가 
## csv_source_target_table_결과.jpg 결과의 내부텍스트처럼 생성되었는데 원인파악후 수정보완한 전체소스
## 
## sql_1.jpg: INSERT OVERWRITE TABLE ${T_SID}.H_MSID_LDONG_M_PPL_PRST_S PARTITION (STRD_YM='${C_MONTH}') + WITH source_update AS ( ... FROM ${T_SID}.H_MSID_BLOCK_M_PPL_PRST_S A ...
## sql_2.jpg: new_data AS ( ... FROM ${T_SID}.H_MSID_LDONG_M_PPL_PRST_S t + JOIN source_update s ... existing_data AS ( ...
## 결과.jpg: target_table이 AS, s 로 잘못 추출됨
## 
## 원인 분석:
## 1. PARTITION (...) 처리 문제: INSERT OVERWRITE TABLE 테이블명 PARTITION (...) 에서 PARTITION 키워드 뒤 괄호를 처리하지 못해 target_table 추출이 실패하고 AS가 target으로 잡힘
## 2. JOIN source_update s에서 alias s가 target으로 추출: CTE 이름인 source_update의 alias s가 target_table로 잡힘
## 버그 1: extract_target_tables에서 INSERT OVERWRITE TABLE 테이블명 PARTITION (...) 패턴의 정규식이 PARTITION 뒤의 내용을 테이블명 이후 토큰으로 계속 읽어서 AS, s가 target으로 잡힘
## 버그 2: JOIN source_update s 에서 alias s가 target으로 추출됨 → INSERT OVERWRITE TABLE ... PARTITION (...) WITH ... 패턴에서 WITH 이후를
##  INSERT INTO 패턴으로 재매칭하면서 WITH source_update AS (... JOIN ... s 가 INSERT INTO ([^\s(]+) 에 매칭됨
## 
## 정확히 분석 후 수정합니다.
## UPDATE\s+([^\s(]+) 패턴이 source_update AS와 source_update s의 update 부분에 매칭되어 AS, s를 target_table로 추출하고 있었습니다. 
## source_update라는 CTE 이름 안의 update 부분을 UPDATE DML 키워드로 오인한 것입니다.
## 수정 방법: UPDATE 패턴에 \b word boundary를 앞에 추가해도 source_update에서 update는 word boundary가 있으므로 안 되고, 
## ^ 또는 앞에 공백/줄바꿈이 오는 경우만 매칭되도록 lookahead 추가가 필요합니다.
## 확인됐습니다. \b가 없어서 source_update의 update 부분에 매칭됩니다. 모든 패턴에 \b word boundary를 추가하고, 
## 추가로 PARTITION 절 처리와 INSERT target 추출 후 PARTITION/CLUSTER 등 후속 키워드 제거도 보완합니다.
## 
## target과 source 모두 정상 추출됩니다. 이제 이전 테스트 케이스(주석 포함 쿼리)도 함께 회귀 테스트합니다.
## 원인 및 수정 내용
## 문제
## 1) target_table = AS
## 2) target_table = s
## 3) PARTITION (...) 후속 처리
## 4) 모든 패턴 안전성
## 
## 원인
## 1) UPDATE\s+([^\s(]+) 패턴이 source_update AS의 update AS 부분에 매칭
## 2) 같은 패턴이 source_update s(JOIN alias)의 update s 부분에 매칭
## 3) INSERT OVERWRITE TABLE 테이블명 PARTITION (...) 에서 테이블명 뒤 PARTITION 절이 target에 섞일 수 있는 구조
## 4) 기존 패턴에 \b word boundary가 없는 것들 존재
## 
## 수정
## 1) (?<![_\w])\bUPDATE\s+ — 앞에 _ 포함 word 문자가 없을 때만 매칭되도록 lookbehind 추가
## 2) 위와 동일 수정으로 해결
## 3) POST_TABLE_KEYWORDS 집합 추가하여 테이블명으로 오인되는 후속 키워드 필터링
## 4) 전체 패턴에 \b 추가

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
# 11.5. INSERT 컬럼목록 괄호 제거  ★ 신규 추가
#
# INSERT [OVERWRITE] [TABLE] table_name (col1, col2, ...) WITH/SELECT/VALUES
# 패턴에서 테이블명 직후의 컬럼목록 괄호를 제거하여
# strip_function_args가 테이블명을 함수로 오인하지 않도록 처리
# ==============================

def strip_insert_col_list(sql):
    """
    INSERT ... table_name\n(col1, col2)\nWITH|SELECT|VALUES 패턴에서
    테이블명 바로 뒤 컬럼목록 괄호를 __INSERT_COLS__ 로 치환.
    """
    # INSERT (OVERWRITE) (TABLE) <table_name> (<col_list>) 뒤에
    # WITH / SELECT / VALUES 가 오는 패턴
    # table_name은 영문자, 숫자, _, $, {, }, . 허용 (변수 포함)
    pattern = re.compile(
        r'(INSERT\s+(?:OVERWRITE\s+)?(?:TABLE\s+)?'   # INSERT [OVERWRITE] [TABLE]
        r'[\w${}.\-]+)'                                # table_name
        r'(\s*\([^)]*\))'                              # (col_list)  ← 제거 대상
        r'(\s*(?:WITH|SELECT|VALUES)\b)',              # WITH|SELECT|VALUES
        re.IGNORECASE | re.DOTALL
    )
    return pattern.sub(r'\1 __INSERT_COLS__\3', sql)


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

        if fn_name in ('FROM', 'JOIN', 'USING', 'WITH', 'ON', 'AS',
                       'SELECT', 'WHERE', 'HAVING', 'SET',
                       'INSERT', 'UPDATE', 'DELETE', 'MERGE',
                       'CREATE', 'TABLE', 'VIEW', 'INTO',
                       'WHEN', 'THEN', 'ELSE', 'AND', 'OR', 'NOT',
                       'EXISTS', 'IN', 'ANY', 'ALL', 'CASE'):
            result.append(sql[i:paren_start+1])
            i = paren_start + 1
            continue

        # ★ 추가: fn_start 직전 문자가 '.', '}', '$' 이거나
        #          fn_start 이전 토큰이 변수/테이블명처럼 보이면
        #          (즉, ${T_A}.TB_0 형태에서 TB_0 이 fn_name으로 잡힌 경우)
        #          함수가 아닌 테이블명으로 간주하고 괄호만 스킵
        if fn_start > 0:
            prev_char = sql[fn_start - 1]
            if prev_char in ('.', '}', '$'):
                # 괄호 내부를 스킵하되 함수 치환은 하지 않음
                result.append(sql[i:paren_start+1])
                i = paren_start + 1
                continue

        result.append(sql[i:fn_start + len(m.group(1))])
        inner, end_pos = extract_paren_content(sql, paren_start)
        result.append('(__FUNC_ARGS__)')
        i = end_pos + 1

    return ''.join(result)



def strip_inline_comments(sql):
    """-- 한 줄 주석 및 /* */ 블록 주석 제거 (파싱 안전성 강화용)"""
    sql = re.sub(r'--[^\n]*', '', sql)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql



def strip_inline_comments(sql):
    """-- 한 줄 주석 및 /* */ 블록 주석 제거 (파싱 안전성 강화용)"""
    sql = re.sub(r'--[^\n]*', '', sql)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql

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

    # -- 주석이 괄호 depth 계산을 방해하지 않도록 사전 제거
    query = strip_inline_comments(query)

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

    # -- 주석이 FROM/JOIN 파싱을 방해하지 않도록 사전 제거
    query = strip_inline_comments(query)
    q = remove_string_literals(query)
    q = strip_select_columns(q)

    # ★ SET 절 내부에 서브쿼리가 있는 경우 (UPDATE ... SET (cols) = (SELECT ... FROM ...))
    # strip_update_set 전에 SET 절 내 괄호 서브쿼리에서 소스 추출
    set_sources = _extract_sources_from_set_subqueries(q)
    sources.update(set_sources)

    q = strip_update_set(q)

    # ★ INSERT 컬럼목록 괄호 제거 (strip_function_args 전에 먼저 실행)
    q = strip_insert_col_list(q)

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
    """
    INSERT/CREATE/UPDATE/DELETE/MERGE/ALTER/DROP/TRUNCATE 대상 테이블 추출.
    - 모든 DML 패턴에 \b word boundary 추가하여 source_update 같은 CTE 이름 내 부분 매칭 방지
    - PARTITION/CLUSTER/STORED/LOCATION 등 테이블명 후속 절 무시
    - INSERT OVERWRITE TABLE ... PARTITION (...) 정상 처리
    """
    targets = set()

    # ★ 테이블명 캡처 후 후속에 오는 PARTITION/CLUSTER/STORED 등 절이 있어도
    #   테이블명만 정확히 추출하도록 ([^\s(]+) 사용 (공백/괄호 전까지)
    # ★ 모든 패턴 앞에 \b 추가 → source_update 내 update 부분 오매칭 방지
    patterns = [
        r'\bINSERT\s+OVERWRITE\s+TABLE\s+([^\s(]+)',
        r'\bINSERT\s+OVERWRITE\s+(?!TABLE\b)([^\s(]+)',
        r'\bINSERT\s+INTO\s+TABLE\s+([^\s(]+)',   # ★ Hive: INSERT INTO TABLE 테이블명
        r'\bINSERT\s+INTO\s+(?!TABLE\b)([^\s(]+)',  # ★ 일반: INSERT INTO 테이블명 (TABLE 키워드 제외)
        r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+)?(?:TEMPORARY\s+|TEMP\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)',  # ★ TEMPORARY/TEMP/OR REPLACE/IF NOT EXISTS 수식어 지원
        r'(?<![_\w])\bUPDATE\s+([^\s(]+)',   # ★ 앞에 word char(_포함)가 없을 때만
        r'\bDELETE\s+FROM\s+([^\s(]+)',
        r'\bMERGE\s+INTO\s+([^\s(]+)',
        r'\bALTER\s+TABLE\s+([^\s(]+)',
        r'\bDROP\s+TABLE\s+([^\s(]+)',
        r'\bTRUNCATE\s+TABLE\s+([^\s(]+)'
    ]

    # PARTITION/CLUSTER/STORED/LOCATION 등 테이블명 뒤 절 키워드
    POST_TABLE_KEYWORDS = {
        'PARTITION', 'CLUSTER', 'STORED', 'LOCATION', 'ROW', 'FORMAT',
        'FIELDS', 'LINES', 'TERMINATED', 'WITH', 'SELECT', 'AS', 'SET',
        'WHERE', 'VALUES', 'ON', 'USING', 'IF'
    }

    for pat in patterns:
        for match in re.finditer(pat, query, re.IGNORECASE):
            raw = match.group(1).strip().rstrip(';,')
            # 테이블명 뒤에 PARTITION 같은 키워드가 붙어있으면 제거
            raw_up = raw.upper()
            for kw in POST_TABLE_KEYWORDS:
                if raw_up == kw:
                    raw = None
                    break
            if not raw:
                continue
            tbl = clean_table(raw)
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