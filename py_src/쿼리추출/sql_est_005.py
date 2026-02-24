## 파이썬으로 쿼리추출 소스코드 요청
## 1.추출 대상 확장자
## .sh
## .hql
## .sql
## .uld
## .ld
## 2. 추출 대상 쿼리 유형
## CREATE
## CREATE VIEW
## CREATE TEMPORARY VIEW
## ALTER VIEW
## REPLACE VIEW
## MERGE
## UPSERT
## INSERT
## UPDATE
## DELETE
## SELECT
## CTAS (CREATE TABLE AS)
## -세미콜론(;) 기준으로 완전 문장 추출
## 
## 3. dat 생성 규칙
## 저장 위치:
## 현재실행디렉토리/out/
## 파일명:
## 프로그램명_소스마지막디렉토리_YYYYMMDD_HHMMSS.dat
## 예:
## sql_extractor_projectA_20260215_091530.dat
## 
## 4. dat 컬럼 레이아웃 및 구분자
## 구분자 "|"로path는 절대경로로 아래와 같은 레이아웃으로 생서
## file_path|file_name|path_with_file|query_text
## 
## -파일경로/파일명은 쿼리문 행개수만큼 중복 생성됨
## 
## 파일생성시 아래쿼리의 경우 
## select col_1
##       ,col_2
##  from tb_1
## where a='1'
## 아래와 같은으로 생성하도록 쿼리 보완 전체코드 요청
## file_path|file_name|path_with_file|select col_1
## file_path|file_name|path_with_file|      ,col_2
## file_path|file_name|path_with_file| from tb_1
## file_path|file_name|path_with_file|where a='1'
## 
## 쿼리 유형 컬럼 추가 (C/R/U/D 자동분류)하고 파일 건수 결과도 출력하도록 수정한 전체소스
## 
## 아래 단계로 확장요청
## WITH 절 포함 강화 정규식으로 보완하고
## 파일의 전체행건수도 추가로 출력하는 소스코드 전체 요청
## 
## 아래 단계로 확장요청
## WITH 절 포함 강화 정규식으로 보완하고
## 파일의 전체행건수도 추가로 출력하는 소스코드 전체 요청
## 
## 소스유지하고 구분자만 "^"으로 변경한 전체소스
##
## 기존소스 유지하고 세미콜론(;) 기준 완전 문장 추출할때 ; 도 쿼리문생성시 포함해서 추출한 전체소스
##
## 기존소스 유지하고 쿼리문의 ";"도 "query_text" 항목에 추가하여 파일생성하도록 수정한 전체소스
##
## 기존소스 유지하고 파일명만 아래기준으로 수정한 전체소스
## dat 생성 규칙
## 저장 위치:
## 현재실행디렉토리/out/
## 파일명:
## 프로그램명_소스마지막디렉토리_YYYYMMDD_HHMMSS.dat
## 예:
## sql_extractor_projectA_20260215_091530.dat
## 
## 파일명은 예시처럼이 아닌 
## 프로그램명_소스마지막디렉토리_YYYYMMDD_HHMMSS.dat
## 의 형태인 실행프로그램명과 쿼리소스의마지막디렉토리명으로 변경한 전체소스 다시 생성
## 
## 기존소스 유지하고 아래내용 반영한 전체소스
## WITH 다중 CTE 완전 강화 버전
## 주석 제거 후 추출
## 
## 기존소스 유지하고 아래내용 반영한 전체소스
## WITH 다중 CTE 완전 강화 버전
## 주석 제거 후 추출
## 
## 기존소스 유지하고 아래 문장이 들어간 쿼리는 대문자 포함하여 모두 제거한 수정한 전체소스
## "insert into sidtest.ad1901_rgb_ac190212_svc(svc_mgmt_num)"
## "dual" 
## 
## ■요청 : 기존 소스 유지하고 아래 내용 반영한 전체소스
## 1. MAIN_QUERY_START 에 아래 명령시작 쿼리구문 추가추출
## ALTER, DROP, TRUNCATE, RENAME, DECLARE, BEGINE, EXECUTE, COMMIT, ENT
## 2./* 과 */ 사이의 주석은 제외
## 3. 첫번째 단어가 "#" 인 행 제외
## 
## 추가 
## 기존 소스 유지하고 첫단어가 "/*" 으로 시작하고 "*/"으로 끝나는 주석제외한 로직 반영한 전체소스
## 
## 기존 소스 유지하고
## DBMS_OUTPUT로 시작되는 라인 모두제외한 전체소스

import os
import re
import sys
from datetime import datetime


# ==============================
# MAIN_QUERY_START 키워드 확장
# ==============================
MAIN_QUERY_START = re.compile(
    r'^\s*(SELECT|INSERT|UPDATE|DELETE|MERGE|WITH|ALTER|DROP|TRUNCATE|RENAME|DECLARE|BEGIN|EXECUTE|COMMIT|ENT)\b',
    re.IGNORECASE
)


def remove_comments(sql_text: str) -> str:
    """
    1. /* ... */ 블록 주석 제거
    2. -- 주석 제거
    """

    # 블록 주석 제거 (멀티라인)
    sql_text = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)

    # -- 주석 제거
    sql_text = re.sub(r'--.*', '', sql_text)

    return sql_text


def should_skip_line(line: str) -> bool:
    stripped = line.strip()

    # 빈 줄 제외
    if not stripped:
        return True

    # 첫 단어가 "#"
    if stripped.startswith("#"):
        return True

    # DBMS_OUTPUT로 시작하는 라인 제외 (대소문자 무시)
    if stripped.upper().startswith("DBMS_OUTPUT"):
        return True

    return False


def should_exclude_query(query: str) -> bool:
    q_upper = query.upper()

    # 특정 INSERT 제거
    if "INSERT INTO SIDTEST.AD1901_RGB_AC190212_SVC(SVC_MGMT_NUM)" in q_upper:
        return True

    # dual 포함 제거
    if "DUAL" in q_upper:
        return True

    return False


def extract_queries_from_file(file_path: str):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    # 1. 주석 제거
    content = remove_comments(content)

    lines = content.splitlines()

    queries = []
    current_query = []
    inside_query = False
    paren_balance = 0

    for line in lines:

        if should_skip_line(line):
            continue

        if not inside_query:
            if MAIN_QUERY_START.search(line):
                inside_query = True
                current_query = [line]
                paren_balance = line.count("(") - line.count(")")
        else:
            current_query.append(line)
            paren_balance += line.count("(") - line.count(")")

        # 세미콜론 기준 종료 + 괄호 균형 확인
        if inside_query and ";" in line and paren_balance <= 0:
            full_query = "\n".join(current_query).strip()

            if not should_exclude_query(full_query):
                queries.append(full_query)

            current_query = []
            inside_query = False
            paren_balance = 0

    return queries


def generate_output_filename(source_dir: str):
    program_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    last_dir_name = os.path.basename(os.path.normpath(source_dir))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    file_name = f"{program_name}_{last_dir_name}_{timestamp}.dat"

    output_dir = os.path.join(os.getcwd(), "out")
    os.makedirs(output_dir, exist_ok=True)

    return os.path.join(output_dir, file_name)


def write_dat_file(output_path: str, queries: list):
    with open(output_path, 'w', encoding='utf-8') as f:
        for query in queries:
            # 세미콜론 포함하여 query_text로 저장
            clean_query = query.replace("\n", " ").strip()
            f.write(f"{clean_query}^\n")


def main(source_dir: str):
    all_queries = []

    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.lower().endswith((".sql", ".txt")):
                file_path = os.path.join(root, file)
                extracted = extract_queries_from_file(file_path)
                all_queries.extend(extracted)

    output_file = generate_output_filename(source_dir)
    write_dat_file(output_file, all_queries)

    print(f"총 추출 쿼리 수: {len(all_queries)}")
    print(f"생성 파일: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python 프로그램명.py <소스디렉토리>")
        sys.exit(1)

    source_directory = sys.argv[1]
    main(source_directory)