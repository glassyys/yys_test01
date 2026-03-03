import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC_DIR = ROOT / "src"
OUTPUT_CSV = ROOT / "source_target_tables.csv"

COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
COMMENT_LINE_RE = re.compile(r"--.*?$", re.MULTILINE)

TARGET_PATTERNS = [
    ("CREATE_TABLE", re.compile(r"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE)),
    ("CREATE_VIEW", re.compile(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE)),
    ("INSERT", re.compile(r"\bINSERT\s+(?:OVERWRITE\s+TABLE|INTO)\s+([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE)),
    ("UPDATE", re.compile(r"\bUPDATE\s+([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE)),
    ("MERGE", re.compile(r"\bMERGE\s+INTO\s+([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE)),
    ("DELETE", re.compile(r"\bDELETE\s+FROM\s+([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE)),
    ("TRUNCATE", re.compile(r"\bTRUNCATE\s+TABLE\s+([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE)),
    ("DROP", re.compile(r"\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE)),
    ("ALTER", re.compile(r"\bALTER\s+TABLE\s+([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE)),
]

SOURCE_PATTERNS = [
    re.compile(r"\bFROM\s+([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE),
    re.compile(r"\bJOIN\s+([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE),
    re.compile(r"\bUSING\s+([`\"\[]?[A-Za-z_][\w.]*[`\"\]]?)", re.IGNORECASE),
]

INVALID_USING_TOKENS = {
    "PARQUET",
    "ORC",
    "DELTA",
    "ICEBERG",
    "CSV",
    "JSON",
    "TEXT",
    "AVRO",
}


def clean_sql(sql_text: str) -> str:
    text = COMMENT_BLOCK_RE.sub(" ", sql_text)
    text = COMMENT_LINE_RE.sub("", text)
    return text


def split_statements(sql_text: str) -> list[str]:
    parts = [p.strip() for p in sql_text.split(";")]
    return [p for p in parts if p]


def normalize_table_name(name: str) -> str:
    return name.strip().strip("`\"[]")


def extract_target(statement: str) -> tuple[str, str] | tuple[None, None]:
    for stmt_type, pattern in TARGET_PATTERNS:
        m = pattern.search(statement)
        if m:
            return stmt_type, normalize_table_name(m.group(1))
    return None, None


def extract_sources(statement: str, target_table: str | None) -> list[str]:
    sources: list[str] = []
    for pattern in SOURCE_PATTERNS:
        for m in pattern.finditer(statement):
            table = normalize_table_name(m.group(1))
            if pattern.pattern.startswith("\\bUSING") and table.upper() in INVALID_USING_TOKENS:
                continue
            sources.append(table)

    unique_sources: list[str] = []
    seen: set[str] = set()
    for s in sources:
        key = s.lower()
        if key == (target_table or "").lower():
            continue
        if key not in seen:
            seen.add(key)
            unique_sources.append(s)
    return unique_sources


def collect_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not SRC_DIR.exists():
        raise FileNotFoundError(f"src 디렉터리가 없습니다: {SRC_DIR}")

    files = sorted(p for p in SRC_DIR.rglob("*") if p.is_file())

    for file_path in files:
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        statements = split_statements(clean_sql(content))
        for idx, statement in enumerate(statements, start=1):
            stmt_type, target_table = extract_target(statement)
            if not target_table:
                continue

            sources = extract_sources(statement, target_table)
            if not sources:
                rows.append(
                    {
                        "file": str(file_path.relative_to(ROOT)),
                        "statement_no": str(idx),
                        "statement_type": stmt_type,
                        "target_table": target_table,
                        "source_table": "",
                    }
                )
                continue

            for src in sources:
                rows.append(
                    {
                        "file": str(file_path.relative_to(ROOT)),
                        "statement_no": str(idx),
                        "statement_type": stmt_type,
                        "target_table": target_table,
                        "source_table": src,
                    }
                )

    return rows


def write_csv(rows: list[dict[str, str]]) -> None:
    headers = ["file", "statement_no", "statement_type", "target_table", "source_table"]
    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = collect_rows()
    write_csv(rows)
    print(f"CSV 생성 완료: {OUTPUT_CSV}")
    print(f"총 {len(rows)}건")


if __name__ == "__main__":
    main()
