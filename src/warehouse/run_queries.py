"""Analytical Query Runner for Kenya Retail Market Intelligence Data Warehouse.

Executes business intelligence and macroeconomic queries against the PostgreSQL
Star Schema and displays formatted tabular insights.

Usage:
  python src/warehouse/run_queries.py
  python src/warehouse/run_queries.py --query 1
  python src/warehouse/run_queries.py --query 2 --limit 10
"""
import argparse
import os
import re
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# Add project root to sys.path
repo_root = Path(__file__).resolve().parent.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


def get_database_url():
    """Resolves database URL for either local host or docker network execution."""
    if "DATABASE_URL" in os.environ:
        return os.environ["DATABASE_URL"]

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    user = os.getenv("POSTGRES_USER", "retail_user")
    password = os.getenv("POSTGRES_PASSWORD", "retail_password")
    db = os.getenv("POSTGRES_DB", "kenya_market_db")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def parse_queries(sql_path: Path):
    """Parses analytical_queries.sql into named query dictionaries."""
    raw_text = sql_path.read_text(encoding="utf-8")
    blocks = re.split(r"-- -{50,}", raw_text)

    queries = {}
    query_num = 1

    for block in blocks:
        if "query_id:" not in block:
            continue

        # Extract title and business question
        title_match = re.search(r"QUERY\s+(\d+):\s*(.*)", block)
        qid_match = re.search(r"query_id:\s*(\w+)", block)
        question_match = re.search(r"Business Question:\s*(.*?)(?=--\s*query_id|\Z)", block, re.DOTALL)

        # Extract SQL after query_id
        sql_part = re.split(r"--\s*query_id:\s*\w+", block)[-1].strip()

        # Clean trailing comments
        sql_lines = [line for line in sql_part.splitlines() if not line.strip().startswith("--")]
        clean_sql = "\n".join(sql_lines).strip()

        if not clean_sql:
            continue

        num = int(title_match.group(1)) if title_match else query_num
        title = title_match.group(2).strip() if title_match else f"Analytical Query {num}"
        qid = qid_match.group(1).strip() if qid_match else f"query_{num}"
        question = " ".join(question_match.group(1).split()) if question_match else ""

        queries[num] = {
            "num": num,
            "id": qid,
            "title": title,
            "question": question,
            "sql": clean_sql
        }
        query_num += 1

    return queries


def run_query(engine, query_info: dict, limit: int = None):
    """Executes a single analytical query and renders the formatted output."""
    num = query_info["num"]
    title = query_info["title"]
    question = query_info["question"]
    sql = query_info["sql"]

    print("\n" + "=" * 80)
    print(f"QUERY {num}: {title.upper()}")
    print("=" * 80)
    if question:
        print(f"Business Context: {question}\n")

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)

        if df.empty:
            print("Notice: Query returned 0 rows (ensure warehouse has been loaded via load_warehouse.py).")
            return

        total_rows = len(df)
        if limit and limit < total_rows:
            display_df = df.head(limit)
            note = f" (Displaying top {limit} of {total_rows} rows)"
        else:
            display_df = df
            note = f" ({total_rows} rows returned)"

        print(f"Results{note}:")
        print(display_df.to_string(index=False))

    except Exception as e:
        print(f"Execution Error in Query {num}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Kenya Retail Market Intelligence - Warehouse Analytical Query Runner")
    parser.add_argument(
        "--query",
        type=str,
        default="all",
        help="Query number to run (1-5) or 'all' to run all queries (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum rows to display per query (default: 20)",
    )
    args = parser.parse_args()

    sql_file = repo_root / "src" / "warehouse" / "analytical_queries.sql"
    if not sql_file.exists():
        print(f"Error: SQL query file not found at {sql_file}")
        sys.exit(1)

    queries = parse_queries(sql_file)
    if not queries:
        print(f"Error: No queries parsed from {sql_file}")
        sys.exit(1)

    db_url = get_database_url()
    safe_url = db_url.split("@")[0].split(":")[0] + "://***@" + db_url.split("@")[-1]
    print(f"Connecting to Data Warehouse at: {safe_url}")

    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
    except Exception as e:
        print(f"\nDatabase Connection Failed: {e}")
        print("\nPlease ensure PostgreSQL is running:")
        print("  docker compose --profile warehouse up -d postgres")
        print("  or on host port 5433.")
        sys.exit(1)

    if args.query.lower() == "all":
        for num in sorted(queries.keys()):
            run_query(engine, queries[num], limit=args.limit)
    else:
        try:
            target_num = int(args.query)
            if target_num in queries:
                run_query(engine, queries[target_num], limit=args.limit)
            else:
                print(f"Query {target_num} not found. Available queries: {list(queries.keys())}")
        except ValueError:
            print(f"Invalid query selector '{args.query}'. Use 1-5 or 'all'.")

    print("\n" + "=" * 80)
    print("Analytical query execution completed.")
    print("=" * 80)


if __name__ == "__main__":
    main()

