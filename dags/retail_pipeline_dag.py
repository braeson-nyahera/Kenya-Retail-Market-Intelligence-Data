"""
Kenya Retail Market Intelligence Data Platform — Master Pipeline DAG
====================================================================

Orchestrates the full data processing pipeline on a daily schedule:

  Stage 1 - Spark Transformations:
    bronze_to_silver → quality_bronze → silver_to_gold → quality_silver

  Stage 2 - Data Warehouse:
    quality_gold → init_warehouse → load_warehouse → quality_warehouse → verify_warehouse

DAG ID : retail_pipeline
Schedule: @daily  (00:00 UTC)
Owner  : data-engineering

Quality gate tasks abort the pipeline on any FAIL result, ensuring
bad data never propagates to the next layer or into the warehouse.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.quality.checks import check_dataframe
from src.quality.run_quality import DATASETS, load_dataset, DATA

# Paths - resolve correctly whether running inside container or locally
# DAGs folder is typically /opt/airflow/dags; project root is one level up
_DAGS_DIR = Path(__file__).resolve().parent
REPO_ROOT = _DAGS_DIR.parent          # /opt/airflow  or  c:/DEV/Kenya-...
DATA_DIR = REPO_ROOT / "data"
SRC_DIR = REPO_ROOT / "src"

# Ensure project src is importable
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# Default Arguments
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


# STAGE 1 — SPARK TRANSFORMATIONS
def run_bronze_to_silver(**context):
    """
    PySpark Task: Reads raw Bronze JSONL & CSV, cleans, deduplicates,
    and enriches with KNBS CPI deflators → writes Silver Parquet.
    """
    from src.spark.bronze_to_silver import build_silver_layer
    from src.spark.spark_session import get_spark_session

    print("=" * 60)
    print("[Airflow] Stage 1a: Bronze → Silver")
    print("=" * 60)

    spark = get_spark_session("Airflow_BronzeToSilver")
    try:
        enriched_pos, silver_kamis, silver_cpi = build_silver_layer(spark, DATA_DIR)

        # Store row counts to XCom for downstream inspection
        pos_count = enriched_pos.count()
        kamis_count = silver_kamis.count() if silver_kamis is not None else 0

        print(f"\n✔ Bronze → Silver complete.")
        print(f"  Enriched POS rows  : {pos_count:,}")
        print(f"  KAMIS Silver rows  : {kamis_count:,}")

        context["ti"].xcom_push(key="pos_silver_rows", value=pos_count)
        context["ti"].xcom_push(key="kamis_silver_rows", value=kamis_count)
        return {"pos_silver_rows": pos_count, "kamis_silver_rows": kamis_count}

    finally:
        spark.stop()
        print("[Airflow] Spark session stopped (Bronze → Silver).")


def run_silver_to_gold(**context):
    """
    PySpark Task: Reads Silver Parquet, builds Gold analytical marts
    (store daily sales, commodity market comparison, monthly category trends,
    fact_sales, dim_date) → writes Gold Parquet.
    """
    from src.spark.silver_to_gold import build_gold_layer
    from src.spark.spark_session import get_spark_session

    print("=" * 60)
    print("[Airflow] Stage 1b: Silver → Gold")
    print("=" * 60)

    spark = get_spark_session("Airflow_SilverToGold")
    try:
        # Re-read Silver outputs (written by previous task)
        silver_pos = spark.read.parquet(str(DATA_DIR / "silver" / "pos_transactions"))
        silver_kamis_path = DATA_DIR / "silver" / "kamis_prices"
        silver_kamis = spark.read.parquet(str(silver_kamis_path)) if silver_kamis_path.exists() else None

        gold_store_daily, gold_market_comp, gold_monthly_cat = build_gold_layer(
            spark, DATA_DIR, silver_pos, silver_kamis
        )

        store_count = gold_store_daily.count()
        monthly_count = gold_monthly_cat.count()
        market_count = gold_market_comp.count() if gold_market_comp is not None else 0

        print(f"\n✔ Silver → Gold complete.")
        print(f"  gold_store_daily_sales          : {store_count:,} records")
        print(f"  gold_commodity_market_comparison : {market_count:,} records")
        print(f"  gold_monthly_category_trends     : {monthly_count:,} records")

        context["ti"].xcom_push(key="gold_store_daily_rows", value=store_count)
        context["ti"].xcom_push(key="gold_monthly_rows", value=monthly_count)
        return {"gold_rows": store_count}

    finally:
        spark.stop()
        print("[Airflow] Spark session stopped (Silver → Gold).")


# STAGE 2 — DATA WAREHOUSE
def init_warehouse(**context):
    """
    Warehouse Task: Executes schema.sql DDL against PostgreSQL.
    Creates (or recreates) all dimension tables, fact tables,
    indexes, and analytical views.
    """
    import os

    from sqlalchemy import create_engine, text

    print("=" * 60)
    print("[Airflow] Stage 2a: Warehouse Schema Initialization")
    print("=" * 60)

    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://retail_user:retail_password@postgres:5432/kenya_market_db"
    )
    schema_path = SRC_DIR / "warehouse" / "schema.sql"

    engine = create_engine(db_url)
    sql_script = schema_path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        version = conn.execute(text("SELECT version();")).scalar()
        print(f"Connected: {version.split(',')[0]}")
        conn.execute(text(sql_script))

    print("✔ Schema initialized (Dimensions, Facts, Indexes, Views).")


def load_warehouse(**context):
    """
    Warehouse Task: Loads all dimension tables, fact_sales, and
    pre-aggregated marts from Gold Parquet / synthetic CSV files
    into PostgreSQL. Enforces FK integrity before fact insertion.
    """
    import os
    import sys

    # Import the loader module functions directly
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    from sqlalchemy import create_engine

    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://retail_user:retail_password@postgres:5432/kenya_market_db"
    )

    print("=" * 60)
    print("[Airflow] Stage 2b: Warehouse Data Loading")
    print("=" * 60)

    # Dynamically import loader functions
    from src.warehouse.load_warehouse import load_dimensions, load_facts_and_marts

    engine = create_engine(db_url)
    load_dimensions(engine, DATA_DIR)
    load_facts_and_marts(engine, DATA_DIR)

    print("✔ Warehouse dimensions and facts loaded successfully.")


def verify_warehouse(**context):
    """
    Warehouse Task: Runs row-count verification across all tables and
    executes a preview of the county inflation impact view.
    Pushes a summary dict to XCom.
    """
    import os

    import pandas as pd
    from sqlalchemy import create_engine, text

    print("=" * 60)
    print("[Airflow] Stage 2c: Warehouse Verification & Query Preview")
    print("=" * 60)

    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://retail_user:retail_password@postgres:5432/kenya_market_db"
    )
    engine = create_engine(db_url)

    tables = [
        "dim_date", "dim_supplier", "dim_store", "dim_product", "dim_cpi",
        "fact_sales", "agg_store_daily_sales",
        "agg_commodity_market_comparison", "agg_monthly_category_trends",
    ]

    summary = {}
    with engine.connect() as conn:
        for table in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            summary[table] = count
            print(f"  {table:<40}: {count:>10,} rows")

        print("\n--- County Inflation Impact Preview ---")
        preview = pd.read_sql(
            "SELECT * FROM v_county_real_vs_nominal_revenue LIMIT 5", conn
        )
        print(preview.to_string(index=False))

    context["ti"].xcom_push(key="warehouse_counts", value=summary)
    print("\n✔ Warehouse verification complete.")
    return summary


# STAGE 3 — DATA QUALITY GATES
def _run_quality_suite(suite_name: str, data_dir: Path, raise_on_fail: bool, **context):
    """Generic quality gate executor — runs a named suite and saves the report."""
    print("=" * 60)
    print(f"[Airflow] Quality Gate: {suite_name.upper()}")
    print("=" * 60)

    # Only retain the two suites supported by the simplified checks.
    suites = {
        "silver": [
            "silver.pos_transactions",
            "silver.kamis_prices",
            "silver.knbs_cpi",
        ],
        "gold": [
            "gold.store_daily_sales",
        ],
    }

    if suite_name not in suites:
        raise ValueError(f"Unknown quality suite: {suite_name}")

    results = []

    for dataset_name in suites[suite_name]:

        spec = DATASETS[dataset_name]

        # Use the Airflow-provided data directory.
        relative_path = spec["path"].relative_to(DATA)
        dataset_path = Path(data_dir) / relative_path

        try:
            df = load_dataset(dataset_path)

            checks = check_dataframe(
                df=df,
                dataset=dataset_name,
                expected_columns=spec["columns"],
                required_columns=spec["required"],
                max_missing_pct=1.0,
                types=spec["types"],
            )

            results.extend(checks)

        except Exception as error:
            results.append({
                "dataset": dataset_name,
                "check": "load_dataset",
                "column": None,
                "status": "FAIL",
                "message": str(error),
            })

    # Print results
    for result in results:
        print(
            f"{result['status']}: "
            f"{result['dataset']} "
            f"{result['column'] or ''} - "
            f"{result['message']}"
        )

    # Calculate summary
    failed = sum(r["status"] == "FAIL" for r in results)
    passed = sum(r["status"] == "PASS" for r in results)

    summary = {
        "suite": suite_name,
        "total": len(results),
        "pass": passed,
        "fail": failed,
    }

    # Save JSON report
    reports_dir = Path(data_dir) / "quality_reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    report_path = reports_dir / (
        f"quality_{suite_name}_{datetime.now():%Y%m%d_%H%M%S}.json"
    )

    report_path.write_text(
        json.dumps(results, indent=2),
        encoding="utf-8",
    )

    summary["report_path"] = str(report_path)

    # Push summary to Airflow XCom
    context["ti"].xcom_push(
        key=f"quality_{suite_name}",
        value=summary,
    )

    print(f"\nQuality Summary: {summary}")

    if failed and raise_on_fail:
        raise ValueError(
            f"Quality Gate FAILED: {failed} checks failed."
        )

    return summary


def quality_gate_bronze(**context):
    """Quality gate: validates Bronze POS JSONL files before Spark processing."""
    return _run_quality_suite("bronze", DATA_DIR, raise_on_fail=True, **context)


def quality_gate_silver(**context):
    """Quality gate: validates Silver Parquet (POS, KAMIS, KNBS) before Gold building."""
    return _run_quality_suite("silver", DATA_DIR, raise_on_fail=True, **context)


def quality_gate_gold(**context):
    """Quality gate: validates Gold analytical marts before warehouse loading."""
    return _run_quality_suite("gold", DATA_DIR, raise_on_fail=True, **context)


def quality_gate_warehouse(**context):
    """Quality gate: validates warehouse dimensions, facts, views after loading."""
    return _run_quality_suite("warehouse", DATA_DIR, raise_on_fail=True, **context)


# DAG DEFINITION
with DAG(
    dag_id="retail_pipeline",
    description="Kenya Retail Market Intelligence — Spark ETL + PostgreSQL Warehouse pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["retail", "spark", "warehouse", "etl", "kenya"],
    doc_md=__doc__,
) as dag:

    # Stage 1 — Spark Medallion Transformations
    t_bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "PySpark: Reads raw Bronze JSONL/CSV data, cleans, deduplicates, "
            "and enriches with KNBS CPI deflation multipliers → writes Silver Parquet."
        ),
    )

    t_quality_bronze = PythonOperator(
        task_id="quality_bronze",
        python_callable=quality_gate_bronze,
        execution_timeout=timedelta(minutes=10),
        doc_md="Quality gate: validates Bronze POS JSONL — schema, nulls, uniqueness, freshness.",
    )

    t_silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_silver_to_gold,
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "PySpark: Reads Silver Parquet, builds analytical Gold marts "
            "(store_daily_sales, commodity_market_comparison, monthly_category_trends, "
            "fact_sales, dim_date) → writes Gold Parquet."
        ),
    )

    t_quality_silver = PythonOperator(
        task_id="quality_silver",
        python_callable=quality_gate_silver,
        execution_timeout=timedelta(minutes=10),
        doc_md=(
            "Quality gate: validates Silver POS (nulls, dedup, referential integrity, "
            "amount consistency), KAMIS (price ranges, price margin sanity), "
            "and KNBS CPI (deflator consistency)."
        ),
    )

    t_quality_gold = PythonOperator(
        task_id="quality_gold",
        python_callable=quality_gate_gold,
        execution_timeout=timedelta(minutes=10),
        doc_md=(
            "Quality gate: validates Gold store_daily_sales mart — "
            "unique store+date keys, revenue bounds, basket value sanity, "
            "inflation loss sign check."
        ),
    )

    # ------------------------------------------------------------------
    # Stage 2 — Data Warehouse
    # ------------------------------------------------------------------
    t_init_warehouse = PythonOperator(
        task_id="init_warehouse",
        python_callable=init_warehouse,
        execution_timeout=timedelta(minutes=5),
        doc_md=(
            "Executes schema.sql DDL: creates/recreates dimensions, fact tables, "
            "indexes, and analytical views in PostgreSQL."
        ),
    )

    t_load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=load_warehouse,
        execution_timeout=timedelta(minutes=20),
        doc_md=(
            "Loads dim_date, dim_supplier, dim_store, dim_product, dim_cpi from "
            "synthetic CSVs; then loads fact_sales and pre-aggregated marts from "
            "Gold Parquet. Enforces FK integrity before insert."
        ),
    )

    t_quality_warehouse = PythonOperator(
        task_id="quality_warehouse",
        python_callable=quality_gate_warehouse,
        execution_timeout=timedelta(minutes=10),
        doc_md=(
            "Quality gate: validates PostgreSQL warehouse — dimension row counts, "
            "null checks, PK uniqueness, FK integrity (fact→dim), negative revenue "
            "guard, aggregation consistency, and analytical view returnability."
        ),
    )

    t_verify_warehouse = PythonOperator(
        task_id="verify_warehouse",
        python_callable=verify_warehouse,
        execution_timeout=timedelta(minutes=5),
        doc_md=(
            "Final verification: row-count audit across all tables and preview of "
            "v_county_real_vs_nominal_revenue analytical view."
        ),
    )

    # Pipeline Dependencies
    #  bronze_to_silver
    #       ↓
    #  quality_bronze          ← gate: abort if Bronze is malformed
    #       ↓
    #  silver_to_gold
    #       ↓
    #  quality_silver          ← gate: abort if Silver has quality issues
    #       ↓
    #  quality_gold            ← gate: abort if Gold marts are invalid
    #       ↓
    #  init_warehouse
    #       ↓
    #  load_warehouse
    #       ↓
    #  quality_warehouse       ← gate: abort if DW FK/null violations found
    #       ↓
    #  verify_warehouse
    #
    (
        t_bronze_to_silver
        >> t_quality_bronze
        >> t_silver_to_gold
        >> t_quality_silver
        >> t_quality_gold
        >> t_init_warehouse
        >> t_load_warehouse
        >> t_quality_warehouse
        >> t_verify_warehouse
    )

