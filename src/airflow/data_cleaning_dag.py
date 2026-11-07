"""Airflow DAG for the Bronze-to-Silver cleaning and validation workflow."""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
DATA_DIR = PROJECT_ROOT / "data"


def clean_bronze_to_silver(**context):
    """Clean raw POS, KAMIS, KNBS, and dimension data into Silver Parquet."""
    from src.spark.bronze_to_silver import build_silver_layer
    from src.spark.spark_session import get_spark_session

    spark = get_spark_session("Airflow_DataCleaning")
    try:
        silver_pos, silver_kamis, silver_cpi = build_silver_layer(spark, DATA_DIR)
        summary = {
            "pos_rows": silver_pos.count(),
            "kamis_rows": silver_kamis.count() if silver_kamis is not None else 0,
            "cpi_rows": silver_cpi.count() if silver_cpi is not None else 0,
        }
        context["ti"].xcom_push(key="cleaning_summary", value=summary)
        return summary
    finally:
        spark.stop()


def validate_silver_data():
    """Run the pulled completeness, required-field, and type checks on Silver data."""
    from src.quality.checks import check_dataframe
    from src.quality.run_quality import DATASETS, REPORTS, load_dataset

    results = []
    for name, spec in DATASETS.items():
        if not name.startswith("silver."):
            continue
        dataset = load_dataset(spec["path"])
        results.extend(
            check_dataframe(
                dataset,
                name,
                spec["columns"],
                spec["required"],
                max_missing_pct=1.0,
                types=spec["types"],
            )
        )

    REPORTS.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS / f"airflow_silver_quality_{datetime.now():%Y%m%d_%H%M%S}.json"
    report_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    failures = [result for result in results if result["status"] == "FAIL"]
    if failures:
        raise ValueError(f"Silver quality checks failed: {len(failures)} failures; see {report_path}")
    return {"checks": len(results), "report": str(report_path)}


with DAG(
    dag_id="kenya_retail_data_cleaning",
    description="Clean Bronze retail data into Silver Parquet and validate it.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kenya", "retail", "cleaning", "silver"],
) as dag:
    bronze_to_silver = PythonOperator(
        task_id="clean_bronze_to_silver",
        python_callable=clean_bronze_to_silver,
    )

    validate_silver = PythonOperator(
        task_id="validate_silver_data",
        python_callable=validate_silver_data,
    )

    bronze_to_silver >> validate_silver
