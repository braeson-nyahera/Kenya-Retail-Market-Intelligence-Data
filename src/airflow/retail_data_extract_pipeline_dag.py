"""Airflow DAG for the Kenya retail market-intelligence batch pipeline.

Deploy this file to Airflow's DAGs folder (or mount this repository's ``src``
directory there). The Airflow worker must install this project's dependencies.
"""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.downloaders.kamis import main as run_kamis
from src.downloaders.knbs import main as run_knbs
from src.downloaders.synthetic_data import main as run_synthetic_data


@contextmanager
def project_working_directory():
    """Run existing scripts from the repository root for their relative paths."""
    original_directory = Path.cwd()
    os.chdir(PROJECT_ROOT)
    try:
        yield
    finally:
        os.chdir(original_directory)


def run_from_project_root(pipeline_step):
    """Execute one existing pipeline entry point with its expected working directory."""
    with project_working_directory():
        pipeline_step()


with DAG(
    dag_id="kenya_retail_market_intelligence",
    description="Download Kenya market data and generate synthetic retail data.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kenya", "retail", "market-intelligence"],
) as dag:
    extract_kamis = PythonOperator(
        task_id="extract_kamis_market_prices",
        python_callable=run_from_project_root,
        op_args=[run_kamis],
    )

    extract_knbs = PythonOperator(
        task_id="extract_knbs_cpi_deflator",
        python_callable=run_from_project_root,
        op_args=[run_knbs],
    )

    generate_synthetic_data = PythonOperator(
        task_id="generate_synthetic_retail_data",
        python_callable=run_from_project_root,
        op_args=[run_synthetic_data],
    )

    extract_kamis >> extract_knbs >> generate_synthetic_data
