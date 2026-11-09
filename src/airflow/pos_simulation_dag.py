"""Airflow DAG for randomized POS receipt simulation."""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
POS_EVENT_COUNT = int(os.getenv("POS_SIMULATION_EVENT_COUNT", "10"))

# Kafka may briefly be unavailable during startup; retry those transient runs.
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


def simulate_pos_transactions():
    """Emit receipt events with randomized 30-120 second inter-arrival times."""
    subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "src/pos_simulation/pos_producer.py"),
            "--source",
            str(PROJECT_ROOT / "data/synthetic/historical_pos_lines.csv"),
            "--limit",
            str(POS_EVENT_COUNT),
            "--random-delay-min",
            "30",
            "--random-delay-max",
            "120",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )


with DAG(
    dag_id="kenya_retail_pos_simulation",
    description="Produce randomized POS receipts to Kafka.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["kenya", "retail", "pos", "simulation"],
) as dag:
    produce_pos_transactions = PythonOperator(
        task_id="produce_pos_transactions",
        python_callable=simulate_pos_transactions,
    )
