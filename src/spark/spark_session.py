"""SparkSession builder for Kenya Retail Market Intelligence Data Platform."""
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession


def get_spark_session(app_name="KenyaRetailMarketDataEngine"):
    """Creates or gets an optimized SparkSession for local or container execution."""
    # Ensure warehouse directory exists
    repo_root = Path(__file__).resolve().parent.parent.parent
    warehouse_dir = repo_root / "data" / ".spark_warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    master = os.getenv("SPARK_MASTER", "local[*]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")  # disable embedded web UI for headless pipeline runs
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

