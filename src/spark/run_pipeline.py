"""Master PySpark Pipeline Orchestrator for Kenya Retail Market Intelligence Data.

Executes end-to-end Medallion transformations:
  Bronze (Raw Ingestion) -> Silver (Cleaning & Deflation Enrichment) -> Gold (Analytical Marts & DW Facts)

Usage:
  python src/spark/run_pipeline.py
"""
import sys
import time
from pathlib import Path

# Add project root to sys.path so modules can be imported when run directly
repo_root = Path(__file__).resolve().parent.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.spark.bronze_to_silver import build_silver_layer
from src.spark.silver_to_gold import build_gold_layer
from src.spark.spark_session import get_spark_session


def main():
    start_time = time.time()
    print("=" * 70)
    print("Kenya Retail Market Intelligence Data Platform")
    print("Starting PySpark Medallion Pipeline (Bronze -> Silver -> Gold)")
    print("=" * 70)

    data_dir = repo_root / "data"
    if not data_dir.exists():
        print(f"Error: Data directory not found at {data_dir}")
        sys.exit(1)

    print(f"Project root: {repo_root}")
    print(f"Data directory: {data_dir}")

    # 1. Initialize Spark
    print("\n[Step 1/3] Initializing PySpark Session...")
    spark = get_spark_session("KenyaRetail_Medallion_Pipeline")
    print(f"PySpark version: {spark.version}")

    try:
        # 2. Bronze to Silver Transformation
        print("\n[Step 2/3] Executing Bronze -> Silver Transformations...")
        enriched_pos, silver_kamis, silver_cpi = build_silver_layer(spark, data_dir)

        # 3. Silver to Gold Transformation
        print("\n[Step 3/3] Executing Silver -> Gold Analytical Marts...")
        gold_store_daily, gold_market_comp, gold_monthly_cat = build_gold_layer(
            spark, data_dir, enriched_pos, silver_kamis
        )

        elapsed = round(time.time() - start_time, 2)
        print("\n" + "=" * 70)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 70)
        print(f"Status:             SUCCESS")
        print(f"Elapsed Time:       {elapsed} seconds")
        print("\nGenerated Gold Analytical Marts (in data/gold/):")
        print(f" - gold_store_daily_sales:          {gold_store_daily.count()} records")
        if gold_market_comp is not None:
            print(f" - gold_commodity_market_comparison: {gold_market_comp.count()} records")
        print(f" - gold_monthly_category_trends:    {gold_monthly_cat.count()} records")

        print("\nSample Preview: gold_store_daily_sales (Top 5):")
        gold_store_daily.show(5, truncate=False)

        if gold_market_comp is not None:
            print("\nSample Preview: gold_commodity_market_comparison (Top 5):")
            gold_market_comp.show(5, truncate=False)

        print("=" * 70)

    finally:
        spark.stop()
        print("Spark session stopped cleanly.")


if __name__ == "__main__":
    main()

