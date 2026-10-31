"""Bronze to Silver Transformation Pipeline using PySpark.
Performs:
  - Reading raw Kafka Bronze JSONL files & historical files
  - Exploding receipt line items
  - Deduplication (resolving injected dirt duplicates)
  - Data quality filtering & quarantine tracking
  - Normalization of KAMIS price strings & KNBS CPI series
  - Integration: Joins POS with Stores, Commodity Map, and KNBS Deflators to compute real revenue
  - Writing clean, partitioned Silver Parquet tables
"""
import os
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.spark.schemas import (
    HISTORICAL_POS_CSV_SCHEMA,
    KAMIS_RAW_SCHEMA,
    KNBS_CPI_SCHEMA,
    POS_BRONZE_SCHEMA,
    PRODUCT_COMMODITY_MAP_SCHEMA,
    PRODUCTS_SCHEMA,
    STORES_SCHEMA,
    SUPPLIERS_SCHEMA,
)


def process_pos_bronze_to_silver(spark, data_dir: Path):
    """Ingests raw POS receipts from Bronze (JSONL), cleans, explodes, and deduplicates."""
    print("\n--- Processing POS Transactions (Bronze -> Silver) ---")
    bronze_pos_pattern = str(data_dir / "bronze" / "pos" / "*" / "*.jsonl")
    historical_pos_csv = data_dir / "synthetic" / "historical_pos_lines.csv"

    has_jsonl = len(list((data_dir / "bronze" / "pos").glob("*/*.jsonl"))) > 0

    if has_jsonl:
        print(f"Reading Kafka Bronze JSONL files from: {bronze_pos_pattern}")
        raw_df = spark.read.schema(POS_BRONZE_SCHEMA).json(bronze_pos_pattern)
        raw_count = raw_df.count()
        print(f"Loaded {raw_count} raw receipt events from Kafka Bronze files.")

        # Explode receipt line items
        exploded_df = raw_df.select(
            F.col("transaction_id"),
            F.col("store_id"),
            F.col("timestamp"),
            F.explode("lines").alias("line"),
            F.col("_partition"),
            F.col("_offset"),
            F.col("_ingested_at"),
        ).select(
            F.col("transaction_id"),
            F.col("line.line_number").alias("line_number"),
            F.col("store_id"),
            F.col("line.product_id").alias("product_id"),
            F.col("line.quantity").alias("quantity"),
            F.col("line.unit_price_kes").alias("unit_price_kes"),
            F.col("timestamp"),
        )
    elif historical_pos_csv.exists():
        print(f"No Bronze JSONL found. Ingesting from historical CSV: {historical_pos_csv}")
        exploded_df = spark.read.option("header", "true").schema(HISTORICAL_POS_CSV_SCHEMA).csv(str(historical_pos_csv))
    else:
        raise FileNotFoundError(f"Neither Bronze JSONL nor historical POS CSV found in {data_dir}")

    total_lines = exploded_df.count()
    print(f"Total raw POS line items before cleaning: {total_lines}")

    # 1. Deduplication on (transaction_id, line_number)
    deduped_df = exploded_df.dropDuplicates(["transaction_id", "line_number"])
    deduped_count = deduped_df.count()
    duplicates_removed = total_lines - deduped_count
    print(f"Duplicates removed: {duplicates_removed} (Remaining: {deduped_count})")

    # 2. Data Quality Filtering
    valid_df = deduped_df.filter(
        (F.col("product_id").isNotNull())
        & (F.col("quantity") > 0)
        & (F.col("unit_price_kes") >= 0)
    )
    valid_count = valid_df.count()
    quarantined_count = deduped_count - valid_count
    print(f"Anomalous / dirty records filtered out: {quarantined_count} (Valid clean records: {valid_count})")

    # 3. Feature Engineering: parse dates and total amount
    silver_pos = valid_df.withColumn(
        "transaction_timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    ).withColumn(
        "transaction_date", F.to_date(F.col("transaction_timestamp"))
    ).withColumn(
        "transaction_year", F.year(F.col("transaction_timestamp"))
    ).withColumn(
        "transaction_month", F.month(F.col("transaction_timestamp"))
    ).withColumn(
        "transaction_hour", F.hour(F.col("transaction_timestamp"))
    ).withColumn(
        "year_month", F.date_format(F.col("transaction_timestamp"), "yyyy-MM")
    ).withColumn(
        "total_amount_kes", F.round(F.col("quantity") * F.col("unit_price_kes"), 2)
    )

    return silver_pos


def process_kamis_bronze_to_silver(spark, data_dir: Path):
    """Cleans and standardizes KAMIS market commodity prices."""
    print("\n--- Processing KAMIS Market Prices (Bronze -> Silver) ---")
    kamis_csv = data_dir / "kamis" / "kamis_combined_products.csv"
    if not kamis_csv.exists():
        print(f"Warning: {kamis_csv} not found, skipping KAMIS.")
        return None

    raw_kamis = spark.read.option("header", "true").schema(KAMIS_RAW_SCHEMA).csv(str(kamis_csv))
    print(f"Raw KAMIS records: {raw_kamis.count()}")

    # Extract numeric price values from strings like "50.00/Kg"
    cleaned_kamis = (
        raw_kamis.withColumn("commodity_name", F.trim(F.col("commodity")))
        .withColumn("market_name", F.trim(F.col("market")))
        .withColumn("county_name", F.trim(F.col("county")))
        .withColumn("market_date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("wholesale_kes", F.regexp_extract(F.col("wholesale"), r"([0-9]+\.?[0-9]*)", 1).cast(DoubleType()))
        .withColumn("retail_kes", F.regexp_extract(F.col("retail"), r"([0-9]+\.?[0-9]*)", 1).cast(DoubleType()))
        .withColumn("supply_volume_kg", F.col("supply volume").cast(DoubleType()))
        .withColumn("year_month", F.date_format(F.col("market_date"), "yyyy-MM"))
        .filter(F.col("wholesale_kes").isNotNull() | F.col("retail_kes").isNotNull())
        .dropDuplicates(["commodity_name", "market_name", "county_name", "market_date"])
        .select(
            "commodity_name",
            "market_name",
            "county_name",
            "market_date",
            "year_month",
            "wholesale_kes",
            "retail_kes",
            "supply_volume_kg",
        )
    )

    print(f"Cleaned KAMIS records: {cleaned_kamis.count()}")
    return cleaned_kamis


def process_knbs_bronze_to_silver(spark, data_dir: Path):
    """Cleans and standardizes KNBS CPI and Deflator multiplier series."""
    print("\n--- Processing KNBS CPI Deflator Series (Bronze -> Silver) ---")
    cpi_csv = data_dir / "knbs_downloads" / "cpi_deflator_series.csv"
    if not cpi_csv.exists():
        print(f"Warning: {cpi_csv} not found, skipping KNBS.")
        return None

    raw_cpi = spark.read.option("header", "true").schema(KNBS_CPI_SCHEMA).csv(str(cpi_csv))
    cleaned_cpi = (
        raw_cpi.withColumn("year_month", F.trim(F.col("Reporting_Month")))
        .withColumn("cpi_index", F.col("Overall_CPI_Index"))
        .withColumn("deflation_multiplier", F.col("Deflation_Multiplier"))
        .withColumn("yoy_inflation_pct", F.col("YoY_Headline_Inflation"))
        .withColumn("mom_inflation_pct", F.col("MoM_Inflation"))
        .select("year_month", "cpi_index", "deflation_multiplier", "yoy_inflation_pct", "mom_inflation_pct")
    )

    print(f"Cleaned KNBS CPI records: {cleaned_cpi.count()}")
    return cleaned_cpi


def process_dimensions(spark, data_dir: Path):
    """Loads and standardizes operational dimensions."""
    print("\n--- Processing Operational Dimensions (Bronze -> Silver) ---")
    stores_csv = data_dir / "synthetic" / "stores.csv"
    products_csv = data_dir / "synthetic" / "products.csv"
    suppliers_csv = data_dir / "synthetic" / "suppliers.csv"
    item_map_csv = data_dir / "synthetic" / "product_commodity_map.csv"

    dim_stores = spark.read.option("header", "true").schema(STORES_SCHEMA).csv(str(stores_csv))
    dim_products = (
        spark.read.option("header", "true")
        .schema(PRODUCTS_SCHEMA)
        .csv(str(products_csv))
        .filter(F.col("base_price_kes") > 0)  # filter dirt negative price
    )
    dim_suppliers = spark.read.option("header", "true").schema(SUPPLIERS_SCHEMA).csv(str(suppliers_csv))
    dim_item_map = spark.read.option("header", "true").schema(PRODUCT_COMMODITY_MAP_SCHEMA).csv(str(item_map_csv))

    # Join products with commodity mapping
    dim_products_enriched = dim_products.join(dim_item_map, on="product_id", how="left")

    return dim_stores, dim_products_enriched, dim_suppliers


def build_silver_layer(spark, data_dir: Path):
    """Executes the full Bronze -> Silver pipeline, including macro-economic deflator enrichment."""
    silver_dir = data_dir / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    # 1. Process Dimensions
    dim_stores, dim_products, dim_suppliers = process_dimensions(spark, data_dir)

    # 2. Process KNBS & KAMIS
    silver_cpi = process_knbs_bronze_to_silver(spark, data_dir)
    silver_kamis = process_kamis_bronze_to_silver(spark, data_dir)

    # 3. Process POS Transactions
    silver_pos = process_pos_bronze_to_silver(spark, data_dir)

    # 4. Enrich POS Transactions with Store info, Commodity metadata, and Real / Deflated Value
    print("\n--- Enriching POS Transactions with Dimensions & KNBS Deflator ---")
    enriched_pos = (
        silver_pos.join(
            dim_stores.select(
                F.col("store_id"),
                F.col("store_name"),
                F.col("county").alias("store_county"),
            ),
            on="store_id",
            how="left",
        )
        .join(
            dim_products.select(
                F.col("product_id"),
                F.col("product_name"),
                F.col("supplier_id"),
                F.col("kamis_commodity_name"),
                F.col("kamis_category"),
            ),
            on="product_id",
            how="left",
        )
    )

    if silver_cpi is not None:
        enriched_pos = enriched_pos.join(
            silver_cpi.select("year_month", "deflation_multiplier", "cpi_index"),
            on="year_month",
            how="left",
        ).withColumn(
            "real_amount_kes",
            F.round(F.col("total_amount_kes") * F.coalesce(F.col("deflation_multiplier"), F.lit(1.0)), 2),
        )
    else:
        enriched_pos = enriched_pos.withColumn("real_amount_kes", F.col("total_amount_kes"))

    # 5. Persist Silver tables as Parquet
    print("\nWriting Silver Parquet tables...")
    out_pos = silver_dir / "pos_transactions"
    enriched_pos.write.mode("overwrite").partitionBy("year_month").parquet(str(out_pos))
    print(f"Saved Silver POS to: {out_pos}")

    out_stores = silver_dir / "dim_stores"
    dim_stores.write.mode("overwrite").parquet(str(out_stores))

    out_products = silver_dir / "dim_products"
    dim_products.write.mode("overwrite").parquet(str(out_products))

    if silver_kamis is not None:
        out_kamis = silver_dir / "kamis_prices"
        silver_kamis.write.mode("overwrite").partitionBy("year_month").parquet(str(out_kamis))
        print(f"Saved Silver KAMIS to: {out_kamis}")

    if silver_cpi is not None:
        out_cpi = silver_dir / "knbs_cpi"
        silver_cpi.write.mode("overwrite").parquet(str(out_cpi))
        print(f"Saved Silver KNBS CPI to: {out_cpi}")

    print("--- Silver Layer Processing Completed Successfully! ---")
    return enriched_pos, silver_kamis, silver_cpi

