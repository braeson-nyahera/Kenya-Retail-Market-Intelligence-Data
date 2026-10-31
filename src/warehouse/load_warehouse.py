"""Warehouse Ingestion Pipeline for Kenya Retail Market Intelligence Data Platform.

Loads Dimensions, Facts, and Pre-Aggregated Marts from Gold/Silver/Synthetic layers
into PostgreSQL Star Schema.

Usage:
  python src/warehouse/load_warehouse.py
"""
import os
import sys
from datetime import datetime, timedelta
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

    # When running on host machine, postgres is mapped to port 5433
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    user = os.getenv("POSTGRES_USER", "retail_user")
    password = os.getenv("POSTGRES_PASSWORD", "retail_password")
    db = os.getenv("POSTGRES_DB", "kenya_market_db")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def init_schema(engine, schema_sql_path: Path):
    """Executes schema DDL to create tables, constraints, indexes, and views."""
    print(f"\nExecuting DDL from {schema_sql_path}...")
    sql_script = schema_sql_path.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql_script))
    print("Schema initialized successfully (Dimensions, Facts, Indexes, Views).")


def generate_dim_date_df(start_date="2026-01-01", end_date="2026-12-31"):
    """Generates complete date dimension records for the operational calendar."""
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({"full_date": dates})
    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["day_of_month"] = df["full_date"].dt.day
    df["day_of_week"] = df["full_date"].dt.dayofweek + 1
    df["day_name"] = df["full_date"].dt.day_name()
    df["week_of_year"] = df["full_date"].dt.isocalendar().week.astype(int)
    df["month"] = df["full_date"].dt.month
    df["month_name"] = df["full_date"].dt.month_name()
    df["quarter"] = df["full_date"].dt.quarter
    df["year"] = df["full_date"].dt.year
    df["is_weekend"] = df["day_of_week"].isin([6, 7])
    df["year_month"] = df["full_date"].dt.strftime("%Y-%m")
    return df


def load_dimensions(engine, data_dir: Path):
    """Loads all dimension tables: dim_date, dim_supplier, dim_store, dim_product, dim_cpi."""
    print("\n--- Loading Dimension Tables ---")

    # 1. Dim Date
    dim_date_df = generate_dim_date_df("2026-01-01", "2026-12-31")
    dim_date_df.to_sql("dim_date", engine, if_exists="append", index=False)
    print(f"Loaded dim_date: {len(dim_date_df)} rows")

    # 2. Dim Supplier
    sup_file = data_dir / "synthetic" / "suppliers.csv"
    if sup_file.exists():
        sup_df = pd.read_csv(sup_file)
        sup_df.to_sql("dim_supplier", engine, if_exists="append", index=False)
        print(f"Loaded dim_supplier: {len(sup_df)} rows")

    # 3. Dim Store
    store_file = data_dir / "synthetic" / "stores.csv"
    if store_file.exists():
        store_df = pd.read_csv(store_file)
        if "region" not in store_df.columns:
            store_df["region"] = store_df["county"]
        store_df.to_sql("dim_store", engine, if_exists="append", index=False)
        print(f"Loaded dim_store: {len(store_df)} rows")

    # 4. Dim Product (joined with commodity map)
    prod_file = data_dir / "synthetic" / "products.csv"
    map_file = data_dir / "synthetic" / "product_commodity_map.csv"
    if prod_file.exists():
        prod_df = pd.read_csv(prod_file)
        # Filter negative price dirt
        prod_df = prod_df[prod_df["base_price_kes"] > 0]
        if map_file.exists():
            map_df = pd.read_csv(map_file)
            prod_df = prod_df.merge(map_df, on="product_id", how="left")
        else:
            prod_df["kamis_commodity_name"] = None
            prod_df["kamis_category"] = None
        prod_df.to_sql("dim_product", engine, if_exists="append", index=False)
        print(f"Loaded dim_product: {len(prod_df)} rows")

    # 5. Dim CPI (KNBS)
    cpi_file = data_dir / "knbs_downloads" / "cpi_deflator_series.csv"
    if cpi_file.exists():
        cpi_df = pd.read_csv(cpi_file)
        cpi_dim = pd.DataFrame({
            "year_month": cpi_df["Reporting_Month"].str.strip(),
            "overall_cpi": cpi_df["Overall_CPI_Index"],
            "deflation_multiplier": cpi_df["Deflation_Multiplier"],
            "yoy_headline_inflation": cpi_df["YoY_Headline_Inflation"],
            "mom_inflation": cpi_df["MoM_Inflation"],
        })
        cpi_dim.to_sql("dim_cpi", engine, if_exists="append", index=False)
        print(f"Loaded dim_cpi: {len(cpi_dim)} rows")


def load_facts_and_marts(engine, data_dir: Path):
    """Loads fact_sales and analytical marts into PostgreSQL."""
    print("\n--- Loading Fact Tables and Pre-Aggregated Marts ---")

    gold_dir = data_dir / "gold"
    silver_dir = data_dir / "silver"

    # Check for Parquet sources first, then fallback to CSV/JSONL
    fact_sales_path = gold_dir / "fact_sales"
    store_daily_path = gold_dir / "gold_store_daily_sales"
    market_comp_path = gold_dir / "gold_commodity_market_comparison"
    monthly_cat_path = gold_dir / "gold_monthly_category_trends"

    # 1. Load Pre-Aggregated Marts
    if store_daily_path.exists():
        store_daily_df = pd.read_parquet(store_daily_path)
        store_daily_df["date_key"] = pd.to_datetime(store_daily_df["transaction_date"]).dt.strftime("%Y%m%d").astype(int)
        store_daily_df.to_sql("agg_store_daily_sales", engine, if_exists="append", index=False)
        print(f"Loaded agg_store_daily_sales: {len(store_daily_df)} rows")

    if market_comp_path.exists():
        market_comp_df = pd.read_parquet(market_comp_path)
        market_comp_df.to_sql("agg_commodity_market_comparison", engine, if_exists="append", index=False)
        print(f"Loaded agg_commodity_market_comparison: {len(market_comp_df)} rows")

    if monthly_cat_path.exists():
        monthly_cat_df = pd.read_parquet(monthly_cat_path)
        monthly_cat_df.to_sql("agg_monthly_category_trends", engine, if_exists="append", index=False)
        print(f"Loaded agg_monthly_category_trends: {len(monthly_cat_df)} rows")

    # 2. Load Fact Sales
    if fact_sales_path.exists():
        print(f"Reading fact_sales from {fact_sales_path}...")
        fact_df = pd.read_parquet(fact_sales_path)
    elif (silver_dir / "pos_transactions").exists():
        print(f"Reading fact_sales from {silver_dir / 'pos_transactions'}...")
        fact_df = pd.read_parquet(silver_dir / "pos_transactions")
        fact_df["date_key"] = pd.to_datetime(fact_df["transaction_date"]).dt.strftime("%Y%m%d").astype(int)
        fact_df = fact_df[[
            "transaction_id", "line_number", "date_key", "transaction_timestamp",
            "store_id", "product_id", "quantity", "unit_price_kes",
            "total_amount_kes", "real_amount_kes", "year_month"
        ]]
    else:
        # Fallback to historical CSV
        csv_file = data_dir / "synthetic" / "historical_pos_lines.csv"
        print(f"Gold/Silver not yet generated. Ingesting from {csv_file} (sampling for initial load)...")
        fact_df = pd.read_csv(csv_file, nrows=10000)
        # Clean basic dirt
        fact_df = fact_df.dropna(subset=["product_id"])
        fact_df = fact_df[(fact_df["quantity"] > 0) & (fact_df["unit_price_kes"] >= 0)]
        fact_df = fact_df.drop_duplicates(subset=["transaction_id", "line_number"])
        fact_df["transaction_timestamp"] = pd.to_datetime(fact_df["timestamp"])
        fact_df["date_key"] = fact_df["transaction_timestamp"].dt.strftime("%Y%m%d").astype(int)
        fact_df["year_month"] = fact_df["transaction_timestamp"].dt.strftime("%Y-%m")
        fact_df["total_amount_kes"] = (fact_df["quantity"] * fact_df["unit_price_kes"]).round(2)
        fact_df["real_amount_kes"] = fact_df["total_amount_kes"]

    # Filter out any foreign key mismatches with dimensions
    dim_products = pd.read_sql("SELECT product_id FROM dim_product", engine)["product_id"].tolist()
    dim_stores = pd.read_sql("SELECT store_id FROM dim_store", engine)["store_id"].tolist()
    dim_dates = set(pd.read_sql("SELECT date_key FROM dim_date", engine)["date_key"].tolist())

    fact_clean = fact_df[
        fact_df["product_id"].isin(dim_products)
        & fact_df["store_id"].isin(dim_stores)
        & fact_df["date_key"].isin(dim_dates)
    ]

    # Ensure transaction_timestamp column exists
    if "transaction_timestamp" not in fact_clean.columns and "timestamp" in fact_clean.columns:
        fact_clean["transaction_timestamp"] = pd.to_datetime(fact_clean["timestamp"])

    target_cols = [
        "transaction_id", "line_number", "date_key", "transaction_timestamp",
        "store_id", "product_id", "quantity", "unit_price_kes",
        "total_amount_kes", "real_amount_kes", "year_month"
    ]
    fact_to_insert = fact_clean[[c for c in target_cols if c in fact_clean.columns]]

    print(f"Loading {len(fact_to_insert)} records into fact_sales in batches...")
    chunk_size = 5000
    fact_to_insert.to_sql("fact_sales", engine, if_exists="append", index=False, chunksize=chunk_size)
    print("fact_sales load complete.")


def verify_warehouse(engine):
    """Executes verification counts and displays an executive preview."""
    print("\n" + "=" * 70)
    print("DATA WAREHOUSE VERIFICATION & SUMMARY")
    print("=" * 70)

    tables = [
        "dim_date", "dim_supplier", "dim_store", "dim_product", "dim_cpi",
        "fact_sales", "agg_store_daily_sales", "agg_commodity_market_comparison", "agg_monthly_category_trends"
    ]

    with engine.connect() as conn:
        for t in tables:
            try:
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                print(f"Table {t:<35}: {cnt:>10,} rows")
            except Exception as e:
                print(f"Table {t:<35}: Error ({e})")

        print("\n--- Analytical View Preview: v_county_real_vs_nominal_revenue ---")
        preview_df = pd.read_sql("SELECT * FROM v_county_real_vs_nominal_revenue LIMIT 5", conn)
        print(preview_df.to_string(index=False))

    print("=" * 70)


def main():
    print("=" * 70)
    print("Kenya Retail Market Intelligence Data Warehouse Loader")
    print("=" * 70)

    data_dir = repo_root / "data"
    schema_sql = repo_root / "src" / "warehouse" / "schema.sql"

    db_url = get_database_url()
    # Mask password for logging
    safe_url = db_url.split("@")[0].split(":")[0] + "://***@" + db_url.split("@")[-1]
    print(f"Connecting to PostgreSQL at: {safe_url}")

    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version();")).scalar()
            print(f"Connected to: {version.split(',')[0]}")

        # 1. Initialize Schema
        init_schema(engine, schema_sql)

        # 2. Load Dimensions
        load_dimensions(engine, data_dir)

        # 3. Load Facts & Marts
        load_facts_and_marts(engine, data_dir)

        # 4. Verify Warehouse
        verify_warehouse(engine)

        print("\nWarehouse setup & ingestion completed successfully!")

    except Exception as e:
        print(f"\nError loading warehouse: {e}")
        print("\nTip: Ensure PostgreSQL is running via: docker compose --profile db up -d")
        sys.exit(1)


if __name__ == "__main__":
    main()

