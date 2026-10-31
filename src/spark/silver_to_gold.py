"""Silver to Gold Analytical Aggregation & Mart Generation Pipeline using PySpark.
Produces:
  1. gold_store_daily_sales: Daily store metrics (nominal vs real revenue, basket sizes, transactions)
  2. gold_commodity_market_comparison: Supermarket POS prices compared against official KAMIS commodity market prices
  3. gold_monthly_category_trends: Macro-economic monthly category performance with inflation metrics
  4. fact_sales & dimensional tables ready for PostgreSQL Data Warehouse ingestion
"""
from pathlib import Path
from pyspark.sql import functions as F


def build_store_daily_sales_mart(enriched_pos_df):
    """Aggregates store performance on a daily level with inflation adjustments."""
    print("Computing gold_store_daily_sales mart...")
    gold_store_daily = (
        enriched_pos_df.groupBy("store_id", "store_name", "store_county", "transaction_date", "year_month")
        .agg(
            F.countDistinct("transaction_id").alias("total_transactions"),
            F.sum("quantity").alias("total_items_sold"),
            F.round(F.sum("total_amount_kes"), 2).alias("nominal_revenue_kes"),
            F.round(F.sum("real_amount_kes"), 2).alias("real_revenue_kes"),
            F.round(F.sum("total_amount_kes") / F.countDistinct("transaction_id"), 2).alias("avg_basket_value_kes"),
            F.round(F.sum("total_amount_kes") - F.sum("real_amount_kes"), 2).alias("inflation_loss_kes"),
        )
        .orderBy("transaction_date", "store_id")
    )
    return gold_store_daily


def build_commodity_market_comparison_mart(enriched_pos_df, silver_kamis_df):
    """Compares retail supermarket prices with official KAMIS county wholesale and retail commodity prices."""
    if silver_kamis_df is None:
        print("KAMIS data not available. Skipping commodity market comparison mart.")
        return None

    print("Computing gold_commodity_market_comparison mart...")

    # Aggregate POS by commodity, county, and month
    pos_agg = (
        enriched_pos_df.filter(F.col("kamis_commodity_name").isNotNull())
        .groupBy("kamis_commodity_name", "store_county", "year_month")
        .agg(
            F.round(F.avg("unit_price_kes"), 2).alias("retail_pos_avg_price"),
            F.sum("quantity").alias("retail_pos_volume_sold"),
            F.round(F.sum("total_amount_kes"), 2).alias("retail_pos_total_spend_kes"),
        )
    )

    # Aggregate KAMIS by commodity, county, and month
    kamis_agg = (
        silver_kamis_df.groupBy("commodity_name", "county_name", "year_month")
        .agg(
            F.round(F.avg("retail_kes"), 2).alias("kamis_retail_avg_price"),
            F.round(F.avg("wholesale_kes"), 2).alias("kamis_wholesale_avg_price"),
            F.round(F.avg("supply_volume_kg"), 2).alias("kamis_avg_supply_volume_kg"),
        )
    )

    # Join and compute price divergence spread
    comparison_mart = (
        pos_agg.join(
            kamis_agg,
            (pos_agg["kamis_commodity_name"] == kamis_agg["commodity_name"])
            & (pos_agg["store_county"] == kamis_agg["county_name"])
            & (pos_agg["year_month"] == kamis_agg["year_month"]),
            how="inner",
        )
        .withColumn(
            "price_spread_kes",
            F.round(F.col("retail_pos_avg_price") - F.col("kamis_retail_avg_price"), 2),
        )
        .withColumn(
            "supermarket_premium_pct",
            F.round(
                (F.col("retail_pos_avg_price") - F.col("kamis_retail_avg_price"))
                / F.col("kamis_retail_avg_price")
                * 100,
                2,
            ),
        )
        .select(
            pos_agg["kamis_commodity_name"].alias("commodity"),
            pos_agg["store_county"].alias("county"),
            pos_agg["year_month"],
            "retail_pos_avg_price",
            "kamis_retail_avg_price",
            "kamis_wholesale_avg_price",
            "price_spread_kes",
            "supermarket_premium_pct",
            "retail_pos_volume_sold",
            "kamis_avg_supply_volume_kg",
        )
        .orderBy("year_month", "commodity", "county")
    )

    return comparison_mart


def build_monthly_category_trends_mart(enriched_pos_df):
    """Aggregates monthly commodity category performance against inflation trends."""
    print("Computing gold_monthly_category_trends mart...")
    monthly_trends = (
        enriched_pos_df.groupBy("year_month", "kamis_category")
        .agg(
            F.countDistinct("store_id").alias("active_stores"),
            F.countDistinct("product_id").alias("active_products"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("total_amount_kes"), 2).alias("nominal_revenue_kes"),
            F.round(F.sum("real_amount_kes"), 2).alias("real_revenue_kes"),
            F.first("cpi_index", ignorenulls=True).alias("cpi_index"),
        )
        .orderBy("year_month", "kamis_category")
    )
    return monthly_trends


def build_fact_and_dimensions_for_dw(enriched_pos_df):
    """Generates clean Star Schema Fact and Date Dimension tables ready for PostgreSQL Data Warehouse."""
    print("Generating Star Schema Fact and Date Dimension tables...")

    # Dim Date
    dim_date = (
        enriched_pos_df.select("transaction_date", "transaction_year", "transaction_month")
        .distinct()
        .withColumn("date_key", F.date_format("transaction_date", "yyyyMMdd").cast("int"))
        .withColumn("day_of_month", F.dayofmonth("transaction_date"))
        .withColumn("day_of_week", F.dayofweek("transaction_date"))
        .withColumn("quarter", F.quarter("transaction_date"))
        .select("date_key", "transaction_date", "transaction_year", "transaction_month", "day_of_month", "day_of_week", "quarter")
        .orderBy("date_key")
    )

    # Fact Sales Line Items
    fact_sales = (
        enriched_pos_df.withColumn("date_key", F.date_format("transaction_date", "yyyyMMdd").cast("int"))
        .select(
            "transaction_id",
            "line_number",
            "date_key",
            "store_id",
            "product_id",
            "quantity",
            "unit_price_kes",
            "total_amount_kes",
            "real_amount_kes",
            "year_month",
        )
    )

    return fact_sales, dim_date


def build_gold_layer(spark, data_dir: Path, enriched_pos_df, silver_kamis_df):
    """Executes the Silver -> Gold analytical aggregation pipeline and saves to Parquet."""
    gold_dir = data_dir / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    print("\n--- Generating Gold Layer Analytical Marts ---")

    # 1. Store Daily Sales Mart
    gold_store_daily = build_store_daily_sales_mart(enriched_pos_df)
    out_store_daily = gold_dir / "gold_store_daily_sales"
    gold_store_daily.write.mode("overwrite").parquet(str(out_store_daily))
    print(f"Saved gold_store_daily_sales to: {out_store_daily}")

    # 2. Commodity Market Comparison Mart (Supermarket vs KAMIS)
    gold_market_comp = build_commodity_market_comparison_mart(enriched_pos_df, silver_kamis_df)
    if gold_market_comp is not None:
        out_market_comp = gold_dir / "gold_commodity_market_comparison"
        gold_market_comp.write.mode("overwrite").parquet(str(out_market_comp))
        print(f"Saved gold_commodity_market_comparison to: {out_market_comp}")

    # 3. Monthly Category Trends Mart
    gold_monthly_cat = build_monthly_category_trends_mart(enriched_pos_df)
    out_monthly_cat = gold_dir / "gold_monthly_category_trends"
    gold_monthly_cat.write.mode("overwrite").parquet(str(out_monthly_cat))
    print(f"Saved gold_monthly_category_trends to: {out_monthly_cat}")

    # 4. Star Schema DW Exports
    fact_sales, dim_date = build_fact_and_dimensions_for_dw(enriched_pos_df)
    out_fact_sales = gold_dir / "fact_sales"
    fact_sales.write.mode("overwrite").partitionBy("year_month").parquet(str(out_fact_sales))
    print(f"Saved fact_sales to: {out_fact_sales}")

    out_dim_date = gold_dir / "dim_date"
    dim_date.write.mode("overwrite").parquet(str(out_dim_date))
    print(f"Saved dim_date to: {out_dim_date}")

    print("--- Gold Layer Processing Completed Successfully! ---")
    return gold_store_daily, gold_market_comp, gold_monthly_cat

