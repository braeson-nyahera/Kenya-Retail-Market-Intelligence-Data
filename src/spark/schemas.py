"""Data schemas for PySpark Medallion Layers."""
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# 1. Bronze Layer Schema (Raw POS JSONL from Kafka Bronze writer)
POS_LINE_ITEM_SCHEMA = StructType([
    StructField("line_number", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price_kes", DoubleType(), True),
])

POS_BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("lines", ArrayType(POS_LINE_ITEM_SCHEMA), True),
    StructField("_partition", IntegerType(), True),
    StructField("_offset", LongType(), True),
    StructField("_ingested_at", StringType(), True),
])

# 2. Historical POS Lines CSV (Fallback / Backfill)
HISTORICAL_POS_CSV_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("line_number", IntegerType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price_kes", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

# 3. KAMIS Market Prices Schema
KAMIS_RAW_SCHEMA = StructType([
    StructField("commodity", StringType(), True),
    StructField("classification", StringType(), True),
    StructField("grade", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("market", StringType(), True),
    StructField("wholesale", StringType(), True),
    StructField("retail", StringType(), True),
    StructField("supply volume", StringType(), True),
    StructField("county", StringType(), True),
    StructField("date", StringType(), True),
    StructField("scraped_id", StringType(), True),
    StructField("scraped_name", StringType(), True),
])

# 4. KNBS CPI & Deflator Series Schema
KNBS_CPI_SCHEMA = StructType([
    StructField("Reporting_Month", StringType(), True),
    StructField("Overall_CPI_Index", DoubleType(), True),
    StructField("Deflation_Multiplier", DoubleType(), True),
    StructField("YoY_Headline_Inflation", DoubleType(), True),
    StructField("MoM_Inflation", DoubleType(), True),
])

# 5. Operational Dimension Schemas
STORES_SCHEMA = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("county", StringType(), True),
    StructField("is_active", BooleanType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("supplier_id", StringType(), True),
    StructField("base_price_kes", DoubleType(), True),
])

SUPPLIERS_SCHEMA = StructType([
    StructField("supplier_id", StringType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("contact_email", StringType(), True),
    StructField("region", StringType(), True),
])

PRODUCT_COMMODITY_MAP_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("kamis_commodity_name", StringType(), True),
    StructField("kamis_category", StringType(), True),
])

