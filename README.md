# Kenya Retail Market Intelligence Data Platform

Retail market data intelligence and transaction streaming platform for Kenyan market commodities, CPI indicators, and POS retail transactions.

---

## Architecture Overview

```
                      +-----------------------------+
                      |   historical_pos_lines.csv  |
                      +--------------+--------------+
                                     |
                                     v
                       [ pos_producer.py ]
                                     |
                                     v
                    Apache Kafka: pos.transactions
                                     |
                     +---------------+---------------+
                     |                               |
          (Valid transactions)              (Anomalies / Dirt)
                     |                               |
                     v                               v
           [ bronze_writer.py ]           Kafka: pos.transactions.dlq
                     |
                     v
    +----------------------------------+
    |           BRONZE LAYER           |
    |  data/bronze/pos/ingest_date=... |
    +-----------------+----------------+
                      |
                      | PySpark (bronze_to_silver.py)
                      v
    +----------------------------------+
    |           SILVER LAYER           |
    |  Deduplicated, Cleaned POS lines |
    |  KAMIS Wholesale & Retail Prices |
    |  Enriched with KNBS CPI Deflator |
    +-----------------+----------------+
                      |
                      | PySpark (silver_to_gold.py)
                      v
    +----------------------------------+
    |            GOLD LAYER            |
    |  - gold_store_daily_sales        |
    |  - gold_commodity_comparison     |
    |  - gold_monthly_category_trends  |
    |  - fact_sales & dim_date (DW)    |
    +----------------------------------+
```

---

## 1. Starting Kafka & Kafka UI

Kafka and Kafka UI are grouped under the `kafka` profile:

```bash
docker compose --profile kafka up -d
```

- **Kafka UI**: Open your browser at [http://localhost:8080](http://localhost:8080)
- **Broker**: `localhost:9092` (host) / `kafka:9092` (Docker network)

---

## 2. Running the POS Simulation

### Option A: Using Docker Compose Profile (Recommended)
You can trigger the entire simulation as a containerized service with one command:

```bash
# Trigger the simulation (starts Bronze consumer + streams 1,000 receipts):
docker compose --profile simulation up
```

You can also run the producer or consumer independently via Docker Compose:

```bash
# Run only the producer:
docker compose --profile producer up

# Run only the bronze consumer:
docker compose --profile consumer up
```

All ingested batches are written directly to `./data/bronze/pos/` on your host machine.

---

### Option B: Running from Host Terminal (Python)

If you prefer running directly from your host environment:

```bash
# Install dependencies
uv sync # or: pip install kafka-python pandas

# Run all-in-one:
python src/pos_simulation/run_all.py --limit 1000 --delay 0.01

# Or run consumer and producer in separate terminals:
# Terminal 1:
python src/pos_simulation/bronze_writer.py --exit-when-idle 15
# Terminal 2:
python src/pos_simulation/pos_producer.py --limit 1000 --delay 0.01
```

---

## 3. Data Ingestion & Quality Checks (Kafka)

The Bronze writer validates each receipt:
- **Valid receipts**: Stored as partitioned JSON-Lines in `data/bronze/pos/ingest_date=YYYY-MM-DD/batch_<timestamp>.jsonl` with partition/offset metadata.
- **Injected anomalies**: Transactions with negative quantities, bad prices, or missing products are redirected to the Dead Letter Queue (`pos.transactions.dlq`) with header annotations describing the failure reason.

---

## 4. PySpark Medallion Processing Pipeline

The PySpark pipeline processes data through **Bronze -> Silver -> Gold** layers:

### Running the PySpark Pipeline

#### Option A: Run via Docker Compose (Recommended)
```bash
docker compose --profile spark up --build
```

#### Option B: Run via Host Python / uv
```bash
uv sync # installs pyspark
python src/spark/run_pipeline.py
```

### Layer Details:

#### **Silver Layer (`data/silver/`)**
- **POS Transactions**: Explodes receipt line items, deduplicates transactions (resolving injected anomalies), casts dates, and calculates transaction totals.
- **KAMIS Market Prices**: Cleans price strings (e.g. `"50.00/Kg"` -> `50.00` numeric float), parses supply volume, and formats county dates.
- **KNBS Deflator Series**: Formats Year-Month CPI indices and inflation multipliers.
- **Macro Enrichment**: Joins POS lines with KNBS deflators to compute **inflation-adjusted real revenue** (`real_amount_kes = total_amount_kes * Deflation_Multiplier`).

#### **Gold Layer (`data/gold/`)**
- **`gold_store_daily_sales`**: Daily store-level nominal revenue, real revenue, basket sizes, and inflation erosion losses.
- **`gold_commodity_market_comparison`**: Compares supermarket POS prices directly against KAMIS county agricultural market prices (spread and premium %).
- **`gold_monthly_category_trends`**: Monthly category-level performance tracking against headline inflation.
- **`fact_sales` & `dim_date`**: Clean dimensional models ready for PostgreSQL Data Warehouse loading.

---

## 5. Data Warehouse (PostgreSQL Star Schema)

Start PostgreSQL and load the full star schema:

```bash
# Start PostgreSQL with the warehouse profile
docker compose --profile warehouse up -d

# Initialize schema + load data  (run on host with uv)
uv run python src/warehouse/load_warehouse.py

# Run the 5 BI analytical queries
uv run python src/warehouse/run_queries.py
uv run python src/warehouse/run_queries.py --query 1    # specific query
uv run python src/warehouse/run_queries.py --limit 10  # limit rows displayed
```

### Schema Overview

| Table | Type | Description |
|---|---|---|
| `dim_date` | Dimension | Full 2026 calendar with week, quarter, weekend flags |
| `dim_store` | Dimension | 30 stores across Kenyan counties |
| `dim_product` | Dimension | Products mapped to KAMIS commodity categories |
| `dim_supplier` | Dimension | Supplier registry with region |
| `dim_cpi` | Dimension | KNBS monthly CPI deflators and inflation rates |
| `fact_sales` | Fact | Grain: transaction line item (nominal + real KES) |
| `agg_store_daily_sales` | Mart | Daily store revenue, basket size, inflation loss |
| `agg_commodity_market_comparison` | Mart | Supermarket vs. KAMIS open-air price spreads |
| `agg_monthly_category_trends` | Mart | Monthly category volumes vs. CPI |

### Analytical Queries (5 BI Queries)

1. **County Inflation Impact** — nominal vs. real revenue & purchasing power erosion per county
2. **Supermarket vs. KAMIS Spread** — which commodities carry the highest retail premium
3. **Store Rankings** — revenue rank, basket value, items per transaction
4. **Monthly Category Trends** — sales volume growth vs. KNBS headline inflation MoM
5. **Supplier Concentration** — supplier revenue share and category coverage

---

## 6. Apache Airflow — Pipeline Orchestration

Airflow orchestrates the full processing pipeline (Spark ETL → Warehouse):

```
bronze_to_silver → silver_to_gold → init_warehouse → load_warehouse → verify_warehouse
```

### Start Airflow

```bash
# Start PostgreSQL (metadata + data warehouse) + Airflow
docker compose --profile warehouse --profile airflow up -d

# Open Airflow UI:  http://localhost:8085
# Credentials:      admin / admin
```

### Triggering the DAG

1. Open [http://localhost:8085](http://localhost:8085) in your browser.
2. Find the **`retail_pipeline`** DAG in the list.
3. Toggle it **ON** (the slider on the left).
4. Click the **▶ Trigger DAG** button to run it immediately.
5. Click the DAG name → **Graph** view to watch task-by-task progress.

### DAG Tasks

| Task | Stage | Description |
|---|---|---|
| `bronze_to_silver` | Spark | Cleans raw POS + KAMIS data, enriches with CPI deflators |
| `silver_to_gold` | Spark | Builds Gold analytical marts + fact_sales Parquet |
| `init_warehouse` | Warehouse | Executes `schema.sql` DDL in PostgreSQL |
| `load_warehouse` | Warehouse | Loads dimensions, facts, and pre-aggregated marts |
| `verify_warehouse` | Warehouse | Row-count audit + analytical view preview |

### Running without Docker (local dev)

```bash
pip install apache-airflow
export AIRFLOW_HOME=./airflow_home
export DATABASE_URL="postgresql://retail_user:retail_password@localhost:5433/kenya_market_db"

airflow db migrate
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin --email admin@local

# In two terminals:
airflow scheduler
airflow webserver --port 8085
```


---

## 7. Data Quality

The `src/quality/` module runs **40+ automated checks** across every layer of the pipeline.
Quality gate tasks in the Airflow DAG abort the pipeline on any `FAIL`, preventing bad data
from propagating downstream.

### Suites

| Suite | Datasets validated | Checks |
|---|---|---|
| `bronze` | Bronze POS JSONL | File existence, schema, row count, null rates, TX uniqueness, freshness |
| `silver` | Silver POS, KAMIS, KNBS CPI | Dedup, referential integrity (store/product dims), amount consistency, real≤nominal, price ranges, deflator consistency |
| `gold` | Gold store_daily_sales | Unique (store, date) key, revenue > 0, basket value bounds, inflation loss sign |
| `warehouse` | All PostgreSQL tables + views | Row counts, null rates, PK uniqueness, FK integrity (fact→dim), aggregation consistency, view returnability |

### Running Manually

```bash
# All suites
uv run python src/quality/run_quality.py

# Single suite
uv run python src/quality/run_quality.py --suite bronze
uv run python src/quality/run_quality.py --suite silver
uv run python src/quality/run_quality.py --suite gold
uv run python src/quality/run_quality.py --suite warehouse

# Report only (never exit with error code)
uv run python src/quality/run_quality.py --no-fail
```

Reports are saved to `data/quality_reports/<suite>_<timestamp>.json`.

### Airflow Quality Gates

The DAG includes 4 quality gate tasks that act as hard stop-gates:

```
bronze_to_silver → quality_bronze → silver_to_gold → quality_silver
    → quality_gold → init_warehouse → load_warehouse → quality_warehouse → verify_warehouse
```

If any gate fails, Airflow marks the run as `failed` and downstream tasks do not execute.
Re-run from the failed gate once the underlying data issue is fixed.

### Check Types

| Check | Description |
|---|---|
| `null_check` | Column null % must not exceed threshold |
| `range_check` | Numeric values must fall within [min, max] |
| `unique_check` | Column(s) must form a unique key |
| `referential_check` | Values must exist in a reference dimension |
| `schema_check` | DataFrame must have all expected columns |
| `freshness_check` | Most recent date must be within N days of today |
| `value_set_check` | Categorical values must belong to allowed set |
| `row_count_check` | Table must have at least min_rows rows |
| `duplicate_check` | Exact duplicate rows must not exceed threshold |
| `amount_consistency` | total_amount ≈ quantity × unit_price |
| `real_amount_sanity` | real_amount ≤ nominal_amount (inflation check) |
| `aggregation_consistency` | Fact totals match pre-aggregated mart totals |
| `view_returnability` | Analytical SQL views must return rows |
