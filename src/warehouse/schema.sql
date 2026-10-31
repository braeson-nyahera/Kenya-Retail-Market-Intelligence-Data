-- ============================================================================
-- Kenya Retail Market Intelligence Data Platform - PostgreSQL Dimensional Model
-- Star Schema DDL: Dimensions, Facts, Pre-Aggregated Marts, Indexes, and Views
-- ============================================================================

-- Clean up existing tables if recreating
DROP VIEW IF EXISTS v_county_real_vs_nominal_revenue CASCADE;
DROP VIEW IF EXISTS v_supermarket_vs_market_price_spread CASCADE;
DROP VIEW IF EXISTS v_top_performing_products CASCADE;

DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS agg_store_daily_sales CASCADE;
DROP TABLE IF EXISTS agg_commodity_market_comparison CASCADE;
DROP TABLE IF EXISTS agg_monthly_category_trends CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;
DROP TABLE IF EXISTS dim_cpi CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- ----------------------------------------------------------------------------
-- 1. DIMENSION TABLES
-- ----------------------------------------------------------------------------

-- Date Dimension
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,                       -- e.g. 20260115
    full_date DATE NOT NULL UNIQUE,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,                       -- 1 = Monday, 7 = Sunday
    day_name VARCHAR(15) NOT NULL,                  -- 'Monday', 'Tuesday'
    week_of_year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,                -- 'January', 'February'
    quarter INT NOT NULL,                           -- 1, 2, 3, 4
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    year_month VARCHAR(7) NOT NULL                  -- '2026-01'
);

-- Supplier Dimension
CREATE TABLE dim_supplier (
    supplier_id VARCHAR(20) PRIMARY KEY,
    supplier_name VARCHAR(100) NOT NULL,
    contact_email VARCHAR(100),
    region VARCHAR(50)
);

-- Store Dimension
CREATE TABLE dim_store (
    store_id VARCHAR(20) PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    county VARCHAR(50) NOT NULL,
    region VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE
);

-- Product Dimension
CREATE TABLE dim_product (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    supplier_id VARCHAR(20) REFERENCES dim_supplier(supplier_id),
    kamis_commodity_name VARCHAR(100),
    kamis_category VARCHAR(50),
    base_price_kes NUMERIC(10, 2)
);

-- KNBS CPI & Macroeconomic Dimension
CREATE TABLE dim_cpi (
    year_month VARCHAR(7) PRIMARY KEY,
    overall_cpi NUMERIC(10, 4) NOT NULL,
    deflation_multiplier NUMERIC(10, 6) NOT NULL,
    yoy_headline_inflation NUMERIC(6, 2),
    mom_inflation NUMERIC(6, 2)
);

-- ----------------------------------------------------------------------------
-- 2. FACT TABLES
-- ----------------------------------------------------------------------------

-- Core POS Sales Fact Table (Grain: Transaction Line Item)
CREATE TABLE fact_sales (
    transaction_id VARCHAR(50) NOT NULL,
    line_number INT NOT NULL,
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    transaction_timestamp TIMESTAMP NOT NULL,
    store_id VARCHAR(20) NOT NULL REFERENCES dim_store(store_id),
    product_id VARCHAR(20) NOT NULL REFERENCES dim_product(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price_kes NUMERIC(10, 2) NOT NULL CHECK (unit_price_kes >= 0),
    total_amount_kes NUMERIC(12, 2) NOT NULL,       -- Nominal revenue
    real_amount_kes NUMERIC(12, 2) NOT NULL,        -- Deflated revenue via KNBS multiplier
    year_month VARCHAR(7) NOT NULL,
    PRIMARY KEY (transaction_id, line_number)
);

-- Pre-Aggregated Mart: Daily Store Performance
CREATE TABLE agg_store_daily_sales (
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    transaction_date DATE NOT NULL,
    store_id VARCHAR(20) NOT NULL REFERENCES dim_store(store_id),
    store_name VARCHAR(100) NOT NULL,
    store_county VARCHAR(50) NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    total_transactions INT NOT NULL,
    total_items_sold INT NOT NULL,
    nominal_revenue_kes NUMERIC(14, 2) NOT NULL,
    real_revenue_kes NUMERIC(14, 2) NOT NULL,
    avg_basket_value_kes NUMERIC(10, 2),
    inflation_loss_kes NUMERIC(14, 2),
    PRIMARY KEY (date_key, store_id)
);

-- Pre-Aggregated Mart: Commodity Market Intelligence (Supermarket vs KAMIS open-air)
CREATE TABLE agg_commodity_market_comparison (
    year_month VARCHAR(7) NOT NULL,
    commodity VARCHAR(100) NOT NULL,
    county VARCHAR(50) NOT NULL,
    retail_pos_avg_price NUMERIC(10, 2) NOT NULL,
    kamis_retail_avg_price NUMERIC(10, 2),
    kamis_wholesale_avg_price NUMERIC(10, 2),
    price_spread_kes NUMERIC(10, 2),
    supermarket_premium_pct NUMERIC(6, 2),
    retail_pos_volume_sold INT,
    kamis_avg_supply_volume_kg NUMERIC(12, 2),
    PRIMARY KEY (year_month, commodity, county)
);

-- Pre-Aggregated Mart: Monthly Category Trends
CREATE TABLE agg_monthly_category_trends (
    year_month VARCHAR(7) NOT NULL,
    kamis_category VARCHAR(50) NOT NULL,
    active_stores INT,
    active_products INT,
    total_units_sold INT,
    nominal_revenue_kes NUMERIC(14, 2),
    real_revenue_kes NUMERIC(14, 2),
    cpi_index NUMERIC(10, 4),
    PRIMARY KEY (year_month, kamis_category)
);

-- ----------------------------------------------------------------------------
-- 3. PERFORMANCE INDEXES
-- ----------------------------------------------------------------------------

CREATE INDEX idx_fact_sales_date_key ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_store_id ON fact_sales(store_id);
CREATE INDEX idx_fact_sales_product_id ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_year_month ON fact_sales(year_month);

CREATE INDEX idx_agg_store_daily_date ON agg_store_daily_sales(transaction_date);
CREATE INDEX idx_agg_store_daily_county ON agg_store_daily_sales(store_county);
CREATE INDEX idx_agg_market_comp_commodity ON agg_commodity_market_comparison(commodity);

-- ----------------------------------------------------------------------------
-- 4. ANALYTICAL BUSINESS VIEWS
-- ----------------------------------------------------------------------------

-- View 1: County-Level Real vs Nominal Revenue & Inflation Impact
CREATE VIEW v_county_real_vs_nominal_revenue AS
SELECT 
    s.county,
    COUNT(DISTINCT f.transaction_id) AS total_receipts,
    SUM(f.quantity) AS total_items_sold,
    SUM(f.total_amount_kes) AS nominal_revenue_kes,
    SUM(f.real_amount_kes) AS real_revenue_kes,
    ROUND(SUM(f.total_amount_kes) - SUM(f.real_amount_kes), 2) AS inflation_erosion_kes,
    ROUND(
        (SUM(f.total_amount_kes) - SUM(f.real_amount_kes)) / NULLIF(SUM(f.total_amount_kes), 0) * 100, 
        2
    ) AS inflation_loss_pct
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY s.county
ORDER BY nominal_revenue_kes DESC;

-- View 2: Supermarket Retail Markup vs KAMIS Agricultural Market Benchmark
CREATE VIEW v_supermarket_vs_market_price_spread AS
SELECT 
    commodity,
    county,
    year_month,
    retail_pos_avg_price,
    kamis_retail_avg_price,
    price_spread_kes,
    supermarket_premium_pct,
    retail_pos_volume_sold
FROM agg_commodity_market_comparison
WHERE kamis_retail_avg_price IS NOT NULL
ORDER BY supermarket_premium_pct DESC;

-- View 3: Top Selling Commodities and Category Performance
CREATE VIEW v_top_performing_products AS
SELECT 
    p.kamis_category,
    p.kamis_commodity_name,
    p.product_name,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.total_amount_kes) AS total_nominal_revenue_kes,
    SUM(f.real_amount_kes) AS total_real_revenue_kes,
    ROUND(AVG(f.unit_price_kes), 2) AS avg_selling_price_kes
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.kamis_category, p.kamis_commodity_name, p.product_name
ORDER BY total_nominal_revenue_kes DESC;

