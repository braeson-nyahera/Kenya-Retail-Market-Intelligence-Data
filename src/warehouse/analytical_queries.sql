-- ============================================================================
-- Kenya Retail Market Intelligence Data Platform - Analytical Queries
-- ============================================================================
-- This script contains 5 core business intelligence and macroeconomic queries
-- executed against the PostgreSQL Star Schema (Dimensions & Facts).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- QUERY 1: County-Level Real vs. Nominal Revenue & Purchasing Power Loss
-- Business Question:
-- How has inflation eroded retail purchasing power across Kenyan counties?
-- What is the variance between nominal revenue and real deflated revenue?
-- ----------------------------------------------------------------------------
-- query_id: query_1_county_inflation_impact
SELECT 
    s.county,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.total_amount_kes), 2) AS nominal_revenue_kes,
    ROUND(SUM(f.real_amount_kes), 2) AS real_revenue_kes,
    ROUND(SUM(f.total_amount_kes) - SUM(f.real_amount_kes), 2) AS inflation_loss_kes,
    ROUND(
        (SUM(f.total_amount_kes) - SUM(f.real_amount_kes)) / NULLIF(SUM(f.total_amount_kes), 0) * 100, 
        2
    ) AS purchasing_power_loss_pct
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY s.county
ORDER BY nominal_revenue_kes DESC;


-- ----------------------------------------------------------------------------
-- QUERY 2: Supermarket Retail Markup vs. KAMIS Open-Air Market Commodity Spread
-- Business Question:
-- How do supermarket shelf prices compare to KAMIS open-air agricultural market prices?
-- Which agricultural commodities carry the highest retail premium in each county?
-- ----------------------------------------------------------------------------
-- query_id: query_2_supermarket_vs_kamis_spread
SELECT 
    commodity,
    county,
    year_month,
    retail_pos_avg_price AS supermarket_price_kes,
    kamis_retail_avg_price AS open_market_price_kes,
    kamis_wholesale_avg_price AS wholesale_price_kes,
    price_spread_kes,
    supermarket_premium_pct,
    retail_pos_volume_sold AS supermarket_units_sold
FROM agg_commodity_market_comparison
WHERE kamis_retail_avg_price IS NOT NULL
  AND kamis_retail_avg_price > 0
ORDER BY supermarket_premium_pct DESC
LIMIT 20;


-- ----------------------------------------------------------------------------
-- QUERY 3: Store Performance Rankings & Basket Value Analysis
-- Business Question:
-- Which retail stores generate the highest transaction volume, revenue, 
-- and average transaction basket value?
-- ----------------------------------------------------------------------------
-- query_id: query_3_store_rankings_and_baskets
WITH store_metrics AS (
    SELECT 
        s.store_id,
        s.store_name,
        s.county,
        COUNT(DISTINCT f.transaction_id) AS total_transactions,
        SUM(f.quantity) AS total_items_sold,
        SUM(f.total_amount_kes) AS total_nominal_revenue_kes,
        SUM(f.real_amount_kes) AS total_real_revenue_kes
    FROM fact_sales f
    JOIN dim_store s ON f.store_id = s.store_id
    GROUP BY s.store_id, s.store_name, s.county
)
SELECT 
    store_name,
    county,
    total_transactions,
    total_items_sold,
    ROUND(total_nominal_revenue_kes, 2) AS total_revenue_kes,
    ROUND(total_real_revenue_kes, 2) AS real_revenue_kes,
    ROUND(total_nominal_revenue_kes / NULLIF(total_transactions, 0), 2) AS avg_basket_value_kes,
    ROUND(CAST(total_items_sold AS NUMERIC) / NULLIF(total_transactions, 0), 2) AS avg_items_per_basket,
    RANK() OVER (ORDER BY total_nominal_revenue_kes DESC) AS national_revenue_rank,
    RANK() OVER (PARTITION BY county ORDER BY total_nominal_revenue_kes DESC) AS county_revenue_rank
FROM store_metrics
ORDER BY total_revenue_kes DESC;


-- ----------------------------------------------------------------------------
-- QUERY 4: Monthly Category Sales Trends vs. KNBS Headline Inflation
-- Business Question:
-- How have monthly sales volumes for different commodity categories 
-- trended against headline inflation? Are consumers pivoting toward essentials?
-- ----------------------------------------------------------------------------
-- query_id: query_4_monthly_category_inflation_trends
SELECT 
    c.year_month,
    c.kamis_category,
    c.total_units_sold,
    ROUND(c.nominal_revenue_kes, 2) AS nominal_revenue_kes,
    ROUND(c.real_revenue_kes, 2) AS real_revenue_kes,
    k.overall_cpi,
    k.yoy_headline_inflation,
    k.deflation_multiplier,
    ROUND(
        LAG(c.total_units_sold, 1) OVER (
            PARTITION BY c.kamis_category ORDER BY c.year_month
        ), 0
    ) AS prev_month_units_sold,
    ROUND(
        (c.total_units_sold - LAG(c.total_units_sold, 1) OVER (
            PARTITION BY c.kamis_category ORDER BY c.year_month
        )) / NULLIF(LAG(c.total_units_sold, 1) OVER (
            PARTITION BY c.kamis_category ORDER BY c.year_month
        ), 0) * 100, 
        2
    ) AS mom_volume_growth_pct
FROM agg_monthly_category_trends c
LEFT JOIN dim_cpi k ON c.year_month = k.year_month
ORDER BY c.year_month DESC, c.nominal_revenue_kes DESC;


-- ----------------------------------------------------------------------------
-- QUERY 5: Supplier Concentration & Revenue Contribution Analysis
-- Business Question:
-- Which suppliers contribute the most revenue and merchandise volume 
-- to the retail network, and what is their exposure across product categories?
-- ----------------------------------------------------------------------------
-- query_id: query_5_supplier_concentration
SELECT 
    sup.supplier_id,
    sup.supplier_name,
    sup.region AS supplier_region,
    COUNT(DISTINCT p.product_id) AS products_supplied,
    COUNT(DISTINCT p.kamis_category) AS categories_supplied,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.total_amount_kes), 2) AS supplier_nominal_revenue_kes,
    ROUND(
        SUM(f.total_amount_kes) / NULLIF(SUM(SUM(f.total_amount_kes)) OVER (), 0) * 100,
        2
    ) AS revenue_share_pct
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_supplier sup ON p.supplier_id = sup.supplier_id
GROUP BY sup.supplier_id, sup.supplier_name, sup.region
ORDER BY supplier_nominal_revenue_kes DESC;

