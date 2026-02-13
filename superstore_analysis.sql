/* ============================================================
   Project: Superstore Sales Analysis
   Database: PostgreSQL
   Description: Full ETL + Analytical Queries
   ============================================================ */

-- ============================================================
-- 1. CLEANUP (Drop tables if they already exist)
-- ============================================================

DROP TABLE IF EXISTS superstore;
DROP TABLE IF EXISTS superstore_stage;

-- ============================================================
-- 2. CREATE STAGE TABLE (All TEXT for safe import)
-- ============================================================

CREATE TABLE superstore_stage (
    row_id TEXT,
    order_id TEXT,
    order_date TEXT,
    ship_date TEXT,
    ship_mode TEXT,
    customer_id TEXT,
    customer_name TEXT,
    segment TEXT,
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    region TEXT,
    product_id TEXT,
    category TEXT,
    sub_category TEXT,
    product_name TEXT,
    sales TEXT,
    quantity TEXT,
    discount TEXT,
    profit TEXT
);

-- ============================================================
-- 3. LOAD DATA INTO STAGE TABLE
-- NOTE: Update file path before running
-- ============================================================

COPY superstore_stage
FROM 'C:\Maincrafts Professional Internship\Task 1\Data Analytics & Business Intelligence\Superstore_Dataset\Clean_Superstore.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    QUOTE '"',
    ESCAPE '"'
);

-- ============================================================
-- 4. DATA CLEANING
-- ============================================================

-- Remove currency symbols and trim spaces
UPDATE superstore_stage
SET sales = REPLACE(TRIM(sales), '$', ''),
    profit = REPLACE(TRIM(profit), '$', '');

-- Convert dash placeholders to NULL
UPDATE superstore_stage
SET profit = NULL
WHERE TRIM(profit) = '-';

-- Remove percentage sign if exists
UPDATE superstore_stage
SET discount = REPLACE(discount, '%', '');

-- ============================================================
-- 5. CREATE FINAL STRUCTURED TABLE
-- ============================================================

CREATE TABLE superstore (
    row_id INTEGER,
    order_id TEXT,
    order_date DATE,
    ship_date DATE,
    ship_mode TEXT,
    customer_id TEXT,
    customer_name TEXT,
    segment TEXT,
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    region TEXT,
    product_id TEXT,
    category TEXT,
    sub_category TEXT,
    product_name TEXT,
    sales NUMERIC,
    quantity INTEGER,
    discount NUMERIC,
    profit NUMERIC
);

-- ============================================================
-- 6. INSERT CLEAN DATA INTO FINAL TABLE
-- ============================================================

INSERT INTO superstore
SELECT
    row_id::INTEGER,
    order_id,
    order_date::DATE,
    ship_date::DATE,
    ship_mode,
    customer_id,
    customer_name,
    segment,
    country,
    city,
    state,
    postal_code,
    region,
    product_id,
    category,
    sub_category,
    product_name,
    sales::NUMERIC,
    quantity::INTEGER,
    discount::NUMERIC,
    profit::NUMERIC
FROM superstore_stage;

-- ============================================================
-- 7. VALIDATION
-- ============================================================

SELECT COUNT(*) AS total_rows FROM superstore;

-- ============================================================
-- 8. KPI ANALYSIS
-- ============================================================

SELECT
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT order_id) AS total_orders
FROM superstore;

-- Profit Margin
SELECT
    ROUND(SUM(profit) / SUM(sales) * 100, 2) AS profit_margin_percent
FROM superstore;

-- ============================================================
-- 9. REGION-WISE ANALYSIS
-- ============================================================

SELECT
    region,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit
FROM superstore
GROUP BY region
ORDER BY total_sales DESC;

-- ============================================================
-- 10. CATEGORY-WISE ANALYSIS
-- ============================================================

SELECT
    category,
    SUM(profit) AS total_profit
FROM superstore
GROUP BY category
ORDER BY total_profit DESC;

-- ============================================================
-- 11. YEAR-WISE SALES TREND
-- ============================================================

SELECT
    EXTRACT(YEAR FROM order_date) AS year,
    SUM(sales) AS total_sales
FROM superstore
GROUP BY year
ORDER BY year;

-- ============================================================
-- 12. TOP 10 CUSTOMERS BY SALES
-- ============================================================

SELECT
    customer_name,
    SUM(sales) AS total_sales
FROM superstore
GROUP BY customer_name
ORDER BY total_sales DESC
LIMIT 10;

-- ============================================================
-- 13. TOP 5 PRODUCTS BY SALES
-- ============================================================

SELECT
    product_name,
    SUM(sales) AS total_sales
FROM superstore
GROUP BY product_name
ORDER BY total_sales DESC
LIMIT 5;
