-- 02_data_cleaning.sql
-- Step 1: Deduplicate retail_sales_raw (Fixing the double-append)
DELETE FROM retail_sales_raw
WHERE ctid NOT IN (
    SELECT MIN(ctid)
    FROM retail_sales_raw
    GROUP BY invoice_no, stock_code, quantity, invoice_date, unit_price, customer_id
);

-- Step 2: Create Cleaned Retail Table (Removing returns/outliers)
CREATE TABLE IF NOT EXISTS fct_retail_sales AS
SELECT 
    invoice_no,
    stock_code,
    description,
    quantity,
    invoice_date,
    unit_price,
    customer_id,
    country,
    (quantity * unit_price) as line_total
FROM retail_sales_raw
WHERE quantity > 0 
  AND unit_price > 0 
  AND customer_id IS NOT NULL;

-- Step 3: Create a Unified Product Dim Table
CREATE TABLE IF NOT EXISTS dim_products_retail AS
SELECT DISTINCT 
    stock_code, 
    MAX(description) as product_name
FROM retail_sales_raw
GROUP BY stock_code;
