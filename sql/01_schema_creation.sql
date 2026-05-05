-- 01_schema_creation.sql
-- Core Schema for Retail Analytics

CREATE TABLE IF NOT EXISTS retail_sales_raw (
    invoice_no VARCHAR(20),
    stock_code VARCHAR(20),
    description TEXT,
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(10, 2),
    customer_id VARCHAR(20),
    country VARCHAR(100)
);

-- Example: Revenue by Product (Retail)
CREATE OR REPLACE VIEW view_retail_revenue AS
SELECT 
    stock_code, 
    description, 
    SUM(quantity * unit_price) as total_revenue,
    SUM(quantity) as total_quantity
FROM retail_sales_raw
WHERE quantity > 0 AND unit_price > 0
GROUP BY stock_code, description;
