-- 03_analytical_queries.sql
-- Pillar 1: Inventory & Financials (Online Retail Dataset)

-- 1. ABC ANALYSIS (Pareto Principle)
-- Identify the top revenue-generating products (A-items)
WITH product_revenue AS (
    SELECT 
        stock_code,
        SUM(quantity * unit_price) as revenue
    FROM retail_sales_raw
    WHERE quantity > 0 AND unit_price > 0
    GROUP BY stock_code
),
revenue_ranked AS (
    SELECT 
        stock_code,
        revenue,
        SUM(revenue) OVER (ORDER BY revenue DESC) / SUM(revenue) OVER () as cumulative_pct
    FROM product_revenue
)
SELECT 
    stock_code,
    revenue,
    CASE 
        WHEN cumulative_pct <= 0.80 THEN 'A'
        WHEN cumulative_pct <= 0.95 THEN 'B'
        ELSE 'C'
    END as abc_category
FROM revenue_ranked;

-- 2. RFM SEGMENTATION (Customer Value)
WITH customer_metrics AS (
    SELECT 
        customer_id,
        MAX(invoice_date) as last_purchase_date,
        COUNT(DISTINCT invoice_no) as frequency,
        SUM(quantity * unit_price) as monetary
    FROM retail_sales_raw
    WHERE quantity > 0 AND customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT 
    customer_id,
    NTILE(5) OVER (ORDER BY last_purchase_date) as recency_score,
    NTILE(5) OVER (ORDER BY frequency) as frequency_score,
    NTILE(5) OVER (ORDER BY monetary) as monetary_score
FROM customer_metrics;

-- Pillar 2: Demand & Behavior (Instacart Dataset)

-- 3. MARKET BASKET ANALYSIS (Association)
-- Find products frequently bought together in the same order
WITH order_pairs AS (
    SELECT 
        a.product_id as prod_a,
        b.product_id as prod_b
    FROM instacart_order_items a
    JOIN instacart_order_items b ON a.order_id = b.order_id AND a.product_id < b.product_id
    LIMIT 1000000 -- Limit for performance on initial runs
)
SELECT 
    pa.product_name as product_1,
    pb.product_name as product_2,
    COUNT(*) as times_bought_together
FROM order_pairs op
JOIN instacart_products pa ON op.prod_a = pa.product_id
JOIN instacart_products pb ON op.prod_b = pb.product_id
GROUP BY pa.product_name, pb.product_name
ORDER BY times_bought_together DESC
LIMIT 10;

-- 4. PEAK DEMAND ANALYSIS
-- Identify busiest times for inventory replenishment planning
SELECT 
    order_hour_of_day,
    COUNT(order_id) as total_orders
FROM instacart_orders
GROUP BY order_hour_of_day
ORDER BY total_orders DESC;
