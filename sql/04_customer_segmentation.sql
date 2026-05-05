-- 04_customer_segmentation.sql
-- Goal: Categorize customers into segments like 'Champions', 'At Risk', and 'Loyal'

-- Step 1: Calculate Base RFM metrics
DROP TABLE IF EXISTS rfm_base;
CREATE TEMP TABLE rfm_base AS
WITH customer_metrics AS (
    SELECT 
        customer_id,
        MAX(invoice_date) as last_purchase_date,
        COUNT(DISTINCT invoice_no) as frequency,
        SUM(line_total) as monetary
    FROM fct_retail_sales
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
),
dataset_max_date AS (
    SELECT MAX(invoice_date) as max_date FROM fct_retail_sales
)
SELECT 
    cm.customer_id,
    (EXTRACT(DAY FROM (SELECT max_date FROM dataset_max_date) - cm.last_purchase_date))::INT as recency,
    cm.frequency,
    cm.monetary
FROM customer_metrics cm;

-- Step 2: Assign scores (1-5) based on percentiles
DROP TABLE IF EXISTS rfm_scores;
CREATE TEMP TABLE rfm_scores AS
SELECT 
    customer_id,
    recency,
    frequency,
    monetary,
    NTILE(5) OVER (ORDER BY recency DESC) as r_score, -- Lower recency is better (5)
    NTILE(5) OVER (ORDER BY frequency ASC) as f_score, -- Higher frequency is better (5)
    NTILE(5) OVER (ORDER BY monetary ASC) as m_score   -- Higher monetary is better (5)
FROM rfm_base;

-- Step 3: Final Segmentation
DROP TABLE IF EXISTS customer_segments;
CREATE TABLE customer_segments AS
SELECT 
    customer_id,
    recency,
    frequency,
    monetary,
    (r_score + f_score + m_score) / 3.0 as rfm_score_avg,
    CASE 
        WHEN (r_score + f_score + m_score) >= 13 THEN 'Champions'
        WHEN (r_score + f_score + m_score) >= 10 THEN 'Loyal Customers'
        WHEN (r_score + f_score + m_score) >= 7 THEN 'Potential Loyalist'
        WHEN (r_score + f_score + m_score) >= 4 THEN 'At Risk / Hibernating'
        ELSE 'Lost Customers'
    END as segment
FROM rfm_scores;

-- Preview results
SELECT segment, COUNT(*) as customer_count, ROUND(AVG(monetary)::numeric, 2) as avg_spend
FROM customer_segments
GROUP BY segment
ORDER BY avg_spend DESC;
