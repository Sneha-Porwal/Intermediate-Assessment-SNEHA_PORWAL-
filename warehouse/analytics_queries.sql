--  Monthly Revenue
SELECT 
    DATE_TRUNC('month', transaction_date) AS month,
    SUM(amount) AS total_revenue
FROM fact_transactions
GROUP BY month
ORDER BY month;


--  Top 5 Products
SELECT 
    product_id,
    SUM(amount) AS total_sales
FROM fact_transactions
GROUP BY product_id
ORDER BY total_sales DESC
LIMIT 5;


-- Regional Performance
SELECT 
    c.region,
    SUM(f.amount) AS total_revenue
FROM fact_transactions f
JOIN dim_customers c
    ON f.customer_sk = c.customer_sk
GROUP BY c.region
ORDER BY total_revenue DESC;


-- 4Customer Retention
SELECT 
    c.customer_id,
    COUNT(f.transaction_id) AS total_transactions
FROM fact_transactions f
JOIN dim_customers c
    ON f.customer_sk = c.customer_sk
GROUP BY c.customer_id
HAVING COUNT(f.transaction_id) > 1;
