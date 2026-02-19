import psycopg2


def run_gold_sql():

    DB_CONFIG = {
        "host": "localhost",
        "database": "assessment_db",
        "user": "postgres",
        "password": "Tiger",
        "port": 5432
    }

    print("Connecting to PostgreSQL for Gold Layer...")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("Creating Gold Aggregation Tables...")

    gold_sql = """

    -- KPI SUMMARY
    DROP TABLE IF EXISTS gold_kpi_summary;

    CREATE TABLE gold_kpi_summary AS
    SELECT
        SUM(amount) AS total_revenue,
        COUNT(transaction_id) AS total_transactions,
        COUNT(DISTINCT customer_sk) AS unique_customers,
        SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) AS total_fraud_transactions
    FROM fact_transactions;


    -- MONTHLY REVENUE
    DROP TABLE IF EXISTS gold_monthly_revenue;

    CREATE TABLE gold_monthly_revenue AS
    SELECT
        DATE_TRUNC('month', transaction_date) AS month,
        SUM(amount) AS total_revenue
    FROM fact_transactions
    GROUP BY month
    ORDER BY month;


    -- TOP PRODUCTS
    DROP TABLE IF EXISTS gold_top_products;

    CREATE TABLE gold_top_products AS
    SELECT
        p.product_id,
        p.product_name,
        SUM(f.amount) AS total_sales
    FROM fact_transactions f
    JOIN dim_products p
        ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_name
    ORDER BY total_sales DESC
    LIMIT 5;


    -- REGIONAL PERFORMANCE
    DROP TABLE IF EXISTS gold_sales_by_region;

    CREATE TABLE gold_sales_by_region AS
    SELECT
        c.region,
        SUM(f.amount) AS total_revenue
    FROM fact_transactions f
    JOIN dim_customers c
        ON f.customer_sk = c.customer_sk
    WHERE c.is_current = TRUE
    GROUP BY c.region
    ORDER BY total_revenue DESC;


    -- CUSTOMER RETENTION
    DROP TABLE IF EXISTS gold_customer_retention;

    CREATE TABLE gold_customer_retention AS
    SELECT
        c.customer_id,
        COUNT(f.transaction_id) AS total_transactions
    FROM fact_transactions f
    JOIN dim_customers c
        ON f.customer_sk = c.customer_sk
    GROUP BY c.customer_id
    HAVING COUNT(f.transaction_id) > 1;


    -- FRAUD SUMMARY
    DROP TABLE IF EXISTS gold_fraud_summary;

    CREATE TABLE gold_fraud_summary AS
    SELECT
        COUNT(*) AS total_flagged_transactions,
        SUM(amount) AS total_flagged_amount
    FROM fact_transactions
    WHERE fraud_flag = TRUE;

    """

    cursor.execute(gold_sql)
    conn.commit()

    cursor.close()
    conn.close()

    print("Gold Layer Tables Created Successfully.")
