import psycopg2


def create_tables():

    DB_CONFIG = {
        "host": "localhost",
        "database": "assessment_db",
        "user": "postgres",
        "password": "Tiger",
        "port": 5432
    }

    print("Connecting to PostgreSQL...")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    schema_sql = """

    -- ==================================================
    -- DROP TABLES
    -- ==================================================
    DROP TABLE IF EXISTS fact_transactions CASCADE;
    DROP TABLE IF EXISTS dim_customers CASCADE;
    DROP TABLE IF EXISTS dim_products CASCADE;

    -- ==================================================
    -- DIM CUSTOMERS (SCD TYPE 2)
    -- ==================================================
    CREATE TABLE dim_customers (
        customer_sk SERIAL PRIMARY KEY,
        customer_id INT NOT NULL,
        name VARCHAR(100),
        region VARCHAR(50),
        signup_date DATE,
        is_current BOOLEAN NOT NULL DEFAULT TRUE,
        effective_from DATE,
        effective_to DATE
    );

    CREATE INDEX idx_customer_lookup
        ON dim_customers(customer_id, is_current);

    CREATE INDEX idx_customer_id
        ON dim_customers(customer_id);

    -- ==================================================
    -- DIM PRODUCTS
    -- ==================================================
    CREATE TABLE dim_products (
        product_id INT PRIMARY KEY,
        product_name VARCHAR(100),
        category VARCHAR(50),
        price DECIMAL(10,2) NOT NULL
    );

    CREATE INDEX idx_product_category
        ON dim_products(category);

    -- ==================================================
    -- FACT TRANSACTIONS
    -- ==================================================
    CREATE TABLE fact_transactions (
        transaction_id INT PRIMARY KEY,
        customer_sk INT NOT NULL,
        product_id INT NOT NULL,
        amount DECIMAL(10,2) NOT NULL,
        transaction_date TIMESTAMP NOT NULL,
        status VARCHAR(50),
        channel VARCHAR(50),
        fraud_flag BOOLEAN NOT NULL DEFAULT FALSE,

        FOREIGN KEY (customer_sk) REFERENCES dim_customers(customer_sk),
        FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
    );

    CREATE INDEX idx_fact_date
        ON fact_transactions(transaction_date);

    CREATE INDEX idx_fact_customer
        ON fact_transactions(customer_sk);

    CREATE INDEX idx_fact_product
        ON fact_transactions(product_id);

    CREATE INDEX idx_fact_fraud
        ON fact_transactions(fraud_flag);

    """

    cursor.execute(schema_sql)
    conn.commit()

    cursor.close()
    conn.close()

    print("Warehouse schema created successfully.")
