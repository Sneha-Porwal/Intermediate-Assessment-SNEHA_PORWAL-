-- Drop tables if they exist (reverse order of dependencies)
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_products;

CREATE TABLE dim_customers (
    customer_sk SERIAL PRIMARY KEY,
    customer_id INT,
    name VARCHAR(100),
    region VARCHAR(50),
    signup_date DATE,
    is_current BOOLEAN,
    effective_from DATE,
    effective_to DATE
);

CREATE TABLE dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2)
);

CREATE TABLE fact_transactions (
    transaction_id INT PRIMARY KEY,
    customer_sk INT,
    product_id INT,
    amount DECIMAL(10,2),
    transaction_date TIMESTAMP,
    status VARCHAR(50),
    channel VARCHAR(50),

    FOREIGN KEY (customer_sk) REFERENCES dim_customers(customer_sk),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);
