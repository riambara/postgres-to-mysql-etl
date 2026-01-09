-- Create data warehouse database
CREATE DATABASE IF NOT EXISTS data_warehouse;
USE data_warehouse;

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    supplier_id INT,
    supplier_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    profit_margin DECIMAL(5,2),
    stock_quantity INT,
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_date DATE,
    status VARCHAR(20),
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);

-- Create indexes
CREATE INDEX idx_fact_orders_date ON fact_orders(order_date);
CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_id);
CREATE INDEX idx_fact_orders_product ON fact_orders(product_id);