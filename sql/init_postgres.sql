-- Create raw_data schema
CREATE SCHEMA IF NOT EXISTS raw_data;

-- Customers table
CREATE TABLE IF NOT EXISTS raw_data.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Suppliers table
CREATE TABLE IF NOT EXISTS raw_data.suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(100),
    contact_name VARCHAR(100),
    contact_phone VARCHAR(20),
    address VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products table
CREATE TABLE IF NOT EXISTS raw_data.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    supplier_id INT REFERENCES raw_data.suppliers(supplier_id),
    category VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    stock_quantity INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE IF NOT EXISTS raw_data.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES raw_data.customers(customer_id),
    product_id INT REFERENCES raw_data.products(product_id),
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_date DATE,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_customers_updated ON raw_data.customers(updated_at);
CREATE INDEX idx_products_updated ON raw_data.products(updated_at);
CREATE INDEX idx_orders_updated ON raw_data.orders(updated_at);
CREATE INDEX idx_suppliers_updated ON raw_data.suppliers(updated_at);