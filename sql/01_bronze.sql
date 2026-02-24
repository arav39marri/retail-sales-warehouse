-- Bronze Layer: Raw data ingestion from source systems
-- This layer preserves raw data as-is from source systems

-- Raw POS Sales
CREATE TABLE IF NOT EXISTS raw.pos_sales (
    sale_id VARCHAR(50) PRIMARY KEY,
    sale_date TIMESTAMP,
    store_id VARCHAR(20),
    product_id VARCHAR(30),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_method VARCHAR(20),
    customer_id VARCHAR(30),
    source_file VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw ERP Inventory
CREATE TABLE IF NOT EXISTS raw.erp_inventory (
    inventory_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(30),
    warehouse_id VARCHAR(20),
    quantity_on_hand INTEGER,
    quantity_reserved INTEGER,
    quantity_available INTEGER,
    last_updated TIMESTAMP,
    source_file VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw CRM Customers
CREATE TABLE IF NOT EXISTS raw.crm_customers (
    customer_id VARCHAR(30) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    created_at TIMESTAMP,
    source_file VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw Products
CREATE TABLE IF NOT EXISTS raw.products (
    product_id VARCHAR(30) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    brand VARCHAR(100),
    supplier_id VARCHAR(30),
    cost_price DECIMAL(10,2),
    list_price DECIMAL(10,2),
    source_file VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
