-- Silver Layer: Cleaned and validated data
-- This layer handles data quality, deduplication, and standardization

-- Cleaned Sales
CREATE TABLE IF NOT EXISTS clean.sales AS
SELECT 
    sale_id,
    COALESCE(sale_date, CURRENT_TIMESTAMP) AS sale_date,
    store_id,
    product_id,
    COALESCE(quantity, 0) AS quantity,
    COALESCE(unit_price, 0) AS unit_price,
    COALESCE(discount, 0) AS discount,
    COALESCE(total_amount, 0) AS total_amount,
    payment_method,
    customer_id,
    loaded_at
FROM raw.pos_sales
WHERE sale_id IS NOT NULL;

-- Cleaned Products
CREATE TABLE IF NOT EXISTS clean.products AS
SELECT 
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    supplier_id,
    COALESCE(cost_price, 0) AS cost_price,
    COALESCE(list_price, 0) AS list_price,
    loaded_at
FROM raw.products
WHERE product_id IS NOT NULL;

-- Cleaned Customers
CREATE TABLE IF NOT EXISTS clean.customers AS
SELECT 
    customer_id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    LOWER(TRIM(email)) AS email,
    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS phone,
    TRIM(address) AS address,
    TRIM(city) AS city,
    TRIM(state) AS state,
    TRIM(zip_code) AS zip_code,
    TRIM(country) AS country,
    customer_segment,
    created_at,
    loaded_at
FROM raw.crm_customers
WHERE customer_id IS NOT NULL;

-- Cleaned Inventory
CREATE TABLE IF NOT EXISTS clean.inventory AS
SELECT 
    inventory_id,
    product_id,
    warehouse_id,
    COALESCE(quantity_on_hand, 0) AS quantity_on_hand,
    COALESCE(quantity_reserved, 0) AS quantity_reserved,
    COALESCE(quantity_available, 0) AS quantity_available,
    last_updated,
    loaded_at
FROM raw.erp_inventory
WHERE inventory_id IS NOT NULL;
