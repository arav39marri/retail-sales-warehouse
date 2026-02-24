-- Gold Layer: Business-ready aggregated views
-- This layer provides analytics-ready summaries and KPIs

-- Daily Sales Summary
CREATE TABLE IF NOT EXISTS gold.daily_sales_summary AS
SELECT 
    DATE(sale_date) AS sale_date,
    store_id,
    COUNT(DISTINCT sale_id) AS total_transactions,
    SUM(quantity) AS total_units_sold,
    SUM(total_amount) AS total_revenue,
    SUM(discount) AS total_discounts,
    AVG(total_amount) AS avg_transaction_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM clean.sales
GROUP BY DATE(sale_date), store_id;

-- Customer 360 View
CREATE TABLE IF NOT EXISTS gold.customer_360 AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.customer_segment,
    COUNT(s.sale_id) AS total_transactions,
    SUM(s.total_amount) AS lifetime_value,
    AVG(s.total_amount) AS avg_order_value,
    MIN(s.sale_date) AS first_purchase_date,
    MAX(s.sale_date) AS last_purchase_date,
    DATE_PART('day', MAX(s.sale_date) - MIN(s.sale_date)) AS customer_tenure_days
FROM clean.customers c
LEFT JOIN clean.sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.customer_segment;

-- Product Performance
CREATE TABLE IF NOT EXISTS gold.product_performance AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    COUNT(s.sale_id) AS times_sold,
    SUM(s.quantity) AS units_sold,
    SUM(s.total_amount) AS total_revenue,
    AVG(s.unit_price) AS avg_selling_price,
    p.list_price,
    p.cost_price,
    (SUM(s.total_amount) - (p.cost_price * SUM(s.quantity))) AS gross_profit
FROM clean.products p
LEFT JOIN clean.sales s ON p.product_id = s.product_id
GROUP BY p.product_id, p.product_name, p.category, p.brand, p.list_price, p.cost_price;

-- Store Performance
CREATE TABLE IF NOT EXISTS gold.store_performance AS
SELECT 
    store_id,
    COUNT(DISTINCT sale_id) AS total_transactions,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_transaction,
    COUNT(DISTINCT customer_id) AS customers_served
FROM clean.sales
GROUP BY store_id;
