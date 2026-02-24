-- Performance Optimization: Indexes, partitions, and query tuning

-- Create indexes for common query patterns

-- Sales table indexes
CREATE INDEX IF NOT EXISTS idx_sales_date ON clean.sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_store ON clean.sales(store_id);
CREATE INDEX IF NOT EXISTS idx_sales_product ON clean.sales(product_id);
CREATE INDEX IF NOT EXISTS idx_sales_customer ON clean.sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_date_store ON clean.sales(sale_date, store_id);

-- Composite index for common queries
CREATE INDEX IF NOT EXISTS idx_sales_product_date ON clean.sales(product_id, sale_date);

-- Products indexes
CREATE INDEX IF NOT EXISTS idx_products_category ON clean.products(category);
CREATE INDEX IF NOT EXISTS idx_products_brand ON clean.products(brand);

-- Customers indexes
CREATE INDEX IF NOT EXISTS idx_customers_segment ON clean.customers(customer_segment);
CREATE INDEX IF NOT EXISTS idx_customers_city ON clean.customers(city);

-- Partition sales by month for better query performance
CREATE TABLE IF NOT EXISTS clean.sales_partitioned (
    LIKE clean.sales INCLUDING ALL
) PARTITION BY RANGE (sale_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS clean.sales_2024_01 PARTITION OF clean.sales_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE IF NOT EXISTS clean.sales_2024_02 PARTITION OF clean.sales_partitioned
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- Materialized view for fast reporting
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_store_sales AS
SELECT 
    DATE_TRUNC('month', sale_date) AS month,
    store_id,
    SUM(total_amount) AS revenue,
    COUNT(*) AS transactions
FROM clean.sales
GROUP BY DATE_TRUNC('month', sale_date), store_id
WITH DATA;

-- Refresh materialized view (schedule via cron)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_monthly_store 
ON mv_monthly_store_sales(month, store_id);

-- Analyze tables for query planner
ANALYZE clean.sales;
ANALYZE clean.products;
ANALYZE clean.customers;

-- Set statistics target for better query plans
ALTER TABLE clean.sales ALTER COLUMN sale_date SET STATISTICS 100;
ALTER TABLE clean.sales ALTER COLUMN store_id SET STATISTICS 100;
