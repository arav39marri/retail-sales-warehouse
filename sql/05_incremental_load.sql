-- Incremental Load: Efficient data loading strategies
-- Loads only new/changed data since last load

-- Create watermark table to track last load times
CREATE TABLE IF NOT EXISTS etl.watermarks (
    table_name VARCHAR(100) PRIMARY KEY,
    last_load_timestamp TIMESTAMP,
    last_load_id VARCHAR(50),
    status VARCHAR(20) DEFAULT 'SUCCESS',
    records_loaded INTEGER DEFAULT 0
);

-- Incremental sales load (by timestamp)
CREATE OR REPLACE PROCEDURE etl.load_sales_incremental()
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_load TIMESTAMP;
    v_count INTEGER;
BEGIN
    -- Get last load timestamp
    SELECT COALESCE(last_load_timestamp, '1900-01-01') 
    INTO v_last_load 
    FROM etl.watermarks 
    WHERE table_name = 'pos_sales';
    
    -- Insert new records
    INSERT INTO clean.sales (
        sale_id, sale_date, store_id, product_id, quantity,
        unit_price, discount, total_amount, payment_method, customer_id, loaded_at
    )
    SELECT 
        sale_id, sale_date, store_id, product_id, quantity,
        unit_price, discount, total_amount, payment_method, customer_id, loaded_at
    FROM raw.pos_sales
    WHERE loaded_at > v_last_load
    ON CONFLICT (sale_id) DO NOTHING;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- Update watermark
    INSERT INTO etl.watermarks (table_name, last_load_timestamp, records_loaded)
    VALUES ('pos_sales', CURRENT_TIMESTAMP, v_count)
    ON CONFLICT (table_name) DO UPDATE 
    SET last_load_timestamp = CURRENT_TIMESTAMP, records_loaded = v_count;
    
    COMMIT;
END;
$$;

-- Incremental customer load
CREATE OR REPLACE PROCEDURE etl.load_customers_incremental()
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_load TIMESTAMP;
    v_count INTEGER;
BEGIN
    SELECT COALESCE(last_load_timestamp, '1900-01-01') 
    INTO v_last_load 
    FROM etl.watermarks 
    WHERE table_name = 'crm_customers';
    
    INSERT INTO clean.customers (
        customer_id, first_name, last_name, email, phone,
        address, city, state, zip_code, country, customer_segment, created_at, loaded_at
    )
    SELECT 
        customer_id, first_name, last_name, email, phone,
        address, city, state, zip_code, country, customer_segment, created_at, loaded_at
    FROM raw.crm_customers
    WHERE loaded_at > v_last_load
    ON CONFLICT (customer_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        email = EXCLUDED.email;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    INSERT INTO etl.watermarks (table_name, last_load_timestamp, records_loaded)
    VALUES ('crm_customers', CURRENT_TIMESTAMP, v_count)
    ON CONFLICT (table_name) DO UPDATE 
    SET last_load_timestamp = CURRENT_TIMESTAMP, records_loaded = v_count;
    
    COMMIT;
END;
$$;

-- Master procedure to run all incremental loads
CREATE OR REPLACE PROCEDURE etl.run_incremental_load()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL etl.load_sales_incremental();
    CALL etl.load_customers_incremental();
    -- Add more incremental loads here
END;
$$;
