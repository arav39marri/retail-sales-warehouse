-- SCD Type 2: Slowly Changing Dimensions
-- Tracks historical changes in dimension tables

-- SCD for Customers (track address/segment changes)
CREATE TABLE IF NOT EXISTS gold.dim_customer_scd (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(30) NOT NULL,
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
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- Function to handle SCD Type 2 for customers
CREATE OR REPLACE FUNCTION scd.update_customer_scd()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if any tracked columns changed
    IF OLD.address <> NEW.address 
       OR OLD.city <> NEW.city 
       OR OLD.state <> NEW.state
       OR OLD.customer_segment <> NEW.customer_segment THEN
        -- Mark current record as expired
        UPDATE gold.dim_customer_scd 
        SET effective_to = CURRENT_TIMESTAMP, is_current = FALSE 
        WHERE customer_id = OLD.customer_id AND is_current = TRUE;
        
        -- Insert new record
        INSERT INTO gold.dim_customer_scd (
            customer_id, first_name, last_name, email, phone,
            address, city, state, zip_code, country, customer_segment
        ) VALUES (
            NEW.customer_id, NEW.first_name, NEW.last_name, NEW.email, NEW.phone,
            NEW.address, NEW.city, NEW.state, NEW.zip_code, NEW.country, NEW.customer_segment
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER trg_customer_scd
AFTER UPDATE ON clean.customers
FOR EACH ROW
EXECUTE FUNCTION scd.update_customer_scd();

-- SCD for Products (track price/category changes)
CREATE TABLE IF NOT EXISTS gold.dim_product_scd (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(30) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    brand VARCHAR(100),
    cost_price DECIMAL(10,2),
    list_price DECIMAL(10,2),
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- Function to handle SCD Type 2 for products
CREATE OR REPLACE FUNCTION scd.update_product_scd()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.category <> NEW.category 
       OR OLD.sub_category <> NEW.sub_category
       OR OLD.list_price <> NEW.list_price
       OR OLD.cost_price <> NEW.cost_price THEN
        UPDATE gold.dim_product_scd 
        SET effective_to = CURRENT_TIMESTAMP, is_current = FALSE 
        WHERE product_id = OLD.product_id AND is_current = TRUE;
        
        INSERT INTO gold.dim_product_scd (
            product_id, product_name, category, sub_category, 
            brand, cost_price, list_price
        ) VALUES (
            NEW.product_id, NEW.product_name, NEW.category, NEW.sub_category,
            NEW.brand, NEW.cost_price, NEW.list_price
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_product_scd
AFTER UPDATE ON clean.products
FOR EACH ROW
EXECUTE FUNCTION scd.update_product_scd();
