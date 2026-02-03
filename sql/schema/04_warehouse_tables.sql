-- ============================================
-- Akhdar BI Command Center
-- 04_warehouse_tables.sql - Star Schema
-- ============================================

-- ----------------------------------------
-- DIMENSION: Date
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.dim_date CASCADE;
CREATE TABLE warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,  -- YYYYMMDD format
    full_date DATE NOT NULL UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Populate dim_date for 2025-2027
INSERT INTO warehouse.dim_date (date_key, full_date, year, quarter, month, month_name, 
    week_of_year, day_of_month, day_of_week, day_name, is_weekend, fiscal_year, fiscal_quarter)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
    d as full_date,
    EXTRACT(YEAR FROM d)::INTEGER as year,
    EXTRACT(QUARTER FROM d)::INTEGER as quarter,
    EXTRACT(MONTH FROM d)::INTEGER as month,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(WEEK FROM d)::INTEGER as week_of_year,
    EXTRACT(DAY FROM d)::INTEGER as day_of_month,
    EXTRACT(DOW FROM d)::INTEGER as day_of_week,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend,
    EXTRACT(YEAR FROM d)::INTEGER as fiscal_year,
    EXTRACT(QUARTER FROM d)::INTEGER as fiscal_quarter
FROM generate_series('2025-01-01'::date, '2027-12-31'::date, '1 day'::interval) d;

-- ----------------------------------------
-- DIMENSION: Product
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.dim_product CASCADE;
CREATE TABLE warehouse.dim_product (
    product_key SERIAL PRIMARY KEY,
    internal_sku VARCHAR(50) UNIQUE NOT NULL,
    product_handle VARCHAR(255),
    product_title VARCHAR(500),
    size_ml INTEGER,
    recipe_id VARCHAR(50),
    product_category VARCHAR(100),
    vendor VARCHAR(255),
    variant_price NUMERIC(10, 2),
    is_active BOOLEAN,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- DIMENSION: Customer (PII-safe)
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.dim_customer CASCADE;
CREATE TABLE warehouse.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id_hash VARCHAR(64) UNIQUE NOT NULL,  -- SHA256 of email
    customer_id BIGINT,
    city VARCHAR(100),
    province VARCHAR(100),
    province_code VARCHAR(10),
    country VARCHAR(100),
    country_code VARCHAR(10),
    accepts_email_marketing BOOLEAN,
    accepts_sms_marketing BOOLEAN,
    first_order_date DATE,
    total_orders INTEGER DEFAULT 0,
    total_spent NUMERIC(10, 2) DEFAULT 0,
    customer_segment VARCHAR(50),  -- new, returning, vip
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- DIMENSION: Channel
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.dim_channel CASCADE;
CREATE TABLE warehouse.dim_channel (
    channel_key SERIAL PRIMARY KEY,
    channel_code VARCHAR(50) UNIQUE NOT NULL,
    channel_name VARCHAR(100),
    channel_type VARCHAR(50),  -- web, pos, api
    is_online BOOLEAN,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed default channels
INSERT INTO warehouse.dim_channel (channel_code, channel_name, channel_type, is_online) VALUES
('web', 'Website', 'web', TRUE),
('pos', 'Point of Sale', 'pos', FALSE),
('api', 'API', 'api', TRUE),
('unknown', 'Unknown', 'other', TRUE);

-- ----------------------------------------
-- DIMENSION: Shipping Method
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.dim_shipping_method CASCADE;
CREATE TABLE warehouse.dim_shipping_method (
    shipping_method_key SERIAL PRIMARY KEY,
    shipping_method_code VARCHAR(100) UNIQUE NOT NULL,
    shipping_method_name VARCHAR(100),
    is_local_delivery BOOLEAN,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- DIMENSION: Material
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.dim_material CASCADE;
CREATE TABLE warehouse.dim_material (
    material_key SERIAL PRIMARY KEY,
    material_id VARCHAR(50) UNIQUE NOT NULL,
    material_name VARCHAR(255),
    ingredient_match VARCHAR(255),
    category VARCHAR(100),
    unit VARCHAR(20),
    cost_per_unit NUMERIC(10, 4),
    cost_per_ml NUMERIC(10, 4),
    has_known_cost BOOLEAN,
    supplier VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- FACT: Order (1 row per order - order-level metrics)
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.fact_order CASCADE;
CREATE TABLE warehouse.fact_order (
    order_key SERIAL PRIMARY KEY,
    order_id BIGINT UNIQUE NOT NULL,
    order_number VARCHAR(50),
    order_date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    customer_key INTEGER REFERENCES warehouse.dim_customer(customer_key),
    channel_key INTEGER REFERENCES warehouse.dim_channel(channel_key),
    shipping_method_key INTEGER REFERENCES warehouse.dim_shipping_method(shipping_method_key),
    
    -- Order-level amounts (not duplicated from line items)
    gross_product_sales NUMERIC(10, 2),  -- SUM(lineitem_price * qty) for this order
    order_discount_amount NUMERIC(10, 2),
    subtotal NUMERIC(10, 2),
    shipping_amount NUMERIC(10, 2),
    tax_amount NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    refunded_amount NUMERIC(10, 2),
    net_sales NUMERIC(10, 2),  -- subtotal - refunded_amount
    
    -- Counts
    line_item_count INTEGER,
    unit_count INTEGER,
    
    -- Status
    financial_status VARCHAR(50),
    fulfillment_status VARCHAR(50),
    risk_level VARCHAR(20),
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE,
    paid_at TIMESTAMP WITH TIME ZONE,
    fulfilled_at TIMESTAMP WITH TIME ZONE,
    
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- FACT: Order Line (1 row per line item)
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.fact_order_line CASCADE;
CREATE TABLE warehouse.fact_order_line (
    order_line_key SERIAL PRIMARY KEY,
    order_key INTEGER REFERENCES warehouse.fact_order(order_key),
    order_id BIGINT,
    line_number INTEGER,
    product_key INTEGER REFERENCES warehouse.dim_product(product_key),
    order_date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    
    -- Line-level amounts
    quantity INTEGER,
    unit_price NUMERIC(10, 2),
    gross_line_revenue NUMERIC(10, 2),  -- unit_price * quantity
    line_discount NUMERIC(10, 2),       -- direct line discount
    allocated_order_discount NUMERIC(10, 2),  -- proportionally allocated order discount
    net_line_revenue NUMERIC(10, 2),    -- gross - line_discount - allocated_order_discount
    
    -- COGS (from recipe)
    estimated_cogs NUMERIC(10, 4),
    has_missing_cost BOOLEAN,
    gross_margin NUMERIC(10, 4),
    margin_percent NUMERIC(10, 4),
    
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- FACT: COGS Estimate (detail by material)
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.fact_cogs_estimate CASCADE;
CREATE TABLE warehouse.fact_cogs_estimate (
    cogs_key SERIAL PRIMARY KEY,
    order_line_key INTEGER REFERENCES warehouse.fact_order_line(order_line_key),
    product_key INTEGER REFERENCES warehouse.dim_product(product_key),
    material_key INTEGER REFERENCES warehouse.dim_material(material_key),
    
    ingredient_name VARCHAR(255),
    amount_ml NUMERIC(10, 4),
    cost_per_ml NUMERIC(10, 4),
    line_cost NUMERIC(10, 4),
    has_known_cost BOOLEAN,
    
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- FACT: Marketing Spend (Optional)
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.fact_marketing_spend CASCADE;
CREATE TABLE warehouse.fact_marketing_spend (
    marketing_key SERIAL PRIMARY KEY,
    campaign_name VARCHAR(500),
    platform VARCHAR(50) DEFAULT 'meta',
    
    reach INTEGER,
    impressions INTEGER,
    amount_spent NUMERIC(10, 2),
    link_clicks INTEGER,
    landing_page_views INTEGER,
    
    cpm NUMERIC(10, 4),
    cpc NUMERIC(10, 4),
    ctr NUMERIC(10, 4),
    
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- FACT: GSC Daily (Optional)
-- ----------------------------------------
DROP TABLE IF EXISTS warehouse.fact_gsc_daily CASCADE;
CREATE TABLE warehouse.fact_gsc_daily (
    gsc_daily_key SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    
    clicks INTEGER,
    impressions INTEGER,
    ctr NUMERIC(10, 4),
    avg_position NUMERIC(10, 2),
    
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- Create indexes for performance
-- ----------------------------------------
CREATE INDEX idx_fact_order_date ON warehouse.fact_order(order_date_key);
CREATE INDEX idx_fact_order_customer ON warehouse.fact_order(customer_key);
CREATE INDEX idx_fact_order_line_order ON warehouse.fact_order_line(order_key);
CREATE INDEX idx_fact_order_line_product ON warehouse.fact_order_line(product_key);
CREATE INDEX idx_fact_order_line_date ON warehouse.fact_order_line(order_date_key);
