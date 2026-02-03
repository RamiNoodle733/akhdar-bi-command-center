-- ============================================
-- Akhdar BI Command Center
-- 03_staging_tables.sql - Cleaned/Typed Data
-- ============================================

-- ----------------------------------------
-- STAGING: Orders (1 row per order)
-- ----------------------------------------
DROP TABLE IF EXISTS staging.stg_orders CASCADE;
CREATE TABLE staging.stg_orders (
    order_id BIGINT PRIMARY KEY,
    order_number VARCHAR(50),
    email VARCHAR(255),
    financial_status VARCHAR(50),
    fulfillment_status VARCHAR(50),
    currency VARCHAR(10),
    subtotal NUMERIC(10, 2),
    shipping NUMERIC(10, 2),
    taxes NUMERIC(10, 2),
    total NUMERIC(10, 2),
    discount_code VARCHAR(100),
    discount_amount NUMERIC(10, 2),
    refunded_amount NUMERIC(10, 2),
    shipping_method VARCHAR(100),
    risk_level VARCHAR(20),
    source VARCHAR(50),
    payment_method VARCHAR(100),
    billing_city VARCHAR(100),
    billing_province VARCHAR(50),
    billing_country VARCHAR(10),
    billing_zip VARCHAR(20),
    shipping_city VARCHAR(100),
    shipping_province VARCHAR(50),
    shipping_country VARCHAR(10),
    shipping_zip VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE,
    paid_at TIMESTAMP WITH TIME ZONE,
    fulfilled_at TIMESTAMP WITH TIME ZONE,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- STAGING: Order Lines (1 row per line item)
-- ----------------------------------------
DROP TABLE IF EXISTS staging.stg_order_lines CASCADE;
CREATE TABLE staging.stg_order_lines (
    order_line_id SERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    order_number VARCHAR(50),
    line_number INTEGER,
    lineitem_name VARCHAR(500),
    lineitem_sku VARCHAR(100),
    lineitem_quantity INTEGER,
    lineitem_price NUMERIC(10, 2),
    lineitem_compare_at_price NUMERIC(10, 2),
    lineitem_discount NUMERIC(10, 2),
    lineitem_fulfillment_status VARCHAR(50),
    vendor VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES staging.stg_orders(order_id)
);

-- ----------------------------------------
-- STAGING: Products
-- ----------------------------------------
DROP TABLE IF EXISTS staging.stg_products CASCADE;
CREATE TABLE staging.stg_products (
    product_id SERIAL PRIMARY KEY,
    handle VARCHAR(255) UNIQUE,
    title VARCHAR(500),
    vendor VARCHAR(255),
    product_category VARCHAR(500),
    product_type VARCHAR(100),
    tags TEXT,
    variant_sku VARCHAR(100),
    variant_price NUMERIC(10, 2),
    variant_compare_at_price NUMERIC(10, 2),
    variant_inventory_qty INTEGER,
    is_published BOOLEAN,
    status VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- STAGING: Customers
-- ----------------------------------------
DROP TABLE IF EXISTS staging.stg_customers CASCADE;
CREATE TABLE staging.stg_customers (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    accepts_email_marketing BOOLEAN,
    accepts_sms_marketing BOOLEAN,
    city VARCHAR(100),
    province VARCHAR(100),
    province_code VARCHAR(10),
    country VARCHAR(100),
    country_code VARCHAR(10),
    zip VARCHAR(20),
    total_spent NUMERIC(10, 2),
    total_orders INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- STAGING: Product SKU Map
-- ----------------------------------------
DROP TABLE IF EXISTS staging.stg_product_sku_map CASCADE;
CREATE TABLE staging.stg_product_sku_map (
    internal_sku VARCHAR(50) PRIMARY KEY,
    lineitem_name VARCHAR(500),
    product_handle VARCHAR(255),
    size_ml INTEGER,
    recipe_id VARCHAR(50),
    product_category VARCHAR(100),
    is_active BOOLEAN,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- STAGING: Material Costs
-- ----------------------------------------
DROP TABLE IF EXISTS staging.stg_material_costs CASCADE;
CREATE TABLE staging.stg_material_costs (
    material_id VARCHAR(50) PRIMARY KEY,
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
-- STAGING: Recipes
-- ----------------------------------------
DROP TABLE IF EXISTS staging.stg_recipes CASCADE;
CREATE TABLE staging.stg_recipes (
    recipe_line_id SERIAL PRIMARY KEY,
    recipe_id VARCHAR(50),
    recipe_name VARCHAR(100),
    variant VARCHAR(100),
    batch_size_ml INTEGER,
    ingredient_match VARCHAR(255),
    percent NUMERIC(10, 4),
    amount_ml NUMERIC(10, 4),
    material_id VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- STAGING: Meta Ads (Optional)
-- ----------------------------------------
DROP TABLE IF EXISTS staging.stg_meta_ads CASCADE;
CREATE TABLE staging.stg_meta_ads (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(500),
    reach INTEGER,
    frequency NUMERIC(10, 4),
    impressions INTEGER,
    cpm NUMERIC(10, 4),
    amount_spent NUMERIC(10, 2),
    link_clicks INTEGER,
    cpc NUMERIC(10, 4),
    ctr NUMERIC(10, 4),
    landing_page_views INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------
-- STAGING: Google Search Console (Optional)
-- ----------------------------------------
DROP TABLE IF EXISTS staging.stg_gsc_daily CASCADE;
CREATE TABLE staging.stg_gsc_daily (
    gsc_daily_id SERIAL PRIMARY KEY,
    date DATE,
    clicks INTEGER,
    impressions INTEGER,
    ctr NUMERIC(10, 4),
    position NUMERIC(10, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
