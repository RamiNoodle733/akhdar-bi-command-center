-- ============================================
-- Akhdar BI Command Center
-- 06_powerbi_export.sql - PII-Safe Export Views
-- ============================================
-- These views are designed for Power BI import.
-- NO PII fields (emails, addresses, phone numbers, names).
-- Use hashed customer IDs for joins.

-- ----------------------------------------
-- EXPORT: Fact Orders (PII-safe)
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.fact_orders CASCADE;
CREATE VIEW powerbi_export.fact_orders AS
SELECT
    fo.order_key,
    fo.order_id,
    fo.order_number,
    fo.order_date_key,
    fo.customer_key,
    fo.channel_key,
    fo.shipping_method_key,
    fo.gross_product_sales,
    fo.order_discount_amount,
    fo.subtotal,
    fo.shipping_amount,
    fo.tax_amount,
    fo.total_amount,
    fo.refunded_amount,
    fo.net_sales,
    fo.line_item_count,
    fo.unit_count,
    fo.financial_status,
    fo.fulfillment_status,
    fo.risk_level,
    fo.created_at,
    fo.paid_at,
    fo.fulfilled_at
FROM warehouse.fact_order fo;

-- ----------------------------------------
-- EXPORT: Fact Order Lines (PII-safe)
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.fact_order_lines CASCADE;
CREATE VIEW powerbi_export.fact_order_lines AS
SELECT
    fol.order_line_key,
    fol.order_key,
    fol.order_id,
    fol.line_number,
    fol.product_key,
    fol.order_date_key,
    fol.quantity,
    fol.unit_price,
    fol.gross_line_revenue,
    fol.line_discount,
    fol.allocated_order_discount,
    fol.net_line_revenue,
    fol.estimated_cogs,
    fol.has_missing_cost,
    fol.gross_margin,
    fol.margin_percent
FROM warehouse.fact_order_line fol;

-- ----------------------------------------
-- EXPORT: Dim Date
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.dim_date CASCADE;
CREATE VIEW powerbi_export.dim_date AS
SELECT * FROM warehouse.dim_date;

-- ----------------------------------------
-- EXPORT: Dim Product
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.dim_product CASCADE;
CREATE VIEW powerbi_export.dim_product AS
SELECT
    product_key,
    internal_sku,
    product_handle,
    product_title,
    size_ml,
    recipe_id,
    product_category,
    vendor,
    variant_price,
    is_active
FROM warehouse.dim_product;

-- ----------------------------------------
-- EXPORT: Dim Customer (PII-safe)
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.dim_customer CASCADE;
CREATE VIEW powerbi_export.dim_customer AS
SELECT
    customer_key,
    customer_id_hash,  -- Hashed, not raw email
    -- NO: email, first_name, last_name, phone, address
    city,
    province,
    province_code,
    country,
    country_code,
    accepts_email_marketing,
    accepts_sms_marketing,
    first_order_date,
    total_orders,
    total_spent,
    customer_segment
FROM warehouse.dim_customer;

-- ----------------------------------------
-- EXPORT: Dim Channel
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.dim_channel CASCADE;
CREATE VIEW powerbi_export.dim_channel AS
SELECT * FROM warehouse.dim_channel;

-- ----------------------------------------
-- EXPORT: Dim Shipping Method
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.dim_shipping_method CASCADE;
CREATE VIEW powerbi_export.dim_shipping_method AS
SELECT * FROM warehouse.dim_shipping_method;

-- ----------------------------------------
-- EXPORT: Dim Material
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.dim_material CASCADE;
CREATE VIEW powerbi_export.dim_material AS
SELECT * FROM warehouse.dim_material;

-- ----------------------------------------
-- EXPORT: KPI Summary (pre-aggregated for cards)
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.kpi_summary CASCADE;
CREATE VIEW powerbi_export.kpi_summary AS
SELECT
    'all_time' as period,
    total_orders,
    total_units_sold,
    total_gross_product_sales,
    total_discounts,
    total_refunds,
    total_net_sales,
    refund_rate_percent,
    overall_aov,
    orders_with_discount,
    discount_usage_rate_percent
FROM marts.kpi_revenue_totals;

-- ----------------------------------------
-- EXPORT: Marketing (if available)
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.fact_marketing CASCADE;
CREATE VIEW powerbi_export.fact_marketing AS
SELECT * FROM warehouse.fact_marketing_spend;

-- ----------------------------------------
-- EXPORT: SEO (if available)
-- ----------------------------------------
DROP VIEW IF EXISTS powerbi_export.fact_seo_daily CASCADE;
CREATE VIEW powerbi_export.fact_seo_daily AS
SELECT
    fg.gsc_daily_key,
    fg.date_key,
    fg.clicks,
    fg.impressions,
    fg.ctr,
    fg.avg_position
FROM warehouse.fact_gsc_daily fg;
