-- ============================================
-- Akhdar BI Command Center
-- 05_marts.sql - KPI Views (Business Metrics)
-- ============================================
-- These views define the official KPI calculations.
-- All dashboards should read from these views for consistency.

-- ----------------------------------------
-- KPI: Sales Summary (Daily/Weekly/Monthly)
-- ----------------------------------------
DROP VIEW IF EXISTS marts.kpi_sales_summary CASCADE;
CREATE VIEW marts.kpi_sales_summary AS
SELECT 
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_name,
    d.is_weekend,
    
    -- Order counts
    COUNT(DISTINCT fo.order_id) as orders_count,
    SUM(fo.unit_count) as units_sold,
    
    -- Revenue metrics (from fact_order to avoid double counting)
    SUM(fo.gross_product_sales) as gross_product_sales,
    SUM(fo.order_discount_amount) as discounts_total,
    SUM(fo.refunded_amount) as refunds_total,
    SUM(fo.net_sales) as net_sales,
    SUM(fo.shipping_amount) as shipping_collected,
    SUM(fo.tax_amount) as taxes_collected,
    SUM(fo.total_amount) as total_collected,
    
    -- Averages
    CASE WHEN COUNT(DISTINCT fo.order_id) > 0 
        THEN SUM(fo.net_sales) / COUNT(DISTINCT fo.order_id) 
        ELSE 0 
    END as aov,
    
    CASE WHEN COUNT(DISTINCT fo.order_id) > 0 
        THEN SUM(fo.unit_count)::NUMERIC / COUNT(DISTINCT fo.order_id) 
        ELSE 0 
    END as avg_units_per_order

FROM warehouse.dim_date d
LEFT JOIN warehouse.fact_order fo ON d.date_key = fo.order_date_key
WHERE d.full_date <= CURRENT_DATE
GROUP BY d.date_key, d.full_date, d.year, d.month, d.month_name, 
         d.week_of_year, d.day_name, d.is_weekend
ORDER BY d.full_date DESC;

-- ----------------------------------------
-- KPI: Revenue Totals (Running totals)
-- ----------------------------------------
DROP VIEW IF EXISTS marts.kpi_revenue_totals CASCADE;
CREATE VIEW marts.kpi_revenue_totals AS
SELECT
    -- All-time totals
    COUNT(DISTINCT order_id) as total_orders,
    SUM(unit_count) as total_units_sold,
    SUM(gross_product_sales) as total_gross_product_sales,
    SUM(order_discount_amount) as total_discounts,
    SUM(refunded_amount) as total_refunds,
    SUM(net_sales) as total_net_sales,
    
    -- Rates
    CASE WHEN SUM(gross_product_sales) > 0 
        THEN SUM(refunded_amount) / SUM(gross_product_sales) * 100 
        ELSE 0 
    END as refund_rate_percent,
    
    CASE WHEN COUNT(DISTINCT order_id) > 0 
        THEN SUM(net_sales) / COUNT(DISTINCT order_id) 
        ELSE 0 
    END as overall_aov,
    
    -- Counts with discounts
    COUNT(DISTINCT CASE WHEN order_discount_amount > 0 THEN order_id END) as orders_with_discount,
    CASE WHEN COUNT(DISTINCT order_id) > 0 
        THEN COUNT(DISTINCT CASE WHEN order_discount_amount > 0 THEN order_id END)::NUMERIC 
             / COUNT(DISTINCT order_id) * 100 
        ELSE 0 
    END as discount_usage_rate_percent

FROM warehouse.fact_order;

-- ----------------------------------------
-- KPI: Product Profitability
-- ----------------------------------------
DROP VIEW IF EXISTS marts.kpi_product_profitability CASCADE;
CREATE VIEW marts.kpi_product_profitability AS
SELECT
    dp.internal_sku,
    dp.product_title,
    dp.size_ml,
    dp.variant_price as list_price,
    dp.is_active,
    
    -- Volume
    COUNT(DISTINCT fol.order_key) as orders_containing_product,
    SUM(fol.quantity) as units_sold,
    
    -- Revenue
    SUM(fol.gross_line_revenue) as gross_revenue,
    SUM(fol.line_discount) as line_discounts,
    SUM(fol.allocated_order_discount) as allocated_order_discounts,
    SUM(fol.net_line_revenue) as net_revenue,
    
    -- COGS and Margin
    SUM(fol.estimated_cogs * fol.quantity) as total_estimated_cogs,
    SUM(fol.gross_margin * fol.quantity) as total_gross_margin,
    
    -- Margin percentages
    CASE WHEN SUM(fol.net_line_revenue) > 0 
        THEN SUM(fol.gross_margin * fol.quantity) / SUM(fol.net_line_revenue) * 100 
        ELSE 0 
    END as margin_percent,
    
    -- Per-unit metrics
    CASE WHEN SUM(fol.quantity) > 0 
        THEN SUM(fol.net_line_revenue) / SUM(fol.quantity) 
        ELSE 0 
    END as avg_selling_price,
    
    -- Cost accuracy flag
    BOOL_OR(fol.has_missing_cost) as has_missing_cost,
    
    -- Note about cost completeness
    CASE WHEN BOOL_OR(fol.has_missing_cost) 
        THEN 'COGS incomplete - some ingredients missing costs' 
        ELSE 'COGS complete' 
    END as cogs_status

FROM warehouse.dim_product dp
LEFT JOIN warehouse.fact_order_line fol ON dp.product_key = fol.product_key
GROUP BY dp.product_key, dp.internal_sku, dp.product_title, dp.size_ml, 
         dp.variant_price, dp.is_active
ORDER BY SUM(fol.net_line_revenue) DESC NULLS LAST;

-- ----------------------------------------
-- KPI: COGS Detail by Product
-- ----------------------------------------
DROP VIEW IF EXISTS marts.kpi_cogs_detail CASCADE;
CREATE VIEW marts.kpi_cogs_detail AS
SELECT
    dp.internal_sku,
    dp.product_title,
    dm.material_name,
    dm.category as material_category,
    dm.has_known_cost,
    
    -- Average usage per unit
    AVG(fce.amount_ml) as avg_amount_ml_per_unit,
    AVG(fce.cost_per_ml) as cost_per_ml,
    AVG(fce.line_cost) as avg_cost_per_unit,
    
    -- Total usage
    SUM(fce.amount_ml) as total_ml_used,
    SUM(fce.line_cost) as total_cost

FROM warehouse.fact_cogs_estimate fce
JOIN warehouse.dim_product dp ON fce.product_key = dp.product_key
JOIN warehouse.dim_material dm ON fce.material_key = dm.material_key
GROUP BY dp.internal_sku, dp.product_title, dm.material_name, 
         dm.category, dm.has_known_cost
ORDER BY dp.internal_sku, SUM(fce.line_cost) DESC NULLS LAST;

-- ----------------------------------------
-- KPI: Customer Segments (New vs Returning)
-- ----------------------------------------
DROP VIEW IF EXISTS marts.kpi_customer_segments CASCADE;
CREATE VIEW marts.kpi_customer_segments AS
SELECT
    dc.customer_segment,
    COUNT(DISTINCT dc.customer_key) as customer_count,
    SUM(dc.total_orders) as total_orders,
    SUM(dc.total_spent) as total_revenue,
    AVG(dc.total_spent) as avg_customer_value,
    AVG(dc.total_orders) as avg_orders_per_customer

FROM warehouse.dim_customer dc
WHERE dc.total_orders > 0
GROUP BY dc.customer_segment
ORDER BY SUM(dc.total_spent) DESC;

-- ----------------------------------------
-- KPI: New vs Returning (by order)
-- ----------------------------------------
DROP VIEW IF EXISTS marts.kpi_new_vs_returning CASCADE;
CREATE VIEW marts.kpi_new_vs_returning AS
WITH order_customer AS (
    SELECT 
        fo.order_id,
        fo.order_date_key,
        fo.net_sales,
        dc.customer_key,
        dc.first_order_date,
        d.full_date as order_date,
        CASE 
            WHEN dc.first_order_date = d.full_date THEN 'new'
            ELSE 'returning'
        END as customer_type
    FROM warehouse.fact_order fo
    JOIN warehouse.dim_customer dc ON fo.customer_key = dc.customer_key
    JOIN warehouse.dim_date d ON fo.order_date_key = d.date_key
)
SELECT
    customer_type,
    COUNT(DISTINCT order_id) as orders,
    COUNT(DISTINCT customer_key) as unique_customers,
    SUM(net_sales) as revenue,
    AVG(net_sales) as avg_order_value
FROM order_customer
GROUP BY customer_type;

-- ----------------------------------------
-- KPI: Repeat Purchase Rate
-- ----------------------------------------
DROP VIEW IF EXISTS marts.kpi_repeat_purchase_rate CASCADE;
CREATE VIEW marts.kpi_repeat_purchase_rate AS
SELECT
    COUNT(DISTINCT customer_key) as total_customers_with_orders,
    COUNT(DISTINCT CASE WHEN total_orders = 1 THEN customer_key END) as one_time_customers,
    COUNT(DISTINCT CASE WHEN total_orders > 1 THEN customer_key END) as repeat_customers,
    
    CASE WHEN COUNT(DISTINCT customer_key) > 0 
        THEN COUNT(DISTINCT CASE WHEN total_orders > 1 THEN customer_key END)::NUMERIC 
             / COUNT(DISTINCT customer_key) * 100 
        ELSE 0 
    END as repeat_purchase_rate_percent,
    
    AVG(total_orders) as avg_orders_per_customer,
    AVG(total_spent) as avg_lifetime_value

FROM warehouse.dim_customer
WHERE total_orders > 0;

-- ----------------------------------------
-- KPI: Shipping Method Analysis
-- ----------------------------------------
DROP VIEW IF EXISTS marts.kpi_shipping_methods CASCADE;
CREATE VIEW marts.kpi_shipping_methods AS
SELECT
    sm.shipping_method_name,
    sm.is_local_delivery,
    COUNT(DISTINCT fo.order_id) as order_count,
    SUM(fo.net_sales) as revenue,
    SUM(fo.shipping_amount) as shipping_collected,
    AVG(fo.net_sales) as avg_order_value

FROM warehouse.fact_order fo
JOIN warehouse.dim_shipping_method sm ON fo.shipping_method_key = sm.shipping_method_key
GROUP BY sm.shipping_method_key, sm.shipping_method_name, sm.is_local_delivery
ORDER BY COUNT(DISTINCT fo.order_id) DESC;

-- ----------------------------------------
-- KPI: Marketing Performance (Optional)
-- ----------------------------------------
DROP VIEW IF EXISTS marts.kpi_marketing_performance CASCADE;
CREATE VIEW marts.kpi_marketing_performance AS
SELECT
    campaign_name,
    platform,
    reach,
    impressions,
    amount_spent,
    link_clicks,
    landing_page_views,
    cpm,
    cpc,
    ctr,
    
    -- Estimated ROAS (requires attribution - placeholder)
    NULL::NUMERIC as estimated_roas

FROM warehouse.fact_marketing_spend;

-- ----------------------------------------
-- KPI: SEO Performance (Optional)
-- ----------------------------------------
DROP VIEW IF EXISTS marts.kpi_seo_performance CASCADE;
CREATE VIEW marts.kpi_seo_performance AS
SELECT
    d.full_date,
    d.week_of_year,
    d.month,
    fg.clicks,
    fg.impressions,
    fg.ctr,
    fg.avg_position

FROM warehouse.fact_gsc_daily fg
JOIN warehouse.dim_date d ON fg.date_key = d.date_key
ORDER BY d.full_date DESC;

-- ----------------------------------------
-- VALIDATION: Order Totals Check
-- This view validates that our calculations match source data
-- ----------------------------------------
DROP VIEW IF EXISTS marts.validation_order_totals CASCADE;
CREATE VIEW marts.validation_order_totals AS
SELECT
    fo.order_id,
    fo.order_number,
    fo.gross_product_sales,
    fo.order_discount_amount,
    fo.subtotal,
    fo.gross_product_sales - fo.order_discount_amount as calculated_subtotal,
    ABS(fo.subtotal - (fo.gross_product_sales - fo.order_discount_amount)) as subtotal_diff,
    CASE 
        WHEN ABS(fo.subtotal - (fo.gross_product_sales - fo.order_discount_amount)) <= 0.01 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END as subtotal_validation,
    
    fo.total_amount,
    fo.subtotal + fo.shipping_amount + fo.tax_amount as calculated_total,
    ABS(fo.total_amount - (fo.subtotal + fo.shipping_amount + fo.tax_amount)) as total_diff,
    CASE 
        WHEN ABS(fo.total_amount - (fo.subtotal + fo.shipping_amount + fo.tax_amount)) <= 0.01 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END as total_validation

FROM warehouse.fact_order fo;
