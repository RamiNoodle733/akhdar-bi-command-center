"""
Akhdar BI Command Center - Build Fact Tables
=============================================
Populate the star schema fact tables with proper grain handling.
"""
import logging
from sqlalchemy import text
from etl.config import get_engine

logger = logging.getLogger(__name__)


def build_facts():
    """Build all fact tables from staging data."""
    engine = get_engine()
    
    with engine.connect() as conn:
        # Build fact_order (1 row per order - order-level metrics only)
        logger.info("Building warehouse.fact_order...")
        conn.execute(text("""
            TRUNCATE TABLE warehouse.fact_order CASCADE;
            
            WITH order_line_summary AS (
                -- Calculate line-level aggregates per order
                SELECT 
                    order_id,
                    SUM(lineitem_quantity) as unit_count,
                    COUNT(*) as line_item_count,
                    SUM(lineitem_price * lineitem_quantity) as calculated_gross
                FROM staging.stg_order_lines
                GROUP BY order_id
            )
            INSERT INTO warehouse.fact_order (
                order_id, order_number, order_date_key, customer_key, channel_key,
                shipping_method_key, gross_product_sales, order_discount_amount,
                subtotal, shipping_amount, tax_amount, total_amount, refunded_amount,
                net_sales, line_item_count, unit_count, financial_status,
                fulfillment_status, risk_level, created_at, paid_at, fulfilled_at
            )
            SELECT 
                o.order_id,
                o.order_number,
                TO_CHAR(o.created_at, 'YYYYMMDD')::INTEGER as order_date_key,
                dc.customer_key,
                COALESCE(ch.channel_key, (SELECT channel_key FROM warehouse.dim_channel WHERE channel_code = 'web')) as channel_key,
                COALESCE(sm.shipping_method_key, (SELECT shipping_method_key FROM warehouse.dim_shipping_method LIMIT 1)) as shipping_method_key,
                
                -- Use calculated gross from line items (more accurate)
                COALESCE(ols.calculated_gross, o.subtotal + o.discount_amount) as gross_product_sales,
                o.discount_amount as order_discount_amount,
                o.subtotal,
                o.shipping as shipping_amount,
                o.taxes as tax_amount,
                o.total as total_amount,
                o.refunded_amount,
                o.subtotal - o.refunded_amount as net_sales,
                
                COALESCE(ols.line_item_count, 1) as line_item_count,
                COALESCE(ols.unit_count, 1) as unit_count,
                o.financial_status,
                o.fulfillment_status,
                o.risk_level,
                o.created_at,
                o.paid_at,
                o.fulfilled_at
                
            FROM staging.stg_orders o
            LEFT JOIN order_line_summary ols ON o.order_id = ols.order_id
            LEFT JOIN warehouse.dim_customer dc 
                ON encode(sha256(LOWER(o.email)::bytea), 'hex') = dc.customer_id_hash
            LEFT JOIN warehouse.dim_channel ch ON LOWER(o.source) = ch.channel_code
            LEFT JOIN warehouse.dim_shipping_method sm 
                ON LOWER(REPLACE(o.shipping_method, ' ', '_')) = sm.shipping_method_code;
        """))
        conn.commit()
        
        # Build fact_order_line with discount allocation
        logger.info("Building warehouse.fact_order_line...")
        conn.execute(text("""
            TRUNCATE TABLE warehouse.fact_order_line CASCADE;
            
            WITH line_with_allocation AS (
                SELECT 
                    ol.order_id,
                    ol.line_number,
                    ol.lineitem_name,
                    ol.lineitem_quantity as quantity,
                    ol.lineitem_price as unit_price,
                    ol.lineitem_price * ol.lineitem_quantity as gross_line_revenue,
                    ol.lineitem_discount as line_discount,
                    ol.created_at,
                    fo.order_key,
                    fo.order_date_key,
                    fo.order_discount_amount,
                    fo.gross_product_sales,
                    -- Proportional discount allocation
                    CASE 
                        WHEN fo.gross_product_sales > 0 
                        THEN (ol.lineitem_price * ol.lineitem_quantity / fo.gross_product_sales) 
                             * fo.order_discount_amount
                        ELSE 0 
                    END as allocated_order_discount
                FROM staging.stg_order_lines ol
                JOIN warehouse.fact_order fo ON ol.order_id = fo.order_id
            )
            INSERT INTO warehouse.fact_order_line (
                order_key, order_id, line_number, product_key, order_date_key,
                quantity, unit_price, gross_line_revenue, line_discount,
                allocated_order_discount, net_line_revenue,
                estimated_cogs, has_missing_cost, gross_margin, margin_percent
            )
            SELECT 
                lwa.order_key,
                lwa.order_id,
                lwa.line_number,
                dp.product_key,
                lwa.order_date_key,
                lwa.quantity,
                lwa.unit_price,
                lwa.gross_line_revenue,
                lwa.line_discount,
                lwa.allocated_order_discount,
                lwa.gross_line_revenue - lwa.line_discount - lwa.allocated_order_discount as net_line_revenue,
                
                -- COGS calculation will be added in next step
                NULL as estimated_cogs,
                NULL as has_missing_cost,
                NULL as gross_margin,
                NULL as margin_percent
                
            FROM line_with_allocation lwa
            LEFT JOIN staging.stg_product_sku_map skm 
                ON lwa.lineitem_name = skm.lineitem_name
            LEFT JOIN warehouse.dim_product dp 
                ON skm.internal_sku = dp.internal_sku;
        """))
        conn.commit()
        
        # Build fact_cogs_estimate and update fact_order_line
        logger.info("Building warehouse.fact_cogs_estimate and updating COGS...")
        
        # First, populate fact_cogs_estimate
        conn.execute(text("""
            TRUNCATE TABLE warehouse.fact_cogs_estimate CASCADE;
            
            INSERT INTO warehouse.fact_cogs_estimate (
                order_line_key, product_key, material_key,
                ingredient_name, amount_ml, cost_per_ml, line_cost, has_known_cost
            )
            SELECT 
                fol.order_line_key,
                fol.product_key,
                dm.material_key,
                r.ingredient_match,
                r.amount_ml,
                COALESCE(dm.cost_per_ml, dm.cost_per_unit) as cost_per_ml,
                CASE 
                    WHEN dm.has_known_cost AND dm.cost_per_ml IS NOT NULL 
                    THEN r.amount_ml * dm.cost_per_ml
                    WHEN dm.has_known_cost AND dm.cost_per_unit IS NOT NULL
                    THEN dm.cost_per_unit  -- For packaging (1 unit per bottle)
                    ELSE 0
                END as line_cost,
                COALESCE(dm.has_known_cost, false)
            FROM warehouse.fact_order_line fol
            JOIN warehouse.dim_product dp ON fol.product_key = dp.product_key
            JOIN staging.stg_recipes r 
                ON dp.recipe_id = r.recipe_id 
                AND r.batch_size_ml = dp.size_ml
                AND r.variant = 'final'
            LEFT JOIN warehouse.dim_material dm ON r.material_id = dm.material_id
            WHERE fol.product_key IS NOT NULL;
        """))
        conn.commit()
        
        # Now update fact_order_line with calculated COGS
        conn.execute(text("""
            WITH cogs_summary AS (
                SELECT 
                    order_line_key,
                    SUM(line_cost) as total_cogs,
                    BOOL_OR(NOT has_known_cost) as has_missing_cost
                FROM warehouse.fact_cogs_estimate
                GROUP BY order_line_key
            )
            UPDATE warehouse.fact_order_line fol
            SET 
                estimated_cogs = COALESCE(cs.total_cogs, 0),
                has_missing_cost = COALESCE(cs.has_missing_cost, true),
                gross_margin = fol.net_line_revenue / NULLIF(fol.quantity, 0) - COALESCE(cs.total_cogs, 0),
                margin_percent = CASE 
                    WHEN fol.net_line_revenue > 0 
                    THEN ((fol.net_line_revenue / NULLIF(fol.quantity, 0) - COALESCE(cs.total_cogs, 0)) 
                          / (fol.net_line_revenue / NULLIF(fol.quantity, 0))) * 100
                    ELSE 0 
                END
            FROM cogs_summary cs
            WHERE fol.order_line_key = cs.order_line_key;
            
            -- Set defaults for lines without COGS data
            UPDATE warehouse.fact_order_line
            SET 
                estimated_cogs = 0,
                has_missing_cost = true,
                gross_margin = net_line_revenue / NULLIF(quantity, 0),
                margin_percent = 100
            WHERE estimated_cogs IS NULL;
        """))
        conn.commit()
        
        # Build fact_marketing_spend (optional)
        logger.info("Building warehouse.fact_marketing_spend (if available)...")
        conn.execute(text("""
            TRUNCATE TABLE warehouse.fact_marketing_spend CASCADE;
            
            INSERT INTO warehouse.fact_marketing_spend (
                campaign_name, platform, reach, impressions, amount_spent,
                link_clicks, landing_page_views, cpm, cpc, ctr
            )
            SELECT 
                campaign_name,
                'meta' as platform,
                reach,
                impressions,
                amount_spent,
                link_clicks,
                landing_page_views,
                cpm,
                cpc,
                ctr
            FROM staging.stg_meta_ads
            WHERE campaign_name IS NOT NULL;
        """))
        conn.commit()
        
        # Build fact_gsc_daily (optional)
        logger.info("Building warehouse.fact_gsc_daily (if available)...")
        conn.execute(text("""
            TRUNCATE TABLE warehouse.fact_gsc_daily CASCADE;
            
            INSERT INTO warehouse.fact_gsc_daily (
                date_key, clicks, impressions, ctr, avg_position
            )
            SELECT 
                TO_CHAR(date, 'YYYYMMDD')::INTEGER as date_key,
                clicks,
                impressions,
                ctr,
                position as avg_position
            FROM staging.stg_gsc_daily
            WHERE date IS NOT NULL;
        """))
        conn.commit()
        
        logger.info("Fact tables built successfully!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    build_facts()
