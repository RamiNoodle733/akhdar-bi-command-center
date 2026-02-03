"""
Akhdar BI Command Center - Build Staging Tables
================================================
Transform raw data into cleaned, typed staging tables.
"""
import logging
import hashlib
from sqlalchemy import text
from etl.config import get_engine

logger = logging.getLogger(__name__)


def hash_email(email: str) -> str:
    """Create SHA256 hash of email for PII-safe customer ID."""
    if not email:
        return hashlib.sha256('unknown'.encode()).hexdigest()
    return hashlib.sha256(email.lower().strip().encode()).hexdigest()


def build_staging_tables():
    """Build all staging tables from raw data."""
    engine = get_engine()
    
    with engine.connect() as conn:
        # Build staging orders (dedupe to 1 row per order)
        logger.info("Building staging.stg_orders...")
        conn.execute(text("""
            TRUNCATE TABLE staging.stg_orders CASCADE;
            
            INSERT INTO staging.stg_orders (
                order_id, order_number, email, financial_status, fulfillment_status,
                currency, subtotal, shipping, taxes, total, discount_code, discount_amount,
                refunded_amount, shipping_method, risk_level, source, payment_method,
                billing_city, billing_province, billing_country, billing_zip,
                shipping_city, shipping_province, shipping_country, shipping_zip,
                created_at, paid_at, fulfilled_at, cancelled_at
            )
            SELECT DISTINCT ON (id)
                NULLIF(id, '')::BIGINT as order_id,
                name as order_number,
                email,
                financial_status,
                fulfillment_status,
                currency,
                NULLIF(subtotal, '')::NUMERIC as subtotal,
                NULLIF(shipping, '')::NUMERIC as shipping,
                NULLIF(taxes, '')::NUMERIC as taxes,
                NULLIF(total, '')::NUMERIC as total,
                NULLIF(discount_code, '') as discount_code,
                COALESCE(NULLIF(discount_amount, '')::NUMERIC, 0) as discount_amount,
                COALESCE(NULLIF(refunded_amount, '')::NUMERIC, 0) as refunded_amount,
                shipping_method,
                risk_level,
                source,
                payment_method,
                billing_city,
                billing_province,
                billing_country,
                billing_zip,
                shipping_city,
                shipping_province,
                shipping_country,
                shipping_zip,
                NULLIF(created_at, '')::TIMESTAMP WITH TIME ZONE as created_at,
                NULLIF(paid_at, '')::TIMESTAMP WITH TIME ZONE as paid_at,
                NULLIF(fulfilled_at, '')::TIMESTAMP WITH TIME ZONE as fulfilled_at,
                NULLIF(cancelled_at, '')::TIMESTAMP WITH TIME ZONE as cancelled_at
            FROM raw.orders
            WHERE id IS NOT NULL AND id != ''
            ORDER BY id, created_at;
        """))
        conn.commit()
        
        # Build staging order lines (1 row per line item)
        logger.info("Building staging.stg_order_lines...")
        conn.execute(text("""
            TRUNCATE TABLE staging.stg_order_lines CASCADE;
            
            INSERT INTO staging.stg_order_lines (
                order_id, order_number, line_number, lineitem_name, lineitem_sku,
                lineitem_quantity, lineitem_price, lineitem_compare_at_price,
                lineitem_discount, lineitem_fulfillment_status, vendor, created_at
            )
            SELECT 
                NULLIF(id, '')::BIGINT as order_id,
                name as order_number,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY lineitem_name) as line_number,
                lineitem_name,
                NULLIF(lineitem_sku, '') as lineitem_sku,
                COALESCE(NULLIF(lineitem_quantity, '')::INTEGER, 1) as lineitem_quantity,
                NULLIF(lineitem_price, '')::NUMERIC as lineitem_price,
                NULLIF(lineitem_compare_at_price, '')::NUMERIC as lineitem_compare_at_price,
                COALESCE(NULLIF(lineitem_discount, '')::NUMERIC, 0) as lineitem_discount,
                lineitem_fulfillment_status,
                vendor,
                NULLIF(created_at, '')::TIMESTAMP WITH TIME ZONE as created_at
            FROM raw.orders
            WHERE id IS NOT NULL AND id != ''
              AND lineitem_name IS NOT NULL AND lineitem_name != '';
        """))
        conn.commit()
        
        # Build staging products
        logger.info("Building staging.stg_products...")
        conn.execute(text("""
            TRUNCATE TABLE staging.stg_products CASCADE;
            
            INSERT INTO staging.stg_products (
                handle, title, vendor, product_category, product_type, tags,
                variant_sku, variant_price, variant_compare_at_price,
                variant_inventory_qty, is_published, status
            )
            SELECT DISTINCT ON (handle)
                handle,
                title,
                vendor,
                product_category,
                type as product_type,
                tags,
                NULLIF(variant_sku, '') as variant_sku,
                NULLIF(variant_price, '')::NUMERIC as variant_price,
                NULLIF(variant_compare_at_price, '')::NUMERIC as variant_compare_at_price,
                COALESCE(NULLIF(variant_inventory_qty, '')::INTEGER, 0) as variant_inventory_qty,
                UPPER(COALESCE(published, 'FALSE')) = 'TRUE' as is_published,
                status
            FROM raw.products
            WHERE handle IS NOT NULL AND handle != ''
            ORDER BY handle, title;
        """))
        conn.commit()
        
        # Build staging customers
        logger.info("Building staging.stg_customers...")
        conn.execute(text("""
            TRUNCATE TABLE staging.stg_customers CASCADE;
            
            INSERT INTO staging.stg_customers (
                customer_id, first_name, last_name, email,
                accepts_email_marketing, accepts_sms_marketing,
                city, province, province_code, country, country_code, zip,
                total_spent, total_orders
            )
            SELECT 
                NULLIF(customer_id, '')::BIGINT as customer_id,
                first_name,
                last_name,
                email,
                UPPER(COALESCE(accepts_email_marketing, 'no')) = 'YES' as accepts_email_marketing,
                UPPER(COALESCE(accepts_sms_marketing, 'no')) = 'YES' as accepts_sms_marketing,
                default_address_city as city,
                default_address_province_code as province,
                default_address_province_code as province_code,
                default_address_country_code as country,
                default_address_country_code as country_code,
                default_address_zip as zip,
                COALESCE(NULLIF(total_spent, '')::NUMERIC, 0) as total_spent,
                COALESCE(NULLIF(total_orders, '')::INTEGER, 0) as total_orders
            FROM raw.customers
            WHERE customer_id IS NOT NULL AND customer_id != '';
        """))
        conn.commit()
        
        # Build staging product SKU map
        logger.info("Building staging.stg_product_sku_map...")
        conn.execute(text("""
            TRUNCATE TABLE staging.stg_product_sku_map CASCADE;
            
            INSERT INTO staging.stg_product_sku_map (
                internal_sku, lineitem_name, product_handle, size_ml, 
                recipe_id, product_category, is_active
            )
            SELECT 
                internal_sku,
                lineitem_name,
                product_handle,
                NULLIF(size_ml, '')::INTEGER as size_ml,
                recipe_id,
                product_category,
                UPPER(COALESCE(is_active, 'true')) = 'TRUE' as is_active
            FROM raw.product_sku_map
            WHERE internal_sku IS NOT NULL AND internal_sku != '';
        """))
        conn.commit()
        
        # Build staging material costs
        logger.info("Building staging.stg_material_costs...")
        conn.execute(text("""
            TRUNCATE TABLE staging.stg_material_costs CASCADE;
            
            INSERT INTO staging.stg_material_costs (
                material_id, material_name, ingredient_match, category, unit,
                cost_per_unit, cost_per_ml, has_known_cost, supplier
            )
            SELECT 
                material_id,
                material_name,
                ingredient_match,
                category,
                unit,
                NULLIF(cost_per_unit, '')::NUMERIC as cost_per_unit,
                NULLIF(cost_per_ml, '')::NUMERIC as cost_per_ml,
                UPPER(COALESCE(has_known_cost, 'false')) = 'TRUE' as has_known_cost,
                supplier
            FROM raw.material_costs
            WHERE material_id IS NOT NULL AND material_id != '';
        """))
        conn.commit()
        
        # Build staging recipes
        logger.info("Building staging.stg_recipes...")
        conn.execute(text("""
            TRUNCATE TABLE staging.stg_recipes CASCADE;
            
            INSERT INTO staging.stg_recipes (
                recipe_id, recipe_name, variant, batch_size_ml,
                ingredient_match, percent, amount_ml, material_id
            )
            SELECT 
                recipe_id,
                recipe_name,
                variant,
                NULLIF(batch_size_ml, '')::INTEGER as batch_size_ml,
                ingredient_match,
                NULLIF(percent, '')::NUMERIC as percent,
                NULLIF(amount_ml, '')::NUMERIC as amount_ml,
                material_id
            FROM raw.recipes
            WHERE recipe_id IS NOT NULL AND recipe_id != '';
        """))
        conn.commit()
        
        # Build staging meta ads (optional - handle missing columns gracefully)
        logger.info("Building staging.stg_meta_ads (if available)...")
        try:
            # Check what columns exist in raw.meta_ads
            result = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'meta_ads'"))
            columns = {row[0] for row in result.fetchall()}
            
            if 'campaign_name' in columns:
                conn.execute(text("""
                    TRUNCATE TABLE staging.stg_meta_ads CASCADE;
                    
                    INSERT INTO staging.stg_meta_ads (
                        campaign_name, reach, impressions, amount_spent, link_clicks, landing_page_views
                    )
                    SELECT 
                        campaign_name,
                        NULLIF(reach, '')::INTEGER as reach,
                        NULLIF(impressions, '')::INTEGER as impressions,
                        COALESCE(
                            NULLIF(REPLACE(REPLACE(amount_spent, '$', ''), ',', ''), '')::NUMERIC,
                            NULLIF(REPLACE(REPLACE(amount_spent_usd, '$', ''), ',', ''), '')::NUMERIC,
                            0
                        ) as amount_spent,
                        NULLIF(link_clicks, '')::INTEGER as link_clicks,
                        NULLIF(landing_page_views, '')::INTEGER as landing_page_views
                    FROM raw.meta_ads
                    WHERE campaign_name IS NOT NULL AND campaign_name != '';
                """))
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.warning(f"Could not build stg_meta_ads: {e}")
        
        # Build staging GSC (optional)
        logger.info("Building staging.stg_gsc_daily (if available)...")
        try:
            conn.execute(text("""
                TRUNCATE TABLE staging.stg_gsc_daily CASCADE;
                
                INSERT INTO staging.stg_gsc_daily (
                    date, clicks, impressions, ctr, position
                )
                SELECT 
                    TO_DATE(date, 'YYYY-MM-DD') as date,
                    NULLIF(clicks, '')::INTEGER as clicks,
                    NULLIF(impressions, '')::INTEGER as impressions,
                    NULLIF(REPLACE(ctr, '%', ''), '')::NUMERIC / 100 as ctr,
                    NULLIF(position, '')::NUMERIC as position
                FROM raw.gsc_daily
                WHERE date IS NOT NULL AND date != '';
            """))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.warning(f"Could not build stg_gsc_daily: {e}")
        
        logger.info("Staging tables built successfully!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    build_staging_tables()
