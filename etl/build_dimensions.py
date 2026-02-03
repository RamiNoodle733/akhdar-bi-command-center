"""
Akhdar BI Command Center - Build Dimension Tables
==================================================
Populate the star schema dimension tables.
"""
import logging
import hashlib
from sqlalchemy import text
from etl.config import get_engine

logger = logging.getLogger(__name__)


def build_dimensions():
    """Build all dimension tables from staging data."""
    engine = get_engine()
    
    with engine.connect() as conn:
        # dim_date is already populated in schema creation
        logger.info("dim_date already populated from schema init")
        
        # Build dim_product
        logger.info("Building warehouse.dim_product...")
        conn.execute(text("""
            TRUNCATE TABLE warehouse.dim_product CASCADE;
            
            INSERT INTO warehouse.dim_product (
                internal_sku, product_handle, product_title, size_ml,
                recipe_id, product_category, vendor, variant_price, is_active
            )
            SELECT 
                skm.internal_sku,
                skm.product_handle,
                skm.lineitem_name as product_title,
                skm.size_ml,
                skm.recipe_id,
                skm.product_category,
                COALESCE(p.vendor, 'Akhdar Perfumes') as vendor,
                COALESCE(p.variant_price, 10.50) as variant_price,
                skm.is_active
            FROM staging.stg_product_sku_map skm
            LEFT JOIN staging.stg_products p ON skm.product_handle = p.handle;
        """))
        conn.commit()
        
        # Build dim_customer with hashed emails
        logger.info("Building warehouse.dim_customer...")
        conn.execute(text("""
            TRUNCATE TABLE warehouse.dim_customer CASCADE;
            
            -- First, get unique customers from orders (more complete than customer export)
            WITH order_customers AS (
                SELECT DISTINCT
                    email,
                    MIN(created_at) as first_order_date
                FROM staging.stg_orders
                WHERE email IS NOT NULL AND email != ''
                GROUP BY email
            ),
            customer_stats AS (
                SELECT
                    email,
                    COUNT(DISTINCT order_id) as total_orders,
                    SUM(net_sales) as total_spent
                FROM (
                    SELECT order_id, email, subtotal - refunded_amount as net_sales
                    FROM staging.stg_orders
                ) o
                GROUP BY email
            )
            INSERT INTO warehouse.dim_customer (
                customer_id_hash, customer_id, city, province, province_code,
                country, country_code, accepts_email_marketing, accepts_sms_marketing,
                first_order_date, total_orders, total_spent, customer_segment
            )
            SELECT 
                encode(sha256(LOWER(oc.email)::bytea), 'hex') as customer_id_hash,
                c.customer_id,
                c.city,
                c.province,
                c.province_code,
                c.country,
                c.country_code,
                COALESCE(c.accepts_email_marketing, false),
                COALESCE(c.accepts_sms_marketing, false),
                DATE(oc.first_order_date) as first_order_date,
                COALESCE(cs.total_orders, 0) as total_orders,
                COALESCE(cs.total_spent, 0) as total_spent,
                CASE 
                    WHEN COALESCE(cs.total_orders, 0) = 0 THEN 'prospect'
                    WHEN COALESCE(cs.total_orders, 0) = 1 THEN 'new'
                    WHEN COALESCE(cs.total_orders, 0) >= 2 THEN 'returning'
                    ELSE 'unknown'
                END as customer_segment
            FROM order_customers oc
            LEFT JOIN staging.stg_customers c ON LOWER(oc.email) = LOWER(c.email)
            LEFT JOIN customer_stats cs ON LOWER(oc.email) = LOWER(cs.email);
        """))
        conn.commit()
        
        # Build dim_shipping_method
        logger.info("Building warehouse.dim_shipping_method...")
        conn.execute(text("""
            TRUNCATE TABLE warehouse.dim_shipping_method CASCADE;
            
            INSERT INTO warehouse.dim_shipping_method (
                shipping_method_code, shipping_method_name, is_local_delivery
            )
            SELECT DISTINCT
                LOWER(REPLACE(shipping_method, ' ', '_')) as shipping_method_code,
                shipping_method as shipping_method_name,
                LOWER(shipping_method) LIKE '%local%' as is_local_delivery
            FROM staging.stg_orders
            WHERE shipping_method IS NOT NULL AND shipping_method != ''
            
            UNION
            
            SELECT 'unknown', 'Unknown', false
            WHERE NOT EXISTS (
                SELECT 1 FROM staging.stg_orders 
                WHERE shipping_method IS NOT NULL AND shipping_method != ''
            );
        """))
        conn.commit()
        
        # Build dim_material
        logger.info("Building warehouse.dim_material...")
        conn.execute(text("""
            TRUNCATE TABLE warehouse.dim_material CASCADE;
            
            INSERT INTO warehouse.dim_material (
                material_id, material_name, ingredient_match, category, unit,
                cost_per_unit, cost_per_ml, has_known_cost, supplier
            )
            SELECT 
                material_id, material_name, ingredient_match, category, unit,
                cost_per_unit, cost_per_ml, has_known_cost, supplier
            FROM staging.stg_material_costs;
        """))
        conn.commit()
        
        logger.info("Dimension tables built successfully!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    build_dimensions()
