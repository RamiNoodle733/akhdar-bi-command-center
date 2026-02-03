"""
Akhdar BI Command Center - Database Configuration
=================================================
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

# Database connection settings
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'akhdar_bi'),
    'user': os.getenv('POSTGRES_USER', 'akhdar'),
    'password': os.getenv('POSTGRES_PASSWORD', 'akhdar_dev_2025'),
}

def get_connection_string():
    """Get PostgreSQL connection string."""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

def get_engine():
    """Create SQLAlchemy engine."""
    return create_engine(get_connection_string(), poolclass=NullPool)

# File paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR = os.path.join(PROJECT_ROOT, 'data', 'raw')
DATA_REFERENCE_DIR = os.path.join(PROJECT_ROOT, 'data', 'reference')
DATA_SAMPLE_DIR = os.path.join(PROJECT_ROOT, 'data', 'sample')
SQL_SCHEMA_DIR = os.path.join(PROJECT_ROOT, 'sql', 'schema')
SQL_MARTS_DIR = os.path.join(PROJECT_ROOT, 'sql', 'marts')

# Data file mappings (raw file -> table name)
RAW_FILE_MAPPINGS = {
    'orders_export_1.csv': 'raw.orders',
    'products_export_1.csv': 'raw.products',
    'customers_export.csv': 'raw.customers',
    'discounts_export_1.csv': 'raw.discounts',
}

# Reference file mappings (SKU map is public, costs/recipes are private)
REFERENCE_FILE_MAPPINGS = {
    'product_sku_map.csv': 'raw.product_sku_map',
}

# Private reference files (loaded from raw folder - gitignored)
PRIVATE_REFERENCE_MAPPINGS = {
    'material_costs.csv': 'raw.material_costs',
    'recipes.csv': 'raw.recipes',
}

# Sample file mappings (for demo/testing)
SAMPLE_FILE_MAPPINGS = {
    'sample_orders.csv': 'raw.orders',
    'sample_products.csv': 'raw.products',
    'sample_customers.csv': 'raw.customers',
    'sample_discounts.csv': 'raw.discounts',
}

# Optional data sources
OPTIONAL_FILE_MAPPINGS = {
    'رامي-فايز-عبد-الرزاق-Campaigns-Sep-1-2025-Dec-1-2025.csv': 'raw.meta_ads',
}

# GSC files (special handling)
GSC_CHART_FILE = 'akhdarperfumes.com-Performance-on-Search-2026-02-03/Chart.csv'
