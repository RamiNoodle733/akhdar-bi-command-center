"""
Akhdar BI Command Center - Raw Data Ingestion
==============================================
Load CSV files into raw PostgreSQL tables.
Tables are created dynamically from CSV structure.
"""
import os
import pandas as pd
import logging
from sqlalchemy import text
from etl.config import (
    get_engine, 
    DATA_RAW_DIR, 
    DATA_REFERENCE_DIR, 
    DATA_SAMPLE_DIR,
    RAW_FILE_MAPPINGS, 
    REFERENCE_FILE_MAPPINGS,
    PRIVATE_REFERENCE_MAPPINGS,
    SAMPLE_FILE_MAPPINGS,
    OPTIONAL_FILE_MAPPINGS,
    GSC_CHART_FILE,
    PROJECT_ROOT,
)

logger = logging.getLogger(__name__)


def clean_column_name(col: str) -> str:
    """Convert column name to snake_case for Postgres."""
    return (
        col.lower()
        .replace(' ', '_')
        .replace('(', '')
        .replace(')', '')
        .replace('/', '_')
        .replace('-', '_')
        .replace('.', '_')
        .replace(':', '_')
    )


def strip_apostrophe_prefix(value):
    """Remove leading apostrophe from values (e.g., '77083 -> 77083)."""
    if isinstance(value, str) and value.startswith("'"):
        return value[1:]
    return value


def load_csv_to_table(filepath: str, table_name: str, engine) -> bool:
    """
    Load a CSV file into a raw PostgreSQL table.
    Uses if_exists='replace' to dynamically create tables from CSV structure.
    
    Args:
        filepath: Path to CSV file
        table_name: Target table (schema.table format)
        engine: SQLAlchemy engine
        
    Returns:
        True if successful, False otherwise
    """
    if not os.path.exists(filepath):
        logger.warning(f"File not found: {filepath}")
        return False
    
    try:
        # Read CSV - all as strings to preserve data
        df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
        
        if df.empty:
            logger.warning(f"Empty file: {filepath}")
            return False
        
        # Clean column names for Postgres compatibility
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Strip apostrophe prefixes from all values
        for col in df.columns:
            df[col] = df[col].apply(strip_apostrophe_prefix)
        
        # Add loaded_at timestamp
        df['loaded_at'] = pd.Timestamp.now()
        
        # Parse schema and table
        schema, table = table_name.split('.')
        
        # Use replace to dynamically create/recreate table
        with engine.connect() as conn:
            df.to_sql(
                table,
                conn,
                schema=schema,
                if_exists='replace',  # Creates table if not exists
                index=False,
                method='multi',
                chunksize=1000
            )
            conn.commit()
        
        logger.info(f"Loaded {len(df)} rows into {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error loading {filepath} to {table_name}: {e}")
        return False


def load_raw_data(use_sample: bool = False):
    """
    Load all raw data files into PostgreSQL.
    
    Args:
        use_sample: If True, use sample data instead of real data
    """
    engine = get_engine()
    
    # Determine which data directory and mappings to use
    if use_sample:
        data_dir = DATA_SAMPLE_DIR
        file_mappings = SAMPLE_FILE_MAPPINGS
        logger.info("Loading SAMPLE data (for demo/testing)")
    else:
        data_dir = DATA_RAW_DIR
        file_mappings = RAW_FILE_MAPPINGS
        logger.info("Loading RAW data from Shopify exports")
    
    # Load main data files
    for filename, table_name in file_mappings.items():
        filepath = os.path.join(data_dir, filename)
        load_csv_to_table(filepath, table_name, engine)
    
    # Always load reference files from reference directory
    logger.info("Loading reference data (SKU map)")
    for filename, table_name in REFERENCE_FILE_MAPPINGS.items():
        filepath = os.path.join(DATA_REFERENCE_DIR, filename)
        load_csv_to_table(filepath, table_name, engine)
    
    # Load private reference files (costs, recipes) from raw directory
    logger.info("Loading private reference data (materials, recipes)")
    for filename, table_name in PRIVATE_REFERENCE_MAPPINGS.items():
        filepath = os.path.join(DATA_RAW_DIR, filename)
        load_csv_to_table(filepath, table_name, engine)
    
    # Try to load optional files (Meta ads, GSC) - don't fail if missing
    logger.info("Attempting to load optional data sources...")
    for filename, table_name in OPTIONAL_FILE_MAPPINGS.items():
        filepath = os.path.join(DATA_RAW_DIR, filename)
        if os.path.exists(filepath):
            load_meta_ads(filepath, engine)
        else:
            logger.info(f"Optional file not found (skipping): {filename}")
    
    # Try GSC data
    gsc_path = os.path.join(DATA_RAW_DIR, GSC_CHART_FILE)
    if os.path.exists(gsc_path):
        load_gsc_data(gsc_path, engine)
    else:
        # Check if data is in project root (user may not have moved files)
        gsc_path_root = os.path.join(PROJECT_ROOT, GSC_CHART_FILE)
        if os.path.exists(gsc_path_root):
            load_gsc_data(gsc_path_root, engine)
        else:
            logger.info("GSC data not found (skipping)")


def load_meta_ads(filepath: str, engine):
    """Load Meta Ads data with special handling for format."""
    try:
        df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
        
        if df.empty:
            logger.warning("Meta ads file is empty")
            return
        
        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Map columns to our schema
        column_mapping = {
            'campaign_name': 'campaign_name',
            'reach': 'reach',
            'frequency': 'frequency',
            'impressions': 'impressions',
            'cpm_cost_per_1_000_impressions': 'cpm',
            'amount_spent_usd': 'amount_spent',
            'link_clicks': 'link_clicks',
            'cpc_cost_per_link_click': 'cpc',
            'ctr_link_click_through_rate': 'ctr',
            'landing_page_views': 'landing_page_views',
        }
        
        # Rename columns that exist
        rename_map = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_map)
        
        # Keep only columns we need
        keep_cols = [c for c in column_mapping.values() if c in df.columns]
        df = df[keep_cols]
        
        # Add loaded_at
        df['loaded_at'] = pd.Timestamp.now()
        
        with engine.connect() as conn:
            df.to_sql('meta_ads', conn, schema='raw', if_exists='replace', index=False)
            conn.commit()
        
        logger.info(f"Loaded {len(df)} rows into raw.meta_ads")
        
    except Exception as e:
        logger.warning(f"Could not load Meta ads data: {e}")


def load_gsc_data(filepath: str, engine):
    """Load Google Search Console Chart.csv data."""
    try:
        df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
        
        if df.empty:
            logger.warning("GSC file is empty")
            return
        
        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Add loaded_at
        df['loaded_at'] = pd.Timestamp.now()
        
        with engine.connect() as conn:
            df.to_sql('gsc_daily', conn, schema='raw', if_exists='replace', index=False)
            conn.commit()
        
        logger.info(f"Loaded {len(df)} rows into raw.gsc_daily")
        
    except Exception as e:
        logger.warning(f"Could not load GSC data: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_raw_data(use_sample=False)
