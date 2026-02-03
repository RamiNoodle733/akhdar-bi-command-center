"""
Akhdar BI Command Center - Main ETL Runner
==========================================
Single entry point to run the full ETL pipeline.

Usage:
    python -m etl.run_all              # Run full pipeline
    python -m etl.run_all --step load  # Load raw data only
    python -m etl.run_all --step build # Build staging + warehouse only
    python -m etl.run_all --sample     # Use sample data (for demo)
"""
import os
import sys
import logging
import argparse
from sqlalchemy import text
from etl.config import get_engine, SQL_SCHEMA_DIR, SQL_MARTS_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def init_database():
    """Initialize database schemas if they don't exist."""
    engine = get_engine()
    
    # Run schema SQL files in order
    schema_files = sorted([f for f in os.listdir(SQL_SCHEMA_DIR) if f.endswith('.sql')])
    
    with engine.connect() as conn:
        for schema_file in schema_files:
            filepath = os.path.join(SQL_SCHEMA_DIR, schema_file)
            logger.info(f"Executing {schema_file}...")
            
            with open(filepath, 'r') as f:
                sql = f.read()
                
            # Split on semicolons and execute each statement
            statements = [s.strip() for s in sql.split(';') if s.strip()]
            for stmt in statements:
                # Skip comments and empty statements
                lines = [l for l in stmt.split('\n') if l.strip() and not l.strip().startswith('--')]
                if not lines:
                    continue
                    
                try:
                    conn.execute(text(stmt))
                    conn.commit()  # Commit after each successful statement
                except Exception as e:
                    # Rollback failed transaction and continue
                    conn.rollback()
                    # Ignore errors for things like "table already exists"
                    if 'already exists' not in str(e).lower():
                        logger.warning(f"Statement warning: {e}")
    
    logger.info("Database schemas initialized!")


def build_marts():
    """Build KPI mart views."""
    engine = get_engine()
    
    mart_files = sorted([f for f in os.listdir(SQL_MARTS_DIR) if f.endswith('.sql')])
    
    with engine.connect() as conn:
        for mart_file in mart_files:
            filepath = os.path.join(SQL_MARTS_DIR, mart_file)
            logger.info(f"Executing {mart_file}...")
            
            with open(filepath, 'r') as f:
                sql = f.read()
                
            statements = [s.strip() for s in sql.split(';') if s.strip()]
            for stmt in statements:
                # Skip comments and empty statements
                lines = [l for l in stmt.split('\n') if l.strip() and not l.strip().startswith('--')]
                if not lines:
                    continue
                    
                try:
                    conn.execute(text(stmt))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.warning(f"Mart statement warning: {e}")
    
    logger.info("KPI mart views built!")


def run_pipeline(step: str = 'all', use_sample: bool = False):
    """
    Run the ETL pipeline.
    
    Args:
        step: Which step to run ('load', 'build', 'all')
        use_sample: Whether to use sample data
    """
    logger.info("=" * 60)
    logger.info("Akhdar BI Command Center - ETL Pipeline")
    logger.info("=" * 60)
    
    if step in ('all', 'load'):
        # Initialize database
        logger.info("\n[1/5] Initializing database schemas...")
        init_database()
        
        # Load raw data
        logger.info("\n[2/5] Loading raw data...")
        from etl.ingest_raw import load_raw_data
        load_raw_data(use_sample=use_sample)
    
    if step in ('all', 'build'):
        # Build staging tables
        logger.info("\n[3/5] Building staging tables...")
        from etl.build_staging import build_staging_tables
        build_staging_tables()
        
        # Build dimension tables
        logger.info("\n[4/5] Building dimension tables...")
        from etl.build_dimensions import build_dimensions
        build_dimensions()
        
        # Build fact tables
        logger.info("\n[5/5] Building fact tables...")
        from etl.build_facts import build_facts
        build_facts()
        
        # Build KPI marts
        logger.info("\n[BONUS] Building KPI mart views...")
        build_marts()
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ ETL Pipeline Complete!")
    logger.info("=" * 60)
    
    # Print summary
    print_summary()


def print_summary():
    """Print a summary of the loaded data."""
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            # Get counts
            orders = conn.execute(text("SELECT COUNT(*) FROM warehouse.fact_order")).scalar()
            lines = conn.execute(text("SELECT COUNT(*) FROM warehouse.fact_order_line")).scalar()
            customers = conn.execute(text("SELECT COUNT(*) FROM warehouse.dim_customer")).scalar()
            products = conn.execute(text("SELECT COUNT(*) FROM warehouse.dim_product")).scalar()
            
            # Get totals
            result = conn.execute(text("""
                SELECT 
                    COALESCE(SUM(net_sales), 0) as total_revenue,
                    COALESCE(SUM(unit_count), 0) as total_units
                FROM warehouse.fact_order
            """)).fetchone()
            
            print("\n📊 Data Summary:")
            print(f"   Orders:    {orders}")
            print(f"   Line Items: {lines}")
            print(f"   Customers: {customers}")
            print(f"   Products:  {products}")
            print(f"   Revenue:   ${result[0]:,.2f}")
            print(f"   Units:     {result[1]}")
            
    except Exception as e:
        logger.warning(f"Could not print summary: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Akhdar BI Command Center ETL Pipeline')
    parser.add_argument('--step', choices=['load', 'build', 'all'], default='all',
                        help='Which step to run (default: all)')
    parser.add_argument('--sample', action='store_true',
                        help='Use sample data instead of real data')
    
    args = parser.parse_args()
    
    try:
        run_pipeline(step=args.step, use_sample=args.sample)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
