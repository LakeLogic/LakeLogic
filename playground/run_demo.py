import polars as pl
from lakeguard import DataProcessor
from loguru import logger
import sys
from pathlib import Path

# Configure logging to be pretty
logger.remove()
logger.add(sys.stderr, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")

def smoke_test():
    # 1. Load Data
    base_dir = Path(__file__).resolve().parent
    source_file = base_dir / "customers.csv"
    contract_file = base_dir / "contract.yaml"

    if not source_file.exists():
        logger.error(f"Test data not found: {source_file}")
        return

    df = pl.read_csv(source_file)
    logger.info(f"Loaded {len(df)} customer records")
    
    # 2. Initialize Processor with the new Customer Contract
    try:
        processor = DataProcessor(engine="polars", contract=contract_file)
        
        # 3. Run Validation & Transformation
        good_df, bad_df = processor.run(df)
        
        # 4. Results Inspection
        logger.info("--- TEST RESULTS ---")
        logger.info(f"Good Records: {len(good_df)}")
        logger.info(f"Quarantined Records: {len(bad_df)}")
        
        if len(good_df) > 0:
            logger.info("Sample Good Record (with enrichment/transformation):")
            # Show top 2
            print(good_df.head(2))
            
        if len(bad_df) > 0:
            logger.warning("Sample Quarantined Record (with error details):")
            # Show top 2
            print(bad_df.head(2))
            
        # 5. Save for manual inspection
        good_df.write_csv(base_dir / "good_customers.csv")
        bad_df.write_csv(base_dir / "bad_customers.csv")
        logger.success("Results saved to good_customers.csv and bad_customers.csv")

    except Exception as e:
        logger.exception(f"Smoke test failed: {e}")

if __name__ == "__main__":
    smoke_test()
