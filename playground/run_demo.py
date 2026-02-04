import polars as pl
from lakeguard import DataProcessor
import os
from loguru import logger
import sys

# Configure logging to be pretty
logger.remove()
logger.add(sys.stderr, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")

def smoke_test():
    # 1. Load Data
    source_file = "test_customers.csv"
    if not os.path.exists(source_file):
        logger.error(f"Test data not found: {source_file}")
        return

    df = pl.read_csv(source_file)
    logger.info(f"Loaded {len(df)} customer records")
    
    # 2. Initialize Processor with the new Customer Contract
    try:
        processor = DataProcessor(engine="polars", contract="test_contract.yaml")
        
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
        good_df.write_csv("good_customers.csv")
        bad_df.write_csv("bad_customers.csv")
        logger.success("Results saved to good_customers.csv and bad_customers.csv")

    except Exception as e:
        logger.exception(f"Smoke test failed: {e}")

if __name__ == "__main__":
    smoke_test()
