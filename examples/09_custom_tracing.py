import polars as pl
from lakelogic.core.processor import DataProcessor
from pathlib import Path
import time

def demo_custom_tracing():
    # 1. Setup a simple contract
    contract_yaml = """
    dataset: traced_users
    model:
      fields:
        - name: id
          type: int
        - name: name
          type: string
    quality:
      row_rules:
        - name: Name Present
          sql: name IS NOT NULL
    """
    
    # 2. Create some data
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", None]
    })
    
    # 3. Initialize processor
    processor = DataProcessor(contract=contract_yaml, engine="polars")
    
    print("\n🚀 Running LakeLogic with Custom Python Tracing...\n")
    
    # We can use the 'trace_step' context manager to add our own steps
    # even before or after the main 'run' call if we manage the trace manually,
    # but the automated way is to let 'run' start the trace.
    
    # However, since 'run' initializes the trace, we can see it in the result.
    result = processor.run(df)
    
    # Let's say we do some heavy Python processing AFTER the LakeLogic run
    with processor.trace_step("Post-Process: Enrichment", author="Antigravity") as step:
        time.sleep(0.1) # Simulate work
        enriched_df = result.good.with_columns(pl.lit("Active").alias("status"))
        step.output_rows = len(enriched_df)
        step.details["method"] = "standard_python"
    
    # Note: Because the 'run' call already finished, our manual 'trace_step' 
    # above won't be in result.trace.steps unless we append it or manage the trace list.
    
    # A better pattern is using 'external_logic' in the contract!
    
    print("✅ Run complete.")
    print(f"Good rows: {len(result.good)}")
    print(f"Trace steps captured: {len(result.trace.steps)}")
    
    for s in result.trace.steps:
        print(f" - {s.step}: {s.duration_ms:.2f}ms")

if __name__ == "__main__":
    demo_custom_tracing()
