"""Smoke test for generate_from_sample."""
import sys
sys.path.insert(0, ".")

from lakelogic import DataGenerator

CONTRACT = "examples/06_advanced_workflows/synthetic_data_generation/contract_orders.yaml"
CSV      = "examples/06_advanced_workflows/synthetic_data_generation/data/orders_sample.csv"
PARQUET  = "examples/06_advanced_workflows/synthetic_data_generation/data/orders_sample.parquet"

gen = DataGenerator(CONTRACT, seed=42)

# Test 1: seed from CSV
df = gen.generate_from_sample(CSV, rows=200)
assert df.shape == (200, 10), f"Expected 200 rows, got {df.shape}"
statuses = sorted(df["status"].drop_nulls().unique().to_list())
print(f"[1] CSV seed OK  -> shape={df.shape}, status={statuses}")

# Test 2: seed from Parquet + invalid_ratio
df2 = gen.generate_from_sample(PARQUET, rows=100, invalid_ratio=0.1)
assert df2.shape == (100, 10), f"Expected 100 rows, got {df2.shape}"
print(f"[2] Parquet seed OK -> shape={df2.shape}")

# Test 3: columns filter  (only status + region from file; rest synthetic)
df3 = gen.generate_from_sample(CSV, rows=50, columns=["status", "region"])
assert df3.shape == (50, 10)
# status values should all come from the real file's values
real_statuses = {"pending", "confirmed", "shipped", "delivered", "cancelled"}
gen_statuses  = set(df3["status"].drop_nulls().to_list())
assert gen_statuses.issubset(real_statuses), f"Unexpected statuses: {gen_statuses - real_statuses}"
print(f"[3] Columns filter OK -> shape={df3.shape}, statuses={sorted(gen_statuses)}")

# Test 4: seed from an in-memory polars DataFrame
import polars as pl
seed_df = pl.read_csv(CSV)
df4 = gen.generate_from_sample(seed_df, rows=50)
assert df4.shape == (50, 10)
print(f"[4] DataFrame seed OK -> shape={df4.shape}")

# Test 5: pandas output format
df5 = gen.generate_from_sample(CSV, rows=30, output_format="pandas")
import pandas as pd
assert isinstance(df5, pd.DataFrame)
assert len(df5) == 30
print(f"[5] Pandas output OK -> shape={df5.shape}")

print("\nALL TESTS PASSED")

# ── from_file tests (contract-free) ─────────────────────────────────────────
print("\n--- from_file (no contract) ---")

# Test 6: from_file with CSV — schema inferred, generate() auto-seeds from file
gen2 = DataGenerator.from_file(CSV, seed=42)
df6 = gen2.generate(rows=300)
assert df6.shape[0] == 300, f"Expected 300 rows, got {df6.shape[0]}"
assert set(df6.columns) == set(["order_id","customer_email","customer_name","status",
                                  "amount","tax_rate","quantity","order_date","region","is_b2b"])
statuses6 = sorted(df6["status"].drop_nulls().unique().to_list())
print(f"[6] from_file CSV -> shape={df6.shape}, status={statuses6}")

# Test 7: from_file with Parquet
gen3 = DataGenerator.from_file(PARQUET, seed=7)
df7 = gen3.generate(rows=100)
assert df7.shape == (100, 10)
print(f"[7] from_file Parquet -> shape={df7.shape}")

# Test 8: from_file with invalid_ratio still works
df8 = gen2.generate(rows=200, invalid_ratio=0.2)
assert df8.shape == (200, 10)
print(f"[8] from_file + invalid_ratio -> shape={df8.shape}")

# Test 9: from_file from an in-memory polars DataFrame
import polars as pl
mem_df = pl.read_csv(CSV)
gen4 = DataGenerator.from_file(mem_df, seed=0)
df9 = gen4.generate(rows=50)
assert df9.shape == (50, 10)
print(f"[9] from_file DataFrame -> shape={df9.shape}")

# Test 10: from_file + pandas output
df10 = gen2.generate(rows=30, output_format="pandas")
import pandas as pd
assert isinstance(df10, pd.DataFrame) and len(df10) == 30
print(f"[10] from_file pandas output -> shape={df10.shape}")

print("\nALL from_file TESTS PASSED")
