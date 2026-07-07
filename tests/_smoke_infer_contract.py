"""Smoke test for infer_contract / ContractInferrer / ContractDraft."""

import sys
import pathlib

sys.path.insert(0, ".")

from lakelogic import infer_contract, ContractInferrer, ContractDraft, DataGenerator

ZOOPLA_JSON = (
    r"D:\Github\_SaaS\SaaS_lineagelogic_RA-azure-ecommerce"
    r"\RA-azure-ecommerce\src\data\10001.json"
)
ORDERS_CSV = "examples/06_advanced_workflows/synthetic_data_generation/data/orders_sample.csv"
ORDERS_PARQUET = "examples/06_advanced_workflows/synthetic_data_generation/data/orders_sample.parquet"

# ── Test 1: single JSON record (Zoopla-style) ────────────────────────────────
draft1 = infer_contract(ZOOPLA_JSON, title="Zoopla Listing", layer="bronze")
assert isinstance(draft1, ContractDraft)
assert len(draft1.fields) > 0
print(f"[1] JSON inference OK -> {draft1}")

# ── Test 2: CSV inference with rules ────────────────────────────────────────
draft2 = infer_contract(ORDERS_CSV, suggest_rules=True)
assert len(draft2.fields) == 10
has_rules = bool(draft2.quality.get("row_rules"))
print(f"[2] CSV + rules OK  -> {draft2}, rules={has_rules}")

# ── Test 3: Parquet inference ────────────────────────────────────────────────
draft3 = infer_contract(ORDERS_PARQUET, suggest_rules=False)
assert len(draft3.fields) == 10
assert not draft3.quality
print(f"[3] Parquet OK      -> {draft3}")

# ── Test 4: to_yaml / to_dict ────────────────────────────────────────────────
yml = draft2.to_yaml()
assert "model:" in yml and "fields:" in yml
d = draft2.to_dict()
assert "model" in d and "info" in d
print("[4] Serialisation OK")

# ── Test 5: save to disk ─────────────────────────────────────────────────────
import tempfile

with tempfile.TemporaryDirectory() as tmp:
    p = draft2.save(pathlib.Path(tmp) / "test_contract.yaml")
    assert p.exists()
    txt = p.read_text(encoding="utf-8")
    assert "fields:" in txt
print("[5] save() OK")

# ── Test 6: to_generator() chain ─────────────────────────────────────────────
gen = draft2.to_generator(seed=0)
assert isinstance(gen, DataGenerator)
df = gen.generate(rows=50)
assert df.shape[0] == 50
print(f"[6] to_generator().generate() OK -> shape={df.shape}")

# ── Test 7: full chain from Zoopla JSON ──────────────────────────────────────
gen2 = infer_contract(ZOOPLA_JSON, title="Zoopla").to_generator(seed=7)
df2 = gen2.generate(rows=100)
assert df2.shape[0] == 100
print(f"[7] Zoopla chain OK -> shape={df2.shape}, cols={df2.columns[:5]}")

# ── Test 8: in-memory DataFrame ──────────────────────────────────────────────
import polars as pl

mem_df = pl.read_csv(ORDERS_CSV)
draft4 = infer_contract(mem_df, title="Orders DF")
assert len(draft4.fields) == 10
print(f"[8] DataFrame input OK -> {draft4}")

# ── Test 9: show() prints something ──────────────────────────────────────────
import io
import contextlib

buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    draft2.show()
assert "fields:" in buf.getvalue()
print("[9] show() OK")

# ── Test 10: import symbol check ─────────────────────────────────────────────
from lakelogic import infer_contract as ic, ContractDraft as CD, ContractInferrer as CI

assert ic is infer_contract
assert CD is ContractDraft
assert CI is ContractInferrer
print("[10] Import symbols OK")

print("\nALL infer_contract TESTS PASSED")
