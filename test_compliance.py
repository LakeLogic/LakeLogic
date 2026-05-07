import sys
import os
import polars as pl

sys.path.append(r"C:\_Personal\_SaaS\lakelogic\examples\colab")
sys.path.append(r"C:\_Personal\_SaaS\lakelogic")

import _setup as s
import lakelogic as ll

ENGINE = "duckdb"

contract_path = s.write_contract(
    """
version: 1.0.0
dataset: customers

model:
  fields:
    - name: customer_id
      type: string
      required: true
    - name: name
      type: string
      pii: true
    - name: email
      type: string
      pii: true
    - name: phone
      type: string
      pii: true
    - name: lifetime_value
      type: float
""",
    "02_compliance_governance_demo/customers.yaml",
)

source_df = ll.DataGenerator(contract_path).generate(rows=100, output_format=ENGINE)
proc = ll.DataProcessor(contract_path, engine=ENGINE)
good, _ = proc.run(source_df)
good = s.to_polars(good)

sample_id = good["customer_id"][0]

from lakelogic.core.gdpr import forget_subjects

compliance_event = {
    "framework": "GDPR",
    "article": "Article 17",
    "trigger": "subject_request",
    "legal_basis": "consent_withdrawn",
    "request_id": "DSR-2026-0142",
    "strategy": "hash",
    "strategy_per_field": {
        "email": "hash",
        "phone": "redact",
        "name": "nullify"
    }
}

proc.contract.compliance = compliance_event

erased = proc.forget(
    good,
    subject_column="customer_id",
    subject_ids=[sample_id],
)

print("\n--- Pipeline Metadata (proc.last_report) ---")
import json
print(json.dumps(proc.last_report["compliance_event"], indent=2))
print("--------------------------------------------\n")

audit_cols = [
    c
    for c in erased.columns
    if "customer_id" in c or "name" in c or "email" in c or "phone" in c or "lifetime_value" in c or "_lakelogic_" in c
]
print(erased.filter(pl.col("customer_id") == sample_id).select(audit_cols[:8]))
