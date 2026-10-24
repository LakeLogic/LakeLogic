# Profiling

Profile a table before you write its rules. A profile is a summary of what is actually in the data —
per column: type, how much is empty, how many distinct values, the range — plus **findings**: what a
person should look at. Only counts and summaries leave the engine, never rows.

[**Try it in Colab**](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/11_profile_and_test_a_table.ipynb):
profile a claims table, agree its rules as a contract, and test every row — no warehouse needed.

## From the command line

```bash
# A file or folder (local, s3://, abfss://, gs://); the format is detected
lakelogic profile landing/orders/ --findings

# A table, by SQL pushdown
lakelogic profile main.sales.orders --engine databricks --findings
lakelogic profile orders --engine duckdb --database lake.duckdb

# A large table: sample it
lakelogic profile main.sales.orders --engine databricks --sample-percent 10 --findings
```

`--findings` adds the checks; `--contract` marks a contract's personal-data columns as sensitive so
their values are never shown; `--output` writes the JSON to a file. See `lakelogic profile --help`.

## From Python

```python
import duckdb
from lakelogic.core.profile_assess import assess_table

con = duckdb.connect("lake.duckdb")                  # or a SparkSession, or a SQLAlchemy engine
entry = assess_table("orders", con)                  # add contract="orders.yaml" to check its rules
print(entry["finding_counts"])                       # {'confirmed': 0, 'potential': 2, 'gap': 1}
for f in entry["findings"]:
    print(f["kind"], "—", f["title"], "—", f.get("evidence"))
```

`assess_tables([...], connection)` profiles several tables at once. For a large table pass
`settings={"mode": "sample", "sample_unit": "percent", "sample_percent": 10}`.

## Reading the findings

| Kind | Meaning |
|---|---|
| **confirmed** | A rule you agreed (in the contract) failed. |
| **potential** | An observation to check with the table's owner — e.g. a repeated id, a mostly empty column. It may be legitimate. |
| **gap** | A check that is missing — no contract, no key, no freshness target, a categorical column without accepted values. |

Profile without a contract first; agree the rules with the owner and write them into the contract;
profile again with the contract, and the agreed rules become **confirmed** checks. Then run the
contract on every load (`lakelogic run`) so each change is tested.
