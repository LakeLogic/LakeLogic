# Example: Bronze Ingestion (Schema Gate)

This example shows how to use LakeGuard as a **schema gate** during raw ingestion. The contract allows **no unknown fields** and quarantines anything that doesn’t match the expected schema.

## Files

- Contract: `examples/ingestion/contract.yaml`
- Raw data: `examples/ingestion/data/raw_crm.csv`
- Runner: `examples/ingestion/run.py`

## Contract (excerpt)

```yaml
version: 1.0.0
dataset: bronze_crm_users

schema_policy:
  evolution: strict
  unknown_fields: drop

model:
  fields:
    - name: user_id
      type: integer
      required: true
    - name: signup_date
      type: date
    - name: email
      type: string
```

## Raw Input (excerpt)

```csv
user_id,signup_date,email,extra_col
1001,2024-01-01,alice@example.com,unexpected
1002,2024-01-05,bob@example.com,unexpected
,2024-01-10,missing@example.com,unexpected
```

## Run

```bash
cd examples/ingestion
python run.py
```

This produces:
- `good_crm.csv`
- `bad_crm.csv` (quarantined with error reasons)

In this example, the `extra_col` column is dropped (policy: `drop`), and the row with a missing `user_id` is quarantined.
