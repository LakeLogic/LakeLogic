# Example: Managing Customer Data

This example keeps your Customer list clean and safe using a real contract and real data.

## Files

- Contract: `examples/customer_onboarding/contract.yaml`
- Raw data: `examples/customer_onboarding/customers.csv`
- Reference data: `examples/customer_onboarding/dim_geography.csv`, `examples/customer_onboarding/marketing_opt_outs.csv`
- Runner: `examples/customer_onboarding/run.py`

## The Simple Goal
We want to make sure every customer in our list:
1. Has an **email**.
2. Is at least **18 years old**.
3. Is **not** on our "Do Not Contact" list.

```mermaid
sequenceDiagram
    participant S as Raw Customers
    participant L as LakeGuard 🛡️
    participant D as Geography Table
    participant G as Clean Data ✅
    participant B as Quarantine 🛑

    S->>L: Send 1,000 Rows
    L->>D: Check Country Names
    L->>L: Check Age > 18
    L->>G: 950 Good Rows
    L->>B: 50 Bad Rows (with reasons)
```

## What happens to the "Bad Data"?

When LakeGuard finds a problem, it doesn't just delete it. It puts the row in a special "Bad Data" folder and adds two notes:

| Customer   | Problem         | Why?                         |
| :--------- | :-------------- | :--------------------------- |
| John Doe   | `email_missing` | The email field was empty    |
| Jane Smith | `under_age`     | The birthday shows she is 15 |

## How to try it
1. Run this command in your terminal:
```bash
cd examples/customer_onboarding
python run.py
```
2. Look for `good_customers.csv` and `bad_customers.csv` in this folder.

## Contract (excerpt)

```yaml
transformations:
  - rename: { from: email_address, to: email }
  - lookup:
      field: country_name
      reference: dim_geography
      on: country_id
      key: id
      value: name
```

## Raw Input (excerpt)

```csv
customer_id,email_address,first_name,last_name,birth_date,membership_level,country_id
1,john.doe@example.com,John,Doe,1985-05-15,GOLD,1
4,,NoEmail,User,1970-01-01,PLATINUM,3
7,invalid-email,Bad,Email,1990-01-01,BRONZE,2
```
