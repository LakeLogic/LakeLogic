# Example: Managing Customer Data

This example shows how to keep your Customer list clean and safe.

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
1. Copy the `contract.yaml` file from this folder.
2. Run this command in your terminal:
```bash
lakeguard run --engine polars --contract contract.yaml --source customers.csv
```
3. Look for `good_customers.csv` and `bad_customers.csv` in your folder!
