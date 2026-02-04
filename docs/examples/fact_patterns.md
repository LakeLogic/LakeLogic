# Fact Table Patterns

LakeGuard handles the most common fact table designs used in modern Data Lakehouses. By choosing the right **Strategy**, you can automate the complex logic required for each type.

## Fact Table Comparison

| Pattern | Grain | Update Type | LakeGuard Strategy | Use Case |
| :--- | :--- | :--- | :--- | :--- |
| **Transaction** | One per event | Insert-only | `append` | Sales, Clicks, Orders |
| **Periodic Snapshot** | One per period | Insert-only | `overwrite` / `append` | Daily Balances, Inventory |
| **Accumulating Snapshot** | One per process | Updated | `merge` | Order Lifecycles, Workflows |
| **Factless** | One per event | Insert-only | `append` | Attendance, Promotions |

---

## 1. Transaction Facts (`append`)
The simplest form. Every time something happens, a new row is added. 
- **LakeGuard Rule**: Use `strategy: append`.
- **Validation**: LakeGuard ensures no duplicate `transaction_id` enters the table.

## 2. Periodic Snapshots (`overwrite`)
Captures the state of things at a specific point in time (e.g., "End of Day").
- **LakeGuard Rule**: Often used with `strategy: overwrite` for a specific partition (e.g., `WHERE date = '2024-01-01'`).
- **Validation**: Checks that "Total Balance" matches the sum of transactions for that day.

## 3. Accumulating Snapshots (`merge`)
Used for processes that have a beginning, middle, and end (like a Shipping workflow). The same row is updated as the order moves from "Ordered" to "Shipped" to "Delivered."
- **LakeGuard Rule**: Use `strategy: merge`. 
- **Validation**: Ensures that timestamps always move forward (e.g., `delivered_at` cannot be before `ordered_at`).

## 4. Factless Facts (`append`)
Used to record that something *didn't* happen or to track coverage (like "Which students attended which class?"). There is no "Amount" or "Quantity" column.
- **LakeGuard Rule**: Use `strategy: append`.
- **Validation**: Critical for checking "Referential Integrity" (e.g., Did the student actually exist in the Student table?).
