"""Run one LakeLogic contract against XML, Excel, JSON and JSON Lines files.

    pip install "lakelogic[polars]"       # includes openpyxl, used for Excel
    python run.py

For each file it prints how many rows passed the contract and how many were quarantined,
with the reason. Every file contains a few deliberately bad rows:
  * a missing customer (null) -> fails "required" (an empty string "" is not null, and passes)
  * a negative amount        -> fails the "amount >= 0" rule
  * a non-numeric amount     -> fails the cast to double
The Excel file is created by this script (data/orders.xlsx), so you can open it too.
"""

from pathlib import Path

from lakelogic import DataProcessor

HERE = Path(__file__).parent
DATA = HERE / "data"

# One contract for every file: the format is detected from the file extension.
CONTRACT = {
    "version": "1.0.0",
    "dataset": "orders",
    "model": {
        "fields": [
            {"name": "order_id", "type": "string", "required": True},
            {"name": "customer", "type": "string", "required": True},
            {"name": "country", "type": "string"},
            {"name": "amount", "type": "double"},
        ]
    },
    "quality": {
        "row_rules": [
            {"name": "amount_not_negative", "sql": "amount >= 0"},
        ]
    },
}


def make_excel(path: Path) -> None:
    """Write a small workbook with one bad row (negative amount)."""
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "orders"
    ws.append(["order_id", "customer", "country", "amount"])
    ws.append(["4001", "Jo Kim", "KR", 60.0])
    ws.append(["4002", "Kai Berg", "SE", 12.5])
    ws.append(["4003", "Lea Roy", "CA", -1.0])
    wb.save(path)


def run(path: Path) -> None:
    contract = {**CONTRACT, "source": {"type": "landing", "path": str(path)}}
    good, bad = DataProcessor(engine="polars", contract=contract).run_source()
    total = len(good) + len(bad)
    print(f"\n{path.name}: {total} rows -> {len(good)} good, {len(bad)} quarantined")
    if len(bad):
        reason_col = next((c for c in bad.columns if "error" in c.lower() or "reason" in c.lower()), None)
        cols = [c for c in ("order_id", "customer", "amount", reason_col) if c and c in bad.columns]
        print(bad.select(cols))


if __name__ == "__main__":
    make_excel(DATA / "orders.xlsx")
    for name in ("orders.xml", "orders.xlsx", "orders.json", "orders.jsonl"):
        run(DATA / name)
