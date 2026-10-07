"""Scenario suite: realistic problems per file format, each with the outcome LakeLogic should give.

Used by complex_formats.ipynb (section "Scenario suite"); also runs on its own:
    python scenarios.py
Each scenario writes its own small input file, runs one contract, and checks the result:
how many rows are good, how many are quarantined, the quarantine reasons, or the error raised.
"""

from __future__ import annotations

import gzip
import os
import shutil
import zipfile
from pathlib import Path

import polars as pl
import yaml

from lakelogic import DataProcessor

HERE = Path(__file__).resolve().parent
ENGINE = os.environ.get("ENGINE", "polars")  # polars | duckdb | spark — every format must give the same answer
WORK = HERE / "data" / "scenarios"
RESULTS: list[dict] = []

ORDERS_MODEL = """
model:
  fields:
    - {name: order_id, type: long, required: true}
    - {name: customer, type: string, required: true}
    - {name: amount, type: double}
quality:
  row_rules:
    - {name: amount_not_negative, sql: "amount >= 0"}
"""

# phase: pre — these reshape the SOURCE, so required-field rules (phase pre by default, per OLC)
# see the extracted values on every engine.
ITEMS = """
transformations:
  - {phase: pre, explode: {field: items, output: item}}
  - {phase: pre, json_extract: {field: sku, source: item, path: "$.sku"}}
  - {phase: pre, json_extract: {field: qty, source: item, path: "$.qty", cast: int}}
"""


def _write(folder: Path, files: dict) -> None:
    for name, content in files.items():
        p = folder / name
        p.parent.mkdir(parents=True, exist_ok=True)
        if name.endswith(".gz") and isinstance(content, str):
            with gzip.open(p, "wt", encoding="utf-8") as fh:
                fh.write(content)
        elif name.endswith(".zip") and isinstance(content, dict):
            with zipfile.ZipFile(p, "w") as zf:
                for member, text in content.items():
                    zf.writestr(member, text)
        elif callable(content):
            content(p)
        else:
            p.write_bytes(content if isinstance(content, bytes) else content.encode("utf-8"))


def scenario(group, name, files, contract, *, good=None, bad=None, errors=(), raises=None, warns=None, show=()):
    """Run one scenario and record PASS/FAIL against what LakeLogic should do."""
    folder = WORK / ENGINE / f"{group}_{len(RESULTS):02d}".replace(" ", "_").replace(".", "")
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True)
    _write(folder, files)
    first = folder / next(iter(files))
    text = contract.replace("{path}", first.as_posix()).replace("{dir}", folder.as_posix())
    outcome, problems, detail = "", [], ""
    from loguru import logger

    warnings: list[str] = []
    sink = logger.add(lambda m: warnings.append(m.record["message"]), level="WARNING")
    try:
        g, b = DataProcessor(engine=ENGINE, contract=yaml.safe_load(text)).run_source()
        g, b = _to_polars(g), _to_polars(b)
        reasons = [e for row in b["_lakelogic_errors"].to_list() for e in row] if len(b) else []
        outcome = f"{len(g)} good, {len(b)} quarantined"
        detail = "; ".join(sorted(set(reasons)))[:160]
        if raises:
            problems.append(f"expected an error containing {raises!r}")
        if good is not None and len(g) != good:
            problems.append(f"expected {good} good")
        if bad is not None and len(b) != bad:
            problems.append(f"expected {bad} quarantined")
        for e in errors:
            if not any(e in r for r in reasons):
                problems.append(f"no reason containing {e!r}")
        if warns and not any(warns in w for w in warnings):
            problems.append(f"no warning containing {warns!r}")
        if warns:
            detail = next(w for w in warnings if warns in w)[:160] if not problems else detail
        if show:
            print(g.select([c for c in show if c in g.columns]))
    except Exception as exc:  # noqa: BLE001 - a scenario may expect an error
        msg = f"{type(exc).__name__}: {exc}"
        outcome, detail = "error", msg.splitlines()[0][:160]
        if not raises or raises.lower() not in msg.lower():
            problems.append("unexpected error" if not raises else f"error did not mention {raises!r}")
    finally:
        logger.remove(sink)
    status = "PASS" if not problems else "FAIL"
    RESULTS.append(
        {
            "format": group,
            "scenario": name,
            "result": outcome,
            "detail": detail,
            "status": status,
            "why": "; ".join(problems),
        }
    )
    print(f"[{status}] {group:<11} {name:<55} {outcome}" + (f"  <- {'; '.join(problems)}" if problems else ""))


def _to_polars(df):
    """Spark/DuckDB results as Polars, so every engine is checked the same way."""
    if isinstance(df, pl.DataFrame):
        return df
    if hasattr(df, "toPandas"):
        return pl.from_pandas(df.toPandas())
    if hasattr(df, "pl"):
        return df.pl()
    return pl.DataFrame(df)


def summary() -> pl.DataFrame:
    df = pl.DataFrame(RESULTS)
    passed = (df["status"] == "PASS").sum()
    print(f"\n{passed} of {len(df)} scenarios behaved as expected.")
    return df


def src(fmt: str, extra: str = "", path: str = "{path}") -> str:
    return f'version: "1.0"\ninfo: {{title: scenario}}\nsource:\n  type: landing\n  path: "{path}"\n  format: {fmt}\n{extra}'


# ════════════════════════════════════════════════════════════════════════════
def json_scenarios():
    nested = (
        src("json", "  flatten_nested: true\n")
        + """
model:
  fields:
    - {name: order_id, type: long, required: true}
    - {name: customer_name, type: string, required: true}
    - {name: amount, type: double}
"""
    )
    scenario(
        "json",
        "a whole customer object is missing",
        {"o.json": '[{"order_id": 1, "customer": {"name": "Ada"}, "amount": 5}, {"order_id": 2, "amount": 3}]'},
        nested,
        good=1,
        bad=1,
        errors=["customer_name_required"],
    )
    scenario(
        "json",
        "ids sent as text; one is not a number",
        {"o.json": '[{"order_id": "10", "customer": {"name": "Ada"}}, {"order_id": "X1", "customer": {"name": "Bo"}}]'},
        nested,
        good=1,
        bad=1,
        errors=["order_id cannot be cast"],
    )
    scenario(
        "json",
        "a new field appears (schema drift)",
        {"o.json": '[{"order_id": 1, "customer": {"name": "Ada"}, "amount": 5, "coupon": "SAVE10"}]'},
        nested,
        good=1,
        bad=0,
    )
    scenario(
        "json",
        "an order with an empty items list",
        {"o.json": '[{"order_id": 1, "items": [{"sku": "A", "qty": 1}]}, {"order_id": 2, "items": []}]'},
        src("json", "  flatten_nested: true\n")
        + ITEMS
        + """
model:
  fields:
    - {name: order_id, type: long, required: true}
    - {name: sku, type: string, required: true}
""",
        good=1,
        bad=1,
        errors=["sku_required"],
    )
    scenario(
        "json", "the file is cut off mid-record", {"o.json": '[{"order_id": 1, "customer": '}, nested, raises="json"
    )


def xml_scenarios():
    contract = (
        src("xml", "  flatten_nested: true\n")
        + ITEMS
        + """
model:
  fields:
    - {name: id, type: long, required: true}
    - {name: customer, type: string, required: true}
    - {name: sku, type: string, required: true}
    - {name: qty, type: int, required: true}
"""
    )

    def orders(*body):
        return "<orders>" + "".join(body) + "</orders>"

    scenario(
        "xml",
        "one <item> where others have several",
        {
            "o.xml": orders(
                '<order id="1"><customer>Ada</customer><items><item><sku>A</sku><qty>1</qty></item>'
                "<item><sku>B</sku><qty>2</qty></item></items></order>",
                '<order id="2"><customer>Bo</customer><items><item><sku>C</sku><qty>1</qty></item></items></order>',
            )
        },
        contract,
        good=3,
        bad=0,
    )
    scenario(
        "xml",
        "an order with no <items> at all",
        {
            "o.xml": orders(
                '<order id="1"><customer>Ada</customer><items><item><sku>A</sku><qty>1</qty></item></items></order>',
                '<order id="2"><customer>Bo</customer></order>',
            )
        },
        contract,
        good=1,
        bad=1,
        errors=["sku_required"],
    )
    scenario(
        "xml",
        "the id attribute is missing",
        {
            "o.xml": orders(
                "<order><customer>Ada</customer><items><item><sku>A</sku><qty>1</qty></item></items></order>",
                '<order id="2"><customer>Bo</customer><items><item><sku>B</sku><qty>1</qty></item></items></order>',
            )
        },
        contract,
        good=1,
        bad=1,
        errors=["id_required"],
    )
    scenario(
        "xml",
        "escaped & and CDATA text survive",
        {
            "o.xml": orders(
                '<order id="1"><customer>Smith &amp; Sons</customer><items><item><sku><![CDATA[A<1>]]></sku>'
                "<qty>1</qty></item></items></order>"
            )
        },
        contract,
        good=1,
        bad=0,
        show=["customer", "sku"],
    )
    scenario(
        "xml",
        "namespace prefixes on every tag",
        {
            "o.xml": '<ns:orders xmlns:ns="urn:shop"><ns:order id="1"><ns:customer>Ada</ns:customer><ns:items>'
            "<ns:item><ns:sku>A</ns:sku><ns:qty>1</ns:qty></ns:item></ns:items></ns:order></ns:orders>"
        },
        contract,
        good=1,
        bad=0,
    )
    scenario(
        "xml",
        "records buried at different depths: row_tag",
        {
            "o.xml": '<feed><meta><order id="9"><customer>Meta</customer></order></meta><batch><order id="1"><customer>Ada</customer>'
            "<items><item><sku>A</sku><qty>1</qty></item></items></order></batch></feed>"
        },
        contract.replace("  flatten_nested: true", "  flatten_nested: true\n  options: {row_tag: order}"),
        good=1,
        bad=1,
        errors=["sku_required"],
    )
    scenario(
        "xml",
        "a tag is never closed",
        {"o.xml": "<orders><order id='1'><customer>Ada</order></orders>"},
        contract,
        raises="mismatched tag",
    )


def excel_scenarios():
    import openpyxl

    def book(rows, sheet="Orders", extra_first=False):
        def write(p):
            wb = openpyxl.Workbook()
            if extra_first:
                wb.active.title = "Cover"
                ws = wb.create_sheet(sheet)
            else:
                ws = wb.active
                ws.title = sheet
            for r in rows:
                ws.append(r)
            wb.save(p)

        return write

    head = ["order_id", "customer", "amount"]
    scenario(
        "excel",
        "two title rows; header on row 3",
        {"o.xlsx": book([["Finance export"], ["Generated 2026-10-07"], head, [1, "Ada", 5], [2, "Bo", -1]])},
        src("xlsx", "  options: {header_row: 3}\n") + ORDERS_MODEL,
        good=1,
        bad=1,
        errors=["amount_not_negative"],
    )
    scenario(
        "excel",
        "the named tab does not exist",
        {"o.xlsx": book([head, [1, "Ada", 5]])},
        src("xlsx", "  options: {sheet_name: Q4}\n") + ORDERS_MODEL,
        raises="not found",
    )
    scenario(
        "excel",
        "data is on the second tab",
        {"o.xlsx": book([head, [1, "Ada", 5]], extra_first=True)},
        src("xlsx", "  options: {sheet_name: Orders}\n") + ORDERS_MODEL,
        good=1,
        bad=0,
    )
    scenario(
        "excel",
        "numbers typed as text cells",
        {"o.xlsx": book([head, ["1", "Ada", "10.5"], ["2", "Bo", "ten"]])},
        src("xlsx") + ORDERS_MODEL,
        good=1,
        bad=1,
        errors=["amount cannot be cast"],
    )
    scenario(
        "excel",
        "a TOTAL row nobody told us about",
        {"o.xlsx": book([head, [1, "Ada", 5], [], ["TOTAL", None, 5]])},
        src("xlsx") + ORDERS_MODEL,
        good=1,
        bad=1,
        errors=["order_id cannot be cast"],
    )
    scenario(
        "excel",
        "...and the same file with skip_footer: 1",
        {"o.xlsx": book([head, [1, "Ada", 5], [], ["TOTAL", None, 5]])},
        src("xlsx", "  options: {skip_footer: 1}\n") + ORDERS_MODEL,
        good=1,
        bad=0,
    )


def csv_scenarios():
    scenario(
        "csv",
        "tab-separated",
        {"o.tsv": "order_id\tcustomer\tamount\n1\tAda\t5\n"},
        src("csv", '  options: {delimiter: "\\t"}\n') + ORDERS_MODEL,
        good=1,
        bad=0,
    )
    scenario(
        "csv",
        "pipe-separated, a pipe inside a quoted name",
        {"o.csv": 'order_id|customer|amount\n1|"A|B Ltd"|5\n'},
        src("csv", '  options: {delimiter: "|"}\n') + ORDERS_MODEL,
        good=1,
        bad=0,
        show=["customer"],
    )
    scenario(
        "csv",
        "UTF-8 file with a byte-order mark",
        {"o.csv": "\ufefforder_id,customer,amount\n1,Ada,5\n".encode("utf-8")},
        src("csv") + ORDERS_MODEL,
        good=1,
        bad=0,
    )
    scenario(
        "csv",
        "Latin-1 file, encoding not declared",
        {"o.csv": "order_id,customer,amount\n1,Zoë,5\n".encode("latin-1")},
        src("csv") + ORDERS_MODEL,
        raises="utf-8",
    )
    scenario(
        "csv",
        "...and with encoding: latin-1",
        {"o.csv": "order_id,customer,amount\n1,Zoë,5\n".encode("latin-1")},
        src("csv", "  options: {encoding: latin-1}\n") + ORDERS_MODEL,
        good=1,
        bad=0,
        show=["customer"],
    )
    scenario(
        "csv",
        "a row with a column missing (null amount fails the rule)",
        {"o.csv": "order_id,customer,amount\n1,Ada,5\n2,Bo\n"},
        src("csv") + ORDERS_MODEL,
        good=1,
        bad=1,
        errors=["amount_not_negative"],
    )
    scenario(
        "csv",
        "NULL and N/A written as text",
        {"o.csv": "order_id,customer,amount\n1,Ada,5\n2,NULL,N/A\n"},
        src("csv", '  options: {null_values: ["NULL", "N/A"]}\n') + ORDERS_MODEL,
        good=1,
        bad=1,
        errors=["customer_required"],
    )
    scenario(
        "csv",
        "European numbers: 1.234,50",
        {"o.csv": "order_id;customer;amount\n1;Ada;1.234,50\n2;Bo;-3,00\n"},
        src("csv", '  options: {delimiter: ";", decimal_comma: true}\n') + ORDERS_MODEL,
        good=1,
        bad=1,
        errors=["amount_not_negative"],
        show=["amount"],
    )
    scenario(
        "csv",
        "a note with a line break inside quotes",
        {"o.csv": 'order_id,customer,amount\n1,"Ada\nLovelace",5\n'},
        src("csv") + ORDERS_MODEL,
        good=1,
        bad=0,
    )


def compressed_scenarios():
    scenario(
        "compressed",
        ".json.gz",
        {"o.json.gz": '[{"order_id": 1, "customer": "Ada", "amount": 5}]'},
        src("json") + ORDERS_MODEL,
        good=1,
        bad=0,
    )
    scenario(
        "compressed",
        "a .gz that is not really gzip",
        {"o.csv.gz": b"order_id,customer\n1,Ada\n"},
        src("csv") + ORDERS_MODEL,
        raises="gzip",
    )
    scenario(
        "compressed",
        "a zip with no CSV inside",
        {"o.zip": {"notes.txt": "nothing here"}},
        src("csv") + ORDERS_MODEL,
        raises="No files",
    )
    scenario(
        "compressed",
        "zip: two folders, columns in a different order",
        {
            "o.zip": {
                "a/one.csv": "order_id,customer,amount\n1,Ada,5\n",
                "b/two.csv": "amount,order_id,customer\n7,2,Bo\n",
            }
        },
        src("csv") + ORDERS_MODEL,
        good=2,
        bad=0,
    )
    scenario(
        "compressed",
        "zip: one file lacks a column -> warning, nulls",
        {
            "o.zip": {
                "2026-09/orders.csv": "order_id,customer,amount\n1,Ada,5\n",
                "2026-10/orders.csv": "order_id,customer\n2,Bo\n",
            }
        },
        src("csv") + ORDERS_MODEL,
        good=1,
        bad=1,
        errors=["amount_not_negative"],
        warns="do not share one structure",
    )
    scenario(
        "compressed",
        "zip: one CSV is header-only",
        {"o.zip": {"a.csv": "order_id,customer,amount\n1,Ada,5\n", "b.csv": "order_id,customer,amount\n"}},
        src("csv") + ORDERS_MODEL,
        good=1,
        bad=0,
    )


def avro_scenarios():
    def avro(frame):
        return lambda p: frame.write_avro(p)

    model = """
model:
  fields:
    - {name: order_id, type: long, required: true}
    - {name: customer_name, type: string, required: true}
    - {name: amount, type: double}
"""
    scenario(
        "avro",
        "a later file adds a column (schema evolution)",
        {
            "a.avro": avro(pl.DataFrame({"order_id": [1], "customer": [{"name": "Ada"}]})),
            "b.avro": avro(pl.DataFrame({"order_id": [2], "customer": [{"name": "Bo"}], "amount": [5.0]})),
        },
        src("avro", "  flatten_nested: true\n", path="{dir}/*.avro") + model,
        good=2,
        bad=0,
        show=["order_id", "amount"],
    )
    scenario(
        "avro",
        "a null nested record",
        {"a.avro": avro(pl.DataFrame({"order_id": [1, 2], "customer": [{"name": "Ada"}, None]}))},
        src("avro", "  flatten_nested: true\n") + model,
        good=1,
        bad=1,
        errors=["customer_name_required"],
    )
    scenario(
        "avro",
        "amount sent as a string",
        {
            "a.avro": avro(
                pl.DataFrame(
                    {"order_id": [1, 2], "customer": [{"name": "Ada"}, {"name": "Bo"}], "amount": ["5.5", "abc"]}
                )
            )
        },
        src("avro", "  flatten_nested: true\n") + model,
        good=1,
        bad=1,
        errors=["amount cannot be cast"],
    )


def fixed_width_scenarios():
    layout = """
model:
  fields:
    - {name: rec_type, range: [0, 1], type: string}
    - {name: sort_code, range: [1, 7], type: string, required: true}
    - {name: amount_pence, range: [7, 15], type: long, required: true}
"""
    fw = lambda extra="": src("fixed_width", "  record_length: 15\n" + extra) + layout
    scenario(
        "fixed_width",
        "a record cut short",
        {"s.dat": "D20157500001050\nD3096340000\n"},
        fw(),
        good=1,
        bad=1,
        errors=["expected 15, got 11"],
    )
    scenario(
        "fixed_width",
        "a record too long",
        {"s.dat": "D20157500001050\nD30963400002000XX\n"},
        fw(),
        good=1,
        bad=1,
        errors=["expected 15, got 17"],
    )
    scenario(
        "fixed_width",
        "no line breaks (mainframe fixed-length)",
        {"s.dat": "D20157500001050D30963400002000"},
        fw(),
        good=2,
        bad=0,
    )
    scenario(
        "fixed_width",
        "Windows line endings (CRLF)",
        {"s.dat": b"D20157500001050\r\nD30963400002000\r\n"},
        fw(),
        good=2,
        bad=0,
    )
    scenario(
        "fixed_width",
        "EBCDIC from a mainframe (cp037)",
        {"s.dat": "D20157500001050\n".encode("cp037")},
        fw("  encoding: cp037\n"),
        good=1,
        bad=0,
    )
    scenario(
        "fixed_width",
        "a letter in the amount",
        {"s.dat": "D2015750000X050\n"},
        fw(),
        good=0,
        bad=1,
        errors=["amount_pence cannot be cast"],
    )
    scenario(
        "fixed_width",
        "sender trims trailing spaces",
        {"s.dat": "D201575000010  \nD2015\n".replace("  \n", "\n")},
        fw(),
        good=0,
        bad=2,
        errors=["expected 15, got 13"],
    )
    scenario(
        "fixed_width",
        "implied decimals: 00001050 -> 10.50",
        {"s.dat": "D20157500001050\nD2015750000X050\n"},
        src("fixed_width", "  record_length: 15\n  options: {implied_decimals: {amount: 2}}\n")
        + """
model:
  fields:
    - {name: sort_code, range: [1, 7], type: string, required: true}
    - {name: amount, range: [7, 15], type: double, required: true}
""",
        good=1,
        bad=1,
        errors=["amount cannot be cast"],
        show=["amount"],
    )
    scenario(
        "fixed_width",
        "dates as YYYYMMDD; month 13 is flagged",
        {"s.dat": "D20261007\nD20261399\n"},
        src("fixed_width", '  options: {date_formats: {value_date: "yyyyMMdd"}}\n')
        + """
model:
  fields:
    - {name: rec_type, range: [0, 1], type: string}
    - {name: value_date, range: [1, 9], type: date, required: true}
""",
        good=1,
        bad=1,
        errors=["value_date cannot be cast"],
        show=["value_date"],
    )


def xls_scenarios():
    legacy = HERE / "data" / "orders_legacy.xls"
    scenario(
        "xls",
        "Excel 97-2003, named tab, header on row 2",
        {"orders_legacy.xls": legacy.read_bytes()},
        src("xls", "  options: {sheet_name: Ledger, header_row: 2}\n")
        + """
model:
  fields:
    - {name: order_id, type: long, required: true}
    - {name: booked_on, type: date, required: true}
    - {name: amount, type: double}
quality:
  row_rules:
    - {name: amount_not_negative, sql: "amount >= 0"}
""",
        good=1,
        bad=1,
        errors=["amount_not_negative"],
    )


ALL = [
    json_scenarios,
    xml_scenarios,
    excel_scenarios,
    csv_scenarios,
    compressed_scenarios,
    avro_scenarios,
    fixed_width_scenarios,
    xls_scenarios,
]

if __name__ == "__main__":
    import sys

    from loguru import logger

    logger.remove()
    if ENGINE == "spark":  # one quiet local session, shared by every scenario
        from pyspark.sql import SparkSession

        SparkSession.builder.master("local[2]").config("spark.ui.showConsoleProgress", "false").config(
            "spark.ui.enabled", "false"
        ).config("spark.sql.shuffle.partitions", "2").getOrCreate().sparkContext.setLogLevel("ERROR")
    wanted = sys.argv[1:]  # e.g. `python scenarios.py csv xml` runs only those groups
    for fn in ALL:
        if not wanted or any(fn.__name__.startswith(w) for w in wanted):
            fn()
    with pl.Config(tbl_rows=100, fmt_str_lengths=70, tbl_width_chars=200):
        print(summary().filter(pl.col("status") == "FAIL"))
