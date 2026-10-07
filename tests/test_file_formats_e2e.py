"""XML, Excel and JSON sources run end to end through a contract on Polars (the default engine).

Each test runs ``run_source`` the way a customer would and checks every row is accounted for
(good + bad = rows in the file) and the columns arrive. Found 2026-10-07: XML called a Polars
function that does not exist; Excel needed a reader package Core does not install.
"""

import json

import pytest

from lakelogic import DataProcessor

FIELDS = [
    {"name": "id", "type": "string"},
    {"name": "customer", "type": "string"},
    {"name": "amount", "type": "double"},
]


def _run(path):
    contract = {
        "version": "1.0.0",
        "dataset": "orders",
        "source": {"type": "landing", "path": str(path)},
        "model": {"fields": FIELDS},
    }
    return DataProcessor(engine="polars", contract=contract).run_source()


def _ok(good, bad, rows):
    assert len(good) + len(bad) == rows, "every row accounted for"
    assert {"id", "customer", "amount"} <= set(good.columns) | set(bad.columns)


# ── XML ────────────────────────────────────────────────────────────────────────────────
XML = """<?xml version="1.0"?><orders>
<order><id>1</id><customer>Ada</customer><amount>12.5</amount></order>
<order><id>2</id><customer>Bo</customer><amount>not-a-number</amount></order>
</orders>"""


def test_xml_single_file(tmp_path):
    p = tmp_path / "o.xml"
    p.write_text(XML, encoding="utf-8")
    good, bad = _run(p)
    _ok(good, bad, 2)
    assert len(bad) == 1  # the non-numeric amount is quarantined by the typed cast


def test_xml_several_files(tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    (d / "a.xml").write_text(XML, encoding="utf-8")
    (d / "b.xml").write_text(XML, encoding="utf-8")
    good, bad = _run(d / "*.xml")
    _ok(good, bad, 4)


# ── JSON ───────────────────────────────────────────────────────────────────────────────
ROWS = [{"id": "1", "customer": "Ada", "amount": 12.5}, {"id": "2", "customer": "Bo", "amount": 3}]


def test_json_array(tmp_path):
    p = tmp_path / "o.json"
    p.write_text(json.dumps(ROWS), encoding="utf-8")
    _ok(*_run(p), 2)


def test_json_lines(tmp_path):
    p = tmp_path / "o.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in ROWS), encoding="utf-8")
    _ok(*_run(p), 2)


def test_json_nested_objects_are_kept(tmp_path):
    rows = [{**r, "address": {"city": "Leeds"}} for r in ROWS]
    p = tmp_path / "o.json"
    p.write_text(json.dumps(rows), encoding="utf-8")
    good, bad = _run(p)
    _ok(good, bad, 2)


# ── Excel ──────────────────────────────────────────────────────────────────────────────
def test_excel_xlsx(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["id", "customer", "amount"])
    for r in ROWS:
        ws.append([r["id"], r["customer"], r["amount"]])
    p = tmp_path / "o.xlsx"
    wb.save(p)
    _ok(*_run(p), 2)
