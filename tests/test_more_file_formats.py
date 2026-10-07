"""Compressed files, CSV dialects, Avro and old Excel .xls (added 2026-10-07).

Before this: a `.csv.gz` went to the CSV reader as-is, a `.zip` was not read, every CSV was
read as comma/UTF-8/header-on-line-1, `.avro` had no reader, and `.xls` failed with "File is
not a zip file" (openpyxl reads only .xlsx). Excel date cells also landed as
"2026-10-01 00:00:00", which a `date` field cannot cast.
"""
import datetime as dt
import glob
import gzip
import os
import tempfile
import zipfile
from pathlib import Path

import openpyxl
import pytest

from lakelogic import DataProcessor

pl = pytest.importorskip("polars")

ORDERS = {"fields": [{"name": "order_id", "type": "long", "required": True},
                     {"name": "customer", "type": "string", "required": True},
                     {"name": "amount", "type": "double"}]}
POSITIVE = {"row_rules": [{"name": "amount_not_negative", "sql": "amount >= 0"}]}
EXAMPLES = Path(__file__).resolve().parents[1] / "examples" / "file_formats" / "data"


def _run(path, model=ORDERS, quality=POSITIVE, **source):
    contract = {"version": "1.0", "info": {"title": "t"},
                "source": {"type": "landing", "path": str(path), **source}, "model": model, "quality": quality}
    return DataProcessor(engine="polars", contract=contract).run_source()


def _unpack_dirs():
    return set(glob.glob(os.path.join(tempfile.gettempdir(), "lakelogic_unpack_*")))


# ── .gz / .zip ────────────────────────────────────────────────────────────────

def test_gzipped_csv_reads_with_or_without_a_declared_format(tmp_path):
    p = tmp_path / "orders.csv.gz"
    with gzip.open(p, "wt") as fh:
        fh.write("order_id,customer,amount\n1,Ada,10.5\n2,Bo,n/a\n")
    before = _unpack_dirs()
    for source in ({"format": "csv"}, {}):
        good, bad = _run(p, **source)
        assert good["order_id"].to_list() == [1]
        assert bad["order_id"].to_list() == [2]
    assert _unpack_dirs() == before  # the temp copies are removed after the load


def test_gzipped_json_is_read_as_json(tmp_path):
    p = tmp_path / "orders.json.gz"
    with gzip.open(p, "wt") as fh:
        fh.write('[{"order_id": 1, "customer": "Ada", "amount": 1}, {"order_id": 2, "customer": null, "amount": 2}]')
    good, bad = _run(p, format="json")
    assert good["order_id"].to_list() == [1]
    assert bad["order_id"].to_list() == [2]


def test_zip_reads_every_member_of_the_format_and_reports_where_each_row_came_from(tmp_path):
    p = tmp_path / "bundle.zip"
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("north/a.csv", "order_id,customer,amount\n1,Ada,5\n2,,6\n")
        zf.writestr("south/b.csv", "order_id,customer,amount\n3,Bo,8\n")
        zf.writestr("README.txt", "notes, not data")
        zf.writestr("__MACOSX/._a.csv", "junk")
    good, bad = _run(p, format="csv")
    assert sorted(good["order_id"].to_list()) == [1, 3]
    assert bad["order_id"].to_list() == [2]
    assert sorted(set(good["_source_file"].to_list())) == [f"{p}!north/a.csv", f"{p}!south/b.csv"]
    assert "notes" not in good.columns  # the README was not read as a CSV


def test_zip_archive_member_selects_files_and_a_miss_is_a_clear_error(tmp_path):
    p = tmp_path / "bundle.zip"
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("a.csv", "order_id,customer,amount\n1,Ada,5\n")
        zf.writestr("b.csv", "order_id,customer,amount\n2,Bo,6\n")
    good, _ = _run(p, format="csv", options={"archive_member": "b*.csv"})
    assert good["order_id"].to_list() == [2]
    with pytest.raises(ValueError, match="No files in .* match archive_member='x.csv'"):
        _run(p, format="csv", options={"archive_member": "x.csv"})


def test_zip_member_cannot_escape_the_temp_folder(tmp_path):
    from lakelogic.core.processor import _decompress_local

    p = tmp_path / "evil.zip"
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("../../escape.csv", "a\n1\n")
    paths, origin = _decompress_local([str(p)], fmt="csv")
    assert os.path.dirname(paths[0]) != str(tmp_path) and paths[0].endswith("escape.csv")
    assert not (tmp_path.parent / "escape.csv").exists()


# ── CSV dialects ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("sep", ["\\t", "|", ";"])
def test_csv_delimiters(tmp_path, sep):
    real = "\t" if sep == "\\t" else sep
    p = tmp_path / "o.csv"
    p.write_text(real.join(["order_id", "customer", "amount"]) + "\n" + real.join(["1", "Ada", "5"]) + "\n")
    good, _ = _run(p, format="csv", options={"delimiter": sep})
    assert good.select(["order_id", "customer", "amount"]).rows() == [(1, "Ada", 5.0)]


def test_european_csv_latin1_title_line_decimal_comma_and_multiline_quotes(tmp_path):
    p = tmp_path / "eu.csv"
    p.write_bytes((
        "Export - Octobre\n"
        "order_id;customer;amount;note\n"
        '1;François;1.234,50;"a, b"\n'
        '2;Zoë;7,25;"line one\nline two"\n'
        "3;Jürgen;-2,00;\n"
    ).encode("latin-1"))
    model = {"fields": ORDERS["fields"] + [{"name": "note", "type": "string"}]}
    good, bad = _run(p, model=model, format="csv",
                     options={"delimiter": ";", "encoding": "latin-1", "skip_rows": 1, "decimal_comma": True})
    assert good.select(["customer", "amount", "note"]).rows() == [
        ("François", 1234.5, "a, b"),  # the comma in a text field is left alone
        ("Zoë", 7.25, "line one\nline two"),
    ]
    assert bad["order_id"].to_list() == [3]


def test_a_non_utf8_encoding_cannot_be_streamed():
    from lakelogic.core.processor import _csv_read_kwargs

    assert _csv_read_kwargs({"encoding": "latin-1"}) == {"encoding": "latin-1"}
    assert _csv_read_kwargs({"encoding": "UTF-8"}) == {}
    with pytest.raises(ValueError, match="needs an in-memory read"):
        _csv_read_kwargs({"encoding": "latin-1"}, lazy=True)


# ── Avro ──────────────────────────────────────────────────────────────────────

def test_avro_keeps_types_and_nested_fields_flatten_like_json(tmp_path):
    p = tmp_path / "events.avro"
    pl.DataFrame({
        "order_id": [1, 2],
        "customer": [{"name": "Ada", "tier": "gold"}, {"name": None, "tier": "silver"}],
        "items": [[{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}], [{"sku": "C", "qty": 1}]],
    }).write_avro(p)
    contract = {"version": "1.0", "info": {"title": "a"},
                "source": {"type": "landing", "path": str(p), "format": "avro", "flatten_nested": True},
                "transformations": [{"explode": {"field": "items", "output": "item"}},
                                    {"json_extract": {"field": "sku", "source": "item", "path": "$.sku"}}],
                "model": {"fields": [{"name": "order_id", "type": "long", "required": True},
                                     {"name": "customer_name", "type": "string", "required": True},
                                     {"name": "sku", "type": "string", "required": True}]}}
    good, bad = DataProcessor(engine="polars", contract=contract).run_source()
    assert good.select(["order_id", "customer_name", "sku"]).rows() == [(1, "Ada", "A"), (1, "Ada", "B")]
    # Order 2 fails a PRE check (no customer name), so it is quarantined before the post `explode`,
    # in the shape it arrived in — on every engine (lakelogic.core.rule_phases).
    assert bad["order_id"].to_list() == [2] and "items" in bad.columns


# ── Excel: .xls and dates ─────────────────────────────────────────────────────

DATED = {"fields": [{"name": "order_id", "type": "long", "required": True},
                    {"name": "booked_on", "type": "date", "required": True},
                    {"name": "amount", "type": "double"}]}


def test_xls_reads_a_named_sheet_with_header_on_row_2_and_real_dates():
    pytest.importorskip("xlrd")
    good, bad = _run(EXAMPLES / "orders_legacy.xls", model=DATED, format="xls",
                     options={"sheet_name": "Ledger", "header_row": 2})
    assert good.select(["order_id", "booked_on", "amount"]).rows() == [(12001, dt.date(2026, 10, 1), 15.0)]
    assert bad["order_id"].to_list() == [12002]


def test_xlsx_date_cells_cast_to_a_date_field(tmp_path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["order_id", "booked_on", "amount"])
    ws.append([1, dt.datetime(2026, 10, 1), 3.5])
    p = tmp_path / "d.xlsx"
    wb.save(p)
    good, bad = _run(p, model=DATED, format="xlsx")
    assert good["booked_on"].to_list() == [dt.date(2026, 10, 1)]
    assert len(bad) == 0


# ── Bugs the scenario suite found (2026-10-07) ────────────────────────────────

def test_a_csv_file_not_named_dot_csv_is_read_not_treated_as_a_folder(tmp_path):
    p = tmp_path / "orders.tsv"
    p.write_text("order_id\tcustomer\tamount\n1\tAda\t5\n")
    good, _ = _run(p, format="csv", options={"delimiter": "\t"})
    assert good["order_id"].to_list() == [1]


XML_ITEMS = {
    "transformations": [{"explode": {"field": "items", "output": "item"}},
                        {"json_extract": {"field": "sku", "source": "item", "path": "$.sku"}}],
    "model": {"fields": [{"name": "id", "type": "long", "required": True},
                         {"name": "sku", "type": "string", "required": True}]},
}


def _xml(p, **source):
    contract = {"version": "1.0", "info": {"title": "x"},
                "source": {"type": "landing", "path": str(p), "format": "xml", "flatten_nested": True, **source},
                **XML_ITEMS}
    return DataProcessor(engine="polars", contract=contract).run_source()


def test_xml_file_with_a_single_record_is_one_row(tmp_path):
    p = tmp_path / "o.xml"
    p.write_text('<ns:orders xmlns:ns="urn:x"><ns:order id="1"><ns:items><ns:item><ns:sku>A</ns:sku>'
                 '</ns:item></ns:items></ns:order></ns:orders>')
    good, bad = _xml(p)
    assert good.select(["id", "sku"]).rows() == [(1, "A")] and len(bad) == 0


def test_xml_one_item_in_every_record_is_still_a_list(tmp_path):
    p = tmp_path / "o.xml"
    p.write_text('<orders><order id="1"><items><item><sku>A</sku></item></items></order>'
                 '<order id="2"><items><item><sku>B</sku></item></items></order></orders>')
    good, _ = _xml(p)
    assert good.select(["id", "sku"]).rows() == [(1, "A"), (2, "B")]


def test_xml_row_tag_picks_records_at_any_depth(tmp_path):
    p = tmp_path / "o.xml"
    p.write_text('<feed><meta><x>1</x></meta><batch><order id="1"><items><item><sku>A</sku></item></items></order>'
                 '</batch><batch><order id="2"><items><item><sku>B</sku></item></items></order></batch></feed>')
    good, _ = _xml(p, options={"row_tag": "order"})
    assert sorted(good["sku"].to_list()) == ["A", "B"]


# ── Warnings: only the real ones (2026-10-07) ─────────────────────────────────

@pytest.fixture
def warnings_seen():
    from loguru import logger

    seen: list = []
    sink = logger.add(lambda m: seen.append(m.record["message"]), level="WARNING")
    yield seen
    logger.remove(sink)


def test_nested_steps_and_fixed_width_settings_raise_no_false_warnings(tmp_path, warnings_seen):
    p = tmp_path / "o.xml"
    p.write_text('<orders><order id="1"><items><item><sku>A</sku></item></items></order></orders>')
    _xml(p)
    f = tmp_path / "s.dat"
    f.write_text("D20157500001050\n")
    contract = {"version": "1.0", "info": {"title": "bacs"},
                "source": {"type": "landing", "path": str(f), "format": "fixed_width", "record_length": 15,
                           "encoding": "ascii", "skip_rows": 0, "skip_footer": 0},
                "model": {"fields": [{"name": "sort_code", "type": "string"}]}}
    contract["model"]["fields"][0]["range"] = [1, 7]
    DataProcessor(engine="polars", contract=contract).run_source()
    assert not [w for w in warnings_seen if "Schema drift" in w or "Unknown key" in w], warnings_seen


def test_real_drift_is_still_reported(tmp_path, warnings_seen):
    p = tmp_path / "o.csv"
    p.write_text("order_id,customer,amount,coupon\n1,Ada,5,X\n")
    _run(p, format="csv")
    assert any("unknown=['coupon']" in w for w in warnings_seen)


def test_archive_files_with_different_columns_warn(tmp_path, warnings_seen):
    p = tmp_path / "b.zip"
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("2026-09/o.csv", "order_id,customer,amount\n1,Ada,5\n")
        zf.writestr("2026-10/o.csv", "customer,order_id\nBo,2\n")
    _run(p, format="csv")
    warn = next(w for w in warnings_seen if "do not share one structure" in w)
    assert "2026-10/o.csv lacks ['amount']" in warn


def test_archive_files_differing_only_in_column_order_do_not_warn(tmp_path, warnings_seen):
    p = tmp_path / "b.zip"
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("a.csv", "order_id,customer,amount\n1,Ada,5\n")
        zf.writestr("b.csv", "amount,customer,order_id\n6,Bo,2\n")
    _run(p, format="csv")
    assert not [w for w in warnings_seen if "structure" in w]


def test_running_a_contract_keeps_the_host_applications_log_handlers(tmp_path, warnings_seen):
    p = tmp_path / "o.csv"
    p.write_text("order_id,customer,amount,coupon\n1,Ada,5,X\n")
    _run(p, format="csv")
    _run(p, format="csv")  # a second DataProcessor used to remove every handler, this sink included
    assert sum("unknown=['coupon']" in w for w in warnings_seen) == 2


# ── Every engine, one answer (2026-10-07) ─────────────────────────────────────

@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_implied_decimals_and_date_formats_flag_what_does_not_fit(tmp_path, engine):
    p = tmp_path / "s.dat"
    p.write_text("00001050 20261007\n-0000250 20261399\n0000X050 20261008\n")
    contract = {"version": "1.0", "info": {"title": "fw"},
                "source": {"type": "landing", "path": str(p), "format": "fixed_width",
                           "options": {"implied_decimals": {"amount": 2}, "date_formats": {"booked": "yyyyMMdd"}}},
                "model": {"fields": [{"name": "amount", "range": [0, 8], "type": "double", "required": True},
                                     {"name": "booked", "range": [9, 17], "type": "date", "required": True}]}}
    good, bad = DataProcessor(engine=engine, contract=contract).run_source()
    good = good if isinstance(good, pl.DataFrame) else good.pl()
    bad = bad if isinstance(bad, pl.DataFrame) else bad.pl()
    assert good.select(["amount", "booked"]).rows() == [(10.5, dt.date(2026, 10, 7))]
    reasons = sorted(e for row in bad["_lakelogic_errors"].to_list() for e in row)
    assert any("booked cannot be cast" in r for r in reasons)  # month 13: flagged, not nulled
    assert any("amount cannot be cast" in r for r in reasons)  # 0000X050 left as is, then quarantined


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_explode_keeps_a_row_whose_list_is_empty(tmp_path, engine):
    p = tmp_path / "o.json"
    p.write_text('[{"order_id": 1, "items": [{"sku": "A"}]}, {"order_id": 2, "items": []}]')
    contract = {"version": "1.0", "info": {"title": "x"},
                "source": {"type": "landing", "path": str(p), "format": "json", "flatten_nested": True},
                "transformations": [{"explode": {"field": "items", "output": "item"}},
                                    {"json_extract": {"field": "sku", "source": "item", "path": "$.sku"}}],
                "model": {"fields": [{"name": "order_id", "type": "long", "required": True},
                                     {"name": "sku", "type": "string", "required": True}]}}
    good, bad = DataProcessor(engine=engine, contract=contract).run_source()
    assert len(good) == 1 and len(bad) == 1  # order 2 is quarantined, not silently dropped


def test_a_cloud_zip_is_fetched_then_read(tmp_path, monkeypatch):
    fsspec = pytest.importorskip("fsspec")
    buf = __import__("io").BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a.csv", "order_id,customer,amount\n1,Ada,5\n")
    with fsspec.open("memory://landing/orders.zip", "wb") as fh:
        fh.write(buf.getvalue())
    proc = DataProcessor(engine="polars", contract={
        "version": "1.0", "info": {"title": "z"},
        "source": {"type": "landing", "path": "memory://landing/orders.zip", "format": "csv"}, "model": ORDERS})
    monkeypatch.setattr(proc, "_is_uri_path", lambda p: str(p).startswith("memory://"))
    monkeypatch.setattr(proc, "_get_cloud_storage_options", lambda p: {})
    monkeypatch.setattr(proc, "_expand_source_files", lambda p: [{"path": "memory://landing/orders.zip", "mtime": 1.0}])
    good, _ = proc.run_source()
    assert good["order_id"].to_list() == [1]
