"""`read_sample`: one reader for a sample file, with CSV dates parsed."""

import polars as pl
import pytest

from lakelogic import infer_contract, read_sample


def test_csv_dates_are_parsed_and_infer_contract_keeps_them(tmp_path):
    p = tmp_path / "charges.csv"
    p.write_text("charge_id,amount,created\nch_1,10.5,2026-01-02\nch_2,3,2026-01-03\n")

    df = read_sample(str(p))

    assert df.schema["created"] == pl.Date
    assert df.height == 2
    types = {f["name"]: f["type"] for f in infer_contract(df, suggest_rules=False).fields}
    assert types == {"charge_id": "string", "amount": "double", "created": "date"}


def test_infer_contract_on_a_path_still_leaves_csv_dates_as_text(tmp_path):
    p = tmp_path / "charges.csv"
    p.write_text("created\n2026-01-02\n")

    types = {f["name"]: f["type"] for f in infer_contract(str(p), suggest_rules=False).fields}

    assert types == {"created": "string"}


def test_max_rows_caps_the_read(tmp_path):
    p = tmp_path / "n.csv"
    p.write_text("n\n" + "\n".join(str(i) for i in range(50)) + "\n")

    assert read_sample(str(p), max_rows=10).height == 10


def test_unknown_extension_names_it(tmp_path):
    p = tmp_path / "notes.txt"
    p.write_text("hello")

    with pytest.raises(ValueError, match=r"\.txt"):
        read_sample(str(p))


def test_a_sql_script_gives_its_table_and_rows(tmp_path):
    p = tmp_path / "customers.sql"
    p.write_text(
        "CREATE TABLE customers (id INTEGER, name VARCHAR, joined DATE);\n"
        "INSERT INTO customers VALUES (1, 'Ada', '2026-01-02'), (2, 'Bo', NULL);\n"
    )

    df = read_sample(str(p))

    assert df.columns == ["id", "name", "joined"]
    assert df.height == 2
    assert df.schema["joined"] == pl.Date


def test_a_schema_only_script_reads_as_an_empty_table(tmp_path):
    p = tmp_path / "orders.sql"
    p.write_text("CREATE TABLE orders (order_id VARCHAR, total DOUBLE);")

    df = read_sample(str(p))

    assert df.columns == ["order_id", "total"] and df.height == 0


@pytest.mark.parametrize("script,says", [
    ("CREATE TABLE a (x INT); CREATE TABLE b (y INT);", "exactly one table"),
    ("SELECT * FROM read_csv('/etc/passwd');", "only hold CREATE TABLE and INSERT"),
    ("CREATE TABLE a AS SELECT * FROM read_csv('/etc/passwd');", r"(?i)external access|disabled"),
    ("COPY (SELECT 1) TO 'x.csv';", "only hold CREATE TABLE and INSERT"),
])
def test_a_sql_script_cannot_reach_outside(tmp_path, script, says):
    p = tmp_path / "x.sql"
    p.write_text(script)

    with pytest.raises(Exception, match=says):
        read_sample(str(p))


def test_excel_and_xml_are_profiled(tmp_path):
    from lakelogic import profile

    pd = pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    p = tmp_path / "orders.xlsx"
    pd.DataFrame({"order_id": ["o1", None, "o3"], "total": [1.5, 2.0, 3.0]}).to_excel(p, index=False)

    doc = profile(str(p))

    assert doc["row_count"] == 3
    assert {c["name"]: c.get("null_count") for c in doc["columns"]} == {"order_id": 1, "total": 0}


def test_a_zip_unpacks_into_its_readable_files(tmp_path):
    import zipfile

    from lakelogic import sample_files

    z = tmp_path / "upload.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("data/customers.csv", "id,name\n1,Ada\n")
        zf.writestr("data/orders.json", '[{"order_id": "o1"}]')
        zf.writestr("docs/invoice_1.pdf", b"%PDF-1.4")
        zf.writestr("__MACOSX/._customers.csv", "junk")
        zf.writestr("notes.txt", "not a sample")
        zf.writestr("../escape.csv", "a\n1\n")

    names = sorted(p.name for p in sample_files(z, tmp_path / "out"))

    assert names == ["customers.csv", "escape.csv", "invoice_1.pdf", "orders.json"]
    assert all((tmp_path / "out" / n).exists() for n in names)
    assert not (tmp_path / "escape.csv").exists()


def test_a_zip_over_its_limits_is_refused_unread(tmp_path):
    import zipfile

    from lakelogic import sample_files

    z = tmp_path / "big.zip"
    with zipfile.ZipFile(z, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("a.csv", "x\n" + "1\n" * 600_000)

    with pytest.raises(ValueError, match="MB"):
        sample_files(z, tmp_path / "out", max_bytes=1024 * 1024)
    assert not (tmp_path / "out").exists()
