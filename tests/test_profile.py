"""Tests for lakelogic.core.profile -- the one-document data profiler."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import jsonschema
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import sqlglot
from sqlglot import exp
from typer.testing import CliRunner

import lakelogic
from lakelogic.core import profile as P


# ── helpers ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def con():
    c = duckdb.connect()
    c.execute(
        """
        CREATE TABLE main.trips AS SELECT * FROM (VALUES
          (1, 'alice@example.com', 'NYC', 10.5, TIMESTAMP '2024-01-01 10:00:00', [1,2]),
          (2, 'bob@example.com',   NULL,  20.0, TIMESTAMP '2024-03-05 08:00:00', [3]),
          (3, NULL,                'SF',  NULL, TIMESTAMP '2024-02-01 00:00:00', NULL),
          (4, 'dan@example.com',   'NYC', 5.0,  NULL,                            [4])
        ) t(trip_id, email, city_code, fare, updated_at, tags)
        """
    )
    return c


def _assert_aggregate_only(sql: str, dialect: str) -> None:
    tree = sqlglot.parse_one(sql, read=dialect)
    assert isinstance(tree, exp.Select)
    assert tree.args.get("limit") is None
    assert tree.args.get("group") is None
    assert tree.args.get("where") is None
    assert not list(tree.find_all(exp.Star)) or all(isinstance(s.parent, exp.Count) for s in tree.find_all(exp.Star))
    for item in tree.expressions:
        node = item.this if isinstance(item, exp.Alias) else item
        assert isinstance(node, exp.AggFunc) or node.find(exp.AggFunc) is node, (
            f"non-aggregate select item: {item.sql()}"
        )


def _cols(doc):
    return {c["name"]: c for c in doc["columns"]}


# ── Backend A: table pushdown ────────────────────────────────────────────────


def test_table_pushdown_profiles_with_one_aggregate_query(con):
    doc = lakelogic.profile("main.trips", connection=con)
    assert doc["row_count"] == 4
    assert doc["sampling"] == {"method": "full", "rows_scanned": 4, "files_scanned": None, "bytes": None}
    assert doc["pushdown"]["rows_returned"] == 1
    _assert_aggregate_only(doc["pushdown"]["sql"], "duckdb")
    c = _cols(doc)
    assert c["fare"]["null_count"] == 1 and c["fare"]["null_pct"] == 25.0
    assert c["fare"]["min"] == 5.0 and c["fare"]["max"] == 20.0
    assert c["city_code"]["distinct_approx"] == 2
    assert doc["freshness"] == {"column": "updated_at", "value": "2024-03-05T08:00:00"}


def test_pushdown_never_fetches_rows(con, monkeypatch):
    """Every SQL statement the profiler runs must be DESCRIBE or one aggregate."""
    seen = []
    real = P.DuckDBExecutor.query

    def spy(self, sql):
        seen.append(sql)
        names, rows = real(self, sql)
        assert len(rows) == 1
        return names, rows

    monkeypatch.setattr(P.DuckDBExecutor, "query", spy)
    P.profile_table("main.trips", con)
    assert len(seen) == 1
    _assert_aggregate_only(seen[0], "duckdb")


def test_aggregate_row_rejects_multi_row_results(con):
    ex = P.DuckDBExecutor(con)
    with pytest.raises(P.AggregateRowError):
        ex.aggregate_row("SELECT * FROM main.trips")


def test_complex_columns_get_counts_only(con):
    doc = lakelogic.profile("main.trips", connection=con)
    tags = _cols(doc)["tags"]
    assert tags["null_count"] == 1
    assert tags["min"] is None and tags["distinct_approx"] is None and tags["len_min"] is None


@pytest.mark.parametrize("dialect", ["databricks", "snowflake", "postgres", "tsql", "duckdb"])
def test_generated_sql_is_aggregate_only_per_dialect(dialect):
    schema = [("trip id", "string"), ("fare", "decimal(10,2)"), ("ts", "timestamp"), ("attrs", "struct<a:int>")]
    plan = P.build_profile_sql("cat.sch.trips", schema, dialect)
    read = {"databricks": "databricks", "tsql": "tsql"}.get(dialect, dialect)
    _assert_aggregate_only(plan.sql, read)


def test_databricks_sql_shape():
    plan = P.build_profile_sql(
        "main.rides.trips", [("rider email", "string"), ("fare", "double")], "databricks", sensitive={"rider email"}
    )
    assert "FROM `main`.`rides`.`trips`" in plan.sql
    assert "APPROX_COUNT_DISTINCT(`fare`)" in plan.sql
    assert "LENGTH(CAST(`rider email` AS STRING))" in plan.sql
    assert "MIN(`rider email`)" not in plan.sql and "MAX(`rider email`)" not in plan.sql


def test_postgres_uses_exact_distinct():
    plan = P.build_profile_sql("public.t", [("a", "integer")], "postgres")
    assert 'COUNT(DISTINCT "a")' in plan.sql


def test_databricks_executor_parses_statement_api(monkeypatch):
    calls = []

    class R:
        def __init__(self, data):
            self._d = data

        def raise_for_status(self):
            pass

        def json(self):
            return self._d

    def post(url, json, headers, timeout):
        calls.append(json)
        return R(
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": [{"name": "rc"}]}},
                "result": {"data_array": [["7"]]},
            }
        )

    import requests

    monkeypatch.setattr(requests, "post", post)
    ex = P.DatabricksStatementExecutor(host="h.example", token="t", warehouse_id="w")
    assert ex.aggregate_row("SELECT COUNT(*) AS rc FROM x") == {"rc": "7"}
    assert calls[0]["row_limit"] == 2


# ── Sensitive suppression ────────────────────────────────────────────────────


def test_sensitive_columns_never_select_values(con):
    doc = lakelogic.profile("main.trips", connection=con, sensitive_columns=["city_code"])
    c = _cols(doc)
    for name in ("email", "city_code"):
        assert c[name]["sensitive"] is True
        assert c[name]["min"] is None and c[name]["max"] is None
        assert c[name]["null_count"] is not None
    sql = doc["pushdown"]["sql"]
    assert 'MIN("email")' not in sql and 'MAX("city_code")' not in sql
    assert "alice" not in json.dumps(doc)


def test_contract_classification_marks_sensitive(con, tmp_path):
    ct = tmp_path / "c.yaml"
    ct.write_text("model:\n  fields:\n    - name: fare\n      classification: confidential\n", encoding="utf-8")
    doc = lakelogic.profile("main.trips", connection=con, contract=str(ct))
    assert _cols(doc)["fare"]["sensitive"] is True
    assert _cols(doc)["fare"]["max"] is None


def test_sensitive_never_wins_freshness(con):
    doc = lakelogic.profile("main.trips", connection=con, sensitive_columns=["updated_at"])
    assert doc["freshness"] is None


def test_approx_distinct_capped_at_non_null_count():
    c = P._column_entry("n", "VARCHAR", 600, False, null_count=10, distinct=653)
    assert c["distinct_approx"] == 590


# ── Null-not-zero ────────────────────────────────────────────────────────────


def test_uncomputable_stats_are_null_not_zero(con):
    con.execute("CREATE TABLE main.empty_t (a INTEGER, b VARCHAR)")
    doc = lakelogic.profile("main.empty_t", connection=con)
    assert doc["row_count"] == 0
    for c in doc["columns"]:
        assert c["null_pct"] is None  # no rows -> no percentage, not 0%
        assert c["min"] is None and c["max"] is None
    assert doc["freshness"] is None


# ── Backend B: Parquet / Delta ───────────────────────────────────────────────


def _write_parquet(tmp_path: Path) -> Path:
    d = tmp_path / "pq"
    d.mkdir()
    pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", None, "ccc"], "v": [1.5, None, 3.0]}), d / "p1.parquet")
    pq.write_table(pa.table({"id": [4, 5], "name": ["dd", "e"], "v": [None, 9.0]}), d / "p2.parquet")
    return d


def test_parquet_uses_footer_metadata(tmp_path, monkeypatch):
    d = _write_parquet(tmp_path)
    doc = lakelogic.profile(str(d), fmt="parquet", read_distincts=False, sensitive_columns=["name"])
    assert doc["source"]["kind"] == "parquet"
    assert doc["row_count"] == 5
    assert doc["sampling"]["rows_scanned"] == 0 and doc["sampling"]["files_scanned"] == 2
    c = _cols(doc)
    assert c["id"]["min"] == 1 and c["id"]["max"] == 5
    assert c["v"]["null_count"] == 2 and c["v"]["null_pct"] == 40.0
    assert c["name"]["null_count"] == 1
    assert c["name"]["min"] is None and c["name"]["max"] is None  # footer has "a".."e"; suppressed
    assert c["id"]["distinct_approx"] is None  # not measured without a read
    assert doc["stats_source"]["distinct_length"] == "not measured"


def test_parquet_metadata_does_not_scan_data(tmp_path, monkeypatch):
    d = _write_parquet(tmp_path)

    def guarded(q, *a, **k):
        assert "read_parquet" not in q, "metadata-only profile must not read data"
        return _real_sql(q, *a, **k)

    monkeypatch.setattr(duckdb, "sql", guarded)
    monkeypatch.setattr(P, "_read_stats_with_duckdb", lambda *a, **k: pytest.fail("read happened"))
    doc = P.profile_parquet(str(d), read_distincts=False, sensitive_columns=["name"])
    assert doc["row_count"] == 5


_real_sql = duckdb.sql


def test_parquet_read_only_for_distincts(tmp_path):
    d = _write_parquet(tmp_path)
    doc = lakelogic.profile(str(d), fmt="parquet", sensitive_columns=["name"])
    c = _cols(doc)
    assert c["id"]["distinct_approx"] == 5
    assert c["name"]["len_min"] == 1 and c["name"]["len_max"] == 3
    assert c["v"]["null_count"] == 2  # still from metadata


def test_parquet_without_statistics_is_not_measured(tmp_path):
    d = tmp_path / "nostats"
    d.mkdir()
    pq.write_table(pa.table({"id": [1, 2]}), d / "x.parquet", write_statistics=False)
    doc = P.profile_parquet(str(d), read_distincts=False)
    c = _cols(doc)["id"]
    assert c["null_count"] is None and c["min"] is None and c["null_pct"] is None


def test_delta_uses_log_stats(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    path = tmp_path / "dt"
    deltalake.write_deltalake(str(path), pa.table({"id": [1, 2, 3], "x": ["a", None, "c"]}))
    deltalake.write_deltalake(str(path), pa.table({"id": [9], "x": ["zz"]}), mode="append")
    doc = lakelogic.profile(str(path), read_distincts=False)
    assert doc["source"]["kind"] == "delta"
    assert doc["row_count"] == 4
    c = _cols(doc)
    assert c["id"]["min"] == 1 and c["id"]["max"] == 9
    assert c["x"]["null_count"] == 1


# ── Backend C: CSV / JSON folders + file checks ──────────────────────────────


def _landing(tmp_path: Path) -> Path:
    root = tmp_path / "landing"
    for day, body in [
        ("d_01", "id,status\n1,a\n2,b\n"),
        ("d_02", "id,status\n3,a\n4,c\n"),
        ("d_03", "id,status\n5,a\n"),
    ]:
        (root / day).mkdir(parents=True)
        (root / day / "f.csv").write_text(body, encoding="utf-8")
    return root


def test_csv_folder_samples_newest_files(tmp_path):
    root = _landing(tmp_path)
    doc = lakelogic.profile(str(root), max_files=2)
    s = doc["sampling"]
    assert s["method"] == "sample" and s["files_scanned"] == 2 and s["max_files"] == 2
    assert doc["row_count"] == 3  # d_03 (1) + d_02 (2): the latest partitions
    fc = doc["file_checks"]
    assert fc["file_count"] == 3 and fc["files_sampled"] == 2
    assert fc["newest_file"].replace("\\", "/").endswith("d_03/f.csv")
    assert fc["newest_file_time"].endswith("Z")
    assert fc["header_consistent"] is True and fc["ragged_rows"] == 0 and fc["empty_files"] == []


def test_csv_full_when_everything_read(tmp_path):
    doc = lakelogic.profile(str(_landing(tmp_path)))
    assert doc["sampling"]["method"] == "full" and doc["row_count"] == 5


def test_csv_byte_cap_is_respected(tmp_path):
    root = _landing(tmp_path)
    doc = lakelogic.profile(str(root), max_bytes=30)
    assert doc["sampling"]["bytes"] <= 30
    assert doc["sampling"]["method"] == "sample"


def test_csv_header_inconsistency(tmp_path):
    root = _landing(tmp_path)
    (root / "d_04").mkdir()
    (root / "d_04" / "f.csv").write_text("id,state\n6,x\n", encoding="utf-8")
    fc = lakelogic.profile(str(root))["file_checks"]
    assert fc["header_consistent"] is False
    assert ["id", "state"] in fc["header_variants"]


def test_csv_ragged_rows(tmp_path):
    root = _landing(tmp_path)
    (root / "d_02" / "f.csv").write_text('id,status\n3,a,EXTRA\n4\n"5","multi\nline"\n', encoding="utf-8")
    fc = lakelogic.profile(str(root))["file_checks"]
    assert fc["ragged_rows"] == 2  # quoted multiline row is not ragged


def test_csv_empty_files(tmp_path):
    root = _landing(tmp_path)
    (root / "d_05").mkdir()
    (root / "d_05" / "empty.csv").write_text("", encoding="utf-8")
    (root / "d_05" / "header_only.csv").write_text("id,status\n", encoding="utf-8")
    fc = lakelogic.profile(str(root))["file_checks"]
    assert len(fc["empty_files"]) == 2


def test_csv_encoding_and_delimiter_guess(tmp_path):
    root = tmp_path / "semi"
    root.mkdir()
    (root / "a.csv").write_bytes("id;city\n1;Zürich\n2;Genève\n".encode("cp1252"))
    doc = lakelogic.profile(str(root))
    fc = doc["file_checks"]
    assert fc["delimiters"] == [";"] and fc["encodings"] == ["cp1252"]
    assert doc["row_count"] == 2


def test_json_folder_malformed_lines(tmp_path):
    root = tmp_path / "j"
    root.mkdir()
    (root / "a.json").write_text('{"id": 1, "v": "x"}\n{"id": 2, "v": null}\n', encoding="utf-8")
    (root / "b.json").write_text('{"id": 3}\n{broken\n', encoding="utf-8")
    doc = lakelogic.profile(str(root), fmt="json")
    fc = doc["file_checks"]
    assert fc["malformed_rows"] == 1 and fc["header_consistent"] is None and fc["ragged_rows"] is None


def test_csv_sensitive_detected_by_value(tmp_path):
    root = tmp_path / "pii"
    root.mkdir()
    (root / "a.csv").write_text("id,contact\n1,a@example.com\n2,b@example.com\n", encoding="utf-8")
    c = _cols(lakelogic.profile(str(root)))["contact"]
    assert c["sensitive"] is True and c["min"] is None and c["max"] is None


# ── Document stability ───────────────────────────────────────────────────────


def test_document_matches_schema_and_version(con, tmp_path):
    for doc in (
        lakelogic.profile("main.trips", connection=con),
        lakelogic.profile(str(_landing(tmp_path))),
        lakelogic.profile(str(_write_parquet(tmp_path)), fmt="parquet"),
    ):
        jsonschema.validate(doc, P.PROFILE_JSON_SCHEMA)
        assert doc["profile_version"] == "1.0"
        assert tuple(k for k in doc if k in P.DOCUMENT_KEYS) == P.DOCUMENT_KEYS
        for c in doc["columns"]:
            assert tuple(c) == P.COLUMN_KEYS
        assert doc["profiled_at"].endswith("Z")
        json.dumps(doc)  # serialisable without default=


def test_cli_profile_table_and_folder(tmp_path):
    from lakelogic.cli.main import app

    db = tmp_path / "x.duckdb"
    c = duckdb.connect(str(db))
    c.execute("CREATE TABLE t AS SELECT 1 AS a, 'x' AS b")
    c.close()
    runner = CliRunner()
    r = runner.invoke(app, ["profile", "t", "--engine", "duckdb", "--database", str(db)])
    assert r.exit_code == 0, r.output
    doc = json.loads(r.stdout)
    assert doc["row_count"] == 1 and doc["source"]["kind"] == "table"
    out = tmp_path / "p.json"
    r = runner.invoke(app, ["profile", str(_landing(tmp_path)), "--max-files", "1", "-o", str(out)])
    assert r.exit_code == 0, r.output
    assert json.loads(out.read_text())["sampling"]["files_scanned"] == 1
