"""Database ingestion, found against real Azure SQL and Azure PostgreSQL (2026-10-07).

1. `load_mode: incremental` never advanced: the database reader never recorded the highest
   watermark value, and the run-log lookup skipped every run logged without a status
   (`status != 'failed'` is NULL). Every "incremental" run re-read the whole table.
2. A watermark from a timestamp WITHOUT a time zone was read as local time.
3. DuckDB could not read any database (`postgres_scan(uri, table)` no longer exists) and ignored
   source.query / projection / the incremental filter.
4. A bare `mssql://` URI gave SQLAlchemy no ODBC driver, so chunked (`fetch_size`) reads failed.
"""

import datetime as dt
import sqlite3

import polars as pl
import pytest

from lakelogic import DataProcessor
from lakelogic.core.processor import _sqlalchemy_db_uri
from lakelogic.core.run_log import get_last_run_watermark, write_run_log

pytest.importorskip("duckdb")


def _contract(db_path, run_log_db, **source):
    return {
        "version": "1.0.0",
        "dataset": "rides",
        "info": {"title": "rides"},
        "source": {"type": "database", "path": f"sqlite:///{db_path.as_posix()}", **source},
        "model": {
            "fields": [
                {"name": "ride_id", "type": "long", "required": True},
                {"name": "status", "type": "string"},
                {"name": "updated_at", "type": "timestamp"},
            ]
        },
        "metadata": {"run_log_table": "run_log", "run_log_backend": "duckdb", "run_log_database": str(run_log_db)},
    }


def _run(contract):
    proc = DataProcessor(engine="duckdb", contract=contract)
    good, bad = proc.run_source()[:2]
    write_run_log(proc.last_report, proc.contract, engine_name="duckdb")  # what the pipeline runner does
    return (good if isinstance(good, pl.DataFrame) else good.pl()).height


def test_incremental_database_load_reads_only_new_and_changed_rows(tmp_path):
    db = tmp_path / "src.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE rides (ride_id INTEGER PRIMARY KEY, status TEXT, updated_at TEXT)")
    con.executemany("INSERT INTO rides VALUES (?, ?, ?)", [(i, "completed", "2026-10-01 09:00:00") for i in range(20)])
    con.commit()
    c = _contract(db, tmp_path / "log.duckdb", load_mode="incremental", watermark_field="updated_at")
    assert _run(c) == 20
    con.executemany(
        "INSERT INTO rides VALUES (?, ?, ?)", [(100 + i, "completed", "2026-10-01 10:00:00") for i in range(5)]
    )
    con.execute("UPDATE rides SET status = 'refunded', updated_at = '2026-10-01 10:01:00' WHERE ride_id < 3")
    con.commit()
    assert _run(c) == 8  # 5 new + 3 changed, not the whole table
    assert _run(c) == 0
    con.close()


def test_run_log_lookup_counts_a_run_logged_without_a_status(tmp_path):
    import duckdb

    db = tmp_path / "log.duckdb"
    con = duckdb.connect(str(db))
    con.execute(
        "CREATE TABLE run_log (dataset VARCHAR, contract VARCHAR, stage VARCHAR, status VARCHAR, "
        "data_layer VARCHAR, max_source_mtime DOUBLE)"
    )
    con.execute("INSERT INTO run_log VALUES ('rides', 'rides', 'default', NULL, NULL, 42.0)")
    con.close()
    contract = type(
        "C",
        (),
        {
            "metadata": {"run_log_table": "run_log", "run_log_backend": "duckdb", "run_log_database": str(db)},
            "_base_path": None,
        },
    )()
    assert get_last_run_watermark(contract, "rides", "default", dataset="rides") == 42.0


def test_a_timestamp_without_a_time_zone_is_a_utc_watermark():
    proc = object.__new__(DataProcessor)
    proc._source_max_mtime = None
    proc.contract = type("C", (), {"source": type("S", (), {"watermark_field": "t"})()})()
    proc._capture_db_watermark(pl.DataFrame({"t": [dt.datetime(2026, 10, 1, 9, 0), dt.datetime(2026, 10, 1, 8, 0)]}))
    assert proc._source_max_mtime == dt.datetime(2026, 10, 1, 9, 0, tzinfo=dt.timezone.utc).timestamp()
    proc._capture_db_watermark(pl.DataFrame({"t": [dt.datetime(2026, 9, 1)]}))  # an earlier chunk
    assert proc._source_max_mtime == dt.datetime(2026, 10, 1, 9, 0, tzinfo=dt.timezone.utc).timestamp()


def test_a_bare_mssql_uri_gets_an_odbc_driver_for_sqlalchemy():
    out = _sqlalchemy_db_uri("mssql://u:p%21@h.database.windows.net:1433/db?encrypt=true")
    assert out.startswith("mssql+pyodbc://u:p%21@h.database.windows.net:1433/db?")
    assert "driver=ODBC+Driver" in out and "Encrypt=yes" in out and "encrypt=true" not in out
    assert _sqlalchemy_db_uri("postgresql://u:p@h/db") == "postgresql://u:p@h/db"


# ── Native SQL Server CDC (found against Azure SQL S3, 2026-10-07) ─────────────


def _cdc_proc(monkeypatch, row):
    """A processor whose LSN-range query returns ``row`` (no SQL Server needed)."""
    monkeypatch.setattr(pl, "read_database_uri", lambda q, uri: pl.DataFrame([row]))
    return object.__new__(DataProcessor)


def test_cdc_range_starts_at_the_capture_minimum_when_the_watermark_is_older(monkeypatch):
    p = _cdc_proc(monkeypatch, {"min_lsn": "0x00000050", "max_lsn": "0x00000090", "from_lsn": "0x00000010"})
    # The watermark maps to an LSN BEFORE this capture instance existed: SQL Server would refuse
    # that range, so it is clamped to the capture's own minimum.
    assert p._sqlserver_cdc_lsn_range("mssql://x", "dbo_rides", "2026-10-01 09:00:00.000000") == (
        "0x00000050",
        "0x00000090",
    )


def test_cdc_range_is_none_when_nothing_changed_after_the_watermark(monkeypatch):
    p = _cdc_proc(monkeypatch, {"min_lsn": "0x00000050", "max_lsn": "0x00000090", "from_lsn": None})
    assert p._sqlserver_cdc_lsn_range("mssql://x", "dbo_rides", "2026-10-07 21:00:00.000000") is None


def test_the_cdc_query_carries_the_commit_time_used_as_the_watermark():
    p = object.__new__(DataProcessor)
    q = p._build_cdc_query("azuresql", "rides", '"ride_id"', "dbo_rides", lsn_range=("0x01", "0x02"))
    assert "fn_cdc_get_all_changes_dbo_rides(0x01, 0x02, 'all')" in q
    assert "fn_cdc_map_lsn_to_time(__$start_lsn) AS _lakelogic_cdc_ts" in q


def test_a_native_cdc_contract_needs_no_op_field_of_its_own():
    p = DataProcessor(
        engine="polars",
        contract={
            "version": "1.0.0",
            "dataset": "r",
            "info": {"title": "r"},
            "source": {
                "type": "database",
                "path": "mssql://u:p@h/db",
                "load_mode": "cdc",
                "options": {"cdc_provider": "azuresql"},
            },
            "model": {"fields": [{"name": "ride_id", "type": "long"}]},
        },
    )
    assert p.contract.source.cdc_op_field == "_lakelogic_cdc_op"
    assert p.contract.source.cdc_delete_values == ["delete"]


def test_polars_casts_space_separated_timestamps_instead_of_quarantining_them():
    # "2026-10-01 09:00:00" (SQL/Excel/MongoDB text) became NULL on Polars and was quarantined.
    p = DataProcessor(
        engine="polars",
        contract={
            "version": "1.0.0",
            "dataset": "r",
            "info": {"title": "r"},
            "model": {"fields": [{"name": "t", "type": "timestamp"}, {"name": "d", "type": "date"}]},
        },
    )
    src = pl.DataFrame(
        {
            "t": ["2026-10-01 09:00:00", "2026-10-01T09:00:00.5Z", "2026-10-01", "nope"],
            "d": ["2026-10-01", "2026-10-01 09:00:00", "2026-10-01", "2026-10-01"],
        }
    )
    good, bad = p.run(src)[:2]
    assert good.height == 3 and bad.height == 1
    assert good["t"][0] == dt.datetime(2026, 10, 1, 9, 0)
    assert good["d"][1] == dt.date(2026, 10, 1)
