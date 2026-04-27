from __future__ import annotations

import builtins
import sqlite3
import sys
import types
from datetime import datetime

from lakelogic.cli.run_log_reader import RunLogReader
from lakelogic.core.models import DataContract, Info


def _contract(metadata=None, title="Orders", dataset="orders", base_path=None):
    contract = DataContract(version="1.0.0", dataset=dataset, info=Info(title=title), metadata=metadata or {})
    if base_path is not None:
        setattr(contract, "_base_path", base_path)
    return contract


def test_run_log_reader_shortcuts_and_helpers(tmp_path):
    reader = RunLogReader("duckdb")
    contract = _contract()
    assert reader.last_success(contract) is None
    assert reader.last_success_info(contract) == (None, "no_run_log_table")
    assert RunLogReader("polars").last_success_info(_contract({"run_log_table": "tbl", "run_log_backend": "other"})) == (
        None,
        "unsupported_backend",
    )
    assert reader._contract_key(contract) == "Orders"
    assert reader._contract_key(DataContract(version="1.0.0", dataset="orders")) == "orders"
    assert reader._contract_key(DataContract(version="1.0.0")) == "unknown"
    assert reader._parse_timestamp(datetime(2026, 1, 1)) == datetime(2026, 1, 1)
    assert reader._parse_timestamp("2026-01-01T00:00:00") == datetime(2026, 1, 1, 0, 0)
    assert reader._parse_timestamp("bad") is None
    assert reader._resolve_path("logs/run.db", tmp_path) == tmp_path / "logs/run.db"

    spark_reader = RunLogReader("spark")
    spark_contract = _contract({"run_log_table": "run_log", "run_log_backend": "spark"})
    spark_reader._read_spark = lambda table, contract: (datetime(2026, 1, 1), None)
    assert spark_reader.last_success_info(spark_contract) == (datetime(2026, 1, 1), None)


def test_run_log_reader_duckdb_paths(monkeypatch, tmp_path):
    reader = RunLogReader("duckdb")
    contract = _contract({"run_log_table": "run_log"}, base_path=tmp_path)
    assert reader.last_success_info(contract) == (None, "run_log_db_missing")

    original_import = builtins.__import__
    db_path = tmp_path / "logs" / "lakelogic_run_logs.duckdb"
    db_path.parent.mkdir(parents=True)
    db_path.write_text("placeholder", encoding="utf-8")

    class FakeConnection:
        def __init__(self, result=None, raises=False):
            self.result = result
            self.raises = raises

        def execute(self, query, params):
            if self.raises:
                raise RuntimeError("missing table")
            return types.SimpleNamespace(fetchone=lambda: self.result)

        def close(self):
            return None

    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(connect=lambda database: FakeConnection(raises=True)))
    assert reader.last_success_info(contract) == (None, "run_log_table_missing")

    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(connect=lambda database: FakeConnection(result=(None,))))
    assert reader.last_success_info(contract) == (None, "run_log_entry_missing")

    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(connect=lambda database: FakeConnection(result=("2026-01-02T03:04:05",))))
    ts, reason = reader.last_success_info(contract)
    assert ts == datetime(2026, 1, 2, 3, 4, 5)
    assert reason is None

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "duckdb":
            raise ImportError("missing")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    assert reader._read_duckdb("run_log", contract, {"run_log_database": str(db_path)}) == (None, "duckdb_unavailable")


def test_run_log_reader_sqlite_and_spark_paths(monkeypatch, tmp_path):
    reader = RunLogReader("spark")
    contract = _contract({"run_log_table": "run_log"}, base_path=tmp_path)
    original_import = builtins.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "pyspark.sql":
            raise ImportError("missing")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    assert reader._read_spark("run_log", contract) == (None, "spark_unavailable")
    monkeypatch.setattr(builtins, "__import__", original_import)

    class FakeSparkSession:
        builder = types.SimpleNamespace(getOrCreate=lambda: types.SimpleNamespace(
            catalog=types.SimpleNamespace(tableExists=lambda name: True),
            sql=lambda query: types.SimpleNamespace(collect=lambda: [{"last_ts": "2026-02-03T04:05:06"}]),
        ))

    fake_pyspark = types.ModuleType("pyspark")
    fake_pyspark_sql = types.ModuleType("pyspark.sql")
    fake_pyspark_sql.SparkSession = FakeSparkSession
    fake_pyspark.sql = fake_pyspark_sql
    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_pyspark_sql)
    ts, reason = reader._read_spark("run_log", contract)
    assert ts == datetime(2026, 2, 3, 4, 5, 6)
    assert reason is None

    missing_table_spark = types.SimpleNamespace(
        builder=types.SimpleNamespace(getOrCreate=lambda: types.SimpleNamespace(catalog=types.SimpleNamespace(tableExists=lambda name: False)))
    )
    fake_pyspark_sql.SparkSession = missing_table_spark
    assert reader._read_spark("run_log", contract) == (None, "run_log_table_missing")

    empty_rows_spark = types.SimpleNamespace(
        builder=types.SimpleNamespace(getOrCreate=lambda: types.SimpleNamespace(
            catalog=types.SimpleNamespace(tableExists=lambda name: True),
            sql=lambda query: types.SimpleNamespace(collect=lambda: []),
        ))
    )
    fake_pyspark_sql.SparkSession = empty_rows_spark
    assert reader._read_spark("run_log", contract) == (None, "run_log_entry_missing")

    null_value_spark = types.SimpleNamespace(
        builder=types.SimpleNamespace(getOrCreate=lambda: types.SimpleNamespace(
            catalog=types.SimpleNamespace(tableExists=lambda name: True),
            sql=lambda query: types.SimpleNamespace(collect=lambda: [{"last_ts": None}]),
        ))
    )
    fake_pyspark_sql.SparkSession = null_value_spark
    assert reader._read_spark("run_log", contract) == (None, "run_log_entry_missing")

    sqlite_reader = RunLogReader("duckdb")
    sqlite_contract = _contract({"run_log_table": "run_log", "run_log_backend": "sqlite"}, base_path=tmp_path)
    assert sqlite_reader.last_success_info(sqlite_contract) == (None, "run_log_db_missing")

    db_path = tmp_path / "logs" / "lakelogic_run_logs.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE run_log (contract TEXT, timestamp TEXT)")
    conn.execute("INSERT INTO run_log (contract, timestamp) VALUES (?, ?)", ("Orders", "2026-03-04T05:06:07"))
    conn.commit()
    conn.close()

    ts, reason = sqlite_reader.last_success_info(sqlite_contract)
    assert ts == datetime(2026, 3, 4, 5, 6, 7)
    assert reason is None

    sqlite_contract.metadata["run_log_table"] = "missing_table"
    assert sqlite_reader.last_success_info(sqlite_contract) == (None, "run_log_table_missing")
    sqlite_contract.metadata["run_log_table"] = "run_log"

    conn = sqlite3.connect(str(db_path))
    conn.execute("DELETE FROM run_log")
    conn.commit()
    conn.close()
    assert sqlite_reader.last_success_info(sqlite_contract) == (None, "run_log_entry_missing")
