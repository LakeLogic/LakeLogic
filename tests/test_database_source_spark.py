"""Tests for Spark JDBC database-source support in DataProcessor.

The URI->JDBC translator is a pure function and is tested directly. The Spark
read path is exercised with a fake ``pyspark`` module (recording reader), so no
live Spark session or JDBC driver jar is required.
"""

from __future__ import annotations

import sys
import types

import pytest

from lakelogic.core import processor as proc_mod
from lakelogic.core.processor import _sqlalchemy_uri_to_jdbc


# ── URI -> JDBC translation ────────────────────────────────────────────────
@pytest.mark.parametrize(
    "uri, url, driver",
    [
        ("sqlite:///demo.db", "jdbc:sqlite:demo.db", "org.sqlite.JDBC"),
        ("postgresql://u:p@host:5432/analytics", "jdbc:postgresql://host:5432/analytics", "org.postgresql.Driver"),
        ("postgresql://u:p@host/analytics", "jdbc:postgresql://host:5432/analytics", "org.postgresql.Driver"),
        ("mysql+pymysql://root:pw@db:3306/orders", "jdbc:mysql://db:3306/orders", "com.mysql.cj.jdbc.Driver"),
        (
            "mssql+pyodbc://sa:pw@sql:1433/warehouse",
            "jdbc:sqlserver://sql:1433;databaseName=warehouse",
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        ),
    ],
)
def test_uri_to_jdbc_mappings(uri, url, driver):
    r = _sqlalchemy_uri_to_jdbc(uri)
    assert r["url"] == url
    assert r["driver"] == driver
    assert r["jar_hint"]  # a maven coordinate hint is always provided


def test_uri_to_jdbc_extracts_credentials():
    r = _sqlalchemy_uri_to_jdbc("postgresql://alice:s3cret@h/db")
    assert r["user"] == "alice"
    assert r["password"] == "s3cret"


def test_uri_to_jdbc_unsupported_dialect_raises():
    with pytest.raises(ValueError, match="No Spark JDBC mapping"):
        _sqlalchemy_uri_to_jdbc("cockroachdb://x@y/z")


# ── Fake pyspark plumbing ──────────────────────────────────────────────────
class _FakeReader:
    def __init__(self, load_error: Exception | None = None):
        self.options: dict = {}
        self._fmt = None
        self._load_error = load_error

    def format(self, fmt):
        self._fmt = fmt
        return self

    def option(self, k, v):
        self.options[k] = v
        return self

    def load(self):
        if self._load_error is not None:
            raise self._load_error
        return types.SimpleNamespace(columns=["id", "total"])


def _install_fake_pyspark(monkeypatch, reader):
    fake_spark = types.SimpleNamespace(read=reader)
    session_cls = types.SimpleNamespace(
        getActiveSession=lambda: fake_spark,
        builder=types.SimpleNamespace(getOrCreate=lambda: fake_spark),
    )
    mod = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")
    sql_mod.SparkSession = session_cls
    mod.sql = sql_mod
    monkeypatch.setitem(sys.modules, "pyspark", mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_mod)


def _make_spark_processor(uri, options=None):
    proc = object.__new__(proc_mod.DataProcessor)
    proc.engine_name = "spark"
    proc.last_source_path = None
    proc.last_run_id = None
    proc.contract = types.SimpleNamespace(
        dataset="massive_orders",
        info=types.SimpleNamespace(title="bronze_massive_orders"),
        source=types.SimpleNamespace(
            path=uri,
            options=options or {},
            watermark_field=None,
            load_mode="full",
            query=None,
        ),
        model=types.SimpleNamespace(fields=[types.SimpleNamespace(name="id"), types.SimpleNamespace(name="total")]),
    )
    return proc


def test_spark_jdbc_read_sets_options_and_runs(monkeypatch):
    reader = _FakeReader()
    _install_fake_pyspark(monkeypatch, reader)
    proc = _make_spark_processor("postgresql://u:p@host:5432/analytics", options={"fetch_size": 500})

    captured = {}
    monkeypatch.setattr(proc, "run", lambda df, source_path=None: captured.update(df=df, source_path=source_path))

    proc._run_database_source()

    assert reader.options["url"] == "jdbc:postgresql://host:5432/analytics"
    assert reader.options["driver"] == "org.postgresql.Driver"
    assert reader.options["fetchsize"] == 500
    assert reader.options["user"] == "u"
    assert reader.options["password"] == "p"
    # column projection pushed into the subquery
    assert 'SELECT "id", "total"' in reader.options["dbtable"]
    assert captured["df"].columns == ["id", "total"]
    assert captured["source_path"] == "database://massive_orders"


def test_spark_jdbc_partitioned_read(monkeypatch):
    reader = _FakeReader()
    _install_fake_pyspark(monkeypatch, reader)
    proc = _make_spark_processor(
        "postgresql://u:p@h/db",
        options={
            "partition_column": "id",
            "partition_num": 4,
            "partition_lower_bound": 1,
            "partition_upper_bound": 1000,
        },
    )
    monkeypatch.setattr(proc, "run", lambda df, source_path=None: None)

    proc._run_database_source()

    assert reader.options["partitionColumn"] == "id"
    assert reader.options["numPartitions"] == 4
    assert reader.options["lowerBound"] == "1"
    assert reader.options["upperBound"] == "1000"


def test_spark_missing_driver_raises_actionable_error(monkeypatch):
    err = Exception("java.lang.ClassNotFoundException: org.sqlite.JDBC")
    reader = _FakeReader(load_error=err)
    _install_fake_pyspark(monkeypatch, reader)
    proc = _make_spark_processor("sqlite:///demo.db")
    monkeypatch.setattr(proc, "run", lambda df, source_path=None: None)

    with pytest.raises(RuntimeError, match="spark.jars.packages='org.xerial:sqlite-jdbc'"):
        proc._run_database_source()
