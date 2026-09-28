"""Parallel jobs of one domain write the same evidence table: a lost create/append race is
retried, a real error is not (2026-09-27, seen live on Databricks)."""
import sys, types
import pytest

from lakelogic.core import evidence_tables as et


def test_conflict_detection():
    assert et._is_concurrency_conflict(Exception("[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table"))
    assert et._is_concurrency_conflict(type("ConcurrentAppendException", (Exception,), {})("x"))
    assert not et._is_concurrency_conflict(Exception("PERMISSION_DENIED: no MODIFY on table"))


class _Writer:
    def __init__(self, fails):
        self.fails = list(fails)
        self.calls = 0
    def format(self, *_): return self
    def mode(self, *_): return self
    def option(self, *_): return self
    def saveAsTable(self, _):
        self.calls += 1
        if self.fails:
            raise self.fails.pop(0)


@pytest.fixture()
def fake_spark(monkeypatch):
    writer = _Writer([])
    class DF:
        write = writer
    class Spark:
        sqls = []
        def sql(self, q): Spark.sqls.append(q)
        def createDataFrame(self, *_ , **__): return DF()
    class Builder:
        def getOrCreate(self): return Spark()
    class SparkSession:
        builder = Builder()
    class Field:
        def __init__(self, name, t, *_): self.name, self.dataType = name, types.SimpleNamespace(simpleString=lambda: "string")
    class StructType(list):
        def __getitem__(self, k): return next(f for f in list.__iter__(self) if f.name == k) if isinstance(k, str) else list.__getitem__(self, k)
    pyspark = types.ModuleType("pyspark"); sql = types.ModuleType("pyspark.sql"); tps = types.ModuleType("pyspark.sql.types")
    sql.SparkSession = SparkSession; tps.StructField = Field; tps.StructType = StructType
    monkeypatch.setitem(sys.modules, "pyspark", pyspark); monkeypatch.setitem(sys.modules, "pyspark.sql", sql); monkeypatch.setitem(sys.modules, "pyspark.sql.types", tps)
    monkeypatch.setattr(et, "spark_type_object", lambda t: t)
    monkeypatch.setattr(et.time, "sleep", lambda *_: None)
    return writer, Spark


def test_append_retries_a_lost_race_and_creates_the_table_first(fake_spark):
    writer, Spark = fake_spark
    writer.fails = [Exception("ConcurrentAppendException: conflicting commit")]
    assert et._write_spark("c.s._lakelogic_retention_evidence", [{"a": 1}], {"a": "string"}) == "c.s._lakelogic_retention_evidence"
    assert writer.calls == 2
    assert any("CREATE TABLE IF NOT EXISTS c.s._lakelogic_retention_evidence" in q for q in Spark.sqls)


def test_a_real_error_is_not_retried(fake_spark):
    writer, _ = fake_spark
    writer.fails = [Exception("PERMISSION_DENIED")]
    with pytest.raises(Exception, match="PERMISSION_DENIED"):
        et._write_spark("c.s.t", [{"a": 1}], {"a": "string"})
    assert writer.calls == 1
