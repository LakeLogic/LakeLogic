"""max_target incremental: the right target table, and a string timestamp stepped by a second.

Live on Databricks (2026-09-30) every "incremental" silver re-read ALL of bronze:
  1. the processor derived the target as `split(".")[0] + table` - catalog only, schema
     dropped - so `table:cat.marketplace.bronze_x` looked up `cat.silver_x`, which does not
     exist, and max_target silently fell back to a 90-day window;
  2. once the lookup works, `_lakelogic_processed_at` is stored as ISO TEXT, and a string
     watermark was stepped by a whole DAY - skipping up to 24h of new rows.
"""
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from lakelogic.core.incremental import IncrementalBoundary


@pytest.fixture
def fake_spark(monkeypatch):
    seen = {}

    class _DF:
        def __init__(self, val):
            self.val = val

        def agg(self, *_):
            return self

        def collect(self):
            return [[self.val]]

    class _Spark:
        value = None

        def table(self, name):
            seen["table"] = name
            if name.startswith("missing"):
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
            return _DF(self.value)

    spark = _Spark()
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = types.SimpleNamespace(
        getActiveSession=lambda: spark if seen.get("active", True) else None,
        builder=types.SimpleNamespace(getOrCreate=lambda: spark),
    )
    funcs = types.ModuleType("pyspark.sql.functions")
    funcs.max = lambda c: c
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", funcs)
    return spark, seen


def test_a_string_timestamp_watermark_steps_one_second(fake_spark):
    spark, _ = fake_spark
    spark.value = "2026-09-30T10:25:27.000Z"
    b = IncrementalBoundary.from_max_target("table:cat.marketplace.silver_x", watermark_field="_lakelogic_processed_at")
    assert b.from_dt.replace(tzinfo=None) == datetime(2026, 9, 30, 10, 25, 28)


def test_a_bare_date_watermark_still_steps_a_day(fake_spark):
    spark, _ = fake_spark
    spark.value = "2026-09-30"
    b = IncrementalBoundary.from_max_target("table:cat.s.t", watermark_field="d")
    assert b.from_dt == datetime(2026, 10, 1)


def test_a_missing_target_falls_back_and_says_so(fake_spark, caplog):
    b = IncrementalBoundary.from_max_target("table:missing.silver_x", watermark_field="w")
    assert "fallback_reason" in b.metadata
    assert datetime.now(timezone.utc) - b.from_dt > timedelta(days=89)
    assert any("could not read the watermark" in r.getMessage() for r in caplog.records)


def test_the_processor_keeps_the_schema_when_deriving_the_target():
    src = (Path(__file__).resolve().parents[1] / "lakelogic" / "core" / "processor.py").read_text(encoding="utf-8")
    assert '_catalog = _src_path.rsplit(".", 1)[0] if "." in _src_path else ""' in src
    assert '_catalog = _src_path.split(".")[0] if "." in _src_path else ""' not in src


def test_a_worker_thread_with_no_active_session_still_reads_the_watermark(fake_spark):
    """getActiveSession() is thread-local and None on the pipeline's worker threads; that
    made every lookup fall back to 90 days on Databricks (2026-09-30)."""
    spark, seen = fake_spark
    seen["active"] = False
    spark.value = datetime(2026, 9, 30, 10, 25, 27)
    b = IncrementalBoundary.from_max_target("table:cat.marketplace.silver_x", watermark_field="w")
    assert "fallback_reason" not in b.metadata
    assert b.from_dt == datetime(2026, 9, 30, 10, 25, 28)
