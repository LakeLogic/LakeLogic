"""
Core processor tests: run(), run_source(), tracing, and ValidationResult API.
"""

import os

import polars as pl
import pytest

from lakelogic import DataProcessor
from lakelogic.core.processor import ValidationResult


# ────────────────────────────────────────────────────────
# ValidationResult API
# ────────────────────────────────────────────────────────
class TestValidationResult:
    """Ensure stable unpacking, attribute access, and repr."""

    def test_two_tuple_unpack(self):
        """The common destructuring pattern must work."""
        result = ValidationResult(good="g", bad="b")
        a, b = result
        assert a == "g"
        assert b == "b"

    def test_getitem(self):
        result = ValidationResult(good="g", bad="b")
        assert result[0] == "g"
        assert result[1] == "b"

    def test_len_is_two(self):
        assert len(ValidationResult(good=1, bad=2)) == 2

    def test_raw_defaults_to_none(self):
        result = ValidationResult(good="g", bad="b")
        assert result.raw is None

    def test_raw_attribute_access(self):
        result = ValidationResult(good="g", bad="b", raw="r")
        assert result.raw == "r"

    def test_trace_attribute_access(self):
        result = ValidationResult(good="g", bad="b", trace="t")
        assert result.trace == "t"

    def test_repr_does_not_raise(self):
        df = pl.DataFrame({"x": [1, 2]})
        result = ValidationResult(good=df, bad=df)
        r = repr(result)
        assert "good=2" in r
        assert "bad=2" in r


# ────────────────────────────────────────────────────────
# DataProcessor.run() — basic contract validation
# ────────────────────────────────────────────────────────
class TestProcessorRun:
    """Ensure run() round-trips correctly."""

    def test_basic_quality_split(self):
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "quality": {"row_rules": [{"name": "positive", "sql": "val > 0"}]},
        }
        df = pl.DataFrame({"val": [1, -1, 3]})
        proc = DataProcessor(engine="polars", contract=contract)
        good, bad = proc.run(df)
        assert len(good) == 2
        assert len(bad) == 1

    def test_unpack_with_attribute_access(self):
        """Ensure .good / .bad match unpacking results."""
        contract = {"version": "1.0.0", "dataset": "test"}
        df = pl.DataFrame({"a": [1]})
        proc = DataProcessor(engine="polars", contract=contract)
        result = proc.run(df)
        good, bad = result
        assert good.shape == result.good.shape
        assert bad.shape == result.bad.shape

    def test_run_returns_validation_result(self):
        contract = {"version": "1.0.0", "dataset": "test"}
        df = pl.DataFrame({"a": [1]})
        proc = DataProcessor(engine="polars", contract=contract)
        result = proc.run(df)
        assert isinstance(result, ValidationResult)

    def test_no_rules_passes_all(self):
        contract = {"version": "1.0.0", "dataset": "test"}
        df = pl.DataFrame({"a": [1, 2, 3]})
        proc = DataProcessor(engine="polars", contract=contract)
        good, bad = proc.run(df)
        assert len(good) == 3
        assert len(bad) == 0

    def test_all_rows_quarantined(self):
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "quality": {"row_rules": [{"name": "impossible", "sql": "1 = 0"}]},
        }
        df = pl.DataFrame({"a": [1, 2]})
        proc = DataProcessor(engine="polars", contract=contract)
        good, bad = proc.run(df)
        assert len(good) == 0
        assert len(bad) == 2


# ────────────────────────────────────────────────────────
# DataProcessor.run_source() — file loading
# ────────────────────────────────────────────────────────
class TestRunSource:
    """Verify run_source loads data and applies contracts."""

    @pytest.fixture
    def csv_source(self, tmp_path):
        """Create a simple CSV file and return its path."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,age\nAlice,30\nBob,25\n")
        return csv_file

    @pytest.fixture
    def parquet_source(self, tmp_path):
        """Create a simple Parquet file and return its path."""
        pq_file = tmp_path / "data.parquet"
        pl.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]}).write_parquet(str(pq_file))
        return pq_file

    def test_run_source_csv_polars(self, csv_source):
        contract = {
            "version": "1.0.0",
            "dataset": "people",
            "source": {"type": "landing", "path": str(csv_source)},
        }
        proc = DataProcessor(engine="polars", contract=contract)
        good, bad = proc.run_source()
        assert len(good) == 2
        assert "name" in good.columns

    def test_run_source_explicit_path(self, csv_source):
        """run_source(path) should override contract source."""
        contract = {"version": "1.0.0", "dataset": "people"}
        proc = DataProcessor(engine="polars", contract=contract)
        good, bad = proc.run_source(str(csv_source))
        assert len(good) == 2

    def test_run_source_parquet_polars(self, parquet_source):
        contract = {
            "version": "1.0.0",
            "dataset": "people",
            "source": {"type": "landing", "path": str(parquet_source)},
        }
        proc = DataProcessor(engine="polars", contract=contract)
        good, bad = proc.run_source()
        assert len(good) == 2

    def test_run_source_with_quality_rules(self, csv_source):
        contract = {
            "version": "1.0.0",
            "dataset": "people",
            "source": {"type": "landing", "path": str(csv_source)},
            "quality": {"row_rules": [{"name": "adult", "sql": "age >= 30"}]},
        }
        proc = DataProcessor(engine="polars", contract=contract)
        good, bad = proc.run_source()
        assert len(good) == 1  # Alice (30)
        assert len(bad) == 1  # Bob (25)

    def test_run_source_no_path_raises(self):
        contract = {"version": "1.0.0", "dataset": "people"}
        proc = DataProcessor(engine="polars", contract=contract)
        with pytest.raises(ValueError, match="No source path"):
            proc.run_source()


# ────────────────────────────────────────────────────────
# Tracing
# ────────────────────────────────────────────────────────
class TestTracing:
    """Verify trace collection and display."""

    def test_trace_step_captures_duration(self):
        contract = {"version": "1.0.0", "dataset": "test"}
        proc = DataProcessor(engine="polars", contract=contract, trace=True)
        proc._active_trace_steps = []
        with proc.trace_step("test_step", detail="value"):
            pass  # no-op
        assert len(proc._active_trace_steps) == 1
        step = proc._active_trace_steps[0]
        assert step.step == "test_step"
        assert step.status == "ok"
        assert step.duration_ms >= 0

    def test_trace_step_records_error(self):
        contract = {"version": "1.0.0", "dataset": "test"}
        proc = DataProcessor(engine="polars", contract=contract, trace=True)
        proc._active_trace_steps = []
        with pytest.raises(ValueError):
            with proc.trace_step("failing_step"):
                raise ValueError("boom")
        step = proc._active_trace_steps[0]
        assert step.status == "error"
        assert "boom" in step.details.get("error", "")

    def test_run_captures_trace(self):
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "quality": {"row_rules": [{"name": "check", "sql": "a > 0"}]},
        }
        df = pl.DataFrame({"a": [1, 2]})
        proc = DataProcessor(engine="polars", contract=contract, trace=True)
        result = proc.run(df)
        assert result.trace is not None
        assert result.trace.total_duration_ms >= 0
        assert len(result.trace.steps) > 0

    def test_show_trace_from_last_result(self, capsys):
        """show_trace() should use last_result when no arg is given."""
        contract = {"version": "1.0.0", "dataset": "test"}
        df = pl.DataFrame({"a": [1]})
        proc = DataProcessor(engine="polars", contract=contract, trace=True)
        proc.run(df)
        # Should not raise
        proc.show_trace()

    def test_run_source_includes_load_step(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("x\n1\n2\n")
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "source": {"type": "landing", "path": str(csv_file)},
        }
        proc = DataProcessor(engine="polars", contract=contract, trace=True)
        result = proc.run_source()
        assert result.trace is not None
        step_names = [s.step for s in result.trace.steps]
        assert "Load Source" in step_names


# ────────────────────────────────────────────────────────
# Edge cases
# ────────────────────────────────────────────────────────
class TestEdgeCases:
    """Boundary conditions and defensive behavior."""

    def test_empty_dataframe(self):
        contract = {"version": "1.0.0", "dataset": "test"}
        df = pl.DataFrame({"a": []})
        proc = DataProcessor(engine="polars", contract=contract)
        good, bad = proc.run(df)
        assert len(good) == 0
        assert len(bad) == 0

    def test_multiple_rules(self):
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "quality": {
                "row_rules": [
                    {"name": "positive", "sql": "val > 0"},
                    {"name": "small", "sql": "val < 100"},
                ]
            },
        }
        df = pl.DataFrame({"val": [-1, 50, 200]})
        proc = DataProcessor(engine="polars", contract=contract)
        good, bad = proc.run(df)
        assert len(good) == 1  # 50 passes both
        assert len(bad) == 2  # -1 fails first, 200 fails second

    def test_processor_init_from_dict(self):
        proc = DataProcessor(engine="polars", contract={"version": "1.0.0", "dataset": "test"})
        assert proc.engine_name == "polars"
        assert proc.contract.version == "1.0.0"

    def test_processor_preserves_last_result(self):
        contract = {"version": "1.0.0", "dataset": "test"}
        df = pl.DataFrame({"a": [1]})
        proc = DataProcessor(engine="polars", contract=contract)
        result = proc.run(df)
        assert proc.last_result is result

    def test_result_raw_captures_input(self):
        """result.raw should contain the original input dataframe."""
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "quality": {"row_rules": [{"name": "check", "sql": "val > 0"}]},
        }
        df = pl.DataFrame({"val": [1, -1, 3]})
        proc = DataProcessor(engine="polars", contract=contract)
        result = proc.run(df)
        assert result.raw is not None
        assert len(result.raw) == 3  # original input


# ────────────────────────────────────────────────────────
# SLO module (extracted)
# ────────────────────────────────────────────────────────
class TestSLOComputation:
    """Test the extracted SLO module."""

    def test_parse_duration_hours(self):
        from lakelogic.core.slo import _parse_duration_seconds

        assert _parse_duration_seconds("24h") == 86400.0

    def test_parse_duration_minutes(self):
        from lakelogic.core.slo import _parse_duration_seconds

        assert _parse_duration_seconds("30m") == 1800.0

    def test_parse_duration_numeric(self):
        from lakelogic.core.slo import _parse_duration_seconds

        # Numeric values treated as hours
        assert _parse_duration_seconds(2) == 7200.0

    def test_parse_duration_none(self):
        from lakelogic.core.slo import _parse_duration_seconds

        assert _parse_duration_seconds(None) is None

    def test_coerce_datetime_none(self):
        from lakelogic.core.slo import _coerce_datetime

        assert _coerce_datetime(None) is None

    def test_coerce_datetime_iso_string(self):
        from lakelogic.core.slo import _coerce_datetime

        dt = _coerce_datetime("2024-01-15T12:00:00Z")
        assert dt is not None
        assert dt.tzinfo is not None


# ────────────────────────────────────────────────────────
# Spark adapter (skipped if PySpark not installed)
# ────────────────────────────────────────────────────────

try:
    import pyspark as _pyspark  # noqa: F401

    _HAS_PYSPARK = True
except ImportError:
    _HAS_PYSPARK = False


@pytest.fixture(scope="module")
def spark_session():
    """Create a local SparkSession for testing (reused across the module)."""
    import sys
    import tempfile

    # On Windows, PySpark needs HADOOP_HOME with bin/winutils.exe.
    # Create a minimal fake hadoop home if not already configured.
    if sys.platform == "win32" and not os.environ.get("HADOOP_HOME"):
        hadoop_home = os.path.join(tempfile.gettempdir(), "hadoop_lakelogic")
        hadoop_bin = os.path.join(hadoop_home, "bin")
        os.makedirs(hadoop_bin, exist_ok=True)
        winutils = os.path.join(hadoop_bin, "winutils.exe")
        if not os.path.exists(winutils):
            # Create a minimal stub — Spark only checks existence for most ops.
            with open(winutils, "wb") as f:
                f.write(b"")
        os.environ["HADOOP_HOME"] = hadoop_home

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("lakelogic-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


import sys as _sys

_SKIP_SPARK = not _HAS_PYSPARK or _sys.platform == "win32"
_SKIP_SPARK_REASON = (
    "PySpark not installed" if not _HAS_PYSPARK else "PySpark local mode unreliable on Windows (run in CI/Databricks)"
)


@pytest.mark.skipif(_SKIP_SPARK, reason=_SKIP_SPARK_REASON)
class TestSparkAdapter:
    """Verify Spark engine works for basic operations."""

    def test_spark_quality_split(self, spark_session):
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "quality": {"row_rules": [{"name": "positive", "sql": "val > 0"}]},
        }
        df = spark_session.createDataFrame([(1,), (-1,), (3,)], ["val"])
        proc = DataProcessor(engine="spark", contract=contract)
        good, bad = proc.run(df)
        assert good.count() == 2
        assert bad.count() == 1

    def test_spark_no_rules(self, spark_session):
        contract = {"version": "1.0.0", "dataset": "test"}
        df = spark_session.createDataFrame([(1,), (2,), (3,)], ["a"])
        proc = DataProcessor(engine="spark", contract=contract)
        good, bad = proc.run(df)
        assert good.count() == 3
        assert bad.count() == 0

    def test_spark_returns_spark_dataframes(self, spark_session):
        from pyspark.sql import DataFrame as SparkDataFrame

        contract = {"version": "1.0.0", "dataset": "test"}
        df = spark_session.createDataFrame([(1,), (2,)], ["a"])
        proc = DataProcessor(engine="spark", contract=contract)
        good, bad = proc.run(df)
        assert isinstance(good, SparkDataFrame)
        assert isinstance(bad, SparkDataFrame)

    def test_spark_multiple_rules(self, spark_session):
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "quality": {
                "row_rules": [
                    {"name": "positive", "sql": "val > 0"},
                    {"name": "small", "sql": "val < 100"},
                ]
            },
        }
        df = spark_session.createDataFrame([(-1,), (50,), (200,)], ["val"])
        proc = DataProcessor(engine="spark", contract=contract)
        good, bad = proc.run(df)
        assert good.count() == 1  # 50 passes both
        assert bad.count() == 2  # -1 fails first, 200 fails second

    def test_spark_all_quarantined(self, spark_session):
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "quality": {"row_rules": [{"name": "impossible", "sql": "1 = 0"}]},
        }
        df = spark_session.createDataFrame([(1,), (2,)], ["a"])
        proc = DataProcessor(engine="spark", contract=contract)
        good, bad = proc.run(df)
        assert good.count() == 0
        assert bad.count() == 2

    def test_spark_result_raw_captures_input(self, spark_session):
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "quality": {"row_rules": [{"name": "check", "sql": "val > 0"}]},
        }
        df = spark_session.createDataFrame([(1,), (-1,), (3,)], ["val"])
        proc = DataProcessor(engine="spark", contract=contract)
        result = proc.run(df)
        assert result.raw is not None
        assert result.raw.count() == 3

    def test_spark_run_source_parquet(self, spark_session, tmp_path):
        import pandas as pd

        pq_file = tmp_path / "data.parquet"
        pd.DataFrame({"x": [10, 20]}).to_parquet(str(pq_file))
        contract = {
            "version": "1.0.0",
            "dataset": "test",
            "source": {"type": "landing", "path": str(pq_file)},
        }
        proc = DataProcessor(engine="spark", contract=contract)
        good, bad = proc.run_source()
        assert good.count() == 2
