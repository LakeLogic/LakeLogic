from __future__ import annotations

import json
import sys
import types
from datetime import date, datetime, timezone

import pytest

from lakelogic.core import incremental as inc


class FakeExpr:
    def __init__(self, text):
        self.text = text

    def __and__(self, other):
        return FakeExpr(f"({self.text} AND {other.text})")

    def __eq__(self, other):
        return FakeExpr(f"({self.text} == {other})")

    def __ne__(self, other):
        return FakeExpr(f"({self.text} != {other})")

    def __ge__(self, other):
        return FakeExpr(f"({self.text} >= {other})")

    def __le__(self, other):
        return FakeExpr(f"({self.text} <= {other})")

    def __mul__(self, other):
        return FakeExpr(f"({self.text} * {other})")

    def __add__(self, other):
        other_text = other.text if isinstance(other, FakeExpr) else other
        return FakeExpr(f"({self.text} + {other_text})")

    def cast(self, dtype):
        return FakeExpr(f"cast({self.text}, {dtype})")

    def alias(self, name):
        return FakeExpr(f"{self.text} AS {name}")

    def __repr__(self):
        return self.text

    __str__ = __repr__


def _install_fake_polars(monkeypatch):
    fake_pl = types.SimpleNamespace(
        lit=lambda value: FakeExpr(str(value)),
        col=lambda name: FakeExpr(name),
        Date="Date",
    )
    monkeypatch.setitem(sys.modules, "polars", fake_pl)


def _install_fake_pyspark(monkeypatch, spark_session):
    fake_functions = types.SimpleNamespace(
        col=lambda name: FakeExpr(f"col({name})"),
        max=lambda value: FakeExpr(f"max({value})"),
        get_json_object=lambda column, path: FakeExpr(f"json({column}, {path})"),
    )
    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.SparkSession = types.SimpleNamespace(getActiveSession=lambda: spark_session)
    fake_pyspark = types.ModuleType("pyspark")
    fake_pyspark.sql = fake_sql
    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_functions)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)


def test_boundary_spark_filter_variants_and_repr():
    boundary = inc.Boundary(
        from_dt=datetime(2024, 3, 1, tzinfo=timezone.utc),
        to_dt=datetime(2024, 3, 31, tzinfo=timezone.utc),
        strategy="lookback",
        partition_filters={"country": "GB", "tenant": 7},
    )

    field_filter = boundary.spark_filter("snapshot_ts")
    assert "country = 'GB'" in field_filter
    assert "tenant = 7" in field_filter
    assert "snapshot_ts >= '2024-03-01T00:00:00+00:00'" in field_filter

    parts_filter = boundary.spark_filter(date_parts=["year", "month", "day"])
    assert "MAKE_DATE(year, month, day) >= '2024-03-01'" in parts_filter

    ym_filter = boundary.spark_filter(date_parts={"year": "yy", "month": "mm"})
    assert "yy = 2024" in ym_filter
    assert "mm <= 3" in ym_filter

    assert boundary.from_date == date(2024, 3, 1)
    assert boundary.to_date == date(2024, 3, 31)
    assert "partition_filters={'country': 'GB', 'tenant': 7}" in repr(boundary)

    with pytest.raises(ValueError, match="2 or 3 elements"):
        boundary.spark_filter(date_parts=["year"])
    with pytest.raises(ValueError, match="at least 'year' and 'month'"):
        boundary.spark_filter(date_parts={"year": "yy"})
    with pytest.raises(ValueError, match="Provide either field"):
        boundary.spark_filter()


def test_boundary_polars_filter_variants(monkeypatch):
    _install_fake_polars(monkeypatch)
    boundary = inc.Boundary(
        from_dt=datetime(2024, 3, 1, tzinfo=timezone.utc),
        to_dt=datetime(2024, 3, 31, tzinfo=timezone.utc),
        strategy="date_range",
        partition_filters={"country": "GB"},
    )

    expr = boundary.polars_filter("snapshot_dt")
    assert "country" in str(expr)
    assert "cast(snapshot_dt, Date)" in str(expr)

    day_expr = boundary.polars_filter(date_parts=["year", "month", "day"])
    assert "year" in str(day_expr)
    assert "10000" in str(day_expr)

    month_expr = boundary.polars_filter(date_parts={"year": "yy", "month": "mm"})
    assert "yy" in str(month_expr)
    assert "mm" in str(month_expr)

    with pytest.raises(ValueError, match="2 or 3 elements"):
        boundary.polars_filter(date_parts=["year"])
    with pytest.raises(ValueError, match="at least 'year' and 'month'"):
        boundary.polars_filter(date_parts={"year": "yy"})
    with pytest.raises(ValueError, match="Provide either field"):
        boundary.polars_filter()


def test_parse_lookback_supports_numbers_and_units():
    assert inc._parse_lookback("90").total_seconds() == 90
    assert inc._parse_lookback("1.5 hours").total_seconds() == 5400
    assert inc._parse_lookback("2weeks").days == 14

    with pytest.raises(ValueError, match="Cannot parse"):
        inc._parse_lookback("later")
    with pytest.raises(ValueError, match="Unknown time unit"):
        inc._parse_lookback("5 fortnights")


def test_from_max_target_success_and_fallback(monkeypatch):
    class FakeAgg:
        def __init__(self, value):
            self.value = value

        def collect(self):
            return [[self.value]]

    class FakeFrame:
        def __init__(self, value):
            self.value = value

        def agg(self, *args, **kwargs):
            return FakeAgg(self.value)

    class FakeSpark:
        def __init__(self, value):
            self.value = value
            self.read = types.SimpleNamespace(
                format=lambda _: types.SimpleNamespace(load=lambda path: FakeFrame(self.value))
            )

        def table(self, name):
            return FakeFrame(self.value)

    _install_fake_pyspark(monkeypatch, FakeSpark(date(2024, 3, 5)))
    boundary = inc.IncrementalBoundary.from_max_target("table:bronze.orders", "snapshot_dt")
    assert boundary.strategy == "max_target"
    assert boundary.from_dt == datetime(2024, 3, 6)
    assert boundary.metadata["watermark_value"] == "2024-03-05"

    class BrokenSpark:
        read = types.SimpleNamespace(
            format=lambda _: types.SimpleNamespace(load=lambda path: (_ for _ in ()).throw(RuntimeError("missing")))
        )

        def table(self, name):
            raise RuntimeError("missing")

    _install_fake_pyspark(monkeypatch, BrokenSpark())
    fallback = inc.IncrementalBoundary.from_max_target("/tmp/target", "snapshot_dt", default_from="2024-01-01T00:00:00")
    assert fallback.from_dt == datetime(2024, 1, 1, 0, 0)
    assert "missing" in fallback.metadata["fallback_reason"]


def test_from_delta_version_covers_heal_skip_reset_and_fallback(monkeypatch):
    class FakeCollectFrame:
        def __init__(self, rows):
            self.rows = rows

        def filter(self, expr):
            return self

        def agg(self, *args, **kwargs):
            return self

        def collect(self):
            return self.rows

    class FakeSpark:
        def __init__(self):
            self.history_version = 8
            self.log_row = {"last_watermark": 5, "last_json_version": None}
            self.props_rows = [{"key": "unrelated", "value": "x"}]

        def sql(self, query):
            if query.startswith("SHOW TBLPROPERTIES"):
                return FakeCollectFrame(self.props_rows)
            if query.startswith("DESCRIBE HISTORY"):
                return FakeCollectFrame([{"version": self.history_version}])
            raise AssertionError(query)

        def table(self, name):
            return FakeCollectFrame([self.log_row])

    spark = FakeSpark()
    _install_fake_pyspark(monkeypatch, spark)
    infos = []
    warnings = []
    monkeypatch.setattr(inc.logger, "info", infos.append)
    monkeypatch.setattr(inc.logger, "warning", warnings.append)

    healed = inc.IncrementalBoundary.from_delta_version(
        "table:bronze.orders",
        "table:silver.orders",
        dataset="orders",
        data_layer="silver",
        domain="commerce",
        system="erp",
    )
    assert healed.metadata["from_version"] == 6
    assert healed.metadata["to_version"] == 8
    assert any("Healed missing Delta property" in message for message in infos)

    spark.props_rows = [{"key": "lakelogic.last_source_version", "value": "8"}]
    skipped = inc.IncrementalBoundary.from_delta_version("table:bronze.orders", "table:silver.orders")
    assert skipped.metadata["skip_sync"] is True
    assert skipped.metadata["from_version"] == 8

    spark.props_rows = [{"key": "lakelogic.last_source_version", "value": "12"}]
    spark.history_version = 3
    reset = inc.IncrementalBoundary.from_delta_version("table:bronze.orders", "table:silver.orders", default_version=1)
    assert reset.metadata["from_version"] == 1
    assert reset.metadata["to_version"] == 3
    assert any("FULL reload" in message for message in warnings)

    _install_fake_pyspark(monkeypatch, None)
    failed = inc.IncrementalBoundary.from_delta_version("/src", "/tgt")
    assert failed.metadata["from_version"] == 0
    assert failed.strategy == "delta_version"


def test_from_pipeline_log_modern_legacy_and_fallback(monkeypatch):
    class FakeFrame:
        def __init__(self, row):
            self.row = row

        def filter(self, expr):
            return self

        def agg(self, *args, **kwargs):
            return self

        def collect(self):
            return [self.row]

    class FakeSpark:
        def __init__(self):
            self.row = {
                "last_source_mtime": 1710000000,
                "last_watermark": None,
                "last_success": "2024-03-10T00:00:00+00:00",
            }

        def table(self, name):
            return FakeFrame(self.row)

    _install_fake_pyspark(monkeypatch, FakeSpark())
    infos = []
    monkeypatch.setattr(inc.logger, "info", infos.append)
    modern = inc.IncrementalBoundary.from_pipeline_log("pipe", dataset="orders", data_layer="bronze")
    assert modern.metadata["source"] == "max_source_mtime"
    assert modern.from_dt.tzinfo == timezone.utc
    assert any("max_source_mtime" in message for message in infos)

    class LegacySpark:
        def table(self, name):
            return FakeFrame({"last_success": date(2024, 3, 10)})

    _install_fake_pyspark(monkeypatch, LegacySpark())
    legacy = inc.IncrementalBoundary.from_pipeline_log("pipe")
    assert legacy.metadata["pipeline_name"] == "pipe"
    assert legacy.from_dt == datetime(2024, 3, 11)

    _install_fake_pyspark(monkeypatch, None)
    fallback = inc.IncrementalBoundary.from_pipeline_log("pipe", default_from="2024-02-01T00:00:00")
    assert fallback.from_dt == datetime(2024, 2, 1, 0, 0)
    assert "pipe" in fallback.metadata["pipeline_name"]


def test_manifest_update_and_resolution(tmp_path):
    manifest = tmp_path / "state" / "manifest.json"
    inc.IncrementalBoundary.update_manifest(str(manifest), "bronze_to_silver", ["2024-03-01", "2024-03-03"])
    inc.IncrementalBoundary.update_manifest(str(manifest), "bronze_to_silver", ["2024-03-02", "2024-03-03"])

    data = json.loads(manifest.read_text(encoding="utf-8"))
    assert data["processed_partitions"] == ["2024-03-01", "2024-03-02", "2024-03-03"]
    assert data["pipeline"] == "bronze_to_silver"

    boundary = inc.IncrementalBoundary.from_manifest(str(manifest))
    assert boundary.metadata["last_partition"] == "2024-03-03"
    assert boundary.from_dt == datetime(2024, 3, 4)

    fallback = inc.IncrementalBoundary.from_manifest(str(tmp_path / "missing.json"), default_from="2024-01-05T00:00:00")
    assert fallback.from_dt == datetime(2024, 1, 5, 0, 0)
    assert "Manifest not found" in fallback.metadata["fallback_reason"]


def test_date_range_lookback_contract_and_source_config_dispatch(monkeypatch, tmp_path):
    reference = datetime(2024, 3, 10, tzinfo=timezone.utc)
    lookback = inc.IncrementalBoundary.from_lookback(
        "7 days", reference_dt=reference, partition_filters={"country": "GB"}
    )
    assert lookback.metadata["delta_seconds"] == 604800
    assert lookback.partition_filters == {"country": "GB"}

    date_range = inc.IncrementalBoundary.from_date_range(
        "2024-03-01", date(2024, 3, 5), partition_filters={"region": "south"}
    )
    assert date_range.from_dt == datetime(2024, 3, 1)
    assert date_range.to_dt == datetime(2024, 3, 5)

    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(
        """
source:
  watermark_strategy: pipeline_log
  pipeline_log_table: lake.logs
  dataset: bronze_orders
  data_layer: bronze
  domain: commerce
  system: erp
  partition_filters:
    country: GB
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        inc.IncrementalBoundary,
        "from_pipeline_log",
        classmethod(
            lambda cls, *args, **kwargs: inc.Boundary(datetime(2024, 3, 1), datetime(2024, 3, 2), "pipeline_log")
        ),
    )
    boundary = inc.IncrementalBoundary.from_contract(str(contract_path), partition_filters={"tenant": 7})
    assert boundary.partition_filters == {"country": "GB", "tenant": 7}

    monkeypatch.setattr(
        inc.IncrementalBoundary,
        "from_lookback",
        classmethod(
            lambda cls, *args, **kwargs: inc.Boundary(
                datetime(2024, 3, 1),
                datetime(2024, 3, 2),
                "lookback",
                partition_filters=kwargs.get("partition_filters") or {},
            )
        ),
    )
    assert inc.IncrementalBoundary.from_contract(str(contract_path), lookback="2 days").strategy == "lookback"

    class SourceConfig:
        def model_dump(self):
            return {
                "watermark_strategy": "manifest",
                "manifest_path": "state/manifest.json",
                "partition_filters": {"country": "GB"},
            }

    monkeypatch.setattr(
        inc.IncrementalBoundary,
        "from_manifest",
        classmethod(lambda cls, *args, **kwargs: inc.Boundary(datetime(2024, 3, 1), datetime(2024, 3, 2), "manifest")),
    )
    source_boundary = inc.IncrementalBoundary.from_source_config(SourceConfig(), partition_filters={"tenant": 7})
    assert source_boundary.strategy == "manifest"
    assert source_boundary.partition_filters == {"country": "GB", "tenant": 7}
