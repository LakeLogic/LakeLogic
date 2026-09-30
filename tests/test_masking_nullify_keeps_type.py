"""`masking: nullify` must keep the column's type.

On Spark the masked column was `NULL` cast to STRING. A silver table created from its contract
holds `gps_lat` as FLOAT, so the MERGE failed with DELTA_FAILED_TO_MERGE_FIELDS ('gps_lat' and
'gps_lat') and every masked numeric column stopped the load. Polars wrote an untyped NULL column
(dtype Null), which has the same shape of problem. A NULL fits any type, so nullify keeps it.
"""

from __future__ import annotations

import inspect

import pytest

from lakelogic.core.masking_engine import MaskingEngine
from lakelogic.core.models import DataContract

pl = pytest.importorskip("polars")
pd = pytest.importorskip("pandas")


def _engine() -> MaskingEngine:
    return MaskingEngine(DataContract(
        version="1.0.0",
        dataset="trips",
        model={"fields": [
            {"name": "trip_id", "type": "string"},
            {"name": "gps_lat", "type": "float", "pii": True, "masking": "nullify"},
            {"name": "seen_at", "type": "timestamp", "pii": True, "masking": "nullify"},
        ]},
    ))


def test_polars_nullify_keeps_float_and_timestamp_types():
    from datetime import datetime

    df = pl.DataFrame({"trip_id": ["a", "b"], "gps_lat": [51.5, None],
                       "seen_at": [datetime(2026, 1, 1), datetime(2026, 1, 2)]})
    out = _engine().apply(df, user_groups=[])
    assert out.schema["gps_lat"] == pl.Float64
    assert out.schema["seen_at"] == df.schema["seen_at"]
    assert out["gps_lat"].null_count() == 2 and out["seen_at"].null_count() == 2


def test_pandas_nullify_keeps_float_type():
    df = pd.DataFrame({"trip_id": ["a", "b"], "gps_lat": [51.5, 52.1]})
    out = _engine().apply(df, user_groups=[])
    assert out["gps_lat"].dtype == "float64"
    assert out["gps_lat"].isna().all()


def test_spark_nullify_casts_to_the_columns_own_type():
    # No local Spark here: pin the expression. Casting to "string" is the bug this file is about.
    src = inspect.getsource(MaskingEngine._apply_spark)
    assert 'F.lit(None).cast(df.schema[col_name].dataType)' in src
    assert 'F.lit(None).cast("string")' not in src


def test_spark_nullify_on_a_real_session_keeps_float():
    pyspark = pytest.importorskip("pyspark")  # noqa: F841
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[1]").getOrCreate()
    df = spark.createDataFrame([("a", 51.5)], "trip_id string, gps_lat float")
    out = _engine().apply(df, user_groups=[])
    assert dict(out.dtypes)["gps_lat"] == "float"
    assert out.collect()[0]["gps_lat"] is None
