"""Quarantine carries its reasons as plain text too (2026-11-16).

`_lakelogic_errors` is a list. Fabric's SQL endpoint hides list columns, so a Power BI report on
the lakehouse could not show why a row was rejected. `_lakelogic_error_text` is the same reasons
joined into one string.
"""

import pandas as pd
import polars as pl

from lakelogic.core.quarantine import ERROR_TEXT_COLUMN, add_error_text

REASONS = ["[pre] Rule failed: estimated_arrival_has_utc_offset", "[pre] Rule failed: flight_number_not_null"]


def test_polars_list_is_joined():
    df = pl.DataFrame({"id": [1, 2], "_lakelogic_errors": [REASONS, [REASONS[0]]]})
    out = add_error_text(df)
    assert out[ERROR_TEXT_COLUMN].to_list() == ["; ".join(REASONS), REASONS[0]]
    assert out["_lakelogic_errors"].to_list() == df["_lakelogic_errors"].to_list(), "the list stays"


def test_polars_lazy_frame_is_supported():
    out = add_error_text(pl.LazyFrame({"_lakelogic_errors": [REASONS]})).collect()
    assert out[ERROR_TEXT_COLUMN][0] == "; ".join(REASONS)


def test_pandas_list_is_joined_and_none_stays_none():
    df = pd.DataFrame({"_lakelogic_errors": [REASONS, None]})
    out = add_error_text(df)
    assert out[ERROR_TEXT_COLUMN].tolist() == ["; ".join(REASONS), None]


def test_a_string_errors_column_is_copied():
    out = add_error_text(pl.DataFrame({"_lakelogic_errors": ["already text"]}))
    assert out[ERROR_TEXT_COLUMN][0] == "already text"


def test_frames_without_errors_are_unchanged():
    df = pl.DataFrame({"id": [1]})
    assert add_error_text(df).columns == ["id"]


def test_an_existing_text_column_is_not_overwritten():
    df = pl.DataFrame({"_lakelogic_errors": [REASONS], ERROR_TEXT_COLUMN: ["kept"]})
    assert add_error_text(df)[ERROR_TEXT_COLUMN][0] == "kept"


def test_spark_array_is_joined():
    import pytest

    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    try:
        spark = SparkSession.builder.master("local[1]").appName("error-text").getOrCreate()
    except Exception as exc:  # no JVM on this machine
        pytest.skip(f"Spark unavailable: {exc}")
    df = spark.createDataFrame([(1, REASONS)], "id INT, _lakelogic_errors ARRAY<STRING>")
    assert add_error_text(df).collect()[0][ERROR_TEXT_COLUMN] == "; ".join(REASONS)
