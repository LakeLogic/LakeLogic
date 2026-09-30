"""SCD1 dims on Spark/Delta must carry their surrogate key.

Live on Databricks (2026-09-29): `gold_rideflow_dim_cancelled_by_sk` was NULL on every row. The
key was computed only by the DataFrame fallback in `_spark_merge_dataframe`, which a Delta
table never reaches — the first write and the Delta MERGE both returned before it — and the
unknown member (`_inject_unknown_member_spark_table`) was defined but never called.
No local Spark: these pin the code paths.
"""
from __future__ import annotations

import inspect

from lakelogic.core import materialization as m


def _merge_src() -> str:
    return inspect.getsource(m._spark_merge_dataframe)


def test_the_key_is_computed_before_either_delta_path():
    src = _merge_src()
    key_at = src.index("_with_scd1_surrogate_key_spark(incoming_df, primary_key, scd1_cfg)")
    assert key_at < src.index("DeltaTable.isDeltaTable")
    assert key_at < src.index("(no existing data)") and key_at < src.index("(new target)")


def test_the_unknown_member_is_added_after_every_delta_write():
    src = _merge_src()
    # after the MERGE, and after both first-write branches
    assert src.count("_inject_scd1_unknown_member_after_write(spark, table_or_path, primary_key, scd1_cfg)") == 3
    assert src.index("merge_builder.execute()") < src.index("_inject_scd1_unknown_member_after_write")


def test_the_key_expression_matches_the_dataframe_fallback():
    helper = inspect.getsource(m._with_scd1_surrogate_key_spark)
    assert 'F.substring(F.sha2(pk_concat, 256), 1, 16)' in helper
    assert 'F.concat_ws("|"' in helper
    assert "F.lit(unknown_sk)" in helper  # the unknown member keeps its key
    assert 'F.substring(F.sha2(pk_concat, 256), 1, 16)' in _merge_src()  # the fallback's own


def test_no_scd1_config_leaves_the_frame_alone():
    sentinel = object()
    assert m._with_scd1_surrogate_key_spark(sentinel, ["k"], None) is sentinel
    assert m._with_scd1_surrogate_key_spark(sentinel, ["k"], {"surrogate_key": ""}) is sentinel
    m._inject_scd1_unknown_member_after_write(None, "t", ["k"], None)  # no-op, no Spark touched
    m._inject_scd1_unknown_member_after_write(None, "t", ["k"], {"surrogate_key": "sk"})
