"""A blank dedup key is never a duplicate of another blank dedup key.

Every engine used to group NULL keys together, so `deduplicate` collapsed all
keyless rows into ONE and silently dropped the rest before any quality rule saw
them (Lakehouse Studio: 5 null `trip_id` became 1; 12 null rider keys became 1).

Now blank-key rows (ANY key column null) are left out of the grouping, and
`blank_keys` decides their fate:

* `quarantine` (default) — quarantined by `<key>_required_for_dedup`, unless an
  exact `<col> IS NOT NULL` rule already covers every key column, in which case that
  rule quarantines them and no second rule is added (one row, one attribution);
* `keep` — kept, ungrouped.

Real duplicates still collapse, honouring `sort_by`/`order`. The run report records
how many rows had a blank key, and none of them is counted as dropped.
"""

from __future__ import annotations

import polars as pl
import pytest

from lakelogic import DataProcessor
from lakelogic.core.models import DataContract
from lakelogic.engines.duckdb import DuckDBAdapter
from lakelogic.engines.polars import PolarsAdapter

ENGINES = ["polars", "duckdb"]

# 5 blank-key rows, plus id=1 twice (a real duplicate) and id=2 once.
ROWS = [
    {"trip_id": None, "trip_date": "2026-01-01", "updated_at": "2026-01-01", "v": "n1"},
    {"trip_id": None, "trip_date": "2026-01-01", "updated_at": "2026-01-02", "v": "n2"},
    {"trip_id": None, "trip_date": "2026-01-02", "updated_at": "2026-01-03", "v": "n3"},
    {"trip_id": None, "trip_date": None, "updated_at": "2026-01-04", "v": "n4"},
    {"trip_id": None, "trip_date": "2026-01-03", "updated_at": "2026-01-05", "v": "n5"},
    {"trip_id": "1", "trip_date": "2026-01-01", "updated_at": "2026-01-01", "v": "old"},
    {"trip_id": "1", "trip_date": "2026-01-01", "updated_at": "2026-06-01", "v": "new"},
    {"trip_id": "2", "trip_date": "2026-01-01", "updated_at": "2026-01-01", "v": "only"},
]


def _contract(dedup: dict, *, required: bool = False, extra_rules=None) -> dict:
    return {
        "version": "1.0.0",
        "info": {"title": "Trips", "table_name": "trips"},
        "model": {
            "fields": [
                {"name": "trip_id", "type": "string", "required": required},
                {"name": "trip_date", "type": "string"},
                {"name": "updated_at", "type": "string"},
                {"name": "v", "type": "string"},
            ]
        },
        "quality": {"row_rules": extra_rules or []},
        "transformations": [{"phase": "pre", "deduplicate": dedup}],
    }


def _run(engine: str, contract: dict, rows=ROWS):
    proc = DataProcessor(engine=engine, contract=contract)
    result = proc.run(pl.DataFrame(rows, schema={k: pl.Utf8 for k in rows[0]}))
    good = pl.from_pandas(result.good) if not isinstance(result.good, pl.DataFrame) else result.good
    bad = pl.from_pandas(result.bad) if not isinstance(result.bad, pl.DataFrame) else result.bad
    return proc, good, bad


def _errors(bad: pl.DataFrame) -> list:
    col = "_lakelogic_errors"
    out = []
    for v in bad[col].to_list():
        out.append(sorted(v) if isinstance(v, list) else [v])
    return out


def _rule_names(engine_cls, contract):
    return [r.name for r in engine_cls(DataContract(**contract)).get_row_rules()]


DEDUP = {"on": ["trip_id"], "sort_by": ["updated_at"], "order": "desc"}


# ── 1. dedup + an existing required rule ─────────────────────────────────────
@pytest.mark.parametrize("engine", ENGINES)
def test_existing_required_rule_quarantines_blank_keys_once(engine):
    proc, good, bad = _run(engine, _contract(DEDUP, required=True))
    assert bad.height == 5, "all five blank-key rows must reach quarantine"
    assert good.height == 2
    # Attributed to the EXISTING rule only — no second, redundant rule.
    for errs in _errors(bad):
        assert len(errs) == 1 and "trip_id" in errs[0] and "for_dedup" not in errs[0]
    names = _rule_names(PolarsAdapter, _contract(DEDUP, required=True))
    assert "trip_id_required" in names and "trip_id_required_for_dedup" not in names
    counts = proc.last_report["counts"]
    assert counts["dedup_blank_keys"] == 5
    assert counts.get("pre_transform_dropped") == 1  # only the real duplicate
    assert counts["source"] == counts["good"] + counts["quarantined"] + 1


@pytest.mark.parametrize("engine", ENGINES)
def test_author_written_not_null_rule_also_counts_as_coverage(engine):
    rules = [{"name": "trip_id_present", "sql": "trip_id IS NOT NULL"}]
    proc, good, bad = _run(engine, _contract(DEDUP, extra_rules=rules))
    assert bad.height == 5
    assert "trip_id_required_for_dedup" not in _rule_names(DuckDBAdapter, _contract(DEDUP, extra_rules=rules))


# ── 2. dedup only → the automatic named rule ─────────────────────────────────
@pytest.mark.parametrize("engine", ENGINES)
def test_dedup_only_quarantines_blank_keys_under_named_rule(engine):
    proc, good, bad = _run(engine, _contract(DEDUP))
    assert bad.height == 5
    assert good.height == 2
    names = _rule_names(PolarsAdapter, _contract(DEDUP))
    assert names.count("trip_id_required_for_dedup") == 1
    for errs in _errors(bad):
        assert len(errs) == 1
        assert "trip_id_required_for_dedup" in errs[0] or "dedup key must not be blank" in errs[0]
    counts = proc.last_report["counts"]
    assert counts["dedup_blank_keys"] == 5
    assert counts["quarantined"] == 5
    assert counts["pre_transform_dropped"] == 1
    assert counts.get("deduplicated") == 1


# ── 3. blank_keys: keep ──────────────────────────────────────────────────────
@pytest.mark.parametrize("engine", ENGINES)
def test_keep_passes_blank_keys_through(engine):
    proc, good, bad = _run(engine, _contract({**DEDUP, "blank_keys": "keep"}))
    assert bad.height == 0
    assert good.height == 7  # 5 blank + id=1 (collapsed) + id=2
    assert good.filter(pl.col("trip_id").is_null()).height == 5
    assert not any(
        n.endswith("_required_for_dedup")
        for n in _rule_names(PolarsAdapter, _contract({**DEDUP, "blank_keys": "keep"}))
    )
    counts = proc.last_report["counts"]
    assert counts["dedup_blank_keys"] == 5
    assert counts["pre_transform_dropped"] == 1


# ── 4. real duplicates still collapse, honouring sort_by ─────────────────────
@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("order,expected", [("desc", "new"), ("asc", "old")])
def test_real_duplicates_still_collapse_by_sort_by(engine, order, expected):
    _, good, _ = _run(engine, _contract({**DEDUP, "order": order}))
    ones = good.filter(pl.col("trip_id") == "1")
    assert ones.height == 1
    assert ones["v"][0] == expected


# ── composite key with one part null ─────────────────────────────────────────
@pytest.mark.parametrize("engine", ENGINES)
def test_composite_key_with_one_part_null_is_blank(engine):
    rows = [
        {"trip_id": "1", "trip_date": None, "updated_at": "a", "v": "x1"},
        {"trip_id": "1", "trip_date": None, "updated_at": "b", "v": "x2"},
        {"trip_id": None, "trip_date": "d", "updated_at": "c", "v": "x3"},
        {"trip_id": "1", "trip_date": "d", "updated_at": "a", "v": "keep-old"},
        {"trip_id": "1", "trip_date": "d", "updated_at": "b", "v": "keep-new"},
    ]
    dedup = {"on": ["trip_id", "trip_date"], "sort_by": ["updated_at"]}
    proc, good, bad = _run(engine, _contract(dedup), rows)
    assert bad.height == 3
    assert good["v"].to_list() == ["keep-new"]
    assert "trip_id__trip_date_required_for_dedup" in _rule_names(PolarsAdapter, _contract(dedup))
    assert proc.last_report["counts"]["dedup_blank_keys"] == 3


@pytest.mark.parametrize("engine", ENGINES)
def test_composite_key_partially_covered_still_gets_the_dedup_rule(engine):
    """Only trip_id is required; trip_date is not — a null trip_date must still be caught."""
    dedup = {"on": ["trip_id", "trip_date"], "sort_by": ["updated_at"]}
    names = _rule_names(PolarsAdapter, _contract(dedup, required=True))
    assert "trip_id_required" in names and "trip_id__trip_date_required_for_dedup" in names


def test_empty_string_key_is_a_value_not_blank():
    rows = [
        {"trip_id": "", "trip_date": "d", "updated_at": "a", "v": "e1"},
        {"trip_id": "", "trip_date": "d", "updated_at": "b", "v": "e2"},
    ]
    proc, good, bad = _run("polars", _contract(DEDUP), rows)
    assert bad.height == 0 and good["v"].to_list() == ["e2"]
    assert proc.last_report["counts"]["dedup_blank_keys"] == 0


def test_no_dedup_means_count_not_measured():
    c = _contract(DEDUP)
    c["transformations"] = []
    proc, _, _ = _run("polars", c)
    assert "dedup_blank_keys" not in proc.last_report["counts"]


def test_run_log_metadata_carries_the_count():
    from lakelogic.core import run_log as rl
    import inspect

    assert '"rows_blank_dedup_key": _counts.get("dedup_blank_keys")' in inspect.getsource(rl)


# ── warehouse SQL generators (no live warehouse here) ────────────────────────
def test_generic_sql_ranks_blank_keys_first_so_none_collapse():
    from lakelogic.engines.generic_sql import GenericSQLAdapter  # noqa: F401
    import inspect
    from lakelogic.engines import bigquery, generic_sql, snowflake

    for mod in (snowflake, bigquery, generic_sql):
        src = inspect.getsource(mod)
        assert "CASE WHEN {blank} THEN 1" in src, mod.__name__


# ── Spark (real JVM; runs with RUN_SPARK_TESTS=1) ─────────────────────────────
@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    s = SparkSession.getActiveSession()
    if s is None:
        s = (
            SparkSession.builder.master("local[1]")
            .appName("lakelogic-dedup-blank-keys")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
    yield s


def _spark_run(spark, contract, rows=ROWS):
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField(k, StringType(), True) for k in rows[0]])
    df = spark.createDataFrame([tuple(r[k] for k in rows[0]) for r in rows], schema)
    proc = DataProcessor(engine="spark", contract=contract)
    result = proc.run(df)
    return proc, result.good, result.bad


def test_spark_four_cases(spark):
    proc, good, bad = _spark_run(spark, _contract(DEDUP, required=True))
    assert bad.count() == 5 and good.count() == 2
    assert proc.last_report["counts"]["dedup_blank_keys"] == 5

    proc, good, bad = _spark_run(spark, _contract(DEDUP))
    assert bad.count() == 5
    errs = [r["_lakelogic_errors"] for r in bad.collect()]
    assert all(len(e) == 1 and "trip_id_required_for_dedup" in e[0] for e in errs), errs

    proc, good, bad = _spark_run(spark, _contract({**DEDUP, "blank_keys": "keep"}))
    assert bad.count() == 0 and good.count() == 7

    ones = good.filter("trip_id = '1'").collect()
    assert len(ones) == 1 and ones[0]["v"] == "new"
