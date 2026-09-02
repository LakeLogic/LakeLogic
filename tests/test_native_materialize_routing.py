"""``materialize()`` must route warehouse writes server-side — and only when safe.

On Snowflake/BigQuery the accepted rows are already a table in the warehouse, so
writing them to another table there is a CTAS. The default path instead fetched
them into pandas and shipped them back through dlt: warehouse -> driver ->
warehouse, bounded by driver memory for data that never needed to move.

The routing is an OPTIMISATION, so the tests below care as much about when it
declines as when it fires. An optimisation that changes where data lands, or that
breaks a pipeline which used to work, is worse than the round trip it saves.
"""

from __future__ import annotations

import pytest

from lakelogic.core.processor import DataProcessor


class _FakeAdapter:
    """Stands in for a warehouse adapter that has completed a run."""

    def __init__(self, good_table="LG_GOOD_1", fail=False):
        self.good_table = good_table
        self.fail = fail
        self.calls: list[tuple[str, str]] = []

    def materialize_native(self, target_table, strategy="overwrite"):
        self.calls.append((target_table, strategy))
        if self.fail:
            raise RuntimeError("no shared connection")
        return {"target": target_table, "rows_written": 5, "strategy": strategy, "server_side": True}


class _NoNativeAdapter:
    """An engine with no server-side writer at all (polars/duckdb/spark)."""

    good_table = "irrelevant"


def _processor(adapter, *, strategy="overwrite", target="table:DB.SCHEMA.T"):
    p = object.__new__(DataProcessor)  # bypass __init__ (needs a real engine)
    p.adapter = adapter
    p.engine_name = "snowflake"
    p.contract = type("C", (), {"materialization": type("M", (), {"strategy": strategy, "target_path": target})()})()
    return p


# ── fires when it should ─────────────────────────────────────────────────────


@pytest.mark.parametrize("strategy", ["append", "overwrite", "replace"])
def test_routes_server_side_for_supported_strategies(strategy):
    adapter = _FakeAdapter()
    out = _processor(adapter, strategy=strategy)._try_native_materialize("table:DB.SCHEMA.T")

    assert out is not None and out["server_side"] is True
    assert adapter.calls == [("DB.SCHEMA.T", strategy)]  # `table:` prefix stripped


# ── declines when it should (each returns None -> caller uses the old path) ──


def test_declines_for_strategies_the_native_writer_does_not_implement():
    """merge/scd2 must reach the dlt path, NOT raise. Deciding before calling — rather
    than catching the adapter's NotImplementedError — is what keeps them working."""
    for strategy in ["merge", "scd2", "upsert"]:
        adapter = _FakeAdapter()
        assert _processor(adapter, strategy=strategy)._try_native_materialize("table:X") is None
        assert adapter.calls == []


def test_declines_for_file_and_uri_targets():
    """A file target genuinely needs the data to leave the warehouse, so the round
    trip there is not waste."""
    for target in ["s3://bucket/path", "gs://bucket/path", "/local/path/out.parquet", "out.delta"]:
        adapter = _FakeAdapter()
        assert _processor(adapter)._try_native_materialize(target) is None
        assert adapter.calls == []


def test_declines_when_the_engine_has_no_native_writer():
    assert _processor(_NoNativeAdapter())._try_native_materialize("table:X") is None


def test_declines_when_no_run_has_completed():
    """good_table unset means either no run, or results already dropped with the
    session. Either way there is nothing to copy."""
    assert _processor(_FakeAdapter(good_table=None))._try_native_materialize("table:X") is None


def test_declines_when_there_is_no_target():
    adapter = _FakeAdapter()
    assert _processor(adapter, target=None)._try_native_materialize(None) is None
    assert adapter.calls == []


# ── never breaks a write that would otherwise have worked ────────────────────


def test_falls_back_and_warns_when_the_native_write_fails(caplog):
    """The fast path failing must not fail the run — the standard path still has to
    get its chance, and the reason must be visible rather than swallowed."""
    from loguru import logger

    records: list[str] = []
    sink = logger.add(lambda m: records.append(str(m)), level="WARNING", format="{message}")
    try:
        adapter = _FakeAdapter(fail=True)
        out = _processor(adapter)._try_native_materialize("table:DB.T")
    finally:
        logger.remove(sink)

    assert out is None  # -> caller falls back to materialize_dataframe
    assert adapter.calls  # it genuinely tried
    warning = " ".join(records)
    assert "falling back" in warning
    assert "no shared connection" in warning  # the real cause, not a generic message
