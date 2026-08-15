"""The DuckDB engine must FAIL LOUD on transforms it does not implement,
rather than silently no-op'ing them (which produced wrong output with no error).

As of the conformance sweep every declared transform op IS implemented, so the
guard's block-list is empty in practice. These tests verify the *mechanism* still
works — i.e. a future op added to the model but not the engine fails loud — by
temporarily removing an op from the supported set.
"""
import polars as pl
import pytest

from lakelogic import DataProcessor
from lakelogic.engines.duckdb import DuckDBAdapter

MODEL = [{"name": "name", "type": "string"}, {"name": "id", "type": "integer"}]
ROWS = pl.DataFrame([{"name": "ABC", "id": 1}])


def _run(transform):
    contract = {
        "version": "1.0.0",
        "info": {"title": "T", "table_name": "t"},
        "model": {"fields": MODEL},
        "transformations": [dict(phase="post", **transform)],
    }
    return DataProcessor(engine="duckdb", contract=contract).run(ROWS)


def test_guard_raises_when_an_op_is_unsupported(monkeypatch):
    # Simulate a not-yet-implemented op by removing it from the supported set.
    monkeypatch.setattr(
        DuckDBAdapter,
        "_DUCKDB_SUPPORTED_TRANSFORMS",
        DuckDBAdapter._DUCKDB_SUPPORTED_TRANSFORMS - {"lower"},
    )
    with pytest.raises(ValueError, match="lower"):
        _run({"lower": {"fields": ["name"]}})


def test_every_model_transform_op_is_implemented():
    # The whole vocabulary is now supported — the guard should never fire in
    # normal operation. If a new op is added to the model, add its handler (and
    # a conformance case) or this fails, flagging the silent-no-op risk.
    from lakelogic.core.models import Transformation

    ops = {f for f in Transformation.model_fields if f != "phase"}
    unsupported = ops - set(DuckDBAdapter._DUCKDB_SUPPORTED_TRANSFORMS)
    assert not unsupported, f"DuckDB engine is missing handlers for: {sorted(unsupported)}"


@pytest.mark.parametrize(
    "transform",
    [
        {"derive": {"field": "x", "sql": "id + 1"}},
        {"filter": {"sql": "id > 0"}},
        {"lower": {"fields": ["name"]}},
        {"upper": {"fields": ["name"]}},
        {"trim": {"fields": ["name"]}},
        {"cast": {"columns": {"id": "string"}}},
        {"coalesce": {"field": "out", "sources": ["name"]}},
        {"select": {"columns": ["id", "name"]}},
        {"drop": {"columns": ["name"]}},
        {"map_values": {"field": "name", "mapping": {"ABC": "Z"}}},
        {"bucket": {"field": "b", "source": "id", "bins": [{"label": "x", "lt": 5}]}},
    ],
)
def test_supported_transform_still_runs(transform):
    good, _ = _run(transform)
    assert good is not None
