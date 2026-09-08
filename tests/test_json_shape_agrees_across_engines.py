"""The same JSON file must yield the same rows on every engine.

THE LIVE DIVERGENCE. One landing zone, one contract, two answers:

    Test  (polars, local dry run)   passed - 200 rows
    Fabric (spark)                  bronze read 10 rows

Neither engine was wrong on its own terms. Polars' `_read_json_flat` auto-detects the shape —
it tries the whole file as one JSON value and falls back to line-by-line. Spark's reader was
told `multiLine=true` unconditionally, which parses each FILE as a single value, so ten JSON
Lines files became ten rows and 190 were discarded with no error.

The green dry-run is the part that matters. The design's Test stage runs on polars and said
"passed on polars - contract-derived rows" over a contract that lost 95% of its data in
production. A cross-engine disagreement about a READ cannot be caught by any amount of
testing on one engine.

The conformance harness could not catch it either, and said so in its own comment:
`_materialise_source` wrote parquet deliberately, "so the case measures the runtime rather
than a text parser". For JSON that distinction is false — the parser IS the runtime — and the
bug walked straight through the blind spot. Cases can now declare `source_format: json` or
`jsonl`, and OLC-S-002 / OLC-S-003 run the same rows in both layouts across duckdb, polars
and Spark. OLC-S-003 failed on Spark the first time it ran, which is the point of it.

These tests are the polars-side unit cover; the cross-engine guarantee lives in the harness.
"""

import json
from pathlib import Path

import polars as pl
import pytest


ROWS = [{"driver_id": f"d{i}", "lat": 51.5 + i / 1000, "city_code": "LON"} for i in range(20)]


def _contract(tmp_path: Path, *, options=None) -> dict:
    source = {"type": "file", "path": f"{tmp_path}/*.json", "format": "json"}
    if options is not None:
        source["options"] = options
    return {
        "version": "1.0.0",
        "info": {"title": "driver_locations", "version": "1.0.0"},
        "source": source,
        "model": {
            "fields": [
                {"name": "driver_id", "type": "string"},
                {"name": "lat", "type": "double"},
                {"name": "city_code", "type": "string"},
            ]
        },
    }


def _write_array(dirpath: Path, batches: int = 4) -> None:
    """One JSON VALUE per file: an array of rows — what `multiLine=true` expects."""
    size = len(ROWS) // batches
    for i in range(batches):
        chunk = ROWS[i * size : (i + 1) * size]
        (dirpath / f"batch_{i:02d}.json").write_text(json.dumps(chunk), encoding="utf-8")


def _write_ndjson(dirpath: Path, batches: int = 4) -> None:
    """One JSON value per LINE — the ordinary landing-zone shape, and Spark's own default."""
    size = len(ROWS) // batches
    for i in range(batches):
        chunk = ROWS[i * size : (i + 1) * size]
        (dirpath / f"batch_{i:02d}.json").write_text("\n".join(json.dumps(r) for r in chunk), encoding="utf-8")


def _read(tmp_path: Path, contract: dict) -> int:
    from lakelogic.core.processor import DataProcessor

    good, _bad = DataProcessor(contract=contract, engine="polars").run_source(str(tmp_path / "*.json"))
    return good.height if hasattr(good, "height") else len(good)


# ── polars reads BOTH shapes, whichever the landing zone holds ───────────────────────────


@pytest.mark.parametrize("writer", [_write_array, _write_ndjson], ids=["array", "ndjson"])
def test_every_row_is_read_whatever_shape_the_files_are(tmp_path, writer):
    writer(tmp_path)
    assert _read(tmp_path, _contract(tmp_path)) == len(ROWS)


def test_a_local_ndjson_file_reads(tmp_path):
    """The local branch called `json.loads` bare, with no line-by-line fallback — so a LOCAL
    JSON Lines file raised while the identical file in cloud storage read fine."""
    _write_ndjson(tmp_path, batches=1)
    assert _read(tmp_path, _contract(tmp_path)) == len(ROWS)


# ── the contract is authoritative on both engines ────────────────────────────────────────


def test_a_declared_shape_is_honoured(tmp_path):
    """`source.options.multiLine` is the SAME field the Spark reader consumes. A contract
    that declares its shape must get the same answer from either engine."""
    _write_ndjson(tmp_path)
    assert _read(tmp_path, _contract(tmp_path, options={"multiLine": False})) == len(ROWS)


def test_either_spelling_is_accepted(tmp_path):
    _write_ndjson(tmp_path)
    assert _read(tmp_path, _contract(tmp_path, options={"multiline": False})) == len(ROWS)


def test_a_contract_that_no_longer_describes_its_data_fails_loudly(tmp_path):
    """Declared one-value-per-file, given JSON Lines. Coping silently would hide a contract
    that has drifted from its data — exactly the failure the field exists to expose. The
    default (no declaration) still auto-detects, so this is opt-in strictness."""
    _write_ndjson(tmp_path, batches=1)
    with pytest.raises(Exception):
        _read(tmp_path, _contract(tmp_path, options={"multiLine": True}))


def test_the_default_is_still_auto_detection(tmp_path):
    """Absent a declaration, trying the whole file and falling back is strictly better than a
    guess — and no existing contract changes behaviour."""
    _write_ndjson(tmp_path, batches=1)
    assert _read(tmp_path, _contract(tmp_path, options={})) == len(ROWS)


# ── the two engines read the same field ──────────────────────────────────────────────────


def test_both_engines_consult_the_same_contract_field():
    """Prose invariants do not hold across engines; a shared field does. Pinned so a future
    edit to one reader is visibly a divergence from the other."""
    import inspect

    from lakelogic.core import processor

    src = inspect.getsource(processor)
    spark_branch = src[src.index('elif fmt == "json":') :][:2600]
    polars_branch = src[src.index("def _parse_json_text") :][:2600]
    for branch in (spark_branch, polars_branch):
        assert '"multiLine"' in branch and '"multiline"' in branch
    # And both fall back to reading the DATA when the contract says nothing, rather than one
    # detecting and the other assuming — which is the divergence itself.
    assert "_json_is_one_value_per_file" in spark_branch
    assert "JSONDecodeError" in polars_branch
