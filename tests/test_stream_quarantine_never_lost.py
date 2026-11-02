"""Found by the streaming benchmark (2026-11-02), beside an Iceberg table.

1. The quarantine inherited the table's format, so it tried to write Iceberg to a plain path and
   failed every time. A path quarantine beside an Iceberg table is now Delta.
2. That failure was logged as a warning and the stream committed its position anyway: 991 rows
   were reported bad, none were saved, and the next run never read them again. In a stream a
   failed quarantine write now stops the batch before the commit, so it is retried.
"""

import pytest

pl = pytest.importorskip("polars")
pytest.importorskip("deltalake")

from lakelogic import DataProcessor, SQLiteCheckpointStore, StreamSink  # noqa: E402
from lakelogic.core.quarantine import materialize_quarantine  # noqa: E402

EVENTS = [{"ride_id": 1, "fare": 10.0}, {"ride_id": 2, "fare": -1.0}, {"ride_id": 3, "fare": 7.5}]


def _contract(tmp_path, table_format="delta", quarantine=None):
    return {
        "version": "1.0.0",
        "info": {"title": "rides"},
        "dataset": "rides",
        "primary_key": ["ride_id"],
        "model": {
            "fields": [{"name": "ride_id", "type": "long", "required": True}, {"name": "fare", "type": "double"}]
        },
        "quality": {"row_rules": [{"name": "fare_positive", "sql": "fare > 0"}]},
        "materialization": {"strategy": "merge", "format": table_format, "target_path": str(tmp_path / "rides")},
        "quarantine": quarantine or {"target": str(tmp_path / "quarantine")},
    }


def test_a_path_quarantine_beside_an_iceberg_table_is_delta(tmp_path):
    contract = _contract(tmp_path)
    contract["materialization"] = {"strategy": "merge", "format": "iceberg", "target_path": "table:local.ns.rides"}
    proc = DataProcessor(contract, engine="polars")
    bad = pl.DataFrame({"ride_id": [2], "fare": [-1.0], "_lakelogic_errors": [["Rule failed: fare_positive"]]})

    out = materialize_quarantine(bad, proc.contract, engine_name="polars")

    assert out["format"] == "delta"
    assert pl.read_delta(str(tmp_path / "quarantine" / "rides")).height == 1


def test_an_explicit_quarantine_format_still_wins(tmp_path):
    contract = _contract(tmp_path, quarantine={"target": str(tmp_path / "q"), "format": "parquet"})
    contract["materialization"]["format"] = "iceberg"
    proc = DataProcessor(contract, engine="polars")
    bad = pl.DataFrame({"ride_id": [2], "fare": [-1.0], "_lakelogic_errors": [["x"]]})
    assert materialize_quarantine(bad, proc.contract, engine_name="polars")["format"] == "parquet"


def _failing_quarantine(tmp_path):
    # An Iceberg quarantine at a plain path cannot be written: a reliable write failure.
    return _contract(tmp_path, quarantine={"target": str(tmp_path / "q"), "format": "iceberg"})


def test_a_stream_does_not_commit_a_batch_whose_quarantine_failed(tmp_path):
    store = SQLiteCheckpointStore(tmp_path / "ckpt.sqlite")
    sink = StreamSink(
        contract=_failing_quarantine(tmp_path),
        source=list(EVENTS),
        engine="polars",
        checkpoint=store,
        checkpoint_key="rides",
        batch_size=10,
    )
    with pytest.raises(RuntimeError, match="not committed and will be retried"):
        sink.run("available_now")
    assert store.load("rides") is None  # nothing committed: the next run reads these events again


def test_a_batch_run_still_continues_with_a_warning(tmp_path):
    proc = DataProcessor(_failing_quarantine(tmp_path), engine="polars")
    result = proc.run(pl.DataFrame(EVENTS))
    proc.materialize(result.good, result.bad)  # no raise outside a stream
    assert pl.read_delta(str(tmp_path / "rides")).height == 2
