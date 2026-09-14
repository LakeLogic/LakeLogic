"""A merge must not die because the plan-truncation optimisation is unavailable.

Before a merge, the Spark path materialises the incoming frame once so Catalyst does not compile
the same lineage twice (see `_materialize_spark_dataframe`). It tries `localCheckpoint(eager=True)`
and falls back to `persist() + count()`.

DATABRICKS SERVERLESS REJECTS BOTH: `localCheckpoint` needs an RDD, and persist raises
`[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute`. The
fallback was unguarded, so that second exception escaped and failed the contract — every silver
and gold merge on serverless compute, with a message that named an optimisation rather than
anything the user declared. It is an optimisation, so the merge now proceeds without it.
"""

from __future__ import annotations

import types

import pytest

from lakelogic.core import materialization as mat


class _ServerlessFrame:
    """Rejects both plan-truncation calls the way Databricks serverless does."""

    def __init__(self):
        self.columns = ["id", "country_code"]
        self.sparkSession = types.SimpleNamespace()
        self.persist_attempted = False

    def localCheckpoint(self, eager=True):
        raise RuntimeError("checkpointDir has not been set")

    def persist(self):
        self.persist_attempted = True
        raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute.")

    def count(self):  # pragma: no cover - never reached once persist raises
        return 2


def _contract():
    return types.SimpleNamespace(
        materialization=types.SimpleNamespace(
            strategy="merge",
            partition_by=["country_code"],
            scd2=None,
            location=None,
            soft_delete_column=None,
            soft_delete_value=True,
            soft_delete_time_column=None,
            soft_delete_reason_column=None,
            merge_dedup_guard=False,
            unknown_member=None,
            track_columns=None,
        ),
        primary_key=["id"],
        model=None,  # `_spark_apply_table_metadata` runs after the merge and reads it
        lineage=None,
        source=types.SimpleNamespace(cdc_op_field=None, cdc_delete_values=None, cdc_timestamp_field=None),
        transformations=[{"phase": "pre", "sql": "SELECT * FROM source"}],
        effective_server=lambda: None,
    )


@pytest.fixture()
def merge_spy(monkeypatch):
    calls = []
    monkeypatch.setattr(
        mat,
        "_spark_merge_dataframe",
        lambda *args, **kwargs: calls.append(args) or {"target": args[2], "rows_written": 2, "format": args[4]},
    )
    return calls


def test_the_merge_still_runs_when_neither_checkpoint_nor_persist_is_available(merge_spy):
    """THE DEFECT: the persist fallback was unguarded, so serverless's refusal killed the merge."""
    df = _ServerlessFrame()

    result = mat._materialize_spark_dataframe(df, _contract(), mat.URIPath("table:cat.sch.trips"), "delta")

    assert df.persist_attempted, "the fallback must still be tried — it works on classic compute"
    assert len(merge_spy) == 1, "the merge did not run"
    assert result["rows_written"] == 2


def test_a_contract_without_transformations_never_touches_either(merge_spy):
    """The truncation is gated on transformations; a pass-through contract skips it entirely."""
    df = _ServerlessFrame()
    contract = _contract()
    contract.transformations = []

    mat._materialize_spark_dataframe(df, contract, mat.URIPath("table:cat.sch.trips"), "delta")

    assert not df.persist_attempted
    assert len(merge_spy) == 1
