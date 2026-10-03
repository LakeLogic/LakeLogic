"""SCD1 surrogate key on the PARTITION-AWARE merge path.

Frames carry the declared SK column as null, exactly as the processor hands them over.

A merge dimension whose `_system.yaml` declares `partition_by` (often a superset, pruned to
nothing for this table) is routed through `_partition_aware_merge`. That path never handed
`materialization.scd1` to `_merge_frames`, and on first write skipped `_merge_frames`
entirely — so the surrogate key column was written all-null and no unknown member existed.
Seen in the Sandbox on gold_rideflow_dim_cancel_reason_code (2026-10-02): 39 rows, 39 null SKs.
"""

from __future__ import annotations

import hashlib

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from lakelogic.core.materialization import materialize_dataframe
from lakelogic.core.models import DataContract

SK = "reason_sk"


def _contract(partition_by):
    return DataContract.model_validate(
        {
            "version": "1.0.0",
            "info": {"title": "dim_reason", "target_layer": "gold"},
            "model": {"fields": [{"name": SK, "type": "string"}, {"name": "reason", "type": "string"}]},
            "primary_key": ["reason"],
            "materialization": {
                "strategy": "merge",
                "partition_by": partition_by,
                "scd1": {
                    "surrogate_key": SK,
                    "surrogate_key_strategy": "hash",
                    "unknown_member": {"enabled": True, "surrogate_key_value": "-1"},
                },
            },
        }
    )


def _hash(v):
    return hashlib.sha256(v.encode("utf-8")).hexdigest()[:16]


def _read(target, fmt):
    if fmt == "delta":
        from deltalake import DeltaTable

        return DeltaTable(str(target)).to_pandas()
    return pd.concat([pd.read_parquet(f) for f in target.rglob("*.parquet")], ignore_index=True)


@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize("runs", [1, 2])
def test_pruned_partition_merge_writes_the_surrogate_key(tmp_path, fmt, runs):
    if fmt == "delta":
        pytest.importorskip("deltalake")
    target = tmp_path / "dim_reason"
    contract = _contract(["country_code"])  # not in the data: pruned to no partitions
    for _ in range(runs):
        materialize_dataframe(
            pd.DataFrame({SK: [None, None], "reason": ["A", "B"]}),
            contract,
            target,
            output_format=fmt,
            engine_name="pandas",
        )
    out = _read(target, fmt)
    assert out[SK].notna().all()
    keyed = dict(zip(out["reason"], out[SK]))
    assert keyed["A"] == _hash("A") and keyed["B"] == _hash("B")
    assert (out[SK] == "-1").sum() == 1
    assert out[SK].is_unique


def test_partitioned_delta_merge_writes_the_surrogate_key(tmp_path):
    pytest.importorskip("deltalake")
    target = tmp_path / "dim_reason"
    df = pd.DataFrame({SK: [None, None], "reason": ["A", "B"], "region": ["eu", "us"]})
    materialize_dataframe(df, _contract(["region"]), target, output_format="delta", engine_name="pandas")
    out = _read(target, "delta")
    real = out[out[SK] != "-1"]
    assert sorted(real[SK]) == sorted([_hash("A"), _hash("B")])
