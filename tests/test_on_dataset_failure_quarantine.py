"""quality.on_dataset_failure: quarantine - a failed DATASET rule sets the batch aside and the
run carries on (owner, 2026-11-18: "keep certain batch[es] and carry on ... a production use
case"). Row rules are unchanged.
"""
from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")
pytest.importorskip("deltalake")


def _contract(tmp_path, policy: str = "quarantine") -> dict:
    return {
        "version": "1.0.0",
        "info": {"title": "orders"},
        "dataset": "orders",
        "primary_key": ["order_id"],
        "model": {"fields": [{"name": "order_id", "type": "integer"}, {"name": "amount", "type": "integer"}]},
        "quality": {
            "on_dataset_failure": policy,
            "row_rules": [{"name": "amount_positive", "sql": "amount > 0"}],
            "dataset_rules": [{"unique": "order_id"}],
        },
        "materialization": {"strategy": "merge", "format": "delta", "target_path": str(tmp_path / "target")},
        "quarantine": {"target": str(tmp_path / "quarantine"), "format": "parquet"},
    }


def _batch(tmp_path, name: str, rows: str) -> str:
    path = tmp_path / name
    path.write_text("order_id,amount\n" + rows, encoding="utf-8")
    return str(path)


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_a_failed_batch_is_set_aside_and_the_others_land(tmp_path, engine):
    from lakelogic import DataProcessor

    batches = [
        _batch(tmp_path, "b1.csv", "1,10\n2,20\n"),
        _batch(tmp_path, "b2.csv", "3,30\n3,31\n4,-1\n"),      # duplicate 3: fails unique
        _batch(tmp_path, "b3.csv", "5,50\n"),
    ]
    outcomes = []
    for path in batches:
        proc = DataProcessor(_contract(tmp_path), engine=engine)
        result = proc.run_source(path)
        proc.materialize(result.good, result.bad)
        outcomes.append((proc.batch_quarantined, len(result.good), len(result.bad), proc.last_report))

    assert [o[0] is not None for o in outcomes] == [False, True, False]
    reason, good_n, bad_n, report = outcomes[1]
    assert good_n == 0 and bad_n == 3                  # the WHOLE batch, including the row-rule reject
    assert "order_id_unique: 1 duplicate value(s)" in reason
    assert report["batch_quarantined"] is True

    target = pl.read_delta(str(tmp_path / "target")).sort("order_id")
    assert target["order_id"].to_list() == [1, 2, 5]   # batches 1 and 3; nothing from batch 2
    quarantined = pl.read_parquet(str(tmp_path / "quarantine" / "*.parquet"))
    assert sorted(quarantined["order_id"].to_list()) == [3, 3, 4]
    errors = [e for row in quarantined["_lakelogic_errors"].to_list() for e in row]
    assert any(e.startswith("Batch rejected: order_id_unique") for e in errors)
    assert "Rule failed: amount_positive (amount > 0)" in errors   # row reason kept


def test_continue_and_stop_are_unchanged(tmp_path):
    from lakelogic import DataProcessor
    from lakelogic.core.processor import DatasetRulesFailed

    path = _batch(tmp_path, "b.csv", "3,30\n3,31\n")
    proc = DataProcessor(_contract(tmp_path, "continue"), engine="polars")
    result = proc.run_source(path)
    assert proc.batch_quarantined is None and len(result.good) == 2

    with pytest.raises(DatasetRulesFailed):
        DataProcessor(_contract(tmp_path, "stop"), engine="polars").run_source(path)


def test_the_old_flag_still_means_stop(tmp_path):
    from lakelogic import DataProcessor
    from lakelogic.core.processor import DatasetRulesFailed

    c = _contract(tmp_path)
    c["quality"].pop("on_dataset_failure")
    c["quality"]["fail_pipeline_on_dataset_error"] = True
    with pytest.raises(DatasetRulesFailed):
        DataProcessor(c, engine="polars").run_source(_batch(tmp_path, "b.csv", "3,30\n3,31\n"))


def test_a_pipeline_moves_the_quarantined_batch_files_and_carries_on(tmp_path):
    import yaml

    from lakelogic.core.registry import DomainRegistry
    from lakelogic.pipeline.runner import LakehousePipeline

    landing = tmp_path / "landing" / "orders"
    landing.mkdir(parents=True)
    (landing / "batch.csv").write_text("order_id,amount\n3,30\n3,31\n", encoding="utf-8")
    contract = _contract(tmp_path)
    contract["info"] |= {"domain": "sales", "system": "shop", "target_layer": "bronze"}
    contract["source"] = {"type": "landing", "path": str(landing), "format": "csv",
                          "post_ingestion": {"action": "archive", "on_error": "move",
                                             "archive_path": str(tmp_path / "archive")}}
    (tmp_path / "orders.yaml").write_text(yaml.safe_dump(contract), encoding="utf-8")
    (tmp_path / "_registry.yaml").write_text(yaml.safe_dump({
        "domain": "sales", "system": "shop", "storage_mode": "path",
        "storage": {"external_location_root": str(tmp_path / "lake")},
        "contracts": [{"layer": "bronze", "entity": "orders", "path": "orders.yaml"}],
    }), encoding="utf-8")

    registry = DomainRegistry.from_yaml(str(tmp_path / "_registry.yaml"), environment="dev", storage_mode="path")
    LakehousePipeline(registry, engine="polars").run()     # carries on: no exception

    assert not (landing / "batch.csv").exists()
    assert list((tmp_path / "archive").rglob("error/**/batch.csv")), "the batch file should be in error/"
    assert not list((tmp_path / "archive").rglob("success/**/*.csv"))
    assert not (tmp_path / "target").exists() or pl.read_delta(str(tmp_path / "target")).height == 0
