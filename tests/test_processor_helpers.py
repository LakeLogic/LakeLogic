from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core import processor as proc_mod
from lakelogic.core.models import DataContract, Quality


def test_validation_result_counts_and_dunder_methods():
    good = pl.DataFrame({"id": [1, 2]})
    bad = [{"id": 3}]
    raw = types.SimpleNamespace(count=lambda: 4)
    result = proc_mod.ValidationResult(good, bad, raw=raw, trace=["step"])

    assert result.source_count == 4
    assert result.good_count == 2
    assert result.bad_count == 1
    assert list(result) == [good, bad]
    assert result[0] is good
    assert len(result) == 2
    assert "good=2" in repr(result)


def test_processor_engine_discovery_and_adapter_dispatch(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)

    monkeypatch.setenv("LAKELOGIC_ENGINE", "duckdb")
    assert processor._discover_engine() == "duckdb"
    monkeypatch.delenv("LAKELOGIC_ENGINE")
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    assert processor._discover_engine() == "spark"
    del sys.modules["pyspark"]
    assert processor._discover_engine() == "polars"

    fake_polars_module = types.ModuleType("lakelogic.engines.polars")
    fake_polars_module.PolarsAdapter = lambda contract: ("polars", contract)
    fake_duckdb_module = types.ModuleType("lakelogic.engines.duckdb")
    fake_duckdb_module.DuckDBAdapter = lambda contract: ("duckdb", contract)
    monkeypatch.setitem(sys.modules, "lakelogic.engines.polars", fake_polars_module)
    monkeypatch.setitem(sys.modules, "lakelogic.engines.duckdb", fake_duckdb_module)

    processor.contract = {"contract": True}
    processor.engine_name = "polars"
    assert processor._get_adapter() == ("polars", {"contract": True})
    processor.engine_name = "duckdb"
    assert processor._get_adapter() == ("duckdb", {"contract": True})
    processor.engine_name = "unknown"
    with pytest.raises(ValueError, match="Unsupported engine"):
        processor._get_adapter()


def test_processor_storage_path_and_empty_frame_helpers(monkeypatch, tmp_path):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.contract = types.SimpleNamespace(_base_path=tmp_path)

    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "acct")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", "key")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
    assert (
        processor._get_cloud_storage_options("abfss://container@acct.dfs.core.windows.net/path")["account_key"] == "key"
    )
    assert processor._get_cloud_storage_options("s3://bucket/path") == {"key": "aws-key", "secret": "aws-secret"}

    monkeypatch.setattr(processor, "_is_uri_path", lambda path: path.startswith("abfss://"))
    assert processor._resolve_source_path("abfss://bucket/path") == "abfss://bucket/path"

    relative = tmp_path / "orders.csv"
    relative.write_text("id\n1\n", encoding="utf-8")
    cwd = Path.cwd()
    try:
        import os

        os.chdir(tmp_path)
        assert processor._resolve_source_path("orders.csv").endswith("orders.csv")
    finally:
        os.chdir(cwd)

    nested = tmp_path / "nested"
    nested.mkdir()
    nested_file = nested / "events.csv"
    nested_file.write_text("id\n1\n", encoding="utf-8")
    processor.contract = types.SimpleNamespace(_base_path=tmp_path)
    assert processor._resolve_source_path(Path("nested/events.csv")).endswith("events.csv")
    assert processor._empty_frame().is_empty()


def test_processor_counts_report_and_delegates(monkeypatch, tmp_path):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.adapter = types.SimpleNamespace(dataset_rule_results=[{"rule": "x"}], ERROR_COLUMN="_lakelogic_errors")
    processor.contract = types.SimpleNamespace(
        info=types.SimpleNamespace(title="Orders", version="1.0.0", table_name="silver_orders"),
        dataset="orders",
        materialization=types.SimpleNamespace(target_path="table:catalog.schema.orders", path=None),
        metadata={"domain": "commerce", "system": "erp"},
    )
    processor.stage = "silver"
    processor.last_run_id = "run-1"
    processor.pipeline_run_id = "pipe-1"
    processor.last_source_path = str(tmp_path / "orders.csv")
    processor._source_files = [{"path": str(tmp_path / "orders.csv")}]
    processor._source_max_mtime = 123.0
    processor._resolved_domain = "commerce"
    processor._resolved_system = "erp"
    processor._resolved_environment = "dev"
    processor._resolved_data_layer = "silver"
    processor._incremental_metadata = {"mode": "full"}
    processor._run_start_time = None

    counts = processor._compute_counts(
        pl.DataFrame({"id": [1, 2, 3]}),
        pl.DataFrame({"id": [1, 2]}),
        pl.DataFrame({"id": [3]}),
    )
    assert counts == {
        "source": 3,
        "total": 3,
        "good": 2,
        "quarantined": 1,
        "quarantine_ratio": 1 / 3,
        "pre_transform_dropped": 0,
    }

    fake_execution_context = types.ModuleType("lakelogic.core.execution_context")
    fake_execution_context.capture_execution_context = lambda engine_name, start_time=None: {"engine": engine_name}
    monkeypatch.setitem(sys.modules, "lakelogic.core.execution_context", fake_execution_context)
    report = processor._build_report(
        "Orders", counts, slos={"freshness": True}, row_rule_failures=["bad"], schema_drift={"extra": []}
    )
    assert report["dataset"] == "orders"
    assert report["execution_context"] == {"engine": "polars"}
    assert report["counts"]["good"] == 2

    fake_lineage = types.ModuleType("lakelogic.core.lineage")
    fake_lineage.inject_lineage = lambda *args: ("good-lined", "bad-lined")
    fake_lineage._preserve_upstream_lineage = lambda df, columns, prefix, engine_name: (
        df,
        columns,
        prefix,
        engine_name,
    )
    fake_lineage.add_columns = lambda df, columns, engine_name: {"df": df, "columns": columns, "engine": engine_name}
    monkeypatch.setitem(sys.modules, "lakelogic.core.lineage", fake_lineage)
    assert processor._inject_lineage("good", "bad") == ("good-lined", "bad-lined")
    assert processor._preserve_upstream_lineage("df", ["col"], "_up") == ("df", ["col"], "_up", "polars")
    assert processor._add_columns("df", {"x": 1}) == {"df": "df", "columns": {"x": 1}, "engine": "polars"}

    fake_slo = types.ModuleType("lakelogic.core.slo")
    fake_slo._parse_duration_seconds = lambda value: 12.5
    fake_slo._get_max_timestamp = lambda df, field, engine_name: "max-ts"
    fake_slo._coerce_datetime = lambda value: "coerced"
    fake_slo._compute_freshness = lambda df, freshness_obj, engine_name: {"fresh": True}
    fake_slo._compute_availability = lambda df, counts, availability_obj, engine_name: {"avail": True}
    fake_slo._non_null_ratio = lambda df, field, engine_name: 0.9
    fake_slo.compute_slos = lambda contract, good_df, counts, engine_name: {"overall": True}
    monkeypatch.setitem(sys.modules, "lakelogic.core.slo", fake_slo)
    assert processor._parse_duration_seconds("12s") == 12.5
    assert processor._get_max_timestamp("df", "loaded_at") == "max-ts"
    assert processor._coerce_datetime("2024-01-01") == "coerced"
    assert processor._compute_freshness("df", {}) == {"fresh": True}
    assert processor._compute_availability("df", counts, {}) == {"avail": True}
    assert processor._non_null_ratio("df", "id") == 0.9
    assert processor._compute_slos("df", counts) == {"overall": True}


def test_processor_write_empty_run_log_and_extract_row_rule_failures(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.contract = types.SimpleNamespace(info=types.SimpleNamespace(title="Orders"), dataset="orders")
    processor._run_log_mode = "all"
    processor.adapter = types.SimpleNamespace(ERROR_COLUMN="_lakelogic_errors")
    processor._build_report = lambda contract_title, counts: {"contract": contract_title, "counts": counts}
    calls = []
    monkeypatch.setattr(
        proc_mod,
        "write_run_log",
        lambda report, contract, engine_name, run_log_mode=None: calls.append((report, engine_name, run_log_mode)),
    )
    processor._write_empty_run_log()
    assert calls[0][0]["status"] == "no_new_data"

    bad_df = pl.DataFrame({"_lakelogic_errors": [["required:id", "range:amount"], ["required:id"]]})
    failures = processor._extract_row_rule_failures(bad_df)
    assert any(item["message"] == "required:id" for item in failures)


def test_processor_load_contract_yaml_and_fact_governance(monkeypatch, tmp_path):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.stage = None
    monkeypatch.setattr(processor, "_apply_stage_overrides", lambda contract: contract)
    monkeypatch.setattr(processor, "_apply_fact_governance", lambda contract: contract)
    monkeypatch.setattr(processor, "_apply_cdc_defaults", lambda contract: contract)

    loaded = processor._load_contract({"version": "1.0.0", "info": {"title": "Orders"}, "metadata": {"flag": "on"}})
    assert isinstance(loaded, DataContract)
    assert loaded.metadata["flag"] == "on"

    yaml_path = tmp_path / "contract.yaml"
    yaml_path.write_text("version: '1.0.0'\ninfo:\n  title: Orders\nmetadata:\n  flag: on\n", encoding="utf-8")
    loaded_from_yaml = processor._load_contract(yaml_path)
    assert loaded_from_yaml.metadata["flag"] == "on"
    assert loaded_from_yaml._base_path == tmp_path

    facts_processor = object.__new__(proc_mod.DataProcessor)
    warning_messages = []
    monkeypatch.setattr(proc_mod.logger, "warning", lambda message: warning_messages.append(message))

    transaction_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(fact=types.SimpleNamespace(type="transaction"), strategy="merge"),
        model=None,
        primary_key=[],
        quality=Quality(),
    )
    with pytest.raises(ValueError, match="requires strategy 'append'"):
        facts_processor._apply_fact_governance(transaction_contract)

    factless_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(fact=types.SimpleNamespace(type="factless"), strategy="append"),
        model=types.SimpleNamespace(fields=[types.SimpleNamespace(name="amount", type="double", foreign_key=None)]),
        primary_key=[],
        quality=Quality(),
    )
    facts_processor._apply_fact_governance(factless_contract)
    assert any("Factless Fact Warning" in message for message in warning_messages)


def test_processor_cdc_defaults_and_accumulating_snapshot_rules():
    processor = object.__new__(proc_mod.DataProcessor)

    cdc_contract = DataContract(
        version="1.0.0",
        info={"title": "Orders"},
        source={"type": "file", "load_mode": "cdc", "cdc_op_field": "op"},
    )
    cdc_contract.materialization = None
    updated = processor._apply_cdc_defaults(cdc_contract)
    assert updated.source.watermark_strategy == "pipeline_log"
    assert updated.materialization.strategy == "merge"
    assert updated.materialization.soft_delete_column == "_lakelogic_is_deleted"

    snapshot_contract = DataContract(
        version="1.0.0",
        info={"title": "Pipeline"},
        model={"fields": [{"name": "placed_date", "type": "date"}, {"name": "shipped_date", "type": "date"}]},
        materialization={
            "strategy": "merge",
            "fact": {"type": "accumulating_snapshot", "milestone_dates": ["placed_date", "shipped_date"]},
        },
    )
    snapshot_contract.quality = Quality()
    governed = processor._apply_fact_governance(snapshot_contract)
    assert governed.quality.row_rules[0].name == "fact_milestone_placed_date_to_shipped_date"


def test_processor_stage_overrides_and_deep_merge(tmp_path):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.stage = "Silver"

    contract = DataContract(
        version="1.0.0",
        info={"title": "Orders"},
        metadata={"domain": "sales"},
        stages={"silver": {"metadata": {"domain": "finance", "environment": "prod"}}},
    )
    contract._base_path = tmp_path
    contract._contract_path = tmp_path / "contract.yaml"

    merged = processor._apply_stage_overrides(contract)
    assert merged.metadata["domain"] == "finance"
    assert merged.metadata["environment"] == "prod"
    assert merged._base_path == tmp_path
    assert processor._deep_merge({"a": {"b": 1}, "c": 1}, {"a": {"d": 2}, "c": 3}) == {
        "a": {"b": 1, "d": 2},
        "c": 3,
    }


def test_processor_notification_context_and_materialize(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.last_run_id = "run-1"
    processor.pipeline_run_id = "pipe-1"
    processor.last_source_path = "landing/orders.csv"
    processor._resolved_environment = "dev"
    processor._resolved_domain = "sales"
    processor._resolved_system = "erp"
    processor._resolved_data_layer = "silver"
    processor._resolved_data_layer = "silver"
    processor._ownership = {"owner": "analytics"}
    processor.contract = types.SimpleNamespace(
        info=types.SimpleNamespace(title="Orders", version="1.0.0", owner="data-team", table_name=None),
        dataset="orders",
        metadata={"priority": "high"},
        materialization=types.SimpleNamespace(target_path="table:catalog.sales.orders", path=None),
        quarantine=types.SimpleNamespace(target="quarantine/orders"),
    )

    context = processor._notification_template_context(
        event="run_failed",
        message="Order pipeline failed",
        subject="Order Failure",
        notification_type="slack",
    )
    assert context["contract"]["dataset"] == "orders"
    assert context["contract"]["domain"] == "sales"
    assert context["metadata"] == {"priority": "high"}

    calls = []
    monkeypatch.setattr(
        proc_mod,
        "materialize_dataframe",
        lambda *args, **kwargs: calls.append(("good", args, kwargs)) or {"target": "ok"},
    )
    monkeypatch.setattr(proc_mod, "materialize_quarantine", lambda *args, **kwargs: calls.append(("bad", args, kwargs)))
    result = processor.materialize("good_df", "bad_df", target_path="override/out")
    assert result == {"target": "ok"}
    assert calls[0][2]["target_path"] == Path("override/out")
    assert calls[1][0] == "bad"


def test_processor_materialize_quarantine_failure_and_trace_helpers(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.contract = types.SimpleNamespace(
        quarantine=types.SimpleNamespace(target="quarantine/orders"),
        materialization=types.SimpleNamespace(target_path=None, path=None),
    )
    warnings = []
    monkeypatch.setattr(proc_mod, "materialize_dataframe", lambda *args, **kwargs: {"target": "ok"})
    monkeypatch.setattr(
        proc_mod,
        "materialize_quarantine",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("quarantine boom")),
    )
    monkeypatch.setattr(proc_mod.logger, "warning", lambda message: warnings.append(message))

    assert processor.materialize("good_df", "bad_df") == {"target": "ok"}
    assert any("Quarantine write failed" in message for message in warnings)

    processor._active_trace_steps = []
    processor._add_current_trace("validate", output_rows=2)
    assert processor._active_trace_steps[0].step == "validate"

    with processor.trace_step("custom_step", source="unit-test"):
        pass
    assert any(step.step == "custom_step" for step in processor._active_trace_steps)


def test_processor_reprocess_resolution_and_polars_filter(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(reprocess_date_column=None, partition_by=["event_date"])
    )
    processor._reprocess_column = None

    infos = []
    monkeypatch.setattr(proc_mod.logger, "info", lambda message: infos.append(message))
    assert processor._resolve_reprocess_date_column() == "event_date"
    assert any("using first partition_by column" in message for message in infos)

    df = pl.DataFrame({"event_date": ["2024-01-01", "2024-01-02", "2024-01-03"], "id": [1, 2, 3]})
    filtered = processor._apply_reprocess_date_filter(df, "2024-01-02", "2024-01-02")
    assert filtered["id"].to_list() == [2]

    processor.contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(reprocess_date_column="event_date", partition_by=[])
    )
    with pytest.raises(ValueError, match="not found in DataFrame"):
        processor._apply_reprocess_date_filter(pl.DataFrame({"other": [1]}), "2024-01-01", None)

    processor.contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(reprocess_date_column=None, partition_by=[])
    )
    with pytest.raises(ValueError, match="Cannot apply date-range reprocessing"):
        processor._resolve_reprocess_date_column()


def test_processor_flatten_json_and_watermark_columns():
    processor = object.__new__(proc_mod.DataProcessor)
    processor.contract = types.SimpleNamespace(
        model=types.SimpleNamespace(
            fields=[types.SimpleNamespace(name="payload_id"), types.SimpleNamespace(name="payload_city")]
        ),
        lineage=types.SimpleNamespace(preserve_upstream=["_lakelogic_loaded_at"], upstream_prefix="_upstream"),
    )

    df = pl.DataFrame(
        {
            "payload": ['{"id": 1, "city": "Paris", "nested": {"code": "FR"}}'],
            "kind": ["event"],
        }
    )
    flattened = processor._flatten_json_df(df, True)
    assert "payload_id" in flattened.columns
    assert "payload_city" in flattened.columns
    assert "payload_nested_code" in flattened.columns

    empty_df = pl.DataFrame(schema={"payload": pl.Utf8})
    empty_flat = processor._flatten_json_df(empty_df, ["payload"])
    assert "payload_id" in empty_flat.columns
    assert "payload_city" in empty_flat.columns

    source_col, target_col = processor._resolve_watermark_columns("_lakelogic_loaded_at")
    assert source_col == "_lakelogic_loaded_at"
    assert target_col == "_upstream_lakelogic_loaded_at"
    assert processor._resolve_watermark_columns("_upstream_lakelogic_loaded_at") == (
        "_lakelogic_loaded_at",
        "_upstream_lakelogic_loaded_at",
    )
    assert processor._resolve_watermark_columns("event_ts") == ("event_ts", "event_ts")


def test_processor_run_covers_notifications_drift_slos_and_trace(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.stage = "silver"
    processor.trace_enabled = True
    processor.last_run_id = None
    processor.pipeline_run_id = "pipe-1"
    processor.last_source_path = None
    processor._active_trace_steps = []
    processor._incremental_metadata = {"max_watermark_value": "2024-01-02"}
    processor._pending_dlt_state_json = '{"cursor": 1}'
    processor._resolved_domain = "sales"
    processor._resolved_system = "erp"
    processor._resolved_data_layer = "silver"
    processor.notify_calls = []
    processor.notify = lambda event, message: processor.notify_calls.append((event, message))
    processor._build_report = lambda title, counts, slos, row_rule_failures, drift: {
        "title": title,
        "counts": counts,
        "slos": slos,
        "row_rule_failures": row_rule_failures,
        "schema_drift": drift,
    }

    processor.contract = types.SimpleNamespace(
        info=types.SimpleNamespace(title="Orders"),
        dataset="orders",
        external_logic=None,
        model=types.SimpleNamespace(
            fields=[types.SimpleNamespace(name="id", pii=False), types.SimpleNamespace(name="email", pii=True)]
        ),
        schema_policy=None,
        server=types.SimpleNamespace(schema_policy=types.SimpleNamespace(unknown_fields="drop")),
        quality=types.SimpleNamespace(
            enforce_required=True, row_rules=[1], dataset_rules=[1], fail_pipeline_on_dataset_error=False
        ),
        transformations=[
            {"rename": {"mappings": {"legacy_email": "email"}}},
            {"drop": {"columns": ["old_col"]}},
            {"derive": {"field": "derived_col"}},
            {"sql": "SELECT event_params_json AS ga_session_id FROM source", "phase": "post"},
            {"filter": {"sql": "id > 1"}, "phase": "pre"},
        ],
        materialization=types.SimpleNamespace(
            strategy="scd2",
            scd2=types.SimpleNamespace(
                model_dump=lambda: {
                    "surrogate_key": "_sk",
                    "effective_from_field": "effective_from",
                    "effective_to_field": "effective_to",
                    "current_flag_field": "is_current",
                    "version_column": "_version",
                    "change_reason_column": "_change_reason",
                }
            ),
        ),
        service_levels=types.SimpleNamespace(row_count=types.SimpleNamespace(min_rows=1, max_rows=10)),
        quarantine=types.SimpleNamespace(enabled=True),
        metadata={"cost": {"type": "flat"}, "domain": "sales", "system": "erp", "data_layer": "silver"},
    )

    good_df = pl.DataFrame({"id": [1, 2], "email": ["a@example.com", "b@example.com"], "extra_col": ["x", "y"]})
    bad_df = pl.DataFrame({"id": [3], "_lakelogic_errors": [["required:email", "range:score"]]})
    processor.adapter = types.SimpleNamespace(
        execute=lambda df: (good_df, bad_df),
        trace=[{"step": "Adapter", "status": "ok", "duration_ms": 1.0, "timestamp": 0.0}],
        dataset_rule_results=[{"name": "row_count_check", "value": 0, "passed": False}],
        schema_drift={
            "missing_fields": ["derived_col", "effective_from", "email", "unexpected_required"],
            "unknown_fields": [
                "legacy_email",
                "old_col",
                "_source_file",
                "_lakelogic_run_id",
                "event_params_json",
                "mystery_col",
            ],
            "policy": "quarantine",
        },
        ERROR_COLUMN="_lakelogic_errors",
        _get_row_count=lambda frame: frame.height,
    )

    fake_external = types.ModuleType("lakelogic.core.external_logic")
    fake_external.apply_external_logic = lambda *args, **kwargs: (args[1], False)
    monkeypatch.setitem(sys.modules, "lakelogic.core.external_logic", fake_external)

    fake_lineage = types.ModuleType("lakelogic.core.lineage")
    fake_lineage.inject_lineage = lambda good, bad, contract, engine_name, run_id, pipeline_run_id, source_path: (
        good.with_columns(pl.lit(run_id).alias("_lakelogic_run_id")),
        bad,
    )
    monkeypatch.setitem(sys.modules, "lakelogic.core.lineage", fake_lineage)

    class FakeMaskingEngine:
        def __init__(self, contract, encryption_key="", hash_salt=""):
            self.contract = contract

        def apply(self, df, user_groups=None):
            return df.with_columns(pl.col("email").str.replace_all("@.*", "@masked"))

        def get_vault_fields(self):
            return [types.SimpleNamespace(name="email")]

        def get_fields_to_mask(self, user_groups=None):
            return [types.SimpleNamespace(name="email", masking="hash")]

    fake_masking = types.ModuleType("lakelogic.core.masking_engine")
    fake_masking.MaskingEngine = FakeMaskingEngine
    monkeypatch.setitem(sys.modules, "lakelogic.core.masking_engine", fake_masking)

    fake_slo = types.ModuleType("lakelogic.core.slo")
    fake_slo.compute_slos = lambda contract, good_df, counts, engine_name: {
        "freshness": {"passed": False, "field": "loaded_at", "delay_seconds": 600, "threshold": "5m", "reason": "late"},
        "availability": {"passed": False, "reason": "no_data"},
    }
    monkeypatch.setitem(sys.modules, "lakelogic.core.slo", fake_slo)

    observer_reports = []
    fake_observer = types.ModuleType("lakelogic.core.observer")
    fake_observer.RemoteObserver = lambda: types.SimpleNamespace(report=lambda report: observer_reports.append(report))
    monkeypatch.setitem(sys.modules, "lakelogic.core.observer", fake_observer)

    fake_cost = types.ModuleType("lakelogic.core.cost_provider")
    fake_cost.resolve_cost_provider = lambda cost_cfg: types.SimpleNamespace(
        estimate=lambda **kwargs: types.SimpleNamespace(estimated_cost=1.25, currency="USD", confidence="high")
    )
    monkeypatch.setitem(sys.modules, "lakelogic.core.cost_provider", fake_cost)

    displayed = []
    fake_cli_main = types.ModuleType("lakelogic.cli.main")
    fake_cli_main._display_trace = lambda trace: displayed.append(trace.run_id)
    monkeypatch.setitem(sys.modules, "lakelogic.cli.main", fake_cli_main)

    result = processor.run(pl.DataFrame({"id": [1, 2, 3], "email": ["a", "b", None]}), source_path="landing/orders.csv")

    assert result.good.columns == ["id", "email", "_lakelogic_run_id"]
    assert result.good["email"].to_list() == ["a@masked", "b@masked"]
    assert result.trace.run_id == processor.last_run_id
    assert processor.last_report["pre_transform_filter"] == "(id > 1)"
    assert processor.last_report["max_watermark_value"] == "2024-01-02"
    assert processor.last_report["dlt_state_json"] == '{"cursor": 1}'
    assert processor.last_report["slo_row_count_min"] == 1
    assert processor.last_report["slo_row_count_max"] == 10
    assert processor.last_report["estimated_cost"] == 1.25
    assert observer_reports
    assert displayed == [processor.last_run_id]
    events = [event for event, _ in processor.notify_calls]
    assert "quarantine" in events
    assert "dataset_quality_check" in events
    assert "schema_drift" in events
    assert "slo_breach" in events
    assert processor._active_trace_steps == []


def test_processor_run_quarantine_disabled_and_trace_fallback(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.trace_enabled = False
    processor.last_run_id = "run-1"
    processor.pipeline_run_id = "pipe-1"
    processor.last_source_path = None
    processor._active_trace_steps = []
    processor._incremental_metadata = {}
    processor._resolved_domain = None
    processor._resolved_system = None
    processor._resolved_data_layer = None
    processor.notify = lambda event, message: None
    processor._build_report = lambda title, counts, slos, row_rule_failures, drift: {"counts": counts}
    processor.contract = types.SimpleNamespace(
        info=types.SimpleNamespace(title="Orders"),
        dataset="orders",
        external_logic=None,
        model=None,
        schema_policy=None,
        server=None,
        quality=types.SimpleNamespace(
            enforce_required=True, row_rules=[], dataset_rules=[], fail_pipeline_on_dataset_error=False
        ),
        transformations=[],
        materialization=None,
        service_levels=None,
        quarantine=types.SimpleNamespace(enabled=False),
        metadata={},
    )
    processor.adapter = types.SimpleNamespace(
        execute=lambda df: (pl.DataFrame({"id": [1]}), pl.DataFrame({"_lakelogic_errors": [["rule failed"]]})),
        trace=[],
        dataset_rule_results=[],
        schema_drift={},
        ERROR_COLUMN="_lakelogic_errors",
        _get_row_count=lambda frame: frame.height,
    )

    fake_external = types.ModuleType("lakelogic.core.external_logic")
    fake_external.apply_external_logic = lambda *args, **kwargs: (args[1], False)
    monkeypatch.setitem(sys.modules, "lakelogic.core.external_logic", fake_external)

    fake_lineage = types.ModuleType("lakelogic.core.lineage")
    fake_lineage.inject_lineage = lambda good, bad, *args, **kwargs: (good, bad)
    monkeypatch.setitem(sys.modules, "lakelogic.core.lineage", fake_lineage)

    fake_slo = types.ModuleType("lakelogic.core.slo")
    fake_slo.compute_slos = lambda contract, good_df, counts, engine_name: {}
    monkeypatch.setitem(sys.modules, "lakelogic.core.slo", fake_slo)

    with pytest.raises(ValueError, match="Quarantine disabled"):
        processor.run(pl.DataFrame({"id": [1], "email": [None]}))

    logger_messages = []
    monkeypatch.setattr(proc_mod.logger, "info", lambda message: logger_messages.append(message))
    monkeypatch.setitem(sys.modules, "lakelogic.cli.main", types.ModuleType("lakelogic.cli.main"))
    processor._active_trace_steps = [{"step": "load", "status": "ok", "duration_ms": 5, "timestamp": 0.0}]
    processor.show_trace()
    assert any("Execution Trace" in message for message in logger_messages)


def test_processor_run_source_rejects_invalid_watermark_strategies(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._active_trace_steps = []
    processor._resolve_source_path = lambda value: str(value)
    fake_catalog = types.ModuleType("lakelogic.engines.catalog_resolver")
    fake_catalog.resolve_catalog_path = lambda path: path
    monkeypatch.setitem(sys.modules, "lakelogic.engines.catalog_resolver", fake_catalog)

    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(
            path="table:catalog.orders", type="table", watermark_strategy="pipeline_log", load_mode="incremental"
        ),
    )
    with pytest.raises(ValueError, match="source type 'table' cannot use"):
        processor.run_source()

    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(
            path="landing/orders.parquet", type="file", watermark_strategy="delta_version", load_mode="incremental"
        ),
    )
    with pytest.raises(ValueError, match="file-based source cannot use"):
        processor.run_source()


def test_processor_expand_source_files_and_partitioned_paths(monkeypatch, tmp_path):
    # Source-glob resolution now uses CWD, not _base_path (source paths are
    # storage paths resolved by the registry). chdir so the test fixtures
    # created under tmp_path are discovered.
    monkeypatch.chdir(tmp_path)
    processor = object.__new__(proc_mod.DataProcessor)
    processor.contract = types.SimpleNamespace(
        _base_path=tmp_path,
        source=types.SimpleNamespace(type="file", format="json"),
    )
    processor._is_uri_path = lambda path: path.startswith("abfss://")
    processor._get_cloud_storage_options = lambda path: {"account_key": "key"}

    landing = tmp_path / "landing"
    landing.mkdir()
    first = landing / "orders_1.json"
    second = landing / "orders_2.json"
    first.write_text("{}", encoding="utf-8")
    second.write_text("{}", encoding="utf-8")
    files = processor._expand_source_files("landing/*.json")
    assert len(files) == 2
    assert all(item["path"].endswith(".json") for item in files)

    processor.contract.source = types.SimpleNamespace(type="table", format="json")
    assert processor._expand_source_files("landing/*.json") is None
    processor.contract.source = types.SimpleNamespace(type="file", format="json")

    fake_fsspec = types.SimpleNamespace(
        get_fs_token_paths=lambda path, storage_options=None: (
            types.SimpleNamespace(info=lambda item: {"last_modified": 10 if item.endswith("a.json") else 20}),
            None,
            ["container/a.json", "container/b.json"],
        )
    )
    monkeypatch.setitem(sys.modules, "fsspec", fake_fsspec)
    cloud_files = processor._expand_source_files("abfss://container/*.json")
    assert cloud_files[0]["path"].startswith("abfss://")
    assert len(cloud_files) == 2

    class ExplodingFsspec:
        def get_fs_token_paths(self, path, storage_options=None):
            raise RuntimeError("403 forbidden")

    monkeypatch.setitem(sys.modules, "fsspec", ExplodingFsspec())
    with pytest.raises(RuntimeError, match="Cloud storage access failed"):
        processor._expand_source_files("abfss://container/*.json")

    expanded = []
    monkeypatch.setattr(
        processor, "_expand_source_files", lambda path: expanded.append(path) or [{"path": path, "mtime": 1.0}]
    )
    partition_cfg = types.SimpleNamespace(
        start_date="2024-01-01", end_date="2024-01-02", lookback_days=2, file_pattern="*.json", format="%Y/%m/%d"
    )
    partitioned = processor._expand_partitioned_paths("abfss://landing/events", partition_cfg)
    assert len(partitioned) == 2
    assert expanded == [
        "abfss://landing/events/2024/01/01/*.json",
        "abfss://landing/events/2024/01/02/*.json",
    ]


def test_processor_run_source_delegates_and_validates_incremental_table_config(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._active_trace_steps = []

    processor.contract = types.SimpleNamespace(source=types.SimpleNamespace(type="dlt"))
    processor._run_dlt_source = lambda: "dlt-result"
    assert processor.run_source() == "dlt-result"

    processor.contract = types.SimpleNamespace(source=types.SimpleNamespace(type="database"))
    processor._run_database_source = lambda: "db-result"
    assert processor.run_source() == "db-result"

    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(
            type="table",
            path="catalog.orders",
            load_mode="incremental",
            watermark_strategy="pipeline_log",
            partition=None,
        ),
        materialization=None,
        server=None,
        lineage=None,
    )
    processor._resolve_source_path = lambda value: "table:catalog.orders"
    with pytest.raises(ValueError, match="source type 'table' cannot use watermark_strategy 'pipeline_log'"):
        processor.run_source()


def test_processor_run_source_partitioned_initial_load_without_files_returns_empty(tmp_path):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._active_trace_steps = []
    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(
            type="file",
            path="landing",
            load_mode="incremental",
            format="csv",
            partition=types.SimpleNamespace(format="year={Y}/month={m}/day={d}"),
        ),
        materialization=None,
        server=None,
        lineage=None,
    )
    processor._resolve_source_path = lambda value: str(tmp_path / "landing")
    processor._get_last_source_watermark = lambda: None
    processor._expand_source_files = lambda path: []
    processor._is_uri_path = lambda path: False

    writes = []
    processor._write_empty_run_log = lambda stage="no_new_data": writes.append(stage)
    processor._empty_frame = lambda: pl.DataFrame(schema={"id": pl.Int64})

    result = processor.run_source()
    assert isinstance(result, proc_mod.ValidationResult)
    assert result.good_count == 0
    assert result.bad_count == 0
    assert result.source_count == 0
    assert writes == ["no_new_data"]


def test_processor_run_source_polars_reads_multiple_csvs_and_applies_targeted_reprocess(tmp_path):
    first = tmp_path / "orders_1.csv"
    second = tmp_path / "orders_2.csv"
    first.write_text("customer_id,status\n1,new\n2,open\n", encoding="utf-8")
    second.write_text("customer_id,status\n3,done\n4,archived\n", encoding="utf-8")

    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._active_trace_steps = []
    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(
            type="file",
            path="landing/*.csv",
            load_mode="full",
            format="csv",
            partition=None,
            flatten_nested=False,
        ),
        materialization=types.SimpleNamespace(reprocess_column=None, partition_by=[]),
        server=None,
        lineage=None,
    )
    processor._resolve_source_path = lambda value: str(tmp_path / "landing")
    processor._expand_source_files = lambda path: [
        {"path": str(first), "mtime": 10},
        {"path": str(second), "mtime": 20},
    ]
    processor._is_uri_path = lambda path: False

    captured = {}
    processor.run = lambda df, source_path=None, reset_trace=False: (
        captured.update({"df": df, "source_path": source_path, "reset_trace": reset_trace}) or "run-result"
    )

    result = processor.run_source(reprocess_column="customer_id", reprocess_values=[2, 3])
    assert result == "run-result"
    assert captured["source_path"] == str(tmp_path / "landing")
    assert captured["reset_trace"] is False
    # infer_schema_length=0 loads raw landing CSV as strings (bronze/raw layer);
    # typed casts happen downstream against the contract. The reprocess filter
    # matches on the string form of the requested values.
    assert captured["df"]["customer_id"].to_list() == ["2", "3"]
    assert set(captured["df"]["_source_file"].to_list()) == {str(first), str(second)}


def test_processor_run_source_polars_json_flattens_and_preserves_upstream(monkeypatch, tmp_path):
    json_path = tmp_path / "orders.json"
    json_path.write_text(
        '[{"payload":"{\\"id\\": 1}", "_lakelogic_loaded_at":"2024-01-01T00:00:00"}]',
        encoding="utf-8",
    )

    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._active_trace_steps = []
    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(
            type="file",
            path=str(json_path),
            load_mode="full",
            format="json",
            partition=None,
            flatten_nested=True,
        ),
        materialization=types.SimpleNamespace(reprocess_date_column=None, partition_by=[]),
        server=None,
        lineage=types.SimpleNamespace(
            enabled=True,
            preserve_upstream=["_lakelogic_loaded_at"],
            upstream_prefix="_upstream",
        ),
    )
    processor._resolve_source_path = lambda value: str(json_path)
    processor._expand_source_files = lambda path: None
    processor._is_uri_path = lambda path: False
    processor._apply_reprocess_date_filter = lambda df, reprocess_from, reprocess_to: df

    flatten_calls = []
    processor._flatten_json_df = lambda df, flatten_nested: (
        flatten_calls.append(flatten_nested) or df.with_columns(pl.lit(1).alias("payload_id"))
    )

    fake_lineage = types.ModuleType("lakelogic.core.lineage")
    fake_lineage._preserve_upstream_lineage = lambda df, columns, prefix, engine_name: df.with_columns(
        pl.col(columns[0]).alias(f"{prefix}_lakelogic_loaded_at")
    )
    monkeypatch.setitem(sys.modules, "lakelogic.core.lineage", fake_lineage)

    captured = {}
    processor.run = lambda df, source_path=None, reset_trace=False: (
        captured.update({"df": df, "source_path": source_path}) or df
    )

    result = processor.run_source()
    assert flatten_calls == [True]
    assert result["payload_id"].to_list() == [1]
    assert result["_upstream_lakelogic_loaded_at"].to_list() == ["2024-01-01T00:00:00"]
    assert captured["source_path"] == str(json_path)


def test_processor_run_dlt_source_uses_previous_state_and_runs_validation(monkeypatch):
    pa = pytest.importorskip("pyarrow")

    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._resolved_data_layer = "bronze"
    processor.contract = types.SimpleNamespace(
        info=types.SimpleNamespace(title="Orders"),
        dataset="orders",
        source=types.SimpleNamespace(dlt=types.SimpleNamespace(source="orders_api", base_url="https://example.test")),
    )

    fake_run_log = types.ModuleType("lakelogic.core.run_log")
    fake_run_log.get_last_run_dlt_state = lambda *args, **kwargs: {"cursor": "abc"}
    monkeypatch.setitem(sys.modules, "lakelogic.core.run_log", fake_run_log)

    extracted_states = []

    class FakeDltAdapter:
        def __init__(self, source, contract_title):
            self.dlt_state_json = '{"cursor": "next"}'

        def extract(self, previous_state=None):
            extracted_states.append(previous_state)
            return pa.table({"id": [1, 2]})

    fake_dlt_module = types.ModuleType("lakelogic.adapters.dlt_adapter")
    fake_dlt_module.DltAdapter = FakeDltAdapter
    monkeypatch.setitem(sys.modules, "lakelogic.adapters.dlt_adapter", fake_dlt_module)

    captured = {}
    processor.run = lambda df, source_path=None: captured.update({"df": df, "source_path": source_path}) or "validated"

    result = processor._run_dlt_source()
    assert result == "validated"
    assert extracted_states == [{"cursor": "abc"}]
    assert processor._pending_dlt_state_json == '{"cursor": "next"}'
    assert captured["df"]["id"].to_list() == [1, 2]
    assert captured["source_path"] == "dlt://orders_api"


def test_processor_run_database_source_polars_uses_projection_and_incremental_watermark(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.contract = types.SimpleNamespace(
        info=types.SimpleNamespace(title="Orders"),
        dataset="orders",
        model=types.SimpleNamespace(fields=[types.SimpleNamespace(name="id"), types.SimpleNamespace(name="status")]),
        source=types.SimpleNamespace(
            path="postgresql://db/orders",
            query=None,
            load_mode="incremental",
            watermark_field="updated_at",
            options={},
        ),
    )
    processor._get_last_source_watermark = lambda: 1_704_067_200.0

    queries = []
    monkeypatch.setattr(
        pl,
        "read_database_uri",
        lambda query, uri, **kwargs: (
            queries.append((query, uri, kwargs)) or pl.DataFrame({"id": [1], "status": ["ok"]})
        ),
    )

    captured = {}
    processor.run = lambda df, source_path=None: captured.update({"df": df, "source_path": source_path}) or "validated"

    result = processor._run_database_source()
    assert result == "validated"
    assert queries[0][1] == "postgresql://db/orders"
    assert 'SELECT "id", "status", "updated_at" FROM "orders"' in queries[0][0]
    assert "WHERE updated_at > '2024-01-01T00:00:00+00:00'" in queries[0][0]
    assert captured["df"]["status"].to_list() == ["ok"]
    assert captured["source_path"] == "database://orders"


def test_processor_run_source_streaming_validates_engine_and_sinks_output(tmp_path):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "duckdb"
    processor.contract = types.SimpleNamespace(source=types.SimpleNamespace(path="orders.csv"))
    with pytest.raises(ValueError, match="Streaming mode requires the 'polars' engine"):
        processor.run_source_streaming()

    csv_path = tmp_path / "orders.csv"
    csv_path.write_text("id,status\n1,new\n2,done\n", encoding="utf-8")

    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.contract = types.SimpleNamespace(source=types.SimpleNamespace(path=str(csv_path)))
    processor._resolve_source_path = lambda value: str(csv_path)
    processor.run = lambda lf, source_path=None: types.SimpleNamespace(good=lf, bad=pl.DataFrame().lazy())

    output_path = tmp_path / "streamed.csv"
    result = processor.run_source_streaming(output_path=str(output_path))
    assert result == {"target": str(output_path), "format": "csv"}
    assert output_path.exists()
    assert "status" in output_path.read_text(encoding="utf-8")


def test_processor_run_source_streaming_returns_result_and_rejects_unsupported_format(tmp_path):
    ndjson_path = tmp_path / "orders.ndjson"
    ndjson_path.write_text('{"id":1,"status":"new"}\n{"id":2,"status":"done"}\n', encoding="utf-8")

    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.contract = types.SimpleNamespace(source=types.SimpleNamespace(path=str(ndjson_path)))
    processor._resolve_source_path = lambda value: str(ndjson_path)
    expected = types.SimpleNamespace(good="good", bad="bad")
    processor.run = lambda lf, source_path=None: expected
    assert processor.run_source_streaming() is expected

    bad_path = tmp_path / "orders.json"
    bad_path.write_text("{}", encoding="utf-8")
    processor.contract = types.SimpleNamespace(source=types.SimpleNamespace(path=str(bad_path)))
    processor._resolve_source_path = lambda value: str(bad_path)
    with pytest.raises(ValueError, match="Streaming mode supports .parquet, .csv, .ndjson/.jsonl files"):
        processor.run_source_streaming()


def test_processor_run_source_polars_ndjson_json_and_multi_file_eager_fallback(monkeypatch, tmp_path):
    ndjson_text = '{"id":1,"meta":{"city":"Paris"}}\n{"id":2,"tags":["a","b"]}\n'
    ndjson_path = "abfss://container/orders.json"

    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._active_trace_steps = []
    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(
            type="file",
            path=ndjson_path,
            load_mode="full",
            format="json",
            partition=None,
            flatten_nested=False,
        ),
        materialization=types.SimpleNamespace(reprocess_date_column=None, partition_by=[]),
        server=None,
        lineage=None,
    )
    processor._resolve_source_path = lambda value: ndjson_path
    processor._expand_source_files = lambda path: None
    processor._is_uri_path = lambda path: True
    processor._get_cloud_storage_options = lambda path: {"account_key": "key"}

    class FakeOpen:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return ndjson_text

    monkeypatch.setitem(sys.modules, "fsspec", types.SimpleNamespace(open=lambda path, mode, **opts: FakeOpen()))

    captured = {}
    processor.run = lambda df, source_path=None, reset_trace=False: (
        captured.update({"df": df, "source_path": source_path}) or df
    )

    result = processor.run_source()
    assert result["id"].to_list() == [1, 2]
    assert result["meta"].to_list() == ['{"city": "Paris"}', None]
    assert result["tags"].to_list() == [None, '["a", "b"]']
    assert captured["source_path"] == ndjson_path

    xml_path = tmp_path / "orders.xml"
    excel_path = tmp_path / "orders.xlsx"
    csv_path = tmp_path / "orders.csv"
    json_path = tmp_path / "orders_2.json"
    xml_path.write_text("<rows></rows>", encoding="utf-8")
    excel_path.write_text("placeholder", encoding="utf-8")
    csv_path.write_text("id\n4\n", encoding="utf-8")
    json_path.write_text('{"id": 3, "status": "new"}', encoding="utf-8")

    processor.contract.source.path = str(tmp_path / "orders*")
    processor._resolve_source_path = lambda value: str(tmp_path)
    processor._expand_source_files = lambda path: [
        {"path": str(xml_path), "mtime": 1.0},
        {"path": str(excel_path), "mtime": 2.0},
        {"path": str(json_path), "mtime": 3.0},
        {"path": str(csv_path), "mtime": 4.0},
    ]
    processor._is_uri_path = lambda path: False

    monkeypatch.setattr(
        pl, "read_xml", lambda path, **kwargs: pl.DataFrame({"id": [1], "source": ["xml"]}), raising=False
    )
    monkeypatch.setattr(
        pl, "read_excel", lambda path, **kwargs: pl.DataFrame({"id": [2], "source": ["xlsx"]}), raising=False
    )
    monkeypatch.setattr(
        pl, "read_csv", lambda path, **kwargs: pl.DataFrame({"id": [4], "source": ["csv"]}), raising=False
    )

    multi = processor.run_source()
    assert sorted(multi["id"].to_list()) == [1, 2, 3, 4]
    assert set(multi["source"].drop_nulls().to_list()) == {"xml", "xlsx", "csv"}


def test_processor_notify_dispatches_and_handles_failures(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.last_run_id = "run-1"
    processor.pipeline_run_id = "pipe-1"
    processor.last_source_path = "landing/orders.csv"
    processor._notifications_enabled = True
    processor._notifications = [
        {"target": "https://hooks.slack.com/services/demo", "on_events": ["failure"]},
        {"target": "ops@example.com", "type": "email", "on_events": ["dataset_quality_check"]},
    ]
    processor._ownership = {"owner": "analytics"}
    processor._resolved_environment = "dev"
    processor._resolved_domain = "sales"
    processor._resolved_system = "erp"
    processor._resolved_data_layer = "silver"
    contract_notifications = [
        types.SimpleNamespace(
            on_events=["failure"],
            type="webhook",
            target="https://contract.example/hook",
            model_dump=lambda by_alias=True: {"target": "https://contract.example/hook", "type": "webhook"},
        ),
        types.SimpleNamespace(
            on_events=["failure"],
            type="webhook",
            target="https://contract.example/hook",
            model_dump=lambda by_alias=True: {"target": "https://contract.example/hook", "type": "webhook"},
        ),
    ]
    processor.contract = types.SimpleNamespace(
        quarantine=types.SimpleNamespace(
            enabled=True,
            notifications=contract_notifications,
            notifications_enabled=True,
            strict_notifications=False,
        ),
        _base_path=Path("."),
    )

    sent = []
    debug_messages = []
    warning_messages = []
    error_messages = []
    monkeypatch.setattr(proc_mod.logger, "debug", lambda message: debug_messages.append(message))
    monkeypatch.setattr(proc_mod.logger, "warning", lambda message: warning_messages.append(message))
    monkeypatch.setattr(proc_mod.logger, "error", lambda message: error_messages.append(message))

    class FakeAdapter:
        def __init__(self, config):
            self.config = config

        def send(self, message, subject=None):
            sent.append((self.config.get("type"), self.config.get("target"), subject, message))

    def _get_notification_adapter(notif_type, config):
        if config.get("target") == "broken-owner":
            raise RuntimeError("missing required fields")
        return FakeAdapter(config)

    fake_notifications = types.ModuleType("lakelogic.notifications.base")
    fake_notifications.resolve_ownership_contacts = lambda ownership, event: [
        {"type": "email", "target": "broken-owner", "on_events": ["failure"], "_source": "owner-email"},
        {
            "type": "slack",
            "target": "https://ownership.example/hook",
            "on_events": ["failure"],
            "_source": "owner-slack",
        },
    ]
    monkeypatch.setitem(sys.modules, "lakelogic.notifications.base", fake_notifications)
    monkeypatch.setattr(proc_mod, "get_notification_adapter", _get_notification_adapter)
    monkeypatch.setattr(
        proc_mod,
        "render_notification_content",
        lambda config, message, subject, context: (
            f"rendered::{message}",
            f"rendered::{subject}",
        ),
    )

    processor.notify("failure", "Pipeline failed")
    processor.notify("dataset_quality_check", "Dataset rule failed")

    assert (
        "webhook",
        "https://contract.example/hook",
        "rendered::[DEV] sales/erp: Failure Alert",
        "rendered::Pipeline failed",
    ) in sent
    assert any(item[0] == "slack" and item[1] == "https://hooks.slack.com/services/demo" for item in sent)
    assert any(item[0] == "email" and item[1] == "ops@example.com" for item in sent)
    assert any("Ownership notification skipped" in message for message in debug_messages)
    assert error_messages == []
    assert warning_messages == []


def test_processor_compute_counts_spark_optimized_and_fallback(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "spark"

    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_functions = types.SimpleNamespace(lit=lambda value: types.SimpleNamespace(alias=lambda name: (value, name)))
    fake_sql_module.functions = fake_functions
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    class FakeGrouped:
        def __init__(self, rows):
            self.rows = rows

        def collect(self):
            return self.rows

    class FakeMarkedFrame:
        def __init__(self, markers):
            self.markers = list(markers)

        def union(self, other):
            return FakeMarkedFrame(self.markers + other.markers)

        def groupBy(self, column):
            counts = {}
            for marker in self.markers:
                counts[marker] = counts.get(marker, 0) + 1
            return types.SimpleNamespace(
                count=lambda: FakeGrouped([{"_count_marker": key, "count": value} for key, value in counts.items()])
            )

    class FakeFrame:
        def __init__(self, marker, rows):
            self.marker = marker
            self.rows = rows

        def select(self, expr):
            return FakeMarkedFrame([self.marker] * self.rows)

    source = FakeFrame("source", 5)
    good = FakeFrame("good", 3)
    bad = FakeFrame("bad", 1)

    counts = processor._compute_counts(source, good, bad)
    assert counts == {
        "source": 5,
        "total": 4,
        "good": 3,
        "quarantined": 1,
        "quarantine_ratio": 0.25,
        "pre_transform_dropped": 1,
    }

    broken = types.SimpleNamespace(select=lambda expr: (_ for _ in ()).throw(RuntimeError("boom")))
    fallback_counts = processor._compute_counts(broken, [1, 2], [3])
    assert fallback_counts == {
        "source": None,
        "total": 3,
        "good": 2,
        "quarantined": 1,
        "quarantine_ratio": 1 / 3,
        "pre_transform_dropped": None,
    }


def test_validation_result_edge_counts_quality_and_repr():
    class CursorCount:
        def fetchone(self):
            return [7]

    class RelationLike:
        def count(self):
            return CursorCount()

    class BrokenLen:
        def __len__(self):
            raise RuntimeError("no length")

    assert proc_mod.ValidationResult._count_rows(None) == 0
    assert proc_mod.ValidationResult._count_rows(RelationLike()) == 7
    assert proc_mod.ValidationResult._count_rows(BrokenLen()) == 0

    empty = proc_mod.ValidationResult(good=[], bad=[], raw=[])
    assert empty.quarantine_ratio == 0.0
    assert empty.quality_score == 100.0

    result = proc_mod.ValidationResult(good=[1, 2], bad=[3, 4], raw=[1, 2, 3, 4], auto_fix_hint="fix it")
    assert result.quarantine_ratio == 0.5
    assert result.quality_score == 50.0
    assert result.trace is None
    assert result.auto_fix_hint == "fix it"


def test_processor_class_constructors_reset_and_adapter_variants(monkeypatch):
    fake_dbt = types.ModuleType("lakelogic.adapters.dbt")
    fake_dbt.load_contract_from_dbt = lambda *args, **kwargs: DataContract(version="1.0.0", dataset="from_dbt")
    monkeypatch.setitem(sys.modules, "lakelogic.adapters.dbt", fake_dbt)

    processor = proc_mod.DataProcessor.from_dbt("schema.yml", model="orders", engine="polars", stage="silver")
    assert processor.contract.dataset == "from_dbt"
    assert processor.stage == "silver"

    reset_calls = []
    processor.contract.reset = lambda targets=None, dry_run=False: reset_calls.append((targets, dry_run)) or {
        "ok": True
    }
    assert processor.reset(targets=["data"], dry_run=True) == {"ok": True}
    assert reset_calls == [(["data"], True)]

    fake_modules = {
        "lakelogic.engines.spark": ("SparkAdapter", "spark"),
        "lakelogic.engines.snowflake": ("SnowflakeAdapter", "snowflake"),
        "lakelogic.engines.bigquery": ("BigQueryAdapter", "bigquery"),
    }
    for module_name, (class_name, value) in fake_modules.items():
        module = types.ModuleType(module_name)
        setattr(module, class_name, lambda contract, value=value: (value, contract))
        monkeypatch.setitem(sys.modules, module_name, module)

    adapter_processor = object.__new__(proc_mod.DataProcessor)
    adapter_processor.contract = {"contract": True}
    adapter_processor.engine_name = "spark"
    assert adapter_processor._get_adapter() == ("spark", {"contract": True})
    adapter_processor.engine_name = "pyspark"
    assert adapter_processor._get_adapter() == ("spark", {"contract": True})
    adapter_processor.engine_name = "snowflake"
    assert adapter_processor._get_adapter() == ("snowflake", {"contract": True})
    adapter_processor.engine_name = "bigquery"
    assert adapter_processor._get_adapter() == ("bigquery", {"contract": True})


def test_processor_load_contract_inline_yaml_file_missing_and_stage_noops(monkeypatch, tmp_path):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.stage = None
    monkeypatch.setattr(processor, "_apply_fact_governance", lambda contract: contract)
    monkeypatch.setattr(processor, "_apply_cdc_defaults", lambda contract: contract)

    loaded = processor._load_contract(DataContract(version="1.0.0", dataset="direct"))
    assert loaded.dataset == "direct"

    inline = """
version: "1.0.0"
dataset: inline
metadata:
  mode: on
source:
  type: file
  path: input.csv
  options:
    flag: true
"""
    loaded_inline = processor._load_contract(inline)
    assert loaded_inline.metadata["mode"] == "on"
    assert loaded_inline.source.options["flag"] is True

    with pytest.raises(FileNotFoundError):
        processor._load_contract(tmp_path / "missing.yaml")

    no_stage = DataContract(version="1.0.0", dataset="base", stages={"silver": {"dataset": "silver"}})
    processor.stage = " "
    assert processor._apply_stage_overrides(no_stage).dataset == "base"
    processor.stage = "gold"
    assert processor._apply_stage_overrides(no_stage).dataset == "base"
    processor.stage = "silver"
    assert processor._apply_stage_overrides(no_stage).dataset == "silver"


def test_processor_cleanup_partition_expansion_cloud_globs_and_source_paths(monkeypatch, tmp_path):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.contract = types.SimpleNamespace(
        _base_path=tmp_path,
        source=types.SimpleNamespace(type="file", format="json"),
    )

    landing = tmp_path / "landing"
    landing.mkdir()
    keep = landing / "keep.csv"
    delete_me = landing / "delete.csv"
    nested = landing / "archive" / "nested.csv"
    nested.parent.mkdir()
    keep.write_text("id\n1\n", encoding="utf-8")
    delete_me.write_text("id\n2\n", encoding="utf-8")
    nested.write_text("id\n3\n", encoding="utf-8")
    processor._source_files = [{"path": str(delete_me)}, {"path": str(nested)}, {"path": str(tmp_path / "missing.csv")}]

    processor._post_ingestion_cleanup(str(landing), "delete")
    assert not delete_me.exists()
    assert nested.exists()

    archive_me = landing / "archive_me.csv"
    archive_me.write_text("id\n4\n", encoding="utf-8")
    processor._source_files = [{"path": str(archive_me)}]
    archive_dir = tmp_path / "archive_out"
    processor._post_ingestion_cleanup(str(landing), "archive", archive_path=str(archive_dir))
    assert (archive_dir / "archive_me.csv").exists()

    missing_archive = landing / "missing_archive.csv"
    missing_archive.write_text("id\n5\n", encoding="utf-8")
    processor._source_files = [{"path": str(missing_archive)}]
    with pytest.raises(ValueError, match="archive_path"):
        processor._post_ingestion_cleanup(str(landing), "archive")
    processor._post_ingestion_cleanup(str(landing), "unknown")

    processor._is_uri_path = lambda path: path.startswith("abfss://")
    assert processor._resolve_source_path("table:catalog.orders") == "table:catalog.orders"
    assert processor._resolve_source_path("abfss://container/path") == "abfss://container/path"
    assert processor._resolve_source_path(tmp_path / "absolute.csv") == str(tmp_path / "absolute.csv")

    local_glob_dir = tmp_path / "glob"
    local_glob_dir.mkdir()
    (local_glob_dir / "a.json").write_text("{}", encoding="utf-8")
    (local_glob_dir / "b.json").write_text("{}", encoding="utf-8")
    local_files = processor._expand_source_files(str(local_glob_dir / "*.json"))
    assert len(local_files) == 2

    processor.contract.source.type = "table"
    assert processor._expand_source_files(str(local_glob_dir / "*.json")) is None
    processor.contract.source.type = "file"

    delta_dir = tmp_path / "delta"
    (delta_dir / "_delta_log").mkdir(parents=True)
    assert processor._expand_source_files(str(delta_dir)) is None

    iceberg_dir = tmp_path / "iceberg"
    (iceberg_dir / "metadata").mkdir(parents=True)
    (iceberg_dir / "data").mkdir()
    assert processor._expand_source_files(str(iceberg_dir)) is None

    class FakeFs:
        def info(self, path):
            return {"last_modified": types.SimpleNamespace(timestamp=lambda: 123.0)}

    fake_fsspec = types.ModuleType("fsspec")
    fake_fsspec.get_fs_token_paths = lambda path, storage_options=None: (
        FakeFs(),
        None,
        ["container/path/a.json"],
    )
    monkeypatch.setitem(sys.modules, "fsspec", fake_fsspec)
    processor._get_cloud_storage_options = lambda path: {"account_key": "key"}
    cloud_files = processor._expand_source_files("abfss://container@acct.dfs.core.windows.net/path/*.json")
    assert cloud_files[0]["path"].startswith("abfss://container@acct.dfs.core.windows.net")

    fake_fsspec.get_fs_token_paths = lambda *args, **kwargs: (_ for _ in ()).throw(PermissionError("403 forbidden"))
    with pytest.raises(RuntimeError, match="Cloud storage access failed"):
        processor._expand_source_files("abfss://container/path/*.json")

    fake_fsspec.get_fs_token_paths = lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("empty glob"))
    assert processor._expand_source_files("abfss://container/path/*.json") is None

    calls = []
    processor._expand_source_files = lambda path: calls.append(path) or [{"path": path, "mtime": 1.0}]
    partition_cfg = types.SimpleNamespace(
        format="year=%Y/month=%m/day=%d/hour=%H",
        start_date=None,
        end_date=None,
        lookback_days=0,
        file_pattern=None,
    )
    files = processor._expand_partitioned_paths(
        str(tmp_path / "events" / "*.json"),
        partition_cfg,
        override_start="2026-01-01",
        override_end="2026-01-01",
    )
    assert files
    assert all(path.endswith("/*.json") for path in calls)


def test_processor_reprocess_spark_filter_and_unsupported_frame(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(reprocess_date_column="event_date", partition_by=[])
    )
    processor._reprocess_column = None

    fake_functions = types.SimpleNamespace(
        col=lambda name: ("col", name),
        lit=lambda value: ("lit", value),
    )
    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.functions = fake_functions
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    class FakeSparkDf:
        sparkSession = object()

        def __init__(self):
            self.filters = []

        def count(self):
            return 5 - len(self.filters)

        def filter(self, expr):
            self.filters.append(expr)
            return self

    spark_df = FakeSparkDf()
    filtered = processor._apply_reprocess_date_filter(spark_df, "2026-01-01", "2026-01-02")
    assert filtered is spark_df
    assert len(spark_df.filters) == 2

    unsupported = object()
    assert processor._apply_reprocess_date_filter(unsupported, "2026-01-01", None) is unsupported


def test_processor_delegates_for_ddl_gdpr_hipaa_external_logic_and_dim_date(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.contract = types.SimpleNamespace(name="contract")
    processor.last_run_id = "run-1"
    processor.last_source_path = "source.csv"
    processor._active_trace_steps = []

    fake_ddl = types.ModuleType("lakelogic.core.ddl")
    fake_ddl.generate_ddl = lambda contract, backend, **kwargs: ("ddl", contract, backend, kwargs)
    fake_ddl.create_table = lambda contract, backend, **kwargs: ("create", contract, backend, kwargs)
    monkeypatch.setitem(sys.modules, "lakelogic.core.ddl", fake_ddl)
    assert processor.generate_ddl(table_name="orders")[0] == "ddl"
    assert processor.create_table(backend="duckdb", dry_run=True)[0] == "create"

    fake_external = types.ModuleType("lakelogic.core.external_logic")
    fake_external.apply_external_logic = lambda *args, **kwargs: ("external", args, kwargs)
    fake_external._run_python_logic = lambda *args, **kwargs: ("python", args, kwargs)
    fake_external._run_notebook_logic = lambda *args, **kwargs: ("notebook", args, kwargs)
    fake_external._load_output_frame = lambda *args, **kwargs: ("loaded", args, kwargs)
    monkeypatch.setitem(sys.modules, "lakelogic.core.external_logic", fake_external)
    assert processor._apply_external_logic("df")[0] == "external"
    assert processor._run_python_logic(Path("logic.py"), "logic", "df")[0] == "python"
    assert processor._run_notebook_logic(Path("logic.ipynb"), "logic", "df")[0] == "notebook"
    assert processor._load_output_frame(Path("out.parquet"), "parquet")[0] == "loaded"

    fake_gdpr = types.ModuleType("lakelogic.core.gdpr")
    fake_gdpr.forget_subjects = (
        lambda *args, audit_report_out=None, **kwargs: (
            audit_report_out.append({"audit": True}) if audit_report_out is not None else None
        )
        or "forgotten"
    )
    fake_gdpr.mask_pii_columns = lambda *args, **kwargs: ("masked", args, kwargs)
    monkeypatch.setitem(sys.modules, "lakelogic.core.gdpr", fake_gdpr)
    assert processor.forget("df", "customer_id", ["1"]) == "forgotten"
    assert processor.last_report == {"audit": True}
    assert processor.mask_pii("df", columns=["email"])[0] == "masked"
    assert processor.forget_hipaa("df", "patient_id", ["p1"]) == "forgotten"
    assert processor.mask_pii_hipaa("df", columns=["email"])[0] == "masked"

    fake_hipaa = types.ModuleType("lakelogic.core.hipaa")
    fake_hipaa.forget_patients = lambda *args, **kwargs: ("forgot-patient", args, kwargs)
    fake_hipaa.mask_phi_columns = lambda *args, **kwargs: ("masked-phi", args, kwargs)
    monkeypatch.setitem(sys.modules, "lakelogic.core.hipaa", fake_hipaa)
    assert processor.forget_patient("df", "patient_id", ["p1"])[0] == "forgot-patient"
    assert processor.mask_phi("df", columns=["mrn"])[0] == "masked-phi"

    fake_dim_date = types.ModuleType("lakelogic.core.dim_date")
    fake_dim_date.generate_date_dimension = lambda **kwargs: ("dim-date", kwargs)
    monkeypatch.setitem(sys.modules, "lakelogic.core.dim_date", fake_dim_date)
    assert processor.generate_date_dimension(start_date="2026-01-01")[0] == "dim-date"


def test_processor_trace_error_and_logging_branches(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor._active_trace_steps = []
    with pytest.raises(RuntimeError, match="boom"):
        with processor.trace_step("failing"):
            raise RuntimeError("boom")
    assert processor._active_trace_steps[-1].status == "error"
    assert processor._active_trace_steps[-1].details["error"] == "boom"

    removed = []
    added = []
    monkeypatch.setattr(proc_mod.logger, "remove", lambda: removed.append(True))
    monkeypatch.setattr(proc_mod.logger, "add", lambda *args, **kwargs: added.append((args, kwargs)))
    monkeypatch.setenv("LAKELOGIC_DEBUG", "false")
    processor._configure_logging()
    assert removed and added

    monkeypatch.setenv("LAKELOGIC_DEBUG", "true")
    removed.clear()
    processor._configure_logging()
    assert removed == []


def test_processor_run_source_dispatch_config_errors_and_partition_early_return(monkeypatch, tmp_path):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._resolved_data_layer = "bronze"
    processor._resolve_source_path = lambda value: str(value)
    processor._is_uri_path = lambda path: False
    processor._empty_frame = lambda: pl.DataFrame(schema={"id": pl.Int64})
    writes = []
    processor._write_empty_run_log = lambda stage="no_new_data": writes.append(stage)

    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(type="dlt", path=None),
        materialization=None,
    )
    processor._run_dlt_source = lambda: "dlt-result"
    assert processor.run_source() == "dlt-result"

    processor.contract.source = types.SimpleNamespace(type="database", path="postgresql://db/orders")
    processor._run_database_source = lambda: "db-result"
    assert processor.run_source() == "db-result"

    processor.contract.source = None
    with pytest.raises(ValueError, match="No source path provided"):
        processor.run_source()

    fake_catalog = types.ModuleType("lakelogic.engines.catalog_resolver")
    fake_catalog.resolve_catalog_path = lambda path: path
    monkeypatch.setitem(sys.modules, "lakelogic.engines.catalog_resolver", fake_catalog)

    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(
            type="table",
            path="table:catalog.orders",
            load_mode="incremental",
            watermark_strategy="pipeline_log",
            partition=None,
        ),
        materialization=None,
    )
    with pytest.raises(ValueError, match="source type 'table' cannot use"):
        processor.run_source()

    processor.contract.source = types.SimpleNamespace(
        type="file",
        path=str(tmp_path / "orders.csv"),
        load_mode="incremental",
        watermark_strategy="delta_version",
        partition=None,
    )
    with pytest.raises(ValueError, match="file-based source cannot use"):
        processor.run_source()

    processor.contract.source = types.SimpleNamespace(
        type="file",
        path=str(tmp_path / "landing"),
        load_mode="full",
        watermark_strategy=None,
        partition=types.SimpleNamespace(format="year=%Y", lookback_days=1),
    )
    processor._expand_partitioned_paths = lambda *args, **kwargs: []
    result = processor.run_source()
    assert isinstance(result, proc_mod.ValidationResult)
    assert writes[-1] == "no_new_data"


def test_processor_run_source_incremental_empty_and_not_found_paths(monkeypatch, tmp_path):
    fake_catalog = types.ModuleType("lakelogic.engines.catalog_resolver")
    fake_catalog.resolve_catalog_path = lambda path: path
    monkeypatch.setitem(sys.modules, "lakelogic.engines.catalog_resolver", fake_catalog)

    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._active_trace_steps = []
    processor._resolved_data_layer = "bronze"
    processor._resolve_source_path = lambda value: str(value)
    processor._is_uri_path = lambda path: False
    processor._empty_frame = lambda: pl.DataFrame(schema={"id": pl.Int64})
    writes = []
    processor._write_empty_run_log = lambda stage="no_new_data": writes.append(stage)
    processor._get_last_source_watermark = lambda: 100.0

    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(
            type="file",
            path=str(tmp_path / "landing"),
            load_mode="incremental",
            watermark_strategy="pipeline_log",
            partition=None,
            format="csv",
        ),
        materialization=None,
        server=None,
        lineage=None,
    )
    processor._expand_source_files = lambda path: [{"path": str(tmp_path / "old.csv"), "mtime": 1.0}]
    result = processor.run_source()
    assert isinstance(result, proc_mod.ValidationResult)
    assert writes[-1] == "no_new_data"

    processor.contract.source.load_mode = "full"
    processor._expand_source_files = lambda path: None
    # The processor now applies _resolve_empty_source_behavior, which fails
    # on missing sources for `load_mode=full` by default. Opt the test
    # contract into the "skip" policy so the FileNotFoundError is treated
    # as benign no_new_data (matching this test's pre-existing assertion).
    processor.contract.source.empty_behavior = "skip"
    missing_path = tmp_path / "missing.csv"
    processor.contract.source.path = str(missing_path)
    monkeypatch.setattr(
        pl,
        "read_csv",
        lambda path, **kwargs: (_ for _ in ()).throw(FileNotFoundError("not found")),
    )
    result = processor.run_source()
    assert isinstance(result, proc_mod.ValidationResult)
    assert processor._run_log_already_written is True


def test_processor_database_source_error_fetch_and_duckdb_paths(monkeypatch):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._resolved_data_layer = "bronze"
    processor.contract = types.SimpleNamespace(
        info=types.SimpleNamespace(title="Orders"),
        dataset=None,
        model=None,
        source=types.SimpleNamespace(path="postgresql://db/orders", query=None, load_mode="full", options={}),
    )
    with pytest.raises(ValueError, match="dataset"):
        processor._run_database_source()

    processor.contract.dataset = "orders"
    processor.contract.source.path = None
    with pytest.raises(ValueError, match="source.path"):
        processor._run_database_source()

    processor.contract.source.path = "postgresql://db/orders"
    processor.contract.source.load_mode = "incremental"
    processor.contract.source.watermark_field = None
    with pytest.raises(ValueError, match="watermark_field"):
        processor._run_database_source()

    processor.contract.source.watermark_field = "updated_at"
    processor.contract.source.options = {"fetch_size": 2}
    processor._get_last_source_watermark = lambda: None

    fake_sqlalchemy = types.ModuleType("sqlalchemy")

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeEngine:
        def execution_options(self, **kwargs):
            return self

        def connect(self):
            return FakeConnection()

    fake_sqlalchemy.create_engine = lambda uri: FakeEngine()
    monkeypatch.setitem(sys.modules, "sqlalchemy", fake_sqlalchemy)
    monkeypatch.setattr(
        pl,
        "read_database",
        lambda query, connection=None, iter_batches=False, batch_size=None: iter(
            [pl.DataFrame({"id": [1]}), pl.DataFrame({"id": [2]})]
        ),
    )
    processor.run = lambda df, source_path=None: proc_mod.ValidationResult(df, pl.DataFrame())
    result = processor._run_database_source()
    assert result.good["id"].to_list() == [1, 2]

    processor.contract.source.options = {}
    monkeypatch.setattr(
        pl,
        "read_database_uri",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("connector missing")),
    )
    with pytest.raises(RuntimeError, match="Polars DB extraction failed"):
        processor._run_database_source()

    duck = object.__new__(proc_mod.DataProcessor)
    duck.engine_name = "duckdb"
    duck._resolved_data_layer = "bronze"
    duck._get_last_source_watermark = lambda: 1.0
    duck.contract = types.SimpleNamespace(
        info=types.SimpleNamespace(title="Orders"),
        dataset="orders",
        model=None,
        source=types.SimpleNamespace(
            path="sqlite:///tmp/orders.db",
            query=None,
            load_mode="incremental",
            watermark_field="updated_at",
            options={},
        ),
    )
    duck.run = lambda df, source_path=None: proc_mod.ValidationResult(df, pl.DataFrame())

    queries = []
    fake_duckdb = types.ModuleType("duckdb")
    fake_duckdb.sql = lambda query: queries.append(query) or types.SimpleNamespace(pl=lambda: pl.DataFrame({"id": [1]}))
    monkeypatch.setitem(sys.modules, "duckdb", fake_duckdb)
    duck_result = duck._run_database_source()
    assert duck_result.good["id"].to_list() == [1]
    assert any("sqlite_scan" in query for query in queries)

    bad_engine = object.__new__(proc_mod.DataProcessor)
    bad_engine.engine_name = "spark"
    bad_engine._resolved_data_layer = "bronze"
    bad_engine.contract = duck.contract
    bad_engine._get_last_source_watermark = lambda: None
    with pytest.raises(ValueError, match="natively supports"):
        bad_engine._run_database_source()


def test_processor_dlt_import_error_and_streaming_parquet_outputs(monkeypatch, tmp_path):
    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor._resolved_data_layer = "bronze"
    processor.contract = types.SimpleNamespace(
        info=types.SimpleNamespace(title="Orders"),
        dataset="orders",
        source=types.SimpleNamespace(dlt=types.SimpleNamespace(source=None, base_url="https://example.test")),
    )

    original_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "lakelogic.adapters.dlt_adapter":
            raise ImportError("missing dlt")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)
    with pytest.raises(ImportError, match="dlt integration requires"):
        processor._run_dlt_source()
    monkeypatch.setattr("builtins.__import__", original_import)

    parquet_path = tmp_path / "orders.parquet"
    pl.DataFrame({"id": [1, 2]}).write_parquet(parquet_path)

    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "polars"
    processor.contract = types.SimpleNamespace(source=types.SimpleNamespace(path=str(parquet_path)))
    processor._resolve_source_path = lambda value: str(value)
    processor.run = lambda lf, source_path=None: proc_mod.ValidationResult(lf, pl.DataFrame())

    out_parquet = tmp_path / "out.parquet"
    result = processor.run_source_streaming(output_path=str(out_parquet))
    assert result == {"target": str(out_parquet), "format": "parquet"}
    assert out_parquet.exists()

    out_default = tmp_path / "out.default"
    result = processor.run_source_streaming(output_path=str(out_default))
    assert result == {"target": str(out_default), "format": "default"}
    assert out_default.exists()


def test_processor_run_source_table_backend_dispatch_and_load_failure(monkeypatch):
    fake_catalog = types.ModuleType("lakelogic.engines.catalog_resolver")
    fake_catalog.resolve_catalog_path = lambda path: path
    monkeypatch.setitem(sys.modules, "lakelogic.engines.catalog_resolver", fake_catalog)

    processor = object.__new__(proc_mod.DataProcessor)
    processor.engine_name = "snowflake"
    processor._active_trace_steps = []
    processor._resolved_data_layer = "silver"
    processor._resolve_source_path = lambda value: str(value)
    processor._is_uri_path = lambda path: False
    processor._expand_source_files = lambda path: None
    processor.contract = types.SimpleNamespace(
        source=types.SimpleNamespace(
            type="table",
            path="table:analytics.orders",
            load_mode="full",
            watermark_strategy=None,
            partition=None,
        ),
        materialization=None,
        server=None,
        lineage=None,
    )
    captured = {}
    processor.run = lambda table_name, source_path=None, reset_trace=False: (
        captured.update({"table_name": table_name, "source_path": source_path, "reset_trace": reset_trace})
        or "table-result"
    )
    assert processor.run_source() == "table-result"
    assert captured == {"table_name": "analytics.orders", "source_path": "analytics.orders", "reset_trace": False}

    processor.engine_name = "unknown"
    processor.run = lambda *args, **kwargs: "should-not-run"
    with pytest.raises(ValueError, match="Could not load data"):
        processor.run_source(source="landing/orders.csv")

    streamer = object.__new__(proc_mod.DataProcessor)
    streamer.engine_name = "polars"
    streamer.contract = types.SimpleNamespace(source=None)
    with pytest.raises(ValueError, match="No source path provided"):
        streamer.run_source_streaming()
