import pytest

from lakelogic.core.models import DataContract


def test_contract_parsing_minimal():
    """Test that a minimal contract can be parsed."""
    data = {"version": "1.0.0", "dataset": "test_ds"}
    contract = DataContract(**data)
    assert contract.version == "1.0.0"
    assert contract.dataset == "test_ds"


def test_contract_quality_rules():
    """Test that quality rules are correctly structured."""
    data = {
        "version": "1.0.0",
        "quality": {"row_rules": [{"name": "test_rule", "sql": "id > 0", "category": "correctness"}]},
    }
    contract = DataContract(**data)
    assert len(contract.quality.row_rules) == 1
    assert contract.quality.row_rules[0].name == "test_rule"
    assert contract.quality.row_rules[0].sql == "id > 0"


def test_row_rule_not_null_list_expands():
    """Ensure not_null accepts a list of fields and expands into rules."""
    from lakelogic.engines.base import EngineAdapter

    class DummyAdapter(EngineAdapter):
        def execute(self, df):
            raise NotImplementedError

    data = {"version": "1.0.0", "quality": {"row_rules": [{"not_null": ["a", "b", "c"]}]}}
    contract = DataContract(**data)
    adapter = DummyAdapter(contract)
    rules = adapter.get_row_rules()
    names = {rule.name for rule in rules}
    assert names == {"a_not_null", "b_not_null", "c_not_null"}


def test_transformation_lookup():
    """Test the lookup transformation model."""
    data = {
        "version": "1.0.0",
        "transformations": [
            {
                "lookup": {
                    "field": "name",
                    "reference": "ref_table",
                    "on": "id",
                    "key": "ref_id",
                    "value": "ref_name",
                    "default_value": "Unknown",
                }
            }
        ],
    }
    contract = DataContract(**data)
    lookup = contract.transformations[0].lookup
    assert lookup.field == "name"
    assert lookup.default_value == "Unknown"


def test_notification_aliases():
    """Test notification target aliases."""
    data = {
        "version": "1.0.0",
        "quarantine": {
            "notifications": [
                {"type": "slack", "channel": "#alerts", "on_events": ["quarantine"]},
                {"type": "email", "to": "owner@example.com", "on_events": ["failure"]},
                {"type": "teams", "url": "https://example.com/webhook", "on_events": ["quarantine"]},
            ]
        },
    }
    contract = DataContract(**data)
    assert contract.quarantine.notifications[0].target == "#alerts"
    assert contract.quarantine.notifications[1].target == "owner@example.com"
    assert contract.quarantine.notifications[2].target == "https://example.com/webhook"


def test_service_level_objective():
    """Test service level parsing with nested objects."""
    data = {
        "version": "1.0.0",
        "service_levels": {
            "freshness": {"description": "Daily", "threshold": "24h", "field": "updated_at"},
            "availability": {"description": "Gold layer", "threshold": 99.9},
        },
    }
    contract = DataContract(**data)
    assert contract.service_levels.freshness.description == "Daily"
    assert contract.service_levels.freshness.threshold == "24h"
    assert contract.service_levels.availability.threshold == 99.9


def test_notification_extra_fields():
    """Extra notification fields should be preserved for adapters."""
    data = {
        "version": "1.0.0",
        "quarantine": {
            "notifications": [
                {
                    "type": "smtp",
                    "target": "alerts@example.com",
                    "smtp_host": "smtp.example.com",
                    "smtp_port": 587,
                    "from_email": "lakelogic@example.com",
                }
            ]
        },
    }
    contract = DataContract(**data)
    notif = contract.quarantine.notifications[0]
    dumped = notif.model_dump()
    assert dumped["smtp_host"] == "smtp.example.com"
    assert dumped["from_email"] == "lakelogic@example.com"


def test_external_logic_parsing():
    """Test external logic configuration parsing."""
    data = {
        "version": "1.0.0",
        "external_logic": {
            "type": "python",
            "path": "gold/build_sales.py",
            "engine": "polars",
            "entrypoint": "build_sales",
            "args": {"target_table": "fact_sales"},
            "output_path": "output/fact_sales.parquet",
            "output_format": "parquet",
            "handles_output": True,
        },
    }
    contract = DataContract(**data)
    logic = contract.external_logic
    assert logic.type == "python"
    assert logic.engine == "polars"
    assert logic.entrypoint == "build_sales"
    assert logic.output_format == "parquet"


def test_external_logic_requires_engine():
    """`engine` is mandatory whenever external_logic is used — external logic runs
    against an engine-specific DataFrame, so it must be declared explicitly."""
    import pytest

    data = {
        "version": "1.0.0",
        "external_logic": {"type": "python", "path": "gold/build.py"},
    }
    with pytest.raises(Exception):
        DataContract(**data)


def test_transformation_rename_mappings():
    """Rename transformation should accept mappings dict."""
    data = {
        "version": "1.0.0",
        "transformations": [{"rename": {"mappings": {"old_a": "new_a", "old_b": "new_b"}}}],
    }
    contract = DataContract(**data)
    rename = contract.transformations[0].rename
    assert rename is not None
    assert rename.iter_pairs() == [("old_a", "new_a"), ("old_b", "new_b")]


def test_quality_rule_category_normalization_warns(caplog):
    """Unknown categories should warn and be normalized to lowercase."""
    import logging

    from loguru import logger

    # Enable loguru -> standard logging propagation for caplog
    class PropagateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropagateHandler(), format="{message}")
    try:
        data = {
            "version": "1.0.0",
            "quality": {"row_rules": [{"name": "weird_cat", "sql": "id > 0", "category": "WeirdCategory"}]},
        }
        with caplog.at_level(logging.WARNING):
            contract = DataContract(**data)
        rule = contract.quality.row_rules[0]
        assert rule.category == "weirdcategory"
        assert any("Unknown quality rule category" in r.message for r in caplog.records)
    finally:
        logger.remove(handler_id)


def test_contract_interceptors_and_load_mode_validation(monkeypatch):
    odcs_contract = DataContract(
        **{
            "kind": "DataContract",
            "apiVersion": "v2",
            "dataset": "orders",
            "schema": [{"name": "id", "type": "int", "required": True}],
            "customProperties": {"lakelogic": {"metadata": {"domain": "commerce"}}},
        }
    )
    assert odcs_contract.version == "v2"
    assert odcs_contract.info.title == "orders"
    assert odcs_contract.model.fields[0].name == "id"
    assert odcs_contract.metadata["domain"] == "commerce"

    soft_delete_contract = DataContract(
        version="1.0",
        soft_deletes={"enabled": True, "flag_field": "is_deleted"},
    )
    assert soft_delete_contract.materialization.soft_delete_column == "is_deleted"
    assert soft_delete_contract.materialization.soft_delete_time_column == "_lakelogic_deleted_at"
    assert soft_delete_contract.materialization.soft_delete_reason_column == "_lakelogic_delete_reason"

    migrated_contract = DataContract(
        version="1.0",
        server={"type": "local", "path": "data/orders", "schema_evolution": "strict", "allow_schema_drift": False},
    )
    assert migrated_contract.server.schema_policy.evolution == "strict"
    assert migrated_contract.server.schema_policy.unknown_fields == "quarantine"

    with pytest.raises(ValueError, match="no run-log backend"):
        DataContract(version="1.0", source={"type": "file", "load_mode": "incremental"}, metadata={})

    monkeypatch.setenv("LAKELOGIC_SKIP_INCREMENTAL_CHECK", "1")
    skipped_incremental = DataContract(version="1.0", source={"type": "file", "load_mode": "incremental"}, metadata={})
    assert skipped_incremental.source.load_mode == "incremental"
    monkeypatch.delenv("LAKELOGIC_SKIP_INCREMENTAL_CHECK", raising=False)

    warnings = []
    monkeypatch.setattr("lakelogic.core.models.logger.warning", lambda message: warnings.append(message))
    incremental_warn = DataContract(
        version="1.0",
        source={"type": "file", "load_mode": "incremental", "watermark_strategy": "lookback"},
        metadata={"run_log_path": "logs/run_log.json"},
    )
    assert incremental_warn.source.load_mode == "incremental"
    assert any("lookback duration is not set" in message for message in warnings)

    with pytest.raises(ValueError, match="cdc_op_field nor cdc_timestamp_field"):
        DataContract(version="1.0", source={"type": "file", "load_mode": "cdc"})


def test_contract_from_yaml_reset_and_effective_server(monkeypatch, tmp_path):
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(
        "\n".join(
            [
                'version: "1.0"',
                "info:",
                '  title: "Orders"',
                '  version: "1.0"',
                "server:",
                '  type: "local"',
                '  path: "prod/orders"',
                '  format: "parquet"',
                "environments:",
                "  dev:",
                '    path: "dev/orders"',
                '    format: "csv"',
                "materialization:",
                '  target_path: "outputs/good.parquet"',
                '  location: "external/location"',
                "quarantine:",
                '  target: "quarantine/bad.parquet"',
                "metadata:",
                '  run_log_dir: "logs/runs"',
                '  run_log_path: "logs/run_log.json"',
            ]
        ),
        encoding="utf-8",
    )

    contract = DataContract.from_yaml(contract_path)
    assert str(contract._base_path) == str(tmp_path)
    assert str(contract._contract_path) == str(contract_path)

    monkeypatch.setenv("LAKELOGIC_ENV", "dev")
    effective = contract.effective_server()
    assert effective.path == "dev/orders"
    assert effective.format == "csv"
    monkeypatch.delenv("LAKELOGIC_ENV", raising=False)

    minimal_override = DataContract(version="1.0", environments={"qa": {"path": "qa/orders"}})
    assert minimal_override.effective_server("qa").path == "qa/orders"
    assert minimal_override.effective_server("qa").format == "parquet"

    output_file = tmp_path / "outputs" / "good.parquet"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("good", encoding="utf-8")
    external_file = tmp_path / "external" / "location"
    external_file.parent.mkdir(parents=True, exist_ok=True)
    external_file.write_text("external", encoding="utf-8")
    quarantine_file = tmp_path / "quarantine" / "bad.parquet"
    quarantine_file.parent.mkdir(parents=True, exist_ok=True)
    quarantine_file.write_text("bad", encoding="utf-8")
    watermark_dir = tmp_path / ".lakelogic"
    watermark_dir.mkdir(parents=True, exist_ok=True)
    (watermark_dir / "watermark_orders.json").write_text("{}", encoding="utf-8")
    run_log_dir = tmp_path / "logs" / "runs"
    run_log_dir.mkdir(parents=True, exist_ok=True)
    (run_log_dir / "run-1.json").write_text("{}", encoding="utf-8")
    run_log_path = tmp_path / "logs" / "run_log.json"
    run_log_path.write_text("{}", encoding="utf-8")

    dry_run = contract.reset(dry_run=True)
    assert dry_run["materialization"]["exists"] is True
    assert dry_run["quarantine"]["exists"] is True
    assert dry_run["watermark"]["dry_run"] is True

    reset_report = contract.reset()
    assert reset_report["materialization"]["deleted"] is True
    assert reset_report["materialization_location"]["deleted"] is True
    assert reset_report["quarantine"]["deleted"] is True
    assert reset_report["watermark"]["deleted"]
    assert all(item["deleted"] is True for item in reset_report["run_log"])
    assert not output_file.exists()
    assert not quarantine_file.exists()
    assert not run_log_path.exists()
