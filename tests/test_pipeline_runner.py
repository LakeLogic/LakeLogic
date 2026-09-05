from __future__ import annotations

import builtins
import sys
import types
from pathlib import Path

import pytest

from lakelogic.pipeline import runner


def test_friendly_validation_error_formats_pydantic_and_generic_errors(monkeypatch):
    class FakeValidationError(Exception):
        def errors(self):
            return [
                {"loc": ("model", "fields", 0, "name"), "msg": "Field required"},
                {"loc": ("server", "path"), "msg": "Invalid path"},
            ]

    fake_pydantic = types.ModuleType("pydantic")
    fake_pydantic.ValidationError = FakeValidationError
    monkeypatch.setitem(sys.modules, "pydantic", fake_pydantic)

    message = runner._friendly_validation_error("orders", FakeValidationError())
    assert "Contract 'orders' has validation errors" in message
    assert "model → fields → 0 → name: Field required" in message
    assert "server → path: Invalid path" in message

    assert runner._friendly_validation_error("orders", RuntimeError("bad ddl")) == "DDL failed for orders: bad ddl"


def test_pipeline_run_summary_append_replace_and_render():
    summary = runner.PipelineRunSummary("run-1", "dev", dry_run=False)
    assert summary.to_dict() == {"run_id": "run-1", "environment": "dev", "dry_run": False, "results": []}
    assert "No contracts processed" in str(summary)

    summary.append("orders", "bronze", "success", rows=10, rows_good=8, rows_bad=2, table_name="bronze_orders")
    summary.append("orders", "bronze", "failed", rows=11, error="boom", table_name="bronze_orders")
    summary.append("customers", "silver", "success", rows="100", table_name="silver_customers")

    assert len(summary.results) == 2
    assert summary.results[0]["status"] == "failed"
    rendered = str(summary)
    assert "PIPELINE RUN SUMMARY" in rendered
    assert "bronze_orders" in rendered
    assert "silver_customers" in rendered
    assert "Error: boom" in rendered

    # has_failures() / failure_details() surface the real per-contract error so
    # callers can put it on a raised exception (job runners drop notebook stdout).
    assert summary.has_failures() is True
    details = summary.failure_details()
    assert "1 contract(s) failed" in details
    assert "orders [bronze]" in details
    assert "boom" in details


def test_pipeline_run_summary_no_failures_details_empty():
    summary = runner.PipelineRunSummary("run-2", "dev", dry_run=False)
    summary.append("orders", "bronze", "success", rows=10, table_name="bronze_orders")
    assert summary.has_failures() is False
    assert summary.failure_details() == ""


def test_runner_mode_and_catalog_reference_detection():
    registry = types.SimpleNamespace(
        storage_mode="uc", storage=None, quarantine=None, lineage=None, materialization=None, server=None
    )
    pipeline = runner.LakehousePipeline(registry, engine="polars")

    assert pipeline._resolve_run_log_mode({"metadata": {"run_log_table": "catalog.logs"}}) == "table"
    assert pipeline._resolve_run_log_mode({}, explicit_mode="json") == "json"
    pipeline.storage_mode = "direct"
    assert pipeline._resolve_run_log_mode({"metadata": {"run_log_table": "catalog.logs"}}) is None

    assert runner.LakehousePipeline._looks_like_catalog_ref("catalog.schema.table") is True
    assert runner.LakehousePipeline._looks_like_catalog_ref("`catalog`.schema.table") is True
    assert runner.LakehousePipeline._looks_like_catalog_ref("table:catalog.schema.table") is False
    assert runner.LakehousePipeline._looks_like_catalog_ref("abfss://container/path") is False
    assert runner.LakehousePipeline._looks_like_catalog_ref("") is False


def test_resolve_uc_paths_for_uc_storage_mode_and_defaults():
    storage = types.SimpleNamespace(
        domain_catalog="catalog.domain",
        quarantine_root="catalog.quarantine",
        run_log_table="catalog.logs",
        external_location_root="abfss://lake/root",
        bronze_path="abfss://lake/bronze",
        silver_path="abfss://lake/silver",
        gold_path="abfss://lake/gold",
    )
    registry = types.SimpleNamespace(
        storage_mode="uc",
        storage=storage,
        quarantine={"enabled": True, "notifications": ["ops"]},
        lineage={"owner": "data-eng"},
        materialization={"silver": {"format": "delta", "compression": "zstd"}},
        server={"silver": {"schema_policy": {"unknown_fields": "quarantine"}, "cast_to_string": True}},
    )
    pipeline = runner.LakehousePipeline(registry, engine="polars")

    contract = {
        "info": {"table_name": "orders", "target_layer": "silver", "domain": "commerce"},
        "source": {"type": "table"},
        "links": [{"path": "catalog.ref.customers"}],
    }
    resolved = pipeline._resolve_uc_paths(contract)

    assert resolved["materialization"]["target_path"] == "table:catalog.domain.orders"
    assert resolved["materialization"]["compression"] == "zstd"
    assert resolved["quarantine"]["target"] == "table:catalog.quarantine.commerce_orders"
    assert resolved["metadata"]["run_log_table"] == "catalog.logs"
    assert resolved["source"]["path"] == "table:catalog.domain.orders"
    assert resolved["lineage"]["owner"] == "data-eng"
    assert resolved["server"]["schema_policy"]["unknown_fields"] == "quarantine"
    assert resolved["server"]["cast_to_string"] is True
    assert resolved["links"][0]["path"] == "table:catalog.ref.customers"
    assert resolved["links"][0]["type"] == "table"


def test_resolve_uc_paths_for_direct_mode_and_credential_warnings(monkeypatch):
    warnings = []
    monkeypatch.setattr(runner.logger, "warning", warnings.append)
    storage = types.SimpleNamespace(
        domain_catalog=None,
        quarantine_root=None,
        run_log_table=None,
        external_location_root="abfss://acct/root",
        bronze_path="abfss://acct/bronze",
        silver_path="abfss://acct/silver",
        gold_path="abfss://acct/gold",
    )
    registry = types.SimpleNamespace(
        storage_mode="direct",
        storage=storage,
        quarantine={"enabled": True},
        lineage={},
        materialization={"gold": {"partition_by": ["event_date"]}},
        server={"gold": {"schema_policy": {"unknown_fields": "drop"}}},
    )
    pipeline = runner.LakehousePipeline(registry, engine="polars")

    contract = {
        "info": {"table_name": "curated_orders", "target_layer": "gold", "domain": "commerce"},
        "source": {},
    }
    resolved = pipeline._resolve_uc_paths(contract)
    assert resolved["materialization"]["target_path"] == "abfss://acct/gold/curated_orders"
    assert resolved["materialization"]["location"] == "abfss://acct/gold/curated_orders"
    assert resolved["materialization"]["format"] == "delta"
    assert resolved["materialization"]["partition_by"] == ["event_date"]
    assert resolved["quarantine"]["target"] == "abfss://acct/gold/_quarantine/commerce_curated_orders"
    assert resolved["source"]["path"] == "abfss://acct/gold/curated_orders"
    assert resolved["source"]["type"] == "landing"
    assert resolved["source"]["format"] == "delta"
    assert resolved["server"]["schema_policy"]["unknown_fields"] == "drop"
    assert any("no credentials detected" in message for message in warnings)

    missing_root_storage = types.SimpleNamespace(
        domain_catalog=None,
        quarantine_root=None,
        run_log_table=None,
        external_location_root=None,
        bronze_path=None,
        silver_path=None,
        gold_path=None,
    )
    bad_registry = types.SimpleNamespace(
        storage_mode="direct",
        storage=missing_root_storage,
        quarantine=None,
        lineage=None,
        materialization=None,
        server=None,
    )
    bad_pipeline = runner.LakehousePipeline(bad_registry, engine="polars")
    with pytest.raises(ValueError, match="external_location_root"):
        bad_pipeline._resolve_uc_paths({"info": {"table_name": "orders", "target_layer": "bronze"}})


def test_dependency_helpers_and_erasure_strategy_resolution():
    contracts = [
        types.SimpleNamespace(entity="bronze_orders", depends_on=[]),
        types.SimpleNamespace(entity="silver_orders", depends_on=["bronze_orders"]),
        types.SimpleNamespace(entity="gold_orders", depends_on=["silver_orders"]),
        types.SimpleNamespace(entity="silver_customers", depends_on=[]),
    ]
    ordered = runner.LakehousePipeline._topological_sort(contracts)
    assert [contract.entity for contract in ordered][:2] == ["bronze_orders", "silver_customers"]
    waves = runner.LakehousePipeline._group_by_dependency_level(contracts)
    assert {contract.entity for contract in waves[0]} == {"bronze_orders", "silver_customers"}
    assert [contract.entity for contract in waves[1]] == ["silver_orders"]
    assert [contract.entity for contract in waves[2]] == ["gold_orders"]

    cyclic = [
        types.SimpleNamespace(entity="a", depends_on=["b"]),
        types.SimpleNamespace(entity="b", depends_on=["a"]),
    ]
    with pytest.raises(ValueError, match="Circular dependency"):
        runner.LakehousePipeline._topological_sort(cyclic)
    with pytest.raises(ValueError, match="Circular dependency"):
        runner.LakehousePipeline._group_by_dependency_level(cyclic)

    registry = types.SimpleNamespace(compliance={"erasure": {"strategy": "hash"}}, storage_mode="uc", storage=None)
    pipeline = runner.LakehousePipeline(registry, engine="polars")
    assert (
        pipeline._resolve_erasure_strategy({"compliance": {"erasure": {"strategy": "redact"}}}, "nullify") == "redact"
    )
    assert pipeline._resolve_erasure_strategy({}, "nullify") == "hash"
    assert pipeline._resolve_erasure_strategy(None, "nullify") == "hash"


def test_write_test_data_and_schema_helpers(monkeypatch, tmp_path):
    df = pytest.importorskip("polars").DataFrame({"id": [1], "status": ["active"]})
    json_path = tmp_path / "data.json"
    csv_path = tmp_path / "data.csv"
    parquet_path = tmp_path / "data.parquet"
    other_path = tmp_path / "data.unknown"
    runner.LakehousePipeline._write_test_data(df, json_path, "json")
    runner.LakehousePipeline._write_test_data(df, csv_path, "csv")
    runner.LakehousePipeline._write_test_data(df, parquet_path, "parquet")
    runner.LakehousePipeline._write_test_data(df, other_path, "other")
    assert json_path.exists()
    assert csv_path.exists()
    assert parquet_path.exists()
    assert other_path.exists()
    runner.LakehousePipeline._write_test_data({"not": "polars"}, tmp_path / "ignored.json", "json")
    assert not (tmp_path / "ignored.json").exists()

    monkeypatch.setattr("lakelogic.core.ddl._resolve_table_name", lambda contract: "catalog.orders")

    class FakeDeltaField:
        def __init__(self, name, type_text):
            self.name = name
            self.type = type_text

    class FakeDeltaSchema:
        fields = [FakeDeltaField("id", 'PrimitiveType("int64")'), FakeDeltaField("created_at", "timestamp")]

    class FakeDeltaTable:
        def __init__(self, path, storage_options=None):
            self.path = path
            self.storage_options = storage_options

        def schema(self):
            return FakeDeltaSchema()

    monkeypatch.setitem(sys.modules, "deltalake", types.SimpleNamespace(DeltaTable=FakeDeltaTable))

    fake_contract = types.SimpleNamespace(materialization=types.SimpleNamespace(target_path="abfss://lake/orders"))
    col_names, col_types = runner.LakehousePipeline._introspect_table_schema(fake_contract, "polars")
    assert col_names == ["id", "created_at"]
    assert col_types == {"id": "BIGINT", "created_at": "TIMESTAMP"}

    spark_contract = types.SimpleNamespace(materialization=types.SimpleNamespace(target_path="table:catalog.orders"))
    fake_df = types.SimpleNamespace(
        columns=["id"],
        schema=types.SimpleNamespace(
            fields=[types.SimpleNamespace(name="id", dataType=types.SimpleNamespace(simpleString=lambda: "int"))]
        ),
    )
    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.SparkSession = types.SimpleNamespace(
        builder=types.SimpleNamespace(
            getOrCreate=lambda: types.SimpleNamespace(table=lambda name: fake_df, sql=lambda stmt: None)
        )
    )
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)
    col_names, col_types = runner.LakehousePipeline._introspect_table_schema(spark_contract, "spark")
    assert col_names == ["id"]
    assert col_types == {"id": "INT"}


def test_execute_alter_statements_routes_by_backend(monkeypatch):
    infos = []
    warnings = []
    monkeypatch.setattr(runner.logger, "info", infos.append)
    monkeypatch.setattr(runner.logger, "warning", warnings.append)

    runner.LakehousePipeline._execute_alter_statements(["ALTER TABLE x ADD COLUMN y INT"], "polars", "orders")
    assert any("schema_mode='merge'" in message for message in infos)

    executed = []
    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.SparkSession = types.SimpleNamespace(
        builder=types.SimpleNamespace(getOrCreate=lambda: types.SimpleNamespace(sql=lambda stmt: executed.append(stmt)))
    )
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)
    runner.LakehousePipeline._execute_alter_statements(["ALTER TABLE x ADD COLUMN y INT"], "spark", "orders")
    assert executed == ["ALTER TABLE x ADD COLUMN y INT"]

    runner.LakehousePipeline._execute_alter_statements(["ALTER TABLE x ADD COLUMN y INT"], "snowflake", "orders")
    assert any("manual execution required" in message for message in infos)


def test_delete_run_log_entries_for_spark_and_delta(monkeypatch):
    infos = []
    debug = []
    monkeypatch.setattr(runner.logger, "info", infos.append)
    monkeypatch.setattr(runner.logger, "debug", debug.append)

    fake_spark = types.SimpleNamespace(
        catalog=types.SimpleNamespace(tableExists=lambda table: table == "catalog.logs"),
        sql=lambda stmt: infos.append(stmt),
    )
    registry = types.SimpleNamespace(storage_mode="uc", storage=None)
    pipeline = runner.LakehousePipeline(registry, engine="spark", spark=fake_spark)
    contract_dict = {
        "metadata": {"run_log_table": "catalog.logs", "domain": "commerce", "system": "erp"},
        "materialization": {"target_path": "table:catalog.domain.orders"},
        "info": {"target_layer": "silver"},
    }
    pipeline._delete_run_log_entries(contract_dict, "orders", "silver")
    assert any("DELETE FROM catalog.logs" in message for message in infos)
    assert any("dataset = 'orders'" in message for message in infos)
    assert any("domain = 'commerce'" in message for message in infos)

    class FakeDeltaTable:
        def __init__(self, path, storage_options=None):
            self.path = path
            self.storage_options = storage_options

        def delete(self, predicate):
            infos.append(f"delta::{self.path}::{predicate}")

    monkeypatch.setitem(sys.modules, "deltalake", types.SimpleNamespace(DeltaTable=FakeDeltaTable))
    monkeypatch.setitem(
        sys.modules,
        "lakelogic.core.run_log",
        types.SimpleNamespace(_build_cloud_opts=lambda table: {"table": table}),
    )
    non_spark_pipeline = runner.LakehousePipeline(registry, engine="polars")
    delta_contract = {
        "metadata": {"run_log_table": "abfss://lake/logs"},
        "info": {"table_name": "customers", "domain": "crm", "target_layer": "gold"},
    }
    non_spark_pipeline._delete_run_log_entries(delta_contract, "customers", "gold")
    assert any(message.startswith("delta::abfss://lake/logs::dataset = 'customers'") for message in infos)


def test_execute_resets_and_reloads_routes_to_processor_and_spark(monkeypatch, tmp_path):
    reset_calls = []
    dropped = []

    class FakeProcessor:
        def __init__(self, contract, engine, pipeline_run_id):
            self.contract = contract
            self.engine = engine
            self.pipeline_run_id = pipeline_run_id

        def reset(self, targets=None, dry_run=False):
            reset_calls.append({"targets": targets, "dry_run": dry_run, "contract": self.contract})

    monkeypatch.setattr(runner, "DataProcessor", FakeProcessor)

    fake_spark = types.SimpleNamespace(
        sql=lambda stmt: dropped.append(stmt),
        catalog=types.SimpleNamespace(tableExists=lambda _: True),
        _jvm=types.SimpleNamespace(
            com=types.SimpleNamespace(
                databricks=types.SimpleNamespace(
                    service=types.SimpleNamespace(
                        DBUtils=lambda *_args: (_ for _ in ()).throw(RuntimeError("no dbutils"))
                    )
                )
            )
        ),
        _jsc=types.SimpleNamespace(sc=lambda: object()),
    )
    storage = types.SimpleNamespace(domain_catalog="catalog.domain", external_location_root=None)
    registry = types.SimpleNamespace(storage_mode="uc", storage=storage)
    pipeline = runner.LakehousePipeline(registry, engine="spark", spark=fake_spark)
    monkeypatch.setattr(
        pipeline,
        "_delete_run_log_entries",
        lambda contract_dict, name, layer: dropped.append(f"runlog::{name}::{layer}"),
    )

    reset_contract = types.SimpleNamespace(
        layer="silver",
        entity="orders",
        contract_dict={
            "materialization": {"target_path": "table:catalog.domain.orders"},
            "quarantine": {"enabled": True, "target": "table:catalog.quarantine.orders"},
            "info": {"table_name": "orders", "domain": "commerce"},
        },
    )
    reload_contract = types.SimpleNamespace(
        layer="gold",
        entity="curated_orders",
        contract_dict={
            "materialization": {"target_path": str(tmp_path / "materialized")},
            "quarantine": {"enabled": True, "target": str(tmp_path / "quarantine")},
        },
    )

    pipeline._execute_resets([reset_contract, reload_contract], {"silver"}, {"gold"}, dry_run=False)
    assert any(
        call[0] == "DROP TABLE IF EXISTS catalog.domain.orders"
        for call in [(stmt,) for stmt in dropped]
        if isinstance(call[0], str)
    )
    assert "DROP TABLE IF EXISTS catalog.quarantine.orders" in dropped
    assert "runlog::orders::silver" in dropped
    assert any(call["targets"] is None and call["dry_run"] is False for call in reset_calls)
    assert any(call["targets"] == ["materialization"] for call in reset_calls)
    assert any(call["targets"] == ["quarantine"] for call in reset_calls)
    assert any(call["targets"] == ["watermark", "run_log"] for call in reset_calls)


def test_execute_resets_deletes_cloud_and_local_quarantine_targets(monkeypatch, tmp_path):
    reset_calls = []
    deleted_run_logs = []
    infos = []
    monkeypatch.setattr(runner.logger, "info", infos.append)

    class FakeProcessor:
        def __init__(self, contract, engine, pipeline_run_id):
            self.contract = contract

        def reset(self, targets=None, dry_run=False):
            reset_calls.append({"targets": targets, "dry_run": dry_run})

    monkeypatch.setattr(runner, "DataProcessor", FakeProcessor)

    cloud_events = []

    class FakeFS:
        def exists(self, path):
            cloud_events.append(("exists", path))
            return True

        def rm(self, path, recursive=False):
            cloud_events.append(("rm", path, recursive))

    fake_fsspec = types.ModuleType("fsspec")
    fake_fsspec.core = types.SimpleNamespace(url_to_fs=lambda path, **opts: (FakeFS(), "resolved/quarantine"))
    monkeypatch.setitem(sys.modules, "fsspec", fake_fsspec)

    local_quarantine = tmp_path / "quarantine_local"
    local_quarantine.mkdir()
    (local_quarantine / "part-0001.parquet").write_text("x", encoding="utf-8")

    pipeline = runner.LakehousePipeline(types.SimpleNamespace(storage_mode="direct", storage=None), engine="polars")
    monkeypatch.setattr(
        pipeline, "_delete_run_log_entries", lambda contract_dict, name, layer: deleted_run_logs.append((name, layer))
    )

    contracts = [
        types.SimpleNamespace(
            layer="silver",
            entity="orders_cloud",
            contract_dict={"quarantine": {"enabled": True, "target": "abfss://acct/root/quarantine/orders"}},
        ),
        types.SimpleNamespace(
            layer="silver",
            entity="orders_local",
            contract_dict={"quarantine": {"enabled": True, "target": str(local_quarantine)}},
        ),
    ]

    pipeline._execute_resets(contracts, {"silver"}, set(), dry_run=False)

    assert any(event[0] == "rm" and event[1] == "resolved/quarantine" and event[2] is True for event in cloud_events)
    assert not local_quarantine.exists()
    assert deleted_run_logs == [("orders_cloud", "silver"), ("orders_local", "silver")]
    assert all(call["targets"] is None and call["dry_run"] is False for call in reset_calls)
    assert any("deleted quarantine cloud path" in message for message in infos)
    assert any("deleted local quarantine path" in message for message in infos)


def test_generate_ddl_only_and_generate_test_data(monkeypatch, tmp_path):
    infos = []
    monkeypatch.setattr(runner.logger, "info", infos.append)

    class FakeProcessor:
        def __init__(self, contract, engine, pipeline_run_id):
            self.contract = contract

        def generate_ddl(self, backend):
            return f"CREATE TABLE {backend}"

        def create_table(self, backend):
            infos.append(f"create::{backend}::{self.contract['info']['table_name']}")

    monkeypatch.setattr(runner, "DataProcessor", FakeProcessor)
    monkeypatch.setitem(
        sys.modules,
        "lakelogic.core.ddl",
        types.SimpleNamespace(
            _resolve_table_name=lambda contract: f"catalog.{contract['info']['table_name']}",
            generate_alter_ddl=lambda contract, engine, existing_cols, existing_column_types=None: (
                ["ALTER TABLE add column x INT"] if existing_cols else []
            ),
        ),
    )

    registry = types.SimpleNamespace(storage_mode="uc", storage=None)
    pipeline = runner.LakehousePipeline(registry, engine="polars")
    monkeypatch.setattr(pipeline, "_introspect_table_schema", lambda contract, engine: (["id"], {"id": "BIGINT"}))
    executed = []
    monkeypatch.setattr(
        pipeline,
        "_execute_alter_statements",
        lambda statements, engine, entity: executed.append((statements, engine, entity)),
    )

    contracts = [
        types.SimpleNamespace(entity="orders", layer="silver", contract_dict={"info": {"table_name": "orders"}})
    ]
    dry_summary = pipeline.generate_ddl_only(contracts, dry_run=True)
    assert dry_summary.results[0]["status"] == "ddl_dry_run"
    real_summary = pipeline.generate_ddl_only(contracts, dry_run=False)
    assert real_summary.results[0]["status"] == "ddl_created"
    assert executed == [(["ALTER TABLE add column x INT"], "polars", "orders")]

    class FakeGenerator:
        def __init__(self, resolved_path, seed=42):
            self.resolved_path = resolved_path
            self.seed = seed

        def generate(self, rows, invalid_ratio, ai, ai_provider=None, ai_model=None):
            return types.SimpleNamespace(shape=(rows, 2), to_pandas=lambda: {"rows": rows})

        def save_with_report(self, df, output_dir, name, format):
            data_path = output_dir / f"{name}.{format}"
            invalid_path = output_dir / f"{name}_invalid.{format}"
            report_path = output_dir / f"{name}_report.json"
            return data_path, invalid_path, report_path

    monkeypatch.setitem(sys.modules, "lakelogic.core.generator", types.SimpleNamespace(DataGenerator=FakeGenerator))
    writes = []
    monkeypatch.setattr(pipeline, "_write_test_data", lambda df, path, fmt: writes.append((Path(path), fmt)))
    bronze_contracts = [
        types.SimpleNamespace(
            entity="landing_orders",
            layer="bronze",
            resolved_path=str(tmp_path / "orders.yaml"),
            contract_dict={
                "model": {"fields": [{"name": "id", "required": True}, {"name": "status", "required": False}]},
                "source": {"path": str(tmp_path / "landing"), "format": "json"},
            },
        ),
        types.SimpleNamespace(
            entity="partitioned_events",
            layer="bronze",
            resolved_path=str(tmp_path / "events.yaml"),
            contract_dict={
                "model": {"fields": [{"name": "id", "required": True}]},
                "source": {
                    "path": str(tmp_path / "partitioned"),
                    "format": "csv",
                    "partition": {"format": "y_%Y/m_%m/d_%d", "lookback_days": 2},
                },
            },
        ),
    ]
    pipeline._generate_test_data(bronze_contracts, rows=6, invalid_ratio=0.1)
    assert any(fmt == "csv" for _, fmt in writes)
    assert (tmp_path / "landing").exists()


def test_generate_test_data_suggest_rules_success(monkeypatch, tmp_path):
    infos = []
    monkeypatch.setattr(runner.logger, "info", infos.append)

    class FakeFrame:
        def to_pandas(self):
            return {"rows": 4}

    class FakeGenerator:
        def __init__(self, resolved_path, seed=42):
            self.resolved_path = resolved_path

        def generate(self, rows, invalid_ratio, ai, ai_provider=None, ai_model=None):
            return FakeFrame()

    monkeypatch.setitem(sys.modules, "lakelogic.core.generator", types.SimpleNamespace(DataGenerator=FakeGenerator))
    monkeypatch.setattr(runner.LakehousePipeline, "_write_test_data", lambda self, df, path, fmt: None)
    monkeypatch.setitem(
        sys.modules,
        "lakelogic.ai.contract_enricher",
        types.SimpleNamespace(
            enrich_contract=lambda contract_dict, pd_df, provider=None, model=None, api_key=None, sample_size=20: {
                **contract_dict,
                "quality": {"row_rules": [{"name": "id_not_null", "sql": "id IS NOT NULL"}]},
            }
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "lakelogic.core.bootstrap",
        types.SimpleNamespace(_format_contract_yaml=lambda data: "quality:\n  row_rules:\n    - name: id_not_null\n"),
    )

    resolved_path = tmp_path / "orders.yaml"
    resolved_path.write_text("version: 1.0.0\n", encoding="utf-8")
    contracts = [
        types.SimpleNamespace(
            entity="orders",
            layer="bronze",
            resolved_path=resolved_path,
            contract_dict={
                "model": {"fields": [{"name": "id", "required": True}]},
                "source": {"path": str(tmp_path / "landing"), "format": "csv"},
            },
        )
    ]

    pipeline = runner.LakehousePipeline(types.SimpleNamespace(storage_mode="direct", storage=None), engine="polars")
    pipeline._generate_test_data(
        contracts, rows=4, invalid_ratio=0.0, suggest_rules=True, ai_provider="openai", ai_model="gpt"
    )

    assert "row_rules" in resolved_path.read_text(encoding="utf-8")
    assert any("Saved suggested rules" in message for message in infos)


def test_generate_ddl_only_raises_on_failures(monkeypatch):
    class FakeProcessor:
        def __init__(self, contract, engine, pipeline_run_id):
            self.contract = contract

        def create_table(self, backend):
            if self.contract["info"]["table_name"] == "bad_orders":
                raise RuntimeError("bad ddl")

    monkeypatch.setattr(runner, "DataProcessor", FakeProcessor)
    monkeypatch.setitem(
        sys.modules,
        "lakelogic.core.ddl",
        types.SimpleNamespace(
            _resolve_table_name=lambda contract: f"catalog.{contract['info']['table_name']}",
            generate_alter_ddl=lambda *args, **kwargs: [],
        ),
    )
    pipeline = runner.LakehousePipeline(types.SimpleNamespace(storage_mode="uc", storage=None), engine="polars")
    monkeypatch.setattr(pipeline, "_introspect_table_schema", lambda contract, engine: ([], {}))
    contracts = [
        types.SimpleNamespace(
            entity="bad_orders", layer="silver", contract_dict={"info": {"table_name": "bad_orders"}}
        ),
    ]
    with pytest.raises(RuntimeError, match="DDL failed for 1 contract"):
        pipeline.generate_ddl_only(contracts, dry_run=False)


def test_execute_resets_uses_ipython_dbutils_for_quarantine_cloud_cleanup(monkeypatch):
    removed = []

    class FakeProcessor:
        def __init__(self, contract, engine, pipeline_run_id):
            self.contract = contract

        def reset(self, targets=None, dry_run=False):
            return None

    monkeypatch.setattr(runner, "DataProcessor", FakeProcessor)
    monkeypatch.setitem(
        sys.modules,
        "IPython",
        types.SimpleNamespace(
            get_ipython=lambda: types.SimpleNamespace(
                user_ns={
                    "dbutils": types.SimpleNamespace(
                        fs=types.SimpleNamespace(rm=lambda path, recursive: removed.append((path, recursive)))
                    )
                }
            )
        ),
    )

    fake_spark = types.SimpleNamespace(
        sql=lambda stmt: None,
        catalog=types.SimpleNamespace(tableExists=lambda _: True),
        _jvm=types.SimpleNamespace(
            com=types.SimpleNamespace(
                databricks=types.SimpleNamespace(
                    service=types.SimpleNamespace(
                        DBUtils=lambda *_args: (_ for _ in ()).throw(RuntimeError("no direct dbutils"))
                    )
                )
            )
        ),
        _jsc=types.SimpleNamespace(sc=lambda: object()),
    )
    storage = types.SimpleNamespace(domain_catalog="catalog.domain", external_location_root="abfss://lake/root")
    pipeline = runner.LakehousePipeline(
        types.SimpleNamespace(storage_mode="uc", storage=storage), engine="spark", spark=fake_spark
    )
    monkeypatch.setattr(pipeline, "_delete_run_log_entries", lambda contract_dict, name, layer: None)

    reset_contract = types.SimpleNamespace(
        layer="silver",
        entity="orders",
        contract_dict={
            "materialization": {"target_path": "table:catalog.domain.orders"},
            "quarantine": {"enabled": True, "target": "table:catalog.quarantine.orders"},
            "info": {"table_name": "orders", "domain": "commerce"},
        },
    )

    pipeline._execute_resets([reset_contract], {"silver"}, set(), dry_run=False)

    assert removed == [("abfss://lake/root/_quarantine/commerce_orders", True)]


def test_process_single_contract_spark_conversion_and_fail_on_quarantine(monkeypatch):
    pl = pytest.importorskip("polars")
    materialized = []

    class FakeDataFrame:
        def __init__(self, cols):
            self.columns = cols

        def to_pandas(self):
            return self

    class FakeProcessor:
        def __init__(self, contract, engine, pipeline_run_id, run_log_mode=None):
            self.contract = types.SimpleNamespace(quarantine=types.SimpleNamespace(fail_on_quarantine=True))
            self.last_report = {
                "counts": {"source": 3, "good": 2, "quarantined": 1},
                "row_rule_failures": [{"message": "id_not_null"}],
            }

        def run_source(self, **kwargs):
            good = FakeDataFrame(["id", "nullable"])
            bad = FakeDataFrame(["id", "nullable"])
            return types.SimpleNamespace(good=good, bad=bad)

        def materialize(self, good_df, bad_df):
            materialized.append((good_df, bad_df))

    converted = []

    class FakeSparkFrame:
        def __init__(self, pdf):
            self.sparkSession = True
            self.pdf = pdf

    fake_spark = types.SimpleNamespace(
        createDataFrame=lambda pdf: converted.append(list(pdf.columns)) or FakeSparkFrame(pdf)
    )
    monkeypatch.setattr(runner, "DataProcessor", FakeProcessor)

    pipeline = runner.LakehousePipeline(
        types.SimpleNamespace(system="crm", domain="sales", storage=None), engine="spark", spark=fake_spark
    )
    summary = runner.PipelineRunSummary("run-1", "dev", dry_run=False)
    contract = types.SimpleNamespace(
        entity="orders", contract_dict={"info": {"title": "Orders", "table_name": "orders"}, "materialization": {}}
    )

    with pytest.raises(ValueError, match=r"Pipeline failed: 1 record\(s\) quarantined") as exc_info:
        pipeline._process_single_contract(
            contract,
            "silver",
            summary,
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            set(),
        )

    assert "id_not_null" in str(exc_info.value)
    assert converted == [["id", "nullable"], ["id", "nullable"]]
    assert len(materialized) == 1


def test_gdpr_and_hipaa_passes_emit_reports(monkeypatch):
    reports = []
    sql_statements = []
    opened = []

    class FakeOpen:
        def __init__(self, path, mode="r", *args, **kwargs):
            self.path = path
            self.mode = mode
            opened.append(path)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def write(self, value):
            return len(value)

    class FakeObserver:
        def report(self, payload):
            reports.append(payload)

    class FakeDataContract:
        def __init__(self, **contract_dict):
            fields = [types.SimpleNamespace(name=name) for name in contract_dict.get("field_names", [])]
            self.model = types.SimpleNamespace(fields=fields)

    class FakeSqlResult:
        def collect(self):
            return [{"num_affected_rows": 2}]

    fake_spark = types.SimpleNamespace(sql=lambda stmt: sql_statements.append(stmt) or FakeSqlResult())
    registry = types.SimpleNamespace(compliance={"erasure": {"strategy": "hash"}}, storage_mode="uc", storage=None)
    pipeline = runner.LakehousePipeline(registry, engine="spark", spark=fake_spark)

    monkeypatch.setattr(runner, "DataContract", FakeDataContract)
    monkeypatch.setattr(runner, "RemoteObserver", lambda: FakeObserver())
    monkeypatch.setattr(runner.os, "makedirs", lambda *args, **kwargs: None)
    monkeypatch.setattr(runner.time, "time", lambda: 123456)
    monkeypatch.setattr(builtins, "open", FakeOpen)
    monkeypatch.setitem(
        sys.modules,
        "lakelogic.core.gdpr",
        types.SimpleNamespace(
            _get_pii_column_names=lambda dc: ["email", "phone"],
            generate_erasure_report=lambda dc, subject_col, subject_ids, strategy, affected, partition_filter=None: {
                "kind": "gdpr",
                "strategy": strategy,
                "affected": affected,
            },
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "lakelogic.core.hipaa",
        types.SimpleNamespace(
            _get_phi_column_names=lambda dc: ["diagnosis", "address"],
            generate_hipaa_erasure_report=lambda dc, patient_col, patient_ids, strategy, affected, partition_filter=None: {
                "kind": "hipaa",
                "strategy": strategy,
                "affected": affected,
            },
        ),
    )

    contract = types.SimpleNamespace(
        entity="patients",
        layer="gold",
        contract_dict={
            "field_names": ["patient_id", "email", "phone", "diagnosis"],
            "materialization": {"target_path": "table:catalog.gold.patients"},
        },
    )
    pipeline._execute_gdpr_pass([contract], "patient_id", ["1", "2"], "nullify", "salt", dry_run=True)
    pipeline._execute_hipaa_pass([contract], "patient_id", ["1"], "redact", "salt", dry_run=False)

    assert any(stmt.startswith("UPDATE catalog.gold.patients SET") for stmt in sql_statements)
    assert {report["kind"] for report in reports} == {"gdpr", "hipaa"}
    assert len(opened) == 2


def test_load_checkpoint_for_spark_and_polars(monkeypatch):
    spark_rows = [{"data_layer": "bronze", "dataset": "orders"}, {"data_layer": "silver", "dataset": "customers"}]
    fake_spark = types.SimpleNamespace(sql=lambda stmt: types.SimpleNamespace(collect=lambda: spark_rows))
    pipeline = runner.LakehousePipeline(
        types.SimpleNamespace(storage=types.SimpleNamespace(run_log_table="catalog.logs")),
        engine="spark",
        spark=fake_spark,
    )

    fake_paths = types.ModuleType("lakelogic.core.paths")
    fake_paths.resolve_run_log_ref = lambda table, engine: f"resolved::{table}::{engine}"
    fake_paths.enrich_azure_storage_options = lambda opts: opts
    monkeypatch.setitem(sys.modules, "lakelogic.core.paths", fake_paths)
    assert pipeline._load_checkpoint("run-1") == {"bronze:orders", "silver:customers"}

    class FakeFiltered:
        def to_dicts(self):
            return [{"data_layer": "gold", "dataset": "payments"}]

    class FakeFrame:
        def filter(self, expr):
            return FakeFiltered()

    class FakeExpr:
        def __eq__(self, other):
            return self

        def __and__(self, other):
            return self

    fake_polars = types.ModuleType("polars")
    fake_polars.col = lambda name: FakeExpr()
    fake_polars.read_delta = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("no delta"))
    fake_polars.read_parquet = lambda *args, **kwargs: FakeFrame()
    monkeypatch.setitem(sys.modules, "polars", fake_polars)
    fake_creds = types.ModuleType("lakelogic.engines.cloud_credentials")
    fake_creds.resolve_storage_options = lambda table: {"table": table}
    monkeypatch.setitem(sys.modules, "lakelogic.engines.cloud_credentials", fake_creds)

    polars_pipeline = runner.LakehousePipeline(
        types.SimpleNamespace(storage=types.SimpleNamespace(run_log_table="abfss://lake/logs")), engine="polars"
    )
    assert polars_pipeline._load_checkpoint("run-2") == {"gold:payments"}


def test_process_contract_with_retry(monkeypatch):
    calls = []

    fake_retry = types.ModuleType("lakelogic.core.retry")

    def retry_call(func, args=(), attempts=1, base_wait_seconds=0, label=None):
        calls.append((attempts, base_wait_seconds, label))
        return func(*args)

    fake_retry.retry_call = retry_call
    monkeypatch.setitem(sys.modules, "lakelogic.core.retry", fake_retry)

    registry = types.SimpleNamespace(domain="commerce", system="erp", storage_mode="uc", storage=None)
    pipeline = runner.LakehousePipeline(registry, engine="polars")
    summary = runner.PipelineRunSummary("run-1", "dev", dry_run=False)

    seen = []
    monkeypatch.setattr(pipeline, "_process_single_contract", lambda *args: seen.append(args[0].entity))
    contract = types.SimpleNamespace(entity="orders")
    pipeline._process_contract_with_retry(
        contract,
        "bronze",
        summary,
        False,
        None,
        None,
        None,
        None,
        None,
        None,
        set(),
        retry_attempts=3,
        retry_base_wait_seconds=5,
    )
    assert seen == ["orders"]
    assert calls == [(3, 5, "orders")]


def test_process_contract_with_retry_timeout_and_parallel_wave(monkeypatch):
    fake_retry = types.ModuleType("lakelogic.core.retry")
    fake_retry.retry_call = lambda func, args=(), attempts=1, base_wait_seconds=0, label=None: func(*args)
    monkeypatch.setitem(sys.modules, "lakelogic.core.retry", fake_retry)

    registry = types.SimpleNamespace(domain="commerce", system="erp", storage_mode="uc", storage=None)
    pipeline = runner.LakehousePipeline(registry, engine="polars")
    summary = runner.PipelineRunSummary("run-1", "dev", dry_run=False)

    thread_events = []

    class FakeAliveThread:
        def __init__(self, target=None, daemon=None):
            self.target = target

        def start(self):
            thread_events.append("start")

        def join(self, timeout=None):
            thread_events.append(timeout)

        def is_alive(self):
            return True

    monkeypatch.setattr(pipeline, "_process_single_contract", lambda *args: None)
    import threading

    real_thread = threading.Thread
    monkeypatch.setattr(threading, "Thread", FakeAliveThread)

    with pytest.raises(runner.EntityTimeoutError, match="orders exceeded timeout"):
        pipeline._process_contract_with_retry(
            types.SimpleNamespace(entity="orders"),
            "bronze",
            summary,
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            set(),
            entity_timeout_minutes=1,
        )
    assert thread_events == ["start", 60]
    monkeypatch.setattr(threading, "Thread", real_thread)

    seen = []
    monkeypatch.setattr(
        pipeline,
        "_process_contract_with_retry",
        lambda contract, *_args, **_kwargs: (
            seen.append(contract.entity) if contract.entity == "ok" else (_ for _ in ()).throw(RuntimeError("boom"))
        ),
    )
    errors = []
    monkeypatch.setattr(runner.logger, "error", errors.append)

    with pytest.raises(RuntimeError, match="boom"):
        pipeline._execute_wave_parallel(
            [types.SimpleNamespace(entity="ok"), types.SimpleNamespace(entity="bad")],
            "bronze",
            summary,
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            set(),
            max_workers=2,
        )
    assert "ok" in seen
    assert any("Parallel execution failed for bad" in message for message in errors)


def test_process_single_contract_success_and_failure_logging(monkeypatch):
    reports = []

    fake_run_log = types.ModuleType("lakelogic.core.run_log")
    fake_run_log.write_run_log = lambda payload, contract, engine_name=None, run_log_mode=None: reports.append(
        (payload, engine_name, run_log_mode)
    )
    # The runner also asks this module to build the failure record.
    from lakelogic.core.run_log import capture_failure as _real_capture_failure

    fake_run_log.capture_failure = _real_capture_failure
    monkeypatch.setitem(sys.modules, "lakelogic.core.run_log", fake_run_log)

    registry = types.SimpleNamespace(
        domain="commerce",
        system="erp",
        ownership={"team": "data"},
        notifications=["slack"],
        notifications_enabled=True,
        storage_mode="uc",
        storage=None,
    )
    pipeline = runner.LakehousePipeline(registry, engine="polars")
    pipeline._created_by_override = "copilot"
    summary = runner.PipelineRunSummary("run-1", "dev", dry_run=False)
    layers_with_new_data = set()

    class EmptyFrame:
        def is_empty(self):
            return True

    class FakeResult:
        good = EmptyFrame()
        bad = EmptyFrame()

    class FakeProcessor:
        instances = []

        def __init__(self, contract, engine, pipeline_run_id, run_log_mode=None):
            self.contract = contract
            self.engine_name = engine
            self._run_log_mode = run_log_mode
            self.last_report = {
                "counts": {"source": 0, "good": 0, "quarantined": 0},
                "row_rule_failures": [],
            }
            self.materialized = []
            FakeProcessor.instances.append(self)

        def run_source(self, **kwargs):
            self.run_source_kwargs = kwargs
            return FakeResult()

        def materialize(self, good, bad):
            self.materialized.append((good, bad))

    monkeypatch.setattr(runner, "DataProcessor", FakeProcessor)
    contract = types.SimpleNamespace(
        entity="orders",
        contract_dict={
            "version": "1.0",
            "info": {"title": "Orders", "table_name": "orders"},
            "lineage": {},
        },
    )

    pipeline._process_single_contract(
        contract,
        "bronze",
        summary,
        False,
        None,
        None,
        None,
        None,
        None,
        None,
        layers_with_new_data,
    )

    assert summary.results[-1]["status"] == "no_new_rows"
    assert "bronze" in layers_with_new_data
    assert FakeProcessor.instances[-1].contract["lineage"]["created_by_override"] == "copilot"
    assert FakeProcessor.instances[-1]._ownership == {"team": "data"}
    assert FakeProcessor.instances[-1]._notifications == ["slack"]
    assert reports[-1][0]["status"] == "succeeded"

    failures = []
    monkeypatch.setattr(runner.logger, "error", failures.append)
    monkeypatch.setattr(
        runner.os, "getenv", lambda key: {"AZURE_CLIENT_ID": "client-1", "AZURE_TENANT_ID": "tenant-1"}.get(key)
    )

    class FailingProcessor:
        def __init__(self, contract, engine, pipeline_run_id, run_log_mode=None):
            raise RuntimeError("403 forbidden")

    monkeypatch.setattr(runner, "DataProcessor", FailingProcessor)
    failing_contract = types.SimpleNamespace(
        entity="customers",
        contract_dict={
            "version": "1.0",
            "info": {"title": "Customers", "table_name": "customers", "version": "1.0"},
            "dataset": "customers",
        },
    )

    with pytest.raises(RuntimeError, match="403 forbidden"):
        pipeline._process_single_contract(
            failing_contract,
            "silver",
            summary,
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            set(),
        )

    assert summary.results[-1]["status"] == "failed"
    assert any("identity: SP client_id=client-1, tenant_id=tenant-1" in message for message in failures)
    assert reports[-1][0]["status"] == "failed"
    assert reports[-1][0]["dataset"] == "customers"


def test_run_orchestrates_ddl_skips_checkpoint_and_circuit_breaker(monkeypatch):
    bronze = types.SimpleNamespace(
        entity="orders", layer="bronze", depends_on=[], contract_dict={"info": {"table_name": "orders"}}
    )
    silver = types.SimpleNamespace(
        entity="customers", layer="silver", depends_on=[], contract_dict={"info": {"table_name": "customers"}}
    )
    registry = types.SimpleNamespace(
        domain="commerce",
        system="erp",
        storage_mode="uc",
        storage=None,
        get_active_contracts=lambda: [bronze, silver],
    )
    pipeline = runner.LakehousePipeline(registry, engine="polars")

    resolved = []
    resets = []
    gdpr_calls = []
    hipaa_calls = []
    test_data_calls = []
    ddl_calls = []
    monkeypatch.setattr(
        pipeline,
        "_resolve_uc_paths",
        lambda contract_dict: resolved.append(contract_dict.get("info", {}).get("table_name")) or contract_dict,
    )
    monkeypatch.setattr(
        pipeline,
        "_execute_resets",
        lambda active, reset_layers, reload_layers, dry_run: resets.append(
            (len(active), reset_layers, reload_layers, dry_run)
        ),
    )
    monkeypatch.setattr(
        pipeline, "_execute_gdpr_pass", lambda *args, **kwargs: gdpr_calls.append(kwargs.get("partition_filter"))
    )
    monkeypatch.setattr(
        pipeline, "_execute_hipaa_pass", lambda *args, **kwargs: hipaa_calls.append(kwargs.get("partition_filter"))
    )
    monkeypatch.setattr(
        pipeline,
        "_generate_test_data",
        lambda contracts, **kwargs: test_data_calls.append((len(contracts), kwargs["rows"])),
    )
    monkeypatch.setattr(
        pipeline,
        "generate_ddl_only",
        lambda contracts, dry_run: ddl_calls.append(([c.entity for c in contracts], dry_run)) or "ddl-summary",
    )

    ddl_result = pipeline.run(
        target_layers="all",
        reset_layers="bronze",
        reload_layers="silver",
        ddl_only=True,
        environment="dev",
        forget_column="customer_id",
        forget_values=["1"],
        forget_partition_column="country_code",
        forget_partition_value="GB",
        forget_patient_column="patient_id",
        forget_patient_ids=["p1"],
        forget_patient_partition_column="region",
        forget_patient_partition_value="eu",
        generate_test_data=True,
        test_data_rows=7,
    )
    assert ddl_result == "ddl-summary"
    assert resolved == ["orders", "customers"]
    assert resets == [(2, {"bronze"}, {"silver"}, False)]
    assert gdpr_calls == [{"column": "country_code", "value": "GB"}]
    assert hipaa_calls == [{"column": "region", "value": "eu"}]
    assert test_data_calls == []
    assert ddl_calls == [(["orders", "customers"], False)]

    test_data_result = pipeline.run(target_layers="bronze", generate_test_data=True, test_data_rows=7)
    assert isinstance(test_data_result, runner.PipelineRunSummary)
    assert test_data_calls == [(1, 7)]

    contract_a = types.SimpleNamespace(
        entity="done", layer="bronze", depends_on=[], contract_dict={"info": {"table_name": "done"}}
    )
    contract_b = types.SimpleNamespace(
        entity="failing", layer="bronze", depends_on=[], contract_dict={"info": {"table_name": "failing"}}
    )
    contract_c = types.SimpleNamespace(
        entity="after_failure", layer="bronze", depends_on=[], contract_dict={"info": {"table_name": "after_failure"}}
    )
    circuit_registry = types.SimpleNamespace(
        domain="commerce",
        system="erp",
        storage_mode="uc",
        storage=None,
        get_active_contracts=lambda: [contract_a, contract_b, contract_c],
    )
    circuit_pipeline = runner.LakehousePipeline(circuit_registry, engine="polars")
    monkeypatch.setattr(circuit_pipeline, "_resolve_uc_paths", lambda contract_dict: contract_dict)
    monkeypatch.setattr(circuit_pipeline, "_load_checkpoint", lambda run_id: {"bronze:done"})
    monkeypatch.setattr(circuit_pipeline, "_topological_sort", lambda contracts: contracts)

    processed = []

    def fake_process(contract, *_args, **_kwargs):
        processed.append(contract.entity)
        if contract.entity == "failing":
            raise RuntimeError("kaput")

    monkeypatch.setattr(circuit_pipeline, "_process_contract_with_retry", fake_process)

    summary = circuit_pipeline.run(target_layers="bronze", resume_from_run="prior-run", max_consecutive_failures=1)
    assert processed == ["failing"]
    assert [result["status"] for result in summary.results] == [
        "skipped_checkpoint",
        "failed",
        "skipped_circuit_breaker",
    ]

    upstream_registry = types.SimpleNamespace(
        domain="commerce",
        system="erp",
        storage_mode="uc",
        storage=None,
        get_active_contracts=lambda: [bronze, silver],
    )
    upstream_pipeline = runner.LakehousePipeline(upstream_registry, engine="polars")
    monkeypatch.setattr(upstream_pipeline, "_resolve_uc_paths", lambda contract_dict: contract_dict)
    monkeypatch.setattr(upstream_pipeline, "_topological_sort", lambda contracts: contracts)
    monkeypatch.setattr(upstream_pipeline, "_process_contract_with_retry", lambda *args, **kwargs: None)

    upstream_summary = upstream_pipeline.run(target_layers="bronze,silver")
    assert upstream_summary.results[0]["status"] == "skipped_no_upstream"


def test_visualize_dag_includes_filters_external_and_downstream():
    contract = types.SimpleNamespace(
        entity="orders",
        layer="bronze",
        depends_on=[],
        contract_dict={
            "version": "1.1",
            "info": {"title": "Orders", "target_layer": "bronze"},
            "model": {"fields": [{"name": "id"}, {"name": "email", "pii": True}]},
            "pipeline": {"frequency": "hourly"},
            "downstream": [{"name": "Executive Dashboard", "type": "dashboard", "platform": "Power BI"}],
        },
    )
    silver = types.SimpleNamespace(
        entity="curated_orders",
        layer="silver",
        depends_on=["orders"],
        contract_dict={"info": {"title": "Curated Orders", "target_layer": "silver"}, "model": {"fields": []}},
    )
    registry = types.SimpleNamespace(
        domain="commerce",
        system="erp",
        external_sources=[
            {"name": "CRM", "source_domain": "Sales", "catalog_path": "crm.api", "consumed_by": ["orders"]}
        ],
        get_active_contracts=lambda: [contract, silver],
    )
    pipeline = runner.LakehousePipeline(registry, engine="polars")

    html = pipeline.visualize_dag(title="Revenue Flow", entity_filter="orders", layer_filter="bronze")
    assert "Revenue Flow" in html
    assert "Filter: Layer: BRONZE / Entity: orders" in html
    assert "Executive Dashboard" in html
    assert "CRM" in html
    assert "🔒 1 PII" in html
    assert "DOWNSTREAM" in html
