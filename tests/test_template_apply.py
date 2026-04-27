from __future__ import annotations

import builtins
import types

from lakelogic.tools import template_apply as ta


def test_merge_list_modes_and_deduplication():
    assert ta._merge_list([], [1, 2], "append") == [1, 2]
    assert ta._merge_list([1, 2], [], "append") == [1, 2]
    assert ta._merge_list([1, 2], [2, 3], "append") == [1, 2, 3]
    assert ta._merge_list([1, 2], [2, 3], "prepend") == [2, 3, 1]
    assert ta._merge_list([1, 2], [2, 3], "replace") == [2, 3]


def test_quote_boolish_and_dump_yaml_round_trip(tmp_path):
    payload = {True: "true", "false": ["true", {False: "false"}]}
    normalized = ta._quote_boolish(payload)
    output_path = tmp_path / "quoted.yaml"

    ta.dump_yaml(normalized, output_path)
    dumped = output_path.read_text(encoding="utf-8")
    loaded = ta.load_yaml(output_path)

    assert "'true'" in dumped
    assert "'false'" in dumped
    assert loaded["true"] == "true"
    assert loaded["false"][0] == "true"


def test_fingerprint_item_falls_back_to_repr(monkeypatch):
    monkeypatch.setattr(ta.json, "dumps", lambda *args, **kwargs: (_ for _ in ()).throw(TypeError("json fail")))
    monkeypatch.setattr(ta.yaml, "safe_dump", lambda *args, **kwargs: (_ for _ in ()).throw(TypeError("yaml fail")))

    class Unserializable:
        def __repr__(self):
            return "UNSERIALIZABLE"

    assert ta._fingerprint_item(Unserializable()) == "UNSERIALIZABLE"


def test_fingerprint_item_uses_yaml_fallback(monkeypatch):
    monkeypatch.setattr(ta.json, "dumps", lambda *args, **kwargs: (_ for _ in ()).throw(TypeError("json fail")))
    monkeypatch.setattr(ta.yaml, "safe_dump", lambda *args, **kwargs: "yaml-value")

    assert ta._fingerprint_item(object()) == "yaml-value"


def test_deep_merge_merges_dicts_and_configured_lists():
    base = {
        "quality": {"row_rules": [{"name": "keep"}], "dataset_rules": [{"unique": "id"}]},
        "other": ["base"],
        "nullable": "value",
    }
    overlay = {
        "quality": {"row_rules": [{"name": "add"}]},
        "other": ["overlay"],
        "nullable": None,
        "new": True,
    }

    merged = ta._deep_merge(
        base,
        overlay,
        path="",
        list_merge_keys={"quality.row_rules", "quality.dataset_rules"},
        list_mode="append",
    )

    assert merged["quality"]["row_rules"] == [{"name": "keep"}, {"name": "add"}]
    assert merged["quality"]["dataset_rules"] == [{"unique": "id"}]
    assert merged["other"] == ["overlay"]
    assert merged["nullable"] == "value"
    assert merged["new"] is True


def test_normalize_entity_name_and_find_column():
    assert ta._normalize_entity_name("bronze_orders") == "orders"
    assert ta._normalize_entity_name("silver_customer") == "customer"
    assert ta._normalize_entity_name("plain_value") == "plain_value"
    assert ta._find_column(["Order_ID", "created_at"], "order_id") == "Order_ID"
    assert ta._find_column(["Order_ID"], "missing") is None


def test_infer_columns_from_csv_sources(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,name\n1,a\n", encoding="utf-8")

    contract = {"source": {"path": str(csv_path)}}
    assert ta._infer_columns_from_source(contract, tmp_path / "contract.yaml") == ["id", "name"]

    directory = tmp_path / "landing"
    directory.mkdir()
    (directory / "batch.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    contract = {"source": {"path": str(directory), "pattern": "*.csv"}}
    assert ta._infer_columns_from_source(contract, tmp_path / "contract.yaml") == ["a", "b"]

    assert ta._infer_columns_from_source({"source": {"path": "s3://bucket/file.csv"}}, tmp_path / "contract.yaml") == []
    assert (
        ta._infer_columns_from_source({"source": {"path": str(tmp_path / "missing.csv")}}, tmp_path / "contract.yaml")
        == []
    )


def test_infer_columns_from_parquet_handles_missing_or_failing_pyarrow(tmp_path, monkeypatch):
    parquet_path = tmp_path / "data.parquet"
    parquet_path.write_text("placeholder", encoding="utf-8")
    contract = {"source": {"path": str(parquet_path)}}
    original_import = builtins.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "pyarrow.parquet":
            raise ImportError("blocked")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    assert ta._infer_columns_from_source(contract, tmp_path / "contract.yaml") == []

    monkeypatch.setattr(builtins, "__import__", original_import)
    fake_parquet = types.SimpleNamespace(read_schema=lambda path: (_ for _ in ()).throw(ValueError("bad parquet")))
    monkeypatch.setitem(__import__("sys").modules, "pyarrow.parquet", fake_parquet)
    assert ta._infer_columns_from_source(contract, tmp_path / "contract.yaml") == []


def test_infer_columns_from_empty_directory_returns_no_columns(tmp_path):
    contract = {"source": {"path": str(tmp_path / "empty")}}
    (tmp_path / "empty").mkdir()

    assert ta._infer_columns_from_source(contract, tmp_path / "contract.yaml") == []


def test_collect_columns_prefers_model_fields_and_can_infer(tmp_path):
    contract = {"model": {"fields": [{"name": "id"}, {"name": "name"}]}}
    assert ta._collect_columns(contract, tmp_path / "contract.yaml", infer_columns=True) == ["id", "name"]

    csv_path = tmp_path / "source.csv"
    csv_path.write_text("x,y\n1,2\n", encoding="utf-8")
    inferred_contract = {"source": {"path": str(csv_path)}}
    assert ta._collect_columns(inferred_contract, tmp_path / "contract.yaml", infer_columns=True) == ["x", "y"]
    assert ta._collect_columns(inferred_contract, tmp_path / "contract.yaml", infer_columns=False) == []


def test_soft_delete_detection_and_application():
    assert ta._soft_delete_already_applied({"metadata": {"soft_delete_applied": True}}) is True
    assert ta._soft_delete_already_applied({"transformations": [{"coalesce": {"output": "is_deleted"}}]}) is True
    assert ta._soft_delete_already_applied({"transformations": [{"derive": {"field": "__deleted_at_flag"}}]}) is True
    assert ta._soft_delete_already_applied({"transformations": []}) is False

    contract = {
        "model": {"fields": [{"name": "operation", "type": "string"}]},
        "transformations": [{"existing": True}],
    }

    ta._apply_soft_delete(
        contract,
        columns=["operation", "deleted_at", "is_deleted"],
        operation_col="operation",
        deleted_at_col="deleted_at",
        flag_col="is_deleted",
        output_col="is_deleted",
        exclude_hard_deletes=True,
        operation_values=["delete", "upsert"],
    )

    assert contract["metadata"]["soft_delete_applied"] is True
    assert contract["quality"]["row_rules"][-1] == {
        "accepted_values": {"field": "operation", "values": ["delete", "upsert"]}
    }
    assert contract["model"]["fields"][-1] == {"name": "is_deleted", "type": "boolean"}
    assert contract["transformations"][0]["lower"]["fields"] == ["operation"]
    assert contract["transformations"][-1] == {"existing": True}


def test_apply_soft_delete_handles_early_return_and_non_list_sections():
    already_applied = {"metadata": {"soft_delete_applied": True}}
    ta._apply_soft_delete(
        already_applied,
        columns=["operation"],
        operation_col="operation",
        deleted_at_col="deleted_at",
        flag_col="is_deleted",
        output_col="is_deleted",
        exclude_hard_deletes=False,
        operation_values=["delete"],
    )
    assert already_applied == {"metadata": {"soft_delete_applied": True}}

    empty_contract = {}
    ta._apply_soft_delete(
        empty_contract,
        columns=[],
        operation_col="operation",
        deleted_at_col="deleted_at",
        flag_col="is_deleted",
        output_col="is_deleted",
        exclude_hard_deletes=False,
        operation_values=["delete"],
    )
    assert empty_contract == {}

    contract = {"transformations": [], "quality": "invalid", "metadata": {}}
    ta._apply_soft_delete(
        contract,
        columns=["deleted_at", "is_deleted"],
        operation_col="operation",
        deleted_at_col="deleted_at",
        flag_col="is_deleted",
        output_col="is_deleted",
        exclude_hard_deletes=False,
        operation_values=["delete"],
    )
    assert contract["metadata"]["soft_delete_applied"] is True
    assert contract["quality"]["row_rules"] == []
    assert any(step.get("sql") for step in contract["transformations"] if isinstance(step, dict))


def test_apply_soft_delete_preserves_existing_output_field(tmp_path):
    contract = {
        "model": {"fields": [{"name": "is_deleted", "type": "boolean"}]},
        "transformations": [],
    }

    ta._apply_soft_delete(
        contract,
        columns=["is_deleted"],
        operation_col="operation",
        deleted_at_col="deleted_at",
        flag_col="is_deleted",
        output_col="is_deleted",
        exclude_hard_deletes=False,
        operation_values=["delete"],
    )

    assert contract["model"]["fields"] == [{"name": "is_deleted", "type": "boolean"}]


def test_collect_registry_and_contract_paths(tmp_path):
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    bronze_path = contracts_dir / "bronze.yaml"
    silver_path = contracts_dir / "silver.yaml"
    bronze_path.write_text("dataset: bronze_orders\n", encoding="utf-8")
    silver_path.write_text("dataset: silver_orders\n", encoding="utf-8")

    registry_path = tmp_path / "registry.yaml"
    registry_path.write_text(
        "entries:\n"
        "  - enabled: true\n"
        "    contracts:\n"
        f"      bronze: contracts/{bronze_path.name}\n"
        "  - enabled: false\n"
        f"    contract_path: contracts/{silver_path.name}\n",
        encoding="utf-8",
    )

    collected = ta._collect_registry_paths(registry_path, "bronze")
    assert collected == [bronze_path.resolve()]

    combined = ta.collect_contract_paths(
        registry=registry_path,
        contracts_dir=contracts_dir,
        contracts=[bronze_path, silver_path],
        stage="bronze",
    )
    assert bronze_path.resolve() in combined
    assert silver_path.resolve() in combined
    assert len(combined) == 2


def test_collect_registry_paths_without_stage_includes_all_contract_entries(tmp_path):
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    bronze_path = contracts_dir / "bronze.yaml"
    silver_path = contracts_dir / "silver.yaml"
    bronze_path.write_text("dataset: bronze_orders\n", encoding="utf-8")
    silver_path.write_text("dataset: silver_orders\n", encoding="utf-8")
    registry_path = tmp_path / "registry.yaml"
    registry_path.write_text(
        "entries:\n"
        "  - enabled: true\n"
        "    contracts:\n"
        f"      bronze: contracts/{bronze_path.name}\n"
        f"      silver: contracts/{silver_path.name}\n",
        encoding="utf-8",
    )

    collected = ta._collect_registry_paths(registry_path, None)
    assert collected == [bronze_path.resolve(), silver_path.resolve()]


def test_apply_contract_template_dry_run_and_write(tmp_path):
    contract_path = tmp_path / "contracts" / "bronze_orders.yaml"
    contract_path.parent.mkdir(parents=True)
    contract_path.write_text(
        "dataset: bronze_orders\n"
        "model:\n"
        "  fields:\n"
        "    - name: id\n"
        "      type: string\n"
        "transformations:\n"
        "  - deduplicate:\n"
        "      by: [id]\n",
        encoding="utf-8",
    )
    template = {
        "quality": {"row_rules": [{"name": "not_empty", "sql": "id <> ''"}]},
        "transformations": [{"sql": "SELECT * FROM source"}],
    }
    output_dir = tmp_path / "output"

    dry_run = ta.apply_contract_template(
        template,
        contracts=[contract_path],
        output_dir=output_dir,
        list_merge_keys=["transformations", "quality.row_rules"],
        dry_run=True,
    )
    assert dry_run[0].written is False
    assert not dry_run[0].output_path.exists()

    written = ta.apply_contract_template(
        template,
        contracts=[contract_path],
        output_dir=output_dir,
        list_merge_keys=["transformations", "quality.row_rules"],
        infer_columns=False,
    )
    assert written[0].written is True
    written_data = ta.load_yaml(written[0].output_path)
    assert written_data["quality"]["row_rules"] == [{"name": "not_empty", "sql": "id <> ''"}]
    assert written_data["transformations"][0]["sql"] == "SELECT * FROM source"
    assert written_data["transformations"][1]["deduplicate"]["by"] == ["id"]


def test_apply_contract_template_accepts_template_path_and_string_options(tmp_path):
    template_path = tmp_path / "template.yaml"
    template_path.write_text(
        "quality:\n  dataset_rules:\n    - unique: id\n",
        encoding="utf-8",
    )
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text("dataset: bronze_orders\n", encoding="utf-8")

    results = ta.apply_contract_template(
        template_path,
        contracts=[contract_path],
        list_merge_keys="quality.dataset_rules,transformations",
        soft_delete_operation_values="delete,merge",
    )

    assert results[0].written is True
    loaded = ta.load_yaml(contract_path)
    assert loaded["quality"]["dataset_rules"] == [{"unique": "id"}]


def test_apply_contract_template_accepts_iterable_soft_delete_values(tmp_path):
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(
        "dataset: bronze_orders\nmodel:\n  fields:\n    - name: operation\n      type: string\n",
        encoding="utf-8",
    )

    results = ta.apply_contract_template(
        {},
        contracts=[contract_path],
        soft_delete=True,
        soft_delete_operation_values=("delete", "merge"),
    )

    assert results[0].written is True


def test_apply_contract_template_filters_entities_and_applies_soft_delete(tmp_path):
    contract_path = tmp_path / "bronze_orders.yaml"
    contract_path.write_text(
        "dataset: bronze_orders\n"
        "source:\n"
        "  path: source.csv\n"
        "model:\n"
        "  fields:\n"
        "    - name: operation\n"
        "      type: string\n",
        encoding="utf-8",
    )
    (tmp_path / "source.csv").write_text("operation\nDELETE\n", encoding="utf-8")

    results = ta.apply_contract_template(
        {},
        contracts=[contract_path],
        entity_include="orders",
        soft_delete=True,
        infer_columns=True,
        soft_delete_keep_hard_deletes=False,
        dry_run=False,
    )
    assert len(results) == 1

    written_data = ta.load_yaml(contract_path)
    assert written_data["metadata"]["soft_delete_applied"] is True
    assert any("filter" in step for step in written_data["transformations"])

    skipped = ta.apply_contract_template({}, contracts=[contract_path], entity_exclude="orders", dry_run=True)
    assert skipped == []


def test_apply_contract_template_output_paths_with_registry_and_contracts_dir(tmp_path):
    registry_root = tmp_path / "registry_root"
    contracts_dir = registry_root / "contracts"
    contracts_dir.mkdir(parents=True)
    contract_path = contracts_dir / "bronze_orders.yaml"
    contract_path.write_text("dataset: bronze_orders\n", encoding="utf-8")
    registry_path = registry_root / "registry.yaml"
    registry_path.write_text(
        "entries:\n  - enabled: true\n    contract_path: contracts/bronze_orders.yaml\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "generated"

    registry_results = ta.apply_contract_template({}, registry=registry_path, output_dir=output_dir, dry_run=True)
    assert registry_results[0].output_path == output_dir / "contracts" / "bronze_orders.yaml"

    outside_contract = tmp_path / "outside.yaml"
    outside_contract.write_text("dataset: bronze_orders\n", encoding="utf-8")
    contract_results = ta.apply_contract_template(
        {}, contracts_dir=contracts_dir, contracts=[outside_contract], output_dir=output_dir, dry_run=True
    )
    assert any(result.output_path == output_dir / "outside.yaml" for result in contract_results)
