from __future__ import annotations

from pathlib import Path

import pytest

from lakelogic.adapters import dbt


def _write_yaml(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_model_contract_conversion_and_yaml_export(tmp_path):
    schema_path = _write_yaml(
        tmp_path / "schema.yml",
        """
models:
  - name: customers
    description: Customer records
    tests:
      - expression_is_true:
          expression: count(*) > 0
    columns:
      - name: id
        data_type: bigint
        description: Customer id
        tests: [not_null, unique]
      - name: status
        data_type: varchar
        tests:
          - accepted_values:
              values: [active, inactive, 1]
          - dbt_utils.expression_is_true:
              expression: status <> 'deleted'
      - name: email
        meta:
          pii: true
      - name: parent_id
        tests:
          - relationships:
              to: ref('parents')
""",
    )

    contract = dbt.load_contract_from_dbt(schema_path)
    adapter = dbt.DbtAdapter(schema_path)
    written = tmp_path / "out" / "customers.yaml"
    text = adapter.to_yaml(output=written)

    assert contract.dataset == "customers"
    assert contract.info.target_layer == "silver"
    assert contract.info.description == "Customer records"
    assert contract.primary_key == ["id"]
    assert [field.name for field in contract.model.fields] == ["id", "status", "email", "parent_id"]
    assert contract.model.fields[0].type == "integer"
    assert contract.model.fields[0].required is True
    assert contract.model.fields[2].pii is True
    row_sql = [rule.sql for rule in contract.quality.row_rules]
    dataset_sql = [rule.sql for rule in contract.quality.dataset_rules]
    assert "status IN ('active', 'inactive', 1)" in row_sql
    assert "parent_id IS NOT NULL" in row_sql
    assert "status <> 'deleted'" in row_sql
    assert "count(*) > 0" in dataset_sql
    assert "SELECT COUNT(*) - COUNT(DISTINCT id) FROM customers" in dataset_sql
    assert written.exists() is True
    assert "dataset: customers" in text


def test_source_contract_conversion_lists_and_export_all(tmp_path):
    schema_path = _write_yaml(
        tmp_path / "sources.yml",
        """
sources:
  - name: raw
    description: Raw landing source
    tables:
      - name: orders
        columns:
          - name: order_id
            data_type: int
            tests: [unique]
          - name: order_date
            data_type: timestamp_ntz
  - name: aux
    tables:
      - name: events
models:
  - name: silver_orders
    columns:
      - name: id
""",
    )

    adapter = dbt.DbtAdapter(schema_path)
    source_contract = dbt.load_contract_from_dbt(schema_path, source_name="raw", source_table="orders")
    output_paths = adapter.export_all(tmp_path / "contracts")

    assert adapter.list_models() == ["silver_orders"]
    assert adapter.list_sources() == [{"source": "raw", "table": "orders"}, {"source": "aux", "table": "events"}]
    assert source_contract.dataset == "orders"
    assert source_contract.info.target_layer == "bronze"
    assert source_contract.info.description == "Raw landing source"
    assert source_contract.primary_key == ["order_id"]
    assert source_contract.model.fields[1].type == "timestamp"
    assert output_paths == [tmp_path / "contracts" / "silver_orders.yaml"]
    assert output_paths[0].exists() is True


def test_source_contract_auto_selects_single_source_and_table(tmp_path):
    schema_path = _write_yaml(
        tmp_path / "single_source.yml",
        """
sources:
  - name: raw
    description: One source only
    tables:
      - name: orders
        columns:
          - name: order_id
            tests:
              - unique: {}
          - name: customer_id
            tests:
              - not_null: {}
""",
    )

    contract = dbt.DbtAdapter(schema_path).source_to_contract()
    assert contract.dataset == "orders"
    assert contract.primary_key == ["order_id"]
    assert contract.model.fields[1].required is True


def test_dbt_adapter_error_paths(tmp_path):
    with pytest.raises(FileNotFoundError):
        dbt.DbtAdapter(tmp_path / "missing.yml")

    empty_models = _write_yaml(tmp_path / "empty_models.yml", "sources: []\n")
    adapter = dbt.DbtAdapter(empty_models)
    with pytest.raises(ValueError, match="No 'models:' block"):
        adapter.model_to_contract()

    multi_model = _write_yaml(
        tmp_path / "multi_model.yml",
        "models:\n  - name: a\n  - name: b\n",
    )
    adapter = dbt.DbtAdapter(multi_model)
    with pytest.raises(ValueError, match="Multiple models found"):
        adapter.model_to_contract()
    with pytest.raises(ValueError, match="Model 'missing' not found"):
        adapter.model_to_contract("missing")

    no_sources = _write_yaml(tmp_path / "no_sources.yml", "models: []\n")
    adapter = dbt.DbtAdapter(no_sources)
    with pytest.raises(ValueError, match="No 'sources:' block"):
        adapter.source_to_contract()

    multi_sources = _write_yaml(
        tmp_path / "multi_sources.yml",
        """
sources:
  - name: raw
    tables:
      - name: a
      - name: b
  - name: aux
    tables:
      - name: c
""",
    )
    adapter = dbt.DbtAdapter(multi_sources)
    with pytest.raises(ValueError, match="Multiple sources found"):
        adapter.source_to_contract()
    with pytest.raises(ValueError, match="Source 'missing' not found"):
        adapter.source_to_contract("missing")
    with pytest.raises(ValueError, match="Multiple tables found"):
        adapter.source_to_contract("raw")
    with pytest.raises(ValueError, match="Table 'missing' not in source 'raw'"):
        adapter.source_to_contract("raw", "missing")


def test_dbt_private_helpers():
    assert dbt._is_pii("email", {}, []) is True
    assert dbt._is_pii("customer_id", {"is_pii": True}, []) is True
    assert dbt._is_pii("customer_code", {}, ["PII"]) is True
    assert dbt._is_pii("customer_code", {}, []) is False

    assert dbt._normalise_type("numeric(18,2)") == "double"
    assert dbt._normalise_type("timestamp_tz") == "timestamp"
    assert dbt._normalise_type("custom_type") == "string"

    rules, is_required, is_unique = dbt._parse_column_test("id", "not_null", "orders")
    assert rules == []
    assert is_required is True
    assert is_unique is False

    rules, is_required, is_unique = dbt._parse_column_test("id", "unique", "orders")
    assert rules == []
    assert is_required is False
    assert is_unique is True

    assert dbt._parse_column_test("id", 123, "orders") == ([], False, False)

    accepted_rules, _, _ = dbt._parse_column_test(
        "status",
        {"accepted_values": {"values": ["new", -1]}},
        "orders",
    )
    assert accepted_rules[0].sql == "status IN ('new', -1)"

    model_rules, _, _ = dbt._parse_column_test("__model__", {"accepted_values": {"values": ["x"]}}, "orders")
    assert model_rules == []

    relationship_rules, _, _ = dbt._parse_column_test("customer_id", {"dbt_utils.relationships": {}}, "orders")
    assert relationship_rules[0].sql == "customer_id IS NOT NULL"

    dict_not_null, is_required, is_unique = dbt._parse_column_test("id", {"not_null": {}}, "orders")
    assert dict_not_null == []
    assert is_required is True
    assert is_unique is False

    dict_unique, is_required, is_unique = dbt._parse_column_test("id", {"unique": {}}, "orders")
    assert dict_unique == []
    assert is_required is False
    assert is_unique is True

    expr_rules, _, _ = dbt._parse_column_test(
        "amount",
        {"expression_is_true": {"expression": "amount > 0"}},
        "orders",
    )
    assert expr_rules[0].sql == "amount > 0"
    assert dbt._parse_column_test("amount", {"dbt_utils.not_null_proportion": {"at_least": 0.9}}, "orders") == (
        [],
        False,
        False,
    )