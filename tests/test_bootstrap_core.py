from __future__ import annotations

import builtins
import sys
import types
from pathlib import Path

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core import bootstrap as bs


def test_schema_parsing_variants_and_schema_df():
    dict_fields = bs._parse_schema_to_fields({"id": "INT", "created_at": "TIMESTAMP"})
    tuple_fields = bs._parse_schema_to_fields([("name", "VARCHAR(100)"), ("amount", "DOUBLE")])
    ddl_fields = bs._parse_schema_to_fields("id STRING, amount DECIMAL(10,2), created_at TIMESTAMP")

    assert dict_fields == [
        {"name": "id", "type": "integer"},
        {"name": "created_at", "type": "timestamp"},
    ]
    assert tuple_fields == [{"name": "name", "type": "string"}, {"name": "amount", "type": "double"}]
    assert ddl_fields[0]["type"] == "string"
    assert ddl_fields[1]["type"] == "double"

    df = bs._schema_to_polars_df({"id": "int", "name": "string"})
    assert df.columns == ["id", "name"]
    assert df.is_empty()
    assert bs._polars_dtype_to_contract(pl.Int64()) == "integer"

    with pytest.raises(ValueError, match="Cannot parse schema"):
        bs._parse_schema_to_fields(123)


def test_format_yaml_fieldlist_and_contract_draft_helpers(tmp_path, monkeypatch):
    formatted = bs._format_contract_yaml(
        {
            "version": "1.0.0",
            "model": {"fields": [{"name": "id", "type": "integer"}, {"name": "status", "type": "string"}]},
            "quality": {"row_rules": [{"name": "id_not_null", "sql": "id IS NOT NULL"}]},
        }
    )
    assert "\n\nmodel:" in formatted
    assert "\n\nquality:" in formatted
    assert "- name: id" in formatted
    assert "- name: status" in formatted

    draft = bs.ContractDraft({"info": {"title": "Orders"}, "model": {"fields": [{"name": "id", "type": "integer"}]}})
    draft.fields.append({"name": "id", "required": True})
    draft.fields.append({"name": "status", "type": "string"})
    assert draft.fields[0] == {"name": "id", "type": "integer", "required": True}
    assert len(draft.fields) == 2
    assert "ContractDraft(title='Orders', fields=2" in repr(draft)

    out_path = tmp_path / "contracts" / "orders.yaml"
    assert draft.save(out_path) == out_path
    with pytest.raises(FileExistsError):
        draft.save(out_path, overwrite=False)

    shown = []
    monkeypatch.setattr("builtins.print", lambda text: shown.append(text))
    draft.show()
    assert shown and "model:" in shown[0]

    class FakeGenerator:
        def __init__(self, path, seed=None, use_faker=True):
            self.path = path
            self.seed = seed
            self.use_faker = use_faker

    monkeypatch.setattr("lakelogic.core.generator.DataGenerator", FakeGenerator)
    generator = draft.to_generator(seed=7, use_faker=False)
    assert generator.seed == 7
    assert generator.use_faker is False
    assert Path(generator.path).exists()


def test_reference_detection_spark_lookup_and_connection_dispatch(monkeypatch):
    inferrer = bs.ContractInferrer("dummy")
    assert inferrer._is_table_reference("catalog.schema.table") is True
    assert inferrer._is_table_reference("orders") is True
    assert inferrer._is_table_reference("data/orders.csv") is False
    assert inferrer._is_schema_input({"id": "int"}) is True
    assert inferrer._is_schema_input([("id", "int")]) is True
    assert inferrer._is_schema_input("id STRING, amount INT") is True
    assert inferrer._is_schema_input("catalog.schema.table") is False

    fake_spark = object()
    monkeypatch.setattr(builtins, "spark", fake_spark, raising=False)
    assert bs.ContractInferrer._find_spark_session() is fake_spark
    monkeypatch.delattr(builtins, "spark", raising=False)

    fake_active = object()
    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.SparkSession = types.SimpleNamespace(getActiveSession=lambda: fake_active)
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    assert bs.ContractInferrer._find_spark_session() is fake_active

    SparkSessionConn = type(
        "SparkSessionConn",
        (),
        {"table": lambda self, ref: types.SimpleNamespace(limit=lambda n: types.SimpleNamespace(toPandas=lambda: __import__("pandas").DataFrame({"id": [1]})))},
    )
    DuckDBPyConnection = type(
        "DuckDBPyConnection",
        (),
        {"sql": lambda self, query: types.SimpleNamespace(pl=lambda: pl.DataFrame({"id": [1]}))},
    )
    spark_conn = SparkSessionConn()
    duck_conn = DuckDBPyConnection()
    engine_conn = type("Engine", (), {})()
    monkeypatch.setattr("pandas.read_sql", lambda query, conn: __import__("pandas").DataFrame({"id": [1]}))
    assert inferrer._read_from_connection(spark_conn, "orders", 10, pl).to_dicts() == [{"id": 1}]
    assert inferrer._read_from_connection(duck_conn, "orders", 10, pl).to_dicts() == [{"id": 1}]
    assert inferrer._read_from_connection(engine_conn, "orders", 10, pl).to_dicts() == [{"id": 1}]


def test_load_flatten_and_xml_paths(tmp_path):
    csv_path = tmp_path / "orders.csv"
    csv_path.write_text("id,status\n1,active\n2,inactive\n", encoding="utf-8")
    json_path = tmp_path / "single.json"
    json_path.write_text('{"id": 1, "status": "active"}', encoding="utf-8")
    xml_path = tmp_path / "orders.xml"
    xml_path.write_text(
        "<rows><row id='1'><status>active</status><meta><city>London</city></meta></row><row id='2'><status>inactive</status></row></rows>",
        encoding="utf-8",
    )

    inf_csv = bs.ContractInferrer(str(csv_path), sample_rows=1)
    df_csv, path_csv = inf_csv._load(pl)
    assert path_csv == csv_path
    assert df_csv.shape == (1, 2)

    inf_json = bs.ContractInferrer(str(json_path))
    df_json, path_json = inf_json._load(pl)
    assert path_json == json_path
    assert df_json.to_dicts() == [{"id": 1, "status": "active"}]

    xml_df = bs.ContractInferrer._load_xml(xml_path, pl)
    assert xml_df.columns == ["@id", "status", "meta_city"]
    assert xml_df.to_dicts()[0]["meta_city"] == "London"

    nested_df = pl.DataFrame({"payload": ['{"city":"London","coords":{"lat":1}}'], "keep": [1]})
    flattened = bs.ContractInferrer(nested_df)._flatten_df(nested_df)
    assert "payload_city" in flattened.columns
    assert "payload_coords_lat" in flattened.columns

    preserved = bs.ContractInferrer(nested_df, preserve_nested=True)._flatten_df(nested_df)
    assert preserved.columns == ["payload", "keep"]

    with pytest.raises(ValueError, match="Supported"):
        bs.ContractInferrer(str(tmp_path / "file.unsupported"))._load(pl)


def test_infer_fields_rules_and_pii_detection():
    df = pl.DataFrame(
        {
            "id": list(range(1, 11)),
            "order_ref": [f"A-{idx}" for idx in range(1, 11)],
            "status": ["active", "inactive", "active", "inactive", "active", "inactive", "active", "inactive", "active", "inactive"],
            "amount": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
            "start_date": [f"2024-01-{idx:02d}" for idx in range(1, 11)],
            "end_date": [f"2024-01-{idx:02d}" for idx in range(11, 21)],
            "created_at": [None] * 10,
            "email": [f"user{idx}@example.com" for idx in range(1, 11)],
        }
    )
    inferrer = bs.ContractInferrer(df)
    pii_map = inferrer._detect_pii_lightweight(df)
    assert pii_map["email"] == "email"

    fields = inferrer._infer_fields(df, pii_map)
    id_field = next(field for field in fields if field["name"] == "id")
    created_field = next(field for field in fields if field["name"] == "created_at")
    email_field = next(field for field in fields if field["name"] == "email")
    assert id_field["required"] is True
    assert "required" not in created_field
    assert email_field["pii"] is True
    assert email_field["classification"] == "email"
    assert "examples" not in email_field

    quality = inferrer._suggest_rules(df)
    row_rule_names = {rule["name"] for rule in quality["row_rules"]}
    dataset_rule_names = {rule["name"] for rule in quality.get("dataset_rules", [])}
    assert "status_not_null" in row_rule_names
    assert "valid_status" in row_rule_names
    assert "valid_amount_range" in row_rule_names
    assert "start_date_before_end_date" in row_rule_names
    assert "order_ref_unique" in dataset_rule_names


def test_presidio_detection_load_table_and_infer_contract_end_to_end(monkeypatch, tmp_path):
    class FakeAnalyzer:
        def analyze(self, text, language="en"):
            return [types.SimpleNamespace(entity_type="PERSON")] if "Alice" in text else []

    monkeypatch.setitem(sys.modules, "presidio_analyzer", types.SimpleNamespace(AnalyzerEngine=lambda: FakeAnalyzer()))
    inferrer = bs.ContractInferrer("dummy", detect_pii=True)
    pd = pytest.importorskip("pandas")
    pii = inferrer._detect_pii_presidio(pd.DataFrame({"name": ["Alice Jones", "Bob"]}))
    assert pii["name"] == "PERSON"

    load_calls = []
    monkeypatch.setattr(bs.ContractInferrer, "_load_table", lambda self, source, pl_mod: (load_calls.append(source) or pl.DataFrame({"id": [1], "state": ["new"]})))
    table_inferrer = bs.ContractInferrer("catalog.schema.orders", connection=object())
    loaded_df, loaded_path = table_inferrer._load(pl)
    assert load_calls == ["catalog.schema.orders"]
    assert loaded_path is None
    assert loaded_df.to_dicts() == [{"id": 1, "state": "new"}]

    monkeypatch.setattr("lakelogic.core.describe_columns.describe_columns", lambda fields, **kwargs: {"status": "Lifecycle state"})
    source_df = pl.DataFrame({"status": ["new", "done"], "email": ["a@example.com", "b@example.com"]})
    draft = bs.infer_contract(
        source_df,
        title="Orders",
        domain="commerce",
        system="erp",
        describe_with_ai=True,
        extra={"model": {"owner": "team-data"}, "tags": ["bronze"]},
    )
    contract = draft.to_dict()
    assert contract["info"]["title"] == "Orders"
    assert contract["info"]["domain"] == "commerce"
    assert contract["model"]["owner"] == "team-data"
    assert any(field.get("description") == "Lifecycle state" for field in contract["model"]["fields"] if field["name"] == "status")
    assert contract["tags"] == ["bronze"]
