"""
Tests for lakelogic.core.ddl — DDL generation from contracts.
"""
import pytest
import sys
import textwrap
from pathlib import Path

from lakelogic.core.models import DataContract, FieldDefinition, Model, Materialization, Info
from lakelogic.core.ddl import (
    generate_ddl,
    generate_drop_ddl,
    generate_alter_ddl,
    create_table,
    _resolve_type,
    _resolve_table_name,
    _normalize_base_type,
    _extract_varchar_length,
    is_safe_widening,
    init_tables_from_directory,
    _resolve_arrow_type,
    _init_delta_table_from_contract,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _make_contract(
    fields=None,
    table_name="bronze.orders",
    primary_key=None,
    partition_by=None,
    cluster_by=None,
    fmt=None,
    description=None,
):
    """Helper to build a minimal contract with model fields."""
    if fields is None:
        fields = [
            FieldDefinition(name="order_id", type="string", required=True, description="Unique order identifier"),
            FieldDefinition(name="customer_id", type="string", required=True),
            FieldDefinition(name="order_date", type="timestamp"),
            FieldDefinition(name="amount", type="decimal(10,2)"),
            FieldDefinition(name="status", type="string"),
            FieldDefinition(name="is_active", type="boolean"),
        ]
    mat = Materialization(
        target_path=f"table:{table_name}",
        partition_by=partition_by or [],
        cluster_by=cluster_by or [],
        format=fmt,
    )
    info = Info(title="Orders Contract", version="1.0", description=description) if description else Info(title="Orders", version="1.0")
    return DataContract(
        version="1.0",
        info=info,
        model=Model(fields=fields),
        materialization=mat,
        primary_key=primary_key or [],
    )


# ── Type Resolution Tests ────────────────────────────────────────────────────

class TestTypeResolution:

    def test_type_normalization_and_varchar_length_helpers(self):
        assert _normalize_base_type("STRING") == "varchar"
        assert _normalize_base_type("INT8") == "bigint"
        assert _extract_varchar_length("VARCHAR(255)") == 255
        assert _extract_varchar_length("TEXT") is None

    def test_safe_widening_helper(self):
        assert is_safe_widening("INT", "BIGINT") is True
        assert is_safe_widening("VARCHAR(50)", "VARCHAR(255)") is True
        assert is_safe_widening("VARCHAR(255)", "VARCHAR(50)") is False
        assert is_safe_widening("BOOLEAN", "VARCHAR") is True
        assert is_safe_widening("DOUBLE", "INT") is False

    def test_string_types(self):
        assert _resolve_type("string", "spark") == "STRING"
        assert _resolve_type("string", "duckdb") == "VARCHAR"
        assert _resolve_type("string", "sqlite") == "TEXT"
        assert _resolve_type("string", "snowflake") == "VARCHAR"
        assert _resolve_type("string", "bigquery") == "STRING"
        assert _resolve_type("string", "postgresql") == "TEXT"

    def test_integer_types(self):
        assert _resolve_type("int", "spark") == "INT"
        assert _resolve_type("integer", "duckdb") == "INTEGER"
        assert _resolve_type("bigint", "snowflake") == "BIGINT"
        assert _resolve_type("bigint", "bigquery") == "INT64"

    def test_float_types(self):
        assert _resolve_type("float", "spark") == "FLOAT"
        assert _resolve_type("double", "postgresql") == "DOUBLE PRECISION"
        assert _resolve_type("float", "bigquery") == "FLOAT64"

    def test_boolean_types(self):
        assert _resolve_type("boolean", "spark") == "BOOLEAN"
        assert _resolve_type("bool", "sqlite") == "INTEGER"  # SQLite has no native bool
        assert _resolve_type("boolean", "bigquery") == "BOOL"

    def test_timestamp_types(self):
        assert _resolve_type("timestamp", "spark") == "TIMESTAMP"
        assert _resolve_type("timestamp", "snowflake") == "TIMESTAMP_NTZ"
        assert _resolve_type("timestamp_tz", "snowflake") == "TIMESTAMP_TZ"
        assert _resolve_type("timestamp", "sqlite") == "TEXT"  # SQLite stores as text

    def test_decimal_parameterised(self):
        assert _resolve_type("decimal(10,2)", "spark") == "DECIMAL(10,2)"
        assert _resolve_type("decimal(10,2)", "snowflake") == "NUMBER(10,2)"
        assert _resolve_type("decimal(10,2)", "bigquery") == "NUMERIC(10,2)"
        assert _resolve_type("decimal(10,2)", "sqlite") == "REAL"  # SQLite ignores precision

    def test_varchar_parameterised(self):
        assert _resolve_type("varchar(255)", "snowflake") == "VARCHAR(255)"
        assert _resolve_type("varchar(255)", "spark") == "STRING"  # Spark ignores length
        assert _resolve_type("varchar(255)", "postgresql") == "VARCHAR(255)"

    def test_json_type(self):
        assert _resolve_type("json", "duckdb") == "JSON"
        assert _resolve_type("json", "snowflake") == "VARIANT"
        assert _resolve_type("json", "postgresql") == "JSONB"
        assert _resolve_type("json", "spark") == "STRING"

    def test_unknown_type_passthrough(self):
        assert _resolve_type("GEOGRAPHY", "bigquery") == "GEOGRAPHY"
        assert _resolve_type("custom_type", "spark") == "CUSTOM_TYPE"


# ── DDL Generation Tests ─────────────────────────────────────────────────────

class TestGenerateDDL:

    def test_basic_spark_ddl(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "spark")
        assert "CREATE TABLE IF NOT EXISTS bronze.orders" in ddl
        assert "order_id STRING NOT NULL" in ddl
        assert "customer_id STRING NOT NULL" in ddl
        assert "order_date TIMESTAMP" in ddl
        assert "amount DECIMAL(10,2)" in ddl
        assert "is_active BOOLEAN" in ddl
        assert "USING DELTA" in ddl  # Default format for Spark
        assert ddl.endswith(";")

    def test_basic_duckdb_ddl(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "duckdb")
        assert "CREATE TABLE IF NOT EXISTS bronze.orders" in ddl
        assert "order_id VARCHAR NOT NULL" in ddl
        assert "amount DECIMAL(10,2)" in ddl
        assert "is_active BOOLEAN" in ddl
        assert "USING" not in ddl  # DuckDB doesn't use USING clause

    def test_basic_snowflake_ddl(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "snowflake")
        assert "order_id VARCHAR NOT NULL" in ddl
        assert "order_date TIMESTAMP_NTZ" in ddl
        assert "amount NUMBER(10,2)" in ddl

    def test_basic_bigquery_ddl(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "bigquery")
        assert "order_id STRING NOT NULL" in ddl
        assert "amount NUMERIC(10,2)" in ddl
        assert "is_active BOOL" in ddl

    def test_basic_sqlite_ddl(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "sqlite")
        assert "order_id TEXT NOT NULL" in ddl
        assert "amount REAL" in ddl  # SQLite ignores decimal precision
        assert "is_active INTEGER" in ddl  # SQLite has no native bool

    def test_basic_postgresql_ddl(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "postgresql")
        assert "order_id TEXT NOT NULL" in ddl
        assert "amount NUMERIC(10,2)" in ddl
        assert "is_active BOOLEAN" in ddl

    def test_primary_key_duckdb(self):
        contract = _make_contract(primary_key=["order_id"])
        ddl = generate_ddl(contract, "duckdb")
        assert "CONSTRAINT pk_bronze_orders PRIMARY KEY (order_id)" in ddl

    def test_primary_key_snowflake(self):
        contract = _make_contract(primary_key=["order_id", "customer_id"])
        ddl = generate_ddl(contract, "snowflake")
        assert "PRIMARY KEY (order_id, customer_id)" in ddl

    def test_primary_key_not_in_spark(self):
        """Spark/Databricks don't enforce PK constraints in DDL."""
        contract = _make_contract(primary_key=["order_id"])
        ddl = generate_ddl(contract, "spark")
        assert "PRIMARY KEY" not in ddl

    def test_partition_by_spark(self):
        contract = _make_contract(partition_by=["order_date"])
        ddl = generate_ddl(contract, "spark")
        assert "PARTITIONED BY (order_date)" in ddl

    def test_partition_by_bigquery(self):
        contract = _make_contract(partition_by=["order_date"])
        ddl = generate_ddl(contract, "bigquery")
        assert "PARTITION BY order_date" in ddl

    def test_partition_by_not_in_duckdb(self):
        """DuckDB doesn't support PARTITIONED BY in DDL."""
        contract = _make_contract(partition_by=["order_date"])
        ddl = generate_ddl(contract, "duckdb")
        assert "PARTITION" not in ddl

    def test_cluster_by_bigquery(self):
        contract = _make_contract(cluster_by=["customer_id", "status"])
        ddl = generate_ddl(contract, "bigquery")
        assert "CLUSTER BY customer_id, status" in ddl

    def test_cluster_by_snowflake(self):
        contract = _make_contract(cluster_by=["customer_id"])
        ddl = generate_ddl(contract, "snowflake")
        assert "CLUSTER BY (customer_id)" in ddl

    def test_custom_format_spark(self):
        contract = _make_contract(fmt="iceberg")
        ddl = generate_ddl(contract, "spark")
        assert "USING ICEBERG" in ddl

    def test_column_comments_spark(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "spark")
        assert "COMMENT 'Unique order identifier'" in ddl

    def test_column_comments_snowflake(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "snowflake")
        assert "COMMENT ON COLUMN bronze.orders.order_id IS 'Unique order identifier'" in ddl

    def test_no_comments_when_disabled(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "spark", include_comments=False)
        assert "COMMENT" not in ddl

    def test_table_description_snowflake(self):
        contract = _make_contract(description="Core orders table")
        ddl = generate_ddl(contract, "snowflake")
        assert "COMMENT ON TABLE bronze.orders IS 'Core orders table'" in ddl

    def test_if_not_exists_disabled(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "duckdb", if_not_exists=False)
        assert "IF NOT EXISTS" not in ddl
        assert "CREATE TABLE bronze.orders" in ddl

    def test_custom_table_name(self):
        contract = _make_contract()
        ddl = generate_ddl(contract, "duckdb", table_name="silver.clean_orders")
        assert "silver.clean_orders" in ddl
        assert "bronze.orders" not in ddl

    def test_no_fields_raises(self):
        contract = DataContract(version="1.0")
        with pytest.raises(ValueError, match="no model.fields"):
            generate_ddl(contract, "duckdb")

    def test_no_table_name_raises(self):
        contract = DataContract(
            version="1.0",
            model=Model(fields=[FieldDefinition(name="id", type="int")]),
        )
        with pytest.raises(ValueError, match="no table name"):
            generate_ddl(contract, "duckdb")

    def test_pii_annotation_spark(self):
        fields = [
            FieldDefinition(name="email", type="string", pii=True),
            FieldDefinition(name="name", type="string", pii=True, description="Full name"),
        ]
        contract = _make_contract(fields=fields)
        ddl = generate_ddl(contract, "spark")
        assert "COMMENT 'PII'" in ddl  # email gets PII comment
        assert "COMMENT 'Full name'" in ddl  # name gets its description


# ── DROP & ALTER Tests ───────────────────────────────────────────────────────

class TestDropDDL:

    def test_drop_table(self):
        contract = _make_contract()
        ddl = generate_drop_ddl(contract, "duckdb")
        assert ddl == "DROP TABLE IF EXISTS bronze.orders;"

    def test_drop_without_if_exists(self):
        contract = _make_contract()
        ddl = generate_drop_ddl(contract, "spark", if_exists=False)
        assert ddl == "DROP TABLE bronze.orders;"


class TestAlterDDL:

    def test_alter_adds_new_columns(self):
        contract = _make_contract()
        # Ensure server block uses "append" to allow new columns automatically
        contract.server = type("MockServer", (), {})()
        contract.server.schema_policy = type("MockPolicy", (), {"evolution": "append"})()
        
        existing = ["order_id", "customer_id", "order_date"]
        stmts = generate_alter_ddl(contract, "duckdb", existing)
        # Should add amount, status, is_active
        assert len(stmts) == 3
        assert any("amount" in s for s in stmts)
        assert any("status" in s for s in stmts)
        assert any("is_active" in s for s in stmts)

    def test_alter_no_changes(self):
        fields = [FieldDefinition(name="id", type="int")]
        contract = _make_contract(fields=fields)
        stmts = generate_alter_ddl(contract, "duckdb", ["id"])
        assert len(stmts) == 0

    def test_alter_case_insensitive(self):
        fields = [FieldDefinition(name="OrderId", type="string")]
        contract = _make_contract(fields=fields)
        stmts = generate_alter_ddl(contract, "duckdb", ["orderid"])
        assert len(stmts) == 0  # Already exists (case-insensitive)

    def test_alter_strict_and_safe_widening_paths(self, monkeypatch):
        contract = _make_contract(fields=[FieldDefinition(name="amount", type="bigint")])
        contract.server = type("MockServer", (), {})()
        contract.server.schema_policy = type("MockPolicy", (), {"evolution": "append"})()

        stmts = generate_alter_ddl(
            contract,
            "duckdb",
            ["amount"],
            existing_column_types={"amount": "INTEGER"},
        )
        assert stmts == ["ALTER TABLE bronze.orders ALTER COLUMN amount TYPE BIGINT;"]

        strict_contract = _make_contract(fields=[FieldDefinition(name="new_col", type="string")])
        strict_contract.server = type("MockServer", (), {})()
        strict_contract.server.schema_policy = type("MockPolicy", (), {"evolution": "strict"})()
        with pytest.raises(ValueError, match="Schema evolution error: New column"):
            generate_alter_ddl(strict_contract, "duckdb", ["amount"])

        warned = []
        monkeypatch.setattr("lakelogic.core.ddl.logger.warning", warned.append)
        unsafe_contract = _make_contract(fields=[FieldDefinition(name="amount", type="int")])
        unsafe_contract.server = type("MockServer", (), {})()
        unsafe_contract.server.schema_policy = type("MockPolicy", (), {"evolution": "append"})()
        stmts = generate_alter_ddl(
            unsafe_contract,
            "duckdb",
            ["amount"],
            existing_column_types={"amount": "DOUBLE"},
        )
        assert stmts == []
        assert any("UNSAFE type change detected" in message for message in warned)


# ── Table Name Resolution Tests ──────────────────────────────────────────────

class TestTableNameResolution:

    def test_from_materialization(self):
        contract = _make_contract(table_name="gold.dim_customer")
        assert _resolve_table_name(contract) == "gold.dim_customer"

    def test_from_dataset(self):
        contract = DataContract(version="1.0", dataset="raw_events")
        assert _resolve_table_name(contract) == "raw_events"

    def test_from_server_path_and_info_table_name(self):
        contract = DataContract(version="1.0", server={"type": "local", "path": "table:catalog.orders"})
        assert _resolve_table_name(contract) == "catalog.orders"

        info_contract = DataContract(version="1.0", info={"title": "Orders", "version": "1.0", "table_name": "mart.orders"})
        assert _resolve_table_name(info_contract) == "mart.orders"

    def test_from_info_title(self):
        contract = DataContract(
            version="1.0",
            info=Info(title="Customer Events", version="1.0"),
        )
        assert _resolve_table_name(contract) == "customer_events"

    def test_none_when_no_info(self):
        contract = DataContract(version="1.0")
        assert _resolve_table_name(contract) is None


# ── Execution Tests (DuckDB) ─────────────────────────────────────────────────


class TestCreateTableSQLite:

    def test_create_table_sqlite_memory(self):
        import sqlite3
        contract = _make_contract(table_name="orders")
        con = sqlite3.connect(":memory:")
        try:
            create_table(contract, "sqlite", connection=con)
            cursor = con.execute("SELECT * FROM orders LIMIT 0")
            col_names = [desc[0] for desc in cursor.description]
            assert "order_id" in col_names
            assert "amount" in col_names
        finally:
            con.close()

    def test_create_table_other_backends_and_directory_init(self, monkeypatch, tmp_path):
        executed = []

        class FakeCursor:
            def execute(self, stmt):
                executed.append(stmt)

            def close(self):
                executed.append("cursor_closed")

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                executed.append("commit")

            def query(self, stmt):
                executed.append(stmt)
                return type("Job", (), {"result": lambda self: None})()

        init_calls = []
        monkeypatch.setattr("lakelogic.core.ddl._init_delta_table_from_contract", lambda contract: init_calls.append(_resolve_table_name(contract)))

        spark_calls = []
        fake_sql_module = type("FakeSparkModule", (), {})()
        fake_sql_module.SparkSession = type(
            "FakeSparkSession",
            (),
            {"builder": type("Builder", (), {"getOrCreate": staticmethod(lambda: type("Spark", (), {"sql": lambda self, stmt: spark_calls.append(stmt)})())})()},
        )
        monkeypatch.setitem(sys.modules, "pyspark", type("FakePyspark", (), {})())
        monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

        direct_contract = _make_contract(table_name="ignored")
        direct_contract.materialization.target_path = str(tmp_path / "delta")
        create_table(direct_contract, "spark")
        assert init_calls == ["orders"] or init_calls

        table_contract = _make_contract(table_name="catalog.orders")
        create_table(table_contract, "spark")
        assert any("CREATE SCHEMA IF NOT EXISTS catalog" in stmt for stmt in spark_calls)

        create_table(_make_contract(table_name="snow.orders"), "snowflake", connection=FakeConnection())
        create_table(_make_contract(table_name="bq.orders"), "bigquery", connection=FakeConnection())
        create_table(_make_contract(table_name="pg.orders"), "postgresql", connection=FakeConnection())
        create_table(_make_contract(table_name="delta.orders"), "polars")
        assert "commit" in executed
        assert any("CREATE TABLE IF NOT EXISTS snow.orders" in stmt for stmt in executed)
        assert any("CREATE TABLE IF NOT EXISTS bq.orders" in stmt for stmt in executed)

        contract_dir = tmp_path / "contracts"
        contract_dir.mkdir()
        (contract_dir / "good.yaml").write_text(
            textwrap.dedent(
                """
                version: "1.0"
                dataset: sample_table
                model:
                  fields:
                    - name: id
                      type: int
                """
            ).strip(),
            encoding="utf-8",
        )
        (contract_dir / "other.yml").write_text(
            textwrap.dedent(
                """
                version: "1.0"
                dataset: other_table
                model:
                  fields:
                    - name: name
                      type: string
                """
            ).strip(),
            encoding="utf-8",
        )
        (contract_dir / "bad.yaml").write_text("[]", encoding="utf-8")

        monkeypatch.setattr("lakelogic.core.ddl.create_table", lambda contract, backend, db_path=None, connection=None, dry_run=False: f"DDL::{_resolve_table_name(contract)}::{backend}")
        results = init_tables_from_directory(contract_dir, "duckdb", dry_run=True)
        assert str(contract_dir / "good.yaml") in results
        assert str(contract_dir / "other.yml") in results
        assert results[str(contract_dir / "good.yaml")] == "DDL::sample_table::duckdb"


# ── DataProcessor Integration Tests ──────────────────────────────────────────

class TestProcessorDDL:

    def test_processor_generate_ddl(self, tmp_path):
        import yaml
        contract_data = {
            "version": "1.0",
            "info": {"title": "Test Contract", "version": "1.0"},
            "model": {
                "fields": [
                    {"name": "id", "type": "int", "required": True},
                    {"name": "name", "type": "string"},
                    {"name": "score", "type": "float"},
                ]
            },
            "materialization": {
                "target_path": "table:analytics.scores",
            },
        }
        contract_file = tmp_path / "test.yaml"
        with open(contract_file, "w") as f:
            yaml.dump(contract_data, f)

        from lakelogic.core.processor import DataProcessor
        proc = DataProcessor(str(contract_file), engine="polars")

        ddl = proc.generate_ddl("duckdb")
        assert "CREATE TABLE IF NOT EXISTS analytics.scores" in ddl
        assert "id INTEGER NOT NULL" in ddl
        assert "name VARCHAR" in ddl
        assert "score FLOAT" in ddl

    def test_processor_create_table_dry_run(self, tmp_path):
        import yaml
        contract_data = {
            "version": "1.0",
            "info": {"title": "Test", "version": "1.0"},
            "model": {
                "fields": [
                    {"name": "id", "type": "int", "required": True},
                    {"name": "value", "type": "decimal(8,2)"},
                ]
            },
            "materialization": {
                "target_path": "table:bronze.metrics",
            },
        }
        contract_file = tmp_path / "test.yaml"
        with open(contract_file, "w") as f:
            yaml.dump(contract_data, f)

        from lakelogic.core.processor import DataProcessor
        proc = DataProcessor(str(contract_file), engine="polars")

        ddl = proc.create_table("spark", dry_run=True)
        assert "CREATE TABLE IF NOT EXISTS bronze.metrics" in ddl
        assert "USING DELTA" in ddl
        assert "id INT NOT NULL" in ddl
        assert "value DECIMAL(8,2)" in ddl

class TestDeltaInitialization:

    def test_resolve_arrow_type_handles_parameterized_and_scalar_types(self):
        pa = pytest.importorskip("pyarrow")

        assert _resolve_arrow_type("decimal(10,2)") == pa.decimal128(10, 2)
        assert _resolve_arrow_type("varchar(255)") == pa.string()
        assert _resolve_arrow_type("binary") == pa.binary()
        assert _resolve_arrow_type("timestamp_tz") == pa.timestamp("us")

    def test_init_delta_table_returns_early_for_missing_fields_and_targets(self, monkeypatch, tmp_path):
        warnings = []
        infos = []
        monkeypatch.setattr("lakelogic.core.ddl.logger.warning", lambda message: warnings.append(message))
        monkeypatch.setattr("lakelogic.core.ddl.logger.info", lambda message: infos.append(message))

        no_fields = DataContract(version="1.0", materialization=Materialization(target_path=str(tmp_path / "data")))
        _init_delta_table_from_contract(no_fields)
        assert any("no model.fields" in message for message in warnings)

        no_target = DataContract(
            version="1.0",
            model=Model(fields=[FieldDefinition(name="id", type="int")]),
            materialization=Materialization(target_path=None),
        )
        _init_delta_table_from_contract(no_target)
        assert any("No materialization.target_path" in message for message in infos)

        table_target = DataContract(
            version="1.0",
            model=Model(fields=[FieldDefinition(name="id", type="int")]),
            materialization=Materialization(target_path="table:catalog.orders"),
        )
        _init_delta_table_from_contract(table_target)
        assert any("catalog table reference" in message for message in infos)

    def test_init_delta_table_handles_cloud_missing_creds_and_existing_table(self, monkeypatch):
        warnings = []
        infos = []
        monkeypatch.setattr("lakelogic.core.ddl.logger.warning", lambda message: warnings.append(message))
        monkeypatch.setattr("lakelogic.core.ddl.logger.info", lambda message: infos.append(message))

        cloud_contract = DataContract(
            version="1.0",
            model=Model(fields=[FieldDefinition(name="id", type="int")]),
            materialization=Materialization(target_path="abfss://container/orders"),
        )
        monkeypatch.setattr("lakelogic.core.materialization._build_storage_options", lambda: {})
        _init_delta_table_from_contract(cloud_contract)
        assert any("Cloud path detected" in message for message in warnings)

        existing_contract = DataContract(
            version="1.0",
            info=Info(title="Orders", version="1.0"),
            model=Model(fields=[FieldDefinition(name="id", type="int")]),
            materialization=Materialization(target_path="C:/warehouse/orders"),
        )

        class FakeExistingDeltaTable:
            def __init__(self, target, storage_options=None):
                self.target = target

        fake_deltalake = type("FakeDeltaLake", (), {"write_deltalake": staticmethod(lambda *args, **kwargs: None), "DeltaTable": FakeExistingDeltaTable})
        monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)
        _init_delta_table_from_contract(existing_contract)
        assert any("already exists" in message for message in infos)

    def test_init_delta_table_creates_schema_and_falls_back_to_empty_write(self, monkeypatch, tmp_path):
        pa = pytest.importorskip("pyarrow")
        info_logs = []
        monkeypatch.setattr("lakelogic.core.ddl.logger.info", lambda message: info_logs.append(message))

        contract = DataContract(
            version="1.0",
            info=Info(title="Orders", version="1.0"),
            model=Model(fields=[FieldDefinition(name="id", type="int", required=True), FieldDefinition(name="payload", type="string")]),
            materialization=Materialization(target_path=str(tmp_path / "delta_orders"), partition_by=["id", "missing"]),
            lineage={
                "enabled": True,
                "capture_source_path": True,
                "source_column_name": "src_path",
                "capture_timestamp": True,
                "timestamp_column_name": "processed_at",
                "capture_run_id": False,
                "capture_domain": False,
                "capture_system": False,
                "capture_created_at": False,
                "capture_created_by": False,
            },
        )

        create_calls = []
        write_calls = []

        class FakeDeltaTable:
            def __init__(self, target, storage_options=None):
                raise RuntimeError("missing")

            @staticmethod
            def create(**kwargs):
                create_calls.append(kwargs)

        fake_deltalake = type(
            "FakeDeltaLake",
            (),
            {"write_deltalake": staticmethod(lambda target, table, **kwargs: write_calls.append((target, table, kwargs))), "DeltaTable": FakeDeltaTable},
        )
        monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)
        _init_delta_table_from_contract(contract)

        schema = create_calls[0]["schema"]
        assert create_calls[0]["partition_by"] == ["id"]
        assert schema.field("id").nullable is False
        assert schema.get_field_index("src_path") >= 0
        assert schema.get_field_index("processed_at") >= 0
        assert not write_calls

        class FakeFallbackDeltaTable(FakeDeltaTable):
            @staticmethod
            def create(**kwargs):
                raise TypeError("old version")

        fake_fallback = type(
            "FakeFallbackDeltaLake",
            (),
            {"write_deltalake": staticmethod(lambda target, table, **kwargs: write_calls.append((target, table, kwargs))), "DeltaTable": FakeFallbackDeltaTable},
        )
        monkeypatch.setitem(sys.modules, "deltalake", fake_fallback)
        _init_delta_table_from_contract(contract)

        assert write_calls[-1][0] == str(tmp_path / "delta_orders")
        assert write_calls[-1][1].num_rows == 0
        assert write_calls[-1][2]["mode"] == "overwrite"
        assert any("Initialized Delta table schema" in message for message in info_logs)

    def test_processor_defaults_to_own_engine(self, tmp_path):
        import yaml
        contract_data = {
            "version": "1.0",
            "model": {
                "fields": [{"name": "id", "type": "int"}]
            },
            "materialization": {"target_path": "table:test_table"},
        }
        contract_file = tmp_path / "test.yaml"
        with open(contract_file, "w") as f:
            yaml.dump(contract_data, f)

        from lakelogic.core.processor import DataProcessor
        proc = DataProcessor(str(contract_file), engine="polars")

        ddl = proc.generate_ddl()  # No backend specified → uses polars generic backend or defaults
        assert "INTEGER" in ddl or "INT" in ddl
