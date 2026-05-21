from __future__ import annotations

import builtins
import datetime as dt
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakelogic.knowledge import KnowledgeBase, KnowledgeEntry
from lakelogic.scanner import Scanner
from lakelogic.scanner.config import (
    ConnectionConfig,
    DiscoveryConfig,
    ObservatoryConfig,
    ScannerConfig,
)
from lakelogic.scanner.connector import (
    BaseConnector,
    DeltaConnector,
    DuckDBConnector,
    HistoryEntry,
    ScannedTable,
    TableMetadata,
    UnityCatalogConnector,
    build_connector,
)
from lakelogic.scanner.schema_drift import (
    BaselineStore,
    LocalBaselineStore,
    SchemaDiff,
    _is_narrowing,
    _type_rank,
    compare_schemas,
)
from lakelogic.scanner.validator import ScannerValidator


class MemoryBaselineStore(BaselineStore):
    def __init__(self, data=None):
        self.data = data or {}
        self.saved = []

    def get(self, table_name):
        return self.data.get(table_name)

    def save(self, table_name, schema):
        self.saved.append((table_name, schema))
        self.data[table_name] = schema


class FakeConnector(BaseConnector):
    def __init__(self, tables=None, metadata=None, min_timestamp=None, fail_discover=False, fail_metadata=False):
        self.tables = tables or []
        self.metadata = metadata or {}
        self.min_timestamp = min_timestamp
        self.fail_discover = fail_discover
        self.fail_metadata = fail_metadata
        self.connected = False

    def connect(self):
        self.connected = True

    def discover(self, config):
        if self.fail_discover:
            raise RuntimeError("boom")
        return self.tables

    def get_metadata(self, table):
        if self.fail_metadata:
            raise RuntimeError("metadata boom")
        return self.metadata.get(table.full_name, TableMetadata(table=table))

    def query_min_timestamp(self, table, columns):
        return self.min_timestamp


def test_knowledge_base_loads_overrides_and_searches(tmp_path: Path) -> None:
    base = tmp_path / "kb.json"
    base.write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "pattern_id": "nulls",
                        "title": "Null spike",
                        "description": "desc",
                        "remediation": "Check upstream",
                        "tags": ["quality"],
                        "examples": ["x"],
                        "severity": "error",
                    },
                    {"pattern_id": "freshness", "title": "Late", "remediation": "Backfill"},
                ]
            }
        ),
        encoding="utf-8",
    )

    kb = KnowledgeBase(extra_path=base)

    assert len(kb) >= 2
    assert kb.lookup("nulls", contract_name="orders") == "Check upstream"
    assert kb.lookup("missing") is None
    assert kb.get("nulls") == KnowledgeEntry(
        pattern_id="nulls",
        title="Null spike",
        description="desc",
        remediation="Check upstream",
        tags=["quality"],
        examples=["x"],
        severity="error",
    )
    assert [entry.pattern_id for entry in kb.search("quality")] == ["nulls"]
    assert {entry.pattern_id for entry in kb.all_entries()} >= {"nulls", "freshness"}


def test_scanner_config_resolves_env_and_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCAN_TOKEN", "secret-token")
    monkeypatch.setenv("SCAN_ENDPOINT", "https://obs.example")
    cfg_path = tmp_path / "scanner.yaml"
    cfg_path.write_text(
        """
connection:
  type: unity_catalog
  host: https://dbc.example
  token: ${SCAN_TOKEN}
  catalog: lake
discovery:
  include_schemas: [silver]
output:
  slo_checks_table: /tmp/checks
observatory:
  endpoint: ${SCAN_ENDPOINT}
  api_key: ${SCAN_TOKEN}
""",
        encoding="utf-8",
    )

    cfg = ScannerConfig.from_yaml(str(cfg_path))

    assert cfg.connection.token == "secret-token"
    assert cfg.observatory.endpoint == "https://obs.example"
    assert cfg.discovery.include_schemas == ["silver"]
    assert cfg.output.slo_checks_backend == "delta"
    assert cfg.domain == "lake"
    assert ScannerConfig.from_args("duckdb", path=":memory:").connection.path == ":memory:"
    with pytest.raises(FileNotFoundError):
        ScannerConfig.from_yaml(str(tmp_path / "missing.yaml"))


def test_scanner_public_facade_wires_connector_and_validator(monkeypatch: pytest.MonkeyPatch) -> None:
    table = ScannedTable("cat", "silver", "orders")
    connector = FakeConnector(
        tables=[table],
        metadata={table.full_name: TableMetadata(table=table, last_modified=dt.datetime.now(dt.timezone.utc))},
    )

    monkeypatch.setattr("lakelogic.scanner.build_connector", lambda config: connector)
    monkeypatch.setattr("lakelogic.scanner.LocalBaselineStore", lambda: MemoryBaselineStore())

    scanner = Scanner.from_args("duckdb", path=":memory:")
    assert scanner.connect() is scanner
    report = scanner.run(pipeline_run_id="run-1")

    assert connector.connected is True
    assert report.pipeline_run_id == "run-1"
    assert report.passed is True


def test_schema_drift_detects_added_removed_type_and_nullability() -> None:
    baseline = [
        {"name": "id", "type": "bigint", "nullable": False},
        {"name": "amount", "type": "double", "nullable": True},
        {"name": "legacy", "type": "string", "nullable": True},
    ]
    current = [
        {"name": "id", "type": "int", "nullable": False},
        {"name": "amount", "type": "double", "nullable": False},
        {"name": "status", "type": "string", "nullable": True},
    ]

    diff = compare_schemas(baseline, current)

    assert _type_rank("decimal(10,2)") == _type_rank("decimal")
    assert _type_rank("mystery") > _type_rank("timestamp")
    assert _is_narrowing("bigint", "int") is True
    assert diff.has_breaking_changes is True
    assert diff.severity() == "breaking"
    assert diff.summary() == "1 column(s) removed, 1 column(s) added, 1 type change(s), 1 nullability change(s)"
    assert diff.to_dict()["type_changes"] == [{"name": "id", "from": "bigint", "to": "int"}]
    assert SchemaDiff().is_empty is True
    assert SchemaDiff(added=[{"name": "x"}]).severity() == "warning"


def test_local_baseline_store_handles_missing_bad_and_saved_files(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "baselines.json"
    store = LocalBaselineStore(str(path))
    assert store.get("missing") is None

    schema = [{"name": "id", "type": "int", "nullable": False}]
    store.save("cat.schema.table", schema)

    reloaded = LocalBaselineStore(str(path))
    assert reloaded.get("cat.schema.table") == schema

    path.write_text("{bad json", encoding="utf-8")
    assert LocalBaselineStore(str(path)).get("cat.schema.table") is None


def test_connectors_discover_filter_and_factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "silver" / "orders" / "_delta_log").mkdir(parents=True)
    (tmp_path / "tmp" / "tmp_bad" / "_delta_log").mkdir(parents=True)

    cfg = DiscoveryConfig(include_schemas=["silver"], max_tables=1)
    delta = DeltaConnector(ConnectionConfig(type="delta", storage_root=str(tmp_path)))
    tables = delta.discover(cfg)

    assert tables == [
        ScannedTable(
            catalog=tmp_path.name,
            schema="silver",
            table="orders",
            storage_path=str(tmp_path / "silver" / "orders"),
            engine="delta",
        )
    ]
    assert tables[0].full_name == f"{tmp_path.name}.silver.orders"
    assert tables[0].layer == "silver"
    assert DeltaConnector(ConnectionConfig(type="delta", storage_root=str(tmp_path / "missing"))).discover(cfg) == []
    assert isinstance(build_connector(ConnectionConfig(type="duckdb", path=":memory:")), DuckDBConnector)
    assert isinstance(build_connector(ConnectionConfig(type="databricks", host="h", token="t")), UnityCatalogConnector)
    with pytest.raises(ValueError, match="Unknown connection type"):
        build_connector(ConnectionConfig(type="snowflake"))

    fake_delta = SimpleNamespace(DeltaTable=object)
    monkeypatch.setitem(sys.modules, "deltalake", fake_delta)
    delta.connect()
    with pytest.raises(ValueError):
        DeltaConnector(ConnectionConfig(type="delta")).connect()


def test_delta_connector_metadata_and_timestamp_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    table = ScannedTable("cat", "schema", "table", storage_path="/delta/table")

    class FakeField:
        def __init__(self, name, type_, nullable):
            self.name = name
            self.type = type_
            self.nullable = nullable

    class FakeSchema:
        fields = [FakeField("id", "long", False)]

    class FakeActions:
        def to_pydict(self):
            return {
                "size_bytes": [10, 20],
                "num_records": [3, None, 4],
                "modification_time": [1000, 2000],
            }

    class FakeDeltaTable:
        def __init__(self, path, storage_options=None):
            self.path = path

        def schema(self):
            return FakeSchema()

        def metadata(self):
            return {}

        def get_add_actions(self, flatten=True):
            return FakeActions()

        def history(self, limit=20):
            return [
                {"timestamp": 3000, "operation": "WRITE", "operationMetrics": {"numOutputRows": "9"}},
                {"operation": "OPTIMIZE"},
            ]

    monkeypatch.setitem(sys.modules, "deltalake", SimpleNamespace(DeltaTable=FakeDeltaTable))

    conn = DeltaConnector(ConnectionConfig(type="delta", storage_root="/root"))
    monkeypatch.setattr(conn, "_get_storage_options", lambda: {})
    meta = conn.get_metadata(table)

    assert meta.num_rows == 7
    assert meta.size_bytes == 30
    assert meta.last_modified == dt.datetime.fromtimestamp(2, tz=dt.timezone.utc)
    assert meta.schema_fields == [{"name": "id", "type": "long", "nullable": False}]
    assert meta.history[0] == HistoryEntry(
        timestamp=dt.datetime.fromtimestamp(3, tz=dt.timezone.utc),
        operation="WRITE",
        num_output_rows=9,
    )

    class FakeDuckCon:
        def __init__(self):
            self.calls = 0

        def execute(self, sql):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("bad column")
            return self

        def fetchone(self):
            return [dt.datetime(2026, 1, 1, 12, 0)]

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=lambda: FakeDuckCon()))
    assert conn.query_min_timestamp(table, ["missing", "created_at"]) == dt.datetime(
        2026, 1, 1, 12, 0, tzinfo=dt.timezone.utc
    )


def test_unity_catalog_connector_success_and_error_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ConnectionConfig(type="unity_catalog", host="https://dbc", token="tok", catalog="main")
    conn = UnityCatalogConnector(cfg)
    calls = []

    class FakeResponse:
        status_code = 204
        text = ""

        def __init__(self, payload=None):
            self.payload = payload or {}

        def raise_for_status(self):
            pass

        def json(self):
            return self.payload

    def fake_get(url, headers=None, params=None, timeout=None):
        calls.append((url, params))
        if url.endswith("/catalogs/main"):
            return FakeResponse()
        if url.endswith("/schemas"):
            return FakeResponse({"schemas": [{"name": "silver"}, {"name": "tmp_skip"}]})
        if url.endswith("/tables") and params["schema_name"] == "silver":
            return FakeResponse({"tables": [{"name": "orders", "storage_location": "/mnt/orders"}]})
        if url.endswith("/tables/main.silver.orders"):
            return FakeResponse(
                {
                    "columns": [{"name": "id", "type_text": "BIGINT", "nullable": False}],
                    "updated_at": 2000,
                    "properties": {"numRows": "12"},
                }
            )
        return FakeResponse({})

    monkeypatch.setitem(sys.modules, "httpx", SimpleNamespace(get=fake_get))

    conn.connect()
    tables = conn.discover(DiscoveryConfig(max_tables=5))
    meta = conn.get_metadata(tables[0])

    assert calls[0][0].endswith("/catalogs/main")
    assert tables[0].engine == "unity_catalog"
    assert meta.num_rows == 12
    assert meta.last_modified == dt.datetime.fromtimestamp(2000, tz=dt.timezone.utc)
    assert meta.schema_fields == [{"name": "id", "type": "BIGINT", "nullable": False}]

    with pytest.raises(ValueError):
        UnityCatalogConnector(ConnectionConfig(type="unity_catalog")).connect()


def test_scanner_validator_runs_all_check_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    table = ScannedTable("cat", "gold", "orders")
    baseline = [{"name": "id", "type": "bigint", "nullable": True}]
    current = [{"name": "id", "type": "int", "nullable": False}, {"name": "status", "type": "string"}]
    history = [HistoryEntry(now, "WRITE", 500)] + [HistoryEntry(now, "WRITE", 100) for _ in range(14)]
    meta = TableMetadata(
        table=table,
        num_rows=500,
        last_modified=now - dt.timedelta(minutes=180),
        schema_fields=current,
        history=history,
    )
    cfg = ScannerConfig.from_args("duckdb", path=":memory:")
    cfg.slo_defaults.freshness.max_delay_minutes = 120
    cfg.slo_defaults.freshness.warn_at_minutes = 60
    cfg.slo_defaults.retention.default = "PT1H"
    connector = FakeConnector(
        tables=[table],
        metadata={table.full_name: meta},
        min_timestamp=now - dt.timedelta(hours=2),
    )
    validator = ScannerValidator(cfg, connector, MemoryBaselineStore({table.full_name: baseline}))
    monkeypatch.setattr(validator, "_write_results", lambda *args: None)

    report = validator.run("pipeline-1")

    assert report.passed is False
    assert [r.check_type for r in report.results] == ["freshness", "row_count", "schema_drift", "retention"]
    assert {r.severity for r in report.failures} == {"fail", "warn"}
    assert report.failures[0].entity == table.full_name

    first_seen = ScannerValidator(cfg, connector, MemoryBaselineStore())
    drift = first_seen._check_schema_drift(table, meta)
    assert drift is not None and "BASELINE SET" in drift.status

    cfg.slo_defaults.volume.anomaly_enabled = False
    assert validator._check_volume(table, meta).passed is True
    assert validator._check_volume(table, TableMetadata(table=table)) is None
    assert validator._check_retention(table, TableMetadata(table=table)) is not None


def test_scanner_validator_error_write_and_observatory_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    table = ScannedTable("cat", "bronze", "bad")
    cfg = ScannerConfig.from_args("duckdb", path=":memory:")

    failed_discovery = ScannerValidator(cfg, FakeConnector(fail_discover=True), MemoryBaselineStore())
    assert failed_discovery.run().passed is False

    failed_metadata = ScannerValidator(cfg, FakeConnector(tables=[table], fail_metadata=True), MemoryBaselineStore())
    assert failed_metadata.scan_table(table)[0].passed is False

    cfg.output.slo_checks_table = "checks"
    cfg.observatory.endpoint = "https://obs"
    cfg.observatory.api_key = "key"
    writes = []
    posts = []

    monkeypatch.setitem(
        sys.modules,
        "lakelogic.core.run_log",
        SimpleNamespace(
            write_slo_checks=lambda registry, results, check_run_id, pipeline_run_id: writes.append(registry)
        ),
    )

    class FakePostResponse:
        status_code = 500
        text = "server exploded"

    monkeypatch.setitem(
        sys.modules,
        "httpx",
        SimpleNamespace(post=lambda *args, **kwargs: posts.append((args, kwargs)) or FakePostResponse()),
    )

    result = failed_metadata.scan_table(table)[0]
    failed_metadata._write_results([result], "check-1", "pipe-1", "now")
    failed_metadata._write_results([], "check-1", None, "now")

    assert writes[0].storage.slo_checks_table == "checks"
    assert posts[0][0][0] == "https://obs/slo-checks"


def test_scanner_remaining_config_and_base_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert KnowledgeBase(extra_path=tmp_path / "no-file").all_entries() == []
    assert KnowledgeEntry.from_dict({"pattern_id": "p", "title": "T"}).severity == "warning"

    cfg = ScannerConfig.from_args(
        "unity_catalog",
        path="/lake",
        host="https://dbc",
        catalog="main",
        token="${TOKEN}",
    )
    assert cfg.connection.storage_root == "/lake"
    assert cfg.connection.host == "https://dbc"
    assert cfg.connection.catalog == "main"

    yaml_path = tmp_path / "scanner.yaml"
    yaml_path.write_text("connection:\n  type: duckdb\n  path: ':memory:'\n", encoding="utf-8")
    monkeypatch.setattr("lakelogic.scanner.build_connector", lambda config: FakeConnector())
    monkeypatch.setattr("lakelogic.scanner.LocalBaselineStore", lambda: MemoryBaselineStore())
    assert isinstance(Scanner.from_yaml(str(yaml_path)), Scanner)

    class MinimalConnector(BaseConnector):
        def connect(self):
            pass

        def discover(self, config):
            return []

        def get_metadata(self, table):
            return TableMetadata(table=table)

    assert MinimalConnector().query_min_timestamp(ScannedTable("c", "s", "t"), ["created_at"]) is None
    assert BaseConnector._matches_exclude("tmp_orders", ["tmp_*"]) is True


def test_delta_connector_cloud_and_failure_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeFs:
        def __init__(self, fail_root=False):
            self.fail_root = fail_root

        def ls(self, path, detail=False):
            if self.fail_root:
                raise RuntimeError("cannot list")
            if path == "s3://bucket/root":
                return ["bucket/root/silver", "bucket/root/tmp_skip", "bucket/root/bronze"]
            if path.endswith("silver"):
                return ["bucket/root/silver/orders", "bucket/root/silver/tmp_bad"]
            raise RuntimeError("schema failed")

        def exists(self, path):
            return path.endswith("orders/_delta_log")

    monkeypatch.setitem(sys.modules, "fsspec", SimpleNamespace(filesystem=lambda protocol, **opts: FakeFs()))
    conn = DeltaConnector(ConnectionConfig(type="delta", storage_root="s3://bucket/root"))
    monkeypatch.setattr(conn, "_get_storage_options", lambda: {"region": "eu"})
    tables = conn.discover(DiscoveryConfig(include_schemas=["silver"]))

    assert tables == [
        ScannedTable(
            catalog="bucket",
            schema="silver",
            table="orders",
            storage_path="s3://bucket/root/silver/orders",
            engine="delta",
        )
    ]

    monkeypatch.setitem(sys.modules, "fsspec", SimpleNamespace(filesystem=lambda protocol, **opts: FakeFs(True)))
    assert conn.discover(DiscoveryConfig()) == []

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "deltalake":
            raise ImportError("no deltalake")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError):
        DeltaConnector(ConnectionConfig(type="delta", storage_root="/x")).connect()
    monkeypatch.setattr(builtins, "__import__", real_import)

    class FailingDeltaTable:
        def __init__(self, path, storage_options=None):
            raise RuntimeError("cannot open")

    monkeypatch.setitem(sys.modules, "deltalake", SimpleNamespace(DeltaTable=FailingDeltaTable))
    assert conn.get_metadata(ScannedTable("c", "s", "t", storage_path="/bad")).schema_fields == []

    monkeypatch.setitem(sys.modules, "duckdb", None)
    assert conn.query_min_timestamp(ScannedTable("c", "s", "t"), ["created_at"]) is None


def test_delta_storage_options_cache_and_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = DeltaConnector(ConnectionConfig(type="delta", storage_root="s3://bucket"))
    assert isinstance(conn._get_storage_options(), dict)
    assert conn._get_storage_options() is conn._storage_options


def test_duckdb_connector_discover_metadata_and_timestamps(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "bronze" / "events" / "_delta_log").mkdir(parents=True)
    table = ScannedTable("cat", "bronze", "events", storage_path=str(tmp_path / "bronze" / "events"))

    class FakeDuckConnection:
        def __init__(self, parquet_fallback=False, no_timestamp=False):
            self.parquet_fallback = parquet_fallback
            self.no_timestamp = no_timestamp
            self.last_sql = ""

        def execute(self, sql):
            self.last_sql = sql
            if self.parquet_fallback and "delta_scan" in sql:
                raise RuntimeError("no delta")
            return self

        def fetchall(self):
            return [("id", "INTEGER")]

        def fetchone(self):
            if "MIN" in self.last_sql and self.no_timestamp:
                return [None]
            if "MIN" in self.last_sql:
                return [dt.datetime(2026, 2, 1)]
            return [42]

    conn = DuckDBConnector(ConnectionConfig(type="duckdb", path=str(tmp_path)))
    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=lambda: FakeDuckConnection()))
    conn.connect()
    assert conn.discover(DiscoveryConfig())[0].engine == "duckdb"
    meta = conn.get_metadata(table)
    assert meta.num_rows == 42
    assert meta.schema_fields == [{"name": "id", "type": "INTEGER", "nullable": True}]
    assert meta.last_modified is not None
    assert conn.query_min_timestamp(table, ["created_at"]) == dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)

    fallback = DuckDBConnector(ConnectionConfig(type="duckdb", path=str(tmp_path)))
    monkeypatch.setitem(
        sys.modules, "duckdb", SimpleNamespace(connect=lambda: FakeDuckConnection(parquet_fallback=True))
    )
    assert fallback.get_metadata(table).num_rows == 42

    no_ts = DuckDBConnector(ConnectionConfig(type="duckdb", path=str(tmp_path)))
    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=lambda: FakeDuckConnection(no_timestamp=True)))
    assert no_ts.query_min_timestamp(table, ["created_at"]) is None


def test_unity_catalog_connector_failure_and_delegate_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = ConnectionConfig(type="unity_catalog", host="https://dbc", token="tok", catalog="main")
    conn = UnityCatalogConnector(cfg)

    class RaisingResponse:
        def raise_for_status(self):
            raise RuntimeError("http error")

    monkeypatch.setitem(sys.modules, "httpx", SimpleNamespace(get=lambda *args, **kwargs: RaisingResponse()))
    with pytest.raises(ConnectionError):
        conn.connect()
    assert conn.discover(DiscoveryConfig()) == []
    assert conn.get_metadata(ScannedTable("main", "silver", "orders")).schema_fields == []

    table = ScannedTable("main", "silver", "orders", storage_path="/mnt/orders")
    monkeypatch.setattr(DeltaConnector, "query_min_timestamp", lambda self, table, columns: dt.datetime(2026, 3, 1))
    assert conn.query_min_timestamp(table, ["created_at"]) == dt.datetime(2026, 3, 1)
    monkeypatch.setattr(
        DeltaConnector, "query_min_timestamp", lambda self, table, columns: (_ for _ in ()).throw(RuntimeError())
    )
    assert conn.query_min_timestamp(table, ["created_at"]) is None
    assert conn.query_min_timestamp(ScannedTable("main", "silver", "orders"), ["created_at"]) is None


def test_scanner_validator_warning_success_and_write_failure_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    table = ScannedTable("cat", "silver", "orders")
    cfg = ScannerConfig.from_args("duckdb", path=":memory:")
    validator = ScannerValidator(cfg, FakeConnector(), MemoryBaselineStore({table.full_name: []}))

    fresh_meta = TableMetadata(table=table, last_modified=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=30))
    assert validator._check_freshness(table, fresh_meta).severity == "pass"

    naive_warn_meta = TableMetadata(
        table=table,
        last_modified=dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - dt.timedelta(minutes=90),
    )
    cfg.slo_defaults.freshness.max_delay_minutes = 120
    cfg.slo_defaults.freshness.warn_at_minutes = 60
    assert validator._check_freshness(table, naive_warn_meta).severity == "warn"

    short_history = TableMetadata(
        table=table,
        num_rows=10,
        history=[
            HistoryEntry(dt.datetime.now(dt.timezone.utc), "WRITE", 10),
            HistoryEntry(dt.datetime.now(dt.timezone.utc), "WRITE", 9),
        ],
    )
    assert "building baseline" in validator._check_volume(table, short_history).status

    unchanged = TableMetadata(table=table, schema_fields=[])
    assert validator._check_schema_drift(table, unchanged) is None
    same_schema = [{"name": "id", "type": "int", "nullable": True}]
    validator.baseline_store = MemoryBaselineStore({table.full_name: same_schema})
    assert validator._check_schema_drift(table, TableMetadata(table=table, schema_fields=same_schema)).status.endswith(
        "NO DRIFT"
    )

    added_only = [{"name": "id", "type": "int", "nullable": True}, {"name": "x", "type": "string"}]
    cfg.slo_defaults.schema_drift.on_column_added = "ignore"
    assert validator._check_schema_drift(table, TableMetadata(table=table, schema_fields=added_only)).severity == "pass"

    cfg.slo_defaults.retention.default = "not-a-period"
    assert validator._check_retention(table, TableMetadata(table=table)) is None
    cfg.slo_defaults.retention.default = "PT1H"
    validator.connector = FakeConnector(min_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=30))
    assert validator._check_retention(table, TableMetadata(table=table)).severity == "pass"

    result = validator._check_freshness(table, fresh_meta)
    cfg.output.slo_checks_table = "checks"
    cfg.observatory.endpoint = "https://obs"
    cfg.observatory.api_key = "key"
    monkeypatch.setitem(
        sys.modules,
        "lakelogic.core.run_log",
        SimpleNamespace(write_slo_checks=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("write failed"))),
    )
    monkeypatch.setattr(
        validator, "_push_observatory", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("push failed"))
    )
    validator._write_results([result], "check", None, "now")

    posts = []

    class OkResponse:
        status_code = 200
        text = "ok"

    monkeypatch.setitem(
        sys.modules,
        "httpx",
        SimpleNamespace(post=lambda *args, **kwargs: posts.append((args, kwargs)) or OkResponse()),
    )
    ScannerValidator(cfg, FakeConnector(), MemoryBaselineStore())._push_observatory([result], "check", None, "now")
    assert posts[0][1]["headers"]["Authorization"] == "Bearer key"
