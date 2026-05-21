import io
import json
import sys
import types
from pathlib import Path

from lakelogic.core.run_log import (
    _build_cloud_opts,
    _cloud_install_hint,
    _cloud_list_json,
    _cloud_read_json,
    _cloud_write_json,
    _flatten_report,
    _is_cloud_path,
    _prepare_table_name,
    _resolve_path,
    _write_run_log_table,
    _write_slo_checks_table,
    get_last_run_dlt_state,
    get_last_run_watermark,
    write_slo_checks,
    write_run_log,
)
from lakelogic.core.slo import SLOCheckResult


def _make_contract(tmp_path: Path, metadata: dict) -> types.SimpleNamespace:
    return types.SimpleNamespace(metadata=metadata, _base_path=tmp_path)


def _sample_report(run_id: str = "run-1", *, stage: str = "silver", dataset: str = "orders") -> dict:
    return {
        "pipeline_run_id": "pipe-1",
        "run_id": run_id,
        "timestamp": "2026-04-17T12:00:00+00:00",
        "start_time": "2026-04-17T11:59:00+00:00",
        "end_time": "2026-04-17T12:00:00+00:00",
        "run_duration_seconds": 60.0,
        "engine": "polars",
        "contract": "orders_contract",
        "contract_version": "1.0.0",
        "stage": stage,
        "dataset": dataset,
        "domain": "sales",
        "system": "erp",
        "environment": "dev",
        "data_layer": "silver",
        "status": "success",
        "source_path": "landing/orders.csv",
        "counts": {
            "source": 120,
            "total": 120,
            "good": 118,
            "quarantined": 2,
            "quarantine_ratio": 0.0167,
        },
        "estimated_cost": 1.25,
        "cost_currency": "USD",
        "cost_confidence": "high",
        "max_source_mtime": 1713355200.0,
        "max_watermark_value": "2026-04-17T11:58:00+00:00",
        "dlt_state_json": json.dumps({"cursor": "abc123"}),
        "slos": {
            "freshness": {
                "age_seconds": 45,
                "passed": True,
                "threshold_seconds": 300,
                "source_age_seconds": 20,
                "source_passed": True,
            },
            "availability": {
                "ratio": 0.99,
                "passed": True,
                "threshold": 0.95,
            },
        },
        "slo_row_count_min": 10,
        "slo_row_count_max": 1000,
        "slo_row_count_anomaly_pass": True,
        "slo_row_count_anomaly_ratio": 1.02,
        "slo_quality_pass": True,
        "slo_quality_ratio": 0.01,
        "slo_quality_severity": "info",
        "slo_schedule_pass": True,
        "slo_duration_seconds": 60.0,
    }


def test_flatten_report_consolidates_json_fields():
    flattened = _flatten_report(_sample_report())

    assert flattened["counts_good"] == 118
    assert flattened["quarantine_ratio"] == 0.0167
    slo_json = json.loads(flattened["slo_json"])
    assert slo_json["freshness"]["seconds"] == 45.0
    assert slo_json["quality"]["severity"] == "info"
    assert json.loads(flattened["report_json"])["dataset"] == "orders"


def test_path_helpers_cover_relative_sqlite_and_cloud_env(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "acct")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", "secret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")

    assert _resolve_path("logs/run.json", tmp_path) == tmp_path / "logs" / "run.json"
    assert _prepare_table_name("analytics.run_logs", "sqlite") == "analytics_run_logs"
    assert _build_cloud_opts("abfss://container/path/run.json") == {
        "account_name": "acct",
        "account_key": "secret",
    }
    assert _build_cloud_opts("s3://bucket/run.json") == {
        "key": "aws-key",
        "secret": "aws-secret",
    }


def test_cloud_path_detection_and_install_hints():
    assert _is_cloud_path("abfss://container/path/run.json") is True
    assert _is_cloud_path("s3://bucket/run.json") is True
    assert _is_cloud_path("C:/logs/run.json") is False
    assert _cloud_install_hint("abfss://container/path") == "fsspec adlfs"
    assert _cloud_install_hint("s3://bucket/path") == "fsspec s3fs"
    assert _cloud_install_hint("gs://bucket/path") == "fsspec gcsfs"


def test_build_cloud_opts_keeps_embedded_azure_account_and_adds_identity(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "unused-account")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", "secret")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "client-secret")

    opts = _build_cloud_opts("abfss://container@embeddedacct.dfs.core.windows.net/logs/run.json")

    assert "account_name" not in opts
    assert opts["account_key"] == "secret"
    assert opts["tenant_id"] == "tenant"
    assert opts["client_id"] == "client"
    assert opts["client_secret"] == "client-secret"


def test_cloud_helpers_round_trip_and_sort(monkeypatch):
    storage: dict[str, str] = {}
    mtimes = {
        "bucket/logs/run_old.json": 10,
        "bucket/logs/run_new.json": 20,
    }

    class Writer(io.StringIO):
        def __init__(self, path: str):
            super().__init__()
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            storage[self.path] = self.getvalue()
            self.close()
            return False

    class Reader(io.StringIO):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()
            return False

    def fake_open(path: str, mode: str, encoding: str = "utf-8", **kwargs):
        if "w" in mode:
            return Writer(path)
        return Reader(storage[path])

    class FakeFS:
        def glob(self, pattern: str):
            return list(mtimes.keys())

        def info(self, match: str):
            return {"mtime": mtimes[match]}

    fake_fsspec = types.SimpleNamespace(
        open=fake_open,
        core=types.SimpleNamespace(url_to_fs=lambda cloud_dir, **opts: (FakeFS(), "bucket/logs/")),
    )

    monkeypatch.setitem(sys.modules, "fsspec", fake_fsspec)

    _cloud_write_json("s3://bucket/logs/run_new.json", {"run_id": "new"})
    storage["s3://bucket/logs/run_old.json"] = json.dumps({"run_id": "old"})

    assert _cloud_read_json("s3://bucket/logs/run_new.json") == {"run_id": "new"}
    assert _cloud_list_json("s3://bucket/logs") == [
        "s3://bucket/logs/run_new.json",
        "s3://bucket/logs/run_old.json",
    ]


def test_write_run_log_to_directory_and_read_back_watermark(tmp_path: Path):
    contract = _make_contract(tmp_path, {"run_log_dir": "logs"})
    report = _sample_report(run_id="dir-1")

    log_path = write_run_log(report, contract, engine_name="polars", run_log_mode="dir")

    assert log_path is not None
    payload = json.loads(Path(log_path).read_text(encoding="utf-8"))
    assert payload["run_id"] == "dir-1"
    assert get_last_run_watermark(contract, "orders_contract", "silver") == 1713355200.0


def test_write_run_log_secondary_targets_and_observatory_push(monkeypatch, tmp_path: Path):
    secondary_calls = []
    posts = []
    infos = []

    fake_materialization = types.ModuleType("lakelogic.core.materialization")
    fake_materialization.write_to_secondary_targets = lambda targets, table, name, strategy: secondary_calls.append(
        (targets, name, strategy, table)
    )
    monkeypatch.setitem(sys.modules, "lakelogic.core.materialization", fake_materialization)

    class FakeResponse:
        status_code = 201
        text = "created"

    monkeypatch.setitem(
        sys.modules,
        "requests",
        types.SimpleNamespace(post=lambda *args, **kwargs: posts.append((args, kwargs)) or FakeResponse()),
    )
    monkeypatch.setattr("lakelogic.core.run_log.logger.info", infos.append)

    contract = types.SimpleNamespace(
        metadata={"run_log_dir": "logs"},
        _base_path=tmp_path,
        materialization=types.SimpleNamespace(secondary_targets=[{"path": "secondary"}]),
        observatory={
            "enabled": True,
            "endpoint": "https://obs.example/ingest",
            "api_key": "key",
            "emit_on": ["success"],
            "environments": ["dev"],
            "layers": ["silver"],
            "include_quarantine_sample": True,
        },
    )
    report = _sample_report(run_id="obs-1")
    report["status"] = "succeeded"
    report["row_rule_failures"] = [{"id": 1}, {"id": 2}]

    path = write_run_log(report, contract, engine_name="polars", run_log_mode="dir")

    assert Path(path).name == "run_obs-1.json"
    assert secondary_calls[0][0] == [{"path": "secondary"}]
    assert secondary_calls[0][1:3] == ("_run_logs", "append")
    assert posts[0][0][0] == "https://obs.example/ingest"
    payload = posts[0][1]["json"]
    assert payload["status"] == "success"
    assert payload["quality_score"] == round(118 / 120, 6)
    assert payload["quarantined_rows"] == [{"id": 1}, {"id": 2}]
    assert posts[0][1]["headers"]["X-API-Key"] == "key"
    assert any("Ingested" in message for message in infos)


def test_write_run_log_to_cloud_directory_and_read_back_watermark(monkeypatch, tmp_path: Path):
    contract = _make_contract(tmp_path, {"run_log_dir": "s3://bucket/logs"})
    storage: dict[str, dict] = {}

    def fake_cloud_write(path: str, data: dict):
        storage[path] = data

    def fake_cloud_list(cloud_dir: str, pattern: str = "run_*.json"):
        return sorted(storage.keys(), reverse=True)

    def fake_cloud_read(path: str):
        return storage.get(path)

    monkeypatch.setattr("lakelogic.core.run_log._cloud_write_json", fake_cloud_write)
    monkeypatch.setattr("lakelogic.core.run_log._cloud_list_json", fake_cloud_list)
    monkeypatch.setattr("lakelogic.core.run_log._cloud_read_json", fake_cloud_read)

    report = _sample_report(run_id="cloud-1")
    log_path = write_run_log(report, contract, engine_name="polars", run_log_mode="dir")

    assert log_path == "s3://bucket/logs/run_cloud-1.json"
    assert get_last_run_watermark(contract, "orders_contract", "silver") == 1713355200.0


def test_write_run_log_to_sqlite_table_and_fetch_precise_state(tmp_path: Path):
    contract = _make_contract(
        tmp_path,
        {
            "run_log_table": "analytics.run_logs",
            "run_log_backend": "sqlite",
            "run_log_database": "db/run_logs.sqlite",
        },
    )
    report = _sample_report(run_id="sqlite-1")

    result = write_run_log(report, contract, engine_name="polars", run_log_mode="table")

    assert result is None
    assert (tmp_path / "db" / "run_logs.sqlite").exists()
    assert (
        get_last_run_watermark(
            contract,
            "orders_contract",
            "silver",
            dataset="orders",
            data_layer="silver",
        )
        == 1713355200.0
    )
    assert get_last_run_dlt_state(
        contract,
        "orders_contract",
        "silver",
        dataset="orders",
        data_layer="silver",
    ) == json.dumps({"cursor": "abc123"})


def test_write_run_log_to_duckdb_and_ignore_failed_or_reprocess_rows(tmp_path: Path):
    contract = _make_contract(
        tmp_path,
        {
            "run_log_table": "analytics.run_logs",
            "run_log_backend": "duckdb",
            "run_log_database": "db/run_logs.duckdb",
        },
    )

    failed = _sample_report(run_id="duck-failed")
    failed["status"] = "failed"
    failed["max_source_mtime"] = 1.0

    reprocess = _sample_report(run_id="duck-reprocess")
    reprocess["stage"] = "reprocess"
    reprocess["max_source_mtime"] = 2.0

    success = _sample_report(run_id="duck-success")
    success["max_source_mtime"] = 3.0
    success["dlt_state_json"] = json.dumps({"cursor": "latest"})

    write_run_log(failed, contract, engine_name="polars", run_log_mode="table")
    write_run_log(reprocess, contract, engine_name="polars", run_log_mode="table")
    result = write_run_log(success, contract, engine_name="polars", run_log_mode="table")

    assert result is None
    assert (tmp_path / "db" / "run_logs.duckdb").exists()
    assert (
        get_last_run_watermark(
            contract,
            "orders_contract",
            "silver",
            dataset="orders",
            data_layer="silver",
        )
        == 3.0
    )
    assert get_last_run_dlt_state(
        contract,
        "orders_contract",
        "silver",
        dataset="orders",
        data_layer="silver",
    ) == json.dumps({"cursor": "latest"})


def test_write_run_log_ignores_unresolved_template_path(tmp_path: Path):
    contract = _make_contract(tmp_path, {"run_log_dir": "{log_dir}"})

    assert write_run_log(_sample_report(), contract, engine_name="polars", run_log_mode="dir") is None


def test_write_run_log_table_returns_none_for_unsupported_backend(tmp_path: Path):
    contract = _make_contract(tmp_path, {"run_log_table": "analytics.run_logs", "run_log_backend": "custom"})

    assert _write_run_log_table(_sample_report(), contract, engine_name="polars") is None


def test_get_last_run_watermark_skips_invalid_and_non_matching_local_json(tmp_path: Path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "run_bad.json").write_text("{not-json", encoding="utf-8")
    (log_dir / "run_other.json").write_text(
        json.dumps({"contract": "other_contract", "stage": "silver", "max_source_mtime": 2.0}),
        encoding="utf-8",
    )
    (log_dir / "run_match.json").write_text(
        json.dumps({"contract": "orders_contract", "stage": "silver", "max_source_mtime": 7.5}),
        encoding="utf-8",
    )

    contract = _make_contract(tmp_path, {"run_log_dir": "logs"})

    assert get_last_run_watermark(contract, "orders_contract", "silver") == 7.5


def test_get_last_run_watermark_and_dlt_state_from_single_cloud_file(monkeypatch, tmp_path: Path):
    contract = _make_contract(tmp_path, {"run_log_path": "s3://bucket/logs/run.json"})
    payload = {
        "contract": "orders_contract",
        "stage": "silver",
        "max_source_mtime": 9.25,
        "dlt_state_json": json.dumps({"cursor": "cloud-state"}),
    }
    monkeypatch.setattr("lakelogic.core.run_log._cloud_read_json", lambda path: payload)

    assert get_last_run_watermark(contract, "orders_contract", "silver") == 9.25
    assert get_last_run_dlt_state(contract, "orders_contract", "silver") is None


def test_get_last_run_watermark_and_dlt_state_from_delta_backend(monkeypatch, tmp_path: Path):
    class FakeScalar:
        def __init__(self, value):
            self.value = value

        def as_py(self):
            return self.value

    class FakeColumn:
        def __init__(self, values):
            self.values = list(values)

        def __getitem__(self, index):
            return FakeScalar(self.values[index])

    class FakeArrowTable:
        def __init__(self, rows):
            self.rows = list(rows)

        def __len__(self):
            return len(self.rows)

        def column(self, name):
            return FakeColumn([row.get(name) for row in self.rows])

        def filter(self, mask):
            return FakeArrowTable([row for row, keep in zip(self.rows, mask) if keep])

        def take(self, indices):
            return FakeArrowTable([self.rows[index] for index in indices])

    class FakeDeltaTable:
        def __init__(self, path, storage_options=None):
            self.path = path

        def to_pyarrow_table(self, columns=None, filters=None):
            return FakeArrowTable(
                [
                    {
                        "max_source_mtime": 3.0,
                        "dlt_state_json": json.dumps({"cursor": "latest"}),
                        "stage": "silver",
                        "status": "success",
                        "timestamp": "2026-04-17T12:00:00+00:00",
                    },
                    {
                        "max_source_mtime": 1.0,
                        "dlt_state_json": json.dumps({"cursor": "old"}),
                        "stage": "reprocess",
                        "status": "success",
                        "timestamp": "2026-04-16T12:00:00+00:00",
                    },
                    {
                        "max_source_mtime": 2.0,
                        "dlt_state_json": None,
                        "stage": "silver",
                        "status": "failed",
                        "timestamp": "2026-04-15T12:00:00+00:00",
                    },
                ]
            )

    fake_deltalake = types.SimpleNamespace(DeltaTable=FakeDeltaTable)
    monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)

    fake_pc = types.SimpleNamespace(
        not_equal=lambda column, value: [item != value for item in column.values],
        is_valid=lambda column: [item is not None for item in column.values],
        and_=lambda left, right: [a and b for a, b in zip(left, right)],
        max=lambda column: FakeScalar(max(column.values)),
        sort_indices=lambda table, sort_keys=None: sorted(
            range(len(table.rows)), key=lambda idx: table.rows[idx][sort_keys[0][0]], reverse=True
        ),
    )
    monkeypatch.setitem(sys.modules, "pyarrow", types.SimpleNamespace(compute=fake_pc))
    monkeypatch.setitem(sys.modules, "pyarrow.compute", fake_pc)

    contract = _make_contract(tmp_path, {"run_log_table": "abfss://container/logs", "run_log_backend": "delta"})

    assert (
        get_last_run_watermark(
            contract,
            "orders_contract",
            "silver",
            dataset="orders",
            data_layer="silver",
        )
        == 3.0
    )
    assert get_last_run_dlt_state(
        contract,
        "orders_contract",
        "silver",
        dataset="orders",
        data_layer="silver",
    ) == json.dumps({"cursor": "latest"})


def test_write_run_log_table_spark_create_merge_and_append(monkeypatch, tmp_path: Path):
    report = _sample_report(run_id="spark-1")
    contract = _make_contract(
        tmp_path,
        {
            "run_log_table": "catalog.analytics.run_logs",
            "run_log_backend": "spark",
            "run_log_table_partition_by": "domain,missing,data_layer",
            "run_log_table_format": "delta",
        },
    )

    sql_statements = []
    infos = []
    warnings = []
    temp_views = []
    writes = []

    class FakeWriter:
        def __init__(self):
            self.partition_cols = []

        def mode(self, value):
            writes.append(("mode", value))
            return self

        def format(self, value):
            writes.append(("format", value))
            return self

        def partitionBy(self, *cols):
            self.partition_cols = list(cols)
            writes.append(("partitionBy", list(cols)))
            return self

        def saveAsTable(self, table):
            writes.append(("saveAsTable", table, list(self.partition_cols)))

    class FakeDataFrame:
        def __init__(self):
            self.write = FakeWriter()

        def createOrReplaceTempView(self, name):
            temp_views.append(name)

    class FakeSpark:
        def __init__(self):
            self.exists = False
            self.catalog = types.SimpleNamespace(
                tableExists=lambda table: self.exists,
                dropTempView=lambda name: temp_views.append(f"drop::{name}"),
            )

        def sql(self, stmt):
            sql_statements.append(stmt.strip())

        def table(self, table_name):
            return types.SimpleNamespace(columns=["run_id", "domain"])

        def createDataFrame(self, rows, schema=None):
            return FakeDataFrame()

    fake_spark = FakeSpark()
    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.SparkSession = types.SimpleNamespace(builder=types.SimpleNamespace(getOrCreate=lambda: fake_spark))
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)
    fake_types_module = types.ModuleType("pyspark.sql.types")
    fake_types_module.StructType = lambda fields: types.SimpleNamespace(fields=fields)
    fake_types_module.StructField = lambda name, data_type, nullable=True: types.SimpleNamespace(name=name)
    fake_types_module.StringType = lambda: "string"
    fake_types_module.LongType = lambda: "long"
    fake_types_module.DoubleType = lambda: "double"
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", fake_types_module)
    monkeypatch.setattr("lakelogic.core.run_log.logger.info", infos.append)
    monkeypatch.setattr("lakelogic.core.run_log.logger.warning", warnings.append)

    created = _write_run_log_table(report, contract, engine_name="spark")
    assert created == "catalog.analytics.run_logs"
    assert any(stmt.startswith("CREATE SCHEMA IF NOT EXISTS catalog.analytics") for stmt in sql_statements)
    assert ("partitionBy", ["domain", "data_layer"]) in writes
    assert any("unknown columns ['missing']" in message for message in warnings)

    fake_spark.exists = True
    contract.metadata["run_log_merge_on_run_id"] = True
    merged = _write_run_log_table(report, contract, engine_name="spark")
    assert merged == "catalog.analytics.run_logs"
    assert any(stmt.startswith("ALTER TABLE catalog.analytics.run_logs ADD COLUMNS") for stmt in sql_statements)
    assert any("MERGE INTO catalog.analytics.run_logs" in stmt for stmt in sql_statements)
    assert any(item.startswith("drop::lakelogic_run_log_updates_") for item in temp_views)

    fake_spark.exists = True
    contract.metadata["run_log_merge_on_run_id"] = False
    appended = _write_run_log_table(report, contract, engine_name="spark")
    assert appended == "catalog.analytics.run_logs"
    assert any(write[0] == "saveAsTable" and write[1] == "catalog.analytics.run_logs" for write in writes)
    assert any("Wrote run log to Spark table catalog.analytics.run_logs" in message for message in infos)


def test_write_run_log_table_delta_and_iceberg_paths(monkeypatch, tmp_path: Path):
    report = _sample_report(run_id="delta-1")
    infos = []
    warnings = []
    monkeypatch.setattr("lakelogic.core.run_log.logger.info", infos.append)
    monkeypatch.setattr("lakelogic.core.run_log.logger.warning", warnings.append)

    fake_pa = types.SimpleNamespace(
        string=lambda: "string",
        float64=lambda: "float64",
        int64=lambda: "int64",
        schema=lambda fields: [types.SimpleNamespace(name=name, type=data_type) for name, data_type in fields],
        array=lambda values, type=None: {"values": values, "type": type},
        table=lambda arrays, schema=None: types.SimpleNamespace(arrays=list(arrays), schema=schema),
    )
    monkeypatch.setitem(sys.modules, "pyarrow", fake_pa)

    delta_writes = []
    merge_calls = []
    delta_state = {"exists": False, "fail_merge": False}

    class FakeMergeBuilder:
        def when_matched_update_all(self):
            merge_calls.append("matched")
            return self

        def when_not_matched_insert_all(self):
            merge_calls.append("not_matched")
            return self

        def execute(self):
            if delta_state["fail_merge"]:
                raise RuntimeError("merge failed")
            merge_calls.append("executed")

    class FakeDeltaTable:
        def __init__(self, path, storage_options=None):
            if not delta_state["exists"]:
                raise RuntimeError("missing")
            self.path = path

        def merge(self, **kwargs):
            merge_calls.append(kwargs["predicate"])
            return FakeMergeBuilder()

    def fake_write_deltalake(target, data, **kwargs):
        delta_writes.append((target, kwargs))
        delta_state["exists"] = True

    monkeypatch.setitem(
        sys.modules,
        "deltalake",
        types.SimpleNamespace(DeltaTable=FakeDeltaTable, write_deltalake=fake_write_deltalake),
    )

    sleep_calls = []
    random_values = []
    monkeypatch.setitem(sys.modules, "time", types.SimpleNamespace(sleep=lambda value: sleep_calls.append(value)))
    monkeypatch.setitem(
        sys.modules,
        "random",
        types.SimpleNamespace(uniform=lambda low, high: random_values.append((low, high)) or 0.05),
    )

    delta_contract = _make_contract(
        tmp_path,
        {"run_log_table": "abfss://container/logs", "run_log_backend": "delta", "run_log_merge_on_run_id": False},
    )
    created = _write_run_log_table(report, delta_contract, engine_name="polars")
    assert created == "abfss://container/logs"
    assert delta_writes[0][1]["mode"] == "overwrite"

    delta_contract.metadata["run_log_merge_on_run_id"] = True
    merged = _write_run_log_table(report, delta_contract, engine_name="polars")
    assert merged == "abfss://container/logs"
    assert merge_calls[:4] == ["target.run_id = source.run_id", "matched", "not_matched", "executed"]

    delta_state["fail_merge"] = True
    failed = _write_run_log_table(report, delta_contract, engine_name="polars")
    assert failed is None
    assert len(sleep_calls) == 5
    assert random_values and random_values[0] == (0.05, 0.2)
    assert any(
        "Failed to write run log to Delta table abfss://container/logs: merge failed" in message for message in warnings
    )

    unresolved_contract = _make_contract(tmp_path, {"run_log_table": "{log_path}", "run_log_backend": "delta"})
    assert _write_run_log_table(report, unresolved_contract, engine_name="polars") is None

    iceberg_appends = []

    class FakeIcebergTable:
        def append(self, arrow_table):
            iceberg_appends.append(arrow_table)

    class FakeCatalog:
        def __init__(self):
            self.created = []

        def load_table(self, full_id):
            if full_id == "default.run_logs":
                raise RuntimeError("missing")
            return FakeIcebergTable()

        def create_table(self, full_id, schema=None):
            self.created.append((full_id, schema))
            return FakeIcebergTable()

    fake_catalog = FakeCatalog()
    monkeypatch.setitem(
        sys.modules,
        "pyiceberg.catalog",
        types.SimpleNamespace(load_catalog=lambda name, **props: fake_catalog),
    )

    iceberg_contract = _make_contract(tmp_path, {"run_log_table": "run_logs", "run_log_backend": "iceberg"})
    assert _write_run_log_table(report, iceberg_contract, engine_name="polars") == "default.run_logs"
    assert fake_catalog.created[0][0] == "default.run_logs"
    assert iceberg_appends


def test_run_log_write_modes_and_watermark_readers_spark_and_delta(monkeypatch, tmp_path: Path):
    report = _sample_report(run_id="writer-1")
    local_contract = _make_contract(tmp_path, {})
    assert write_run_log({}, local_contract, engine_name="polars") is None
    assert write_run_log(report, None, engine_name="polars") is None

    calls = []
    cloud_contract = _make_contract(
        tmp_path, {"run_log_path": "abfss://container/logs/run.json", "run_log_table": "catalog.logs"}
    )
    monkeypatch.setattr(
        "lakelogic.core.run_log._cloud_write_json", lambda path, data: calls.append(("cloud", path, data["run_id"]))
    )
    monkeypatch.setattr(
        "lakelogic.core.run_log._write_run_log_table",
        lambda data, contract, engine_name=None: calls.append(
            ("table", contract.metadata["run_log_table"], engine_name)
        ),
    )
    assert write_run_log(report, cloud_contract, engine_name="spark", run_log_mode="table") is None
    assert calls == [("table", "catalog.logs", "spark")]
    assert (
        write_run_log(report, cloud_contract, engine_name="spark", run_log_mode="dir")
        == "abfss://container/logs/run.json"
    )
    assert calls[-1] == ("cloud", "abfss://container/logs/run.json", "writer-1")

    warnings = []
    monkeypatch.setattr("lakelogic.core.run_log.logger.warning", warnings.append)
    monkeypatch.setitem(sys.modules, "fsspec", None)
    monkeypatch.setattr(
        "lakelogic.core.run_log._cloud_write_json",
        lambda path, data: (_ for _ in ()).throw(ImportError("missing fsspec")),
    )
    assert write_run_log(report, cloud_contract, engine_name="spark", run_log_mode="dir") is None
    assert any("Install with: pip install fsspec adlfs" in message for message in warnings)

    class FakeCondition:
        def __init__(self, text):
            self.text = text

        def __and__(self, other):
            return FakeCondition(f"({self.text} AND {other.text})")

    class FakeColumn:
        def __init__(self, name):
            self.name = name

        def __eq__(self, other):
            return FakeCondition(f"{self.name} == {other}")

        def __ne__(self, other):
            return FakeCondition(f"{self.name} != {other}")

        def isNotNull(self):
            return FakeCondition(f"{self.name} IS NOT NULL")

        def desc(self):
            return f"{self.name} DESC"

        def alias(self, name):
            return name

    class FakeSparkFrame:
        def filter(self, expr):
            return self

        def agg(self, expr):
            return types.SimpleNamespace(collect=lambda: [{"max_mtime": 12.5}])

        def orderBy(self, expr):
            return self

        def limit(self, count):
            return self

        def collect(self):
            return [{"dlt_state_json": json.dumps({"cursor": "spark"})}]

    fake_functions = types.SimpleNamespace(col=lambda name: FakeColumn(name), max=lambda expr: expr)
    fake_spark = types.SimpleNamespace(table=lambda name: FakeSparkFrame())
    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.SparkSession = types.SimpleNamespace(builder=types.SimpleNamespace(getOrCreate=lambda: fake_spark))
    fake_sql_module.functions = fake_functions
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_functions)

    spark_contract = _make_contract(tmp_path, {"run_log_table": "catalog.logs", "run_log_backend": "spark"})
    assert (
        get_last_run_watermark(
            spark_contract, "orders_contract", "silver", engine_name="spark", dataset="orders", data_layer="silver"
        )
        == 12.5
    )
    assert get_last_run_dlt_state(
        spark_contract, "orders_contract", "silver", engine_name="spark", dataset="orders", data_layer="silver"
    ) == json.dumps({"cursor": "spark"})

    class FakeScalar:
        def __init__(self, value):
            self.value = value

        def as_py(self):
            return self.value

    class FakeColumnValues:
        def __init__(self, values):
            self.values = list(values)

        def __getitem__(self, index):
            return FakeScalar(self.values[index])

    class FakeArrowTable:
        def __init__(self, rows):
            self.rows = list(rows)

        def __len__(self):
            return len(self.rows)

        def column(self, name):
            return FakeColumnValues([row.get(name) for row in self.rows])

        def filter(self, mask):
            return FakeArrowTable([row for row, keep in zip(self.rows, mask) if keep])

        def take(self, indices):
            return FakeArrowTable([self.rows[index] for index in indices])

    class FakeDeltaReadTable:
        def __init__(self, path, storage_options=None):
            self.path = path

        def to_pyarrow_table(self, columns=None, filters=None):
            return FakeArrowTable(
                [
                    {
                        "dlt_state_json": None,
                        "stage": "silver",
                        "status": "success",
                        "timestamp": "2026-04-15T00:00:00+00:00",
                    },
                    {
                        "dlt_state_json": json.dumps({"cursor": "delta"}),
                        "stage": "silver",
                        "status": "success",
                        "timestamp": "2026-04-16T00:00:00+00:00",
                    },
                    {
                        "dlt_state_json": json.dumps({"cursor": "old"}),
                        "stage": "reprocess",
                        "status": "success",
                        "timestamp": "2026-04-14T00:00:00+00:00",
                    },
                ]
            )

    fake_pc = types.SimpleNamespace(
        not_equal=lambda column, value: [item != value for item in column.values],
        is_valid=lambda column: [item is not None for item in column.values],
        and_=lambda left, right: [a and b for a, b in zip(left, right)],
        max=lambda column: FakeScalar(max(value for value in column.values if value is not None)),
        sort_indices=lambda table, sort_keys=None: sorted(
            range(len(table.rows)), key=lambda idx: table.rows[idx][sort_keys[0][0]], reverse=True
        ),
    )
    monkeypatch.setitem(sys.modules, "deltalake", types.SimpleNamespace(DeltaTable=FakeDeltaReadTable))
    monkeypatch.setitem(sys.modules, "pyarrow", types.SimpleNamespace(compute=fake_pc))
    monkeypatch.setitem(sys.modules, "pyarrow.compute", fake_pc)

    delta_contract = _make_contract(tmp_path, {"run_log_table": "abfss://container/logs", "run_log_backend": "delta"})
    assert get_last_run_dlt_state(
        delta_contract, "orders_contract", "silver", dataset="orders", data_layer="silver"
    ) == json.dumps({"cursor": "delta"})


def _sample_slo_result() -> SLOCheckResult:
    return SLOCheckResult(
        layer="silver",
        entity="orders",
        check_type="freshness",
        status="OK",
        passed=True,
        severity="pass",
        delay_minutes=3.5,
        slo_max_minutes=60,
        row_count=100,
        anomaly_ratio=1.1,
        anomaly_baseline=95.0,
        quality_ratio=0.01,
        quality_severity="info",
        duration_seconds=2.0,
    )


def test_write_slo_checks_duckdb_sqlite_and_empty_paths(tmp_path: Path):
    result = _sample_slo_result()

    assert write_slo_checks(types.SimpleNamespace(domain="d", system="s"), [], "check-1") is None
    assert _write_slo_checks_table(types.SimpleNamespace(storage=None, metadata={}), []) is None
    assert _write_slo_checks_table(types.SimpleNamespace(storage=None, metadata={}), [{"check_run_id": "x"}]) is None

    duck_db = tmp_path / "slo.duckdb"
    duck_registry = types.SimpleNamespace(
        domain="sales",
        system="erp",
        storage=types.SimpleNamespace(slo_checks_table="analytics.slo_checks"),
        metadata={"slo_checks_backend": "duckdb", "slo_checks_database": str(duck_db)},
    )
    duck_target = write_slo_checks(duck_registry, [result], "check-1", "pipe-1")
    assert duck_target == f"{duck_db}:analytics.slo_checks"

    import duckdb

    con = duckdb.connect(str(duck_db))
    assert con.execute("SELECT check_run_id, passed FROM analytics.slo_checks").fetchone() == ("check-1", True)
    con.close()

    sqlite_db = tmp_path / "slo.sqlite"
    sqlite_registry = types.SimpleNamespace(
        domain="sales",
        system="erp",
        storage=types.SimpleNamespace(slo_checks_table="analytics.slo_checks"),
        metadata={"slo_checks_backend": "sqlite", "slo_checks_database": str(sqlite_db)},
    )
    sqlite_target = write_slo_checks(sqlite_registry, [result], "check-2")
    assert sqlite_target == f"{sqlite_db}:analytics_slo_checks"

    import sqlite3

    con = sqlite3.connect(str(sqlite_db))
    assert con.execute("SELECT check_run_id, passed FROM analytics_slo_checks").fetchone() == ("check-2", 1)
    con.close()


def test_write_slo_checks_spark_and_delta_backends(monkeypatch, tmp_path: Path):
    records = [
        {
            "check_run_id": "check-1",
            "pipeline_run_id": None,
            "checked_at": "now",
            "domain": "sales",
            "system": "erp",
            "layer": "silver",
            "entity": "orders",
            "check_type": "freshness",
            "passed": True,
            "severity": "pass",
            "status": "OK",
        }
    ]

    writes = []

    class FakeWriter:
        def mode(self, value):
            writes.append(("mode", value))
            return self

        def format(self, value):
            writes.append(("format", value))
            return self

        def saveAsTable(self, table):
            writes.append(("saveAsTable", table))

    class FakeSpark:
        def __init__(self):
            self.catalog = types.SimpleNamespace(tableExists=lambda table: False)

        def createDataFrame(self, rows, schema=None):
            assert rows == records
            return types.SimpleNamespace(write=FakeWriter())

    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.SparkSession = types.SimpleNamespace(builder=types.SimpleNamespace(getOrCreate=lambda: FakeSpark()))
    fake_types_module = types.ModuleType("pyspark.sql.types")
    fake_types_module.StructType = lambda fields: fields
    fake_types_module.StructField = lambda name, data_type, nullable=True: (name, data_type, nullable)
    fake_types_module.StringType = lambda: "string"
    fake_types_module.BooleanType = lambda: "bool"
    fake_types_module.DoubleType = lambda: "double"
    fake_types_module.LongType = lambda: "long"
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", fake_types_module)

    spark_registry = types.SimpleNamespace(
        storage=types.SimpleNamespace(slo_checks_table="catalog.slo_checks"),
        metadata={"slo_checks_backend": "spark"},
    )
    assert _write_slo_checks_table(spark_registry, records) == "catalog.slo_checks"
    assert ("mode", "overwrite") in writes
    assert ("saveAsTable", "catalog.slo_checks") in writes

    delta_writes = []
    exists = {"value": False}

    class FakeDeltaTable:
        def __init__(self, table_name, storage_options=None):
            if not exists["value"]:
                raise RuntimeError("missing")

    def fake_write_deltalake(table_name, arrow_table, **kwargs):
        delta_writes.append((table_name, kwargs))
        exists["value"] = True

    monkeypatch.setitem(
        sys.modules,
        "deltalake",
        types.SimpleNamespace(DeltaTable=FakeDeltaTable, write_deltalake=fake_write_deltalake),
    )

    delta_registry = types.SimpleNamespace(
        storage=types.SimpleNamespace(slo_checks_table=str(tmp_path / "slo_delta")),
        metadata={"slo_checks_backend": "delta"},
    )
    delta_target = str(tmp_path / "slo_delta").replace("\\", "/")
    assert _write_slo_checks_table(delta_registry, records) == delta_target
    assert delta_writes[0][1]["mode"] == "overwrite"
    assert _write_slo_checks_table(delta_registry, records) == delta_target
    assert delta_writes[-1][1]["mode"] == "append"

    unresolved = types.SimpleNamespace(
        storage=types.SimpleNamespace(slo_checks_table="{missing}"),
        metadata={"slo_checks_backend": "delta"},
    )
    assert _write_slo_checks_table(unresolved, records) is None


def test_write_run_log_table_dlt_backend(monkeypatch, tmp_path: Path):
    resource_calls = []
    pipeline_calls = []

    class FakePipeline:
        def run(self, resource):
            pipeline_calls.append(resource)

    class FakeDestinations:
        def __init__(self):
            self.__dict__["duckdb"] = lambda **kwargs: ("duckdb-dest", kwargs)

    fake_dlt = types.SimpleNamespace(
        destinations=FakeDestinations(),
        resource=lambda name, write_disposition: lambda fn: resource_calls.append((name, write_disposition)) or fn,
        pipeline=lambda **kwargs: pipeline_calls.append(kwargs) or FakePipeline(),
    )
    monkeypatch.setitem(sys.modules, "dlt", fake_dlt)

    contract = _make_contract(
        tmp_path,
        {
            "run_log_table": "run_logs",
            "run_log_backend": "dlt",
            "dlt_destination": "duckdb",
            "dlt_dataset_name": "lake",
            "dlt_credentials": "secret",
            "dlt_extra_option": "x",
        },
    )

    target = _write_run_log_table(_sample_report(), contract, engine_name="polars")

    assert target == "duckdb:lake.run_logs"
    assert resource_calls == [("run_logs", "append")]
    assert pipeline_calls[0]["destination"] == ("duckdb-dest", {"credentials": "secret", "extra_option": "x"})
