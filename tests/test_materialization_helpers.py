from __future__ import annotations

import os
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest

pl = pytest.importorskip("polars")
pd = pytest.importorskip("pandas")

from lakelogic.core import materialization as mat


def test_uri_path_and_basic_path_helpers(monkeypatch, tmp_path):
    uri = mat.URIPath("abfss://lake/root/orders")
    assert str(uri / "2024" / "03") == "abfss://lake/root/orders/2024/03"
    assert uri.suffix == ""
    assert (uri / "file.parquet").suffix == ".parquet"
    assert str((uri / "file.parquet").parent) == "abfss://lake/root/orders"
    assert (uri / "file.parquet").name == "file.parquet"
    assert str((uri / "file").with_suffix(".csv")) == "abfss://lake/root/orders/file.csv"
    assert mat._safe_partition_value("2024/03 10:00") == "2024_03_10_00"
    assert mat._safe_partition_value(None) == "null"
    assert mat._resolve_path("data/out", tmp_path) == tmp_path / "data" / "out"

    monkeypatch.setattr("lakelogic.core.paths.is_uri_path", lambda path: path.startswith("abfss://"))
    assert mat._is_remote_path("abfss://lake/root") is True
    assert mat._is_remote_path("local/file") is False


def test_storage_option_and_env_resolution(monkeypatch):
    assert mat._build_storage_options({"existing": "x"}) == {"existing": "x"}

    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "acct")
    monkeypatch.setenv("AZURE_CLIENT_ID", "cid")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    assert mat._build_storage_options() == {
        "client_id": "cid",
        "client_secret": "secret",
        "tenant_id": "tenant",
        "account_name": "acct",
    }
    monkeypatch.delenv("AZURE_CLIENT_ID")
    monkeypatch.delenv("AZURE_CLIENT_SECRET")
    monkeypatch.delenv("AZURE_TENANT_ID")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", "key")
    assert mat._build_storage_options() == {"account_key": "key", "account_name": "acct"}

    monkeypatch.setenv("TEST_VALUE", "resolved")
    assert mat._resolve_env_value("env:TEST_VALUE") == "resolved"
    assert mat._resolve_env_value("${ENV:TEST_VALUE}") == "resolved"
    assert mat._resolve_env_value("plain") == "plain"
    assert mat._resolve_env_value(None) is None
    assert mat._resolve_external_location("env:TEST_VALUE") == "resolved"
    assert mat._resolve_external_location(None) is None


def test_frame_has_columns_uses_collect_schema_and_sequence_fallbacks():
    class SchemaOnlyFrame:
        def collect_schema(self):
            return {"id": "int64"}

    class BadLength:
        def __len__(self):
            raise RuntimeError("bad length")

    class BrokenFrame:
        columns = BadLength()
        schema = BadLength()

        def collect_schema(self):
            return {}

    assert mat._frame_has_columns(SchemaOnlyFrame()) is True
    assert mat._frame_has_columns(BrokenFrame()) is False
    assert mat._frame_has_columns([{}]) is False
    assert mat._frame_has_columns([(1, 2)]) is True


def test_resolve_target_and_to_pandas(monkeypatch, tmp_path):
    contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(target_path="out/orders", path=None, format="csv"),
        _base_path=tmp_path,
        effective_server=lambda: types.SimpleNamespace(path="server/path", format="parquet"),
    )
    target, output_format = mat._resolve_target(contract)
    assert target == tmp_path / "out" / "orders"
    assert output_format == "csv"

    remote_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(target_path="abfss://lake/orders", path=None, format=None),
        _base_path=tmp_path,
        effective_server=lambda: types.SimpleNamespace(path=None, format="json"),
    )
    monkeypatch.setattr(mat, "_is_remote_path", lambda path: str(path).startswith("abfss://"))
    remote_target, remote_format = mat._resolve_target(remote_contract)
    assert isinstance(remote_target, mat.URIPath)
    assert remote_format == "json"

    override_target, override_format = mat._resolve_target(remote_contract, override_path=tmp_path / "override")
    assert override_target == tmp_path / "override"
    assert override_format == "json"
    assert mat._resolve_target(types.SimpleNamespace(materialization=None, effective_server=lambda: None), None) == (None, None)

    pandas_df = pd.DataFrame({"id": [1]})
    polars_df = pl.DataFrame({"id": [1]})
    spark_df = types.SimpleNamespace(toPandas=lambda: pandas_df)
    collected = types.SimpleNamespace(to_pandas=lambda: pandas_df)
    lazy_like = types.SimpleNamespace(collect=lambda: collected)
    duck_like = types.SimpleNamespace(df=lambda: pandas_df)
    assert mat._to_pandas(pandas_df).equals(pandas_df)
    assert mat._to_pandas(polars_df).equals(pandas_df)
    assert mat._to_pandas(spark_df).equals(pandas_df)
    assert mat._to_pandas(lazy_like).equals(pandas_df)
    assert mat._to_pandas(duck_like).equals(pandas_df)
    assert mat._to_pandas([{"id": 1}]).equals(pandas_df)
    assert mat._to_pandas({"id": 1}).equals(pandas_df)
    with pytest.raises(TypeError, match="Unsupported dataframe type"):
        mat._to_pandas(object())


def test_safe_write_deltalake_wrapper_and_frame_io(monkeypatch, tmp_path):
    write_calls = []

    def fake_write_deltalake(path, data, **kwargs):
        write_calls.append((path, kwargs.copy(), len(data) if hasattr(data, "__len__") else None))
        return "ok"

    fake_deltalake = types.ModuleType("deltalake")
    fake_deltalake.write_deltalake = fake_write_deltalake
    monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)

    assert mat._safe_write_deltalake("/tmp/path", [], schema_mode="merge", engine="python") == "ok"
    assert "schema_mode" not in write_calls[0][1]
    assert "engine" not in write_calls[0][1]

    df = pl.DataFrame({"id": [1], "name": ["Alice"]})
    csv_path = tmp_path / "frame.csv"
    parquet_path = tmp_path / "frame.parquet"
    mat._write_frame(df, csv_path, "csv")
    mat._write_frame(df, parquet_path, "parquet")
    assert csv_path.exists()
    assert parquet_path.exists()
    assert mat._read_frame(csv_path, "csv").shape == (1, 2)
    assert mat._read_frame(parquet_path, "parquet").shape == (1, 2)

    assert mat._pandas_available() is True
    assert mat._row_count(df) == 1
    assert mat._row_count(pd.DataFrame({"id": [1, 2]})) == 2
    assert mat._row_count([1, 2, 3]) == 3
    assert mat._is_polars_frame(df) is True
    assert mat._is_polars_frame(pd.DataFrame({"id": [1]})) is False
    assert mat._frame_has_columns(df) is True
    assert mat._frame_has_columns(pd.DataFrame()) is False

    append_csv = tmp_path / "append.csv"
    append_parquet = tmp_path / "append.parquet"
    mat._write_frame(df, append_csv, "csv")
    mat._write_frame(df, append_parquet, "parquet")
    assert mat._append_without_pandas(pl.DataFrame({"id": [2], "name": ["Bob"]}), append_csv, "csv") == 2
    assert mat._append_without_pandas(pl.DataFrame({"id": [2], "name": ["Bob"]}), append_parquet, "parquet") == 2


def test_safe_write_deltalake_handles_type_conflicts_and_schema_mismatch(monkeypatch):
    pa = pytest.importorskip("pyarrow")
    incoming = pa.table({"id": ["1"], "value": [1]})

    class FakeSchema:
        def get_field_index(self, name):
            return 0 if name == "id" else 1

        def field(self, index):
            fields = [pa.field("id", pa.int64()), pa.field("value", pa.int64())]
            return fields[index]

        def __iter__(self):
            return iter([pa.field("id", pa.int64()), pa.field("value", pa.int64()), pa.field("extra", pa.string())])

    fake_dt = object()
    monkeypatch.setattr(mat, "_get_pyarrow_schema", lambda dt: FakeSchema())

    calls = []

    def fake_write(path, data, **kwargs):
        calls.append((path, data.schema, kwargs.copy()))
        if len(calls) == 1:
            raise RuntimeError("Cannot merge types string and int64")
        return "ok"

    fake_deltalake = types.ModuleType("deltalake")
    fake_deltalake.write_deltalake = fake_write
    fake_deltalake.DeltaTable = lambda path, storage_options=None: fake_dt
    monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)

    assert mat._safe_write_deltalake("target", incoming) == "ok"
    assert calls[1][1].field(0).type == pa.int64()

    def schema_mismatch_write(path, data, **kwargs):
        raise RuntimeError("Schema mismatch: incoming schema does not match")

    fake_deltalake.write_deltalake = schema_mismatch_write
    with pytest.raises(ValueError, match="Schema Evolution Blocked"):
        mat._safe_write_deltalake("target", incoming)


def test_write_frame_delta_duckdb_and_iceberg_branches(monkeypatch, tmp_path):
    pa = pytest.importorskip("pyarrow")
    pdf = pd.DataFrame({"id": [1], "name": ["Alice"]})

    delta_calls = []
    fake_deltalake = types.ModuleType("deltalake")
    fake_deltalake.write_deltalake = lambda path, data, **kwargs: delta_calls.append((path, kwargs.copy(), data.num_rows))
    monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)
    monkeypatch.setattr(mat, "_sanitize_arrow_nulls", lambda table: table)
    mat._write_frame(pdf, tmp_path / "frame.delta", "delta")
    assert delta_calls[0][1]["mode"] == "overwrite"
    assert delta_calls[0][1]["schema_mode"] == "overwrite"

    duck_calls = []

    class FakeDuckConnection:
        def execute(self, stmt):
            duck_calls.append(stmt)
            return self

        def register(self, name, frame):
            duck_calls.append((name, list(frame.columns)))

        def close(self):
            duck_calls.append("closed")

    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(connect=lambda: FakeDuckConnection()))
    mat._write_frame(pdf, tmp_path / "frame.duckdb", "duckdb")
    assert any(isinstance(item, str) and item.startswith("ATTACH '") for item in duck_calls)
    assert any("CREATE OR REPLACE TABLE target_db.frame AS SELECT * FROM incoming_df" in item for item in duck_calls if isinstance(item, str))

    iceberg_calls = []

    class FakeIcebergConnection(FakeDuckConnection):
        def execute(self, stmt):
            iceberg_calls.append(stmt)
            return self

        def register(self, name, frame):
            iceberg_calls.append((name, list(frame.columns)))

    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(connect=lambda: FakeIcebergConnection()))
    mat._write_frame(pdf, tmp_path / "table_path", "iceberg")
    assert "INSTALL iceberg; LOAD iceberg;" in iceberg_calls
    assert any("COPY incoming_df TO" in item for item in iceberg_calls if isinstance(item, str))


def test_write_frame_parquet_falls_back_to_duckdb(monkeypatch, tmp_path):
    pdf = pd.DataFrame({"id": [1], "name": ["Alice"]})
    commands = []

    class FakeConnection:
        def register(self, name, frame):
            commands.append(("register", name, list(frame.columns)))

        def execute(self, stmt):
            commands.append(("execute", stmt))

        def close(self):
            commands.append(("close",))

    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda self, path, index=False: (_ for _ in ()).throw(RuntimeError("missing parquet engine")))
    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(connect=lambda: FakeConnection()))

    mat._write_frame(pdf, tmp_path / "fallback.parquet", "parquet")

    assert commands[0] == ("register", "incoming_df", ["id", "name"])
    assert any(command[0] == "execute" and "COPY incoming_df TO" in command[1] for command in commands)
    assert commands[-1] == ("close",)


def test_get_pyarrow_schema_prefers_native_then_falls_back(monkeypatch):
    class NativeSchema:
        def field(self, index):
            return types.SimpleNamespace(type=types.SimpleNamespace(__module__="pyarrow.types"))

    native_raw = types.SimpleNamespace(to_pyarrow=lambda: NativeSchema())
    native_dt = types.SimpleNamespace(schema=lambda: native_raw)
    assert isinstance(mat._get_pyarrow_schema(native_dt), NativeSchema)

    import pyarrow as pa

    fallback_dt = types.SimpleNamespace(schema=lambda: object())
    monkeypatch.setattr(pa, "schema", lambda raw: "coerced-schema")
    assert mat._get_pyarrow_schema(fallback_dt) == "coerced-schema"

    dataset_dt = types.SimpleNamespace(
        schema=lambda: object(),
        to_pyarrow_dataset=lambda: types.SimpleNamespace(schema="dataset-schema"),
    )

    def raise_type_error(raw):
        raise TypeError("bad capsule")

    monkeypatch.setattr(pa, "schema", raise_type_error)
    assert mat._get_pyarrow_schema(dataset_dt) == "dataset-schema"


def test_materialize_dataframe_dispatch_and_guard_branches(monkeypatch, tmp_path):
    no_mat_contract = types.SimpleNamespace(materialization=None)
    assert mat.materialize_dataframe(pl.DataFrame({"id": [1]}), no_mat_contract) == {}

    warning_messages = []
    monkeypatch.setattr(mat.logger, "warning", warning_messages.append)
    monkeypatch.setattr(mat, "_resolve_target", lambda contract, override_path=None: (None, None))
    contract = types.SimpleNamespace(materialization=types.SimpleNamespace(strategy="append"))
    assert mat.materialize_dataframe(pl.DataFrame({"id": [1]}), contract) == {}
    assert any("target path or format could not be resolved" in message for message in warning_messages)

    fallback_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="append", partition_by=[], scd2=None, location="abfss://lake/orders"),
        primary_key=[],
        effective_server=lambda: None,
        metadata={},
    )
    monkeypatch.setattr(mat, "_resolve_target", lambda contract, override_path=None: (mat.URIPath("table:catalog.schema.orders"), "parquet"))
    monkeypatch.setattr(mat, "_frame_has_columns", lambda frame: False)
    empty_write = mat.materialize_dataframe(pl.DataFrame(schema={"id": pl.Int64}), fallback_contract, engine_name="polars")
    assert empty_write == {
        "target": "abfss://lake/orders/data.parquet",
        "rows_written": 0,
        "format": "parquet",
    }

    unsupported_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="append", partition_by=[], scd2=None, location=None),
        primary_key=[],
        effective_server=lambda: None,
        metadata={},
    )
    assert mat.materialize_dataframe(pl.DataFrame({"id": [1]}), unsupported_contract, engine_name="polars") == {}


def test_materialize_dataframe_dispatches_to_partition_and_spark_paths(monkeypatch, tmp_path):
    partition_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="merge", partition_by=["event_date"], scd2=None),
        primary_key=["id"],
        effective_server=lambda: None,
        metadata={},
    )
    monkeypatch.setattr(mat, "_resolve_target", lambda contract, override_path=None: (tmp_path / "events", "parquet"))
    monkeypatch.setattr(mat, "_partition_aware_merge", lambda *args, **kwargs: {"target": "partitioned", "rows_written": 3, "format": "parquet"})
    partitioned = mat.materialize_dataframe(pl.DataFrame({"id": [1], "event_date": ["2024-01-01"]}), partition_contract)
    assert partitioned == {"target": "partitioned", "rows_written": 3, "format": "parquet"}

    spark_calls = []
    monkeypatch.setattr(mat, "_materialize_spark_dataframe", lambda *args, **kwargs: spark_calls.append((args, kwargs)) or {"target": "spark", "rows_written": 1, "format": "delta"})
    spark_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="append", partition_by=[]),
        primary_key=[],
        effective_server=lambda: None,
        metadata={},
    )
    spark_df = types.SimpleNamespace(sparkSession=object(), write=object())
    spark_result = mat.materialize_dataframe(spark_df, spark_contract, engine_name="spark")
    assert spark_result == {"target": "spark", "rows_written": 1, "format": "delta"}
    assert spark_calls[0][0][2] == tmp_path / "events"


def test_materialize_dataframe_handles_no_pandas_and_native_polars_paths(monkeypatch, tmp_path):
    target = tmp_path / "orders.csv"
    contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="append", partition_by=[], scd2=None),
        primary_key=[],
        effective_server=lambda: None,
        metadata={},
    )
    monkeypatch.setattr(mat, "_resolve_target", lambda contract, override_path=None: (target, "csv"))
    monkeypatch.setattr(mat, "_frame_has_columns", lambda frame: True)
    monkeypatch.setattr(mat, "_pandas_available", lambda: False)
    monkeypatch.setattr(mat, "_append_without_pandas", lambda frame, path, fmt: 4)
    monkeypatch.setattr(Path, "exists", lambda self: str(self) == str(target), raising=False)
    result = mat.materialize_dataframe(pl.DataFrame({"id": [1]}), contract)
    assert result == {"target": str(target), "rows_written": 4, "format": "csv"}

    partition_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="append", partition_by=["event_date"], scd2=None),
        primary_key=[],
        effective_server=lambda: None,
        metadata={},
    )
    with pytest.raises(ValueError, match="Partitioned materialization requires pandas"):
        mat.materialize_dataframe(pl.DataFrame({"id": [1], "event_date": ["2024-01-01"]}), partition_contract)

    monkeypatch.setattr(mat, "_pandas_available", lambda: True)
    monkeypatch.setattr(mat, "_is_polars_frame", lambda frame: True)
    monkeypatch.setattr(mat, "_write_frame", lambda frame, path, fmt: None)
    monkeypatch.setattr(mat, "_row_count", lambda frame: None)
    monkeypatch.setattr(mat, "_resolve_target", lambda contract, override_path=None: (tmp_path / "orders_native", "csv"))
    collected = types.SimpleNamespace(height=2)
    lazy_df = types.SimpleNamespace(collect=lambda: collected)
    native_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="overwrite", partition_by=[], scd2=None),
        primary_key=[],
        effective_server=lambda: None,
        metadata={},
    )
    native = mat.materialize_dataframe(lazy_df, native_contract, output_format="parquet")
    assert native == {"target": str(tmp_path / "orders_native" / "data.parquet"), "rows_written": 2, "format": "parquet"}


def test_materialize_spark_dataframe_merge_scd2_and_table_writes(monkeypatch, tmp_path):
    class FakeWriter:
        def __init__(self):
            self.formats = []
            self.partitioned = []
            self.options = []
            self.modes = []
            self.saved = []

        def format(self, value):
            self.formats.append(value)
            return self

        def partitionBy(self, *columns):
            self.partitioned.append(columns)
            return self

        def option(self, key, value):
            self.options.append((key, value))
            return self

        def mode(self, value):
            self.modes.append(value)
            return self

        def save(self, path):
            self.saved.append(path)

    class FakeSpark:
        def __init__(self):
            self.conf = types.SimpleNamespace(set=lambda key, value: config_calls.append((key, value)))
            self.table_calls = []

        def table(self, name):
            self.table_calls.append(name)
            if name == "catalog.schema.orders":
                raise RuntimeError("missing")
            return object()

    config_calls = []
    metadata_calls = []
    version_calls = []
    save_table_calls = []
    merge_calls = []
    scd2_calls = []
    monkeypatch.setattr(mat, "_spark_apply_table_metadata", lambda spark, table_name, contract: metadata_calls.append(table_name))
    monkeypatch.setattr(mat, "_spark_update_incremental_version", lambda spark, table_name, version: version_calls.append((table_name, version)))
    monkeypatch.setattr(mat, "_spark_save_as_table", lambda writer, table_name, mode, location=None: save_table_calls.append((table_name, mode, location)))
    monkeypatch.setattr(mat, "_spark_merge_dataframe", lambda *args, **kwargs: merge_calls.append(kwargs) or {"target": str(args[2]), "rows_written": 2, "format": args[4]})
    monkeypatch.setattr(mat, "_spark_scd2_dataframe", lambda *args, **kwargs: scd2_calls.append(kwargs) or {"target": str(args[2]), "rows_written": 3, "format": args[5]})

    def _contract(strategy, **materialization_overrides):
        materialization = {
            "strategy": strategy,
            "partition_by": ["event_date", "missing_col"],
            "scd2": {"track_columns": ["status"]},
            "location": "env:TARGET_LOC",
            "soft_delete_column": "is_deleted",
            "soft_delete_value": True,
            "soft_delete_time_column": "deleted_at",
            "soft_delete_reason_column": "reason",
            "merge_dedup_guard": True,
            "unknown_member": {"enabled": True},
        }
        materialization.update(materialization_overrides)
        return types.SimpleNamespace(
            materialization=types.SimpleNamespace(**materialization),
            primary_key=["id"],
            source=types.SimpleNamespace(cdc_op_field="op", cdc_delete_values=["D"], cdc_timestamp_field="event_ts"),
            effective_server=lambda: types.SimpleNamespace(schema_policy=types.SimpleNamespace(evolution="merge")),
        )

    merge_df = types.SimpleNamespace(sparkSession=FakeSpark(), write=FakeWriter(), columns=["id", "event_date"], count=lambda: 2)
    merge_result = mat._materialize_spark_dataframe(
        merge_df,
        _contract("merge"),
        mat.URIPath("table:catalog.schema.orders"),
        "delta",
        incremental_metadata={"strategy": "delta_version", "to_version": 7},
    )
    assert merge_result == {"target": "table:catalog.schema.orders", "rows_written": 2, "format": "delta"}
    assert merge_calls[0]["merge_dedup_guard"] is True
    assert metadata_calls[0] == "catalog.schema.orders"
    assert version_calls[0] == ("catalog.schema.orders", 7)

    scd2_df = types.SimpleNamespace(sparkSession=FakeSpark(), write=FakeWriter(), columns=["id", "event_date"], count=lambda: 3)
    scd2_result = mat._materialize_spark_dataframe(
        scd2_df,
        _contract("scd2"),
        mat.URIPath("table:catalog.schema.dimension_orders"),
        "delta",
        incremental_metadata={"strategy": "delta_version", "to_version": 9},
    )
    assert scd2_result == {"target": "table:catalog.schema.dimension_orders", "rows_written": 3, "format": "delta"}
    assert scd2_calls[0]["merge_dedup_guard"] is True
    assert metadata_calls[1] == "catalog.schema.dimension_orders"
    assert version_calls[1] == ("catalog.schema.dimension_orders", 9)

    append_df = types.SimpleNamespace(sparkSession=FakeSpark(), write=FakeWriter(), columns=["id", "event_date"], count=lambda: 5)
    append_result = mat._materialize_spark_dataframe(
        append_df,
        _contract("append", location=None),
        mat.URIPath("table:catalog.schema.append_orders"),
        "delta",
        is_reprocess=True,
    )
    assert append_result == {"target": "catalog.schema.append_orders", "rows_written": 5, "format": "delta"}
    assert append_df.write.partitioned == [("event_date",)]
    assert ("mergeSchema", "true") in append_df.write.options
    assert save_table_calls[0] == ("catalog.schema.append_orders", "overwrite", None)
    assert config_calls == [("spark.sql.sources.partitionOverwriteMode", "dynamic")]

    path_df = types.SimpleNamespace(sparkSession=FakeSpark(), write=FakeWriter(), columns=["id"], count=lambda: 1)
    path_result = mat._materialize_spark_dataframe(
        path_df,
        _contract("overwrite", partition_by=[], location=None),
        tmp_path / "spark-out",
        "parquet",
    )
    assert path_result == {"target": str(tmp_path / "spark-out"), "rows_written": 1, "format": "parquet"}
    assert path_df.write.saved == [str(tmp_path / "spark-out")]


def test_materialize_spark_dataframe_requires_primary_key_for_merge_and_scd2():
    spark_df = types.SimpleNamespace(sparkSession=object(), write=types.SimpleNamespace(format=lambda fmt: None), columns=[], count=lambda: 0)
    merge_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="merge", partition_by=[], scd2=None, location=None),
        primary_key=[],
        source=None,
        effective_server=lambda: None,
    )
    with pytest.raises(ValueError, match="primary_key is required for merge strategy"):
        mat._materialize_spark_dataframe(spark_df, merge_contract, Path("out"), "delta")

    scd2_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="scd2", partition_by=[], scd2=None, location=None),
        primary_key=[],
        source=None,
        effective_server=lambda: None,
    )
    with pytest.raises(ValueError, match="primary_key is required for scd2 strategy"):
        mat._materialize_spark_dataframe(spark_df, scd2_contract, Path("out"), "delta")


def test_partition_aware_merge_non_delta_merge_and_scd2(monkeypatch, tmp_path):
    pdf = pd.DataFrame(
        {
            "id": [1, 2],
            "event_date": ["2024-01-01", "2024-01-02"],
            "status": ["new", "done"],
        }
    )
    monkeypatch.setattr(mat, "_to_pandas", lambda frame: pdf)

    merge_calls = []
    scd2_calls = []
    inject_calls = []
    write_calls = []
    monkeypatch.setattr(mat, "_merge_frames", lambda existing, group, primary_key, **kwargs: merge_calls.append((existing.copy(), group.copy(), kwargs)) or group.assign(merged=True))
    monkeypatch.setattr(mat, "_scd2_frames", lambda existing, group, primary_key, scd2_cfg: scd2_calls.append((existing.copy(), group.copy(), scd2_cfg.copy())) or group.assign(_sk=range(1, len(group) + 1), effective_from="2024-01-01", is_current=True))
    monkeypatch.setattr(mat, "_inject_unknown_member_pandas", lambda frame, primary_key, scd2_cfg, unknown_cfg: inject_calls.append((list(primary_key), unknown_cfg.copy())) or frame.assign(unknown=False))
    monkeypatch.setattr(mat, "_write_frame", lambda frame, path, fmt: write_calls.append((frame.copy(), str(path), fmt)))
    monkeypatch.setattr(mat, "_read_frame", lambda path, fmt: pd.DataFrame({"id": [99], "event_date": ["2024-01-01"], "status": ["old"]}))

    contract = types.SimpleNamespace(
        source=types.SimpleNamespace(cdc_op_field="op", cdc_delete_values=["D"], cdc_timestamp_field="event_ts"),
        effective_server=lambda: None,
        metadata={},
    )
    mat_cfg = types.SimpleNamespace(soft_delete_column="is_deleted", soft_delete_value=True, soft_delete_time_column="deleted_at", soft_delete_reason_column="reason", merge_dedup_guard=True)

    existing_path = tmp_path / "merge" / "event_date=2024-01-01" / "data.parquet"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_text("existing", encoding="utf-8")

    merge_result = mat._partition_aware_merge(
        pdf,
        contract,
        tmp_path / "merge",
        "parquet",
        "merge",
        ["event_date"],
        ["id"],
        mat_cfg,
        {},
    )
    assert merge_result == {"target": str(tmp_path / "merge"), "rows_written": 2, "format": "parquet"}
    assert len(merge_calls) == 2
    assert merge_calls[0][2]["merge_dedup_guard"] is True
    assert all(call[2] == "parquet" for call in write_calls[:2])

    scd2_result = mat._partition_aware_merge(
        pdf,
        contract,
        tmp_path / "scd2",
        "parquet",
        "scd2",
        ["event_date"],
        ["id"],
        types.SimpleNamespace(soft_delete_column=None, soft_delete_value=True, soft_delete_time_column=None, soft_delete_reason_column=None, merge_dedup_guard=False),
        {"unknown_member": {"enabled": True}},
    )
    assert scd2_result == {"target": str(tmp_path / "scd2"), "rows_written": 2, "format": "parquet"}
    assert len(scd2_calls) == 2
    assert inject_calls == [(["id"], {"enabled": True}), (["id"], {"enabled": True})]


def test_partition_aware_merge_delta_first_write_and_scd2_overwrite(monkeypatch, tmp_path):
    pdf = pd.DataFrame(
        {
            "id": [1, 2],
            "event_date": ["2024-01-01", "2024-01-02"],
            "status": ["new", "done"],
        }
    )
    monkeypatch.setattr(mat, "_to_pandas", lambda frame: pdf)
    monkeypatch.setattr(mat, "_is_remote_path", lambda path: False)
    monkeypatch.setattr(mat, "_sanitize_arrow_nulls", lambda table: table)
    monkeypatch.setattr(mat, "_maybe_compact_delta", lambda path, contract: None)

    safe_writes = []
    monkeypatch.setattr(mat, "_safe_write_deltalake", lambda path, data, **kwargs: safe_writes.append((path, kwargs.copy(), data.num_rows if hasattr(data, "num_rows") else None)))

    class FakeMergeBuilder:
        def __init__(self, calls):
            self.calls = calls

        def when_matched_update_all(self):
            self.calls.append("matched")
            return self

        def when_not_matched_insert_all(self):
            self.calls.append("insert")
            return self

        def execute(self):
            self.calls.append("execute")

    merge_steps = []

    class FakeDeltaTable:
        def __init__(self, target, storage_options=None):
            self.target = target

        def to_pandas(self, filters=None):
            return pd.DataFrame({"id": [10], "event_date": [filters[0][2]], "status": ["old"]})

        def merge(self, **kwargs):
            merge_steps.append(kwargs)
            return FakeMergeBuilder(merge_steps)

    fake_deltalake = types.ModuleType("deltalake")
    fake_deltalake.DeltaTable = FakeDeltaTable
    fake_deltalake.write_deltalake = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)

    import pyarrow as pa

    merge_calls = []
    scd2_calls = []
    monkeypatch.setattr(mat, "_merge_frames", lambda existing, group, primary_key, **kwargs: merge_calls.append((existing.copy(), group.copy())) or group)
    monkeypatch.setattr(mat, "_seed_soft_delete_columns_pandas", lambda group, *args, **kwargs: group.assign(is_deleted=False))
    monkeypatch.setattr(mat, "_scd2_frames", lambda existing, group, primary_key, scd2_cfg: scd2_calls.append((existing.copy(), group.copy())) or pd.DataFrame({"_sk": [1], "id": [group.iloc[0]["id"]], "event_date": [group.iloc[0]["event_date"]], "status": [group.iloc[0]["status"]], "effective_from": ["2024-01-01"], "effective_to": [None], "is_current": [True], "_version": [1]}))
    monkeypatch.setattr(mat, "_inject_unknown_member_pandas", lambda frame, primary_key, scd2_cfg, unknown_cfg: frame.assign(_lakelogic_source="src"))

    contract = types.SimpleNamespace(
        source=types.SimpleNamespace(cdc_op_field="op", cdc_delete_values=["D"], cdc_timestamp_field="event_ts"),
        effective_server=lambda: types.SimpleNamespace(schema_policy=types.SimpleNamespace(evolution="merge")),
        metadata={},
    )
    mat_cfg = types.SimpleNamespace(soft_delete_column="is_deleted", soft_delete_value=True, soft_delete_time_column="deleted_at", soft_delete_reason_column="reason", merge_dedup_guard=False)

    result_first = mat._partition_aware_merge(
        pdf,
        contract,
        tmp_path / "delta-first",
        "delta",
        "merge",
        ["event_date"],
        ["id"],
        mat_cfg,
        {},
    )
    assert result_first == {"target": str(tmp_path / "delta-first"), "rows_written": 2, "format": "delta"}
    assert safe_writes[0][1]["partition_by"] == ["event_date"]
    assert safe_writes[0][1]["mode"] == "overwrite"

    delta_log = tmp_path / "delta-scd2" / "_delta_log"
    delta_log.mkdir(parents=True, exist_ok=True)
    result_scd2 = mat._partition_aware_merge(
        pdf,
        contract,
        tmp_path / "delta-scd2",
        "delta",
        "scd2",
        ["event_date"],
        ["id"],
        mat_cfg,
        {"unknown_member": {"enabled": True}, "surrogate_key": "_sk", "effective_from_field": "effective_from", "effective_to_field": "effective_to", "current_flag_field": "is_current", "version_column": "_version"},
    )
    assert result_scd2 == {"target": str(tmp_path / "delta-scd2"), "rows_written": 2, "format": "delta"}
    assert len(scd2_calls) == 2
    assert "predicate" in safe_writes[1][1]
    assert "event_date = '2024-01-01'" in safe_writes[1][1]["predicate"]


def test_partition_aware_merge_guards(monkeypatch, tmp_path):
    monkeypatch.setattr(mat, "_to_pandas", lambda frame: pd.DataFrame())
    empty_result = mat._partition_aware_merge(
        pd.DataFrame(),
        types.SimpleNamespace(source=None, effective_server=lambda: None, metadata={}),
        tmp_path / "empty",
        "parquet",
        "merge",
        ["event_date"],
        ["id"],
        types.SimpleNamespace(soft_delete_column=None, soft_delete_value=True, soft_delete_time_column=None, soft_delete_reason_column=None, merge_dedup_guard=False),
        {},
    )
    assert empty_result == {"target": str(tmp_path / "empty"), "rows_written": 0, "format": "parquet"}

    monkeypatch.setattr(mat, "_to_pandas", lambda frame: pd.DataFrame({"event_date": ["2024-01-01"]}))
    with pytest.raises(ValueError, match="primary_key is required for merge/scd2 strategy"):
        mat._partition_aware_merge(
            pd.DataFrame({"event_date": ["2024-01-01"]}),
            types.SimpleNamespace(source=None, effective_server=lambda: None, metadata={}),
            tmp_path / "bad",
            "parquet",
            "merge",
            ["event_date"],
            [],
            types.SimpleNamespace(soft_delete_column=None, soft_delete_value=True, soft_delete_time_column=None, soft_delete_reason_column=None, merge_dedup_guard=False),
            {},
        )


def test_materialize_dataframe_delta_first_write_merge_and_scd2(monkeypatch, tmp_path):
    pytest.importorskip("pyarrow")

    safe_writes = []
    monkeypatch.setattr(mat, "_sanitize_arrow_nulls", lambda table: table)
    monkeypatch.setattr(mat, "_safe_write_deltalake", lambda path, data, **kwargs: safe_writes.append((path, kwargs.copy(), data.num_rows)))
    monkeypatch.setattr(mat, "_is_remote_path", lambda path: False)
    monkeypatch.setattr(mat, "_pandas_available", lambda: True)
    monkeypatch.setattr(mat, "_maybe_compact_delta", lambda path, contract: None)

    fake_deltalake = types.ModuleType("deltalake")
    fake_deltalake.write_deltalake = lambda *args, **kwargs: None

    class MissingDeltaTable:
        def __init__(self, target, storage_options=None):
            raise RuntimeError("missing")

    fake_deltalake.DeltaTable = MissingDeltaTable
    monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)

    monkeypatch.setitem(
        sys.modules,
        "lakelogic.core.ddl",
        types.SimpleNamespace(_resolve_arrow_type=lambda field_type: field_type, _get_fields=lambda contract: []),
    )

    merge_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="merge", partition_by=[], scd2=None, track_columns=None),
        primary_key=["id"],
        effective_server=lambda: types.SimpleNamespace(cast_to_string=True),
        metadata={},
    )
    monkeypatch.setattr(mat, "_resolve_target", lambda contract, override_path=None: (tmp_path / "delta-merge", "delta"))
    merge_result = mat.materialize_dataframe(pl.DataFrame({"id": [1], "status": ["new"]}), merge_contract)
    assert merge_result == {"target": str(tmp_path / "delta-merge"), "rows_written": 1, "format": "delta"}
    assert safe_writes[0][1]["mode"] == "overwrite"

    scd2_calls = []
    monkeypatch.setattr(mat, "_scd2_frames", lambda existing, incoming, primary_key, scd2_cfg: scd2_calls.append((existing.copy(), incoming.copy(), scd2_cfg.copy())) or pd.DataFrame({"id": [1], "status": ["new"], "_sk": [1], "effective_from": ["2024-01-01"], "effective_to": [None], "is_current": [True], "_version": [1]}))
    monkeypatch.setattr(mat, "_inject_unknown_member_pandas", lambda frame, primary_key, scd2_cfg, unknown_cfg: frame.assign(_lakelogic_source="src"))
    scd2_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="scd2", partition_by=[], scd2={"unknown_member": {"enabled": True}}, track_columns=None, merge_dedup_guard=True),
        primary_key=["id"],
        effective_server=lambda: types.SimpleNamespace(cast_to_string=True),
        metadata={},
    )
    monkeypatch.setattr(mat, "_resolve_target", lambda contract, override_path=None: (tmp_path / "delta-scd2", "delta"))
    scd2_result = mat.materialize_dataframe(pl.DataFrame({"id": [1], "status": ["new"]}), scd2_contract)
    assert scd2_result == {"target": str(tmp_path / "delta-scd2"), "rows_written": 1, "format": "delta"}
    assert len(scd2_calls) == 1
    assert safe_writes[1][1]["mode"] == "overwrite"


def test_materialize_dataframe_delta_existing_table_merge_and_append(monkeypatch, tmp_path):
    pytest.importorskip("pyarrow")

    monkeypatch.setattr(mat, "_sanitize_arrow_nulls", lambda table: table)
    monkeypatch.setattr(mat, "_is_remote_path", lambda path: False)
    monkeypatch.setattr(mat, "_pandas_available", lambda: True)
    monkeypatch.setattr(mat, "_maybe_compact_delta", lambda path, contract: None)
    monkeypatch.setitem(
        sys.modules,
        "lakelogic.core.ddl",
        types.SimpleNamespace(_resolve_arrow_type=lambda field_type: field_type, _get_fields=lambda contract: []),
    )

    merge_steps = []
    safe_writes = []
    monkeypatch.setattr(mat, "_safe_write_deltalake", lambda path, data, **kwargs: safe_writes.append((path, kwargs.copy(), data.num_rows)))

    class FakeMergeBuilder:
        def when_matched_update_all(self):
            merge_steps.append("matched")
            return self

        def when_not_matched_insert_all(self):
            merge_steps.append("insert")
            return self

        def execute(self):
            merge_steps.append("execute")

    class FakeActions:
        column_names = ["num_records"]

        def __getitem__(self, key):
            return [2]

    class ExistingDeltaTable:
        def __init__(self, target, storage_options=None):
            self.target = target

        def merge(self, **kwargs):
            merge_steps.append(kwargs)
            return FakeMergeBuilder()

        def to_pandas(self):
            return pd.DataFrame({"id": [1], "status": ["old"]})

        def get_add_actions(self, flatten=True):
            return FakeActions()

        def to_pyarrow_table(self):
            import pyarrow as pa
            return pa.table({"id": [1, 2, 3]})

    fake_deltalake = types.ModuleType("deltalake")
    fake_deltalake.write_deltalake = lambda *args, **kwargs: None
    fake_deltalake.DeltaTable = ExistingDeltaTable
    monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)

    import pyarrow.compute as pc
    monkeypatch.setattr(pc, "sum", lambda values: types.SimpleNamespace(as_py=lambda: 2))

    merge_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="merge", partition_by=[], scd2=None, track_columns=None, merge_dedup_guard=False),
        primary_key=["id"],
        effective_server=lambda: types.SimpleNamespace(cast_to_string=True),
        metadata={},
    )
    monkeypatch.setattr(mat, "_resolve_target", lambda contract, override_path=None: (tmp_path / "delta-existing", "delta"))
    delta_log = tmp_path / "delta-existing" / "_delta_log"
    delta_log.mkdir(parents=True, exist_ok=True)
    merge_result = mat.materialize_dataframe(pl.DataFrame({"id": [1], "status": ["new"]}), merge_contract)
    assert merge_result == {"target": str(tmp_path / "delta-existing"), "rows_written": 1, "format": "delta"}
    assert isinstance(merge_steps[0], dict)
    assert merge_steps[-1] == "execute"

    append_contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(strategy="append", partition_by=[], scd2=None, track_columns=None),
        primary_key=[],
        effective_server=lambda: types.SimpleNamespace(cast_to_string=True),
        metadata={},
    )
    monkeypatch.setattr(mat, "_resolve_target", lambda contract, override_path=None: (tmp_path / "delta-append", "delta"))
    append_result = mat.materialize_dataframe(pl.DataFrame({"id": [1], "status": ["new"]}), append_contract, target_path=tmp_path / "delta-append", output_format="delta")
    assert append_result == {"target": str(tmp_path / "delta-append"), "rows_written": 1, "format": "delta"}
    assert safe_writes[0][1]["mode"] == "append"


def test_spark_save_as_table_and_apply_metadata(monkeypatch):
    sql_calls = []
    saved = []

    class FakeSparkSession:
        builder = types.SimpleNamespace(getOrCreate=lambda: types.SimpleNamespace(sql=lambda stmt: sql_calls.append(stmt)))

    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.SparkSession = FakeSparkSession
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    class FakeWriter:
        def mode(self, mode):
            saved.append(("mode", mode))
            return self

        def save(self, location):
            saved.append(("save", location))

        def saveAsTable(self, table_name):
            saved.append(("saveAsTable", table_name))

    writer = FakeWriter()
    mat._spark_save_as_table(writer, "catalog.schema.orders", "overwrite", location="env:TARGET_LOC")
    assert saved == [("mode", "overwrite"), ("save", "env:TARGET_LOC")]
    assert sql_calls[0] == "CREATE SCHEMA IF NOT EXISTS catalog.schema"
    assert sql_calls[1] == "CREATE TABLE IF NOT EXISTS catalog.schema.orders USING DELTA LOCATION 'env:TARGET_LOC'"

    saved.clear()
    mat._spark_save_as_table(writer, "catalog.schema.orders", "append", location=None)
    assert saved == [("mode", "append"), ("saveAsTable", "catalog.schema.orders")]

    spark = types.SimpleNamespace(sql=lambda stmt: sql_calls.append(stmt))
    contract = types.SimpleNamespace(
        model=types.SimpleNamespace(fields=[types.SimpleNamespace(name="order_id", description="Order identifier"), types.SimpleNamespace(name="note", description="Owner's note")]),
        materialization=types.SimpleNamespace(table_properties={"quality": "gold", "owner": "analytics"}),
    )
    mat._spark_apply_table_metadata(spark, "catalog.schema.orders", contract)
    assert "ALTER TABLE catalog.schema.orders ALTER COLUMN order_id COMMENT 'Order identifier'" in sql_calls
    assert "ALTER TABLE catalog.schema.orders ALTER COLUMN note COMMENT 'Owner\\'s note'" in sql_calls
    assert any(call.startswith("ALTER TABLE catalog.schema.orders SET TBLPROPERTIES") for call in sql_calls)


def test_inject_unknown_member_spark_and_table_paths(monkeypatch):
    created_rows = []
    save_calls = []

    class FakeExpr:
        def __init__(self, value=None):
            self.value = value

        def cast(self, dtype):
            return self

        def __eq__(self, other):
            return ("eq", self.value, other)

    class FakeFunctions:
        @staticmethod
        def col(name):
            return FakeExpr(name)

        @staticmethod
        def lit(value):
            return FakeExpr(value)

    class FakeDataType:
        def __init__(self, text):
            self._text = text

        def simpleString(self):
            return self._text

    class FakeField:
        def __init__(self, name, dtype):
            self.name = name
            self.dataType = FakeDataType(dtype)

    class FakeUnknownWriter:
        def format(self, fmt):
            save_calls.append(("format", fmt))
            return self

        def mode(self, mode):
            save_calls.append(("mode", mode))
            return self

        def saveAsTable(self, table_name):
            save_calls.append(("saveAsTable", table_name))

    class FakeUnknownDF:
        def __init__(self, rows, schema):
            self.rows = rows
            self.columns = list(rows[0].keys()) if rows else []
            self.schema = schema
            self.write = FakeUnknownWriter()

        def withColumn(self, name, expr):
            if name not in self.columns:
                self.columns.append(name)
            return self

        def select(self, *columns):
            self.columns = list(columns)
            return self

    class FakeSpark:
        def __init__(self, schema):
            self.schema = schema
            self.last_created = None

        def createDataFrame(self, rows):
            created_rows.append(rows[0])
            self.last_created = FakeUnknownDF(rows, self.schema)
            return self.last_created

        def table(self, table_name):
            return existing

    class CountResult:
        def __init__(self, value):
            self._value = value

        def count(self):
            return self._value

    class FakeResult:
        def __init__(self, spark, columns, schema, first_row):
            self.sparkSession = spark
            self.columns = columns
            self.schema = schema
            self._first_row = first_row
            self.unioned = None

        def filter(self, expr):
            return CountResult(0)

        def count(self):
            return 1

        def first(self):
            return self._first_row

        def union(self, other):
            self.unioned = other
            return {"unioned": other.columns}

    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.Row = lambda **kwargs: kwargs
    fake_sql_module.functions = FakeFunctions
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    schema = [
        FakeField("_sk", "string"),
        FakeField("name", "char(5)"),
        FakeField("amount", "double"),
        FakeField("is_current", "boolean"),
        FakeField("effective_from", "date"),
        FakeField("effective_to", "timestamp"),
        FakeField("_version", "int"),
        FakeField("_change_reason", "string"),
        FakeField("_lakelogic_run_id", "string"),
    ]
    spark = FakeSpark(schema)
    result = FakeResult(
        spark,
        [field.name for field in schema],
        schema,
        {"_lakelogic_run_id": "run-1"},
    )

    injected = mat._inject_unknown_member_spark(
        result,
        ["id"],
        {
            "surrogate_key": "_sk",
            "effective_from_field": "effective_from",
            "effective_to_field": "effective_to",
            "current_flag_field": "is_current",
            "version_column": "_version",
            "change_reason_column": "_change_reason",
        },
        {"enabled": True, "surrogate_key_value": "-9", "default_values": {"amount": -5.0}},
    )
    assert injected == {"unioned": [field.name for field in schema]}
    assert created_rows[0]["_sk"] == "-9"
    assert created_rows[0]["name"] == "Unkno"
    assert created_rows[0]["amount"] == -5.0
    assert created_rows[0]["_change_reason"] == "unknown_member"
    assert created_rows[0]["_lakelogic_run_id"] == "run-1"

    existing = FakeResult(
        spark,
        ["id", "payload", "_lakelogic_run_id"],
        [FakeField("id", "string"), FakeField("payload", "string"), FakeField("_lakelogic_run_id", "string")],
        {"_lakelogic_run_id": "run-2"},
    )
    mat._inject_unknown_member_spark_table(
        spark,
        "catalog.schema.dim_orders",
        ["id"],
        {},
        {"enabled": True, "default_values": {"id": "_UNKNOWN", "payload": "Unknown payload"}},
    )
    assert created_rows[-1]["id"] == "_UNKNOWN"
    assert created_rows[-1]["payload"] == "Unknown payload"
    assert created_rows[-1]["_lakelogic_run_id"] == "run-2"
    assert save_calls[-3:] == [("format", "delta"), ("mode", "append"), ("saveAsTable", "catalog.schema.dim_orders")]


def test_sanitize_arrow_nulls_casts_only_null_columns(monkeypatch):
    class FakeField:
        def __init__(self, name, field_type):
            self.name = name
            self.type = field_type

        def with_type(self, new_type):
            return FakeField(self.name, new_type)

    class FakeTable:
        def __init__(self):
            self.schema = [FakeField("empty_col", "null"), FakeField("value_col", "int64")]
            self.cast_schema = None

        def cast(self, schema):
            self.cast_schema = schema
            return schema

    fake_pa = types.ModuleType("pyarrow")
    fake_pa.utf8 = lambda: "utf8"
    fake_pa.schema = lambda fields: fields
    fake_pa.types = types.SimpleNamespace(is_null=lambda field_type: field_type == "null")
    monkeypatch.setitem(sys.modules, "pyarrow", fake_pa)

    table = FakeTable()
    casted = mat._sanitize_arrow_nulls(table)
    assert casted[0].type == "utf8"
    assert casted[1].type == "int64"

    clean_table = types.SimpleNamespace(
        schema=[FakeField("value_col", "int64")],
        cast=lambda schema: schema,
    )
    assert mat._sanitize_arrow_nulls(clean_table) is clean_table


def test_read_frame_delta_iceberg_duckdb_and_soft_delete_spark(monkeypatch, tmp_path):
    delta_calls = []

    fake_polars = types.ModuleType("polars")

    def fake_read_delta(path):
        delta_calls.append(("polars", path))
        raise RuntimeError("delta read failed")

    fake_polars.read_delta = fake_read_delta
    monkeypatch.setitem(sys.modules, "polars", fake_polars)

    class FakeDeltaTable:
        def __init__(self, path):
            delta_calls.append(("delta_table", str(path)))

        def to_pandas(self):
            return pd.DataFrame({"id": [2]})

    monkeypatch.setitem(sys.modules, "deltalake", types.SimpleNamespace(DeltaTable=FakeDeltaTable))
    delta_df = mat._read_frame(tmp_path / "orders", "delta")
    assert list(delta_df["id"]) == [2]
    assert delta_calls == [("polars", str(tmp_path / "orders")), ("delta_table", str(tmp_path / "orders"))]

    duck_calls = []

    class FakeDuckResult:
        def to_df(self):
            return pd.DataFrame({"id": [3]})

        def df(self):
            return pd.DataFrame({"id": [4]})

    class FakeDuckConnection:
        def execute(self, stmt):
            duck_calls.append(stmt)
            return FakeDuckResult()

        def close(self):
            duck_calls.append("closed")

    fake_duckdb = types.SimpleNamespace(connect=lambda *args, **kwargs: FakeDuckConnection())
    fake_duckdb.read_parquet = lambda path: FakeDuckResult()
    monkeypatch.setitem(sys.modules, "duckdb", fake_duckdb)

    iceberg_df = mat._read_frame(tmp_path / "warehouse.iceberg", "iceberg")
    assert list(iceberg_df["id"]) == [3]
    duckdb_df = mat._read_frame(tmp_path / "warehouse.duckdb", "duckdb")
    assert list(duckdb_df["id"]) == [4]
    assert any("iceberg_scan" in stmt for stmt in duck_calls if isinstance(stmt, str))
    assert "closed" in duck_calls

    class FakeExpr:
        def __init__(self, text):
            self.text = text

        def cast(self, dtype):
            return FakeExpr(f"cast({self.text},{dtype})")

        def isin(self, values):
            return f"isin({self.text},{tuple(values)})"

    class FakeWhen:
        def __init__(self, cond, value):
            self.cond = cond
            self.value = value

        def otherwise(self, other):
            return f"when({self.cond},{self.value},{other})"

    class FakeFunctions:
        @staticmethod
        def lit(value):
            return FakeExpr(f"lit({value})")

        @staticmethod
        def col(name):
            return FakeExpr(name)

        @staticmethod
        def when(cond, value):
            return FakeWhen(cond, value)

        @staticmethod
        def current_timestamp():
            return FakeExpr("current_timestamp")

        @staticmethod
        def coalesce(*args):
            return "coalesce(" + ",".join(arg.text if hasattr(arg, "text") else str(arg) for arg in args) + ")"

    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.functions = FakeFunctions
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    ops = []

    class FakeSparkFrame:
        def __init__(self):
            self.columns = ["op", "op_ts"]

        def withColumn(self, name, expr):
            ops.append((name, expr if isinstance(expr, str) else getattr(expr, "text", expr)))
            if name not in self.columns:
                self.columns.append(name)
            return self

    seeded = mat._seed_soft_delete_columns_spark(
        FakeSparkFrame(),
        soft_delete_col="is_deleted",
        soft_delete_time_col="deleted_at",
        soft_delete_reason_col="delete_reason",
        cdc_op_field="op",
        cdc_delete_values=["D"],
        cdc_timestamp_field="op_ts",
    )
    assert seeded.columns == ["op", "op_ts", "is_deleted", "deleted_at", "delete_reason"]
    assert [name for name, _ in ops[:3]] == ["is_deleted", "deleted_at", "delete_reason"]
    assert any(name == "delete_reason" and "cdc_delete_signal" in str(expr) for name, expr in ops)


def test_seed_soft_delete_columns_pandas_and_merge_frames(monkeypatch):
    fixed_now = "2024-02-03T04:05:06+00:00"

    class FixedDateTime:
        @staticmethod
        def now(tz=None):
            return types.SimpleNamespace(isoformat=lambda: fixed_now)

    monkeypatch.setattr("datetime.datetime", FixedDateTime)

    seeded = mat._seed_soft_delete_columns_pandas(
        pd.DataFrame(
            {
                "id": [1, 2],
                "op": ["U", "D"],
                "event_ts": [None, "2024-01-01T00:00:00+00:00"],
            }
        ),
        soft_delete_col="is_deleted",
        soft_delete_time_col="deleted_at",
        soft_delete_reason_col="delete_reason",
        cdc_op_field="op",
        cdc_delete_values=["D"],
        cdc_timestamp_field="event_ts",
    )
    assert seeded["is_deleted"].tolist() == [False, True]
    assert seeded["deleted_at"].tolist()[1] == "2024-01-01T00:00:00+00:00"
    assert seeded["delete_reason"].tolist()[1] == "cdc_delete_signal"

    existing = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["old-a", "old-b"],
            "updated_at": ["2024-01-01", "2024-01-01"],
        }
    )
    incoming = pd.DataFrame(
        {
            "id": [1, 1, 3, 2],
            "name": ["mid-a", "new-a", "new-c", "old-b"],
            "op": ["U", "U", "I", "D"],
            "updated_at": ["2024-01-02", "2024-01-03", "2024-01-04", None],
        }
    )
    warnings = []
    monkeypatch.setattr(mat.logger, "warning", lambda message: warnings.append(message))
    merged = mat._merge_frames(
        existing,
        incoming,
        primary_key=["id"],
        soft_delete_col="is_deleted",
        soft_delete_time_col="deleted_at",
        soft_delete_reason_col="delete_reason",
        cdc_op_field="op",
        cdc_delete_values=["D"],
        cdc_timestamp_field="updated_at",
        merge_dedup_guard=True,
    )
    merged = merged.sort_values("id").reset_index(drop=True)
    assert merged["id"].tolist() == [1, 2, 3]
    assert merged.loc[merged["id"] == 1, "name"].iloc[0] == "new-a"
    assert merged.loc[merged["id"] == 2, "is_deleted"].iloc[0] is True
    assert merged.loc[merged["id"] == 2, "deleted_at"].iloc[0] == fixed_now
    assert merged.loc[merged["id"] == 2, "delete_reason"].iloc[0] == "cdc_delete_signal"
    assert any("Merge dedup guard" in message for message in warnings)

    with pytest.raises(ValueError, match="primary_key is required"):
        mat._merge_frames(existing, incoming, primary_key=[])


def test_partition_groups_and_unknown_member_injection(monkeypatch):
    df = pd.DataFrame({"country": ["FR", "FR", "US"], "city": ["Paris", "Lyon", "Austin"], "id": [1, 2, 3]})
    groups = list(mat._partition_groups(df, ["country"]))
    assert len(groups) == 2
    assert groups[0][0] in ({"country": "FR"}, {"country": "US"})

    single = list(mat._partition_groups(df, []))
    assert single == [({}, df)]

    infos = []
    monkeypatch.setattr(mat.logger, "info", lambda message: infos.append(message))
    dim_df = pd.DataFrame(
        {
            "_sk": ["100"],
            "customer_id": [1],
            "name": ["Alice"],
            "effective_from": ["2024-01-01"],
            "effective_to": ["9999-12-31"],
            "is_current": [True],
            "_version": [1],
            "_change_reason": ["initial_load"],
            "_lakelogic_run_id": ["run-1"],
            "amount": [12.5],
        }
    )
    injected = mat._inject_unknown_member_pandas(
        dim_df,
        primary_key=["customer_id"],
        scd2_cfg={
            "surrogate_key": "_sk",
            "effective_from_field": "effective_from",
            "effective_to_field": "effective_to",
            "current_flag_field": "is_current",
            "version_column": "_version",
            "change_reason_column": "_change_reason",
        },
        unknown_cfg={
            "enabled": True,
            "surrogate_key_value": "-1",
            "default_values": {"customer_id": -1},
        },
    )
    assert len(injected) == 2
    unknown_row = injected[injected["_sk"] == "-1"].iloc[0]
    assert unknown_row["customer_id"] == -1
    assert unknown_row["name"] == "Unknown"
    assert unknown_row["amount"] == -1
    assert unknown_row["_version"] == 0
    assert unknown_row["_change_reason"] == "unknown_member"
    assert unknown_row["_lakelogic_run_id"] == "run-1"
    assert any("Injected unknown member row" in message for message in infos)

    unchanged = mat._inject_unknown_member_pandas(
        injected,
        primary_key=["customer_id"],
        scd2_cfg={"surrogate_key": "_sk", "change_reason_column": "_change_reason"},
        unknown_cfg={"enabled": True, "surrogate_key_value": "-1"},
    )
    assert len(unchanged) == 2

    disabled = mat._inject_unknown_member_pandas(
        dim_df,
        primary_key=["customer_id"],
        scd2_cfg={"surrogate_key": "_sk"},
        unknown_cfg={"enabled": False},
    )
    assert disabled.equals(dim_df)


def test_scd2_frames_initial_load_versions_duplicate_keys(monkeypatch):
    class FixedDateTime:
        @staticmethod
        def now(tz=None):
            return datetime(2024, 2, 3, tzinfo=timezone.utc)

    monkeypatch.setattr(mat, "datetime", FixedDateTime)

    existing = pd.DataFrame(columns=["customer_id", "name", "updated_at"])
    incoming = pd.DataFrame(
        {
            "customer_id": [1, 1, 2],
            "name": ["Alice", "Alice v2", "Bob"],
            "updated_at": ["2024-01-02", "2024-01-03", "2024-01-04"],
        }
    )

    result = mat._scd2_frames(
        existing,
        incoming,
        primary_key=["customer_id"],
        scd2_cfg={
            "effective_from_field": "effective_from",
            "effective_to_field": "effective_to",
            "current_flag_field": "is_current",
            "change_date_field": "updated_at",
            "change_reason_column": "_change_reason",
            "version_column": "_version",
            "surrogate_key": "_sk",
        },
    ).sort_values(["customer_id", "_version"]).reset_index(drop=True)

    assert result["customer_id"].tolist() == [1, 1, 2]
    assert result["_version"].tolist() == [1, 2, 1]
    assert result["effective_from"].tolist() == ["1900-01-01", "1900-01-01", "1900-01-01"]
    assert result["_change_reason"].tolist() == ["initial_load", "initial_load", "initial_load"]
    assert result.loc[0, "is_current"] == False
    assert result.loc[0, "effective_to"] == "2024-02-03T00:00:00+00:00"
    assert result.loc[1, "is_current"] == True
    assert result["_sk"].str.len().eq(16).all()


def test_scd2_frames_tracks_changes_and_skips_unchanged_rows():
    existing = pd.DataFrame(
        {
            "customer_id": [1, 3],
            "name": ["Alice", "Cara"],
            "city": ["Paris", "Rome"],
            "updated_at": ["2024-01-01", "2024-01-01"],
            "effective_from": ["2024-01-01", "2024-01-01"],
            "effective_to": ["9999-12-31", "9999-12-31"],
            "is_current": [True, True],
        }
    )
    incoming = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "name": ["Alice Updated", "Bob", "Cara"],
            "city": ["Paris", "Berlin", "Rome"],
            "updated_at": ["2024-02-01", "2024-03-01", "2024-04-01"],
        }
    )

    result = mat._scd2_frames(
        existing,
        incoming,
        primary_key=["customer_id"],
        scd2_cfg={
            "effective_from_field": "effective_from",
            "effective_to_field": "effective_to",
            "current_flag_field": "is_current",
            "change_date_field": "updated_at",
            "track_columns": ["name", "city"],
            "change_reason_column": "_change_reason",
            "version_column": "_version",
            "surrogate_key": "_sk",
        },
    ).sort_values(["customer_id", "effective_from"]).reset_index(drop=True)

    id1 = result[result["customer_id"] == 1].reset_index(drop=True)
    id2 = result[result["customer_id"] == 2].reset_index(drop=True)
    id3 = result[result["customer_id"] == 3].reset_index(drop=True)

    assert len(id1) == 2
    assert id1.loc[0, "is_current"] is False
    assert id1.loc[0, "effective_to"] == "2024-02-01"
    assert id1.loc[1, "is_current"] is True
    assert id1.loc[1, "effective_from"] == "2024-02-01"
    assert id1.loc[1, "_change_reason"] == "name"

    assert len(id2) == 1
    assert id2.loc[0, "effective_from"] == "1900-01-01"
    assert id2.loc[0, "_change_reason"] == "initial_load"
    assert id2.loc[0, "is_current"] is True

    assert len(id3) == 1
    assert id3.loc[0, "name"] == "Cara"
    assert id3.loc[0, "is_current"] is True
    assert result["_version"].tolist() == [1, 2, 1, 1]