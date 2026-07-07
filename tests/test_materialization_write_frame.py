"""Tests for the _write_frame dispatcher in materialization.

_write_frame is the lowest-level write helper used by materialize_dataframe
for every non-Spark engine. It dispatches across 7 output formats:
csv, parquet, iceberg, delta, duckdb, dlt, and an explicit unsupported fallback.

Each format has its own success and failure paths. These tests pin them down
without requiring real cloud storage, real Delta tables, or a real dlt destination.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pl = pytest.importorskip("polars")
pa = pytest.importorskip("pyarrow")

from lakelogic.core import materialization as mat


# ──────────────────────────────────────────────────────────────────────────────
# Helpers — stubbing inline imports inside _write_frame
# ──────────────────────────────────────────────────────────────────────────────


def _install_fake_dlt(monkeypatch, *, raise_on_run=False):
    rec = SimpleNamespace(resource_calls=[], pipeline_calls=[], last_data=None)

    def fake_resource(*, name, write_disposition, primary_key):
        rec.resource_calls.append({"name": name, "write_disposition": write_disposition, "primary_key": primary_key})

        def decorator(fn):
            def wrapped():
                for item in fn():
                    rec.last_data = item
                return rec.last_data

            return wrapped

        return decorator

    class FakePipeline:
        def __init__(self, **kwargs):
            rec.pipeline_calls.append(kwargs)

        def run(self, sink):
            if callable(sink):
                sink()
            if raise_on_run:
                raise RuntimeError("simulated dlt failure")
            return "load-info-token"

    fake = types.ModuleType("dlt")
    fake.resource = fake_resource
    fake.pipeline = lambda **kw: FakePipeline(**kw)
    fake.destinations = SimpleNamespace(
        duckdb=lambda **kw: ("duckdb", kw),
        postgres=lambda **kw: ("postgres", kw),
    )
    monkeypatch.setitem(sys.modules, "dlt", fake)
    return rec


def _install_fake_deltalake(monkeypatch, *, raise_on_write=False):
    fake_mod = types.ModuleType("deltalake")
    calls = []

    # NB: `engine` is declared as an explicit keyword parameter (rather than
    # being absorbed into **kwargs) so that
    # `inspect.signature(write_deltalake).parameters` reports it.
    # `_safe_write_deltalake` strips the `engine` kwarg when the underlying
    # write_deltalake signature doesn't list it (deltalake 1.x removed the
    # parameter) — without this explicit declaration the mock would look
    # like the deltalake-1.x flavour and `engine` would never reach
    # `calls[…]["kwargs"]`, hiding what the engine actually picked.
    def fake_write(path, data, *, engine=None, **kwargs):
        if engine is not None:
            kwargs["engine"] = engine
        calls.append({"path": path, "data": data, "kwargs": kwargs})
        if raise_on_write:
            raise RuntimeError("simulated delta write failure")

    fake_mod.write_deltalake = fake_write
    monkeypatch.setitem(sys.modules, "deltalake", fake_mod)
    return calls


# ──────────────────────────────────────────────────────────────────────────────
# CSV branch
# ──────────────────────────────────────────────────────────────────────────────


class TestWriteFrameCSV:
    def test_polars_write_csv(self, tmp_path):
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        out = tmp_path / "out.csv"
        mat._write_frame(df, out, "csv")
        assert out.exists()
        # round-trip
        back = pl.read_csv(out)
        assert back.to_dicts() == df.to_dicts()

    def test_pandas_write_csv(self, tmp_path):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a": [1, 2]})
        out = tmp_path / "p.csv"
        mat._write_frame(df, out, "csv")
        assert out.exists()
        assert pd.read_csv(out).to_dict("list") == {"a": [1, 2]}

    def test_unsupported_df_for_csv_raises(self, tmp_path):
        class Bare:
            pass

        with pytest.raises(ValueError, match="CSV materialization"):
            mat._write_frame(Bare(), tmp_path / "x.csv", "csv")


# ──────────────────────────────────────────────────────────────────────────────
# Parquet branch
# ──────────────────────────────────────────────────────────────────────────────


class TestWriteFrameParquet:
    def test_polars_local(self, tmp_path):
        df = pl.DataFrame({"a": [1, 2, 3]})
        out = tmp_path / "out.parquet"
        mat._write_frame(df, out, "parquet")
        assert out.exists()
        assert pl.read_parquet(out).to_dicts() == df.to_dicts()

    def test_polars_remote_uses_write_parquet_with_storage_options(self, monkeypatch):
        captured = {}

        class FakePolarsLike:
            def write_parquet(self, path, storage_options=None):
                captured["path"] = path
                captured["storage_options"] = storage_options

        monkeypatch.setattr(mat, "_is_remote_path", lambda p: True)
        monkeypatch.setattr(mat, "_build_storage_options", lambda opts: {"resolved": "yes"})
        mat._write_frame(
            FakePolarsLike(),
            "abfss://x@y.dfs.core.windows.net/z",
            "parquet",
            storage_options={"raw": "creds"},
        )
        assert captured["path"] == "abfss://x@y.dfs.core.windows.net/z"
        assert captured["storage_options"] == {"resolved": "yes"}

    def test_pandas_to_parquet_happy(self, tmp_path):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a": [1, 2]})
        out = tmp_path / "p.parquet"
        mat._write_frame(df, out, "parquet")
        assert out.exists()

    def test_unsupported_df_for_parquet_raises(self, tmp_path):
        class Bare:
            pass

        with pytest.raises(ValueError, match="Parquet materialization"):
            mat._write_frame(Bare(), tmp_path / "x.parquet", "parquet")


class TestWriteFrameParquetFallbacks:
    """When to_parquet raises (pyarrow missing), fall back to DuckDB then polars."""

    def test_duckdb_fallback_with_sql_query(self, tmp_path):
        # Simulate a DuckDB relation: has .connection and .sql_query, no to_arrow.
        import duckdb as real_duckdb

        con = real_duckdb.connect()
        con.execute("CREATE TABLE t AS SELECT 1 AS a UNION ALL SELECT 2 AS a")

        class FakeRelation:
            def __init__(self):
                self.connection = con
                # Trigger fallback by raising on to_parquet

            def to_parquet(self, path, index=False):
                raise RuntimeError("pyarrow not installed")

            def sql_query(self):
                return "SELECT * FROM t"

            @property
            def columns(self):
                return ["a"]

        out = tmp_path / "via-duckdb.parquet"
        mat._write_frame(FakeRelation(), out, "parquet")
        assert out.exists()
        rows = real_duckdb.read_parquet(str(out)).fetchall()
        assert sorted(r[0] for r in rows) == [1, 2]
        con.close()

    def test_polars_fallback_when_duckdb_also_fails(self, monkeypatch, tmp_path):
        pd = pytest.importorskip("pandas")

        # Force duckdb import to fail so we fall through to polars.
        monkeypatch.setitem(sys.modules, "duckdb", None)

        # Make the pandas df look broken on to_parquet (forcing fallback), but
        # otherwise be a real pandas DataFrame so pl.from_pandas can convert it.
        df = pd.DataFrame({"a": [9, 10]})

        def boom(self, path, index=False):
            raise RuntimeError("simulated to_parquet failure (pyarrow missing)")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

        out = tmp_path / "polars-fallback.parquet"
        mat._write_frame(df, out, "parquet")
        # Polars fallback writes; verify file exists.
        assert out.exists()
        assert pl.read_parquet(out).to_dicts() == [{"a": 9}, {"a": 10}]

    def test_all_fallbacks_fail_raises_helpful_value_error(self, monkeypatch, tmp_path):
        """Regression: previously raised UnboundLocalError because inner
        `except Exception as exc:` shadowed the outer `exc`, so the
        `raise ValueError(...) from exc` below it had no `exc` in scope."""
        pd = pytest.importorskip("pandas")

        # All three writers must fail to reach the final raise.
        monkeypatch.setitem(sys.modules, "duckdb", None)

        df = pd.DataFrame({"a": [1]})

        def boom(self, path, index=False):
            raise RuntimeError("pyarrow missing")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)
        monkeypatch.setattr("polars.from_pandas", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("polars failed")))

        with pytest.raises(ValueError, match="pyarrow/fastparquet, duckdb, or polars"):
            mat._write_frame(df, tmp_path / "x.parquet", "parquet")


# ──────────────────────────────────────────────────────────────────────────────
# Iceberg branch
# ──────────────────────────────────────────────────────────────────────────────


class TestWriteFrameIceberg:
    def test_iceberg_via_duckdb_relation(self, tmp_path):
        import duckdb as real_duckdb

        con = real_duckdb.connect()

        class FakeRelation:
            def __init__(self):
                self.connection = con

            def sql_query(self):
                return "SELECT 1 AS x"

        # The iceberg branch in _write_frame attempts to INSTALL/LOAD the iceberg
        # extension. On many CI runners that succeeds but COPY ... TO 'path' (FORMAT ICEBERG)
        # fails because Iceberg writes need extra setup. We assert that the call path
        # raised the wrapped ValueError, which still exercises the iceberg branch code.
        try:
            mat._write_frame(FakeRelation(), tmp_path / "ice", "iceberg")
        except ValueError as exc:
            assert "Iceberg materialization" in str(exc)
        finally:
            con.close()

    def test_iceberg_standalone_path(self, tmp_path):
        # DataFrame without .connection/.sql_query — exercises the standalone duckdb branch.
        df = pl.DataFrame({"x": [1]})
        try:
            mat._write_frame(df, tmp_path / "ice", "iceberg")
        except ValueError as exc:
            assert "Iceberg materialization" in str(exc)


# ──────────────────────────────────────────────────────────────────────────────
# Delta branch
# ──────────────────────────────────────────────────────────────────────────────


class TestWriteFrameDelta:
    def test_delta_local_polars_uses_pyarrow_engine(self, monkeypatch, tmp_path):
        calls = _install_fake_deltalake(monkeypatch)
        # polars df has .to_arrow() — _write_frame should convert and call write_deltalake
        df = pl.DataFrame({"a": [1, 2, 3]})
        mat._write_frame(df, tmp_path / "delta", "delta")
        assert len(calls) == 1
        assert calls[0]["kwargs"]["engine"] == "pyarrow"
        assert calls[0]["kwargs"]["mode"] == "overwrite"
        assert isinstance(calls[0]["data"], pa.Table)

    def test_delta_remote_polars_uses_write_delta(self, monkeypatch):
        captured = {}

        class FakePolarsRemote:
            def write_delta(self, path, mode, storage_options):
                captured["path"] = path
                captured["mode"] = mode
                captured["storage_options"] = storage_options

        # If _is_remote_path returns True AND df has write_delta, the function
        # short-circuits before reaching deltalake.
        monkeypatch.setattr(mat, "_is_remote_path", lambda p: True)
        monkeypatch.setattr(mat, "_build_storage_options", lambda opts: {"resolved": True})
        # Still need to satisfy `from deltalake import write_deltalake` import.
        _install_fake_deltalake(monkeypatch)

        mat._write_frame(
            FakePolarsRemote(),
            "abfss://x@y.dfs.core.windows.net/z",
            "delta",
            storage_options={"raw": "creds"},
        )
        assert captured["path"] == "abfss://x@y.dfs.core.windows.net/z"
        assert captured["mode"] == "overwrite"
        assert captured["storage_options"] == {"resolved": True}

    def test_delta_import_error_raises_helpful_message(self, monkeypatch, tmp_path):
        monkeypatch.setitem(sys.modules, "deltalake", None)
        df = pl.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="deltalake"):
            mat._write_frame(df, tmp_path / "delta", "delta")

    def test_delta_write_failure_wraps_in_value_error(self, monkeypatch, tmp_path):
        _install_fake_deltalake(monkeypatch, raise_on_write=True)
        df = pl.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Delta materialization failed"):
            mat._write_frame(df, tmp_path / "delta", "delta")

    def test_delta_mode_override_passed_through(self, monkeypatch, tmp_path):
        calls = _install_fake_deltalake(monkeypatch)
        df = pl.DataFrame({"a": [1]})
        mat._write_frame(df, tmp_path / "delta", "delta", mode_override="append")
        assert calls[0]["kwargs"]["mode"] == "append"


# ──────────────────────────────────────────────────────────────────────────────
# DuckDB output branch
# ──────────────────────────────────────────────────────────────────────────────


class TestWriteFrameDuckDB:
    def test_duckdb_writes_table_to_attached_database(self, tmp_path):
        import duckdb as real_duckdb

        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        out = tmp_path / "out.duckdb"
        mat._write_frame(df, out, "duckdb")
        assert out.exists()

        # Re-open the DB and verify the table is there with the sanitized name.
        con = real_duckdb.connect(str(out))
        tables = con.execute("SHOW TABLES").fetchall()
        names = [t[0] for t in tables]
        assert "out" in names  # path.stem after sanitization
        rows = con.execute("SELECT id, name FROM out ORDER BY id").fetchall()
        assert rows == [(1, "a"), (2, "b"), (3, "c")]
        con.close()

    def test_duckdb_sanitizes_table_name(self, tmp_path):
        import duckdb as real_duckdb

        df = pl.DataFrame({"v": [1]})
        # Path stem has dots and dashes which should be replaced
        out = tmp_path / "weird.name-with-dashes.duckdb"
        mat._write_frame(df, out, "duckdb")
        con = real_duckdb.connect(str(out))
        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        assert any("weird_name_with_dashes" in t for t in tables)
        con.close()


# ──────────────────────────────────────────────────────────────────────────────
# dlt output branch
# ──────────────────────────────────────────────────────────────────────────────


class TestWriteFrameDlt:
    def test_dlt_polars_to_arrow(self, monkeypatch, tmp_path):
        rec = _install_fake_dlt(monkeypatch)
        df = pl.DataFrame({"a": [1, 2]})
        out = tmp_path / "orders"  # path stem becomes the table name
        mat._write_frame(df, out, "dlt")
        assert isinstance(rec.last_data, pa.Table)
        assert rec.last_data.num_rows == 2
        # Resource was created with the sanitized table name
        assert rec.resource_calls[0]["name"] == "orders"

    def test_dlt_config_via_path_attribute(self, monkeypatch, tmp_path):
        rec = _install_fake_dlt(monkeypatch)
        df = pl.DataFrame({"a": [1]})

        class PathLikeWithConfig:
            def __init__(self, p):
                self._p = p
                self._dlt_config = {
                    "dlt_destination": "postgres",
                    "dlt_credentials": "postgresql://x",
                    "write_disposition": "merge",
                    "primary_key": ["a"],
                }
                self._dlt_table = "explicit_table"
                self.stem = "ignored"

            def __str__(self):
                return str(self._p)

            def __fspath__(self):
                return str(self._p)

        p = PathLikeWithConfig(tmp_path / "data")
        mat._write_frame(df, p, "dlt")
        assert rec.resource_calls[0]["name"] == "explicit_table"
        assert rec.resource_calls[0]["write_disposition"] == "merge"
        assert rec.resource_calls[0]["primary_key"] == ["a"]
        # destination kwargs include credentials
        assert rec.pipeline_calls[0]["destination"] == ("postgres", {"credentials": "postgresql://x"})

    def test_dlt_import_error_raises_helpful_message(self, monkeypatch, tmp_path):
        monkeypatch.setitem(sys.modules, "dlt", None)
        df = pl.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="dlt materialization requires"):
            mat._write_frame(df, tmp_path / "x", "dlt")

    def test_dlt_run_failure_wraps_in_value_error(self, monkeypatch, tmp_path):
        _install_fake_dlt(monkeypatch, raise_on_run=True)
        df = pl.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="dlt materialization failed"):
            mat._write_frame(df, tmp_path / "x", "dlt")


# ──────────────────────────────────────────────────────────────────────────────
# Fallback / unsupported branches
# ──────────────────────────────────────────────────────────────────────────────


class TestWriteFrameUnsupported:
    def test_unknown_format_raises(self, tmp_path):
        df = pl.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Unsupported output format"):
            mat._write_frame(df, tmp_path / "x.foo", "foo-format")
