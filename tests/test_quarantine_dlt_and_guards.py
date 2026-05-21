"""Tests for the previously-uncovered branches in materialize_quarantine.

Existing test_quarantine.py covers the Spark / Snowflake / BigQuery / Iceberg
table writers (via mocking) and the main path-mode happy paths. This file fills:

- The dlt format branch (lines 1006-1060, ~87 lines — the largest single
  uncovered block).
- Empty-dataframe guard (lines 627-636 area).
- ``mode='table'`` with missing ``quarantine.table`` → falls back to path.
- Unresolved target → ValueError.
- ``table:`` prefix in raw_target → dispatches to _write_quarantine_table.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from typing import Any

import pytest

pl = pytest.importorskip("polars")
pa = pytest.importorskip("pyarrow")

from lakelogic.core import quarantine as q
from lakelogic.core.models import DataContract, FieldDefinition, Info, Model, Quarantine


# ──────────────────────────────────────────────────────────────────────────────
# Fake dlt installer (reused from materialization tests)
# ──────────────────────────────────────────────────────────────────────────────


def _install_fake_dlt(monkeypatch, *, raise_on_run=False):
    rec = SimpleNamespace(resource_calls=[], pipeline_calls=[], last_data=None)

    def fake_resource(*, name, write_disposition, **_):
        rec.resource_calls.append({"name": name, "write_disposition": write_disposition})

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

    fake_dlt = types.ModuleType("dlt")
    fake_dlt.resource = fake_resource
    fake_dlt.pipeline = lambda **kw: FakePipeline(**kw)
    fake_dlt.destinations = SimpleNamespace(
        duckdb=lambda **kw: ("duckdb-dest", kw),
        postgres=lambda **kw: ("postgres-dest", kw),
    )
    monkeypatch.setitem(sys.modules, "dlt", fake_dlt)
    return rec


def _contract_with_quarantine(*, target="quarantine_demo/bad.parquet", format="dlt", table=None,
                              dlt_destination=None, dlt_credentials=None,
                              dlt_dataset_name=None, **dlt_extras):
    """Build a contract whose quarantine config carries dlt settings."""
    quarantine_kwargs: dict = {"target": target, "format": format}
    if table is not None:
        quarantine_kwargs["table"] = table
    # dlt config lives in the model_extra of the Quarantine model
    extras = {}
    if dlt_destination is not None:
        extras["dlt_destination"] = dlt_destination
    if dlt_credentials is not None:
        extras["dlt_credentials"] = dlt_credentials
    if dlt_dataset_name is not None:
        extras["dlt_dataset_name"] = dlt_dataset_name
    extras.update(dlt_extras)
    quarantine_kwargs.update(extras)

    return DataContract(
        version="1.0",
        info=Info(title="Orders", version="1.0"),
        dataset="orders",
        metadata={"domain": "commerce"},
        model=Model(fields=[FieldDefinition(name="id", type="integer")]),
        quarantine=Quarantine(**quarantine_kwargs),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Empty-frame guard
# ──────────────────────────────────────────────────────────────────────────────


class TestQuarantineEmptyGuard:
    def test_empty_polars_df_returns_zero_rows(self, tmp_path):
        contract = _contract_with_quarantine(
            target=str(tmp_path / "bad.parquet"), format="parquet"
        )
        empty_df = pl.DataFrame(schema={"id": pl.Int64})  # 0 rows, schema present
        result = q.materialize_quarantine(empty_df, contract)
        assert result["rows_written"] == 0


# ──────────────────────────────────────────────────────────────────────────────
# dlt format — the big uncovered block
# ──────────────────────────────────────────────────────────────────────────────


class TestQuarantineDltFormat:
    def test_dlt_writes_polars_df_via_arrow_conversion(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        contract = _contract_with_quarantine(
            target="quarantine_demo/bad",
            format="dlt",
            dlt_destination="duckdb",
            dlt_dataset_name="quarantine",
        )
        df = pl.DataFrame({"id": [1, 2, 3]})
        result = q.materialize_quarantine(df, contract)
        assert result["format"] == "dlt"
        assert result["dlt_destination"] == "duckdb"
        assert result["rows_written"] == 3
        # The data sent to dlt is an Arrow table
        assert isinstance(rec.last_data, pa.Table)
        assert rec.last_data.num_rows == 3
        # Resource is created with write_disposition='append'
        assert rec.resource_calls[0]["write_disposition"] == "append"

    def test_dlt_writes_arrow_input_directly(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        contract = _contract_with_quarantine(
            target="q/bad",
            format="dlt",
            dlt_destination="duckdb",
        )
        table = pa.table({"id": [10, 20]})
        result = q.materialize_quarantine(table, contract)
        assert rec.last_data is table  # passed through
        assert result["rows_written"] == 2

    def test_dlt_uses_q_table_when_present(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        contract = _contract_with_quarantine(
            target="q/bad",
            format="dlt",
            table="custom_quarantine_table",
            dlt_destination="duckdb",
        )
        df = pl.DataFrame({"id": [1]})
        result = q.materialize_quarantine(df, contract)
        assert rec.resource_calls[0]["name"] == "custom_quarantine_table"
        assert "custom_quarantine_table" in result["target"]

    def test_dlt_falls_back_to_dataset_name_when_no_table(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        contract = _contract_with_quarantine(
            target="q/bad",
            format="dlt",
            dlt_destination="duckdb",
            dlt_dataset_name="quarantine_ds",
        )
        df = pl.DataFrame({"id": [1]})
        q.materialize_quarantine(df, contract)
        # quarantine table name defaults to dataset_name when contract.quarantine.table is None
        assert rec.resource_calls[0]["name"] == "quarantine_ds"

    def test_dlt_credentials_passed_to_destination(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        contract = _contract_with_quarantine(
            target="q/bad",
            format="dlt",
            dlt_destination="postgres",
            dlt_credentials="postgresql://user:pass@localhost/db",
        )
        df = pl.DataFrame({"id": [1]})
        q.materialize_quarantine(df, contract)
        dest = rec.pipeline_calls[0]["destination"]
        # Fake destinations callable returns ("postgres-dest", kwargs); we passed credentials.
        assert dest[1]["credentials"] == "postgresql://user:pass@localhost/db"

    def test_dlt_extra_kwargs_starting_with_dlt_prefix_forwarded(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        contract = _contract_with_quarantine(
            target="q/bad",
            format="dlt",
            dlt_destination="postgres",
            dlt_credentials="creds",
            dlt_schema_name="custom_schema",  # extra kwarg
        )
        df = pl.DataFrame({"id": [1]})
        q.materialize_quarantine(df, contract)
        dest_kwargs = rec.pipeline_calls[0]["destination"][1]
        assert dest_kwargs.get("schema_name") == "custom_schema"

    def test_dlt_import_error_raises_helpful_message(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "dlt", None)
        contract = _contract_with_quarantine(target="q/bad", format="dlt")
        df = pl.DataFrame({"id": [1]})
        with pytest.raises(ImportError, match="dlt quarantine format requires"):
            q.materialize_quarantine(df, contract)


# ──────────────────────────────────────────────────────────────────────────────
# materialize_quarantine guard branches
# ──────────────────────────────────────────────────────────────────────────────


class TestMaterializeQuarantineGuards:
    def test_none_contract_returns_empty(self):
        assert q.materialize_quarantine(pl.DataFrame({"id": [1]}), None) == {}

    def test_no_quarantine_config_returns_empty(self):
        contract = DataContract(
            version="1.0", info=Info(title="t", version="1.0"), dataset="t"
        )
        # quarantine is None by default
        assert q.materialize_quarantine(pl.DataFrame({"id": [1]}), contract) == {}

    def test_table_mode_without_table_name_falls_back_to_path(self, monkeypatch, tmp_path):
        # quarantine.table is None → table mode falls back to path mode and writes
        contract = _contract_with_quarantine(
            target=str(tmp_path / "bad.parquet"), format="parquet", table=None
        )
        warnings = []
        monkeypatch.setattr(q.logger, "warning", warnings.append)

        df = pl.DataFrame({"id": [1, 2]})
        result = q.materialize_quarantine(df, contract, quarantine_mode="table")
        # Path-mode write happened
        assert result["format"] == "parquet"
        assert result["rows_written"] == 2
        assert any("falling back to path" in m for m in warnings)

    def test_table_mode_with_table_name_routes_to_table_writer(self, monkeypatch):
        contract = _contract_with_quarantine(
            target="q/bad", format="parquet", table="my_q_table"
        )
        calls = []
        monkeypatch.setattr(
            q,
            "_write_quarantine_table",
            lambda df, contract, table_name, engine_name=None: calls.append(table_name)
            or {"target": "table", "rows_written": 1},
        )
        df = pl.DataFrame({"id": [1]})
        result = q.materialize_quarantine(
            df, contract, quarantine_mode="table", engine_name="polars"
        )
        assert calls == ["my_q_table"]
        assert result["target"] == "table"

    def test_table_prefix_in_target_routes_to_table_writer(self, monkeypatch):
        contract = _contract_with_quarantine(
            target="table:catalog.schema.quarantine_audit", format="parquet"
        )
        calls = []
        monkeypatch.setattr(
            q,
            "_write_quarantine_table",
            lambda df, contract, table_name, engine_name=None: calls.append(table_name)
            or {"target": f"table:{table_name}", "rows_written": 1},
        )
        df = pl.DataFrame({"id": [1]})
        result = q.materialize_quarantine(df, contract, engine_name="polars")
        assert calls == ["catalog.schema.quarantine_audit"]

    def test_unresolved_template_target_raises_value_error(self):
        # quarantine.target with unresolved {placeholder} → ValueError
        contract = _contract_with_quarantine(
            target="{quarantine_path}/orders/bad.parquet", format="parquet"
        )
        df = pl.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="Quarantine target not fully resolved"):
            q.materialize_quarantine(df, contract)

    def test_no_quarantine_target_returns_empty(self):
        contract = DataContract(
            version="1.0",
            info=Info(title="t", version="1.0"),
            dataset="t",
            quarantine=Quarantine(target=""),  # explicit empty string
        )
        assert q.materialize_quarantine(pl.DataFrame({"id": [1]}), contract) == {}
