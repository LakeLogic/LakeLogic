"""Tests for materialization secondary-target dual-writes and Delta compaction.

Focused on two cohesive surfaces that previously had near-zero coverage:

1. ``write_to_secondary_targets`` and the private ``_run_secondary_targets`` —
   the dlt-driven dual-write path used by the 07_dlt_prefect_pipeline notebook
   (Delta primary + any-database secondary).

2. ``optimize_delta`` and ``_maybe_compact_delta`` — the delta-rs based
   compaction/vacuum path called automatically after Delta writes.

dlt and deltalake are stubbed at the ``sys.modules`` level so these tests run
in the lean ``[dev]`` install with no extras.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

pl = pytest.importorskip("polars")
pa = pytest.importorskip("pyarrow")

from lakelogic.core import materialization as mat


# ──────────────────────────────────────────────────────────────────────────────
# Fake dlt module — installed into sys.modules so the inline `import dlt` inside
# the functions under test resolves to this stub rather than the real package.
# ──────────────────────────────────────────────────────────────────────────────


def _install_fake_dlt(monkeypatch: pytest.MonkeyPatch, *, raise_on_run: bool = False) -> SimpleNamespace:
    """Inject a fake ``dlt`` into sys.modules. Returns the recorder namespace."""
    rec = SimpleNamespace(
        resource_calls=[],
        pipeline_calls=[],
        run_calls=[],
        last_data=None,
    )

    def fake_resource(*, name: str, write_disposition: str, primary_key: Any) -> Any:
        rec.resource_calls.append({"name": name, "write_disposition": write_disposition, "primary_key": primary_key})

        def decorator(fn):
            def wrapped():
                gen = fn()
                # consume the generator so we can verify the payload
                for item in gen:
                    rec.last_data = item
                return rec.last_data

            wrapped.__resource_name__ = name
            return wrapped

        return decorator

    class FakePipeline:
        def __init__(self, **kwargs):
            rec.pipeline_calls.append(kwargs)

        def run(self, sink_or_data):
            # The sink is the wrapped function returned by fake_resource;
            # calling it consumes the generator and captures rec.last_data.
            if callable(sink_or_data):
                sink_or_data()
            rec.run_calls.append({"data": rec.last_data})
            if raise_on_run:
                raise RuntimeError("simulated dlt run failure")

    def fake_pipeline(**kwargs):
        return FakePipeline(**kwargs)

    # destinations.duckdb / .postgres / etc — anything that gets ``getattr``-ed
    # from ``_dlt.destinations.__dict__`` returns a callable that records its kwargs.
    class _Destinations:
        def __init__(self):
            self.__dict__["duckdb"] = lambda **kw: ("duckdb-dest", kw)
            self.__dict__["postgres"] = lambda **kw: ("postgres-dest", kw)
            self.__dict__["motherduck"] = lambda **kw: ("motherduck-dest", kw)

    fake_dlt = types.ModuleType("dlt")
    fake_dlt.resource = fake_resource  # type: ignore[attr-defined]
    fake_dlt.pipeline = fake_pipeline  # type: ignore[attr-defined]
    fake_dlt.destinations = _Destinations()  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "dlt", fake_dlt)
    return rec


# ──────────────────────────────────────────────────────────────────────────────
# write_to_secondary_targets — public helper
# ──────────────────────────────────────────────────────────────────────────────


class TestWriteToSecondaryTargetsGuards:
    """The cheap early-exit guards."""

    def test_returns_empty_when_targets_is_none(self):
        assert mat.write_to_secondary_targets(None, pl.DataFrame({"a": [1]}), "t") == []

    def test_returns_empty_when_targets_is_empty_list(self):
        assert mat.write_to_secondary_targets([], pl.DataFrame({"a": [1]}), "t") == []

    def test_returns_empty_when_targets_is_not_a_list(self):
        # The function asks for ``isinstance(..., list)`` — pass a dict and confirm guard.
        assert mat.write_to_secondary_targets({"format": "dlt"}, pl.DataFrame({"a": [1]}), "t") == []


class TestWriteToSecondaryTargetsCredentialValidation:
    """The branch that complains when a non-local destination has no credentials."""

    def test_fail_on_error_raises_when_credentials_missing(self, monkeypatch):
        _install_fake_dlt(monkeypatch)
        # Postgres is NOT in {"duckdb","filesystem","motherduck","weaviate"}.
        targets = [
            {
                "format": "dlt",
                "dlt_destination": "postgres",
                "fail_on_error": True,
                # no dlt_credentials, no env var
            }
        ]
        monkeypatch.delenv("DESTINATION__POSTGRES__CREDENTIALS", raising=False)
        with pytest.raises(ValueError, match="No credentials for 'postgres'"):
            mat.write_to_secondary_targets(targets, pl.DataFrame({"a": [1]}), "t")

    def test_warning_only_when_credentials_missing_and_fail_on_error_false(self, monkeypatch, caplog):
        _install_fake_dlt(monkeypatch)
        targets = [
            {
                "format": "dlt",
                "dlt_destination": "postgres",
                "fail_on_error": False,
            }
        ]
        monkeypatch.delenv("DESTINATION__POSTGRES__CREDENTIALS", raising=False)
        results = mat.write_to_secondary_targets(targets, pl.DataFrame({"a": [1]}), "t")
        # Continues past the credentials warning — write is attempted and (in our fake)
        # succeeds, producing one result entry.
        assert len(results) == 1
        assert results[0].get("dlt_destination") == "postgres"

    def test_credentials_via_env_var_satisfies_check(self, monkeypatch):
        _install_fake_dlt(monkeypatch)
        monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS", "postgres://x")
        targets = [
            {
                "format": "dlt",
                "dlt_destination": "postgres",
                "fail_on_error": True,  # would raise if creds-check tripped
            }
        ]
        results = mat.write_to_secondary_targets(targets, pl.DataFrame({"a": [1]}), "t")
        assert len(results) == 1
        assert "error" not in results[0]


class TestWriteToSecondaryTargetsStrategyMapping:
    @pytest.mark.parametrize(
        "in_strategy,expected_disposition",
        [
            ("merge", "merge"),
            ("append", "append"),
            ("overwrite", "replace"),
            ("something_unknown", "append"),  # default fallback
        ],
    )
    def test_strategy_maps_to_dlt_write_disposition(self, monkeypatch, in_strategy, expected_disposition):
        rec = _install_fake_dlt(monkeypatch)
        targets = [{"format": "dlt", "dlt_destination": "duckdb"}]
        mat.write_to_secondary_targets(targets, pl.DataFrame({"a": [1]}), "t", strategy=in_strategy)
        assert rec.resource_calls[0]["write_disposition"] == expected_disposition


class TestWriteToSecondaryTargetsInputConversion:
    """df is converted to a pyarrow.Table regardless of input type."""

    def test_polars_input_is_converted_to_arrow(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        targets = [{"format": "dlt", "dlt_destination": "duckdb"}]
        mat.write_to_secondary_targets(targets, pl.DataFrame({"a": [1, 2]}), "t")
        assert isinstance(rec.last_data, pa.Table)
        assert rec.last_data.num_rows == 2

    def test_arrow_input_passes_through(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        table = pa.table({"a": [1, 2, 3]})
        targets = [{"format": "dlt", "dlt_destination": "duckdb"}]
        mat.write_to_secondary_targets(targets, table, "t")
        # Same Arrow table, untouched
        assert rec.last_data is table

    def test_pandas_input_is_converted_to_arrow(self, monkeypatch):
        pd = pytest.importorskip("pandas")
        rec = _install_fake_dlt(monkeypatch)
        targets = [{"format": "dlt", "dlt_destination": "duckdb"}]
        mat.write_to_secondary_targets(targets, pd.DataFrame({"a": [1, 2, 3]}), "t")
        assert isinstance(rec.last_data, pa.Table)
        assert rec.last_data.num_rows == 3


class TestWriteToSecondaryTargetsErrorHandling:
    def test_unsupported_format_logs_warning_and_skips(self, monkeypatch, caplog):
        targets = [{"format": "parquet", "table_name": "x"}]
        results = mat.write_to_secondary_targets(targets, pl.DataFrame({"a": [1]}), "t")
        # No write attempted; nothing recorded for unsupported format
        assert results == []

    def test_dlt_run_failure_captured_when_fail_on_error_false(self, monkeypatch):
        _install_fake_dlt(monkeypatch, raise_on_run=True)
        targets = [{"format": "dlt", "dlt_destination": "duckdb", "fail_on_error": False}]
        results = mat.write_to_secondary_targets(targets, pl.DataFrame({"a": [1]}), "t")
        assert len(results) == 1
        assert "error" in results[0]
        assert "simulated dlt run failure" in results[0]["error"]

    def test_dlt_run_failure_reraises_when_fail_on_error_true(self, monkeypatch):
        _install_fake_dlt(monkeypatch, raise_on_run=True)
        targets = [{"format": "dlt", "dlt_destination": "duckdb", "fail_on_error": True}]
        with pytest.raises(RuntimeError, match="simulated dlt run failure"):
            mat.write_to_secondary_targets(targets, pl.DataFrame({"a": [1]}), "t")


class TestWriteToSecondaryTargetsTableName:
    def test_table_name_override_in_target_wins(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        targets = [
            {
                "format": "dlt",
                "dlt_destination": "duckdb",
                "table_name": "custom_table",
            }
        ]
        mat.write_to_secondary_targets(targets, pl.DataFrame({"a": [1]}), "fallback_table")
        assert rec.resource_calls[0]["name"] == "custom_table"

    def test_falls_back_to_caller_table_name_when_omitted(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        targets = [{"format": "dlt", "dlt_destination": "duckdb"}]
        mat.write_to_secondary_targets(targets, pl.DataFrame({"a": [1]}), "fallback_table")
        assert rec.resource_calls[0]["name"] == "fallback_table"


# ──────────────────────────────────────────────────────────────────────────────
# _run_secondary_targets — private helper used inside materialize_dataframe
# ──────────────────────────────────────────────────────────────────────────────


class TestRunSecondaryTargetsGuards:
    """The cheap early-exit guards on the private helper."""

    def test_no_secondary_targets_attribute_returns_result_unchanged(self):
        contract = SimpleNamespace(dataset="orders")
        mat_cfg = SimpleNamespace()  # no `secondary_targets` attribute
        result = {"foo": "bar"}
        got = mat._run_secondary_targets(mat_cfg, contract, pl.DataFrame({"a": [1]}), "append", [], 1, result)
        assert got is result
        assert "secondary_writes" not in got

    def test_empty_secondary_targets_returns_result_unchanged(self):
        contract = SimpleNamespace(dataset="orders")
        mat_cfg = SimpleNamespace(secondary_targets=[])
        result = {"foo": "bar"}
        got = mat._run_secondary_targets(mat_cfg, contract, pl.DataFrame({"a": [1]}), "append", [], 1, result)
        assert got is result
        assert "secondary_writes" not in got

    def test_non_list_secondary_targets_is_ignored(self):
        contract = SimpleNamespace(dataset="orders")
        mat_cfg = SimpleNamespace(secondary_targets={"format": "dlt"})  # dict, not list
        result: dict = {}
        got = mat._run_secondary_targets(mat_cfg, contract, pl.DataFrame({"a": [1]}), "append", [], 1, result)
        assert "secondary_writes" not in got


class TestRunSecondaryTargetsTableResolution:
    """The 'auto' table_name → contract.dataset shortcut."""

    @pytest.mark.parametrize("raw", [None, "auto", ""])
    def test_auto_uses_contract_dataset(self, monkeypatch, raw):
        rec = _install_fake_dlt(monkeypatch)
        contract = SimpleNamespace(dataset="orders_dataset")
        mat_cfg = SimpleNamespace(secondary_targets=[{"format": "dlt", "dlt_destination": "duckdb", "table_name": raw}])
        result: dict = {}
        mat._run_secondary_targets(mat_cfg, contract, pl.DataFrame({"a": [1]}), "append", [], 1, result)
        assert rec.resource_calls[0]["name"] == "orders_dataset"

    def test_explicit_table_name_overrides_dataset(self, monkeypatch):
        rec = _install_fake_dlt(monkeypatch)
        contract = SimpleNamespace(dataset="orders_dataset")
        mat_cfg = SimpleNamespace(
            secondary_targets=[{"format": "dlt", "dlt_destination": "duckdb", "table_name": "explicit"}]
        )
        mat._run_secondary_targets(mat_cfg, contract, pl.DataFrame({"a": [1]}), "append", [], 1, {})
        assert rec.resource_calls[0]["name"] == "explicit"


class TestRunSecondaryTargetsErrorPropagation:
    def test_failure_captured_in_result_when_fail_on_error_false(self, monkeypatch):
        _install_fake_dlt(monkeypatch, raise_on_run=True)
        contract = SimpleNamespace(dataset="orders")
        mat_cfg = SimpleNamespace(
            secondary_targets=[{"format": "dlt", "dlt_destination": "duckdb", "fail_on_error": False}]
        )
        result: dict = {}
        got = mat._run_secondary_targets(mat_cfg, contract, pl.DataFrame({"a": [1]}), "append", [], 1, result)
        assert len(got["secondary_writes"]) == 1
        assert "error" in got["secondary_writes"][0]

    def test_failure_reraises_when_fail_on_error_true(self, monkeypatch):
        _install_fake_dlt(monkeypatch, raise_on_run=True)
        contract = SimpleNamespace(dataset="orders")
        mat_cfg = SimpleNamespace(
            secondary_targets=[{"format": "dlt", "dlt_destination": "duckdb", "fail_on_error": True}]
        )
        with pytest.raises(RuntimeError, match="simulated dlt run failure"):
            mat._run_secondary_targets(mat_cfg, contract, pl.DataFrame({"a": [1]}), "append", [], 1, {})

    def test_unsupported_format_records_no_write(self, monkeypatch):
        contract = SimpleNamespace(dataset="orders")
        mat_cfg = SimpleNamespace(
            secondary_targets=[{"format": "parquet"}]  # not dlt → branch logs and skips
        )
        result: dict = {}
        got = mat._run_secondary_targets(mat_cfg, contract, pl.DataFrame({"a": [1]}), "append", [], 1, result)
        # The branch only logs a warning; secondary_writes is initialised but stays empty
        assert got["secondary_writes"] == []


# ──────────────────────────────────────────────────────────────────────────────
# optimize_delta — Delta compaction + vacuum
# ──────────────────────────────────────────────────────────────────────────────


def _install_fake_deltalake(monkeypatch: pytest.MonkeyPatch, *, raise_on_init: bool = False) -> MagicMock:
    """Inject a fake ``deltalake`` module exposing a mocked ``DeltaTable``."""
    fake_dt = MagicMock(name="DeltaTable")
    if raise_on_init:
        fake_dt.side_effect = RuntimeError("table not found")
    else:
        instance = MagicMock(name="DeltaTableInstance")
        instance.optimize.compact.return_value = {"files_added": 1, "files_removed": 4}
        instance.vacuum.return_value = ["file_a", "file_b"]
        fake_dt.return_value = instance

    fake_mod = types.ModuleType("deltalake")
    fake_mod.DeltaTable = fake_dt  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "deltalake", fake_mod)
    return fake_dt


class TestOptimizeDelta:
    def test_happy_path_returns_compaction_and_vacuum_blocks(self, monkeypatch):
        _install_fake_deltalake(monkeypatch)
        result = mat.optimize_delta("/tmp/table")
        assert result["target"] == "/tmp/table"
        assert result["compaction"]["status"] == "ok"
        assert result["compaction"]["metrics"] == {"files_added": 1, "files_removed": 4}
        assert result["vacuum"]["status"] == "ok"
        assert result["vacuum"]["files_removed"] == 2
        assert result["vacuum"]["retention_hours"] == 168

    def test_vacuum_skipped_when_disabled(self, monkeypatch):
        _install_fake_deltalake(monkeypatch)
        result = mat.optimize_delta("/tmp/table", vacuum=False)
        assert "compaction" in result
        assert "vacuum" not in result

    def test_custom_retention_hours_is_passed_through(self, monkeypatch):
        fake_dt = _install_fake_deltalake(monkeypatch)
        mat.optimize_delta("/tmp/table", vacuum_retention_hours=24)
        instance = fake_dt.return_value
        instance.vacuum.assert_called_once()
        kwargs = instance.vacuum.call_args.kwargs
        assert kwargs["retention_hours"] == 24
        assert kwargs["enforce_retention_duration"] is True
        assert kwargs["dry_run"] is False

    def test_storage_options_forwarded_to_delta_table(self, monkeypatch):
        fake_dt = _install_fake_deltalake(monkeypatch)
        mat.optimize_delta("abfss://x@y.dfs.core.windows.net/z", storage_options={"k": "v"})
        fake_dt.assert_called_once_with("abfss://x@y.dfs.core.windows.net/z", storage_options={"k": "v"})

    def test_compact_returning_non_dict_yields_empty_metrics(self, monkeypatch):
        _install_fake_deltalake(monkeypatch)
        instance = sys.modules["deltalake"].DeltaTable.return_value
        instance.optimize.compact.return_value = "some-non-dict-string"
        result = mat.optimize_delta("/tmp/table", vacuum=False)
        assert result["compaction"]["metrics"] == {}

    def test_exception_during_compaction_is_captured(self, monkeypatch):
        _install_fake_deltalake(monkeypatch, raise_on_init=True)
        result = mat.optimize_delta("/tmp/table")
        assert "error" in result
        assert "table not found" in result["error"]
        # Compaction never recorded since the failure happened on DeltaTable() init
        assert "compaction" not in result

    def test_import_error_when_deltalake_missing(self, monkeypatch):
        # Make `from deltalake import DeltaTable` fail at import time.
        monkeypatch.setitem(sys.modules, "deltalake", None)
        with pytest.raises(ImportError, match="deltalake"):
            mat.optimize_delta("/tmp/table")


# ──────────────────────────────────────────────────────────────────────────────
# _maybe_compact_delta — auto-mode wrapper called after Delta writes
# ──────────────────────────────────────────────────────────────────────────────


class TestMaybeCompactDelta:
    def test_returns_none_when_contract_has_no_materialization(self):
        contract = SimpleNamespace()
        assert mat._maybe_compact_delta("/tmp/x", contract) is None

    def test_returns_none_when_materialization_has_no_compaction(self):
        contract = SimpleNamespace(materialization=SimpleNamespace(compaction=None))
        assert mat._maybe_compact_delta("/tmp/x", contract) is None

    def test_returns_none_when_compaction_auto_is_false(self):
        contract = SimpleNamespace(materialization=SimpleNamespace(compaction={"auto": False}))
        assert mat._maybe_compact_delta("/tmp/x", contract) is None

    def test_auto_true_invokes_optimize_delta_with_defaults(self, monkeypatch):
        calls = []

        def fake_optimize_delta(target, *, vacuum, vacuum_retention_hours):
            calls.append({"target": target, "vacuum": vacuum, "retention": vacuum_retention_hours})
            return {"ok": True}

        monkeypatch.setattr(mat, "optimize_delta", fake_optimize_delta)
        contract = SimpleNamespace(materialization=SimpleNamespace(compaction={"auto": True}))
        result = mat._maybe_compact_delta("/tmp/x", contract)
        assert result == {"ok": True}
        assert calls == [{"target": "/tmp/x", "vacuum": True, "retention": 168}]

    def test_compaction_config_overrides_are_passed_through(self, monkeypatch):
        calls = []

        def fake_optimize_delta(target, *, vacuum, vacuum_retention_hours):
            calls.append({"vacuum": vacuum, "retention": vacuum_retention_hours})
            return {}

        monkeypatch.setattr(mat, "optimize_delta", fake_optimize_delta)
        contract = SimpleNamespace(
            materialization=SimpleNamespace(compaction={"auto": True, "vacuum": False, "vacuum_retention_hours": 24})
        )
        mat._maybe_compact_delta("/tmp/x", contract)
        assert calls == [{"vacuum": False, "retention": 24}]
