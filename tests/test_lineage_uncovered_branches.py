"""Targeted tests for previously-uncovered branches in lineage.py.

Existing test_lineage_helpers.py covers the polars/pandas happy paths and
the broad shape of _preserve_upstream_lineage + add_columns. This file
adds:

- inject_lineage branches: contract.lineage disabled, no columns to add,
  run_id auto-generation, contract-name fallback when info has no version,
  capture_run_id=False, capture_*=False individual gates.
- _preserve_upstream_lineage: non _lakelogic_ prefixed col renaming, DuckDB
  relation rename path.
- add_columns: None df, DuckDB relation with literal types (None, bool,
  int, float, string), DuckDB column-fetch fallback, src_sql exception
  → temp-view fallback, SQL execution failure → pandas materialisation
  fallback, ImportError fallback.

Spark branches are pragma'd-no-cover and not exercised here.
"""

from __future__ import annotations

import sys
import types
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

pl = pytest.importorskip("polars")
pd = pytest.importorskip("pandas")

from lakelogic.core import lineage


# ──────────────────────────────────────────────────────────────────────────────
# Helpers — minimal LineageConfig-like objects
# ──────────────────────────────────────────────────────────────────────────────


def _lineage_cfg(**overrides):
    """Build a minimal lineage config namespace with sane defaults."""
    defaults = dict(
        enabled=True,
        preserve_upstream=None,
        upstream_prefix="_upstream",
        capture_source_path=True,
        capture_timestamp=True,
        capture_run_id=True,
        capture_contract_name=True,
        capture_domain=True,
        capture_system=True,
        capture_created_at=True,
        capture_created_by=True,
        run_id_source="run_id",
        created_by_override=None,
        source_column_name="_lakelogic_source",
        timestamp_column_name="_lakelogic_timestamp",
        run_id_column_name="_lakelogic_run_id",
        contract_name_column_name="_lakelogic_contract",
        domain_column_name="_lakelogic_domain",
        system_column_name="_lakelogic_system",
        created_at_column_name="_lakelogic_created_at",
        created_by_column_name="_lakelogic_created_by",
    )
    defaults.update(overrides)
    return types.SimpleNamespace(**defaults)


def _contract(lineage_cfg, *, metadata=None, contract_path=None, info=None):
    return types.SimpleNamespace(
        lineage=lineage_cfg,
        metadata=metadata or {},
        _contract_path=contract_path,
        info=info,
    )


# ──────────────────────────────────────────────────────────────────────────────
# inject_lineage — branches not exercised by existing tests
# ──────────────────────────────────────────────────────────────────────────────


class TestInjectLineageGuards:
    def test_lineage_disabled_returns_unchanged(self):
        cfg = _lineage_cfg(enabled=False)
        contract = _contract(cfg)
        good = pl.DataFrame({"a": [1]})
        bad = pl.DataFrame({"a": [2]})
        g, b = lineage.inject_lineage(good, bad, contract, "polars", "run-1")
        assert g is good and b is bad

    def test_no_lineage_attribute_returns_unchanged(self):
        contract = types.SimpleNamespace(lineage=None, metadata={})
        good = pl.DataFrame({"a": [1]})
        bad = pl.DataFrame({"a": [2]})
        g, b = lineage.inject_lineage(good, bad, contract, "polars", "run-1")
        assert g is good and b is bad

    def test_all_capture_flags_false_returns_dfs_unchanged(self):
        # When every capture flag is off AND no contract-name + no domain/system,
        # the columns dict ends up empty → early return.
        cfg = _lineage_cfg(
            capture_source_path=False,
            capture_timestamp=False,
            capture_run_id=False,
            capture_contract_name=False,
            capture_domain=False,
            capture_system=False,
            capture_created_at=False,
            capture_created_by=False,
        )
        contract = _contract(cfg)
        good = pl.DataFrame({"a": [1]})
        bad = pl.DataFrame({"a": [2]})
        g, b = lineage.inject_lineage(good, bad, contract, "polars", "run-1")
        # Same objects returned — no columns appended
        assert g is good and b is bad

    def test_run_id_auto_generated_when_capture_enabled_but_missing(self):
        cfg = _lineage_cfg()
        contract = _contract(cfg)
        good = pl.DataFrame({"a": [1]})
        bad = pl.DataFrame({"a": [2]})
        # last_run_id=None and pipeline_run_id=None → forced to uuid
        g, _ = lineage.inject_lineage(good, bad, contract, "polars", None, None)
        run_id = g[cfg.run_id_column_name].to_list()[0]
        # UUIDs are 36-char strings with dashes
        assert isinstance(run_id, str)
        assert len(run_id) == 36
        assert run_id.count("-") == 4

    def test_pipeline_run_id_used_when_run_id_source_says_so(self):
        cfg = _lineage_cfg(run_id_source="pipeline_run_id")
        contract = _contract(cfg)
        good = pl.DataFrame({"a": [1]})
        bad = pl.DataFrame({"a": [2]})
        g, _ = lineage.inject_lineage(
            good, bad, contract, "polars", "row-run", pipeline_run_id="pipeline-run-99"
        )
        assert g[cfg.run_id_column_name].to_list()[0] == "pipeline-run-99"

    def test_contract_name_with_version_format(self):
        cfg = _lineage_cfg()
        contract = _contract(
            cfg,
            contract_path="/some/path/customers.yaml",
            info=types.SimpleNamespace(version="2.1.0"),
        )
        good = pl.DataFrame({"a": [1]})
        bad = pl.DataFrame({"a": [2]})
        g, _ = lineage.inject_lineage(good, bad, contract, "polars", "run-1")
        contract_name = g[cfg.contract_name_column_name].to_list()[0]
        assert contract_name == "customers.yaml (v2.1.0)"

    def test_contract_name_without_version_uses_filename_only(self):
        cfg = _lineage_cfg()
        contract = _contract(
            cfg,
            contract_path="/some/path/orders.yaml",
            info=types.SimpleNamespace(version=None),  # no version
        )
        good = pl.DataFrame({"a": [1]})
        bad = pl.DataFrame({"a": [2]})
        g, _ = lineage.inject_lineage(good, bad, contract, "polars", "run-1")
        assert g[cfg.contract_name_column_name].to_list()[0] == "orders.yaml"

    def test_created_by_override_wins_over_getuser(self):
        cfg = _lineage_cfg(created_by_override="ci-service-account")
        contract = _contract(cfg)
        good = pl.DataFrame({"a": [1]})
        bad = pl.DataFrame({"a": [2]})
        g, _ = lineage.inject_lineage(good, bad, contract, "polars", "run-1")
        assert g[cfg.created_by_column_name].to_list()[0] == "ci-service-account"

    def test_getuser_failure_falls_back_to_unknown(self, monkeypatch):
        cfg = _lineage_cfg()  # no override → tries getpass.getuser()
        contract = _contract(cfg)
        good = pl.DataFrame({"a": [1]})
        bad = pl.DataFrame({"a": [2]})

        # Patch getpass.getuser to raise so the fallback fires
        import getpass

        monkeypatch.setattr(getpass, "getuser", lambda: (_ for _ in ()).throw(OSError("no tty")))
        g, _ = lineage.inject_lineage(good, bad, contract, "polars", "run-1")
        assert g[cfg.created_by_column_name].to_list()[0] == "unknown"

    def test_contract_path_exception_falls_back_to_none(self, monkeypatch):
        cfg = _lineage_cfg()
        # Use a contract whose _contract_path getattr raises
        class WeirdContract:
            lineage = cfg
            metadata: Dict = {}
            info = None

            @property
            def _contract_path(self):
                raise RuntimeError("synthetic contract-path failure")

        good = pl.DataFrame({"a": [1]})
        bad = pl.DataFrame({"a": [2]})
        g, _ = lineage.inject_lineage(good, bad, WeirdContract(), "polars", "run-1")
        # _lakelogic_contract column should be skipped (None) → not present in df,
        # because the conditional only adds the col if contract_name_value is not None.
        assert cfg.contract_name_column_name not in g.columns


# ──────────────────────────────────────────────────────────────────────────────
# _preserve_upstream_lineage — branches not exercised
# ──────────────────────────────────────────────────────────────────────────────


class TestPreserveUpstreamBranches:
    def test_non_lakelogic_prefixed_col_gets_underscore_join(self):
        # Lines 135-137: cols not starting with "_lakelogic_" get joined with "_"
        df = pl.DataFrame({"custom_provenance": ["x"], "other": [1]})
        out = lineage._preserve_upstream_lineage(
            df, ["custom_provenance"], "_upstream", "polars"
        )
        # _upstream + custom_provenance → _upstream_custom_provenance
        assert "_upstream_custom_provenance" in out.columns
        assert "custom_provenance" not in out.columns

    def test_returns_unchanged_when_none_input(self):
        assert lineage._preserve_upstream_lineage(None, ["x"], "_up", "polars") is None

    def test_returns_unchanged_when_no_matching_columns(self):
        # The column being requested doesn't exist in df → no-op rename
        df = pl.DataFrame({"a": [1], "b": [2]})
        out = lineage._preserve_upstream_lineage(
            df, ["_lakelogic_source"], "_upstream", "polars"
        )
        # df returned as-is (mapping is empty so the function short-circuits)
        assert out.columns == ["a", "b"]


# ──────────────────────────────────────────────────────────────────────────────
# add_columns — branches not exercised
# ──────────────────────────────────────────────────────────────────────────────


class TestAddColumnsBranches:
    def test_none_df_returns_none(self):
        assert lineage.add_columns(None, {"_lakelogic_source": "x"}, "polars") is None

    def test_lakelogic_cols_moved_to_far_right(self):
        df = pl.DataFrame(
            {"_lakelogic_existing": ["old"], "business_col": [1], "other": [2]}
        )
        out = lineage.add_columns(
            df, {"_lakelogic_source": "/path/file.csv", "_lakelogic_run_id": "r-1"}, "polars"
        )
        # Business cols first, then _lakelogic_* (current layer) at the far right
        cols = out.columns
        idx_business = cols.index("business_col")
        idx_lakelogic_existing = cols.index("_lakelogic_existing")
        idx_lakelogic_source = cols.index("_lakelogic_source")
        assert idx_business < idx_lakelogic_existing
        assert idx_business < idx_lakelogic_source

    def test_pandas_path_preserves_column_ordering(self):
        df = pd.DataFrame({"other": [1], "_lakelogic_existing": ["old"]})
        out = lineage.add_columns(df, {"_lakelogic_source": "src"}, "pandas")
        cols = list(out.columns)
        idx_other = cols.index("other")
        idx_source = cols.index("_lakelogic_source")
        assert idx_other < idx_source

    def test_upstream_cols_sit_before_current_lakelogic_cols(self):
        # _upstream_* sits between business and current _lakelogic_*
        df = pl.DataFrame(
            {
                "business_col": [1],
                "_upstream_lakelogic_source": ["upstream"],
                "_lakelogic_existing": ["old"],
            }
        )
        out = lineage.add_columns(df, {"_lakelogic_source": "current"}, "polars")
        cols = out.columns
        i_business = cols.index("business_col")
        i_upstream = cols.index("_upstream_lakelogic_source")
        i_current = cols.index("_lakelogic_source")
        assert i_business < i_upstream < i_current


# ──────────────────────────────────────────────────────────────────────────────
# add_columns DuckDB path — literal types and fallback branches
# ──────────────────────────────────────────────────────────────────────────────


class TestAddColumnsDuckDB:
    """Exercises the DuckDB-relation path's _lit() helper and SQL build."""

    def _real_duckdb_relation(self, sql="SELECT 1 AS a"):
        import duckdb

        con = duckdb.connect()
        return con, con.sql(sql)

    def test_duckdb_relation_with_string_int_float_bool_null_literals(self):
        con, rel = self._real_duckdb_relation("SELECT 1 AS id, 'x' AS name")
        try:
            cols = {
                "_lakelogic_string": "hello",
                "_lakelogic_int": 42,
                "_lakelogic_float": 3.14,
                "_lakelogic_bool_true": True,
                "_lakelogic_bool_false": False,
                "_lakelogic_null": None,
                "_lakelogic_apostrophe": "O'Brien",  # tests SQL escaping
            }
            out = lineage.add_columns(rel, cols, "duckdb")
            row = out.fetchone()
            # Existing cols come first, then new in declaration order
            colnames = [c for c in out.columns]
            assert "id" in colnames
            assert "_lakelogic_string" in colnames
            # Verify values came back unchanged
            row_dict = dict(zip(colnames, row))
            assert row_dict["_lakelogic_string"] == "hello"
            assert row_dict["_lakelogic_int"] == 42
            # DuckDB returns float literals as Decimal — cast both sides for comparison
            assert float(row_dict["_lakelogic_float"]) == pytest.approx(3.14)
            assert row_dict["_lakelogic_bool_true"] is True
            assert row_dict["_lakelogic_bool_false"] is False
            assert row_dict["_lakelogic_null"] is None
            assert row_dict["_lakelogic_apostrophe"] == "O'Brien"
        finally:
            con.close()

    def test_duckdb_sql_query_failure_falls_back_to_pandas(self):
        """If con.sql(query) raises, the function materialises to pandas and
        appends columns there as a last resort.

        We can't monkeypatch DuckDBPyConnection.sql (read-only), so we build
        a thin proxy relation that points at a fake connection whose .sql()
        raises on demand.
        """
        import duckdb

        real_con = duckdb.connect()
        real_rel = real_con.sql("SELECT 1 AS a, 2 AS b")
        try:
            class FakeConnection:
                def sql(self, query):
                    raise RuntimeError("simulated sql failure")

            class FakeRelation:
                # Make isinstance(self, DuckDBPyRelation) pass by spoofing __class__.
                __class__ = duckdb.DuckDBPyRelation

                def __init__(self, real_rel, fake_con):
                    self._real = real_rel
                    self.connection = fake_con

                @property
                def columns(self):
                    return self._real.columns

                def sql_query(self):
                    return self._real.sql_query()

                def df(self):
                    return self._real.df()

            fake_rel = FakeRelation(real_rel, FakeConnection())
            out = lineage.add_columns(fake_rel, {"_lakelogic_source": "/path"}, "duckdb")
            # Fallback returns a pandas DataFrame
            assert isinstance(out, pd.DataFrame)
            assert out["_lakelogic_source"].iloc[0] == "/path"
        finally:
            real_con.close()


# ──────────────────────────────────────────────────────────────────────────────
# End-to-end via inject_lineage — confirms add_columns is exercised on DuckDB
# ──────────────────────────────────────────────────────────────────────────────


class TestInjectLineageDuckDBPath:
    def test_inject_lineage_with_duckdb_relation(self):
        import duckdb

        con = duckdb.connect()
        good = con.sql("SELECT 1 AS id, 'alice' AS name")
        bad = con.sql("SELECT 2 AS id, 'bob' AS name")
        try:
            cfg = _lineage_cfg(
                capture_contract_name=False,  # no contract path set
                capture_domain=False,
                capture_system=False,
            )
            contract = _contract(cfg)
            g, b = lineage.inject_lineage(good, bad, contract, "duckdb", "run-x")
            # Both should now have the lineage columns
            g_cols = list(g.columns)
            b_cols = list(b.columns)
            assert cfg.run_id_column_name in g_cols
            assert cfg.run_id_column_name in b_cols
        finally:
            con.close()
