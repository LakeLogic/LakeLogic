"""
Integration tests for the pipeline driver — DuckDB-backed end-to-end pipeline.

These tests exercise the full pipeline lifecycle (bronze → silver → gold) using
DuckDB as the engine, validating that the refactored driver properly delegates
to the extracted modules (observability, cli_parsers, run_log_reader).

Coverage:
    • DuckDB engine pipeline run (bronze ingest, silver transform, gold aggregate)
    • Summary table write to DuckDB via observability.write_summary_table
    • Metrics JSON emission via observability.emit_metrics
    • RunLogReader last-success query via DuckDB backend
    • Prometheus formatting of metrics snapshot
    • CLI parser round-trips via cli_parsers
    • Backfill window generation
    • Entity and contract filtering
    • Resume / state persistence
"""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
import yaml

from lakelogic.cli import driver
from lakelogic.cli.cli_parsers import (
    build_backfill_windows,
)
from lakelogic.cli.observability import (
    emit_metrics,
    finalize_summary,
    flatten_summary,
    format_prometheus,
)
from lakelogic.cli.run_log_reader import RunLogReader

# ── Helpers ──────────────────────────────────────────────────────────────────

def _write_csv(path: Path, rows: list[dict]) -> Path:
    """Write a list of dicts to a CSV file via pandas."""
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _make_contract(
    dataset: str,
    source_dir: Path,
    out_dir: Path,
    *,
    pattern: str = "*.csv",
    load_mode: str = "full",
    fields: list[dict] | None = None,
    transformations: list[dict] | None = None,
    quality: dict | None = None,
    links: list[dict] | None = None,
    upstream: list[str] | None = None,
    materialization_strategy: str = "overwrite",
    mat_format: str = "csv",
    quarantine: bool = False,
    metadata: dict | None = None,
) -> dict:
    """Build a contract dict for YAML serialization."""
    contract: dict = {
        "version": "1.0.0",
        "dataset": dataset,
        "source": {
            "type": "landing",
            "path": str(source_dir),
            "pattern": pattern,
            "load_mode": load_mode,
        },
        "materialization": {
            "strategy": materialization_strategy,
            "target_path": str(out_dir / dataset),
            "format": mat_format,
        },
    }
    if fields:
        contract["model"] = {"fields": fields}
    if transformations:
        contract["transformations"] = transformations
    if quality:
        contract["quality"] = quality
    if links:
        contract["links"] = links
    if upstream:
        contract["upstream"] = upstream
    if quarantine:
        contract["quarantine"] = {"enabled": True, "target": str(out_dir / f"{dataset}_quarantine")}
    if metadata:
        contract["metadata"] = metadata
    return contract


def _write_contract(path: Path, contract: dict) -> Path:
    """Write a contract dict to YAML."""
    path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    return path


def _write_registry(path: Path, entries: list[dict]) -> Path:
    """Write a registry YAML."""
    registry = {"entries": entries}
    path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    return path


# ── DuckDB End-to-End Pipeline ───────────────────────────────────────────────

@pytest.mark.skip(reason="DuckDB execution engine is deprecated")
class TestDuckDBPipelineE2E:
    """End-to-end pipeline test running on the DuckDB engine."""

    def test_bronze_silver_gold_full_pipeline(self, tmp_path: Path) -> None:
        """Run a full bronze → silver → gold pipeline on DuckDB."""
        landing = tmp_path / "landing"
        out = tmp_path / "out"
        landing.mkdir()
        out.mkdir()

        # Landing data
        _write_csv(
            landing / "orders_2026-02-01.csv",
            [
                {"order_id": "O-1", "customer_id": "C-1", "amount": 100.0, "status": "COMPLETED"},
                {"order_id": "O-2", "customer_id": "C-1", "amount": 50.0, "status": "PENDING"},
                {"order_id": "O-3", "customer_id": "C-2", "amount": 200.0, "status": "COMPLETED"},
            ],
        )

        # Bronze contract
        bronze = _make_contract(
            "bronze_orders",
            landing,
            out,
            fields=[
                {"name": "order_id", "type": "string"},
                {"name": "customer_id", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "status", "type": "string"},
            ],
            quarantine=True,
        )
        bronze_path = _write_contract(tmp_path / "bronze_orders.yaml", bronze)

        # Silver contract — adds quality rule + derivation
        silver = _make_contract(
            "silver_orders",
            out / "bronze_orders",
            out,
            pattern="data.csv",
            upstream=["bronze_orders"],
            fields=[
                {"name": "order_id", "type": "string", "required": True},
                {"name": "customer_id", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "status", "type": "string"},
            ],
            transformations=[
                {"derive": {"field": "is_completed", "sql": "status = 'COMPLETED'"}},
            ],
            quality={
                "row_rules": [
                    {"not_null": {"field": "order_id", "name": "order_id_not_null"}},
                ],
            },
            quarantine=True,
        )
        silver_path = _write_contract(tmp_path / "silver_orders.yaml", silver)

        # Gold contract — aggregate
        gold = _make_contract(
            "gold_order_summary",
            out / "silver_orders",
            out,
            pattern="data.csv",
            upstream=["silver_orders"],
            transformations=[
                {"sql": "SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS order_count FROM source GROUP BY customer_id"},
            ],
        )
        gold_path = _write_contract(tmp_path / "gold_order_summary.yaml", gold)

        # Registries
        system_reg = _write_registry(
            tmp_path / "_system_registry.yaml",
            [
                {
                    "entity": "orders",
                    "enabled": True,
                    "contracts": {
                        "bronze": bronze_path.name,
                        "silver": silver_path.name,
                    },
                },
            ],
        )
        gold_reg = _write_registry(
            tmp_path / "_gold_registry.yaml",
            [{"entity": "analytics", "enabled": True, "contracts": {"gold": gold_path.name}}],
        )

        summary_path = tmp_path / "summary.json"
        drv = driver.PipelineDriver(
            "duckdb", max_workers=1, summary_path=summary_path, fail_fast=True,
        )
        drv.run(
            {"system": system_reg, "gold": gold_reg},
            ["bronze", "silver", "gold"],
            driver.Window(None, None, "full"),
            False,
        )

        # Assertions
        assert (out / "bronze_orders" / "data.csv").exists()
        assert (out / "silver_orders" / "data.csv").exists()
        assert (out / "gold_order_summary" / "data.csv").exists()

        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["metrics"]["successful"] == 3
        assert summary["metrics"]["failed"] == 0

        # Verify gold output
        gold_df = pd.read_csv(out / "gold_order_summary" / "data.csv")
        assert len(gold_df) == 2
        assert set(gold_df.columns) >= {"customer_id", "total_amount", "order_count"}

    def test_duckdb_summary_table_write(self, tmp_path: Path) -> None:
        """Verify summary table is written to DuckDB database file."""
        landing = tmp_path / "landing"
        out = tmp_path / "out"
        landing.mkdir()
        out.mkdir()

        _write_csv(landing / "data.csv", [{"id": 1, "val": 10}])
        contract = _make_contract(
            "bronze_data",
            landing,
            out,
            pattern="data.csv",
            fields=[{"name": "id", "type": "integer"}, {"name": "val", "type": "integer"}],
        )
        _write_contract(tmp_path / "contract.yaml", contract)
        _write_registry(
            tmp_path / "_registry.yaml",
            [{"entity": "test", "enabled": True, "contracts": {"bronze": "contract.yaml"}}],
        )

        db_path = tmp_path / "runs.duckdb"
        drv = driver.PipelineDriver(
            "duckdb",
            max_workers=1,
            summary_table="pipeline_runs",
            summary_backend="duckdb",
            summary_database=str(db_path),
            fail_fast=True,
        )
        drv.run(
            {"system": tmp_path / "_registry.yaml"},
            ["bronze"],
            driver.Window(None, None, "full"),
            False,
        )

        import duckdb
        con = duckdb.connect(str(db_path))
        try:
            rows = con.execute("SELECT run_id, successful, failed FROM pipeline_runs").fetchall()
        finally:
            con.close()

        assert len(rows) == 1
        assert rows[0][1] == 1  # successful
        assert rows[0][2] == 0  # failed

    def test_duckdb_metrics_emission(self, tmp_path: Path) -> None:
        """Verify metrics JSON is emitted during DuckDB pipeline run."""
        landing = tmp_path / "landing"
        out = tmp_path / "out"
        landing.mkdir()
        out.mkdir()

        _write_csv(landing / "data.csv", [{"id": 1, "val": 10}])
        contract = _make_contract(
            "bronze_data",
            landing,
            out,
            pattern="data.csv",
            fields=[{"name": "id", "type": "integer"}, {"name": "val", "type": "integer"}],
        )
        _write_contract(tmp_path / "contract.yaml", contract)
        _write_registry(
            tmp_path / "_registry.yaml",
            [{"entity": "data", "enabled": True, "contracts": {"bronze": "contract.yaml"}}],
        )

        metrics_path = tmp_path / "metrics.json"
        drv = driver.PipelineDriver(
            "duckdb",
            max_workers=1,
            metrics_path=metrics_path,
            metrics_tags={"env": "ci", "team": "data-eng"},
            fail_fast=True,
        )
        drv.run(
            {"system": tmp_path / "_registry.yaml"},
            ["bronze"],
            driver.Window(None, None, "full"),
            False,
        )

        assert metrics_path.exists()
        payload = json.loads(metrics_path.read_text(encoding="utf-8"))
        assert payload["metrics"]["successful"] == 1
        assert payload["tags"]["env"] == "ci"
        assert payload["tags"]["team"] == "data-eng"


# ── Entity and Contract Filtering ────────────────────────────────────────────

class TestDriverFiltering:
    """Tests for entity and contract level filtering."""

    def _setup_multi_entity(self, tmp_path: Path):
        """Set up a registry with two entities for filtering tests."""
        landing = tmp_path / "landing"
        out = tmp_path / "out"
        landing.mkdir()
        out.mkdir()

        _write_csv(landing / "orders.csv", [{"id": 1, "val": 10}])
        _write_csv(landing / "customers.csv", [{"id": 1, "name": "Alice"}])

        contract_a = _make_contract(
            "bronze_orders", landing, out, pattern="orders.csv",
            fields=[{"name": "id", "type": "integer"}, {"name": "val", "type": "integer"}],
        )
        contract_b = _make_contract(
            "bronze_customers", landing, out, pattern="customers.csv",
            fields=[{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}],
        )
        _write_contract(tmp_path / "bronze_orders.yaml", contract_a)
        _write_contract(tmp_path / "bronze_customers.yaml", contract_b)
        _write_registry(
            tmp_path / "_registry.yaml",
            [
                {"entity": "orders", "enabled": True, "contracts": {"bronze": "bronze_orders.yaml"}},
                {"entity": "customers", "enabled": True, "contracts": {"bronze": "bronze_customers.yaml"}},
            ],
        )
        return out

    def test_entity_filter_runs_only_selected(self, tmp_path: Path) -> None:
        """Only the 'orders' entity should run when entity_filter is set."""
        out = self._setup_multi_entity(tmp_path)

        summary_path = tmp_path / "summary.json"
        drv = driver.PipelineDriver("polars", max_workers=1, summary_path=summary_path, fail_fast=True)
        drv.run(
            {"system": tmp_path / "_registry.yaml"},
            ["bronze"],
            driver.Window(None, None, "full"),
            False,
            entity_filter={"orders"},
        )

        assert (out / "bronze_orders" / "data.csv").exists()
        assert not (out / "bronze_customers" / "data.csv").exists()

        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["metrics"]["successful"] == 1
        assert summary["metrics"]["total_contracts"] == 1

    def test_contract_filter(self, tmp_path: Path) -> None:
        """Only the specified contract path should run."""
        out = self._setup_multi_entity(tmp_path)
        target_contract = (tmp_path / "bronze_customers.yaml").resolve()

        summary_path = tmp_path / "summary.json"
        drv = driver.PipelineDriver("polars", max_workers=1, summary_path=summary_path, fail_fast=True)
        drv.run(
            {"system": tmp_path / "_registry.yaml"},
            ["bronze"],
            driver.Window(None, None, "full"),
            False,
            contract_filter={target_contract},
        )

        assert not (out / "bronze_orders" / "data.csv").exists()
        assert (out / "bronze_customers" / "data.csv").exists()


# ── Resume / State Persistence ───────────────────────────────────────────────

class TestDriverResume:
    """Tests for the resume (state persistence) feature."""

    def test_resume_skips_completed(self, tmp_path: Path) -> None:
        """A second run with resume=True should skip already completed datasets."""
        landing = tmp_path / "landing"
        out = tmp_path / "out"
        landing.mkdir()
        out.mkdir()

        _write_csv(landing / "data.csv", [{"id": 1, "val": 10}])
        contract = _make_contract(
            "bronze_data", landing, out, pattern="data.csv",
            fields=[{"name": "id", "type": "integer"}, {"name": "val", "type": "integer"}],
        )
        _write_contract(tmp_path / "contract.yaml", contract)
        _write_registry(
            tmp_path / "_registry.yaml",
            [{"entity": "data", "enabled": True, "contracts": {"bronze": "contract.yaml"}}],
        )

        state_path = tmp_path / "state.json"
        summary_path = tmp_path / "summary.json"

        # First run
        drv = driver.PipelineDriver(
            "polars", max_workers=1, summary_path=summary_path,
            state_path=state_path, fail_fast=True,
        )
        drv.run(
            {"system": tmp_path / "_registry.yaml"},
            ["bronze"],
            driver.Window(None, None, "full"),
            False,
        )
        assert state_path.exists()

        # Second run with resume — should skip
        drv2 = driver.PipelineDriver(
            "polars", max_workers=1, summary_path=tmp_path / "summary2.json",
            state_path=state_path, resume=True, fail_fast=True,
        )
        drv2.run(
            {"system": tmp_path / "_registry.yaml"},
            ["bronze"],
            driver.Window(None, None, "full"),
            False,
        )

        summary2 = json.loads((tmp_path / "summary2.json").read_text(encoding="utf-8"))
        # The contract was already completed, so successful should include it (skip path increments successful)
        assert summary2["metrics"]["successful"] == 1


# ── Standalone Observability Functions ───────────────────────────────────────

class TestObservabilityFunctions:
    """Unit tests for the extracted observability functions."""

    def test_flatten_summary_keys(self) -> None:
        """flatten_summary should produce all expected keys."""
        summary = {
            "run_id": "abc123",
            "started_at": "2026-01-01T00:00:00+00:00",
            "finished_at": "2026-01-01T01:00:00+00:00",
            "duration_seconds": 3600.0,
            "engine": "duckdb",
            "metrics": {
                "total_contracts": 5,
                "successful": 4,
                "failed": 1,
                "skipped_missing_upstream": 0,
                "skipped_no_sources": 0,
                "full_loads": 3,
                "full_loads_due_to_missing_logs": 1,
                "missing_upstreams": 0,
            },
        }
        record = flatten_summary(summary)
        assert record["run_id"] == "abc123"
        assert record["engine"] == "duckdb"
        assert record["successful"] == 4
        assert record["failed"] == 1
        assert "summary_json" in record

    def test_finalize_summary_writes_json(self, tmp_path: Path) -> None:
        """finalize_summary should write JSON to disk."""
        summary = {
            "run_id": "test-run",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "metrics": {"total_contracts": 1, "successful": 1, "failed": 0},
        }
        out = tmp_path / "summary.json"
        finalize_summary(summary, out)

        assert out.exists()
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["run_id"] == "test-run"
        assert "finished_at" in data
        assert "duration_seconds" in data

    def test_emit_metrics_creates_snapshot(self, tmp_path: Path) -> None:
        """emit_metrics should return a valid snapshot dict and write JSON."""
        summary = {
            "run_id": "m-run",
            "started_at": "2026-01-01T00:00:00+00:00",
            "engine": "duckdb",
            "metrics": {"total_contracts": 2, "successful": 2, "failed": 0},
        }
        path = tmp_path / "metrics.json"
        snapshot = emit_metrics(summary, path, None, None, None, "lakelogic", {"env": "test"})

        assert snapshot["tags"]["env"] == "test"
        assert snapshot["metrics"]["successful"] == 2
        assert path.exists()

    def test_format_prometheus_output(self) -> None:
        """format_prometheus should produce valid exposition format."""
        snapshot = {
            "tags": {"env": "prod"},
            "metrics": {"successful": 10, "failed": 2, "skipped_no_sources": None},
        }
        text = format_prometheus(snapshot, "ll")
        assert 'll_successful{env="prod"} 10' in text
        assert 'll_failed{env="prod"} 2' in text
        assert "skipped_no_sources" not in text  # None values are excluded


# ── RunLogReader Integration ─────────────────────────────────────────────────

class TestRunLogReaderIntegration:
    """Tests for RunLogReader with DuckDB and SQLite backends."""

    def test_duckdb_run_log_reader(self, tmp_path: Path) -> None:
        """RunLogReader should read last success from a DuckDB table."""
        import duckdb

        db_path = tmp_path / "logs.duckdb"
        ts = datetime.now(timezone.utc).isoformat()
        con = duckdb.connect(str(db_path))
        con.execute("CREATE TABLE run_logs (contract VARCHAR, timestamp VARCHAR)")
        con.execute("INSERT INTO run_logs VALUES (?, ?)", ["duckdb_test_dataset", ts])
        con.close()

        contract = driver.DataContract(
            version="1.0.0",
            dataset="duckdb_test_dataset",
            metadata={
                "run_log_table": "run_logs",
                "run_log_backend": "duckdb",
                "run_log_database": str(db_path),
            },
        )

        reader = RunLogReader("duckdb")
        result_ts, reason = reader.last_success_info(contract)
        assert result_ts is not None
        assert reason is None

    def test_sqlite_run_log_reader(self, tmp_path: Path) -> None:
        """RunLogReader should read last success from a SQLite table."""
        db_path = tmp_path / "logs.sqlite"
        ts = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE run_logs (contract TEXT, timestamp TEXT)")
        conn.execute("INSERT INTO run_logs VALUES (?, ?)", ("sqlite_test_ds", ts))
        conn.commit()
        conn.close()

        contract = driver.DataContract(
            version="1.0.0",
            dataset="sqlite_test_ds",
            metadata={
                "run_log_table": "run_logs",
                "run_log_backend": "sqlite",
                "run_log_database": str(db_path),
            },
        )

        reader = RunLogReader("polars")
        result_ts, reason = reader.last_success_info(contract)
        assert result_ts is not None
        assert reason is None

    def test_missing_run_log_table(self) -> None:
        """When no run_log_table is configured, reason should explain why."""
        contract = driver.DataContract(version="1.0.0", dataset="no_log")
        reader = RunLogReader("polars")
        ts, reason = reader.last_success_info(contract)
        assert ts is None
        assert reason is not None

    def test_missing_run_log_entry(self, tmp_path: Path) -> None:
        """When the table exists but has no matching entry, reason should be returned."""
        db_path = tmp_path / "empty.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE run_logs (contract TEXT, timestamp TEXT)")
        conn.commit()
        conn.close()

        contract = driver.DataContract(
            version="1.0.0",
            dataset="nonexistent_ds",
            metadata={
                "run_log_table": "run_logs",
                "run_log_backend": "sqlite",
                "run_log_database": str(db_path),
            },
        )

        reader = RunLogReader("polars")
        ts, reason = reader.last_success_info(contract)
        assert ts is None
        assert reason is not None


# ── Backfill Window Generation ───────────────────────────────────────────────

class TestBackfillWindows:
    """Tests for backfill window generation."""

    def test_daily_backfill(self) -> None:
        """Day-level backfill should produce one window per day."""
        windows = build_backfill_windows("2026-02-01", "2026-02-03", "day")
        assert len(windows) == 3
        assert windows[0].label == "backfill_20260201"
        assert windows[2].label == "backfill_20260203"

    def test_weekly_backfill(self) -> None:
        """Week-level backfill should produce windows spanning 7 days."""
        windows = build_backfill_windows("2026-01-01", "2026-01-21", "week")
        assert len(windows) == 3
        assert all(w.label.startswith("backfill_") for w in windows)

    def test_invalid_range_raises(self) -> None:
        """End before start should raise ValueError."""
        with pytest.raises(ValueError):
            build_backfill_windows("2026-02-05", "2026-02-01", "day")


# ── DuckDB Derive & Filter Transforms ───────────────────────────────────────

@pytest.mark.skip(reason="DuckDB execution engine is deprecated")
class TestDuckDBTransforms:
    """Tests for DuckDB derive and filter post-transformations (bug-fix coverage).

    These tests use DataProcessor directly (rather than PipelineDriver) because
    the driver's ``_prepare_contract_for_stage`` intentionally strips
    transformations from bronze contracts.  Using the processor lets us exercise
    the exact engine code path that was fixed.
    """

    def test_derive_transformation(self, tmp_path: Path) -> None:
        """A derive transformation should add a computed column."""
        from lakelogic import DataProcessor

        landing = tmp_path / "landing"
        landing.mkdir()
        _write_csv(
            landing / "products.csv",
            [
                {"product_id": "P-1", "price": 100.0, "tax_rate": 0.1},
                {"product_id": "P-2", "price": 50.0, "tax_rate": 0.2},
            ],
        )

        contract = {
            "version": "1.0.0",
            "dataset": "products",
            "source": {"type": "landing", "path": str(landing), "pattern": "products.csv"},
            "model": {
                "fields": [
                    {"name": "product_id", "type": "string"},
                    {"name": "price", "type": "double"},
                    {"name": "tax_rate", "type": "double"},
                ],
            },
            "transformations": [
                {"derive": {"field": "total_price", "sql": "price * (1 + tax_rate)"}},
            ],
        }

        processor = DataProcessor(engine="duckdb", contract=contract)
        result = processor.run_source(str(landing / "products.csv"))
        good_df = result.good
        if hasattr(good_df, "df"):
            good_df = good_df.df()
        elif hasattr(good_df, "to_pandas"):
            good_df = good_df.to_pandas()

        assert "total_price" in good_df.columns
        total_prices = sorted(good_df["total_price"].tolist())
        assert abs(total_prices[0] - 60.0) < 0.01   # P-2: 50 * 1.2
        assert abs(total_prices[1] - 110.0) < 0.01  # P-1: 100 * 1.1

    def test_filter_transformation(self, tmp_path: Path) -> None:
        """A post-filter transformation should remove non-matching rows."""
        from lakelogic import DataProcessor

        landing = tmp_path / "landing"
        landing.mkdir()
        _write_csv(
            landing / "events.csv",
            [
                {"event_id": "E-1", "event_type": "click", "value": 10},
                {"event_id": "E-2", "event_type": "purchase", "value": 200},
                {"event_id": "E-3", "event_type": "click", "value": 5},
                {"event_id": "E-4", "event_type": "purchase", "value": 150},
            ],
        )

        contract = {
            "version": "1.0.0",
            "dataset": "events",
            "source": {"type": "landing", "path": str(landing), "pattern": "events.csv"},
            "model": {
                "fields": [
                    {"name": "event_id", "type": "string"},
                    {"name": "event_type", "type": "string"},
                    {"name": "value", "type": "integer"},
                ],
            },
            "transformations": [
                {"filter": {"sql": "event_type = 'purchase'"}},
            ],
        }

        processor = DataProcessor(engine="duckdb", contract=contract)
        result = processor.run_source(str(landing / "events.csv"))
        good_df = result.good
        if hasattr(good_df, "df"):
            good_df = good_df.df()
        elif hasattr(good_df, "to_pandas"):
            good_df = good_df.to_pandas()

        assert len(good_df) == 2
        assert set(good_df["event_type"]) == {"purchase"}

    def test_derive_then_filter_chained(self, tmp_path: Path) -> None:
        """Chained derive + filter should compute column then filter on it."""
        from lakelogic import DataProcessor

        landing = tmp_path / "landing"
        landing.mkdir()
        _write_csv(
            landing / "data.csv",
            [
                {"id": 1, "qty": 10, "unit_price": 5.0},
                {"id": 2, "qty": 2, "unit_price": 100.0},
                {"id": 3, "qty": 1, "unit_price": 3.0},
            ],
        )

        contract = {
            "version": "1.0.0",
            "dataset": "items",
            "source": {"type": "landing", "path": str(landing), "pattern": "data.csv"},
            "model": {
                "fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "qty", "type": "integer"},
                    {"name": "unit_price", "type": "double"},
                ],
            },
            "transformations": [
                {"derive": {"field": "total", "sql": "qty * unit_price"}},
                {"filter": {"sql": "total > 10"}},
            ],
        }

        processor = DataProcessor(engine="duckdb", contract=contract)
        result = processor.run_source(str(landing / "data.csv"))
        good_df = result.good
        if hasattr(good_df, "df"):
            good_df = good_df.df()
        elif hasattr(good_df, "to_pandas"):
            good_df = good_df.to_pandas()

        assert "total" in good_df.columns
        # id=1: total=50, id=2: total=200, id=3: total=3 → only 2 pass the filter
        assert len(good_df) == 2
        assert all(good_df["total"] > 10)


# ── RunLogReader Unit Tests ──────────────────────────────────────────────────

class TestRunLogReaderUnit:
    """Comprehensive unit tests for the RunLogReader module."""

    def test_contract_key_from_dataset(self) -> None:
        """contract_key should fall back to dataset when info.title is missing."""
        contract = driver.DataContract(version="1.0.0", dataset="my_dataset")
        reader = RunLogReader("polars")
        assert reader._contract_key(contract) == "my_dataset"

    def test_contract_key_unknown_fallback(self) -> None:
        """contract_key should return 'unknown' when both title and dataset are missing."""
        contract = driver.DataContract(version="1.0.0")
        reader = RunLogReader("polars")
        assert reader._contract_key(contract) == "unknown"

    def test_unsupported_backend(self) -> None:
        """Unsupported run_log_backend should return reason='unsupported_backend'."""
        contract = driver.DataContract(
            version="1.0.0",
            dataset="test",
            metadata={"run_log_table": "logs", "run_log_backend": "cassandra"},
        )
        reader = RunLogReader("polars")
        ts, reason = reader.last_success_info(contract)
        assert ts is None
        assert reason == "unsupported_backend"

    def test_missing_database_file(self, tmp_path: Path) -> None:
        """When the database file doesn't exist, reason should be 'run_log_db_missing'."""
        contract = driver.DataContract(
            version="1.0.0",
            dataset="test",
            metadata={
                "run_log_table": "logs",
                "run_log_backend": "sqlite",
                "run_log_database": str(tmp_path / "nonexistent.sqlite"),
            },
        )
        reader = RunLogReader("polars")
        ts, reason = reader.last_success_info(contract)
        assert ts is None
        assert reason == "run_log_db_missing"

    def test_duckdb_missing_database_file(self, tmp_path: Path) -> None:
        """DuckDB backend with missing db file should return 'run_log_db_missing'."""
        contract = driver.DataContract(
            version="1.0.0",
            dataset="test",
            metadata={
                "run_log_table": "logs",
                "run_log_backend": "duckdb",
                "run_log_database": str(tmp_path / "nonexistent.duckdb"),
            },
        )
        reader = RunLogReader("duckdb")
        ts, reason = reader.last_success_info(contract)
        assert ts is None
        assert reason == "run_log_db_missing"

    def test_sqlite_multiple_entries_returns_max(self, tmp_path: Path) -> None:
        """When multiple entries exist, return the max timestamp."""
        db_path = tmp_path / "logs.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE run_logs (contract TEXT, timestamp TEXT)")
        conn.execute("INSERT INTO run_logs VALUES (?, ?)", ("ds", "2026-01-01T00:00:00+00:00"))
        conn.execute("INSERT INTO run_logs VALUES (?, ?)", ("ds", "2026-01-05T12:00:00+00:00"))
        conn.execute("INSERT INTO run_logs VALUES (?, ?)", ("ds", "2026-01-03T00:00:00+00:00"))
        conn.commit()
        conn.close()

        contract = driver.DataContract(
            version="1.0.0",
            dataset="ds",
            metadata={
                "run_log_table": "run_logs",
                "run_log_backend": "sqlite",
                "run_log_database": str(db_path),
            },
        )
        reader = RunLogReader("polars")
        ts, reason = reader.last_success_info(contract)
        assert ts is not None
        assert reason is None
        assert ts.day == 5  # max is Jan 5

    def test_duckdb_multiple_entries_returns_max(self, tmp_path: Path) -> None:
        """DuckDB last_success should return the most recent timestamp."""
        import duckdb

        db_path = tmp_path / "logs.duckdb"
        con = duckdb.connect(str(db_path))
        con.execute("CREATE TABLE run_logs (contract VARCHAR, timestamp VARCHAR)")
        con.execute("INSERT INTO run_logs VALUES (?, ?)", ["ds", "2026-02-01T00:00:00+00:00"])
        con.execute("INSERT INTO run_logs VALUES (?, ?)", ["ds", "2026-02-10T00:00:00+00:00"])
        con.execute("INSERT INTO run_logs VALUES (?, ?)", ["other", "2026-02-20T00:00:00+00:00"])
        con.close()

        contract = driver.DataContract(
            version="1.0.0",
            dataset="ds",
            metadata={
                "run_log_table": "run_logs",
                "run_log_backend": "duckdb",
                "run_log_database": str(db_path),
            },
        )
        reader = RunLogReader("duckdb")
        ts, reason = reader.last_success_info(contract)
        assert ts is not None
        assert reason is None
        assert ts.day == 10  # max for 'ds' is Feb 10, not Feb 20 ('other')

    def test_last_success_convenience(self, tmp_path: Path) -> None:
        """last_success (without _info) should return just the timestamp."""
        db_path = tmp_path / "logs.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE run_logs (contract TEXT, timestamp TEXT)")
        conn.execute("INSERT INTO run_logs VALUES (?, ?)", ("ds", "2026-01-01T00:00:00+00:00"))
        conn.commit()
        conn.close()

        contract = driver.DataContract(
            version="1.0.0",
            dataset="ds",
            metadata={
                "run_log_table": "run_logs",
                "run_log_backend": "sqlite",
                "run_log_database": str(db_path),
            },
        )
        reader = RunLogReader("polars")
        ts = reader.last_success(contract)
        assert ts is not None
        assert isinstance(ts, datetime)

    def test_parse_timestamp_string(self) -> None:
        """_parse_timestamp should parse ISO format strings."""
        result = RunLogReader._parse_timestamp("2026-01-15T10:30:00+00:00")
        assert result is not None
        assert result.day == 15
        assert result.hour == 10

    def test_parse_timestamp_invalid(self) -> None:
        """_parse_timestamp should return None for invalid values."""
        assert RunLogReader._parse_timestamp("not-a-date") is None

    def test_default_backend_duckdb(self) -> None:
        """When no run_log_backend is set, non-spark engines default to duckdb."""
        contract = driver.DataContract(
            version="1.0.0",
            dataset="test",
            metadata={"run_log_table": "logs"},
        )
        reader = RunLogReader("polars")
        # This will try duckdb backend and fail (no db file), but with duckdb-specific reason
        ts, reason = reader.last_success_info(contract)
        assert ts is None
        assert reason == "run_log_db_missing"


# ── Prometheus Server Lifecycle ──────────────────────────────────────────────

class TestPrometheusServer:
    """Tests for the Prometheus HTTP metrics server."""

    def test_start_and_stop_server(self) -> None:
        """Should start a Prometheus server and stop it cleanly."""
        from lakelogic.cli.observability import start_prometheus_server, stop_prometheus_server

        snapshot = {
            "tags": {"env": "test"},
            "metrics": {"successful": 42, "failed": 0},
        }
        server, thread = start_prometheus_server(
            "127.0.0.1", 0,  # port 0 = OS picks a free port
            lambda: snapshot,
            "test_prefix",
        )

        try:
            assert server is not None
            assert thread is not None
            assert thread.is_alive()

            # Query the /metrics endpoint
            import urllib.request
            port = server.server_address[1]
            url = f"http://127.0.0.1:{port}/metrics"
            response = urllib.request.urlopen(url, timeout=5)
            body = response.read().decode("utf-8")
            assert "test_prefix_successful" in body
            assert "42" in body
        finally:
            stop_prometheus_server(server, thread)

    def test_non_metrics_endpoint_returns_404(self) -> None:
        """Requests to paths other than /metrics should return 404."""
        import urllib.error
        import urllib.request

        from lakelogic.cli.observability import start_prometheus_server, stop_prometheus_server

        server, thread = start_prometheus_server(
            "127.0.0.1", 0,
            lambda: {"tags": {}, "metrics": {}},
            "test",
        )

        try:
            port = server.server_address[1]
            url = f"http://127.0.0.1:{port}/health"
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url, timeout=5)
            assert exc_info.value.code == 404
        finally:
            stop_prometheus_server(server, thread)

    def test_stop_none_server_is_noop(self) -> None:
        """stop_prometheus_server should handle None gracefully."""
        from lakelogic.cli.observability import stop_prometheus_server
        stop_prometheus_server(None, None)  # should not raise


# ── CLI main() Entrypoint Tests ──────────────────────────────────────────────

class TestCLIMain:
    """Tests for the CLI main() entrypoint."""

    def test_main_minimal_run(self, tmp_path: Path, monkeypatch) -> None:
        """main() should complete a basic pipeline run via CLI args."""
        landing = tmp_path / "landing"
        out = tmp_path / "out"
        landing.mkdir()
        out.mkdir()

        _write_csv(landing / "data.csv", [{"id": 1, "val": 10}])
        contract = _make_contract(
            "bronze_data", landing, out, pattern="data.csv",
            fields=[{"name": "id", "type": "integer"}, {"name": "val", "type": "integer"}],
        )
        _write_contract(tmp_path / "contract.yaml", contract)
        _write_registry(
            tmp_path / "_registry.yaml",
            [{"entity": "data", "enabled": True, "contracts": {"bronze": "contract.yaml"}}],
        )

        summary_path = tmp_path / "summary.json"
        import sys
        monkeypatch.setattr(
            sys, "argv",
            [
                "lakelogic-driver",
                "--registry", str(tmp_path / "_registry.yaml"),
                "--layers", "bronze",
                "--engine", "polars",
                "--max-workers", "1",
                "--summary-path", str(summary_path),
                "--window", "none",
            ],
        )

        from lakelogic.cli.driver import main
        main()

        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["metrics"]["successful"] == 1
        assert (out / "bronze_data" / "data.csv").exists()

    def test_main_with_entity_filter(self, tmp_path: Path, monkeypatch) -> None:
        """main() should respect --entities filter."""
        landing = tmp_path / "landing"
        out = tmp_path / "out"
        landing.mkdir()
        out.mkdir()

        _write_csv(landing / "a.csv", [{"id": 1}])
        _write_csv(landing / "b.csv", [{"id": 2}])

        contract_a = _make_contract(
            "bronze_a", landing, out, pattern="a.csv",
            fields=[{"name": "id", "type": "integer"}],
        )
        contract_b = _make_contract(
            "bronze_b", landing, out, pattern="b.csv",
            fields=[{"name": "id", "type": "integer"}],
        )
        _write_contract(tmp_path / "a.yaml", contract_a)
        _write_contract(tmp_path / "b.yaml", contract_b)
        _write_registry(
            tmp_path / "_registry.yaml",
            [
                {"entity": "alpha", "enabled": True, "contracts": {"bronze": "a.yaml"}},
                {"entity": "beta", "enabled": True, "contracts": {"bronze": "b.yaml"}},
            ],
        )

        summary_path = tmp_path / "summary.json"
        import sys
        monkeypatch.setattr(
            sys, "argv",
            [
                "lakelogic-driver",
                "--registry", str(tmp_path / "_registry.yaml"),
                "--layers", "bronze",
                "--engine", "polars",
                "--max-workers", "1",
                "--summary-path", str(summary_path),
                "--window", "none",
                "--entities", "alpha",
            ],
        )

        from lakelogic.cli.driver import main
        main()

        assert (out / "bronze_a" / "data.csv").exists()
        assert not (out / "bronze_b" / "data.csv").exists()

    def test_main_with_overrides(self, tmp_path: Path, monkeypatch) -> None:
        """main() should pass --set overrides to the driver."""
        landing = tmp_path / "landing"
        out = tmp_path / "out"
        landing.mkdir()
        out.mkdir()

        _write_csv(landing / "data.csv", [{"id": 1, "val": 10}])
        contract = _make_contract(
            "bronze_data", landing, out, pattern="data.csv",
            fields=[{"name": "id", "type": "integer"}, {"name": "val", "type": "integer"}],
        )
        _write_contract(tmp_path / "contract.yaml", contract)
        _write_registry(
            tmp_path / "_registry.yaml",
            [{"entity": "data", "enabled": True, "contracts": {"bronze": "contract.yaml"}}],
        )

        summary_path = tmp_path / "summary.json"
        import sys
        monkeypatch.setattr(
            sys, "argv",
            [
                "lakelogic-driver",
                "--registry", str(tmp_path / "_registry.yaml"),
                "--layers", "bronze",
                "--engine", "polars",
                "--max-workers", "1",
                "--summary-path", str(summary_path),
                "--window", "none",
                "--set", "materialization.format=csv",
            ],
        )

        from lakelogic.cli.driver import main
        main()

        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["metrics"]["successful"] == 1

    def test_main_backfill_mode(self, tmp_path: Path, monkeypatch) -> None:
        """main() with backfill dates should run multiple windows."""
        landing = tmp_path / "landing"
        out = tmp_path / "out"
        landing.mkdir()
        out.mkdir()

        _write_csv(landing / "data.csv", [{"id": 1, "val": 10}])
        contract = _make_contract(
            "bronze_data", landing, out, pattern="data.csv",
            fields=[{"name": "id", "type": "integer"}, {"name": "val", "type": "integer"}],
        )
        _write_contract(tmp_path / "contract.yaml", contract)
        _write_registry(
            tmp_path / "_registry.yaml",
            [{"entity": "data", "enabled": True, "contracts": {"bronze": "contract.yaml"}}],
        )

        summary_path = tmp_path / "summary.json"
        import sys
        monkeypatch.setattr(
            sys, "argv",
            [
                "lakelogic-driver",
                "--registry", str(tmp_path / "_registry.yaml"),
                "--layers", "bronze",
                "--engine", "polars",
                "--max-workers", "1",
                "--summary-path", str(summary_path),
                "--window", "none",
                "--backfill-start-date", "2026-02-01",
                "--backfill-end-date", "2026-02-03",
                "--backfill-granularity", "day",
            ],
        )

        from lakelogic.cli.driver import main
        main()

        # Backfill runs overwrite the summary for each window, so last window's summary should exist
        assert summary_path.exists()


# ── Observability Backend Coverage ───────────────────────────────────────────

class TestObservabilityBackends:
    """Tests for summary table write backends and metrics emission."""

    def test_sqlite_summary_table_write(self, tmp_path: Path) -> None:
        """write_summary_table should write a row to SQLite."""
        from lakelogic.cli.observability import write_summary_table

        db_path = tmp_path / "pipeline_runs.sqlite"
        summary = {
            "run_id": "sqlite-test",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": 10.5,
            "engine": "polars",
            "metrics": {
                "total_contracts": 3, "successful": 2, "failed": 1,
                "skipped_missing_upstream": 0, "skipped_no_sources": 0,
                "full_loads": 1, "full_loads_due_to_missing_logs": 0,
                "missing_upstreams": 0,
            },
        }
        write_summary_table(summary, "runs", "sqlite", str(db_path), None, False, "polars")

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT run_id, successful, failed FROM runs").fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0][0] == "sqlite-test"
        assert rows[0][1] == 2
        assert rows[0][2] == 1

    def test_duckdb_summary_table_write_standalone(self, tmp_path: Path) -> None:
        """write_summary_table should write a row to DuckDB."""
        import duckdb

        from lakelogic.cli.observability import write_summary_table

        db_path = tmp_path / "pipeline_runs.duckdb"
        summary = {
            "run_id": "duckdb-test",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": 5.0,
            "engine": "duckdb",
            "metrics": {
                "total_contracts": 1, "successful": 1, "failed": 0,
                "skipped_missing_upstream": 0, "skipped_no_sources": 0,
                "full_loads": 0, "full_loads_due_to_missing_logs": 0,
                "missing_upstreams": 0,
            },
        }
        write_summary_table(summary, "runs", "duckdb", str(db_path), None, False, "duckdb")

        con = duckdb.connect(str(db_path))
        rows = con.execute("SELECT run_id, successful FROM runs").fetchall()
        con.close()
        assert len(rows) == 1
        assert rows[0][0] == "duckdb-test"

    def test_unsupported_backend_warns(self, tmp_path: Path) -> None:
        """Unsupported summary backend should log a warning and not crash."""
        from lakelogic.cli.observability import write_summary_table

        summary = {"run_id": "x", "metrics": {}}
        # Should not raise
        write_summary_table(summary, "runs", "cassandra", None, None, False, "polars")

    def test_no_summary_table_is_noop(self) -> None:
        """When summary_table is None, write_summary_table should be a no-op."""
        from lakelogic.cli.observability import write_summary_table
        write_summary_table({"run_id": "x"}, None, None, None, None, False, "polars")

    def test_emit_metrics_statsd_no_crash(self) -> None:
        """emit_metrics with statsd backend should not crash (even without a statsd server)."""
        summary = {
            "run_id": "statsd-test",
            "engine": "polars",
            "metrics": {"total_contracts": 1, "successful": 1, "failed": 0},
        }
        snapshot = emit_metrics(summary, None, "statsd", "127.0.0.1", 18125, "test", {})
        assert snapshot["metrics"]["successful"] == 1

    def test_sqlite_dot_in_table_name(self, tmp_path: Path) -> None:
        """SQLite writer should sanitize dots in table names."""
        from lakelogic.cli.observability import write_summary_table

        db_path = tmp_path / "runs.sqlite"
        summary = {
            "run_id": "dot-test",
            "started_at": None, "finished_at": None, "duration_seconds": None,
            "engine": "polars",
            "metrics": {
                "total_contracts": 0, "successful": 0, "failed": 0,
                "skipped_missing_upstream": 0, "skipped_no_sources": 0,
                "full_loads": 0, "full_loads_due_to_missing_logs": 0,
                "missing_upstreams": 0,
            },
        }
        write_summary_table(summary, "schema.runs", "sqlite", str(db_path), None, False, "polars")

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT run_id FROM schema_runs").fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0][0] == "dot-test"
