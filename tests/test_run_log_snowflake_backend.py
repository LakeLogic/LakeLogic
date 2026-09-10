"""A declared `snowflake` log backend must actually write.

WHAT THIS CAUGHT. The Snowflake mesh's `_system.yaml` has declared
`run_log_backend: "snowflake"` and `slo_checks_backend: "snowflake"` since it was built.
`run_log.py` had no snowflake branch, so both fell through to
`logger.warning("Unsupported ...")` and returned None — a configured evidence table that
had never once been written, announced only in a log line nobody reads.

That is the exact shape the project keeps banning elsewhere: a declaration that reads as a
capability while the surface behind it does nothing. A warning is not a failure, so
nothing ever went red.
"""

from __future__ import annotations

import pytest

from lakelogic.core import run_log


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append(("execute", sql, params))

    def executemany(self, sql, rows):
        self.log.append(("executemany", sql, list(rows)))

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return FakeCursor(self.log)


@pytest.fixture()
def conn(monkeypatch):
    fake = FakeConn()
    monkeypatch.setattr(run_log, "_snowflake_log_connection", lambda metadata: fake)
    return fake


class Registry:
    """The two places the writers look for configuration, and nothing else."""

    def __init__(self, **metadata):
        self.metadata = metadata
        self.storage = None


def _statements(conn):
    return [sql for kind, sql, _ in conn.log if kind == "execute"]


def _inserted(conn):
    return [(sql, rows) for kind, sql, rows in conn.log if kind == "executemany"]


# ── SLO checks ──────────────────────────────────────────────────────────────


def test_slo_checks_snowflake_backend_writes_rows(conn):
    registry = Registry(
        slo_checks_table="RIDEFLOW.MARKETPLACE._SLO_CHECKS",
        slo_checks_backend="snowflake",
    )
    records = [
        {"check_run_id": "run-1", "entity": "trips", "check_type": "freshness", "passed": True},
        {"check_run_id": "run-1", "entity": "riders", "check_type": "freshness", "passed": False},
    ]

    result = run_log._write_slo_checks_table(registry, records)

    assert result == "RIDEFLOW.MARKETPLACE._SLO_CHECKS"
    inserts = _inserted(conn)
    assert len(inserts) == 1, "one batch, not one round trip per objective"
    sql, rows = inserts[0]
    assert sql.startswith("INSERT INTO RIDEFLOW.MARKETPLACE._SLO_CHECKS (")
    assert len(rows) == 2


def test_slo_checks_row_order_matches_the_column_list(conn):
    """The values are positional, so a column/value mismatch writes the WRONG data.

    This is the failure that does not raise: `passed` landing in `severity` produces a
    table full of plausible-looking rows and a dashboard that is quietly lying.
    """
    registry = Registry(slo_checks_table="T", slo_checks_backend="snowflake")
    record = {c: f"v_{c}" for c in run_log._SLO_CHECKS_COLUMNS}

    run_log._write_slo_checks_table(registry, [record])

    sql, rows = _inserted(conn)[0]
    columns = sql.split("(", 1)[1].split(")", 1)[0].split(", ")
    assert columns == run_log._SLO_CHECKS_COLUMNS
    assert list(rows[0]) == [f"v_{c}" for c in columns]


def test_slo_checks_creates_and_widens_the_table(conn):
    registry = Registry(slo_checks_table="T", slo_checks_backend="snowflake")

    run_log._write_slo_checks_table(registry, [{"check_run_id": "r"}])

    statements = _statements(conn)
    assert any(s.startswith("CREATE TABLE IF NOT EXISTS T (") for s in statements)
    # An existing narrower table must gain the newer columns rather than fail the insert.
    assert any("ADD COLUMN IF NOT EXISTS duration_seconds" in s for s in statements)


def test_no_connection_writes_nothing(monkeypatch):
    """Better to return None than to raise: a log write must not fail the pipeline."""
    monkeypatch.setattr(run_log, "_snowflake_log_connection", lambda metadata: None)
    registry = Registry(slo_checks_table="T", slo_checks_backend="snowflake")

    assert run_log._write_slo_checks_table(registry, [{"check_run_id": "r"}]) is None


# ── Run log ─────────────────────────────────────────────────────────────────


def test_run_log_snowflake_backend_writes_one_row(conn, monkeypatch):
    registry = Registry(
        run_log_table="RIDEFLOW.MARKETPLACE._PIPELINE_RUN_LOG",
        run_log_backend="snowflake",
    )
    monkeypatch.setattr(run_log, "_flatten_report", lambda report: {"run_id": "abc", "status": "success"})

    result = run_log._write_run_log_table({"run_id": "abc"}, _contract_for(registry))

    assert result == "RIDEFLOW.MARKETPLACE._PIPELINE_RUN_LOG"
    inserts = _inserted(conn)
    assert len(inserts) == 1
    sql, rows = inserts[0]
    assert "slo_json" in sql, "the SLO payload column must be written, not dropped"
    assert len(rows) == 1


def _contract_for(registry):
    """`_write_run_log_table` reads its config off a contract's registry."""

    class Contract:
        def __init__(self):
            self.registry = registry
            self.metadata = registry.metadata
            self.storage = None

    return Contract()
