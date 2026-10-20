"""A declared `bigquery` log backend must actually write, and read the watermark back.

The BigQuery delivery target declares `run_log_backend: bigquery` and
`slo_checks_backend: bigquery`. Before this backend existed both fell through to
`logger.warning("Unsupported ...")`: a configured evidence table that was never written, and
incremental loads that reloaded everything because no watermark could be read.

Rows are appended with a LOAD job, never streaming inserts: streamed rows cannot be UPDATEd or
DELETEd for about 30 minutes, and erasure must be able to change any row.
"""

from __future__ import annotations

import pytest

pytest.importorskip("google.cloud.bigquery")

from lakelogic.core import bigquery_tables, run_log  # noqa: E402


class FakeJob:
    def __init__(self, rows=None):
        self._rows = rows or []

    def result(self):
        return self._rows


class FakeRow(dict):
    def values(self):  # BigQuery's Row.values() returns the cells in order
        return list(super().values())


class NotFound(Exception):
    """Stands in for google.api_core.exceptions.NotFound (matched by name)."""


class FakeClient:
    def __init__(self, query_rows=None, tables=None):
        self.queries = []
        self.loads = []
        self.query_rows = query_rows or []
        #: table -> its existing column names; absent = the table does not exist yet.
        self.tables = tables or {}

    def get_table(self, table):
        if table not in self.tables:
            raise NotFound(table)
        return type("T", (), {"schema": [type("F", (), {"name": c}) for c in self.tables[table]]})

    def query(self, sql, job_config=None):
        self.queries.append((sql, job_config))
        return FakeJob(self.query_rows if sql.startswith("SELECT") else [])

    def load_table_from_json(self, rows, table, job_config=None):
        self.loads.append((table, rows, job_config))
        return FakeJob()


@pytest.fixture()
def bq(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(bigquery_tables, "client", lambda metadata=None: fake)
    bigquery_tables._ENSURED.clear()
    return fake


class Registry:
    def __init__(self, **metadata):
        self.metadata = metadata
        self.storage = None


def _contract(**metadata):
    class Contract:
        def __init__(self):
            self.metadata = metadata
            self.registry = Registry(**metadata)
            self.storage = None

    return Contract()


def test_run_log_is_written_with_a_load_job_not_streaming(bq, monkeypatch):
    monkeypatch.setattr(run_log, "_flatten_report", lambda report: {"run_id": "abc", "status": "success",
                                                                    "counts_good": "7", "slo_json": {"a": 1}})
    out = run_log._write_run_log_table(
        {}, _contract(run_log_table="my-proj.marketplace._lakelogic_run_log", run_log_backend="bigquery")
    )
    assert out == "my-proj.marketplace._lakelogic_run_log"
    (table, rows, config), = bq.loads
    assert table == "my-proj.marketplace._lakelogic_run_log"
    assert config.write_disposition == "WRITE_APPEND"
    assert rows[0]["counts_good"] == 7, "typed as INT64, not left a string"
    assert rows[0]["slo_json"] == '{"a": 1}', "a nested payload is stored as JSON text"
    assert set(rows[0]) == {name for name, _ in run_log._RUN_LOG_TABLE_COLUMNS}


def test_the_table_is_created_and_widened_with_a_quoted_name(bq, monkeypatch):
    """Project ids contain '-', which BigQuery accepts only inside backticks."""
    monkeypatch.setattr(run_log, "_flatten_report", lambda report: {"run_id": "abc"})
    run_log._write_run_log_table({}, _contract(run_log_table="my-proj.d._lakelogic_run_log", run_log_backend="bigquery"))
    sqls = [q for q, _ in bq.queries]
    assert sqls[0].startswith("CREATE TABLE IF NOT EXISTS `my-proj.d._lakelogic_run_log` (")
    assert "`report_json` STRING" in sqls[0]
    assert not any(q.startswith("ALTER") for q in sqls), "a new table needs no ALTER"


def test_an_older_table_is_widened_in_one_statement(bq, monkeypatch):
    """One ALTER adding only what is missing. A CREATE plus an ALTER per column (37 queries)
    took about a minute and a half before every run-log row on a live GCP run."""
    monkeypatch.setattr(run_log, "_flatten_report", lambda report: {"run_id": "abc"})
    known = [n for n, _ in run_log._RUN_LOG_TABLE_COLUMNS if n != "report_json"]
    bq.tables["my-proj.d._lakelogic_run_log"] = known
    contract = _contract(run_log_table="my-proj.d._lakelogic_run_log", run_log_backend="bigquery")
    run_log._write_run_log_table({}, contract)
    run_log._write_run_log_table({}, contract)  # the second write checks nothing again
    ddl = [q for q, _ in bq.queries if not q.startswith("SELECT")]
    assert ddl == ["ALTER TABLE `my-proj.d._lakelogic_run_log` ADD COLUMN IF NOT EXISTS `report_json` STRING"]
    assert len(bq.loads) == 2


def test_an_up_to_date_table_gets_no_ddl(bq, monkeypatch):
    monkeypatch.setattr(run_log, "_flatten_report", lambda report: {"run_id": "abc"})
    bq.tables["p.d._lakelogic_run_log"] = [n for n, _ in run_log._RUN_LOG_TABLE_COLUMNS]
    run_log._write_run_log_table({}, _contract(run_log_table="p.d._lakelogic_run_log", run_log_backend="bigquery"))
    assert [q for q, _ in bq.queries if not q.startswith("SELECT")] == []


def test_slo_checks_are_written_in_one_batch_in_column_order(bq):
    registry = Registry(slo_checks_table="p.marketplace._lakelogic_slo_checks", slo_checks_backend="bigquery")
    records = [{"check_run_id": "r", "entity": "trips", "passed": True, "row_count": "12"},
               {"check_run_id": "r", "entity": "riders", "passed": False}]
    assert run_log._write_slo_checks_table(registry, records) == "p.marketplace._lakelogic_slo_checks"
    (_table, rows, config), = bq.loads
    assert len(rows) == 2, "one load job for the batch"
    assert [f.name for f in config.schema] == run_log._SLO_CHECKS_COLUMNS
    assert rows[0]["row_count"] == 12 and rows[1]["passed"] is False


def test_no_client_writes_nothing_and_does_not_raise(monkeypatch):
    monkeypatch.setattr(bigquery_tables, "client", lambda metadata=None: None)
    registry = Registry(slo_checks_table="T", slo_checks_backend="bigquery")
    assert run_log._write_slo_checks_table(registry, [{"check_run_id": "r"}]) is None


def test_the_watermark_is_read_back_from_successful_runs(monkeypatch):
    fake = FakeClient(query_rows=[FakeRow(m=1730000000.0)])
    monkeypatch.setattr(bigquery_tables, "client", lambda metadata=None: fake)
    contract = _contract(run_log_table="p.d._lakelogic_run_log", run_log_backend="bigquery")
    got = run_log.get_last_run_watermark(contract, "Trips", "bronze", dataset="trips", data_layer="bronze")
    assert got == 1730000000.0
    sql, config = fake.queries[-1]
    assert "MAX(max_source_mtime)" in sql and "COALESCE(status, '') != 'failed'" in sql
    assert [p.value for p in config.query_parameters] == ["trips", "bronze"]


def test_a_missing_run_log_table_is_a_first_run(monkeypatch):
    class Boom(FakeClient):
        def query(self, sql, job_config=None):
            raise RuntimeError("Not found: Table p:d._lakelogic_run_log")

    monkeypatch.setattr(bigquery_tables, "client", lambda metadata=None: Boom())
    contract = _contract(run_log_table="p.d._lakelogic_run_log", run_log_backend="bigquery")
    assert run_log.get_last_run_watermark(contract, "Trips", "bronze", dataset="trips") is None
