"""Built-in quality rules are translated to BigQuery SQL before they run.

A composite-key uniqueness rule is written once for every engine as
``COUNT(DISTINCT CONCAT_WS(...))``. BigQuery has no ``CONCAT_WS``, and the engine translated
transformation SQL but sent rule SQL as written, so a gold fact with a composite key failed
on BigQuery with "Function not found: CONCAT_WS" (seen on a live GCP run).
"""
from __future__ import annotations

from types import SimpleNamespace

from lakelogic.core.models import DataContract
from lakelogic.engines.bigquery import BigQueryAdapter


class _Client:
    def __init__(self):
        self.sql = []

    def query(self, sql, job_config=None):
        self.sql.append(sql)
        return SimpleNamespace(result=_Rows)


class _Rows(list):
    total_rows = 1

    def __init__(self):
        super().__init__([(0,)])


def _adapter(contract: dict) -> BigQueryAdapter:
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    adapter.contract = DataContract(**contract)
    adapter.dataset_rule_results = []
    adapter._session_id = None
    return adapter


def test_a_composite_unique_rule_reaches_bigquery_without_concat_ws():
    adapter = _adapter({
        "version": "1.0.0",
        "dataset": "gold_fact_campaigns",
        "model": {"fields": [{"name": "campaign_id", "type": "string"}, {"name": "date_key", "type": "integer"}]},
        "quality": {"dataset_rules": [{"unique": ["campaign_id", "date_key"]}]},
    })
    client = _Client()
    adapter._execute = lambda c, sql: None
    adapter._run_dataset_rules(client, "project.dataset.good")
    rule_sql = [s for s in client.sql if "COUNT(DISTINCT" in s]
    assert rule_sql, client.sql
    assert all("CONCAT_WS" not in s for s in rule_sql), rule_sql
    assert any("ARRAY_TO_STRING([" in s for s in rule_sql)
    assert adapter.dataset_rule_results[0]["passed"] is True


def test_row_rule_sql_is_translated_too():
    adapter = _adapter({
        "version": "1.0.0",
        "dataset": "silver_x",
        "model": {"fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]},
        "quality": {"row_rules": [{"name": "ab_present", "sql": "CONCAT_WS('-', a, b) IS NOT NULL"}]},
    })
    captured = []
    adapter._execute = lambda c, sql: captured.append(sql)
    adapter._apply_row_rules(_Client(), "project.dataset.src", [])
    (built,) = captured
    # The condition BigQuery evaluates is translated; the rule's own text stays in the message.
    assert "NOT COALESCE((ARRAY_TO_STRING([a, b], '-') IS NOT NULL), FALSE)" in built, built
    assert "NOT COALESCE((CONCAT_WS(" not in built


def test_a_tables_columns_come_from_its_metadata_not_a_cached_query():
    # A query result cached from before the table was rewritten carried a column the table no
    # longer had, and the cast step selected it ("Unrecognized name: column_0").
    adapter = _adapter({"version": "1.0.0", "dataset": "silver_x"})

    class _Meta:
        def get_table(self, name):
            assert name == "p.d.bronze_x"
            return SimpleNamespace(schema=[SimpleNamespace(name="campaign_id")])

        def query(self, *a, **k):  # pragma: no cover - must not be asked
            raise AssertionError("queried instead of reading the table's metadata")

    assert adapter._get_columns(_Meta(), "p.d.bronze_x") == ["campaign_id"]


def test_lakelogic_system_columns_survive_the_schema_cast():
    adapter = _adapter({
        "version": "1.0.0", "dataset": "silver_x",
        "model": {"fields": [{"name": "message_sid", "type": "string"}]},
    })
    captured = []
    adapter._execute = lambda c, sql: captured.append(sql)
    adapter._get_columns = lambda c, t: ["message_sid", "_lakelogic_processed_at", "_lakelogic_run_id"]
    adapter._apply_schema(None, "p.d.bronze_x")
    (cast,) = [s for s in captured if "lg_schema" in s or "SAFE_CAST" in s]
    assert "`_lakelogic_processed_at`" in cast and "`_lakelogic_run_id`" in cast, cast


def test_dataset_placeholder_names_the_table_under_test():
    # `SELECT COUNT(*) FROM {dataset} ...` reached BigQuery unreplaced ("Unexpected '{'").
    adapter = _adapter({
        "version": "1.0.0", "dataset": "gold_fact_app_events",
        "model": {"fields": [{"name": "city_sk", "type": "string"}]},
        "quality": {"dataset_rules": [{"name": "no_unknown_city", "sql": "SELECT COUNT(*) FROM {dataset} WHERE city_sk = '-1'", "must_be_less_than": 1}]},
    })
    client = _Client()
    adapter._execute = lambda c, sql: None
    adapter._run_dataset_rules(client, "p.d.good")
    assert client.sql == ["SELECT COUNT(*) FROM gold_fact_app_events WHERE city_sk = '-1'"]
