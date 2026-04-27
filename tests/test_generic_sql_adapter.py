from __future__ import annotations

import types

from lakelogic.engines.generic_sql import GenericSQLAdapter


class FakeCursor:
    def __init__(self, rowcount=0, fetchone_value=None, fetchall_rows=None):
        self.rowcount = rowcount
        self._fetchone_value = fetchone_value
        self._fetchall_rows = fetchall_rows or []
        self.closed = False

    def execute(self, sql):
        self.last_sql = sql

    def fetchone(self):
        return self._fetchone_value

    def fetchall(self):
        return self._fetchall_rows

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.commit_calls = 0
        self.rollback_calls = 0
        self.cursors = []

    def cursor(self):
        cursor = FakeCursor()
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


def _make_contract(fields):
    return types.SimpleNamespace(
        model=types.SimpleNamespace(fields=fields),
        quality=types.SimpleNamespace(enforce_required=False, row_rules=[], dataset_rules=[]),
        dataset="validated_orders",
    )


def _make_adapter(fields=None):
    fields = fields or [
        types.SimpleNamespace(name="id", type="integer", required=True, rules=[], default=None),
        types.SimpleNamespace(name="amount", type="double", required=False, rules=[], default=0),
    ]
    return GenericSQLAdapter(_make_contract(fields), FakeConnection(), dialect="postgres", source_table="raw.orders")


def test_generic_sql_execute_generate_ddl_and_validate_connection(monkeypatch):
    adapter = _make_adapter()
    traces = []
    row_rules = [
        types.SimpleNamespace(name="amount_positive", sql="amount > 0", category="validity"),
        types.SimpleNamespace(name="id_present", sql="id IS NOT NULL", category="completeness"),
    ]
    dataset_rules = [
        types.SimpleNamespace(name="few_nulls", sql="SELECT 0.1", must_be_less_than=0.5, must_be_greater_than=None),
        types.SimpleNamespace(name="enough_rows", sql="SELECT 100", must_be_less_than=None, must_be_greater_than=50),
        types.SimpleNamespace(name="between_range", sql="SELECT 4", must_be_less_than=None, must_be_greater_than=None, must_be_between=(1, 5)),
    ]

    monkeypatch.setattr(adapter, "get_row_rules", lambda: row_rules)
    monkeypatch.setattr(adapter, "get_dataset_rules", lambda: dataset_rules)
    monkeypatch.setattr(adapter, "_transpile", lambda sql, read_dialect="duckdb": sql)
    monkeypatch.setattr(adapter, "_add_trace", lambda *args, **kwargs: traces.append((args, kwargs)))

    def fake_fetch_count(sql):
        if sql == "SELECT COUNT(*) FROM raw.orders":
            return 10
        if sql == "SELECT COUNT(*) FROM raw.orders WHERE NOT (amount > 0)":
            return 1
        if sql == "SELECT COUNT(*) FROM raw.orders WHERE NOT (id IS NOT NULL)":
            return 0
        if sql == "SELECT COUNT(*) FROM raw.orders WHERE (amount > 0) AND (id IS NOT NULL)":
            return 9
        return 0

    monkeypatch.setattr(adapter, "_fetch_count", fake_fetch_count)
    monkeypatch.setattr(adapter, "_fetch_scalar", lambda sql: 0.1 if sql == "SELECT 0.1" else 100 if sql == "SELECT 100" else 4)

    good, bad = adapter.execute()
    assert good["count"] == 9
    assert bad["count"] == 1
    assert bad["rule_failures"] == {"amount_positive": 1, "id_present": 0}
    assert all(result["passed"] for result in adapter.dataset_rule_results)
    assert traces

    ddl = adapter.generate_ddl()
    assert "CREATE TABLE validated_orders" in ddl
    assert "id INTEGER NOT NULL" in ddl

    assert adapter.validate_connection()["status"] == "ok"
    monkeypatch.setattr(adapter, "_fetch_count", lambda sql: (_ for _ in ()).throw(RuntimeError("boom")))
    assert adapter.validate_connection()["status"] == "error"


def test_generic_sql_schema_sync_and_alter_helpers(monkeypatch):
    fields = [
        types.SimpleNamespace(name="id", type="integer", required=True, rules=[], default=None),
        types.SimpleNamespace(name="status", type="text", required=False, rules=[], default="new"),
    ]
    adapter = _make_adapter(fields)
    executed = []

    monkeypatch.setattr(adapter, "_transpile", lambda sql, read_dialect="duckdb": sql)
    monkeypatch.setattr(adapter, "_add_trace", lambda *args, **kwargs: None)
    monkeypatch.setattr(adapter, "_get_table_columns", lambda table=None: ["id", "legacy_col"])
    monkeypatch.setattr(adapter, "_execute_sql", lambda sql: executed.append(sql) or FakeCursor())

    dry_run = adapter.sync_schema(target_table="gold.orders", dry_run=True)
    assert dry_run["added"] == ["status"]
    assert dry_run["extra_in_table"] == ["legacy_col"]
    assert "ALTER TABLE gold.orders ADD COLUMN status TEXT DEFAULT 'new'" in dry_run["alter_statements"][0]

    result = adapter.sync_schema(target_table="gold.orders", dry_run=False)
    assert result["status"] == "ok"
    assert adapter.conn.commit_calls == 1
    assert executed

    add_result = adapter.alter_add_column("gold.orders", "created_at", "TIMESTAMP", default="CURRENT_TIMESTAMP", not_null=True)
    assert add_result["status"] == "ok"
    drop_result = adapter.alter_drop_column("gold.orders", "legacy_col")
    assert drop_result["status"] == "ok"

    monkeypatch.setattr(adapter, "_execute_sql", lambda sql: (_ for _ in ()).throw(RuntimeError("sql failed")))
    error_result = adapter.alter_add_column("gold.orders", "broken")
    assert error_result["status"] == "error"
    assert adapter.conn.rollback_calls >= 1


def test_generic_sql_materialization_and_condition_builders(monkeypatch):
    adapter = _make_adapter()
    executed = []
    row_rules = [types.SimpleNamespace(name="amount_positive", sql="amount > 0", category="validity")]

    monkeypatch.setattr(adapter, "get_row_rules", lambda: row_rules)
    monkeypatch.setattr(adapter, "_transpile", lambda sql, read_dialect="duckdb": sql)
    monkeypatch.setattr(adapter, "_execute_sql", lambda sql: executed.append(sql) or FakeCursor())
    monkeypatch.setattr(adapter, "_fetch_count", lambda sql: 4)
    monkeypatch.setattr(adapter, "_add_trace", lambda *args, **kwargs: None)

    assert adapter._build_pass_condition() == "(amount > 0)"
    assert adapter._build_fail_condition() == "NOT (amount > 0)"
    error_sql = adapter._build_error_columns_sql()
    assert adapter.ERROR_COLUMN in error_sql
    assert adapter.CATEGORY_COLUMN in error_sql

    good = adapter.materialize_good("gold.orders", if_exists="replace")
    bad = adapter.materialize_bad("quarantine.orders", if_exists="append", include_error_columns=True)
    both = adapter.create_tables("gold.orders", "quarantine.orders", if_exists="fail")

    assert good["rows"] == 4
    assert bad["rows"] == 4
    assert both["good"]["status"] == "ok"
    assert both["bad"]["status"] == "ok"
    assert any("CREATE TABLE gold.orders AS SELECT * FROM raw.orders WHERE (amount > 0)" in sql for sql in executed)
    assert any("INSERT INTO quarantine.orders SELECT *" in sql for sql in executed)


def test_generic_sql_merge_insert_update_delete_and_reprocess(monkeypatch):
    adapter = _make_adapter()
    traces = []
    executed = []

    monkeypatch.setattr(adapter, "_transpile", lambda sql, read_dialect="duckdb": sql)
    monkeypatch.setattr(adapter, "_build_pass_condition", lambda: "amount > 0")
    monkeypatch.setattr(adapter, "_fetch_count", lambda sql: 7 if "FROM gold.orders" in sql else 3)
    monkeypatch.setattr(adapter, "_add_trace", lambda *args, **kwargs: traces.append((args, kwargs)))

    def fake_execute_sql(sql):
        executed.append(sql)
        return FakeCursor(rowcount=2)

    monkeypatch.setattr(adapter, "_execute_sql", fake_execute_sql)

    merge = adapter.merge("gold.orders", merge_keys=["id"], update_columns=["amount"])
    insert = adapter.insert_validated("gold.orders")
    update = adapter.update_where("gold.orders", {"status": "'done'"}, "id = 1")
    delete = adapter.delete_where("gold.orders", "id = 1")

    assert merge["status"] == "ok"
    assert insert["rows_inserted"] == 3
    assert update["rows_affected"] == 2
    assert delete["rows_deleted"] == 2
    assert traces

    monkeypatch.setattr(adapter, "execute", lambda df=None: ({"count": 2}, {"count": 1}))
    monkeypatch.setattr(adapter, "merge", lambda **kwargs: {"status": "ok", "rows": 2})
    reprocess = adapter.reprocess_quarantine("quarantine.orders", "gold.orders", ["id"])
    assert reprocess["recovered"] == 2
    assert reprocess["still_quarantined"] == 1
    assert adapter.source_table == "raw.orders"


def test_generic_sql_metadata_materialize_modes_and_error_paths(monkeypatch):
    adapter = _make_adapter()
    warnings = []
    monkeypatch.setattr("lakelogic.engines.generic_sql.logger.warning", warnings.append)
    monkeypatch.setattr(adapter, "_transpile", lambda sql, read_dialect="duckdb": sql)

    columns_cursor = FakeCursor(fetchall_rows=[("ID",), ("Status",)])
    monkeypatch.setattr(adapter, "_execute_sql", lambda sql: columns_cursor)
    assert adapter._get_table_columns("catalog.sales.orders") == ["id", "status"]

    monkeypatch.setattr(adapter, "_execute_sql", lambda sql: (_ for _ in ()).throw(RuntimeError("no metadata")))
    assert adapter._get_table_columns("orders") == []
    assert any("Could not fetch columns for orders" in message for message in warnings)

    executed = []
    monkeypatch.setattr(adapter, "_build_fail_condition", lambda: "NOT (amount > 0)")
    monkeypatch.setattr(adapter, "_build_error_columns_sql", lambda: ", errors_col")
    monkeypatch.setattr(adapter, "_execute_sql", lambda sql: executed.append(sql) or FakeCursor())
    monkeypatch.setattr(adapter, "_fetch_count", lambda sql: 2)
    monkeypatch.setattr(adapter, "_add_trace", lambda *args, **kwargs: None)

    bad_fail = adapter.materialize_bad("quarantine.orders", if_exists="fail", include_error_columns=False)
    assert bad_fail["rows"] == 2
    assert any("CREATE TABLE quarantine.orders AS SELECT * FROM raw.orders WHERE NOT (amount > 0)" in sql for sql in executed)

    created = adapter.create_tables("gold.orders", bad_table=None, if_exists="append")
    assert created["good"]["status"] == "ok"
    assert "bad" not in created

    traces = []
    monkeypatch.setattr(adapter, "_build_pass_condition", lambda: "1=1")
    monkeypatch.setattr(adapter, "_add_trace", lambda *args, **kwargs: traces.append((args, kwargs)))
    inserted = adapter.insert_validated("gold.orders", validate_before_insert=False)
    assert inserted["rows_inserted"] == 2
    assert traces

    monkeypatch.setattr(adapter, "_execute_sql", lambda sql: (_ for _ in ()).throw(RuntimeError("update failed")))
    assert adapter.update_where("gold.orders", {"status": "'done'"}, "id = 1")["status"] == "error"
    assert adapter.delete_where("gold.orders", "id = 1")["status"] == "error"


def test_generic_sql_reprocess_quarantine_no_recoveries(monkeypatch):
    adapter = _make_adapter()
    monkeypatch.setattr(adapter, "execute", lambda df=None: ({"count": 0}, {"count": 3}))
    merge_calls = []
    monkeypatch.setattr(adapter, "merge", lambda **kwargs: merge_calls.append(kwargs) or {"status": "ok"})

    result = adapter.reprocess_quarantine("quarantine.orders", "gold.orders", ["id"])

    assert result == {"status": "ok", "recovered": 0, "still_quarantined": 3, "merge_result": None}
    assert merge_calls == []
    assert adapter.source_table == "raw.orders"
