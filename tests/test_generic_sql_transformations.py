"""``GenericSQLAdapter`` applies transformations as SQL, or refuses by name.

The adapter had no transformation pipeline at all: it evaluated quality rules and
materialised, nothing more. Because rules run AFTER transformations in the
contract's semantics, a contract that renamed or cast a column before validating
was evaluated against UNTRANSFORMED data and reported success — a wrong answer
presented as a clean one.

It now renders transformations as chained derived tables. Subqueries rather than
temp tables deliberately: temp-table syntax and permissions differ across the
dialects this adapter claims (``CREATE TEMP`` vs ``CREATE TEMPORARY``; Trino barely
has them), while a derived table is plain ANSI SQL needing no DDL rights.

These tests run against a REAL database (DuckDB over DB-API), not a mock, so the
generated SQL has to actually parse and execute.
"""

from __future__ import annotations

import pytest

duckdb = pytest.importorskip("duckdb")

from lakelogic.core.models import DataContract
from lakelogic.engines.generic_sql import GenericSQLAdapter


@pytest.fixture
def conn():
    c = duckdb.connect()
    c.execute("CREATE TABLE raw_orders (id INTEGER, old_status VARCHAR, amount VARCHAR)")
    c.execute("INSERT INTO raw_orders VALUES (1,'ok','10.5'),(2,'OK','20.25'),(3,NULL,'5.0')")
    yield c
    c.close()


def _adapter(conn, transformations, rules=None):
    contract = DataContract(
        **{
            "version": "1.0.0",
            "dataset": "orders",
            "model": {
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "status", "type": "string"},
                    {"name": "amount", "type": "float"},
                ]
            },
            "quality": {"row_rules": rules or [{"name": "status_not_null", "sql": "status IS NOT NULL"}]},
            "transformations": transformations,
        }
    )
    return GenericSQLAdapter(contract, conn, dialect="duckdb", source_table="raw_orders")


# ── the core property: rules see TRANSFORMED data ────────────────────────────


def test_rules_are_evaluated_against_renamed_columns(conn):
    """The bug in one line: the rule names `status`, which only exists AFTER the
    rename. Previously the rename never happened and the rule ran on raw data."""
    good, _ = _adapter(conn, [{"rename": {"mappings": {"old_status": "status"}}}]).execute()
    assert good["count"] == 2  # row 3 has NULL status


def test_transformations_chain_in_order(conn):
    """Each step wraps the previous, so a later step sees the earlier one's output."""
    adapter = _adapter(
        conn,
        [
            {"rename": {"mappings": {"old_status": "status"}}},
            {"cast": {"columns": {"amount": "float"}}},
        ],
    )
    sql = adapter._transformation_source()
    assert sql.count("lakelogic_t") == 2  # two nested derived tables
    assert sql.index("old_status") > sql.index("CAST")  # rename is the INNER query
    good, _ = adapter.execute()
    assert good["count"] == 2


def test_filter_reduces_the_row_set_before_rules(conn):
    good, _ = _adapter(
        conn,
        [
            {"rename": {"mappings": {"old_status": "status"}}},
            {"filter": {"sql": "id < 3"}},
        ],
    ).execute()
    assert good["count"] == 2  # row 3 filtered out entirely


def test_upper_is_applied(conn):
    """'ok' and 'OK' collapse once uppercased, proving the function reached the DB."""
    adapter = _adapter(
        conn,
        [
            {"rename": {"mappings": {"old_status": "status"}}},
            {"upper": {"fields": ["status"]}},
        ],
        rules=[{"name": "is_ok", "sql": "status = 'OK'"}],
    )
    good, _ = adapter.execute()
    assert good["count"] == 2  # both 'ok' and 'OK' now pass


def test_generated_sql_executes_on_a_real_database(conn):
    """A pipeline that builds unparseable SQL is worse than one that refuses."""
    adapter = _adapter(conn, [{"rename": {"mappings": {"old_status": "status"}}}])
    src = adapter._transformation_source()
    rows = conn.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
    assert rows == 3


def test_no_transformations_leaves_the_source_untouched(conn):
    """The plain path must stay exactly as it was — no wrapping, no overhead."""
    assert _adapter(conn, [])._transformation_source() == "raw_orders"


# ── refusals: named, never silent ────────────────────────────────────────────


@pytest.mark.parametrize("op", ["join", "lookup", "explode", "pivot", "unpivot"])
def test_inexpressible_ops_are_refused_by_name(conn, op):
    """These need a second relation or reshape the row set. Refusing by NAME is the
    point — the message must say which op, so the user can act on it."""
    payload = {
        "join": {"reference": "r", "on": "id", "key": "id", "fields": ["x"]},
        "lookup": {"field": "x", "reference": "r", "on": "id", "key": "id", "value": "v"},
        "explode": {"field": "x"},
        "pivot": {"on": "x", "using": "y"},
        "unpivot": {"columns": ["a"], "name_field": "k", "value_field": "v"},
    }[op]
    adapter = _adapter(conn, [{op: payload}])
    with pytest.raises(NotImplementedError) as exc:
        adapter.execute()
    assert op in str(exc.value)


def test_unordered_deduplicate_is_refused(conn):
    """Without sort_by the surviving row is arbitrary, so the run is irreproducible.

    The refusal lands at the MODEL layer (sort_by is a required field), before any
    engine sees the contract — which is stricter and earlier than the adapter guard.
    Pinned here so a later relaxation of the model cannot quietly reintroduce
    non-deterministic dedup on this engine."""
    with pytest.raises(Exception) as exc:
        _adapter(conn, [{"deduplicate": {"on": ["id"]}}])
    assert "sort_by" in str(exc.value)


def test_refusal_names_a_way_forward_and_states_the_consequence(conn):
    """A refusal that only says "no" is a dead end. It must say what would go wrong
    (rules evaluated on untransformed data) and what to do instead.

    This is the guarantee the earlier blanket-refusal guard existed to protect; it
    survives here now that only inexpressible ops are refused.
    """
    adapter = _adapter(conn, [{"explode": {"field": "x"}}])
    with pytest.raises(NotImplementedError) as exc:
        adapter.execute()
    msg = str(exc.value)
    assert "skipped them" in msg  # the consequence
    assert "pre-materialise" in msg  # the workaround
    assert "polars" in msg and "duckdb" in msg  # engines that can


def test_refusal_happens_before_any_database_work(conn):
    """Refuse up front. A guard that fires after rules have run has already produced
    the misleading numbers it was meant to prevent."""

    class _ExplodingConn:
        def cursor(self):
            raise AssertionError("touched the database before refusing")

    contract = _adapter(conn, [{"explode": {"field": "x"}}]).contract
    adapter = GenericSQLAdapter(contract, _ExplodingConn(), dialect="postgres", source_table="t")
    with pytest.raises(NotImplementedError):
        adapter.execute()


def test_unsupported_cast_type_is_refused_not_silently_downgraded(conn):
    adapter = _adapter(conn, [{"cast": {"columns": {"amount": "geography"}}}])
    with pytest.raises(NotImplementedError) as exc:
        adapter.execute()
    assert "geography" in str(exc.value)
