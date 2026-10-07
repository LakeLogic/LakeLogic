"""When a check runs (pre vs post) — one definition, every engine (2026-10-07).

Spark ran pre checks before post transforms (the OLC order); Polars and DuckDB ran every check
after them. A gold contract whose `required` column came from its post SQL produced rows on
Polars and quarantined every row on Spark. See lakelogic/core/rule_phases.py.
"""
import polars as pl
import pytest

from lakelogic import DataProcessor
from lakelogic.core.contract_lint import review_contract_dict
from lakelogic.core.rule_phases import effective_phase, phase_conflicts, post_created_columns


def _frames(result):
    g, b = result[0], result[1]
    return (g if isinstance(g, pl.DataFrame) else g.pl()), (b if isinstance(b, pl.DataFrame) else b.pl())


# ── the definition ────────────────────────────────────────────────────────────

def test_post_created_columns_covers_each_kind_and_skips_rewrites():
    contract = {
        "model": {"fields": [{"name": "id"}, {"name": "p_x"}]},
        "transformations": [
            {"derive": {"field": "net", "sql": "a - b"}},
            {"json_extract": {"field": "sku", "source": "item", "path": "$.sku"}},
            {"explode": {"field": "items", "output": "item"}},
            {"explode": {"field": "tags"}},  # in place: a rewrite, not a new column
            {"map_values": {"field": "status", "mapping": {"a": "A"}}},  # in place
            {"rename": {"mappings": {"old": "new"}}},
            {"rollup": {"group_by": ["id"], "aggregations": {"total": "SUM(x)"}}},
            {"sql": "SELECT currency AS currency, CAST(created_at AS DATE) AS created_date, "
                    "CAST(x AS DOUBLE) AS amt FROM source"},
            {"phase": "pre", "derive": {"field": "early", "sql": "1"}},  # pre: not post-created
        ],
    }
    created = post_created_columns(contract)
    assert {"net", "sku", "item", "new", "total", "created_date", "amt"} <= set(created)
    assert not {"tags", "status", "currency", "early", "DATE", "DOUBLE"} & set(created)


def test_a_phase_left_to_default_runs_where_its_columns_exist():
    created = {"net": "derive"}
    assert effective_phase({"name": "r", "sql": "net >= 0"}, created) == "post"
    assert effective_phase({"name": "r", "sql": "amount >= 0"}, created) == "pre"
    assert effective_phase({"name": "net_required", "sql": "net IS NOT NULL"}, created, field="net") == "post"
    # Written phases are honoured as written.
    assert effective_phase({"name": "r", "sql": "net >= 0", "phase": "pre"}, created) == "pre"
    assert effective_phase({"name": "r", "sql": "amount >= 0", "phase": "post"}, created) == "post"


def test_lint_flags_an_explicit_pre_rule_on_a_post_column_only():
    contract = {
        "version": "1.0", "info": {"title": "t"},
        "model": {"fields": [{"name": "amount", "type": "double"}]},
        "transformations": [{"derive": {"field": "net", "sql": "amount - 1"}}],
        "quality": {"row_rules": [{"name": "net_pos", "sql": "net >= 0", "phase": "pre"},
                                  {"name": "net_ok", "sql": "net >= 0"}]},
    }
    assert [c["rule"] for c in phase_conflicts(contract)] == ["net_pos"]
    findings = [f for f in review_contract_dict(contract, "t") if f.check_id == "PHS-001"]
    assert len(findings) == 1 and "net_pos" in findings[0].message and "phase: post" in findings[0].suggestion


# ── every engine, one answer ──────────────────────────────────────────────────

ENGINES = ["polars", "duckdb"]


@pytest.mark.parametrize("engine", ENGINES)
def test_an_explicit_pre_rule_checks_the_source_value(engine):
    contract = {"version": "1.0", "info": {"title": "t"},
                "model": {"fields": [{"name": "id", "type": "int"}, {"name": "amount", "type": "double"}]},
                "transformations": [{"phase": "post", "derive": {"field": "amount", "sql": "ABS(amount)"}}],
                "quality": {"row_rules": [{"name": "amount_nonneg", "phase": "pre", "sql": "amount >= 0"}]}}
    good, bad = _frames(DataProcessor(engine=engine, contract=contract).run(
        pl.DataFrame({"id": [1, 2], "amount": [5.0, -5.0]})))
    assert good.select(["id", "amount"]).rows() == [(1, 5.0)]  # the column survives the rewrite
    assert bad["id"].to_list() == [2]  # -5 failed BEFORE ABS() ran


@pytest.mark.parametrize("engine", ENGINES)
def test_a_required_field_created_post_is_checked_on_the_output(engine):
    contract = {"version": "1.0", "info": {"title": "t"},
                "model": {"fields": [{"name": "id", "type": "int"}, {"name": "amount", "type": "double"},
                                     {"name": "fee", "type": "double"},
                                     {"name": "net", "type": "double", "required": True}]},
                "transformations": [{"derive": {"field": "net", "sql": "amount - fee"}}]}
    good, bad = _frames(DataProcessor(engine=engine, contract=contract).run(
        pl.DataFrame({"id": [1, 2], "amount": [10.0, 10.0], "fee": [2.0, None]})))
    assert sorted(good.select(["id", "net"]).rows()) == [(1, 8.0)]
    assert bad["id"].to_list() == [2]


@pytest.mark.parametrize("engine", ENGINES)
def test_a_row_failing_a_pre_rule_is_not_summed_into_a_post_aggregate(engine):
    contract = {"version": "1.0", "info": {"title": "t"},
                "model": {"fields": [{"name": "status", "type": "string", "required": True},
                                     {"name": "total", "type": "double", "required": True}]},
                "transformations": [{"sql": "SELECT status, SUM(amount) AS total FROM source GROUP BY status"}],
                "quality": {"row_rules": [{"name": "amount_nonneg", "phase": "pre", "sql": "amount >= 0"}]}}
    good, bad = _frames(DataProcessor(engine=engine, contract=contract).run(
        pl.DataFrame({"id": [1, 2, 3], "status": ["a", "a", "b"], "amount": [5.0, -3.0, 4.0]})))
    assert sorted(good.select(["status", "total"]).rows()) == [("a", 5.0), ("b", 4.0)]
    assert bad["id"].to_list() == [2]
