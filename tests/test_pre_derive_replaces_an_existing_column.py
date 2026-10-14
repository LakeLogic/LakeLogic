"""A PRE derive onto a column the frame already has replaces it — on every engine.

Polars SQL's ``SELECT * EXCLUDE (x), <expr> AS x`` silently drops ``x``. The post phase was
fixed for that on 2026-10-07; the pre phase still used it, so a gold ``system`` column derived
from a constant vanished whenever the source carried a ``system`` column too, and every row
then failed the column's own required check (2026-10-13).
"""

import polars as pl
import pytest

from lakelogic import DataProcessor


def _frames(result):
    g, b = result[0], result[1]
    return (g if isinstance(g, pl.DataFrame) else g.pl()), (b if isinstance(b, pl.DataFrame) else b.pl())


def _contract(sql):
    return {
        "version": "1.0.0",
        "info": {"title": "t", "target_layer": "gold"},
        "model": {"fields": [{"name": "id", "type": "long"}, {"name": "system", "type": "string", "required": True}]},
        "transformations": [{"phase": "pre", "derive": {"field": "system", "sql": sql}}],
    }


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_a_constant_derived_onto_an_existing_column_replaces_it(engine):
    frame = pl.DataFrame({"id": [1, 2], "system": ["SYS-1", None]})
    good, bad = _frames(DataProcessor(engine=engine, contract=_contract("'rideflow'")).run(frame))
    assert bad.height == 0
    assert good["system"].to_list() == ["rideflow", "rideflow"]


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_a_derive_that_reads_its_own_column_still_sees_the_old_value(engine):
    frame = pl.DataFrame({"id": [1, 2], "system": [" a ", "b"]})
    good, _bad = _frames(DataProcessor(engine=engine, contract=_contract("UPPER(TRIM(system))")).run(frame))
    assert sorted(good["system"].to_list()) == ["A", "B"]


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_a_derive_onto_a_new_column_is_unchanged(engine):
    frame = pl.DataFrame({"id": [1, 2]})
    good, bad = _frames(DataProcessor(engine=engine, contract=_contract("'rideflow'")).run(frame))
    assert bad.height == 0 and good["system"].to_list() == ["rideflow", "rideflow"]
