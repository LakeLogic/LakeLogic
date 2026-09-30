"""Two Spark-only view bugs that broke every gold fact on Databricks (2026-09-29).

1. Each SQL / pivot / unpivot / rollup step now reads a view name used ONCE. Spark Connect
   resolves a view by name when the plan is analysed, after the loop had re-registered
   `source` as the step's own output, so `FROM source r` read its own result.
2. A link whose path is `table:...` is a table link even with no `type`; it used to go down the
   file branch and be skipped, so a query joining it hit TABLE_OR_VIEW_NOT_FOUND.
No local Spark: the view helper is exercised with a stub frame; the rest pins the code.
"""
from __future__ import annotations

import inspect

from lakelogic.engines.spark import SparkAdapter


class _Frame:
    def __init__(self):
        self.views = []

    def createOrReplaceTempView(self, name):  # noqa: N802 — Spark's name
        self.views.append(name)


def test_every_step_view_name_is_new():
    adapter = SparkAdapter.__new__(SparkAdapter)
    adapter._temp_src = "lakelogic_src_1"
    f = _Frame()
    names = [adapter._step_view(f) for _ in range(3)]
    assert len(set(names)) == 3 and "lakelogic_src_1" not in names
    assert f.views == names


def test_sql_and_shape_steps_read_a_step_view_not_the_reused_source():
    pre = inspect.getsource(SparkAdapter._apply_pre_transformations)
    post = inspect.getsource(SparkAdapter._apply_post_transformations)
    for src in (pre, post):
        assert "self._step_view(current_df), trans.sql" in src
        assert "self._temp_src, trans.sql" not in src
        assert "source_table=self.contract.dataset or self._temp_src" not in src


def test_a_table_path_link_is_registered_as_a_table():
    src = inspect.getsource(SparkAdapter._register_links)
    assert 'str(link.path).startswith("table:")' in src
