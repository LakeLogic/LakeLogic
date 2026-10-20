"""`table:` references resolve to local Delta folders under LAKELOGIC_TABLE_ROOT (2026-10-20).

A project generated for a platform names its tables (`table:`acme_dev_marketplace`.silver.x`).
Without a catalog, polars/duckdb could not read or write those names, so the project could not
run on a laptop or in a plain container. With the root set, each name is a Delta folder.
"""

import textwrap

import polars as pl
import pytest

from lakelogic.engines.catalog_resolver import local_table_path


def test_a_name_maps_to_a_folder_only_when_the_root_is_set(monkeypatch):
    monkeypatch.delenv("LAKELOGIC_TABLE_ROOT", raising=False)
    assert local_table_path("table:a.b.c") is None
    monkeypatch.setenv("LAKELOGIC_TABLE_ROOT", "/data/tables/")
    assert (
        local_table_path("table:`acme_dev_marketplace`.silver.silver_trips")
        == "/data/tables/acme_dev_marketplace/silver/silver_trips"
    )
    assert local_table_path("/already/a/path") is None
    assert local_table_path(None) is None


def test_a_table_target_is_written_and_read_back_as_a_table_source(tmp_path, monkeypatch):
    pytest.importorskip("deltalake")
    from lakelogic import DataProcessor

    monkeypatch.setenv("LAKELOGIC_TABLE_ROOT", str(tmp_path / "tables"))
    landing = tmp_path / "orders.csv"
    pl.DataFrame({"order_id": [1, 2, 3], "amount": [10.0, -1.0, 7.5]}).write_csv(landing)

    silver = tmp_path / "silver.yaml"
    silver.write_text(
        textwrap.dedent(f"""
        version: 1.0.0
        dataset: silver_orders
        info: {{title: Silver orders, owner: t@example.com, target_layer: silver}}
        source: {{type: landing, path: "{landing.as_posix()}", format: csv}}
        model:
          fields:
            - {{name: order_id, type: integer, required: true}}
            - {{name: amount, type: float, required: true}}
        quality:
          row_rules:
            - {{name: positive_amount, sql: "amount > 0"}}
        materialization: {{strategy: overwrite, format: delta, target_path: "table:`acme_dev_shop`.silver.silver_orders"}}
    """),
        encoding="utf-8",
    )
    proc = DataProcessor(str(silver), engine="polars")
    result = proc.run_source(str(landing))
    proc.materialize(result.good, result.bad)
    written = tmp_path / "tables" / "acme_dev_shop" / "silver" / "silver_orders"
    assert (written / "_delta_log").is_dir()

    gold = tmp_path / "gold.yaml"
    gold.write_text(
        textwrap.dedent("""
        version: 1.0.0
        dataset: gold_orders
        info: {title: Gold orders, owner: t@example.com, target_layer: gold}
        source: {type: table, path: "table:`acme_dev_shop`.silver.silver_orders"}
        model:
          fields:
            - {name: order_id, type: integer}
            - {name: amount, type: float}
    """),
        encoding="utf-8",
    )
    out = DataProcessor(str(gold), engine="polars").run_source()
    assert out.good_count == 2  # the negative amount was quarantined at silver
