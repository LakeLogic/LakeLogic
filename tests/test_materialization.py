import polars as pl

from lakelogic import DataProcessor


def test_lineage_injection(tmp_path):
    contract = {
        "version": "1.0.0",
        "dataset": "events",
        "lineage": {"enabled": True},
        "quality": {"row_rules": [{"name": "not_null", "sql": "value IS NOT NULL"}]},
    }
    df = pl.DataFrame({"value": [1, None, 3]})
    processor = DataProcessor(engine="polars", contract=contract)
    good_df, bad_df = processor.run(df, source_path=tmp_path / "source.csv")

    for frame in (good_df, bad_df):
        assert "_lakelogic_source" in frame.columns
        assert "_lakelogic_processed_at" in frame.columns
        assert "_lakelogic_run_id" in frame.columns
        assert frame["_lakelogic_source"][0] == str(tmp_path / "source.csv")


def test_materialization_partitioned_append(tmp_path):
    import pytest
    pytest.importorskip("pandas")
    contract = {
        "version": "1.0.0",
        "dataset": "events",
        "materialization": {
            "strategy": "append",
            "partition_by": ["event_date"],
            "reprocess_policy": "overwrite_partition_safe",
            "target_path": str(tmp_path / "out"),
            "format": "csv",
        },
    }
    df = pl.DataFrame(
        {"event_id": [1, 2], "event_date": ["2024-01-01", "2024-01-02"], "value": [10, 20]}
    )
    processor = DataProcessor(engine="polars", contract=contract)
    processor.materialize(df)

    part_a_dir = tmp_path / "out" / "event_date=2024-01-01"
    part_b_dir = tmp_path / "out" / "event_date=2024-01-02"
    assert list(part_a_dir.glob("data*.csv")), f"No CSV in {part_a_dir}"
    assert list(part_b_dir.glob("data*.csv")), f"No CSV in {part_b_dir}"


def test_materialization_merge(tmp_path):
    import pytest
    pytest.importorskip("pandas")
    contract = {
        "version": "1.0.0",
        "dataset": "customers",
        "primary_key": ["customer_id"],
        "materialization": {
            "strategy": "merge",
            "target_path": str(tmp_path / "customers.csv"),
            "format": "csv",
        },
    }
    processor = DataProcessor(engine="polars", contract=contract)

    df_initial = pl.DataFrame({"customer_id": [1, 2], "name": ["Alice", "Bob"]})
    processor.materialize(df_initial)

    df_update = pl.DataFrame({"customer_id": [2, 3], "name": ["Bobby", "Cara"]})
    processor.materialize(df_update)

    merged_path = tmp_path / "customers.csv"
    assert merged_path.exists()

def test_quarantine_table_duckdb(tmp_path):
    import pytest
    pytest.skip("DuckDB engine is deprecated")
    contract = {
        "version": "1.0.0",
        "dataset": "events",
        "quality": {"row_rules": [{"name": "not_null", "sql": "value IS NOT NULL"}]},
        "quarantine": {"target": "table:quarantine_events"},
        "metadata": {
            "quarantine_table_backend": "duckdb",
            "quarantine_table_database": str(tmp_path / "quarantine.duckdb"),
        },
    }
    df = pl.DataFrame({"value": [1, None]})
    processor = DataProcessor(engine="polars", contract=contract)
    good_df, bad_df = processor.run(df)
    processor.materialize(good_df, bad_df)

    import duckdb
    con = duckdb.connect(str(tmp_path / "quarantine.duckdb"))
    try:
        count = con.execute("SELECT COUNT(*) FROM quarantine_events").fetchone()[0]
    finally:
        con.close()
    assert count == len(bad_df)
