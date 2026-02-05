import polars as pl

from lakeguard import DataProcessor


def test_schema_evolution_append_allows_unknown():
    contract_data = {
        "version": "1.0.0",
        "dataset": "ingest_test",
        "server": {"type": "local", "path": "data", "mode": "ingest", "schema_evolution": "append"},
        "model": {"fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]},
    }
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "new_col": ["x", "y"]})
    processor = DataProcessor(engine="polars", contract=contract_data)
    good_df, bad_df = processor.run(df)

    assert len(bad_df) == 0
    assert "new_col" in good_df.columns


def test_schema_evolution_strict_quarantines_unknown():
    contract_data = {
        "version": "1.0.0",
        "dataset": "ingest_test",
        "server": {"type": "local", "path": "data", "mode": "ingest", "schema_evolution": "strict"},
        "model": {"fields": [{"name": "id", "type": "int"}]},
    }
    df = pl.DataFrame({"id": [1, 2], "extra": [10, 20]})
    processor = DataProcessor(engine="polars", contract=contract_data)
    good_df, bad_df = processor.run(df)

    assert len(good_df) == 0
    assert len(bad_df) == 2


def test_cast_to_string_ingest():
    contract_data = {
        "version": "1.0.0",
        "dataset": "ingest_test",
        "server": {"type": "local", "path": "data", "mode": "ingest", "cast_to_string": True},
        "model": {"fields": [{"name": "id", "type": "int"}, {"name": "amount", "type": "float"}]},
    }
    df = pl.DataFrame({"id": [1, 2], "amount": [10.5, 20.0]})
    processor = DataProcessor(engine="polars", contract=contract_data)
    good_df, _ = processor.run(df)

    assert good_df.dtypes == [pl.Utf8, pl.Utf8]
