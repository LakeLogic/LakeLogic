import polars as pl

from lakelogic import DataProcessor


def test_processor_init():
    """Test processor initialization with different contract formats."""
    # From dict
    data = {"version": "1.0.0", "dataset": "test"}
    proc = DataProcessor(engine="polars", contract=data)
    assert proc.engine_name == "polars"
    assert proc.contract.version == "1.0.0"


def test_processor_run_mock():
    """Test the full run cycle with mock data."""
    contract_data = {
        "version": "1.0.0",
        "dataset": "test_ds",
        "quality": {"row_rules": [{"name": "not_null", "sql": "val IS NOT NULL"}]},
    }
    df = pl.DataFrame({"val": [1, None, 3]})

    processor = DataProcessor(engine="polars", contract=contract_data)
    good_df, bad_df = processor.run(df)

    assert len(good_df) == 2
    assert len(bad_df) == 1


def test_processor_slo_scores():
    """Test freshness and availability scoring."""
    import datetime as dt

    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    contract_data = {
        "version": "1.0.0",
        "dataset": "test_ds",
        "service_levels": {
            "freshness": {"field": "updated_at", "threshold": "24h"},
            "availability": {"field": "email", "threshold": 50.0},
        },
    }
    df = pl.DataFrame(
        {
            "email": ["a@example.com", None],
            "updated_at": [now.isoformat(), now.isoformat()],
        }
    )

    processor = DataProcessor(engine="polars", contract=contract_data)
    processor.run(df)

    slos = processor.last_report.get("slos", {})
    assert slos["freshness"]["passed"] is True
    assert slos["availability"]["passed"] is True


def test_processor_stage_overrides():
    """Stage overrides should merge into the base contract."""
    contract_data = {
        "version": "1.0.0",
        "transformations": [{"rename": {"from": "a", "to": "b"}}],
        "stages": {
            "bronze": {
                "server": {
                    "type": "local",
                    "path": "data/bronze",
                    "mode": "ingest",
                    "cast_to_string": True,
                },
                "transformations": [],
            }
        },
    }
    processor = DataProcessor(engine="polars", contract=contract_data, stage="bronze")
    assert processor.contract.server.mode == "ingest"
    assert processor.contract.server.cast_to_string is True
    assert processor.contract.transformations == []
