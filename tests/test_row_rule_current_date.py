"""A row rule written with CURRENT_DATE / CURRENT_TIMESTAMP must run on Polars.

Polars SQL has no `current_date`; the transform paths already rewrite it, the row-rule
path did not, so any contract with `date_of_birth <= CURRENT_DATE` failed to build.
"""

import datetime as dt

import polars as pl

from lakelogic.core.models import DataContract
from lakelogic.core.processor import DataProcessor


def _contract():
    return DataContract.model_validate(
        {
            "version": "1.0.0",
            "info": {"title": "S"},
            "model": {
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "dob", "type": "date"},
                    {"name": "ts", "type": "timestamp"},
                ]
            },
            "quality": {
                "row_rules": [
                    {"name": "dob_not_future", "sql": "dob <= CURRENT_DATE", "category": "consistency"},
                    {"name": "ts_not_future", "sql": "ts <= CURRENT_TIMESTAMP", "category": "consistency"},
                ]
            },
        }
    )


def test_current_date_row_rules_run_and_quarantine_future_rows():
    df = pl.DataFrame(
        {
            "id": ["ok", "future"],
            "dob": [dt.date(2010, 1, 1), dt.date(2999, 1, 1)],
            "ts": [dt.datetime(2020, 1, 1), dt.datetime(2999, 1, 1)],
        }
    )
    result = DataProcessor(_contract(), engine="polars").run(df)
    assert result.good["id"].to_list() == ["ok"]
    assert result.bad["id"].to_list() == ["future"]
