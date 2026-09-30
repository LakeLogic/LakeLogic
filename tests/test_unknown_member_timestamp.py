"""An unknown member built with no row to copy is stamped NOW, not 1900.

When every row of a dimension's first load was rejected, the unknown member had no row to
copy lineage from and got the 1900 default `_lakelogic_processed_at`. It is never rebuilt,
so the freshness and retention checks read it as a 126-year-old record forever - nine
Build Centre code dimensions failed retention that way (2026-09-30).
"""

from datetime import datetime, timedelta, timezone

import pandas as pd

from lakelogic.core.materialization import _inject_unknown_member_pandas


def test_empty_first_load_stamps_the_unknown_member_now():
    df = pd.DataFrame(
        {
            "trip_type_sk": pd.Series(dtype="object"),
            "trip_type": pd.Series(dtype="object"),
            "_lakelogic_processed_at": pd.Series(dtype="datetime64[ns, UTC]"),
        }
    )
    out = _inject_unknown_member_pandas(df, ["trip_type"], {"surrogate_key": "trip_type_sk"}, {"enabled": True})
    row = out[out["trip_type_sk"] == "-1"].iloc[0]
    stamped = pd.Timestamp(row["_lakelogic_processed_at"]).to_pydatetime()
    assert datetime.now(timezone.utc) - stamped < timedelta(minutes=5)


def test_with_rows_it_copies_the_batch_lineage():
    t = pd.Timestamp("2026-09-30T06:00:00Z")
    df = pd.DataFrame({"trip_type_sk": ["abc"], "trip_type": ["ride"], "_lakelogic_processed_at": [t]})
    out = _inject_unknown_member_pandas(df, ["trip_type"], {"surrogate_key": "trip_type_sk"}, {"enabled": True})
    assert pd.Timestamp(out[out["trip_type_sk"] == "-1"].iloc[0]["_lakelogic_processed_at"]) == t
