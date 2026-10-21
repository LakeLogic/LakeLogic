"""A number stored as text is a measurement, never an allowed-values list (2026-10-20)."""

from lakelogic.core.profile_findings import is_category_candidate


def _col(name, *values):
    return {"name": name, "frequencies": [{"value": v} for v in values]}


def test_numbers_are_not_categories_codes_are():
    assert not is_category_candidate(_col("estimated_eta_minutes", "2", "3", "12"))
    assert not is_category_candidate(_col("rider_rating", "1", "2", "5"))
    assert not is_category_candidate(_col("surge_multiplier", "1.0", "1.5"))
    assert is_category_candidate(_col("payment_method", "card", "cash", "apple_pay"))


def test_dates_stored_as_text_keep_their_earliest_and_latest_names_do_not():
    from lakelogic.core.profile_findings import hide_text_ranges

    doc = {
        "columns": [
            {
                "name": "requested_at",
                "type": "VARCHAR",
                "min": "2026-10-01T08:00:00.000Z",
                "max": "2026-10-13T16:48:21Z",
            },
            {"name": "name", "type": "VARCHAR", "min": "Aaron", "max": "Zoe"},
            {"name": "date_of_birth", "type": "VARCHAR", "min": "1960-01-01", "max": "2004-05-05", "sensitive": True},
        ]
    }
    cols = {c["name"]: c for c in hide_text_ranges(doc)["columns"]}
    assert (
        cols["requested_at"]["min"] == "2026-10-01T08:00:00.000Z"
        and cols["requested_at"]["range_kind"] == "dates_in_text"
    )
    assert cols["name"]["min"] is None and cols["name"]["range_hidden"] == "text"
    assert cols["date_of_birth"]["min"] is None  # a date of birth is personal: still hidden


def test_numbers_in_a_text_column_compare_as_numbers():
    import duckdb
    from lakelogic.core import profile_findings as pf

    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES ('0.00'),('0'),('1.5'),('2.43'),('abc')) v(tip)")
    rel = pf.Relation("t", lambda n: '"' + n + '"', lambda s, k: con.execute(s).fetchmany(k))
    out = dict(pf.outside_accepted(rel, "tip", ["0", "1.5", "2"], "VARCHAR"))
    assert out == {"2.43": 1, "abc": 1}  # 0.00 equals 0; text that is no number is outside
