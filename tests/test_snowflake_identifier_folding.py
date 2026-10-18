"""Snowflake folds unquoted names to UPPER case. Contracts are lowercase; the engine must quote
`trip_id` as "TRIP_ID" and read TRIP_ID back as trip_id, or a normally created table never
matches its contract (the RideFlow demo had to upper-case every contract to run)."""

from lakelogic.engines import snowflake as sf


def _adapter():
    return sf.SnowflakeAdapter.__new__(sf.SnowflakeAdapter)


def test_plain_lowercase_is_quoted_as_snowflake_stores_it():
    a = _adapter()
    assert a._quote_ident("trip_id") == '"TRIP_ID"'
    assert (
        a._quote_qualified("lldemo_dev_marketplace.silver.silver_trips")
        == '"LLDEMO_DEV_MARKETPLACE"."SILVER"."SILVER_TRIPS"'
    )


def test_mixed_case_and_already_quoted_names_are_kept():
    a = _adapter()
    assert a._quote_ident("TripId") == '"TripId"'
    assert a._quote_ident('"trip_id"') == '"trip_id"'


def test_upper_case_names_read_back_in_the_contract_spelling():
    assert sf._unfold("TRIP_ID") == "trip_id"
    assert sf._unfold("_LAKELOGIC_PROCESSED_AT") == "_lakelogic_processed_at"
    assert sf._unfold("TripId") == "TripId"


def test_preserve_case_switch_keeps_the_old_behaviour(monkeypatch):
    monkeypatch.setenv("LAKELOGIC_SNOWFLAKE_PRESERVE_CASE", "1")
    assert _adapter()._quote_ident("trip_id") == '"trip_id"'
    assert sf._unfold("TRIP_ID") == "TRIP_ID"


class _Cur:
    def __init__(self, cols, log):
        self.cols, self.log, self.description = cols, log, None

    def execute(self, sql):
        self.log.append(sql)
        self.description = [(c,) for c in self.cols]
        return self

    def close(self):
        pass


class _Conn:
    def __init__(self, cols):
        self.cols, self.log = cols, []

    def cursor(self):
        return _Cur(self.cols, self.log)


def test_engine_columns_ride_through_the_schema_step():
    """Live run 2026-10-18: silver's deduplicate sorts by _lakelogic_processed_at; the schema
    step selected only contract fields, so the sort column was gone ('invalid identifier')."""
    from lakelogic.core.models import DataContract

    contract = DataContract.model_validate(
        {"version": "1.0.0", "info": {"title": "t"}, "model": {"fields": [{"name": "id", "type": "string"}]}}
    )
    a = sf.SnowflakeAdapter(contract)
    conn = _Conn(["ID", "_LAKELOGIC_PROCESSED_AT", "STRAY"])
    a._execute = lambda c, sql: conn.log.append(sql)
    a._apply_schema(conn, "SRC")
    create = next(s for s in conn.log if s.startswith("CREATE OR REPLACE TEMP TABLE"))
    assert '"_LAKELOGIC_PROCESSED_AT"' in create
