"""lakelogic profile --contract on a SQL warehouse (Snowflake): the table is profiled over a DB-API
connection and lakelogic_profiles is written with SQL, no Spark. DuckDB stands in for Snowflake."""
import duckdb

from lakelogic.core.profile import DbApiExecutor, executor_for
from lakelogic.core.profile_report import active_spark, profile_contract
from lakelogic.core.profile_scope import sample_sql

CONTRACT = {
    "version": "1.0.0", "info": {"title": "Drivers", "table_name": "drivers"},
    "model": {"fields": [{"name": "driver_id", "type": "integer"}, {"name": "email", "type": "string", "pii": True}]},
    "profiling": {"findings": True},
}


def _warehouse():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE drivers AS SELECT range AS driver_id, 'd' || range || '@x.io' AS email FROM range(40)")
    conn.execute("INSERT INTO drivers VALUES (1, 'dup@x.io')")
    return conn


def test_a_warehouse_table_is_profiled_and_written_with_sql():
    conn = _warehouse()
    out = profile_contract(CONTRACT, connection=DbApiExecutor(conn, "duckdb"), results_table="lakelogic_profiles")
    assert out["payload"]["rows_input"] == 41 and out["results_table_error"] is None
    assert out["results_table"] == "lakelogic_profiles"
    rows = conn.execute("SELECT column_name, row_count, min_value FROM lakelogic_profiles ORDER BY 1").fetchall()
    assert rows == [("driver_id", 41, "0"), ("email", 41, None)]  # pii: min withheld


def test_a_second_profile_appends():
    conn = _warehouse()
    for _ in range(2):
        profile_contract(CONTRACT, connection=DbApiExecutor(conn, "duckdb"), results_table="lakelogic_profiles")
    assert conn.execute("SELECT COUNT(DISTINCT profile_id) FROM lakelogic_profiles").fetchone() == (2,)


def test_a_snowflake_connection_gets_the_snowflake_dialect():
    class FakeSnowflake:
        pass

    FakeSnowflake.__module__ = "snowflake.connector.connection"
    assert executor_for(FakeSnowflake()).dialect == "snowflake"


def test_snowflake_fixed_size_sample_has_no_seed():
    sql, _ = sample_sql('"t"', [("a", "INT")], {"mode": "sample", "sample_unit": "rows", "sample_rows": 1000,
                                               "sample_percent": None, "sample_order": "random"},
                        lambda n: f'"{n}"', dialect="snowflake")
    assert sql == 'SELECT * FROM "t" SAMPLE (1000 ROWS)'


def test_no_spark_session_here_means_no_spark():
    assert active_spark() is None
