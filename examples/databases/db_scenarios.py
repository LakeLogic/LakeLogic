"""Database ingestion scenarios against REAL Azure SQL and Azure PostgreSQL.

Seeds a `rides` table, runs LakeLogic `source.type: database` contracts, and checks the outcome.
Connection settings come from the environment (passwords never in files):

    AZSQL_HOST=<server>.database.windows.net  AZSQL_USER=llsqladmin  AZSQL_PASSWORD=...
    PG_HOST=<server>.postgres.database.azure.com  PG_USER=llpgadmin  PG_PASSWORD=...
    ENGINE=polars|duckdb|spark  DB=azuresql|postgres  python db_scenarios.py
"""

from __future__ import annotations

import datetime as dt
import os
import shutil
import uuid
from pathlib import Path
from urllib.parse import quote_plus

import polars as pl

from lakelogic import DataProcessor

ENGINE = os.environ.get("ENGINE", "polars")
DB = os.environ.get("DB", "postgres")
WORK = Path(__file__).resolve().parent / "data" / "db_runs" / DB / ENGINE
RESULTS: list[dict] = []
T0 = dt.datetime(2026, 10, 1, 9, 0, 0)


# ── connections ────────────────────────────────────────────────────────────────


def uri() -> str:
    """The SQLAlchemy-style URI a contract's source.path carries."""
    if DB == "azuresql":
        u, p, h = os.environ["AZSQL_USER"], quote_plus(os.environ["AZSQL_PASSWORD"]), os.environ["AZSQL_HOST"]
        return f"mssql://{u}:{p}@{h}:1433/lakelogic_test?encrypt=true"
    u, p, h = os.environ["PG_USER"], quote_plus(os.environ["PG_PASSWORD"]), os.environ["PG_HOST"]
    return f"postgresql://{u}:{p}@{h}:5432/lakelogic_test?sslmode=require"


def connect():
    """A DB-API connection for seeding (not used by LakeLogic itself)."""
    if DB == "azuresql":
        import pyodbc

        return pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={os.environ['AZSQL_HOST']},1433;DATABASE=lakelogic_test;"
            f"UID={os.environ['AZSQL_USER']};PWD={os.environ['AZSQL_PASSWORD']};Encrypt=yes;",
            autocommit=True,
        )
    import psycopg2

    c = psycopg2.connect(
        host=os.environ["PG_HOST"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        dbname="lakelogic_test",
        sslmode="require",
    )
    c.autocommit = True
    return c


def ride(i: int, minutes: int = 0, **over) -> tuple:
    r = {
        "ride_id": i,
        "city": ["London", "Paris", "Lagos"][i % 3],
        "fare": float(5 + i % 40),
        "status": "completed",
        "updated_at": T0 + dt.timedelta(minutes=minutes),
    }
    r.update(over)
    return (r["ride_id"], r["city"], r["fare"], r["status"], r["updated_at"])


class Table:
    """A fresh `rides_<id>` table for one scenario, dropped afterwards."""

    def __init__(self):
        self.name = f"rides_{ENGINE}_{uuid.uuid4().hex[:6]}"
        self.dir = WORK / self.name
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        self.conn = connect()
        ts = "DATETIME2" if DB == "azuresql" else "TIMESTAMP"
        self.conn.cursor().execute(
            f"CREATE TABLE {self.name} (ride_id INT PRIMARY KEY, city VARCHAR(40), fare FLOAT, "
            f"status VARCHAR(20), updated_at {ts})"
        )

    def insert(self, rows):
        cur = self.conn.cursor()
        mark = "?" if DB == "azuresql" else "%s"
        cur.executemany(f"INSERT INTO {self.name} VALUES ({mark},{mark},{mark},{mark},{mark})", rows)

    def update_status(self, ids, status: str, minutes: int):
        cur = self.conn.cursor()
        mark = "?" if DB == "azuresql" else "%s"
        for i in ids:
            cur.execute(
                f"UPDATE {self.name} SET status={mark}, updated_at={mark} WHERE ride_id={mark}",
                (status, T0 + dt.timedelta(minutes=minutes), i),
            )

    def drop(self):
        try:
            self.conn.cursor().execute(f"DROP TABLE {self.name}")
        finally:
            self.conn.close()


def contract(t: Table, **source) -> dict:
    return {
        "version": "1.0.0",
        "dataset": t.name,
        "info": {"title": t.name},
        "source": {"type": "database", "path": uri(), **source},
        "model": {
            "fields": [
                {"name": "ride_id", "type": "long", "required": True},
                {"name": "city", "type": "string", "required": True},
                {"name": "fare", "type": "double", "required": True},
                {"name": "status", "type": "string"},
                {"name": "updated_at", "type": "timestamp"},
            ]
        },
        "quality": {"row_rules": [{"name": "fare_not_negative", "sql": "fare >= 0"}]},
        # The run log remembers the incremental watermark between runs (a local DuckDB file here).
        "metadata": {
            "run_log_table": "run_log",
            "run_log_backend": "duckdb",
            "run_log_database": str(t.dir / "run_log.duckdb"),
        },
    }


def run(c: dict):
    proc = DataProcessor(engine=ENGINE, contract=c)
    good, bad = proc.run_source()[:2]
    # The pipeline runner writes the run log after each run (it remembers the incremental
    # watermark); a standalone DataProcessor does not, so do what the runner does.
    from lakelogic.core.run_log import write_run_log

    if proc.last_report:
        write_run_log(proc.last_report, proc.contract, engine_name=ENGINE)
    return _as_polars(good), _as_polars(bad)


def _as_polars(d):
    """Results from any engine as Polars, so every engine is checked the same way."""
    if isinstance(d, pl.DataFrame):
        return d
    return (
        d.pl()
        if hasattr(d, "pl")
        else pl.from_arrow(d.toArrow())
        if hasattr(d, "toArrow")
        else pl.from_pandas(d.toPandas())
    )


def scenario(name, fn, **expect):
    t = None
    try:
        t = Table()
        got = fn(t)
        problems = [f"{k}: expected {v}, got {got.get(k)}" for k, v in expect.items() if got.get(k) != v]
        outcome = ", ".join(f"{k}={v}" for k, v in got.items())
    except Exception as exc:  # noqa: BLE001 - a scenario may surface a real failure
        problems, outcome = [f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"], "error"
    finally:
        if t is not None:
            t.drop()
    status = "PASS" if not problems else "FAIL"
    RESULTS.append(
        {"db": DB, "engine": ENGINE, "scenario": name, "status": status, "result": outcome, "why": "; ".join(problems)}
    )
    print(f"[{status}] {DB:<8} {ENGINE:<6} {name:<46} {outcome}" + (f"  <- {'; '.join(problems)}" if problems else ""))


# ── scenarios ────────────────────────────────────────────────────────────────

SEED = (
    [ride(i) for i in range(500)]
    + [ride(1000 + i, fare=-5.0) for i in range(20)]
    + [ride(2000 + i, city=None) for i in range(10)]
)


def full_load(t):
    t.insert(SEED)
    good, bad = run(contract(t))
    return {"good": good.height, "quarantined": bad.height, "fare_is_double": str(good.schema.get("fare")) == "Float64"}


def incremental(t):
    t.insert(SEED)
    c = contract(t, load_mode="incremental", watermark_field="updated_at")
    g1, b1 = run(c)
    t.insert([ride(3000 + i, minutes=60) for i in range(50)])
    t.update_status(range(10), "refunded", minutes=61)
    g2, b2 = run(c)
    g3, b3 = run(c)
    return {
        "first_run": g1.height + b1.height,
        "second_run": g2.height + b2.height,
        "refunds_seen": g2.filter(pl.col("status") == "refunded").height,
        "third_run": g3.height + b3.height,
    }


def custom_query(t):
    t.insert(SEED)
    good, bad = run(contract(t, query=f"SELECT * FROM {t.name} WHERE city = 'Paris'"))
    return {"good": good.height, "quarantined": bad.height, "only_paris": set(good["city"].to_list()) == {"Paris"}}


def chunked(t):
    t.insert(SEED)
    good, bad = run(contract(t, options={"fetch_size": 100}))
    return {"good": good.height, "quarantined": bad.height}


def partitioned(t):
    t.insert(SEED)
    good, bad = run(
        contract(
            t,
            options={
                "partition_column": "ride_id",
                "partition_num": 4,
                "partition_lower_bound": 0,
                "partition_upper_bound": 2100,
            },
        )
    )
    return {"good": good.height, "quarantined": bad.height}


def cdc_azuresql(t):
    """Native SQL Server CDC: inserts, updates AND deletes, read from the change log."""
    import time

    cur = t.conn.cursor()
    cur.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1) "
        "EXEC sys.sp_cdc_enable_db"
    )
    cur.execute(f"EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = '{t.name}', @role_name = NULL")
    t.insert([ride(i) for i in range(100)])
    c = contract(
        t,
        load_mode="cdc",
        watermark_field="updated_at",
        options={"cdc_provider": "azuresql", "cdc_capture_instance": f"dbo_{t.name}"},
    )

    def wait_for_capture(expected_min):
        # Azure SQL's capture job runs every ~20s; wait until the change table has caught up.
        for _ in range(30):
            n = cur.execute(f"SELECT COUNT(*) FROM cdc.dbo_{t.name}_CT").fetchone()[0]
            if n >= expected_min:
                return
            time.sleep(5)

    wait_for_capture(100)
    g1, b1 = run(c)
    t.update_status(range(10), "refunded", minutes=60)
    cur.execute(f"DELETE FROM {t.name} WHERE ride_id BETWEEN 50 AND 54")
    t.insert([ride(500 + i, minutes=61) for i in range(3)])
    wait_for_capture(100 + 20 + 5 + 3)  # updates log a before AND an after image
    g2, b2 = run(c)
    g3, b3 = run(c)
    ops = g2["_lakelogic_cdc_op"].value_counts().sort("_lakelogic_cdc_op").rows() if g2.height else []
    cur.execute(
        f"EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = '{t.name}', @capture_instance = 'all'"
    )
    return {
        "first_run": g1.height + b1.height,
        "second_run": g2.height + b2.height,
        "ops": dict(ops),
        "third_run": g3.height + b3.height,
    }


ALL = [
    (
        "full load: 500 good, 20 negative fares, 10 no city",
        full_load,
        dict(good=500, quarantined=30, fare_is_double=True),
    ),
    (
        "incremental by updated_at: 50 new + 10 updated",
        incremental,
        dict(first_run=530, second_run=60, refunds_seen=10, third_run=0),
    ),
    ("custom source.query (Paris only)", custom_query, dict(good=167, quarantined=7, only_paris=True)),
    ("chunked read (fetch_size 100)", chunked, dict(good=500, quarantined=30)),
    ("partitioned read (4 partitions on ride_id)", partitioned, dict(good=500, quarantined=30)),
]
CDC = [
    (
        "CDC: 100 inserts, then 10 updates + 5 deletes + 3 inserts",
        cdc_azuresql,
        dict(first_run=100, second_run=18, ops={"delete": 5, "insert": 3, "update": 10}, third_run=0),
    ),
]


def summary() -> pl.DataFrame:
    df = pl.DataFrame(RESULTS)
    print(f"\n{(df['status'] == 'PASS').sum()} of {len(df)} database scenarios behaved as expected ({DB}, {ENGINE}).")
    return df


if __name__ == "__main__":
    import sys

    from loguru import logger

    logger.remove()
    wanted = sys.argv[1:]
    for name, fn, expect in ALL + (CDC if DB == "azuresql" else []):
        if not wanted or any(w in name for w in wanted):
            scenario(name, fn, **expect)
    summary()
