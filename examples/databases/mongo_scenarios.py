"""MongoDB-protocol ingestion: MongoDB (Docker), and the same contract against Cosmos DB's MongoDB API.

Everything is declared in the CONTRACT — `source.type: database` with a `mongodb://` path read from an
environment variable (`env:MONGO_URI`) and `dataset` = the collection:

    docker run -d --name lakelogic-mongo-dev -p 27017:27017 mongo:7
    MONGO_URI=mongodb://localhost:27017 ENGINE=polars python mongo_scenarios.py
"""

from __future__ import annotations

import datetime as dt
import os
import shutil
import uuid
from pathlib import Path

import polars as pl
from pymongo import MongoClient

from lakelogic import DataProcessor
from lakelogic.core.run_log import write_run_log

ENGINE = os.environ.get("ENGINE", "polars")
WORK = Path(__file__).resolve().parent / "data" / "mongo_runs" / ENGINE
RESULTS: list[dict] = []
T0 = dt.datetime(2026, 10, 1, 9, 0)


def contract(coll: str, run_dir: Path, **source) -> dict:
    return {
        "version": "1.0.0", "dataset": coll, "info": {"title": coll},
        "source": {"type": "database", "path": "env:MONGO_URI", "flatten_nested": True,
                   "options": {"database": "lakelogic_test"}, **source},
        "model": {"fields": [
            {"name": "_id", "type": "string", "required": True},
            {"name": "ride_id", "type": "long", "required": True},
            {"name": "customer_name", "type": "string", "required": True},
            {"name": "fare", "type": "double", "required": True},
            {"name": "status", "type": "string"},
            {"name": "updated_at", "type": "timestamp"},
        ]},
        "quality": {"row_rules": [{"name": "fare_not_negative", "sql": "fare >= 0"}]},
        "metadata": {"run_log_table": "run_log", "run_log_backend": "duckdb",
                     "run_log_database": str(run_dir / "run_log.duckdb")},
    }


def doc(i: int, minutes: int = 0, **over) -> dict:
    d = {"ride_id": i, "customer": {"name": f"rider{i}", "tier": "gold"}, "fare": float(5 + i % 40),
         "status": "completed", "updated_at": T0 + dt.timedelta(minutes=minutes),
         "items": [{"sku": "A", "qty": 1}]}
    d.update(over)
    return d


class Coll:
    def __init__(self):
        self.name = f"rides_{ENGINE}_{uuid.uuid4().hex[:6]}"
        self.dir = WORK / self.name
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        self.client = MongoClient(os.environ["MONGO_URI"])
        self.coll = self.client["lakelogic_test"][self.name]

    def drop(self):
        self.coll.drop()
        self.client.close()


def run(c):
    proc = DataProcessor(engine=ENGINE, contract=c)
    good, bad = proc.run_source()[:2]
    if proc.last_report:
        write_run_log(proc.last_report, proc.contract, engine_name=ENGINE)  # what the pipeline runner does

    def as_pl(d):
        if isinstance(d, pl.DataFrame):
            return d
        return d.pl() if hasattr(d, "pl") else pl.from_pandas(d.toPandas())

    return as_pl(good), as_pl(bad)


def scenario(name, fn, **expect):
    c = None
    try:
        c = Coll()
        got = fn(c)
        problems = [f"{k}: expected {v}, got {got.get(k)}" for k, v in expect.items() if got.get(k) != v]
        outcome = ", ".join(f"{k}={v}" for k, v in got.items())
    except Exception as exc:  # noqa: BLE001
        problems, outcome = [f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"], "error"
    finally:
        if c is not None:
            c.drop()
    status = "PASS" if not problems else "FAIL"
    RESULTS.append({"engine": ENGINE, "scenario": name, "status": status, "result": outcome, "why": "; ".join(problems)})
    print(f"[{status}] {ENGINE:<6} {name:<52} {outcome}" + (f"  <- {'; '.join(problems)}" if problems else ""))


def full_load(c):
    c.coll.insert_many([doc(i) for i in range(200)] + [doc(1000 + i, fare=-1.0) for i in range(5)]
                       + [doc(2000 + i, fare="abc") for i in range(3)] + [{"ride_id": 3000, "fare": 2.0}])
    good, bad = run(contract(c.name, c.dir))
    return {"good": good.height, "quarantined": bad.height,
            "nested_flattened": "customer_name" in good.columns and "customer_tier" in good.columns}


def incremental(c):
    c.coll.insert_many([doc(i) for i in range(100)])
    k = contract(c.name, c.dir, load_mode="incremental", watermark_field="updated_at")
    g1, _ = run(k)
    c.coll.insert_many([doc(500 + i, minutes=60) for i in range(20)])
    c.coll.update_many({"ride_id": {"$lt": 5}}, {"$set": {"status": "refunded", "updated_at": T0 + dt.timedelta(minutes=61)}})
    g2, _ = run(k)
    g3, b3 = run(k)
    return {"first_run": g1.height, "second_run": g2.height, "third_run": g3.height + b3.height}


def server_filter(c):
    c.coll.insert_many([doc(i, status="cancelled" if i % 4 == 0 else "completed") for i in range(100)])
    good, _ = run(contract(c.name, c.dir, options={"database": "lakelogic_test", "filter": {"status": "completed"}}))
    return {"good": good.height, "only_completed": set(good["status"].to_list()) == {"completed"}}


ALL = [
    ("full load: nested docs, bad fares, a wrong type, a missing field", full_load,
     dict(good=200, quarantined=9, nested_flattened=True)),
    ("incremental by updated_at: 20 new + 5 updated", incremental, dict(first_run=100, second_run=25, third_run=0)),
    ("server-side filter (status = completed)", server_filter, dict(good=75, only_completed=True)),
]

if __name__ == "__main__":
    from loguru import logger

    logger.remove()
    for name, fn, expect in ALL:
        scenario(name, fn, **expect)
    df = pl.DataFrame(RESULTS)
    print(f"\n{(df['status'] == 'PASS').sum()} of {len(df)} MongoDB scenarios behaved as expected ({ENGINE}).")
