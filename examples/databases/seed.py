"""Seeds a `rides` table (or MongoDB collection) for databases.ipynb. Not part of LakeLogic.

Each source gets the same data: 200 good rides, 10 with a negative fare, 5 with no city.
`changes()` then adds 20 new rides and refunds 5 existing ones, an hour later.
Connection strings come from the same environment variables the contracts name.
"""

from __future__ import annotations

import datetime as dt
import os
import sqlite3
from pathlib import Path
from urllib.parse import unquote, urlparse

T0 = dt.datetime(2026, 10, 1, 9, 0)


def _rows(ids, minutes=0, **over):
    out = []
    for i in ids:
        r = {"ride_id": i, "city": ["London", "Paris", "Lagos"][i % 3], "fare": float(5 + i % 40),
             "status": "completed", "updated_at": T0 + dt.timedelta(minutes=minutes)}
        r.update(over)
        out.append(r)
    return out


def _initial():
    return _rows(range(200)) + _rows(range(1000, 1010), fare=-1.0) + _rows(range(2000, 2005), city=None)


def _changes():
    return _rows(range(500, 520), minutes=60)


COLS = ("ride_id", "city", "fare", "status", "updated_at")


def _sql_conn(kind):
    if kind == "sqlite":
        Path("data").mkdir(exist_ok=True)
        return sqlite3.connect("data/rides.db"), "?", "TEXT"
    u = urlparse(os.environ["POSTGRES_URI" if kind == "postgres" else "AZURE_SQL_URI"])
    if kind == "postgres":
        import psycopg2

        c = psycopg2.connect(host=u.hostname, port=u.port or 5432, user=unquote(u.username),
                             password=unquote(u.password), dbname=u.path.lstrip("/"), sslmode="require")
        c.autocommit = True
        return c, "%s", "TIMESTAMP"
    import pyodbc

    c = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={u.hostname},{u.port or 1433};DATABASE={u.path.lstrip('/')};"
        f"UID={unquote(u.username)};PWD={unquote(u.password)};Encrypt=yes;",
        autocommit=True,
    )
    return c, "?", "DATETIME2"


def _value(kind, v):
    if kind == "sqlite" and isinstance(v, dt.datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return v


def seed(kind: str) -> None:
    """Drop and recreate `rides` with the initial 215 rows."""
    if kind == "mongodb":
        from pymongo import MongoClient

        with MongoClient(os.environ["MONGO_URI"]) as cl:
            coll = cl["lakelogic_test"]["rides"]
            coll.drop()
            docs = [{"ride_id": r["ride_id"], "customer": {"name": f"rider{r['ride_id']}", "tier": "gold"},
                     "fare": r["fare"], "status": r["status"], "updated_at": r["updated_at"],
                     "items": [{"sku": "A", "qty": 1}]} for r in _initial()]
            docs[-1].pop("customer")  # one document missing a required field
            coll.insert_many(docs + [{"ride_id": 3000, "customer": {"name": "x"}, "fare": "abc",
                                      "status": "completed", "updated_at": T0}])
        return
    con, mark, ts = _sql_conn(kind)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS rides")
    cur.execute(f"CREATE TABLE rides (ride_id INT PRIMARY KEY, city VARCHAR(40), fare FLOAT, "
                f"status VARCHAR(20), updated_at {ts})")
    cur.executemany(f"INSERT INTO rides VALUES ({','.join([mark] * 5)})",
                    [tuple(_value(kind, r[c]) for c in COLS) for r in _initial()])
    con.commit()
    con.close()


def changes(kind: str) -> None:
    """An hour later: 20 new rides and 5 refunds."""
    later = T0 + dt.timedelta(minutes=61)
    if kind == "mongodb":
        from pymongo import MongoClient

        with MongoClient(os.environ["MONGO_URI"]) as cl:
            coll = cl["lakelogic_test"]["rides"]
            coll.insert_many([{"ride_id": r["ride_id"], "customer": {"name": f"rider{r['ride_id']}"},
                               "fare": r["fare"], "status": "completed", "updated_at": r["updated_at"]}
                              for r in _changes()])
            coll.update_many({"ride_id": {"$lt": 5}}, {"$set": {"status": "refunded", "updated_at": later}})
        return
    con, mark, _ = _sql_conn(kind)
    cur = con.cursor()
    cur.executemany(f"INSERT INTO rides VALUES ({','.join([mark] * 5)})",
                    [tuple(_value(kind, r[c]) for c in COLS) for r in _changes()])
    cur.execute(f"UPDATE rides SET status = 'refunded', updated_at = {mark} WHERE ride_id < 5",
                (_value(kind, later),))
    con.commit()
    con.close()
