"""Prove the playbook recipe `ingest-database-to-landing` for real.

Seeds a small table in a real database, runs the recipe's contract (parameters filled in) with
LakeLogic Core, writes Parquet to a real ADLS Gen2 landing folder, reads it back, then adds rows
and runs again to show the incremental read takes only the new ones.

Environment (never printed):
  DB=postgres|azuresql
  SRC_DB_URL                    the source connection (the recipe's only secret)
  SEED_DB_URL                   the same database, for seeding (may equal SRC_DB_URL)
  AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_ACCOUNT_KEY
  LANDING_ROOT                  abfss://<fs>@<account>.dfs.core.windows.net/<prefix>
"""
from __future__ import annotations

import copy
import os
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import sqlalchemy as sa
import yaml

RECIPE = Path(os.environ.get("RECIPE_PATH", "../open-lakehouse-contract/recipes/ingest/database-to-landing.yaml"))
DB = os.environ.get("DB", "postgres")


def fill(obj, values):
    """The recipe's contract with every {parameter} replaced."""
    if isinstance(obj, dict):
        return {fill(k, values): fill(v, values) for k, v in obj.items()}
    if isinstance(obj, list):
        return [fill(v, values) for v in obj]
    if isinstance(obj, str):
        for k, v in values.items():
            obj = obj.replace("{" + k + "}", v)
    return obj


def seed(engine, table: str, start: int, n: int, at: datetime):
    with engine.begin() as con:
        if start == 0:
            con.execute(sa.text(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, city VARCHAR(40), fare FLOAT, updated_at {'DATETIME2' if DB == 'azuresql' else 'TIMESTAMP'})"))
        rows = [{"id": i, "city": ["LON", "PAR", "NYC"][i % 3], "fare": 10.0 + i, "updated_at": at.replace(tzinfo=None) + timedelta(seconds=i)}
                for i in range(start, start + n)]
        con.execute(sa.text(f"INSERT INTO {table} (id, city, fare, updated_at) VALUES (:id, :city, :fare, :updated_at)"), rows)


def run(contract: dict, workdir: Path):
    from lakelogic import DataProcessor
    from lakelogic.core.materialization import materialize_dataframe
    from lakelogic.core.run_log import write_run_log

    proc = DataProcessor(engine="polars", contract=contract)
    good, bad = proc.run_source()[:2]
    if proc.last_report:
        write_run_log(proc.last_report, proc.contract, engine_name="polars")
    materialize_dataframe(good, proc.contract, engine_name="polars")
    return good.height, (bad.height if bad is not None else 0)


def read_back(path: str) -> int:
    """Rows in every Parquet file under the landing folder, listed through the storage API."""
    import adlfs

    fs = adlfs.AzureBlobFileSystem(account_name=os.environ["AZURE_STORAGE_ACCOUNT"],
                                   account_key=os.environ["AZURE_STORAGE_ACCOUNT_KEY"])
    root = path.split("://", 1)[1].replace(f"@{os.environ['AZURE_STORAGE_ACCOUNT']}.dfs.core.windows.net", "")
    files = [f for f in fs.find(root) if f.endswith(".parquet")]
    print(f"landed files: {len(files)}")
    total = 0
    for f in files:
        with fs.open(f, "rb") as fh:
            total += pl.read_parquet(fh).height
    return total


def main() -> int:
    recipe = yaml.safe_load(RECIPE.read_text(encoding="utf-8"))
    table = f"playbook_orders_{uuid.uuid4().hex[:6]}"
    landing = f"{os.environ['LANDING_ROOT'].rstrip('/')}/playbook/{DB}/{table}"
    workdir = Path(tempfile.mkdtemp(prefix="playbook-"))
    contract = fill(copy.deepcopy(recipe["contract"]), {
        "source_table": table, "source_system": "playbook", "watermark_column": "updated_at",
        "landing_path": landing, "fetch_size": os.environ.get("FETCH_SIZE", "10000"),
    })
    contract["model"]["fields"] = [
        {"name": "id", "type": "long", "required": True},
        {"name": "city", "type": "string"},
        {"name": "fare", "type": "double"},
        {"name": "updated_at", "type": "timestamp"},
    ]
    contract["metadata"] = {"run_log_table": "run_log", "run_log_backend": "duckdb",
                            "run_log_database": str(workdir / "run_log.duckdb")}
    # Count the chunks Core reads, to prove fetch_size bounds every round trip.
    from loguru import logger
    chunks = []
    sink = logger.add(lambda m: chunks.append(1) if "Processing database chunk" in m else None, level="INFO")
    engine = sa.create_engine(os.environ.get("SEED_DB_URL") or os.environ["SRC_DB_URL"])
    t0 = datetime.now(timezone.utc) - timedelta(hours=2)
    try:
        seed(engine, table, 0, 50, t0)
        first = run(contract, workdir)
        first_chunks = len(chunks)
        seed(engine, table, 50, 7, datetime.now(timezone.utc) - timedelta(minutes=5))
        second = run(contract, workdir)
        second_chunks = len(chunks) - first_chunks
        logger.remove(sink)
        landed = read_back(landing)
    finally:
        with engine.begin() as con:
            con.execute(sa.text(f"DROP TABLE {table}"))
    print(f"{DB}: first run {first[0]} rows ({first[1]} quarantined); second run {second[0]} rows; "
          f"read back from landing {landed} rows; chunks read {first_chunks} then {second_chunks} "
          f"(fetch_size {contract['source']['options']['fetch_size']})")
    fetch = int(contract["source"]["options"]["fetch_size"])
    expect = (-(-50 // fetch), -(-7 // fetch))
    ok = first[0] == 50 and second[0] == 7 and landed == 57 and (first_chunks, second_chunks) == expect
    print("PROVEN" if ok else "NOT PROVEN")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
