"""
Run the multi-frame external-logic example on each engine.

Proves LakeLogic hands the external step BOTH the source frame (trips) and a
linked reference frame (drivers) — and that the SAME enrich_trips.py runs on
Polars, DuckDB, and (opt-in) Spark.

    python examples/external_logic/run_demo.py            # polars + duckdb
    LAKELOGIC_DEMO_SPARK=1 python .../run_demo.py         # + spark (needs pyspark)

The `drivers` link declares filter: "status = 'active'", so the inactive driver
(d2) is loaded out — enriched trips for d2 get NULL driver_name (left join).
"""

import os
import tempfile
from pathlib import Path

import polars as pl
import yaml

from lakelogic.core.models import DataContract
from lakelogic.core.processor import DataProcessor

HERE = Path(__file__).resolve().parent


def _make_inputs(tmp: Path):
    # SOURCE: trips (3 drivers)
    trips = pl.DataFrame({"trip_id": ["t1", "t2", "t3"], "driver_id": ["d1", "d2", "d3"], "fare": [12.5, 20.0, 8.0]})
    # LINK: drivers — d2 is inactive, filtered out at load time
    pl.DataFrame(
        {
            "driver_id": ["d1", "d2", "d3"],
            "driver_name": ["Ana", "Bo", "Cy"],
            "driver_city": ["SF", "LA", "NYC"],
            "status": ["active", "inactive", "active"],
        }
    ).write_parquet(tmp / "drivers.parquet")
    return trips


def _load_contract(link_path: Path, engine: str) -> DataContract:
    doc = yaml.safe_load((HERE / "trips_enriched.olc.yaml").read_text())
    doc["external_logic"]["engine"] = engine
    c = DataContract(**doc)
    c._base_path = str(HERE)  # so enrich_trips.py resolves
    c.links[0].path = str(link_path)  # absolute link path for the demo
    return c


def _run(engine: str, trips, link_path: Path):
    contract = _load_contract(link_path, engine)
    if engine == "spark":
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[1]").appName("demo").config("spark.ui.enabled", "false").getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        df = spark.createDataFrame(trips.to_pandas())
    else:
        df = trips

    result = DataProcessor(contract, engine=engine).run(df)
    good = result.good_df if hasattr(result, "good_df") else result[0]
    rows = (
        good.toPandas().to_dict("records")
        if engine == "spark"
        else (good.to_dicts() if hasattr(good, "to_dicts") else good.to_pandas().to_dict("records"))
    )
    print(f"\n=== engine: {engine} - enriched trips (source JOIN active-drivers link) ===")
    for r in sorted(rows, key=lambda r: r["trip_id"]):
        print(
            f"  {r['trip_id']}  {r['driver_id']}  fare={r['fare']:<5}  name={r.get('driver_name')}  city={r.get('driver_city')}"
        )  # noqa: E501


def main():
    tmp = Path(tempfile.mkdtemp())
    trips = _make_inputs(tmp)
    link_path = tmp / "drivers.parquet"

    engines = ["polars", "duckdb"]
    if os.environ.get("LAKELOGIC_DEMO_SPARK") == "1":
        engines.append("spark")

    for eng in engines:
        _run(eng, trips, link_path)
    print("\nDone. d2 (inactive) has NULL name/city — filtered out of the link at load time.")


if __name__ == "__main__":
    main()
