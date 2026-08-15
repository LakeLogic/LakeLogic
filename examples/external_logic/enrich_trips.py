"""
Concrete external-logic step (follows examples/external_logic/_template.py).

Receives the validated `trips` SOURCE frame plus the `drivers` REFERENCE frame
(a link), left-joins them, and RETURNS the enriched frame — LakeLogic then runs
quality gates and materializes it. Engine-agnostic: the same script runs whether
LakeLogic hands us a Polars, DuckDB (Polars-backed), or Spark frame.
"""
from typing import Any, Dict, Optional


def _kind(df: Any) -> str:
    mod = type(df).__module__
    return "spark" if mod.startswith("pyspark") else "polars" if mod.startswith("polars") else "pandas"


def run(
    good_df: Any,
    links: Optional[Dict[str, Any]] = None,
    engine: str = "polars",
    contract: Any = None,
    **kwargs: Any,
) -> Any:
    links = links or {}
    drivers = links.get("drivers")
    if drivers is None:
        raise ValueError("external_logic expected a 'drivers' link but received none.")

    kind = _kind(good_df)
    if kind == "spark":
        enriched = good_df.join(drivers, on="driver_id", how="left")
    elif kind == "polars":
        import polars as pl

        s = good_df.collect() if isinstance(good_df, pl.LazyFrame) else good_df
        d = drivers.collect() if isinstance(drivers, pl.LazyFrame) else drivers
        enriched = s.join(d, on="driver_id", how="left")
    else:  # pandas
        enriched = good_df.merge(drivers, on="driver_id", how="left")

    # Local guard (contract-level rules also run on the returned frame).
    n = enriched.count() if kind == "spark" else len(enriched)
    if n == 0:
        raise ValueError("enrich_trips produced 0 rows.")
    return enriched
