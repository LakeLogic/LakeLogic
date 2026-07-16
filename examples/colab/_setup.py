"""
Shared setup for all LakeLogic Colab notebooks.

Usage (first cell of every notebook):
    # %pip install -q "lakelogic[polars]"
    import subprocess, sys, importlib
    if importlib.util.find_spec("lakelogic") is None:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "lakelogic[polars]"])

    import urllib.request, os, sys
    _setup_url = "https://raw.githubusercontent.com/LakeLogic/LakeLogic/main/examples/colab/_setup.py"
    _setup_path = "_setup.py"
    if not os.path.exists(_setup_path):
        urllib.request.urlretrieve(_setup_url, _setup_path)
    from _setup import *
"""

import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# 1. Detect environment
# ---------------------------------------------------------------------------
IN_COLAB = "google.colab" in sys.modules
IN_JUPYTER = "ipykernel" in sys.modules

# ---------------------------------------------------------------------------
# 2. Resolve working directory
# ---------------------------------------------------------------------------
if IN_COLAB:
    WORKDIR = Path("/content/lakelogic_demo")
    WORKDIR.mkdir(exist_ok=True)
    os.chdir(WORKDIR)
else:
    WORKDIR = Path.cwd()

# ---------------------------------------------------------------------------
# 3. Imports used by every notebook
# ---------------------------------------------------------------------------
import lakelogic  # noqa: E402
from lakelogic import DataProcessor  # noqa: E402

# Configure dataframe displays so columns and string values aren't truncated
try:
    import polars as pl

    pl.Config.set_fmt_str_lengths(200)
    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_cols(50)
except ImportError:
    pass

try:
    import pandas as pd

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", None)
except ImportError:
    pass

VERSION = lakelogic.__version__


# ---------------------------------------------------------------------------
# 4. Helper: write an inline YAML contract to disk and return its path
# ---------------------------------------------------------------------------
def write_contract(yaml_text: str, filename: str = "contract.yaml") -> str:
    """Write a YAML string to *filename* in WORKDIR and return the path."""
    p = WORKDIR / filename if IN_COLAB else Path(filename)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml_text.strip() + "\n", encoding="utf-8")
    return str(p)


# ---------------------------------------------------------------------------
# 5. Helper: pretty-print a run report
# ---------------------------------------------------------------------------
def print_report(processor: DataProcessor) -> dict:
    """Print a concise run report and return the raw dict."""
    r = processor.last_report
    if not r:
        print("No report available.")
        return {}
    c = r.get("counts", {})
    print(f"Run ID      : {r.get('run_id', '?')}")
    print(f"Timestamp   : {r.get('timestamp', '?')}")
    print(f"Source      : {c.get('source', '?')}")
    print(f"Good        : {c.get('good', '?')}")
    print(f"Quarantined : {c.get('quarantined', '?')}")
    failures = r.get("row_rule_failures", [])
    if failures:
        print("\nRule failures:")
        for f in failures:
            if isinstance(f, dict):
                rule_name = f.get("rule", f.get("name", "?"))
                count = f.get("failed_count", f.get("count"))
                if count is not None:
                    print(f"  {rule_name}: {count} rows")
                else:
                    print(f"  {rule_name}")
            else:
                print(f"  {f}")
    return r


# ---------------------------------------------------------------------------
# 6. Helper: reconciliation assertion
# ---------------------------------------------------------------------------
def assert_reconciliation(source_df, good_df, bad_df):
    """Print and assert that source == good + bad."""

    def _count(df):
        if hasattr(df, "shape"):
            return df.shape[0]
        if hasattr(df, "count") and callable(df.count):
            try:
                return df.count()
            except Exception:
                pass
        return len(df)

    s = _count(source_df)
    g = _count(good_df)
    b = _count(bad_df)
    print(f"source={s}  good={g}  bad={b}")
    print(f"{s} == {g} + {b} -> {s == g + b}")
    assert s == g + b, f"Reconciliation failed: {s} != {g} + {b}"


# ---------------------------------------------------------------------------
# 7. Helper: Convert engine dataframes to Polars for unified notebook analysis
# ---------------------------------------------------------------------------
def to_polars(df):
    """Convert Spark, Pandas, or DuckDB DataFrames into Polars for Colab analysis.

    Retained for backward compatibility; prefer the engine-neutral ``preview`` /
    ``row_count`` helpers below so notebook bodies stay engine-agnostic.
    """
    if df is None:
        return None
    import polars as pl

    if hasattr(df, "toPandas"):
        # PySpark to Pandas to Polars
        return pl.DataFrame(df.toPandas())
    if hasattr(df, "to_pandas") and type(df).__name__ != "DataFrame":
        # It might be DuckDB relation or something else
        return pl.DataFrame(df.to_pandas())
    if type(df).__name__ == "DataFrame" and "pandas" in str(type(df)):
        return pl.DataFrame(df)
    return df


# ---------------------------------------------------------------------------
# 7b. Engine-neutral helpers — keep notebook bodies free of any single engine
# ---------------------------------------------------------------------------
# LakeLogic's whole premise is that the contract is decoupled from the compute
# engine. These helpers let the notebooks build inputs and display results
# without importing polars / pandas / spark directly, so the demo code reads as
# "LakeLogic on ENGINE", never "LakeLogic wrapping polars".

def to_frame(records, engine: str = "duckdb"):
    """Build an engine-native dataframe from a list of dicts for the given ENGINE.

    Used to buffer streaming events (or any in-memory records) into a frame that
    ``DataProcessor.run()`` accepts, without hardcoding a single engine in the
    notebook body.
    """
    eng = (engine or "duckdb").lower()
    if eng == "spark":
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            return spark.createDataFrame(records)
        except Exception:
            pass  # fall through to a local frame if Spark isn't available
    # Polars and DuckDB adapters both accept a Polars frame natively
    # (DuckDB registers it via Arrow), so one path serves both.
    import polars as pl

    return pl.DataFrame(records)


def row_count(df) -> int:
    """Engine-agnostic row count for Polars / Pandas / DuckDB / Spark frames."""
    if df is None:
        return 0
    if hasattr(df, "height"):  # polars
        return int(df.height)
    if hasattr(df, "shape") and not callable(getattr(df, "shape")):  # pandas
        return int(df.shape[0])
    if hasattr(df, "count") and callable(df.count):  # spark / duckdb relation
        try:
            return int(df.count())
        except Exception:
            pass
    try:
        return len(df)
    except Exception:
        return 0


def read_table(path):
    """Read a materialized Delta table (or parquet dir/file) for inspection.

    Engine-neutral from the notebook's point of view — it just hands you back a
    frame you can pass to ``preview`` / ``to_records``. (Uses Polars internally as
    the reader; that's an implementation detail, not something the demo asserts.)
    """
    import polars as _pl
    from pathlib import Path as _P

    try:
        return _pl.read_delta(str(path))
    except Exception:
        pass
    p = _P(path)
    files = [str(x) for x in p.rglob("*.parquet")] if p.is_dir() else [str(p)]
    if files:
        return _pl.read_parquet(files)
    raise FileNotFoundError(f"No Delta or parquet data found at {path}")


def to_records(df, n=None):
    """Return rows as a list of plain dicts — the universal, engine-free shape.

    Handy when a demo needs to pick a value, filter, or list column names without
    reaching for a specific dataframe API. Pass ``n`` to cap the rows returned.
    """
    if df is None:
        return []
    d = df
    if n is not None:
        if hasattr(d, "limit"):  # spark / duckdb relation
            try:
                d = d.limit(n)
            except Exception:
                pass
        elif hasattr(d, "head"):  # polars / pandas
            try:
                d = d.head(n)
            except Exception:
                pass
    if hasattr(d, "to_dicts"):  # polars
        recs = d.to_dicts()
    elif hasattr(d, "toPandas"):  # spark
        recs = d.toPandas().to_dict("records")
    elif hasattr(d, "to_dict") and not hasattr(d, "to_dicts"):  # pandas
        try:
            recs = d.to_dict("records")
        except Exception:
            recs = list(d)
    elif hasattr(d, "to_pandas"):  # duckdb relation / arrow
        recs = d.to_pandas().to_dict("records")
    else:
        recs = list(d)
    return recs[:n] if n is not None else recs


def preview(df, n: int = 5, columns=None):
    """Return a display-friendly view of any engine frame.

    Renders cleanly in Colab/Jupyter regardless of which engine produced ``df``,
    without the notebook body ever importing a dataframe library. Pandas gives the
    nicest table if it's available; otherwise the native (Polars/DuckDB/Spark)
    frame is returned — notebooks render those too.
    """
    if df is None:
        return None

    # 1. Optional column projection + row limit, expressed engine-neutrally.
    sliced = df
    if columns is not None and hasattr(sliced, "select"):
        try:
            sliced = sliced.select(columns)
        except Exception:
            sliced = df
    if hasattr(sliced, "limit"):  # spark / duckdb relation
        try:
            sliced = sliced.limit(n)
        except Exception:
            pass
    elif hasattr(sliced, "head"):  # polars / pandas
        try:
            sliced = sliced.head(n)
        except Exception:
            pass

    # 2. Prefer pandas for display, but fall back to the native frame if pandas
    #    (or the arrow->pandas bridge) isn't installed.
    try:
        if hasattr(sliced, "toPandas"):  # spark
            return sliced.toPandas()
        if hasattr(sliced, "to_pandas"):  # polars / duckdb relation / arrow
            return sliced.to_pandas()
    except Exception:
        pass
    return sliced


# ---------------------------------------------------------------------------
# 8. Announce
# ---------------------------------------------------------------------------
print(f"lakelogic v{VERSION} | {'Colab' if IN_COLAB else 'Local'} | {WORKDIR}")
