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
    """Convert Spark, Pandas, or DuckDB DataFrames into Polars for Colab analysis."""
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
# 8. Announce
# ---------------------------------------------------------------------------
print(f"lakelogic v{VERSION} | {'Colab' if IN_COLAB else 'Local'} | {WORKDIR}")
