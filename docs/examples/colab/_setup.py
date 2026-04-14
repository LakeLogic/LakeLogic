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
import json
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
from lakelogic import DataProcessor, DataGenerator  # noqa: E402

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
    p.write_text(yaml_text.strip() + "\n")
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
                rule_name = f.get('rule', f.get('name', '?'))
                count = f.get('failed_count', f.get('count'))
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
    s = len(source_df)
    g = len(good_df)
    b = len(bad_df)
    print(f"source={s}  good={g}  bad={b}")
    print(f"{s} == {g} + {b} -> {s == g + b}")
    assert s == g + b, f"Reconciliation failed: {s} != {g} + {b}"

# ---------------------------------------------------------------------------
# 7. Announce
# ---------------------------------------------------------------------------
print(f"lakelogic v{VERSION} | {'Colab' if IN_COLAB else 'Local'} | {WORKDIR}")
