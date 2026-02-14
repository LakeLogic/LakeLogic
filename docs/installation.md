# Installation Guide

LakeLogic is designed to be lightweight. Install only what you need and scale up anytime.

## 1. Using [uv](https://github.com/astral-sh/uv) (Recommended)

`uv` is the fastest way to install and manage LakeLogic.

```bash
# Install everything (recommended for testing)
uv pip install "lakelogic[all]"

# Or install only what you need
uv pip install "lakelogic[polars]"
uv pip install "lakelogic[spark]"
```

## 2. Using pip

If you prefer standard `pip`:

```bash
pip install "lakelogic[all]"
```

## Installation Options (Extras)

| Extra | What it includes | Use case |
| :--- | :--- | :--- |
| `[polars]` | Polars engine | High-speed local processing. |
| `[pandas]` | Pandas + DuckDB | For data science teams. |
| `[spark]` | PySpark | Large-scale Lakehouse jobs. |
| `[duckdb]` | DuckDB native | Fast analytical SQL in-memory. |
| `[snowflake]` | Snowflake connector | Run contracts directly in Snowflake (table-only). |
| `[bigquery]` | BigQuery client | Run contracts directly in BigQuery (table-only). |
| `[notebook]` | nbclient + nbformat | Run external notebook logic hooks. |
| `[profiling]` | DataProfiler + Presidio | Schema profiling + PII detection (bootstrap). |
| `[notifications]` | Secret manager clients (Azure/AWS/GCP/Vault) + cryptography | Enable optional notification secret providers. |
| `[all]` | **Every engine** | When you want total flexibility. |

## Engine Selection Tips

Selecting the right engine depends on your data source and operating system:

* **Polars (Default)**: Best for remote URLs (`https://`), local files (CSV/Parquet), and high-speed local processing.
* **Spark**: Best for large-scale Lakehouse jobs and distributed data. **Note**: Spark on Windows does not natively support reading directly from `https://` URLs for CSV/Parquet. Download remote files locally first if you must use Spark on Windows.
* **DuckDB**: Best for complex SQL analysis and native Iceberg/Delta support on local machines.

**Materialization notes:**

* **Spark engine**: Supports `append`, `overwrite`, `merge`, and `scd2` strategies natively. Uses distributed DataFrame operations for merge/SCD2, avoiding driver memory bottlenecks at scale. Delta Lake `MERGE INTO` is used when available.
* **DuckDB engine**: Full support for local and cloud data lakes. Supports `iceberg` and `delta` formats natively.
* **Polars engine**: Supports `delta` format natively via `deltalake`.
* **Pandas engine**: Full materialization support.

Install `[duckdb]` or `[polars]` for high-performance OSS processing. After installing, it is recommended to "warm" your environment for modern formats:

```bash

lakelogic setup-oss

```

This command pre-installs the necessary DuckDB extensions (Iceberg, Delta, Cloud Drivers) so they are available offline and during runtime.

---

## Developer Installation

If you want to contribute to LakeLogic:

1. **Clone the repo**:

   ```bash

   git clone https://github.com/LineageLogic/LakeLogic.git
   cd lakelogic

   ```

2. **Sync with uv**:

   ```bash

   uv sync

   ```

3. **Run tests**:

   ```bash

   uv run pytest

   ```

### Clean Developer Install (Recommended for Windows/Jupyter)

To install in editable mode while suppressing warnings about script paths and avoiding dependency conflicts (e.g., NumPy version mismatches with `pandas-ta`):

```bash

pip install -e . --no-warn-script-location --no-deps

```

## Requirements

* **Python**: 3.9 or higher.
* **OS**: Windows, macOS, or Linux.
