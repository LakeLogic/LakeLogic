# Installation Guide 🚀

LakeGuard is designed to be lightweight. You can install only the engines you need, or get everything at once.

## 1. Using [uv](https://github.com/astral-sh/uv) (Recommended)
`uv` is the fastest way to install and manage LakeGuard.

```bash
# Install everything (recommended for testing)
uv pip install "lakeguard[all]"

# Or install only what you need
uv pip install "lakeguard[polars]"
uv pip install "lakeguard[spark]"
```

## 2. Using pip
If you prefer standard `pip`:

```bash
pip install "lakeguard[all]"
```

## Installation Options (Extras)

| Extra | What it includes | Use case |
| :--- | :--- | :--- |
| `[polars]` | Polars engine | High-speed local processing. |
| `[pandas]` | Pandas + DuckDB | For data science teams. |
| `[spark]` | PySpark | Large-scale Lakehouse jobs. |
| `[duckdb]` | DuckDB native | Fast analytical SQL in-memory. |
| `[docs]` | MkDocs + Plugins | Only if you are building this website. |
| `[all]` | **Every engine** | When you want total flexibility. |

---

## Developer Installation
If you want to contribute to LakeGuard:

1. **Clone the repo**:
   ```bash
   git clone https://github.com/your-username/lakeguard.git
   cd lakeguard
   ```

2. **Sync with uv**:
   ```bash
   uv sync
   ```

3. **Run tests**:
   ```bash
   uv run pytest
   ```

## Requirements
- **Python**: 3.9 or higher.
- **OS**: Windows, macOS, or Linux.
