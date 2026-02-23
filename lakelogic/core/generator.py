"""
lakelogic.core.generator
------------------------
Contract-aware synthetic data generator.

Reads a DataContract and generates rows that respect field types, nullability,
accepted_values, and range constraints. Optionally injects a controlled ratio of
invalid records so you can stress-test quarantine logic without touching real data.

Python API
----------
    from lakelogic import DataGenerator

    # Pure synthetic generation from a contract
    gen = DataGenerator("contracts/orders.yaml")
    df  = gen.generate(rows=1_000, invalid_ratio=0.05)          # 5% bad rows
    gen.save(df, "sample_orders.parquet", format="parquet")

    # Contract-free: infer schema AND seed from an existing file — no YAML required
    gen = DataGenerator.from_file("data/zoopla_sample.csv")     # schema inferred
    df  = gen.generate(rows=5_000)                              # values mirror the file
    df  = gen.generate(rows=500, invalid_ratio=0.1)             # 10% bad rows

    # Contract + file: use schema from contract, distributions from file
    gen = DataGenerator("contracts/orders.yaml")
    df  = gen.generate_from_sample("data/orders_sample.csv", rows=5_000)

CLI
---
    lakelogic generate --contract contracts/orders.yaml --rows 1000 \\
                       --invalid-ratio 0.05 --format parquet --output sample.parquet
"""

from __future__ import annotations

import random
import re
import string

# Module-level alias used in _build_field_rules SQL parsing helpers
_re = re

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

# ---------------------------------------------------------------------------
# Contract type → Polars dtype mapping
# ---------------------------------------------------------------------------

def _polars_dtype(ftype: str):
    """Return the polars DataType for a contract field type string."""
    try:
        import polars as pl
    except ImportError:
        return None

    _MAP = {
        "string":   pl.Utf8,
        "str":      pl.Utf8,
        "text":     pl.Utf8,
        "varchar":  pl.Utf8,
        "integer":  pl.Int64,
        "int":      pl.Int64,
        "int32":    pl.Int32,
        "int64":    pl.Int64,
        "long":     pl.Int64,
        "double":   pl.Float64,
        "float":    pl.Float64,
        "float32":  pl.Float32,
        "float64":  pl.Float64,
        "decimal":  pl.Float64,
        "number":   pl.Float64,
        "boolean":  pl.Boolean,
        "bool":     pl.Boolean,
        "date":     pl.Utf8,       # stored as ISO string; processor parses
        "timestamp":pl.Utf8,
        "datetime": pl.Utf8,
    }
    return _MAP.get(ftype.lower().split("(")[0].strip())

# ---------------------------------------------------------------------------
# Optional deps — only fail at call time, not at import time
# ---------------------------------------------------------------------------

def _try_faker():
    try:
        from faker import Faker
        return Faker()
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Semantic hints — map common field names to Faker generators
# ---------------------------------------------------------------------------

_SEMANTIC_HINTS: Dict[str, str] = {
    "email": "email",
    "name": "name",
    "first_name": "first_name",
    "last_name": "last_name",
    "phone": "phone_number",
    "address": "address",
    "city": "city",
    # NOTE: 'country' intentionally omitted — Faker returns full names ('United Kingdom')
    # but most dbt accepted_values tests list ISO codes ('GB').  When an accepted_values
    # constraint is present, _make_valid_value picks from it first (before Faker), so
    # removing the hint here means the no-constraint path also uses ISO-safe random strings.
    "postcode": "postcode",
    "zip": "zipcode",
    "company": "company",
    "url": "url",
    "ip": "ipv4",
    "uuid": "uuid4",
    "description": "sentence",
    "notes": "text",
    "comment": "sentence",
}


class DataGenerator:
    """
    Generate synthetic data from a LakeLogic contract YAML.

    Parameters
    ----------
    contract_path : str | Path
        Path to the contract YAML file.
    seed : int, optional
        Random seed for reproducibility.
    use_faker : bool
        If True (default) and Faker is installed, use semantic generation
        for string fields with recognisable names (email, name, …).
    """

    def __init__(
        self,
        contract_path: str | Path,
        seed: Optional[int] = None,
        use_faker: bool = True,
    ) -> None:
        self.contract_path = Path(contract_path)
        self.seed = seed
        self._rng = random.Random(seed)
        self._faker = _try_faker() if use_faker else None
        if self._faker and seed is not None:
            self._faker.seed_instance(seed)
        self._contract_raw: Dict[str, Any] = self._load_yaml()
        self._fields: List[Dict[str, Any]] = self._extract_fields()
        self._quality: Dict[str, Any] = self._contract_raw.get("quality", {}) or {}
        # Fields that are integer + flagged unique in quality rules — these need
        # special treatment in generate_from_sample (single-record seed files).
        self._unique_integer_fields: set = self._extract_unique_integer_fields()
        # Populated by from_file(); generate() uses these automatically
        self._auto_sample_pools: Optional[Dict[str, List[Any]]] = None


    @classmethod
    def from_file(
        cls,
        source,
        *,
        seed: Optional[int] = None,
        use_faker: bool = True,
    ) -> "DataGenerator":
        """
        Create a ``DataGenerator`` directly from an existing data file — **no contract needed**.

        The schema (column names and types) is inferred from the file itself and a
        temporary contract is built in memory.  The file's actual values become the
        sampling pool, so ``generate()`` produces rows that mirror the source
        distribution without repeating the file path a second time.

        Parameters
        ----------
        source : str | Path | polars.DataFrame | pandas.DataFrame
            Seed file or in-memory DataFrame.  File formats auto-detected from extension:
            ``.csv``, ``.parquet``, ``.json``, ``.ndjson`` / ``.jsonl``, ``.xlsx`` / ``.xls``.
        seed : int, optional
            Random seed for reproducibility.
        use_faker : bool
            If True and Faker is installed, apply semantic generation for string
            fields that have no observed values (all-null columns).

        Returns
        -------
        DataGenerator
            Instance backed by the inferred schema.  Call ``.generate(rows=N)``
            directly — values are sampled from the source file automatically.

        Examples
        --------
        ::

            from lakelogic import DataGenerator

            # No contract YAML required
            gen = DataGenerator.from_file("data/zoopla_sample.csv")
            df  = gen.generate(rows=5_000)

            # With reproducibility seed and bad-row injection
            gen = DataGenerator.from_file("data/zoopla_sample.csv", seed=42)
            df  = gen.generate(rows=500, invalid_ratio=0.1)

            # From an in-memory DataFrame
            import polars as pl
            seed_df = pl.read_parquet("data/seed.parquet")
            gen = DataGenerator.from_file(seed_df)
            df  = gen.generate(rows=1_000)
        """
        import tempfile
        import polars as pl

        # ── 1. Load source into a polars DataFrame ────────────────────────────
        if isinstance(source, pl.DataFrame):
            df = source
        elif hasattr(source, "to_dict"):  # pandas DataFrame
            df = pl.from_pandas(source)
        else:
            p = Path(source)
            ext = p.suffix.lower()
            if ext == ".csv":
                df = pl.read_csv(p, infer_schema_length=10_000)
            elif ext == ".parquet":
                df = pl.read_parquet(p)
            elif ext == ".json":
                df = pl.read_json(p)
            elif ext in (".ndjson", ".jsonl"):
                df = pl.read_ndjson(p)
            elif ext in (".xlsx", ".xls"):
                try:
                    import pandas as pd
                    df = pl.from_pandas(pd.read_excel(p))
                except ImportError as exc:
                    raise ImportError(
                        "Excel support requires pandas and openpyxl: "
                        "pip install pandas openpyxl"
                    ) from exc
            else:
                raise ValueError(
                    f"Cannot infer file format from extension {ext!r}. "
                    "Supported: .csv, .parquet, .json, .ndjson, .jsonl, .xlsx, .xls"
                )

        # ── 2. Map polars dtypes → contract type strings ──────────────────────
        _DTYPE_MAP = {
            pl.Utf8: "string",
            pl.String: "string",
            pl.Int8: "integer",
            pl.Int16: "integer",
            pl.Int32: "integer",
            pl.Int64: "integer",
            pl.UInt8: "integer",
            pl.UInt16: "integer",
            pl.UInt32: "integer",
            pl.UInt64: "integer",
            pl.Float32: "double",
            pl.Float64: "double",
            pl.Boolean: "boolean",
            pl.Date: "date",
            pl.Datetime: "timestamp",
        }

        fields = []
        for col_name in df.columns:
            col_dtype = df[col_name].dtype
            # Match by type class (handles temporal subtypes like Datetime(tu, tz))
            ctype = "string"  # safe default
            for dtype_cls, ctype_str in _DTYPE_MAP.items():
                if isinstance(col_dtype, type(dtype_cls)):
                    ctype = ctype_str
                    break
            fields.append({"name": col_name, "type": ctype})

        # ── 3. Build a minimal contract dict and write to a temp YAML ─────────
        contract_data = {
            "info": {
                "title": "_inferred_from_file",
                "version": "0.0.0",
                "description": "Auto-generated contract inferred from source file.",
            },
            "model": {"fields": fields},
            "quality": {},
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as tmp:
            yaml.dump(contract_data, tmp, sort_keys=False, allow_unicode=True)
            tmp_path = tmp.name

        # ── 4. Instantiate and attach auto-pools ──────────────────────────────
        instance = cls(tmp_path, seed=seed, use_faker=use_faker)

        # Build value pools from non-null unique values in each column
        pools: Dict[str, List[Any]] = {}
        for col_name in df.columns:
            series = df[col_name].drop_nulls()
            if not series.is_empty():
                pools[col_name] = series.unique().to_list()

        instance._auto_sample_pools = pools
        return instance

    @classmethod
    def from_dbt(
        cls,
        schema_path,
        *,
        model: Optional[str] = None,
        source_name: Optional[str] = None,
        source_table: Optional[str] = None,
        seed: Optional[int] = None,
        use_faker: bool = True,
    ) -> "DataGenerator":
        """
        Create a ``DataGenerator`` from a dbt ``schema.yml`` / ``sources.yml``.

        Converts the dbt model/source to a temporary LakeLogic contract and
        returns a generator backed by that schema.  All ``generate()`` and
        ``save()`` methods work identically.

        Parameters
        ----------
        schema_path
            Path to the dbt schema YAML file.
        model
            dbt model name.  May be omitted when the file has exactly one model.
        source_name / source_table
            dbt source identifiers (for ``sources.yml`` files).
        seed
            Random seed for reproducibility.
        use_faker
            If True and Faker is installed, use semantic generation for
            string fields with recognisable names (email, name, …).

        Examples
        --------
        >>> gen = DataGenerator.from_dbt("models/schema.yml", model="customers")
        >>> df  = gen.generate(rows=500, invalid_ratio=0.05)
        """
        import re
        import tempfile
        import yaml

        from lakelogic.adapters.dbt import load_contract_from_dbt
        contract = load_contract_from_dbt(
            schema_path,
            model=model,
            source_name=source_name,
            source_table=source_table,
        )

        # Serialise contract to a mutable dict
        data = contract.model_dump(exclude_none=True, by_alias=True)

        # ── Back-fill field-level hints from SQL row rules ─────────────────
        # The generator's _build_field_rules() only understands structured
        # rule dicts (accepted_values / range).  The dbt adapter emits plain
        # QualityRule SQL strings, so we parse them back here and inject the
        # values directly on each field definition.
        row_rules = (data.get("quality") or {}).get("row_rules") or []
        fields_by_name: Dict[str, Dict] = {}
        for f in (data.get("model") or {}).get("fields") or []:
            fields_by_name[f.get("name", "")] = f

        for rule in row_rules:
            if not isinstance(rule, dict):
                continue
            sql = rule.get("sql", "")
            if not sql:
                continue

            # Pattern: <col> IN ('a', 'b', 'c') or <col> IN (1, 2, 3)
            m_in = re.match(
                r"^\s*(\w+)\s+IN\s*\((.+)\)\s*$", sql, re.IGNORECASE | re.DOTALL
            )
            if m_in:
                col   = m_in.group(1)
                inner = m_in.group(2)
                # Parse quoted strings or bare numbers
                values = re.findall(r"'([^']*)'|\b(-?\d+(?:\.\d+)?)\b", inner)
                parsed = [s or n for s, n in values]
                if parsed and col in fields_by_name:
                    # Inject as structured accepted_values so _build_field_rules picks it up
                    field_def = fields_by_name[col]
                    field_def.setdefault("accepted_values", parsed)
                continue

            # Pattern: <col> >= <number>   (min constraint from expression_is_true)
            m_gte = re.match(r"^\s*(\w+)\s*>=\s*(-?\d+(?:\.\d+)?)\s*$", sql)
            if m_gte:
                col, val = m_gte.group(1), float(m_gte.group(2))
                if col in fields_by_name:
                    fields_by_name[col].setdefault("min", val)
                continue

            # Pattern: <col> > <number>
            m_gt = re.match(r"^\s*(\w+)\s*>\s*(-?\d+(?:\.\d+)?)\s*$", sql)
            if m_gt:
                col, val = m_gt.group(1), float(m_gt.group(2))
                if col in fields_by_name:
                    fields_by_name[col].setdefault("min", val + 0.00001)
                continue

            # Pattern: <col> <= <number>   (max constraint)
            m_lte = re.match(r"^\s*(\w+)\s*<=\s*(-?\d+(?:\.\d+)?)\s*$", sql)
            if m_lte:
                col, val = m_lte.group(1), float(m_lte.group(2))
                if col in fields_by_name:
                    fields_by_name[col].setdefault("max", val)
                continue

            # Pattern: <col> LIKE '%@%'  (email format) — mark the field name so
            # Faker's semantic hint picks it up (already does via field name "email")
            # No extra action needed.

        # Write enriched contract to temp YAML
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as tmp:
            yaml.dump(data, tmp, sort_keys=False, allow_unicode=True)
            tmp_path = tmp.name

        return cls(tmp_path, seed=seed, use_faker=use_faker)



    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        rows: int = 100,
        invalid_ratio: float = 0.0,
        output_format: str = "polars",
        reference_data: Optional[Dict[str, List[Any]]] = None,
    ):
        """
        Generate ``rows`` synthetic rows from the contract schema.

        Parameters
        ----------
        rows : int
            Total number of rows to generate.
        invalid_ratio : float
            Fraction of rows (0.0–1.0) that intentionally break quality rules.
            Useful for verifying quarantine logic.  e.g. ``0.1`` = 10% bad rows.
        output_format : str
            ``"polars"``   → returns a ``polars.DataFrame``
            ``"pandas"``   → returns a ``pandas.DataFrame``
        reference_data : dict, optional
            FK-aware generation pool.  Maps FK column names to a list (or
            polars/pandas Series) of valid PK values drawn from the reference
            table.  The generator samples from this pool for FK columns instead
            of generating random values.

            If a field declares ``foreign_key`` in the contract but the column
            is NOT present in ``reference_data``, the generator falls back to a
            synthetic pool of N distinct surrogate integers — useful in CI
            where no live reference table is available.

            Example::

                agents_df = DataGenerator("silver_agents.yaml").generate(rows=20)
                orders_df = DataGenerator("gold_orders.yaml").generate(
                    rows=200,
                    reference_data={"agent_id": agents_df["agent_id"].to_list()},
                )
                # Every orders_df["agent_id"] value exists in agents_df["agent_id"]

        Returns
        -------
        DataFrame (Polars or Pandas depending on output_format)
        """
        # Normalise reference_data values to plain Python lists
        fk_pools: Dict[str, List[Any]] = {}
        if reference_data:
            for col, pool in reference_data.items():
                if hasattr(pool, "to_list"):   # polars / pandas Series
                    fk_pools[col] = pool.to_list()
                elif hasattr(pool, "tolist"):  # numpy array
                    fk_pools[col] = pool.tolist()
                else:
                    fk_pools[col] = list(pool)

        n_invalid = int(rows * invalid_ratio)
        n_valid   = rows - n_invalid

        # from_file() stores auto-pools; pass them so generate() mirrors the source file
        # without the caller needing to specify the file path a second time.
        auto_pools = getattr(self, "_auto_sample_pools", None) or None

        valid_records   = [self._make_row(invalid=False, fk_pools=fk_pools, sample_pools=auto_pools) for _ in range(n_valid)]
        invalid_records = [self._make_row(invalid=True,  fk_pools=fk_pools, sample_pools=auto_pools) for _ in range(n_invalid)]

        # Shuffle so bad rows aren't all at the end
        all_records = valid_records + invalid_records
        self._rng.shuffle(all_records)

        return self._to_frame(all_records, output_format)


    def generate_from_sample(
        self,
        source,
        rows: int = 100,
        invalid_ratio: float = 0.0,
        output_format: str = "polars",
        columns: Optional[List[str]] = None,
    ):
        """
        Generate synthetic rows seeded by the value distribution in an existing file.

        Instead of producing purely random values, each column is sampled from the
        unique values observed in ``source`` (wherever that column exists in both the
        file and the contract schema).  Contract quality rules still apply — so
        ``invalid_ratio`` injects rows that intentionally break them.

        Parameters
        ----------
        source : str | Path | polars.DataFrame | pandas.DataFrame
            Seed file or in-memory DataFrame.  File formats auto-detected from extension:

            - ``.csv``             — comma-separated values
            - ``.parquet``         — Apache Parquet
            - ``.json``            — JSON array
            - ``.ndjson`` / ``.jsonl`` — newline-delimited JSON
            - ``.xlsx`` / ``.xls``    — Excel (requires ``openpyxl``)

        rows : int
            Total number of rows to generate (default 100).
        invalid_ratio : float
            Fraction of rows (0.0–1.0) that intentionally break quality rules.
            Useful for verifying quarantine logic.  e.g. ``0.1`` = 10% bad rows.
        output_format : str
            ``"polars"`` (default) or ``"pandas"``.
        columns : list of str, optional
            Restrict seeding to these specific column names.  Columns in the
            contract but absent from this list fall back to normal synthetic
            generation.  Defaults to all columns present in both file and schema.

        Returns
        -------
        DataFrame (Polars or Pandas depending on output_format)

        Examples
        --------
        Seed from a CSV file::

            from lakelogic import DataGenerator
            gen = DataGenerator("contracts/orders.yaml")

            # Mirror the distribution of a sample CSV
            df = gen.generate_from_sample("data/orders_sample.csv", rows=5_000)

        Mix file-seeded columns with bad rows::

            df = gen.generate_from_sample(
                "data/orders.parquet",
                rows=500,
                invalid_ratio=0.05,
            )

        Pass an existing DataFrame directly::

            import polars as pl
            seed = pl.read_parquet("data/seed.parquet")
            df = gen.generate_from_sample(seed, rows=200)

        Only seed specific columns (others use normal synthetic generation)::

            df = gen.generate_from_sample(
                "data/orders.csv",
                rows=1_000,
                columns=["status", "region"],
            )
        """
        sample_pools = self._load_sample_pools(source, columns=columns)

        n_invalid = int(rows * invalid_ratio)
        n_valid   = rows - n_invalid

        valid_rows   = [
            self._make_row(invalid=False, sample_pools=sample_pools)
            for _ in range(n_valid)
        ]
        invalid_rows = [
            self._make_row(invalid=True, sample_pools=sample_pools)
            for _ in range(n_invalid)
        ]

        all_records = valid_rows + invalid_rows
        self._rng.shuffle(all_records)
        return self._to_frame(all_records, output_format)

    # ------------------------------------------------------------------

    def save(
        self,
        df,
        output: str | Path,
        format: str = "parquet",
    ) -> Path:
        """
        Save a generated DataFrame to disk.

        Parameters
        ----------
        df : polars.DataFrame | pandas.DataFrame
        output : str | Path
            Destination file path.
        format : str
            ``"parquet"``, ``"csv"``, or ``"json"``.

        Returns
        -------
        Path
            The resolved output path.
        """
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        fmt = format.lower()

        if hasattr(df, "write_parquet"):  # Polars
            if fmt == "parquet":
                df.write_parquet(output)
            elif fmt == "csv":
                df.write_csv(output)
            elif fmt == "json":
                df.write_ndjson(output)
            else:
                raise ValueError(f"Unsupported format: {fmt}")
        else:  # Pandas
            if fmt == "parquet":
                df.to_parquet(output, index=False)
            elif fmt == "csv":
                df.to_csv(output, index=False)
            elif fmt == "json":
                df.to_json(output, orient="records", lines=True)
            else:
                raise ValueError(f"Unsupported format: {fmt}")

        return output

    def save_partitioned(
        self,
        df,
        output_dir: str | Path,
        filename_field: Optional[str] = None,
        format: str = "json",
        filename_template: str = "{value}",
        orient: str = "records",
        one_row_per_file: bool = True,
    ) -> List[Path]:
        """
        Save a generated DataFrame as one file per unique key, named by field value(s).

        This replicates landing-zone patterns where each entity (e.g. a property
        listing) arrives as its own file named after its primary key::

            output_dir/
            ├── 10001.json
            ├── 10002.json
            └── 10003.json

        Parameters
        ----------
        df : polars.DataFrame | pandas.DataFrame
            The generated DataFrame (output of ``generate()``).
        output_dir : str | Path
            Directory to write files into (created if it does not exist).
        filename_field : str, optional
            Column whose value is used as the primary key for grouping rows and
            as ``{value}`` in *filename_template*.
            Can be omitted when *filename_template* fully specifies the name via
            ``{column_name}`` placeholders (see below).
        format : str
            ``"json"`` (default), ``"parquet"``, or ``"csv"``.
        filename_template : str
            Template string for the filename stem.  Supports two kinds of placeholders:

            ``{value}``
                Replaced by the value of *filename_field*.  Backward-compatible shorthand.

            ``{column_name}``
                Replaced by the value of *column_name* for each row.  You can mix
                field references and static text freely.

            Examples (``listing_id=10001``, ``postcode="SW1A 1AA"``, ``property_type="flat"``):

            .. code-block:: text

               "{value}"                    → 10001.json          (default)
               "zoopla_{value}"             → zoopla_10001.json    (static prefix)
               "{value}_raw"                → 10001_raw.json       (static suffix)
               "{listing_id}_{postcode}"    → 10001_SW1A 1AA.json  (two fields)
               "listing_{listing_id}_{property_type}"  → listing_10001_flat.json

        orient : str
            JSON orientation when *format* is ``"json"``.
            ``"records"`` (default) writes a single JSON object per file.
            Use ``"lines"`` for NDJSON when ``one_row_per_file=False``.
        one_row_per_file : bool
            When ``True`` (default) each file contains exactly one JSON object
            (the canonical single-entity landing-zone pattern).
            When ``False``, all rows sharing the same key are written to the
            same file as a JSON array.

        Returns
        -------
        list[Path]
            Sorted list of paths written.

        Examples
        --------
        >>> gen = DataGenerator("contracts/bronze_zoopla_listings_v1.0.0.yaml")
        >>> df  = gen.generate(rows=50)

        Simple — just listing_id as the name:

        >>> paths = gen.save_partitioned(
        ...     df,
        ...     output_dir="data/landing/zoopla/listings",
        ...     filename_field="listing_id",
        ... )
        # → 10001.json, 10002.json, …

        Static prefix + field:

        >>> paths = gen.save_partitioned(
        ...     df,
        ...     output_dir="data/landing/zoopla/listings",
        ...     filename_field="listing_id",
        ...     filename_template="zoopla_{value}",
        ... )
        # → zoopla_10001.json, zoopla_10002.json, …

        Composite key from two columns:

        >>> paths = gen.save_partitioned(
        ...     df,
        ...     output_dir="data/landing/zoopla/listings",
        ...     filename_template="{listing_id}_{property_type}",
        ... )
        # → 10001_flat.json, 10002_terraced.json, …
        """
        import json as _json

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        fmt = format.lower()

        # ── Normalise to list-of-dicts for uniform handling ───────────────
        is_polars = hasattr(df, "write_parquet")
        columns = list(df.columns) if is_polars else list(df.columns)

        if filename_field is not None and filename_field not in columns:
            raise ValueError(
                f"filename_field '{filename_field}' not found in DataFrame. "
                f"Available columns: {columns}"
            )

        rows_iter = df.to_dicts() if is_polars else df.to_dict(orient="records")

        # ── Group rows by the primary key for file-per-entity splitting ───
        # Key is the filename_field value when provided, otherwise the full
        # resolved stem so each row gets its own file.
        groups: Dict[str, List[Dict]] = {}
        for row in rows_iter:
            key = (
                str(row[filename_field])
                if filename_field is not None
                else self._resolve_filename_stem(row, filename_template, None)
            )
            groups.setdefault(key, []).append(row)

        written: List[Path] = []
        ext = {"json": ".json", "parquet": ".parquet", "csv": ".csv"}.get(fmt, f".{fmt}")

        for key, group_rows in groups.items():
            # Resolve the stem from the first row in the group so per-row
            # field values (e.g. postcode) are reflected in the name.
            stem = self._resolve_filename_stem(
                group_rows[0], filename_template, key
            )
            dest = output_dir / f"{stem}{ext}"

            if fmt == "json":
                payload = group_rows[0] if one_row_per_file else group_rows
                dest.write_text(
                    _json.dumps(payload, default=str, indent=2),
                    encoding="utf-8",
                )

            elif fmt == "parquet":
                if is_polars:
                    import polars as pl
                    pl.DataFrame(group_rows).write_parquet(dest)
                else:
                    import pandas as pd
                    pd.DataFrame(group_rows).to_parquet(dest, index=False)

            elif fmt == "csv":
                if is_polars:
                    import polars as pl
                    pl.DataFrame(group_rows).write_csv(dest)
                else:
                    import pandas as pd
                    pd.DataFrame(group_rows).to_csv(dest, index=False)

            else:
                raise ValueError(
                    f"Unsupported format '{fmt}'. Choose 'json', 'parquet', or 'csv'."
                )

            written.append(dest)

        return sorted(written)

    @staticmethod
    def _resolve_filename_stem(
        row: Dict[str, Any],
        template: str,
        primary_value: Optional[str],
    ) -> str:
        """
        Resolve a filename stem from a row dict and a template string.

        Replacement order
        -----------------
        1. ``{value}`` → *primary_value* (the ``filename_field`` value).
        2. ``{column_name}`` → the value of that column in *row*, cast to str.
           Spaces are replaced with underscores so filenames stay shell-safe.
        3. Any ``{placeholder}`` that does not match a column name is left as-is
           so callers get a clear hint rather than a silent wrong filename.

        Parameters
        ----------
        row : dict
            A single row as a plain Python dict.
        template : str
            Template string, e.g. ``"zoopla_{listing_id}_{property_type}"``.
        primary_value : str | None
            Value of the ``filename_field`` column (used for ``{value}``).
            Pass ``None`` when there is no primary field.
        """
        import re as _re

        stem = template

        # 1. {value} shorthand
        if primary_value is not None:
            stem = stem.replace("{value}", str(primary_value))

        # 2. {column_name} → row value (space → underscore for safety)
        def _sub(match: "re.Match") -> str:  # type: ignore[name-defined]
            field = match.group(1)
            if field in row:
                return str(row[field]).replace(" ", "_")
            return match.group(0)  # leave unresolvable placeholders intact

        stem = _re.sub(r"\{([^}]+)\}", _sub, stem)
        return stem

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_sample_pools(
        self,
        source,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, List[Any]]:
        """
        Read *source* and return a dict mapping column name → list of unique values.

        *source* may be a file path (str / Path) or an existing polars / pandas
        DataFrame.  File format is inferred from the extension.
        """
        import polars as pl

        # ── Load into a Polars DataFrame ──────────────────────────────────────
        if isinstance(source, pl.DataFrame):
            df = source
        elif hasattr(source, "to_dict"):  # pandas DataFrame
            df = pl.from_pandas(source)
        else:
            p = Path(source)
            ext = p.suffix.lower()
            if ext == ".csv":
                df = pl.read_csv(p, infer_schema_length=10_000)
            elif ext == ".parquet":
                df = pl.read_parquet(p)
            elif ext == ".json":
                df = pl.read_json(p)
            elif ext in (".ndjson", ".jsonl"):
                df = pl.read_ndjson(p)
            elif ext in (".xlsx", ".xls"):
                try:
                    import pandas as pd
                    df = pl.from_pandas(pd.read_excel(p))
                except ImportError as exc:
                    raise ImportError(
                        "Excel support requires pandas and openpyxl: "
                        "pip install pandas openpyxl"
                    ) from exc
            else:
                raise ValueError(
                    f"Cannot infer file format from extension {ext!r}. "
                    "Supported: .csv, .parquet, .json, .ndjson, .jsonl, .xlsx, .xls"
                )

        # ── Identify which columns to pool ────────────────────────────────────
        schema_cols = {f["name"] for f in self._fields if "name" in f}
        file_cols   = set(df.columns)
        target_cols = schema_cols & file_cols
        if columns:
            target_cols = target_cols & set(columns)

        # ── Build pools: unique non-null values per column ────────────────────
        # We use df.to_dicts() rather than series.unique().to_list() so that
        # Struct / List / nested columns come back as proper Python dicts/lists
        # instead of Polars' internal display strings like {["val1"],["val2"]}.
        # This preserves the original nested JSON structure in generated output.
        import json as _json

        all_rows = df.to_dicts()
        pools: Dict[str, List[Any]] = {}

        for col in target_cols:
            seen: dict = {}  # json-key → original value
            for row in all_rows:
                val = row.get(col)
                if val is None:
                    continue
                try:
                    key = _json.dumps(val, sort_keys=True, default=str)
                except Exception:
                    key = str(val)
                if key not in seen:
                    seen[key] = val
            if seen:
                pools[col] = list(seen.values())

        return pools

    def _load_yaml(self) -> Dict[str, Any]:
        with open(self.contract_path, encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    def _extract_fields(self) -> List[Dict[str, Any]]:
        model = self._contract_raw.get("model") or {}
        return model.get("fields") or []

    def _extract_unique_integer_fields(self) -> set:
        """
        Return the set of field names that are:
          - integer type in the contract schema, AND
          - listed as a uniqueness rule in quality.dataset_rules

        These fields need special treatment in generate_from_sample when the
        seed source is a single record — a pool of one value would otherwise
        cause every generated row to carry the same ID.
        """
        integer_types = {"integer", "int", "int32", "int64", "long"}
        integer_fields = {
            f["name"]
            for f in self._fields
            if (f.get("type") or "").lower() in integer_types and "name" in f
        }

        quality = self._contract_raw.get("quality") or {}
        unique_rule_fields = {
            rule.get("sql", "").strip()  # dataset_rules store the column name in sql
            for rule in (quality.get("dataset_rules") or [])
            if rule.get("category") == "uniqueness"
        }

        return integer_fields & unique_rule_fields

    def _make_row(
        self,
        invalid: bool,
        fk_pools: Optional[Dict[str, List[Any]]] = None,
        sample_pools: Optional[Dict[str, List[Any]]] = None,
    ) -> Dict[str, Any]:
        row: Dict[str, Any] = {}
        field_rules = self._build_field_rules(fk_pools=fk_pools)

        for field in self._fields:
            name: str = field.get("name", "col")
            ftype: str = (field.get("type") or "string").lower()
            required: bool = field.get("required", False)
            nullable: bool = not required

            rules = field_rules.get(name, {})

            if invalid and self._rng.random() < 0.4:
                # 40% chance each field is broken in an invalid row
                row[name] = self._make_invalid_value(name, ftype, rules, nullable)
            else:
                row[name] = self._make_valid_value(
                    name, ftype, rules, nullable, sample_pools=sample_pools
                )

        return row

    def _make_valid_value(
        self,
        name: str,
        ftype: str,
        rules: Dict[str, Any],
        nullable: bool,
        sample_pools: Optional[Dict[str, List[Any]]] = None,
    ) -> Any:
        # Null injection for nullable fields
        if nullable and self._rng.random() < 0.03:
            return None

        # File-seeded pool — highest priority after null injection.
        # Sampling real observed values keeps the generated dataset realistic
        # (e.g. real postcodes, real status codes, realistic price ranges).
        #
        # Exception: when the pool has exactly ONE value AND the field looks like
        # a primary-key identifier (integer type + name contains "id"), we spread
        # values around the seed instead of repeating it.
        #
        # Why name-based rather than quality-rule-based: with a single source
        # record, suggest_rules never emits a uniqueness rule (needs >1 row to
        # confirm all values are distinct), so `_unique_integer_fields` is always
        # empty when seeding from a single JSON file.
        #
        # Non-ID integers (bathrooms=3, total_bedrooms=4) correctly repeat.
        if sample_pools and name in sample_pools:
            pool = sample_pools[name]
            is_int_type = ftype in ("integer", "int", "int32", "int64", "long")
            is_id_field = "id" in name.lower()
            single_value_id_int = len(pool) == 1 and is_int_type and is_id_field
            if pool and not single_value_id_int:
                return self._rng.choice(pool)
            if single_value_id_int:
                # Spread around the seed value so IDs stay realistic
                # (10001 → random in 10001..20001)
                seed_val = int(pool[0])
                return self._rng.randint(seed_val, seed_val + 10_000)

        # accepted_values ALWAYS wins — even over Faker semantic hints.
        # This is critical: Faker's country() returns 'United Kingdom', not 'GB'.
        # When a dbt accepted_values test exists, the generator must pick from that list.
        accepted = rules.get("accepted_values")
        if accepted:
            return self._rng.choice(accepted)

        min_val = rules.get("min")
        max_val = rules.get("max")

        if ftype in ("integer", "int", "int32", "int64", "long"):
            lo = int(min_val) if min_val is not None else 1
            hi = int(max_val) if max_val is not None else 10_000
            return self._rng.randint(lo, hi)

        if ftype in ("double", "float", "float32", "float64", "decimal", "number"):
            lo = float(min_val) if min_val is not None else 0.01
            hi = float(max_val) if max_val is not None else 1_000.0
            return round(self._rng.uniform(lo, hi), 4)

        if ftype in ("boolean", "bool"):
            return self._rng.choice([True, False])

        if ftype in ("date",):
            base = date(2023, 1, 1)
            return (base + timedelta(days=self._rng.randint(0, 730))).isoformat()

        if ftype in ("timestamp", "datetime"):
            base = datetime(2023, 1, 1)
            return (base + timedelta(seconds=self._rng.randint(0, 60 * 60 * 24 * 730))).isoformat()

        # String — try Faker semantic generation, then format-aware fallback
        return self._string_value(name)

    def _make_invalid_value(
        self,
        name: str,
        ftype: str,
        rules: Dict[str, Any],
        nullable: bool,
    ) -> Any:
        strategies = []

        # Null on required field — always a valid "bad" strategy
        strategies.append(lambda: None)

        accepted = rules.get("accepted_values")
        if accepted:
            # Value outside accepted list — must stay same Python type
            if ftype in ("boolean", "bool"):
                # Booleans have no "outside" value; force None (fails required rule)
                pass
            else:
                strategies.append(
                    lambda: "INVALID_" + "".join(self._rng.choices(string.ascii_uppercase, k=4))
                )

        min_val = rules.get("min")
        max_val = rules.get("max")

        if ftype in ("integer", "int", "int32", "int64", "long"):
            if min_val is not None:
                strategies.append(lambda: int(min_val) - self._rng.randint(1, 100))
            if max_val is not None:
                strategies.append(lambda: int(max_val) + self._rng.randint(1, 100))
            if len(strategies) == 1:  # only None so far
                strategies.append(lambda: -self._rng.randint(1, 999))

        elif ftype in ("double", "float", "float32", "float64", "decimal", "number"):
            if min_val is not None:
                strategies.append(lambda: float(min_val) - self._rng.uniform(0.01, 10.0))
            if max_val is not None:
                strategies.append(lambda: float(max_val) + self._rng.uniform(0.01, 10.0))
            if len(strategies) == 1:
                strategies.append(lambda: -self._rng.uniform(0.01, 999.0))

        elif ftype in ("boolean", "bool"):
            # Only valid Python booleans or None — never strings
            # None (already added) is the only way to violate a boolean required rule
            pass

        else:
            # String fields: empty string or known-bad sentinel stays as str
            strategies.append(lambda: "")  # fails not-null / pattern rules

        return self._rng.choice(strategies)()

    def _string_value(self, name: str) -> str:
        name_lower = name.lower()

        if self._faker:
            for hint, method in _SEMANTIC_HINTS.items():
                if hint in name_lower:
                    return str(getattr(self._faker, method)())

        # Format-aware fallbacks for common field names — avoids Faker dependency.
        # These must produce values that pass the most common dbt expression_is_true checks.
        if "email" in name_lower or name_lower == "mail":
            user   = "".join(self._rng.choices(string.ascii_lowercase, k=self._rng.randint(4, 10)))
            domain = "".join(self._rng.choices(string.ascii_lowercase, k=self._rng.randint(3, 8)))
            tld    = self._rng.choice(["com", "org", "net", "io", "co"])
            return f"{user}@{domain}.{tld}"

        if "phone" in name_lower or "mobile" in name_lower:
            digits = "".join(self._rng.choices(string.digits, k=10))
            return f"+1{digits}"

        if "url" in name_lower or "website" in name_lower or "link" in name_lower:
            slug = "".join(self._rng.choices(string.ascii_lowercase, k=self._rng.randint(4, 10)))
            return f"https://www.{slug}.com"

        # Generic random alphanumeric fallback
        length = self._rng.randint(5, 12)
        return "".join(self._rng.choices(string.ascii_lowercase + string.digits, k=length))

    def _build_field_rules(self, fk_pools: Optional[Dict[str, List[Any]]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Parse quality rules into a per-field lookup of { min, max, accepted_values }.

        Seeds from field-level definitions first (so injected accepted_values /
        min / max from DataGenerator.from_dbt() are always honoured), then
        overlays any structured quality row_rules on top.

        FK columns: if ``fk_pools`` contains a pool for the column, that pool
        becomes the ``accepted_values`` for generation.  If no pool is supplied
        but the field declares a ``foreign_key``, a surrogate integer pool of
        1–N is used so the generated data is at least type-correct.
        """
        result: Dict[str, Dict[str, Any]] = {}

        # ── 1. Seed from field-level definitions ──────────────────────────────
        for field in self._fields:
            fname = field.get("name", "")
            if not fname:
                continue
            entry = result.setdefault(fname, {})
            if "accepted_values" in field:
                entry.setdefault("accepted_values", field["accepted_values"])
            if "min" in field:
                entry.setdefault("min", field["min"])
            if "max" in field:
                entry.setdefault("max", field["max"])

            # ── FK hint — inject caller-supplied pool or a surrogate fallback ──
            fk = field.get("foreign_key")  # dict with keys: contract, column (+ severity)
            if fk and isinstance(fk, dict):
                if fk_pools and fname in fk_pools:
                    # Caller provided a real pool from the reference table
                    entry["accepted_values"] = fk_pools[fname]
                    entry["_fk_contract"] = fk.get("contract")
                    entry["_fk_column"]   = fk.get("column")
                elif "accepted_values" not in entry:
                    # No pool supplied — generate surrogate integers for CI safety
                    # (valid type, referentially meaningless, but won't break type checks)
                    surrogate_n = max(10, len(self._fields) * 5)
                    entry["accepted_values"] = list(range(1, surrogate_n + 1))
                    entry["_fk_contract"] = fk.get("contract")
                    entry["_fk_column"]   = fk.get("column")
                    entry["_fk_surrogate"] = True  # flag: values are synthetic, not real

        # ── 2. Overlay structured quality row_rules ───────────────────────────
        quality = self._quality

        for rule_item in quality.get("row_rules", []):
            if not isinstance(rule_item, dict):
                continue

            # accepted_values structured rule
            if "accepted_values" in rule_item:
                av = rule_item["accepted_values"]
                field = av.get("field") if isinstance(av, dict) else None
                values = av.get("values") if isinstance(av, dict) else None
                if field and values:
                    result.setdefault(field, {})["accepted_values"] = values

            # range structured rule
            if "range" in rule_item:
                rng = rule_item["range"]
                field = rng.get("field") if isinstance(rng, dict) else None
                if field:
                    result.setdefault(field, {})
                    if "min" in rng:
                        result[field]["min"] = rng["min"]
                    if "max" in rng:
                        result[field]["max"] = rng["max"]

            # referential_integrity structured rule — overlay the caller pool
            # (quality rule takes same pool as field-level hint; already seeded above)
            if "referential_integrity" in rule_item:
                ri = rule_item["referential_integrity"]
                if isinstance(ri, dict):
                    field = ri.get("field")
                    if field and fk_pools and field in fk_pools:
                        result.setdefault(field, {})["accepted_values"] = fk_pools[field]

            # ── SQL-variant FK/RI rules ────────────────────────────────────────
            # Handles QualityRule entries written as raw SQL rather than the
            # structured referential_integrity block.  Two sub-cases:
            #
            #  (a) col IN ('x', 'y')  — literal values in SQL
            #      → extract literals and use directly as accepted_values
            #      → works even without fk_pools (no runtime reference data needed)
            #      Example contract YAML:
            #        - name: valid_status
            #          sql: "status IN ('active', 'inactive')"
            #          category: validity
            #
            #  (b) col IN (SELECT col FROM table)  — subquery
            #      → cannot statically evaluate; apply fk_pools if caller provided it
            #      Example contract YAML:
            #        - name: valid_agent
            #          sql: "agent_id IN (SELECT agent_id FROM silver.agents)"
            #          category: integrity    # ← this category flags it as FK/RI
            #
            #      At generate() call site:
            #        agents = DataGenerator("silver_agents.yaml").generate(rows=20)
            #        orders = DataGenerator("gold_orders.yaml").generate(
            #            rows=200,
            #            reference_data={"agent_id": agents["agent_id"]},
            #        )
            sql_str       = rule_item.get("sql", "")
            rule_category = rule_item.get("category", "")

            if sql_str:
                # Sub-case (a): col IN (literal, values) — no subquery
                m_lit = _re.match(
                    r"^\s*(\w+)\s+IN\s*\(([^)]+)\)\s*$", sql_str, _re.IGNORECASE
                )
                if m_lit:
                    col   = m_lit.group(1)
                    inner = m_lit.group(2)
                    if not _re.search(r"\bSELECT\b", inner, _re.IGNORECASE):
                        # Extract quoted strings or bare numbers
                        parsed = [
                            s or n for s, n in
                            _re.findall(r"'([^']*)'|\b(-?\d+(?:\.\d+)?)\b", inner)
                        ]
                        if parsed:
                            # setdefault: field-level accepted_values takes priority
                            result.setdefault(col, {}).setdefault("accepted_values", parsed)

                # Sub-case (b): subquery or category == integrity → apply fk_pools
                m_sub = _re.match(
                    r"^\s*(\w+)\s+IN\s*\(\s*SELECT\b", sql_str, _re.IGNORECASE
                )
                is_integrity_category = rule_category in (
                    "integrity", "referential_integrity", "referential"
                )
                if (m_sub or is_integrity_category) and fk_pools:
                    # Primary: column named before IN in the SQL
                    col_from_sql = m_sub.group(1) if m_sub else None
                    # Fallback: any fk_pool key whose name appears verbatim in SQL
                    candidates = (
                        [col_from_sql] if col_from_sql
                        else [c for c in fk_pools
                              if _re.search(r"\b" + _re.escape(c) + r"\b", sql_str)]
                    )
                    for col in candidates:
                        if col and col in fk_pools:
                            # Override — fk_pools always wins over surrogate fallback
                            result.setdefault(col, {})["accepted_values"] = fk_pools[col]
                            result[col]["_fk_from_sql"] = True  # audit flag

        return result

    def _to_frame(self, records: List[Dict[str, Any]], fmt: str):
        if fmt == "polars":
            import polars as pl

            df = pl.DataFrame(records)

            # Cast every column to its contract-declared dtype so the processor
            # never sees Utf8View where it expects Boolean / Int64 / Float64.
            cast_exprs = []
            for field in self._fields:
                col_name = field.get("name", "")
                ftype    = (field.get("type") or "string").lower()
                dtype    = _polars_dtype(ftype)
                if dtype is not None and col_name in df.columns:
                    current = df[col_name].dtype
                    if current != dtype:
                        cast_exprs.append(
                            pl.col(col_name).cast(dtype, strict=False).alias(col_name)
                        )
            if cast_exprs:
                df = df.with_columns(cast_exprs)

            return df

        elif fmt == "pandas":
            import pandas as pd
            return pd.DataFrame(records)
        else:
            raise ValueError(f"output_format must be 'polars' or 'pandas', got: {fmt!r}")
