"""
lakelogic.core.bootstrap
------------------------
Contract inference from existing data files.

Generates a LakeLogic-native contract YAML from any CSV, Parquet, JSON, NDJSON
or Excel file (or an in-memory Polars / Pandas DataFrame), without requiring any
prior contract definition.

Python API
----------
    from lakelogic import infer_contract

    # Infer a contract from a file and save it
    draft = infer_contract("data/zoopla_sample.csv")
    draft.show()                           # pretty-print the YAML
    draft.save("contracts/zoopla.yaml")    # write to disk

    # Chain straight into DataGenerator (no intermediate file needed)
    gen = infer_contract("data/zoopla_sample.csv").to_generator(seed=42)
    df  = gen.generate(rows=5_000)

    # Suggest quality rules automatically
    draft = infer_contract("data/orders.parquet", suggest_rules=True)
    draft.save("contracts/orders.yaml")

    # Pass an in-memory DataFrame
    import polars as pl
    df_seed = pl.read_json("data/10001.json")
    draft = infer_contract(df_seed, title="Zoopla Listings", layer="bronze")
"""

from __future__ import annotations

import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

# ---------------------------------------------------------------------------
# Polars dtype → LakeLogic contract type mapping
# ---------------------------------------------------------------------------

_POLARS_DTYPE_MAP: Dict[str, str] = {
    # String / text
    "utf8":    "string",
    "string":  "string",
    "large_utf8": "string",
    "categorical": "string",
    "enum":    "string",
    # Integers
    "int8":    "integer",
    "int16":   "integer",
    "int32":   "integer",
    "int64":   "integer",
    "uint8":   "integer",
    "uint16":  "integer",
    "uint32":  "integer",
    "uint64":  "integer",
    # Floats
    "float32": "double",
    "float64": "double",
    "decimal": "double",
    # Temporal
    "date":      "date",
    "time":      "string",
    "datetime":  "timestamp",
    "duration":  "string",
    # Boolean
    "boolean": "boolean",
    "bool":    "boolean",
    # Structural — stored as string at bronze
    "list":    "string",
    "array":   "string",
    "struct":  "string",
    "object":  "string",
    # Null / unknown
    "null":    "string",
    "unknown": "string",
}


def _polars_dtype_to_contract(dtype) -> str:
    """Map a polars DataType instance to a LakeLogic contract type string."""
    # dtype.__class__.__name__ gives e.g. "Int64", "Datetime", "List"
    base = dtype.__class__.__name__.lower()
    return _POLARS_DTYPE_MAP.get(base, "string")


# ---------------------------------------------------------------------------
# YAML serialisation — readable formatting with blank lines between sections
# ---------------------------------------------------------------------------


def _format_contract_yaml(data: dict) -> str:
    """
    Serialise *data* to a YAML string with human-friendly blank-line spacing.

    Rules applied (in order):
    1. Blank line before every top-level key (indent == 0) — covers all standard
       sections (version, info, model, quality, server) AND any extra keys such
       as ``lineage``, ``owner``, ``tags``, ``sla``, etc.
    2. Blank line between consecutive ``- name:`` list items inside:
       - ``model.fields``
       - ``quality.row_rules``
       - ``quality.dataset_rules``
       - ``transformations`` (any depth)
    """
    # FieldList is a list subclass — tell PyYAML to represent it as a plain
    # sequence so safe_load() can read it back without Python-tag errors.
    # We use a local Dumper subclass to avoid mutating the global default Dumper.
    class _Dumper(yaml.Dumper):
        pass
    _Dumper.add_representer(
        FieldList,
        lambda dumper, data: dumper.represent_sequence("tag:yaml.org,2002:seq", data),
    )
    raw = yaml.dump(data, Dumper=_Dumper, sort_keys=False, allow_unicode=True, default_flow_style=False)
    lines = raw.splitlines()
    out: list[str] = []

    for i, line in enumerate(lines):
        stripped = line.lstrip()
        indent = len(line) - len(stripped)

        # ── rule 1: blank line before every top-level key ────────────────
        # A top-level key is: zero indentation, non-empty, contains ":",
        # and does NOT start with "#" (comment) or "-" (list item).
        is_top_level_key = (
            indent == 0
            and stripped
            and ":" in stripped
            and not stripped.startswith("#")
            and not stripped.startswith("-")
        )
        if is_top_level_key and i > 0 and out and out[-1].strip() != "":
            out.append("")

        # ── rule 2: blank line before every - name: item (fields / rules) ─
        # Unconditionally add a blank unless the immediately preceding output
        # line is already blank — covers nested list endings like '- value'.
        elif stripped.startswith("- name:"):
            if out and out[-1].strip() != "":
                out.append("")

        out.append(line)

    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# ContractDraft — the result object
# ---------------------------------------------------------------------------


class FieldList(list):
    """
    A list of field dicts that upserts by ``name`` on ``.append()``.

    If a field with the same ``name`` already exists the new attributes are
    **merged into** the existing entry (new keys win, existing keys not present
    in the new dict are preserved).  If no field with that name exists the dict
    is appended normally — identical to plain ``list.append``.

    This means callers can safely do::

        draft.fields.append({"name": "listing_id", "min": 10001, "max": 99999})

    … and the inferred ``listing_id`` entry will be *updated* rather than
    duplicated, even if it already exists from ``infer_contract``.
    """

    def append(self, item: Dict[str, Any]) -> None:  # type: ignore[override]
        name = item.get("name") if isinstance(item, dict) else None
        if name:
            for existing in self:
                if isinstance(existing, dict) and existing.get("name") == name:
                    # Merge: new attributes overwrite, unmentioned keys kept
                    existing.update(item)
                    return
        super().append(item)


class ContractDraft:
    """
    An inferred contract ready to save, inspect, or use directly.

    Attributes
    ----------
    fields : list of dict
        Inferred field definitions (name, type, and optional hints).
    quality : dict
        Suggested quality rules (populated when ``suggest_rules=True``).

    Examples
    --------
    ::

        from lakelogic import infer_contract

        draft = infer_contract("data/orders.csv", suggest_rules=True)

        # Inspect
        print(draft)
        draft.show()               # print YAML to stdout
        draft.to_yaml()            # return YAML as a string

        # Save
        draft.save("contracts/orders.yaml")

        # Chain into DataGenerator
        gen = draft.to_generator(seed=42)
        df  = gen.generate(rows=1_000)
    """

    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def fields(self) -> "FieldList":
        """Upsert-by-name list of field dicts: [{name, type, ...}, ...].

        ``.append({"name": "col", ...})`` merges into an existing field with
        the same name rather than adding a duplicate.
        """
        model = self._data.setdefault("model", {})
        raw = model.get("fields")
        if not isinstance(raw, FieldList):
            # Wrap the backing list in FieldList once (in-place replacement)
            wrapped = FieldList(raw or [])
            model["fields"] = wrapped
        return model["fields"]

    @property
    def quality(self) -> Dict[str, Any]:
        """Suggested quality rules dict."""
        return self._data.get("quality") or {}

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Return the full contract as a plain Python dict."""
        import copy
        return copy.deepcopy(self._data)

    def to_yaml(self) -> str:
        """Return the contract as a formatted YAML string with section spacing."""
        return _format_contract_yaml(self._data)

    def show(self) -> None:
        """Pretty-print the formatted contract YAML to stdout."""
        print(self.to_yaml())

    def save(
        self,
        path: Union[str, Path],
        overwrite: bool = True,
    ) -> Path:
        """
        Write the contract to a YAML file.

        Parameters
        ----------
        path : str | Path
            Destination file path.  Parent directories are created automatically.
        overwrite : bool
            If False, raises ``FileExistsError`` when the file already exists.

        Returns
        -------
        Path
            The resolved output path.
        """
        out = Path(path)
        if out.exists() and not overwrite:
            raise FileExistsError(
                f"Contract file already exists: {out}. "
                "Pass overwrite=True to replace it."
            )
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(_format_contract_yaml(self._data), encoding="utf-8")
        return out

    # ------------------------------------------------------------------
    # Integration with DataGenerator
    # ------------------------------------------------------------------

    def to_generator(
        self,
        seed: Optional[int] = None,
        use_faker: bool = True,
    ) -> "DataGenerator":  # type: ignore[return]
        """
        Return a :class:`DataGenerator` backed by this inferred contract.

        The generator is immediately usable via ``.generate(rows=N)``.

        Parameters
        ----------
        seed : int, optional
            Random seed for reproducible generation.
        use_faker : bool
            If True and Faker is installed, use semantic generation for
            string fields (email, name, phone, …).

        Returns
        -------
        DataGenerator

        Examples
        --------
        ::

            gen = infer_contract("data/orders.csv").to_generator(seed=0)
            df  = gen.generate(rows=500)
        """
        from lakelogic.core.generator import DataGenerator

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(_format_contract_yaml(self._data))
            tmp_path = tmp.name

        return DataGenerator(tmp_path, seed=seed, use_faker=use_faker)

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        n = len(self.fields)
        title = (self._data.get("info") or {}).get("title", "untitled")
        has_rules = bool(self.quality.get("row_rules") or self.quality.get("dataset_rules"))
        rules_str = " + rules" if has_rules else ""
        return f"ContractDraft(title={title!r}, fields={n}{rules_str})"


# ---------------------------------------------------------------------------
# ContractInferrer — the engine
# ---------------------------------------------------------------------------


class ContractInferrer:
    """
    Infer a LakeLogic contract from an existing data file or DataFrame.

    Usually you call the module-level :func:`infer_contract` convenience
    function rather than instantiating this class directly.

    Parameters
    ----------
    source : str | Path | polars.DataFrame | pandas.DataFrame
        Data source.  File formats are auto-detected from extension:
        ``.csv``, ``.parquet``, ``.json``, ``.ndjson`` / ``.jsonl``, ``.xlsx`` / ``.xls``.
    title : str, optional
        Contract title written to ``info.title``.  Defaults to the file stem.
    version : str
        Contract version string (default ``"1.0.0"``).
    description : str, optional
        Contract description.
    layer : str
        Data layer hint (``"bronze"``, ``"silver"``, …); used in the
        ``info.target_layer`` field (default ``"bronze"``).
    sample_rows : int
        Maximum number of rows to read from the file for inference (default
        10 000).  Has no effect when a DataFrame is passed directly.
    suggest_rules : bool
        If True (default), auto-suggest quality rules:

        - ``not_null`` — when < 1% of values are null
        - ``unique``   — when all values in the column are distinct
        - ``accepted_values`` — when a string column has ≤ 20 distinct values

    detect_pii : bool
        If True, run Presidio PII analysis and flag ``pii: true`` on detected
        fields.  Requires ``pip install lakelogic[profiling]``.
    pii_sample_size : int
        Number of sample values per column used for PII detection (default 50).
    extra : dict, optional
        Additional top-level sections to merge into the contract.
        Any key/value pairs are written directly into the contract YAML
        after the standard sections (``version``, ``info``, ``model``,
        ``server``, ``quality``).  Use this to attach custom metadata
        without modifying the inferrer source.

        Common examples::

            extra={
                "lineage": {
                    "source_system": "Zoopla API",
                    "pipeline": "bronze_zoopla_listings",
                    "upstream": ["raw.zoopla_listings_api"],
                },
                "owner": {"team": "data-eng", "slack": "#data-platform"},
                "tags": ["pii", "real-estate", "bronze"],
                "sla": {"freshness_hours": 24},
            }

    """

    def __init__(
        self,
        source: Any,
        *,
        title: Optional[str] = None,
        version: str = "1.0.0",
        description: Optional[str] = None,
        layer: str = "bronze",
        domain: Optional[str] = None,
        system: Optional[str] = None,
        sample_rows: int = 10_000,
        suggest_rules: bool = True,
        detect_pii: bool = False,
        pii_sample_size: int = 50,
        extra: Optional[Dict[str, Any]] = None,
        preserve_nested: Union[bool, List[str]] = False,
    ) -> None:
        self.source = source
        self.title = title
        self.version = version
        self.description = description
        self.layer = layer
        self.domain = domain
        self.system = system
        self.sample_rows = sample_rows
        self.suggest_rules = suggest_rules
        self.detect_pii = detect_pii
        self.pii_sample_size = pii_sample_size
        self.extra: Dict[str, Any] = extra or {}
        # preserve_nested controls nested JSON flattening:
        #   False (default) — explode all nested fields into parent_child columns
        #   True            — keep all nested values as raw JSON strings (bronze)
        #   ["col", ...]    — preserve only the listed columns; explode the rest
        self.preserve_nested: Union[bool, List[str]] = preserve_nested

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def infer(self) -> ContractDraft:
        """Run inference and return a :class:`ContractDraft`."""
        import polars as pl

        # ── 1. Load into Polars ───────────────────────────────────────────
        df, source_path = self._load(pl)

        # ── 1b. Flatten nested JSON columns (unless preserved) ────────────
        df = self._flatten_df(df)

        # ── 2. Build title / description from source path ─────────────────
        if source_path:
            auto_title = source_path.stem.replace("_", " ").replace("-", " ").title()
        else:
            auto_title = "Inferred Contract"

        title = self.title or auto_title
        description = self.description or f"Auto-inferred contract. Source: {source_path or 'DataFrame'}."

        # ── 3. Infer fields + quality ─────────────────────────────────────
        pii_map: Dict[str, str] = {}
        if self.detect_pii:
            # Use pandas for Presidio (it works on plain Python iterables)
            pd_df = df.to_pandas()
            pii_map = self._detect_pii(pd_df)

        fields = self._infer_fields(df, pii_map)
        quality: Dict[str, Any] = {}
        if self.suggest_rules:
            quality = self._suggest_rules(df)

        # ── 4. Assemble contract dict ─────────────────────────────────────
        info_block: Dict[str, Any] = {
                "title": title,
                "version": self.version,
                "description": description,
                "target_layer": self.layer,
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        if self.domain is not None:
            info_block["domain"] = self.domain
        if self.system is not None:
            info_block["system"] = self.system

        contract: Dict[str, Any] = {
            "version": "1.0.0",
            "info": info_block,
            "model": {"fields": fields},
        }
        if source_path:
            contract["server"] = {
                "type": "local",
                "path": str(source_path),
                "format": source_path.suffix.lstrip(".").lower(),
            }
        if quality:
            contract["quality"] = quality

        # ── 5. Merge extra top-level sections ──────────────────────────────
        # Done last so callers can override auto-generated keys if needed.
        # Deep-merge is used for "model" so that extra={"model": {}} or
        # extra={"model": {"description": "..."}} does NOT wipe the inferred
        # fields list.  The caller must supply extra["model"]["fields"] explicitly
        # to fully replace the inferred fields.  All other top-level keys use
        # plain replace semantics (same as before).
        if self.extra:
            for key, value in self.extra.items():
                if key == "model" and isinstance(value, dict) and isinstance(contract.get("model"), dict):
                    # Deep merge: keep inferred fields unless caller explicitly supplies them
                    merged_model = dict(contract["model"])
                    for mk, mv in value.items():
                        if mk == "fields" and not mv:
                            # Caller passed fields: [] or fields: None — skip, keep inferred
                            continue
                        merged_model[mk] = mv
                    contract["model"] = merged_model
                else:
                    contract[key] = value

        return ContractDraft(contract)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load(self, pl):
        """Load source into a polars DataFrame, return (df, source_path)."""
        source = self.source

        if isinstance(source, pl.DataFrame):
            return source, None

        if hasattr(source, "to_dict"):  # pandas DataFrame
            return pl.from_pandas(source), None

        p = Path(source)
        ext = p.suffix.lower()

        if ext == ".csv":
            df = pl.read_csv(p, infer_schema_length=self.sample_rows, n_rows=self.sample_rows)
        elif ext == ".parquet":
            df = pl.read_parquet(p).head(self.sample_rows)
        elif ext == ".json":
            try:
                df = pl.read_json(p)
            except Exception:
                # Fallback: single-record JSON (not an array) → wrap in list
                import json
                raw = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    raw = [raw]
                df = pl.from_dicts(raw)
            df = df.head(self.sample_rows)
        elif ext in (".ndjson", ".jsonl"):
            df = pl.read_ndjson(p).head(self.sample_rows)
        elif ext in (".xlsx", ".xls"):
            try:
                import pandas as pd
                df = pl.from_pandas(pd.read_excel(p, nrows=self.sample_rows))
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
        return df, p

    def _flatten_df(self, df) -> Any:
        """
        Recursively explode nested JSON columns into flat ``parent_child`` columns.

        Controlled by ``self.preserve_nested``:

        - ``False`` (default) — explode every column whose sampled values are
          dicts or lists.  This also includes string columns that contain
          serialised JSON objects/arrays (common when reading a bronze Delta
          table where nested fields were stored as JSON strings).
        - ``True``            — keep all nested values as raw JSON strings
          (original bronze behaviour).  JSON-string columns are left as-is.
        - ``list``            — preserve only the named columns; explode the rest.
        """
        import json as _json
        import polars as pl

        # preserve_nested=True → skip all flattening
        if self.preserve_nested is True:
            return df

        preserved: set = set(self.preserve_nested) if isinstance(self.preserve_nested, list) else set()

        # ── Pre-pass: parse JSON-string columns into Python dicts/lists ──────
        # When a DataFrame comes from a Delta/Parquet/Parquet-on-bronze table,
        # nested objects land as plain strings (e.g. '{"price": 1500, ...}').
        # We sample each string column; if the majority of non-null values
        # deserialise to a dict or list we replace the values in the row dict
        # so the existing explode loop can handle them normally.
        #
        # This makes preserve_nested work for DataFrames, not just file reads.

        def _try_parse_json(val: Any) -> Any:
            """Return parsed JSON if val is a JSON object/array string, else val."""
            if not isinstance(val, str):
                return val
            stripped = val.strip()
            if not (stripped.startswith("{") or stripped.startswith("[")):
                return val
            try:
                return _json.loads(stripped)
            except Exception:
                return val

        def _is_json_col(rows: List[Dict], col: str, sample: int = 20) -> bool:
            """Return True when ≥ 50% of sampled non-null values parse as dict/list."""
            hits = 0
            checked = 0
            for row in rows[:sample]:
                val = row.get(col)
                if val is None:
                    continue
                checked += 1
                parsed = _try_parse_json(val)
                if isinstance(parsed, (dict, list)):
                    hits += 1
            return checked > 0 and (hits / checked) >= 0.5

        # Convert DataFrame to plain Python dicts for easy manipulation
        rows = df.to_dicts()

        if rows:
            # Identify string columns that are actually serialised JSON
            json_string_cols = [
                col for col in rows[0]
                if col not in preserved
                and isinstance(rows[0].get(col), str)  # quick type pre-filter
                and _is_json_col(rows, col)
            ]
            # Deserialise in-place so the explode loop sees real dicts/lists
            if json_string_cols:
                parsed_rows = []
                for row in rows:
                    new_row = dict(row)
                    for col in json_string_cols:
                        new_row[col] = _try_parse_json(row.get(col))
                    parsed_rows.append(new_row)
                rows = parsed_rows

        def _explode_col(rows: List[Dict], col: str, prefix: str) -> List[Dict]:
            """Replace *col* in each row with flattened ``prefix_key`` entries."""
            # Collect all child keys seen across all rows for this column
            all_keys: dict = {}  # key → sample value (for type inference)
            for row in rows:
                val = row.get(col)
                if isinstance(val, dict):
                    all_keys.update({k: v for k, v in val.items() if k not in all_keys})
                elif isinstance(val, list) and val and isinstance(val[0], dict):
                    # List-of-dicts: explode first element's keys
                    all_keys.update({k: v for k, v in val[0].items() if k not in all_keys})

            if not all_keys:
                return rows  # nothing to explode — leave as is

            new_rows = []
            for row in rows:
                new_row = {k: v for k, v in row.items() if k != col}
                val = row.get(col)
                if isinstance(val, dict):
                    for key in all_keys:
                        child = val.get(key)
                        new_row[f"{prefix}_{key}"] = (
                            _json.dumps(child, ensure_ascii=False)
                            if isinstance(child, (dict, list)) else child
                        )
                elif isinstance(val, list):
                    # Represent list as JSON string for the column itself
                    new_row[f"{prefix}_values"] = _json.dumps(val, ensure_ascii=False)
                else:
                    # null / scalar — fill all child cols with None
                    for key in all_keys:
                        new_row[f"{prefix}_{key}"] = None
                new_rows.append(new_row)
            return new_rows

        changed = True
        # Iterate until no more nested columns remain (handles deeply nested JSON)
        max_depth = 5
        depth = 0
        while changed and depth < max_depth:
            changed = False
            depth += 1

            # Re-run JSON-string detection each iteration: columns that were
            # serialised back to a JSON string during a previous parent explosion
            # (e.g. location_coordinates after location was flattened) need to
            # be detected and deserialised before the next explode pass.
            if rows:
                json_string_cols_iter = [
                    col for col in rows[0]
                    if col not in preserved
                    and isinstance(rows[0].get(col), str)
                    and _is_json_col(rows, col)
                ]
                if json_string_cols_iter:
                    parsed_rows = []
                    for row in rows:
                        new_row = dict(row)
                        for col in json_string_cols_iter:
                            new_row[col] = _try_parse_json(row.get(col))
                        parsed_rows.append(new_row)
                    rows = parsed_rows

            # Detect columns with nested values
            cols_to_explode = []
            for col in list(rows[0].keys()) if rows else []:
                if col in preserved:
                    continue
                for row in rows:
                    val = row.get(col)
                    if isinstance(val, (dict, list)):
                        cols_to_explode.append(col)
                        break
            for col in cols_to_explode:
                rows = _explode_col(rows, col, col)
                changed = True

        # Rebuild polars DataFrame from flattened rows
        if not rows:
            return df
        try:
            return pl.from_dicts(rows)
        except Exception:
            return df  # fallback: return original if reconstruction fails

    def _infer_fields(
        self,
        df,
        pii_map: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """Build a list of field dicts from a polars DataFrame."""
        fields = []
        for col_name in df.columns:
            col_dtype = df[col_name].dtype
            ctype = _polars_dtype_to_contract(col_dtype)
            series = df[col_name].drop_nulls()
            null_ratio = 1.0 - len(series) / max(len(df), 1)
            required = null_ratio < 0.01

            field: Dict[str, Any] = {
                "name": col_name,
                "type": ctype,
            }
            if required:
                field["required"] = True

            # PII tagging
            if col_name in pii_map:
                field["pii"] = True
                field["classification"] = pii_map[col_name].lower()

            # One representative example per column.
            # Skipped entirely for PII-flagged columns to avoid leaking sensitive data.
            if col_name not in pii_map and not series.is_empty():
                if ctype == "string":
                    # Only show an example if the column has low cardinality
                    # (i.e. it's a useful, non-free-text field).
                    if 0 < series.n_unique() <= 20:
                        field["examples"] = [sorted(series.unique().to_list())[0]]
                # For non-string types with examples-worthy cardinality, skip —
                # the field type + range rule conveys enough information.

            fields.append(field)
        return fields

    def _suggest_rules(self, df) -> Dict[str, Any]:
        """Suggest structured quality rules from observed data.

        Rule selection heuristics
        -------------------------
        * ``not_null``       — every column with < 1% nulls.
        * ``unique``         — dataset-level; columns where every value is distinct.
        * ``accepted_values`` — string columns with ≤ 20 distinct values;
                               also numeric columns whose name signals an enum
                               (keywords: status, type, option, flag, category, mode).
        * ``range``          — numeric columns, unless the column name signals
                               an identifier or temporal value (keywords: id, key,
                               date, ts, timestamp) or an enum (see above).

        Columns whose name contains ``id``, ``key``, ``date``, ``ts``,
        ``timestamp``, ``desc``, or ``description`` receive only a ``not_null``
        rule — their values are too varied / too high-cardinality to be useful
        as range or enum constraints.

        Dual sql + business format
        --------------------------
        Row rules intentionally carry **both** a ``sql`` field and a structured
        semantic block (``accepted_values`` / ``range``).  They serve different
        consumers:

        * ``sql`` — executed at runtime by the validation engine.
        * ``accepted_values`` / ``range`` — machine-readable business semantics
          consumed by contract registries, data catalogues, and documentation
          generators.  The SQL can be re-derived from these blocks; having both
          means each consumer takes what it needs without extra computation.
        """
        import re as _re

        row_rules: List[Dict[str, Any]] = []
        dataset_rules: List[Dict[str, Any]] = []
        total = len(df)
        if total == 0:
            return {}

        # ── Keyword-based column classification ──────────────────────────────
        # Columns matching these patterns get ONLY a not_null rule; the values
        # are identifiers, timestamps, or free text — range/enum rules add noise.
        _ID_LIKE = _re.compile(
            r"(^|_)(id|key|uuid|guid|hash|token|date|dt|ts|timestamp|desc|description)($|_)",
            _re.IGNORECASE,
        )
        # Numeric columns matching these patterns are treated as enums
        # (accepted_values), not continuous ranges.
        _ENUM_LIKE = _re.compile(
            r"(^|_)(status|type|option|flag|category|mode|kind|class|tier|level|grade|code)($|_)",
            _re.IGNORECASE,
        )

        def _is_id_like(col: str) -> bool:
            return bool(_ID_LIKE.search(col))

        def _is_enum_like(col: str) -> bool:
            return bool(_ENUM_LIKE.search(col))

        for col_name in df.columns:
            series = df[col_name]
            non_null = series.drop_nulls()
            null_ratio = 1.0 - len(non_null) / total
            n_unique = non_null.n_unique() if not non_null.is_empty() else 0
            dtype_name = series.dtype.__class__.__name__.lower()
            is_string = dtype_name in ("utf8", "string", "categorical", "large_utf8", "enum")
            is_numeric = dtype_name in (
                "int8", "int16", "int32", "int64",
                "uint8", "uint16", "uint32", "uint64",
                "float32", "float64", "decimal",
            )
            skip_domain_rules = _is_id_like(col_name)   # only not_null for id/date/desc cols
            prefer_enum = _is_enum_like(col_name)        # status/type/option → accepted_values

            # ── not_null ───────────────────────────────────────────────────
            if null_ratio < 0.01:
                row_rules.append({
                    "name": f"{col_name}_not_null",
                    "sql": f"{col_name} IS NOT NULL",
                    "category": "completeness",
                })

            if skip_domain_rules:
                # id/date/desc columns: existence check is enough
                continue

            # ── unique (dataset-level) ─────────────────────────────────────
            if n_unique == total and total > 1:
                dataset_rules.append({
                    "name": f"{col_name}_unique",
                    "sql": f"{col_name}",
                    "category": "uniqueness",
                })

            # ── accepted_values ────────────────────────────────────────────
            # String columns with low cardinality, OR numeric columns whose
            # name signals an enumerated value (status, type, option, …).
            #
            # Cardinality guard: how many distinct values is "few" depends on
            # how many rows we have.  With a tiny sample (e.g. 2-3 rows) nearly
            # every column looks unique, so accepted_values would pin the rule
            # to sample artefacts.  We use a two-tier threshold:
            #   • tiny samples (total < 10): only emit if n_unique <= 5
            #   • normal samples (total >= 10): standard ceiling of 20
            #   In both cases n_unique must be < 50% of total so columns that
            #   happen to be fully unique in the sample don't get pinned.
            #
            # JSON guard: skip if sampled values look like serialised dicts/arrays
            # — those are structured data fields (coordinates, nested objects),
            # not categorical enums.

            def _is_json_values(series_nonnull) -> bool:
                """True when the majority of sample values are JSON objects/arrays."""
                sample = series_nonnull.head(10).to_list()
                hits = sum(
                    1 for v in sample
                    if isinstance(v, str) and v.strip()[:1] in ("{", "[")
                )
                return len(sample) > 0 and hits / len(sample) >= 0.5

            _cardinality_ceil = 5 if total < 10 else 20
            _is_categorical = (
                0 < n_unique <= _cardinality_ceil
                and n_unique < max(1, total * 0.5)   # not unique within sample
            )

            if is_string and _is_categorical and not _is_json_values(non_null):
                values = sorted(str(v) for v in non_null.unique().to_list())
                # sql + structured block: sql is for the engine; accepted_values
                # is the business-readable declaration (see docstring above).
                row_rules.append({
                    "name": f"valid_{col_name}",
                    "sql": f"{col_name} IN ({', '.join(repr(v) for v in values)})",
                    "category": "validity",
                    "accepted_values": {
                        "field": col_name,
                        "values": values,
                    },
                })
            elif is_numeric and prefer_enum and 0 < n_unique <= 20 and not non_null.is_empty():
                # Treat enum-like numeric columns as accepted_values, not range
                values = sorted(non_null.unique().to_list())
                row_rules.append({
                    "name": f"valid_{col_name}",
                    "sql": f"{col_name} IN ({', '.join(str(v) for v in values)})",
                    "category": "validity",
                    "accepted_values": {
                        "field": col_name,
                        "values": values,
                    },
                })

            # ── range ──────────────────────────────────────────────────────
            # Continuous numeric columns only; skip if already handled as enum.
            elif is_numeric and not prefer_enum and not non_null.is_empty():
                col_min = non_null.min()
                col_max = non_null.max()
                if col_min is not None and col_max is not None:
                    row_rules.append({
                        "name": f"valid_{col_name}_range",
                        "sql": f"{col_name} BETWEEN {col_min} AND {col_max}",
                        "category": "validity",
                        "range": {
                            "field": col_name,
                            "min": col_min,
                            "max": col_max,
                        },
                    })

        result: Dict[str, Any] = {}
        if row_rules:
            result["row_rules"] = row_rules
        if dataset_rules:
            result["dataset_rules"] = dataset_rules
        return result

    def _detect_pii(self, pd_df) -> Dict[str, str]:
        """Run Presidio PII detection on sampled column values."""
        try:
            from presidio_analyzer import AnalyzerEngine
        except ImportError as exc:
            raise ImportError(
                "PII detection requires presidio-analyzer: "
                "pip install lakelogic[profiling]"
            ) from exc

        analyzer = AnalyzerEngine()
        pii_map: Dict[str, str] = {}
        for col in pd_df.columns:
            sample = pd_df[col].dropna().astype(str).head(self.pii_sample_size)
            found: List[str] = []
            for value in sample.tolist():
                try:
                    results = analyzer.analyze(text=value, language="en")
                except Exception:
                    results = []
                for r in results:
                    found.append(r.entity_type)
            if found:
                pii_map[col] = max(set(found), key=found.count)
        return pii_map


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------


def infer_contract(
    source: Any,
    *,
    title: Optional[str] = None,
    version: str = "1.0.0",
    description: Optional[str] = None,
    layer: str = "bronze",
    domain: Optional[str] = None,
    system: Optional[str] = None,
    sample_rows: int = 10_000,
    suggest_rules: bool = True,
    detect_pii: bool = False,
    pii_sample_size: int = 50,
    extra: Optional[Dict[str, Any]] = None,
    preserve_nested: Union[bool, List[str]] = False,
) -> ContractDraft:
    """
    Infer a LakeLogic contract from an existing data file — no contract needed upfront.

    Reads the file (or DataFrame), infers column names and types, optionally suggests
    quality rules, and returns a :class:`ContractDraft` object that can be saved to
    YAML, printed, or immediately turned into a :class:`DataGenerator`.

    Parameters
    ----------
    source : str | Path | polars.DataFrame | pandas.DataFrame
        Data source.  Accepts file paths (CSV, Parquet, JSON, NDJSON, Excel) or
        an existing in-memory DataFrame.
    title : str, optional
        Override the auto-derived contract title (defaults to the file stem).
    version : str
        Contract version string (default ``"1.0.0"``).
    description : str, optional
        Contract description (auto-generated when omitted).
    layer : str
        Data layer hint written to ``info.target_layer`` (default ``"bronze"``).
    sample_rows : int
        Max rows to read from the file for inference (default 10 000).
    suggest_rules : bool
        Auto-suggest quality rules from the data (default True):

        - ``not_null``       — columns with < 1% nulls
        - ``unique``         — columns where all values are distinct
        - ``accepted_values`` — string columns with ≤ 20 distinct values
        - ``range``          — numeric columns (observed min/max boundaries)

    detect_pii : bool
        Flag PII fields using Presidio (default False).
        Requires ``pip install lakelogic[profiling]``.
    extra : dict, optional
        Extra top-level sections added to the contract (e.g. ``lineage``,
        ``owner``, ``tags``, ``sla``).  These are merged in after the standard
        auto-generated sections, so they can also be used to override defaults.

        Example::

            draft = infer_contract(
                "data/10001.json",
                title="Zoopla Listing",
                layer="bronze",
                extra={
                    "lineage": {
                        "source_system": "Zoopla API",
                        "upstream": ["raw.zoopla_listings_api"],
                        "pipeline": "bronze_zoopla_ingest",
                    },
                    "owner": {"team": "data-eng", "slack": "#data-platform"},
                    "tags": ["real-estate", "bronze", "api"],
                },
            )

    -------
    ContractDraft
        An object with ``.show()``, ``.save(path)``, ``.to_yaml()``,
        ``.to_dict()``, and ``.to_generator()`` methods.

    Examples
    --------
    Infer from a CSV and save::

        from lakelogic import infer_contract

        draft = infer_contract("data/orders.csv", suggest_rules=True)
        draft.show()                          # inspect YAML
        draft.save("contracts/orders.yaml")   # write to disk

    Chain directly into DataGenerator::

        gen = infer_contract("data/orders.csv").to_generator(seed=42)
        df  = gen.generate(rows=5_000)

    Infer from a single JSON record file::

        draft = infer_contract("data/10001.json", layer="bronze",
                               title="Zoopla Listing")
        draft.save("contracts/bronze_zoopla.yaml")

    Pass an in-memory DataFrame::

        import polars as pl
        df_raw = pl.read_parquet("data/seed.parquet")
        draft  = infer_contract(df_raw, title="My Table")
        gen    = draft.to_generator()
    """
    return ContractInferrer(
        source,
        title=title,
        version=version,
        description=description,
        layer=layer,
        domain=domain,
        system=system,
        sample_rows=sample_rows,
        suggest_rules=suggest_rules,
        detect_pii=detect_pii,
        pii_sample_size=pii_sample_size,
        extra=extra,
        preserve_nested=preserve_nested,
    ).infer()
