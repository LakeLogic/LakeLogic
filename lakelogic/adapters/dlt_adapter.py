"""
Contract-driven dlt pipeline executor.

Translates LakeLogic Data Contract YAML into dlt pipeline configuration,
extracts data as Arrow tables, and hands off to the Polars validation engine.

Two source modes are supported:

Mode 1 — Verified Source::

    source:
      type: dlt
      dlt:
        source: stripe_analytics
        resource: charges
        credentials:
          api_key: ${STRIPE_API_KEY}

Mode 2 — Declarative REST API::

    source:
      type: dlt
      dlt:
        base_url: https://api.example.com/v1/
        credentials:
          api_key: ${API_KEY}
        endpoints:
          - name: users
            path: users
            params:
              limit: 100

Install with: ``pip install lakelogic[dlt]``
"""

from __future__ import annotations

import importlib
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

# ---------------------------------------------------------------------------
# Credential resolution (reuses LakeLogic's ${ENV_VAR} pattern)
# ---------------------------------------------------------------------------

_ENV_RE = re.compile(r"^\$\{(\w+)\}$")


def _resolve_env_value(value: Optional[str]) -> Optional[str]:
    """Resolve ``${ENV_VAR}`` references to environment variable values.

    Mirrors the same helper used across LakeLogic's Snowflake, BigQuery,
    materialization, and quarantine modules.
    """
    if value is None:
        return None
    m = _ENV_RE.match(str(value).strip())
    if m:
        env_name = m.group(1)
        resolved = os.environ.get(env_name)
        if resolved is None:
            logger.warning(f"dlt credential: ${{{env_name}}} is not set in environment")
        return resolved
    return str(value)


_PARAM_RE = re.compile(r"\$\{(\w+)\}")


def _fill_params(value: Any) -> Any:
    """``${NAME}`` anywhere in a string, filled from the environment - how a pipeline passes a
    parameter (Databricks job parameters, Fabric notebook parameters, env). ``{incremental.*}``
    placeholders are dlt's and are left alone. A parameter that is not set is an error: running
    against ``https://${TENANT}.example.com`` would call a wrong address."""
    if not isinstance(value, str) or "${" not in value:
        return value

    def sub(m: "re.Match[str]") -> str:
        name = m.group(1)
        got = os.environ.get(name)
        if got is None:
            raise ValueError(f"the contract needs the parameter {name}; set it for this run")
        return got

    return _PARAM_RE.sub(sub, value)


# ---------------------------------------------------------------------------
# DltAdapter
# ---------------------------------------------------------------------------



def _option(model: Any, key: str) -> Any:
    """An option of a contract model as a plain value: a typed field (OLC >= 0.29 types the dlt
    endpoint's ``incremental``, ``paginator_config``, ``data_selector``, ``primary_key`` and the
    source's ``auth``) or, on an older OLC, the same key in the model's extras. A nested model
    comes back as a dict, the shape dlt takes."""
    fields = getattr(type(model), "model_fields", {}) or {}
    value = getattr(model, key, None) if key in fields else (getattr(model, "model_extra", None) or {}).get(key)
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_none=True)
    return value
class DltAdapter:
    """Contract-driven dlt pipeline executor.

    Translates the ``DltSourceConfig`` from a LakeLogic Data Contract into
    a ``dlt`` pipeline and returns the extracted data as a ``pyarrow.Table``.

    Parameters
    ----------
    source_config
        The ``SourceConfig`` from the parsed Data Contract.
    contract_name
        Title of the contract (used to name the dlt pipeline).
    """

    def __init__(self, source_config: Any, contract_name: str) -> None:
        self.source_config = source_config
        self.cfg = source_config.dlt
        self.contract_name = contract_name
        self._tmp_dir: Optional[Path] = None

        if self.cfg is None:
            raise ValueError("SourceConfig.dlt is None — ensure the contract has a 'dlt' block under 'source'.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self, previous_state: Optional[str] = None, start_from: Optional[str] = None) -> Any:
        """Run the dlt pipeline and return a ``pyarrow.Table``.

        Raises
        ------
        ImportError
            When ``dlt`` is not installed.
        ValueError
            When neither ``source`` nor ``base_url`` is configured.
        """
        try:
            import dlt  # noqa: F401
        except ImportError:
            raise ImportError("dlt integration requires the dlt package. Install with: pip install lakelogic[dlt]")

        credentials = self._resolve_credentials()
        # A starting value given for THIS run (a backfill) replaces the saved position.
        self._start_from = str(start_from) if start_from else None
        if self._start_from:
            logger.info(f"dlt: this run starts from {self._start_from} (saved position not used)")
            previous_state = None

        if self.cfg.source:
            return self._run_verified_source(credentials, previous_state)
        elif self.cfg.base_url:
            return self._run_rest_api(credentials, previous_state)
        else:
            raise ValueError("dlt source must specify either 'source' (verified source) or 'base_url' (REST API mode)")

    # ------------------------------------------------------------------
    # Mode 1: Verified Source
    # ------------------------------------------------------------------

    def _run_verified_source(self, credentials: dict, previous_state: Optional[str] = None) -> Any:
        """Import and run a dlt verified source by name.

        Verified sources are scaffolded locally via ``dlt init <name> <dest>``
        and imported dynamically.
        """
        import dlt

        source_name = self.cfg.source
        resource_name = self.cfg.resource

        logger.info(
            f"dlt: running verified source '{source_name}'" + (f" resource='{resource_name}'" if resource_name else "")
        )

        # Dynamic import — the user must have scaffolded the source locally
        try:
            module = importlib.import_module(source_name)
        except ModuleNotFoundError:
            raise ImportError(
                f"dlt verified source '{source_name}' not found. "
                f"Scaffold it first with: dlt init {source_name} filesystem"
            )

        # Convention: the source function has the same name as the module
        source_fn = getattr(module, source_name, None)
        if source_fn is None:
            # Fallback: look for any dlt.source-decorated callable
            for attr_name in dir(module):
                obj = getattr(module, attr_name)
                if callable(obj) and hasattr(obj, "_dlt_source"):
                    source_fn = obj
                    break

        if source_fn is None:
            raise ValueError(
                f"Could not find a source function in module '{source_name}'. "
                f"Expected a function named '{source_name}' or a @dlt.source."
            )

        source = source_fn(**credentials)

        if resource_name:
            source = source.with_resources(resource_name)

        # The same run as the REST mode: a stable destination and run-log state that sticks.
        return self._run_pipeline(dlt, source, previous_state)

    # ------------------------------------------------------------------
    # Mode 2: Declarative REST API
    # ------------------------------------------------------------------

    def _run_rest_api(self, credentials: dict, previous_state: Optional[str] = None) -> Any:
        """Build and run a dlt REST API source from contract config."""
        import dlt
        from dlt.sources.rest_api import rest_api_source

        logger.info(f"dlt: running REST API source from {self.cfg.base_url}")

        rest_config: Dict[str, Any] = {
            "client": {
                # `${NAME}` is a pipeline parameter (e.g. the tenant URL per environment).
                "base_url": _fill_params(self.cfg.base_url),
            },
            "resources": self._build_resources(),
        }

        # Add authentication if credentials are present
        auth = self._build_auth(credentials)
        if auth:
            rest_config["client"]["auth"] = auth

        source = rest_api_source(rest_config)
        # How deep nested data is split into child tables. 0 keeps a nested object or list as
        # ONE JSON column on the record - what a Bronze copy should be; Silver unnests it.
        # A TEST reads a few pages only (Studio's Test stage against a live API): enough to
        # prove auth, paging and the columns without pulling the whole system.
        pages = getattr(self, "max_pages", None)
        if pages and hasattr(source, "add_limit"):
            source = source.add_limit(int(pages))
        nesting = getattr(self.cfg, "max_table_nesting", None)
        if nesting is not None and hasattr(source, "max_table_nesting"):
            source.max_table_nesting = int(nesting)
        # Only the endpoints' own tables are this contract's rows: child tables (nested lists
        # split out) live in other folders and must not be stacked onto the records.
        self._tables = [str(ep.name) for ep in (self.cfg.endpoints or [])]
        return self._run_pipeline(dlt, source, previous_state)

    def _run_pipeline(self, dlt: Any, source: Any, previous_state: Optional[str]) -> Any:
        """Run ``source`` into a STABLE local destination and return this run's rows.

        Two things made incremental loading start from scratch on every run (2026-11-17):

        * a NEW temporary destination folder each run - dlt keeps incremental state with the
          destination, so a fresh folder reset it to ``initial_value``;
        * the run-log state was written to ``pipeline.state``, a read-only COPY, so it never
          reached the run. On Databricks/Fabric the job's disk is temporary and the run log is
          the only memory between runs, so this lost every cursor there.

        Now the destination folder is fixed per pipeline (only the last run's data files are
        cleared, never dlt's own ``_dlt*`` tables), and run-log state is written through
        ``managed_state`` - the run log wins, as the record of the last successful run.
        """
        import json
        import shutil

        name = f"lakelogic_{self.contract_name}"
        state_dir = Path.cwd() / ".lakelogic" / "dlt_pipelines"
        dest = state_dir / f"{name}__extract"
        data_root = dest / self.contract_name
        if data_root.exists():
            # Only the previous run's TABLE folders go. dlt's own `_dlt*` tables and the files at
            # the dataset root (its `init` marker) stay - without the marker dlt re-creates the
            # folder and fails on Windows ("file already exists").
            for child in data_root.iterdir():
                if child.is_dir() and not child.name.startswith("_dlt"):
                    shutil.rmtree(child, ignore_errors=True)
        self._tmp_dir = dest
        pipeline = dlt.pipeline(
            pipeline_name=name,
            destination=dlt.destinations.filesystem(bucket_url=str(dest.absolute())),
            dataset_name=self.contract_name,
            pipelines_dir=str(state_dir.absolute()),
        )

        if getattr(self, "_start_from", None):
            # A backfill: the saved position must go, or dlt prefers it over the start given.
            with pipeline.managed_state() as state:
                state["sources"] = {}
        elif previous_state:
            try:
                prev_sources = (json.loads(previous_state) or {}).get("sources") or {}
                if prev_sources:
                    with pipeline.managed_state() as state:
                        state["sources"] = prev_sources
                    logger.info("Restored dlt extraction state from lakelogic run logs.")
            except Exception as e:
                logger.warning(f"Failed to restore dlt state: {e}")

        # The folder dlt actually writes: it normalises the dataset name (`Bronze — X` ->
        # `bronze_x`), so clearing by the raw title cleared nothing and a second run re-read the
        # first run's rows beside its own (26 rows for 1 change, Frappe HR, 2026-11-17).
        try:
            real_root = dest / str(pipeline.dataset_name)
            if real_root != data_root and real_root.exists():
                for child in real_root.iterdir():
                    if child.is_dir() and not child.name.startswith("_dlt"):
                        shutil.rmtree(child, ignore_errors=True)
        except Exception:  # noqa: BLE001 - the load-id filter below is the guarantee
            pass

        load_info = pipeline.run(
            source,
            loader_file_format="parquet",
            dataset_name=self.contract_name,
        )
        # ...and only THIS run's files are read, whatever is left in the folder.
        self._load_ids = [str(x) for x in (getattr(load_info, "loads_ids", None) or [])]

        # Extract state to pass up to LakeLogic
        self.dlt_state_json = json.dumps(dict(pipeline.state), default=str)

        return self._collect_parquet_files(dest, pipeline)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_credentials(self) -> dict:
        """Resolve ``${ENV_VAR}`` references in credential values
        and dynamically map any ``dlt_*`` root-level properties.
        """
        resolved: Dict[str, Any] = {}

        # 1. Resolve explicitly nested credentials
        for key, value in self.cfg.credentials.items():
            resolved_val = _resolve_env_value(value)
            if resolved_val is not None:
                resolved[key] = resolved_val

        # 2. Merge root-level dlt_* properties (stripping the prefix)
        if hasattr(self.cfg, "model_extra") and self.cfg.model_extra:
            for key, value in self.cfg.model_extra.items():
                if key.startswith("dlt_"):
                    target_key = key[4:]  # Strip the 'dlt_' prefix
                    resolved_val = _resolve_env_value(value)
                    if resolved_val is not None:
                        resolved[target_key] = resolved_val

        return resolved

    def _build_auth(self, credentials: dict) -> Optional[dict]:
        """Map credential keys to dlt auth config.

        Supports common patterns:
        - ``api_key`` → bearer token
        - ``token`` → bearer token
        - ``username`` + ``password`` → HTTP basic auth
        """
        if not credentials:
            return None

        # An explicit ``auth`` block names the scheme and, for an API key, WHERE it goes:
        #   auth: {type: api_key, name: Authorization, location: header}
        # The secret itself still comes from ``credentials`` (an ${ENV} reference). Without it
        # an API key was always sent as dlt's default, which suits few HR systems.
        explicit = _option(self.cfg, "auth")
        if isinstance(explicit, dict) and explicit.get("type"):
            kind = str(explicit["type"])
            if kind == "api_key" and "api_key" in credentials:
                out = {"type": "api_key", "api_key": credentials["api_key"]}
                for key in ("name", "location"):
                    if explicit.get(key):
                        out[key] = str(explicit[key])
                return out
            if kind == "bearer" and ("token" in credentials or "api_key" in credentials):
                return {"type": "bearer", "token": credentials.get("token") or credentials["api_key"]}
            if kind == "http_basic" and "username" in credentials and "password" in credentials:
                return {"type": "http_basic", "username": credentials["username"],
                        "password": credentials["password"]}
            if kind == "oauth2_client_credentials" and {"client_id", "client_secret"} <= set(credentials):
                return {
                    "type": "oauth2_client_credentials",
                    "access_token_url": str(explicit.get("access_token_url") or ""),
                    "client_id": credentials["client_id"],
                    "client_secret": credentials["client_secret"],
                }

        if "api_key" in credentials:
            return {"type": "api_key", "api_key": credentials["api_key"]}
        elif "token" in credentials:
            return {"type": "bearer", "token": credentials["token"]}
        elif "username" in credentials and "password" in credentials:
            return {
                "type": "http_basic",
                "username": credentials["username"],
                "password": credentials["password"],
            }
        return None

    def _build_resources(self) -> List[dict]:
        """Convert contract endpoint configs to dlt resource format."""
        if not self.cfg.endpoints:
            raise ValueError("REST API mode requires at least one endpoint in 'dlt.endpoints'")

        resources = []
        for ep in self.cfg.endpoints:
            resource: Dict[str, Any] = {
                "name": ep.name,
                "endpoint": {
                    "path": ep.path,
                },
            }
            if ep.params:
                resource["endpoint"]["params"] = {k: _fill_params(v) for k, v in ep.params.items()}
            # Options the endpoint carries beyond name/path/params, passed straight to dlt:
            #   paginator_config  a paginator WITH settings (dlt's dict form), e.g.
            #                     {type: offset, limit: 100, offset_param: limit_start,
            #                      limit_param: limit_page_length, total_path: null}
            #   data_selector     where the records sit in the response, e.g. "data"
            #   incremental       {cursor_path, initial_value[, start_param]} - only rows newer
            #                     than the last run; a param may use {incremental.start_value}
            #   primary_key       the record key, for merge loads
            extra = {k: _option(ep, k) for k in ("paginator_config", "data_selector", "incremental", "primary_key")}
            paginator = extra.get("paginator_config") or ep.paginator
            if paginator:
                resource["endpoint"]["paginator"] = paginator
            if extra.get("data_selector"):
                resource["endpoint"]["data_selector"] = extra["data_selector"]
            if isinstance(extra.get("incremental"), dict):
                inc = {k: _fill_params(v) for k, v in extra["incremental"].items()}
                # This run's starting value (a backfill) wins over the contract's first-run default.
                if getattr(self, "_start_from", None):
                    inc["initial_value"] = self._start_from
                resource["endpoint"]["incremental"] = inc
            if extra.get("primary_key"):
                resource["primary_key"] = extra["primary_key"]
            resources.append(resource)

        return resources

    def _get_tmp_dir(self) -> Path:
        """Create and return a temporary directory for dlt pipeline state."""
        if self._tmp_dir is None:
            self._tmp_dir = Path(tempfile.mkdtemp(prefix=f"lakelogic_dlt_{self.contract_name}_"))
        return self._tmp_dir

    def _collect_parquet_files(self, tmp_dir: Path, pipeline: Any) -> Any:
        """Read all parquet files written by the dlt pipeline and return a
        unified ``pyarrow.Table``.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        # dlt filesystem destination writes parquet files in subdirectories
        # ...skipping dlt's own `_dlt*` tables (state, loads, versions), which stay in the
        # stable destination between runs and are not the source's rows.
        parquet_files = [
            f for f in tmp_dir.rglob("*.parquet")
            if not any(part.startswith("_dlt") for part in f.relative_to(tmp_dir).parts)
        ]
        # REST mode knows its tables: keep only theirs (a child table's folder is
        # `<table>__<child>`, so the exact folder name is required).
        wanted = set(getattr(self, "_tables", None) or [])
        if wanted:
            parquet_files = [f for f in parquet_files if f.parent.name in wanted]
        # Files are named `<load_id>.<file_id>.parquet`: keep this run's loads only.
        load_ids = [x for x in (getattr(self, "_load_ids", None) or []) if x]
        if load_ids:
            parquet_files = [f for f in parquet_files if any(f.name.startswith(f"{lid}.") for lid in load_ids)]

        if not parquet_files:
            logger.warning("dlt: pipeline produced no parquet files")
            return pa.table({})

        tables = [pq.read_table(f) for f in parquet_files]
        try:
            combined = pa.concat_tables(tables, promote_options="default")
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            # Type conflict across parquet files (e.g. one file infers a
            # column as int64/long while another infers it as string).
            # Resolve by detecting conflicts and casting to string — the
            # universal supertype that preserves all values.
            field_types: dict[str, pa.DataType] = {}
            conflicts: set[str] = set()
            for t in tables:
                for field in t.schema:
                    if field.name in field_types:
                        if field_types[field.name] != field.type:
                            conflicts.add(field.name)
                            field_types[field.name] = pa.string()
                    else:
                        field_types[field.name] = field.type
            if conflicts:
                logger.info(
                    f"dlt: auto-casting columns with conflicting types to string: {', '.join(sorted(conflicts))}"
                )
                cast_tables = []
                for t in tables:
                    casts = {}
                    for field in t.schema:
                        if field.name in conflicts and field.type != pa.string():
                            casts[field.name] = t.column(field.name).cast(pa.string())
                        else:
                            casts[field.name] = t.column(field.name)
                    cast_tables.append(pa.table(casts))
                combined = pa.concat_tables(cast_tables, promote_options="default")
            else:
                raise  # No conflicts found — re-raise original error

        logger.info(
            f"dlt: collected {len(parquet_files)} parquet file(s), "
            f"{combined.num_rows} rows, {combined.num_columns} columns"
        )

        return combined
