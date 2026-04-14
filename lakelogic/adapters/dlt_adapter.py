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


# ---------------------------------------------------------------------------
# DltAdapter
# ---------------------------------------------------------------------------


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

    def extract(self, previous_state: Optional[str] = None) -> Any:
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

        # Run the pipeline to a temporary filesystem destination
        # but keep pipeline state (watermarks) persistent
        tmp_dir = self._get_tmp_dir()
        state_dir = Path.cwd() / ".lakelogic" / "dlt_pipelines"
        pipeline = dlt.pipeline(
            pipeline_name=f"lakelogic_{self.contract_name}",
            destination=dlt.destinations.filesystem(bucket_url=str(tmp_dir.absolute())),
            dataset_name=self.contract_name,
            pipelines_dir=str(state_dir.absolute()),
        )

        if previous_state:
            import json

            try:
                pipeline.state.update(json.loads(previous_state))
                logger.info("Restored dlt verified source extraction state from lakelogic run logs.")
            except Exception as e:
                logger.warning(f"Failed to restore dlt state: {e}")

        pipeline.run(
            source,
            loader_file_format="parquet",
            dataset_name=self.contract_name,
        )

        # Extract state to pass up to LakeLogic
        import json

        self.dlt_state_json = json.dumps(dict(pipeline.state), default=str)

        return self._collect_parquet_files(tmp_dir, pipeline)

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
                "base_url": self.cfg.base_url,
            },
            "resources": self._build_resources(),
        }

        # Add authentication if credentials are present
        auth = self._build_auth(credentials)
        if auth:
            rest_config["client"]["auth"] = auth

        source = rest_api_source(rest_config)

        # Run the pipeline to a temporary filesystem destination
        # but keep pipeline state persistent
        tmp_dir = self._get_tmp_dir()
        state_dir = Path.cwd() / ".lakelogic" / "dlt_pipelines"
        pipeline = dlt.pipeline(
            pipeline_name=f"lakelogic_{self.contract_name}",
            destination=dlt.destinations.filesystem(bucket_url=str(tmp_dir.absolute())),
            dataset_name=self.contract_name,
            pipelines_dir=str(state_dir.absolute()),
        )

        if previous_state:
            import json

            try:
                pipeline.state.update(json.loads(previous_state))
                logger.info("Restored dlt extraction state from lakelogic run logs.")
            except Exception as e:
                logger.warning(f"Failed to restore dlt state: {e}")

        pipeline.run(
            source,
            loader_file_format="parquet",
            dataset_name=self.contract_name,
        )

        # Extract state to pass up to LakeLogic
        import json

        self.dlt_state_json = json.dumps(dict(pipeline.state), default=str)

        return self._collect_parquet_files(tmp_dir, pipeline)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_credentials(self) -> dict:
        """Resolve ``${ENV_VAR}`` references in credential values."""
        resolved: Dict[str, str] = {}
        for key, value in self.cfg.credentials.items():
            resolved_val = _resolve_env_value(value)
            if resolved_val is not None:
                resolved[key] = resolved_val
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
                resource["endpoint"]["params"] = ep.params
            if ep.paginator:
                resource["endpoint"]["paginator"] = ep.paginator
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
        parquet_files = list(tmp_dir.rglob("*.parquet"))

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
                    f"dlt: auto-casting columns with conflicting types to string: "
                    f"{', '.join(sorted(conflicts))}"
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
