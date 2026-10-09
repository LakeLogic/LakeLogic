"""Write a frame to any dlt destination (Postgres, SQL Server/Azure SQL, Snowflake, BigQuery, ...).

ONE writer for every dlt write the materializer makes: the primary target
(``materialization.format: dlt``), each ``secondary_targets`` entry, and
``write_to_secondary_targets``. Before this module each path built its own pipeline, and the
primary path read its settings from an attribute nothing ever set, so ``format: dlt`` ignored
``dlt_destination`` and wrote to a local DuckDB file while reporting success.

Credentials are never meant to sit in a contract. ``dlt_credentials`` (a string, or a mapping of
strings) may be:

* ``env:VAR`` / ``${ENV:VAR}`` — an environment variable;
* ``env://VAR`` / ``keyvault://vault/secret`` / ``databricks://scope/key`` — the same resolvers
  ``pii_vault`` uses (``vault_resolver``);
* omitted — dlt then reads ``DESTINATION__<NAME>__CREDENTIALS`` (env or ``secrets.toml``);
* a literal — accepted, but logged as a warning.

Strategies map onto dlt write dispositions: ``append`` → append, ``overwrite`` → replace,
``merge`` → merge (needs a primary key). Anything else (``scd2``, ``fact``...) is refused rather
than silently appended.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Mapping, Optional

from loguru import logger

#: Destinations that need no credentials (local files or managed tokens).
LOCAL_DESTINATIONS = {"duckdb", "filesystem", "motherduck", "weaviate"}

#: Contract keys that configure the writer itself; every other ``dlt_*`` key is passed to the
#: destination factory with the prefix removed (e.g. ``dlt_create_indexes: true``).
_RESERVED = {"dlt_destination", "dlt_credentials", "dlt_dataset_name", "dlt_pipeline_name"}

#: Destinations that store list/struct columns natively. Elsewhere (Postgres, SQL Server, ...)
#: dlt's loader cannot write them — e.g. quarantine's ``_lakelogic_errors`` list — so they are
#: written as JSON text.
NESTED_DESTINATIONS = {"duckdb", "motherduck", "bigquery", "snowflake", "databricks", "filesystem", "clickhouse"}

_DISPOSITIONS = {"append": "append", "overwrite": "replace", "replace": "replace", "merge": "merge"}

_SECRET_SCHEMES = ("env://", "keyvault://", "databricks://", "vault://", "aws-kms://")


class DltNotInstalled(ImportError, ValueError):
    """dlt (or a destination's driver) is not installed. Both an ImportError and a ValueError,
    so callers that caught either before this module existed still do."""


def write_disposition(strategy: Optional[str], primary_key: Optional[Iterable[str]]) -> str:
    """The dlt write disposition for a materialization strategy. Raises ValueError when the
    strategy has no dlt equivalent, or ``merge`` has no primary key."""
    s = (strategy or "append").lower()
    if s not in _DISPOSITIONS:
        raise ValueError(
            f"Strategy '{s}' cannot be written through dlt; use append, overwrite or merge "
            "(SCD2 and fact loads need a lakehouse format such as delta)."
        )
    if _DISPOSITIONS[s] == "merge" and not list(primary_key or []):
        raise ValueError("Strategy 'merge' through dlt needs the contract's primary_key.")
    return _DISPOSITIONS[s]


def _resolve_one(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    if value.startswith(_SECRET_SCHEMES):
        from lakelogic.core.vault_resolver import _resolve

        resolved = _resolve(value)
        if not resolved:
            raise ValueError(f"Could not resolve credentials from {value.split('://', 1)[0]}://")
        return resolved
    from lakelogic.core.materialization import _resolve_env_value

    resolved = _resolve_env_value(value)
    if resolved is None:
        raise ValueError(f"Credentials reference '{value}' is not set in the environment.")
    return resolved


def resolve_credentials(raw: Any) -> Any:
    """Resolve a ``dlt_credentials`` value (string or mapping) to what dlt accepts."""
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        return {k: _resolve_one(v) for k, v in raw.items()}
    return _resolve_one(raw)


def _is_reference(raw: Any) -> bool:
    if isinstance(raw, Mapping):
        return all(_is_reference(v) for k, v in raw.items() if k in ("password", "credentials", "connection_string"))
    return isinstance(raw, str) and raw.startswith(_SECRET_SCHEMES + ("env:", "${ENV:"))


def to_arrow(df: Any):
    """A pyarrow Table from a polars / pandas / pyarrow / Spark frame."""
    import pyarrow as pa

    if isinstance(df, pa.Table):
        return df
    if hasattr(df, "to_arrow"):  # polars
        return df.to_arrow()
    if hasattr(df, "toArrow"):  # Spark 4
        return df.toArrow()
    if hasattr(df, "toPandas"):  # Spark 3
        return pa.Table.from_pandas(df.toPandas(), preserve_index=False)
    if hasattr(df, "arrow") and callable(df.arrow):  # DuckDB relation
        return df.arrow()
    if hasattr(df, "columns"):  # pandas
        return pa.Table.from_pandas(df, preserve_index=False)
    raise TypeError(f"Cannot write a {type(df).__name__} through dlt")


def nested_to_json(table):
    """Replace every list / struct / map column with its JSON text; other columns untouched."""
    import json

    import pyarrow as pa

    for i, field in enumerate(table.schema):
        t = field.type
        if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_struct(t) or pa.types.is_map(t):
            values = [None if v is None else json.dumps(v, default=str) for v in table.column(i).to_pylist()]
            table = table.set_column(i, pa.field(field.name, pa.string()), pa.array(values, type=pa.string()))
    return table


def _scrub(message: str, secrets: Iterable[Any]) -> str:
    for s in secrets:
        if isinstance(s, str) and len(s) >= 4:
            message = message.replace(s, "***")
    # Passwords inside connection URLs: scheme://user:PASSWORD@host
    return re.sub(r"(://[^:/@\s]+:)[^@\s]+@", r"\1***@", message)


def _destination(dlt: Any, name: str, config: Mapping[str, Any], credentials: Any) -> Any:
    dest_kwargs: Dict[str, Any] = {k[4:]: v for k, v in config.items() if k.startswith("dlt_") and k not in _RESERVED}
    if credentials is not None:
        dest_kwargs["credentials"] = credentials
    factory = getattr(dlt.destinations, name, None)
    return factory(**dest_kwargs) if (factory is not None and dest_kwargs) else name


def destination_for(config: Mapping[str, Any]) -> Any:
    """The dlt destination ``config`` describes, credentials resolved — for callers that need
    the destination itself (e.g. to probe for an existing dataset) before writing."""
    import dlt

    name = str(config.get("dlt_destination") or "duckdb")
    return _destination(dlt, name, config, resolve_credentials(config.get("dlt_credentials")))


def pipeline_name(config: Mapping[str, Any], destination: str, dataset_name: str, table: str) -> str:
    """One dlt pipeline (and local state folder) per destination + dataset + table.

    A shared name let a DuckDB run and a Postgres run of the same table reuse one state folder,
    so a load left by one blocked the other."""
    base = str(config.get("dlt_pipeline_name") or f"lakelogic_{dataset_name}_{table}")
    return re.sub(r"[^a-zA-Z0-9_]", "_", f"{base}_{destination}")


def write_dlt(
    df: Any,
    *,
    table_name: str,
    config: Mapping[str, Any],
    strategy: Optional[str] = "append",
    primary_key: Optional[Iterable[str]] = None,
    require_credentials: bool = True,
) -> Dict[str, Any]:
    """Write ``df`` to the dlt destination ``config`` describes. Returns
    ``{"target", "format", "dlt_destination", "rows_written", "write_disposition"}``.

    ``config`` holds the contract keys: ``dlt_destination`` (default duckdb),
    ``dlt_credentials``, ``dlt_dataset_name`` (default ``lakelogic``), ``dlt_pipeline_name`` and
    any other ``dlt_*`` destination option. Raises ValueError with secrets scrubbed.

    ``require_credentials=False`` (a secondary target with ``fail_on_error: false``) only warns
    when no credentials are found, and lets dlt try its own ``secrets.toml``."""
    try:
        import dlt
    except ImportError as exc:
        raise DltNotInstalled(
            "dlt materialization requires the 'dlt' package and the destination's extras, "
            "e.g. pip install 'lakelogic[dlt-postgres]' or 'lakelogic[dlt-mssql]'."
        ) from exc

    destination = str(config.get("dlt_destination") or "duckdb")
    dataset_name = str(config.get("dlt_dataset_name") or "lakelogic")
    raw_credentials = config.get("dlt_credentials")
    pk = list(primary_key or [])
    disposition = write_disposition(strategy, pk)
    table = re.sub(r"[^a-zA-Z0-9_]", "_", str(table_name or "data")) or "data"

    if raw_credentials is not None and destination not in LOCAL_DESTINATIONS and not _is_reference(raw_credentials):
        logger.warning(
            f"dlt target {destination}: credentials are written in the contract; "
            "use env:VAR or keyvault://vault/secret instead."
        )
    credentials = resolve_credentials(raw_credentials)
    if credentials is None and destination not in LOCAL_DESTINATIONS:
        import os

        env_key = f"DESTINATION__{destination.upper()}__CREDENTIALS"
        if not os.environ.get(env_key):
            msg = (
                f"No credentials for dlt destination '{destination}': set dlt_credentials "
                f"(env:VAR or keyvault://...) or the {env_key} environment variable."
            )
            if require_credentials:
                raise ValueError(msg)
            logger.warning(f"{msg} Trying dlt's own configuration.")

    dest = _destination(dlt, destination, config, credentials)

    data = to_arrow(df)
    if destination not in NESTED_DESTINATIONS:
        data = nested_to_json(data)

    @dlt.resource(name=table, write_disposition=disposition, primary_key=pk or None)
    def _rows():
        yield data

    try:
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name(config, destination, dataset_name, table),
            destination=dest,
            dataset_name=dataset_name,
        )
        # dlt keeps a failed load on disk and, on the next run, retries it and IGNORES the new
        # data. Every LakeLogic write carries its whole batch, so a stale package is only ever
        # wrong: drop it before writing.
        if getattr(pipeline, "has_pending_data", False):
            logger.warning(f"dlt pipeline {pipeline.pipeline_name}: dropping a load left by an earlier failed run")
            pipeline.drop_pending_packages()
        pipeline.run(_rows())
    except Exception as exc:
        secrets = [credentials] if isinstance(credentials, str) else list((credentials or {}).values())
        raise ValueError(f"dlt write to {destination} failed: {_scrub(str(exc), secrets)}") from None

    logger.info(f"dlt write: {data.num_rows} rows → {destination}:{dataset_name}.{table} ({disposition})")
    return {
        "target": f"{destination}:{dataset_name}.{table}",
        "format": "dlt",
        "dlt_destination": destination,
        "rows_written": data.num_rows,
        "write_disposition": disposition,
    }
