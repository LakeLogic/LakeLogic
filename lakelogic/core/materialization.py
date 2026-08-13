from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime, timezone
from uuid import uuid4
import os
import re
import shutil

from loguru import logger


def _is_remote_path(path) -> bool:
    """Return True if the path is a cloud storage URI (ADLS, S3, GCS).

    Delegates to :func:`lakelogic.core.paths.is_uri_path`.
    """
    from lakelogic.core.paths import is_uri_path

    return is_uri_path(str(path))


class URIPath:
    """Lightweight Path-compatible wrapper for cloud URIs and table: references.

    Prevents ``pathlib.Path`` from corrupting forward slashes to backslashes
    on Windows while supporting all Path-like operations used by the
    materialization pipeline (``/``, ``str()``, ``exists()``, ``mkdir()``, etc.).
    """

    def __init__(self, uri: str) -> None:
        self._uri = str(uri)

    # ── Path-like interface ──────────────────────────────────────────────────
    def __str__(self) -> str:
        return self._uri

    def __repr__(self) -> str:
        return f"URIPath({self._uri!r})"

    def __fspath__(self) -> str:
        return self._uri

    def __truediv__(self, other) -> "URIPath":
        # Join with forward slash (preserving URI format)
        return URIPath(self._uri.rstrip("/") + "/" + str(other).lstrip("/"))

    @property
    def suffix(self) -> str:
        last = self._uri.rsplit("/", 1)[-1]
        if "." in last:
            return "." + last.rsplit(".", 1)[-1]
        return ""

    @property
    def parent(self) -> "URIPath":
        if "/" in self._uri:
            return URIPath(self._uri.rsplit("/", 1)[0])
        return self

    @property
    def name(self) -> str:
        return self._uri.rsplit("/", 1)[-1]

    def exists(self) -> bool:
        return False  # Cloud paths require fsspec; assume not local

    def is_dir(self) -> bool:
        return False

    def mkdir(self, **kwargs) -> None:
        pass  # Cloud dirs are created on write

    def joinpath(self, *args) -> "URIPath":
        result = self
        for a in args:
            result = result / a
        return result

    def with_suffix(self, suffix: str) -> "URIPath":
        if self.suffix:
            base = self._uri[: -len(self.suffix)]
        else:
            base = self._uri
        return URIPath(base + suffix)


def _build_storage_options(
    storage_options: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, str]]:
    """
    Build cloud storage_options from environment variables if not provided.

    Supports three cloud providers:

    **Azure ADLS** (abfss://, abfs://):
        AZURE_STORAGE_ACCOUNT      — storage account name
        AZURE_CLIENT_ID            — service principal app/client ID
        AZURE_CLIENT_SECRET        — service principal secret
        AZURE_TENANT_ID            — Azure AD tenant ID
        AZURE_STORAGE_ACCOUNT_KEY  — (alternative) account key auth

    **AWS S3** (s3://, s3a://):
        AWS_ACCESS_KEY_ID      — IAM access key
        AWS_SECRET_ACCESS_KEY  — IAM secret key
        AWS_SESSION_TOKEN      — (optional) STS session token
        AWS_REGION             — (optional) AWS region

    **Google Cloud Storage** (gs://, gcs://):
        GOOGLE_SERVICE_ACCOUNT_KEY     — path to JSON service account key file
        GOOGLE_APPLICATION_CREDENTIALS — (alternative) path to JSON key file

    If ``storage_options`` is already provided, it is returned as-is.
    If required env vars are missing, returns None (local mode).
    """
    if storage_options:
        return storage_options

    opts: Dict[str, str] = {}

    # ── Azure ADLS ────────────────────────────────────────────────
    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    client = os.getenv("AZURE_CLIENT_ID")
    secret = os.getenv("AZURE_CLIENT_SECRET")
    tenant = os.getenv("AZURE_TENANT_ID")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

    if client and secret and tenant:
        opts = {
            "client_id": client,
            "client_secret": secret,
            "tenant_id": tenant,
        }
        if account:
            opts["account_name"] = account
        return opts
    if account_key:
        opts = {"account_key": account_key}
        if account:
            opts["account_name"] = account
        return opts


def _get_pyarrow_schema(dt) -> Any:
    """Robustly extract a native PyArrow Schema from a DeltaTable across all deltalake versions.

    - deltalake < 0.18: dt.schema().to_pyarrow() returns native pyarrow types.
    - deltalake >= 0.18: dt.schema().to_pyarrow() returns incompatible arro3 types,
      but dt.schema() implements the PyCapsule API (__arrow_c_schema__) so pa.schema() works.
    - dt.to_pyarrow_dataset().schema works in all versions but requires fsspec setup for remote.
    """
    import pyarrow as pa

    raw = dt.schema()

    if hasattr(raw, "to_pyarrow"):
        s = raw.to_pyarrow()
        if s and getattr(s.field(0).type, "__module__", "").startswith("pyarrow"):
            return s

    try:
        return pa.schema(raw)
    except TypeError:
        pass

    return dt.to_pyarrow_dataset().schema

    # ── AWS S3 ────────────────────────────────────────────────────
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    if aws_key and aws_secret:
        opts = {
            "AWS_ACCESS_KEY_ID": aws_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret,
        }
        aws_token = os.getenv("AWS_SESSION_TOKEN")
        if aws_token:
            opts["AWS_SESSION_TOKEN"] = aws_token
        aws_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        if aws_region:
            opts["AWS_REGION"] = aws_region
        return opts

    # ── Google Cloud Storage ──────────────────────────────────────
    gcs_key = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gcs_key:
        return {"service_account_key": gcs_key}

    return None


def _resolve_external_location(location: Optional[str]) -> Optional[str]:
    """
    Resolve an external location value, expanding env-var placeholders.

    Args:
        location: Raw location string, may use ``env:VAR`` or ``${ENV:VAR}`` syntax.

    Returns:
        Resolved location string, or None.
    """
    if not location:
        return None
    resolved = _resolve_env_value(location)
    return resolved if resolved else location


def _spark_save_as_table(  # pragma: no cover
    writer,
    table_name: str,
    mode: str,
    location: Optional[str] = None,
) -> None:
    """
    Write a Spark DataFrame to a Unity Catalog table, optionally at an external location.

    For external locations, data is written directly to the path via ``save()``
    then the table is registered in UC via ``CREATE TABLE ... USING DELTA LOCATION``.
    This avoids issues with ``saveAsTable`` + ``option("path")`` on Unity Catalog
    which fails when the table doesn't exist or has inconsistent metadata.

    Args:
        writer: Spark DataFrameWriter (already configured with format, partitionBy, etc.)
        table_name: Fully qualified UC table name (catalog.schema.table).
        mode: Spark write mode ('append', 'overwrite').
        location: Optional external storage URI.
    """
    if location:
        resolved_loc = _resolve_external_location(location)
        logger.debug(f"Writing to external location for `{table_name}`")

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        # Ensure the UC schema exists
        parts = table_name.split(".")
        if len(parts) >= 2:
            schema_ref = ".".join(parts[:-1])
            try:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")
            except Exception as e:
                logger.debug(f"Schema creation skipped: {e}")

        # Write data directly to the external storage path.
        # Disable deletion vectors at WRITE time so the Delta protocol never
        # advertises the `deletionVectors` reader feature — which blocks
        # delta-rs (Polars/DuckDB) with "reader features: {'deletionVectors'}
        # not yet supported". A writer-level option is honoured on serverless,
        # where the session-default conf
        # (spark.databricks.delta.properties.defaults.enableDeletionVectors) is
        # silently ignored.
        writer.option("delta.enableDeletionVectors", "false").mode(mode).save(resolved_loc)

        # Register (or confirm) the table in UC pointing to that location
        try:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{resolved_loc}'")
        except Exception as e:
            logger.debug(f"Table registration skipped (may already exist): {e}")
    else:
        # Same DV-disable for managed-table writes (see external branch above).
        writer.option("delta.enableDeletionVectors", "false").mode(mode).saveAsTable(table_name)


def _spark_apply_table_metadata(spark, table_name: str, contract) -> None:  # pragma: no cover
    """
    Apply column comments and table properties to a Spark/Databricks table.

    Reads field descriptions from ``contract.model.fields`` and emits
    ``ALTER TABLE ... ALTER COLUMN ... COMMENT`` for each field with a
    description.  Also applies ``materialization.table_properties`` as
    ``ALTER TABLE ... SET TBLPROPERTIES``.

    This is a best-effort operation; failures are logged but do not
    prevent the materialization from succeeding.

    Args:
        spark: SparkSession.
        table_name: Fully qualified table name.
        contract: DataContract instance.
    """
    # ── Column comments ─────────────────────────────────────────────────────
    if contract.model and contract.model.fields:
        for field in contract.model.fields:
            desc = getattr(field, "description", None)
            if desc:
                escaped = desc.replace("'", "\\'")
                sql = f"ALTER TABLE {table_name} ALTER COLUMN {field.name} COMMENT '{escaped}'"
                try:
                    spark.sql(sql)
                except Exception as exc:
                    logger.debug(f"Could not set column comment on {table_name}.{field.name}: {exc}")

    # ── Table properties ────────────────────────────────────────────────────
    mat = getattr(contract, "materialization", None)
    table_props = getattr(mat, "table_properties", None) if mat else None
    if table_props:
        props = ", ".join(f"'{k}' = '{v}'" for k, v in table_props.items())
        sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ({props})"
        try:
            spark.sql(sql)
            logger.info(f"Applied table properties to {table_name}: {list(table_props.keys())}")
        except Exception as exc:
            logger.debug(f"Could not set table properties on {table_name}: {exc}")


def _sanitize_arrow_nulls(table):
    """
    Replace any Arrow ``null``-typed columns with ``utf8`` (string) nulls.

    Delta Lake doesn't support the Arrow ``Null`` data type.  An all-NULL
    column can end up with this type when:
      - A Pandas DataFrame goes through ``pa.Table.from_pandas`` and the
        column has dtype ``object`` with only ``None`` values.
      - A DuckDB relation with ``CAST(NULL AS VARCHAR)`` is collected to
        Pandas first, losing the SQL type information.

    This is a lossless transformation: the values stay null, only the
    schema type changes so Delta's writer accepts them.
    """
    import pyarrow as pa

    new_fields = []
    changed = False
    for i, field in enumerate(table.schema):
        if pa.types.is_null(field.type):
            new_fields.append(field.with_type(pa.utf8()))
            changed = True
        else:
            new_fields.append(field)
    if not changed:
        return table
    new_schema = pa.schema(new_fields)
    return table.cast(new_schema)


def _safe_partition_value(value: Any) -> str:
    """
    Normalize partition values to safe filesystem-friendly strings.

    Args:
        value: Partition value.

    Returns:
        A sanitized string suitable for directory names.
    """
    if value is None:
        return "null"
    text = str(value)
    safe = text.replace(os.sep, "_").replace(" ", "_").replace(":", "_")
    if os.altsep:
        safe = safe.replace(os.altsep, "_")
    return safe


def _resolve_path(raw_path: str, base_path: Optional[Path]) -> Path:
    """
    Resolve a path, honoring the contract base path for relative values.

    Args:
        raw_path: Raw path string from the contract.
        base_path: Base path to resolve relative paths against.

    Returns:
        A resolved Path.
    """
    path = Path(raw_path)
    if not path.is_absolute() and base_path:
        path = base_path / path
    return path


_ENV_PATTERN = re.compile(r"^\${ENV:([A-Z0-9_]+)}$")


def _resolve_env_value(value: Optional[str]) -> Optional[str]:
    """
    Resolve simple environment variable placeholders.

    Args:
        value: Raw value or env placeholder (env:VAR or ${ENV:VAR}).

    Returns:
        Resolved value or None.
    """
    if value is None:
        return None
    if value.startswith("env:"):
        return os.getenv(value[4:].strip())
    match = _ENV_PATTERN.match(value)
    if match:
        return os.getenv(match.group(1))
    return value


def _resolve_target(contract, override_path: Optional[Path] = None) -> Tuple[Optional[Path], Optional[str]]:
    """
    Resolve the output target path and format from contract metadata.

    Args:
        contract: DataContract instance.
        override_path: Optional override path.

    Returns:
        Tuple of (target_path, output_format).
    """
    if override_path:
        target = override_path
    else:
        mat = contract.materialization if contract else None
        mat_path = None
        if mat is not None:
            mat_path = getattr(mat, "target_path", None) or getattr(mat, "path", None)
        # Resolve environment-aware server (respects LAKELOGIC_ENV / environments block)
        eff_server = contract.effective_server() if contract else None
        server_path = eff_server.path if eff_server else None
        target = mat_path or server_path

    if not target:
        return None, None

    # Storage targets are fully resolved by the registry at load time via
    # {storage_root}/{bronze_path}/etc placeholders — they're not relative
    # to the contract YAML's directory. `_base_path` is for contract-local
    # files (external scripts, fixtures). Passing it here used to prefix
    # CWD-relative storage paths like `./lakehouse_polars/...` with the
    # contract YAML dir, producing nonsense paths.
    target_str = str(target)
    if target_str.startswith("table:") or _is_remote_path(target_str):
        # Don't wrap cloud URIs or table: paths in Path() — it corrupts
        # forward slashes to backslashes on Windows.
        target_path = URIPath(target_str)
    else:
        target_path = _resolve_path(target_str, None)

    output_format = None
    if contract and contract.materialization:
        output_format = getattr(contract.materialization, "format", None)
    if not output_format:
        # Use environment-aware server format
        eff_server = contract.effective_server() if contract else None
        if eff_server and eff_server.format:
            output_format = eff_server.format
    if not output_format:
        output_format = "parquet"
    output_format = output_format.lower()

    return target_path, output_format


def _to_pandas(df: Any) -> Any:
    """
    Convert a dataframe-like object into a pandas DataFrame.

    Args:
        df: Engine dataframe.

    Returns:
        pandas.DataFrame
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for materialization output.") from exc

    if isinstance(df, pd.DataFrame):
        return df
    if isinstance(df, (list, tuple)):
        return pd.DataFrame(df)
    if isinstance(df, dict):
        return pd.DataFrame([df])
    if hasattr(df, "to_pandas"):
        return df.to_pandas()
    if hasattr(df, "toPandas"):
        return df.toPandas()
    if hasattr(df, "df"):
        return df.df()
    if hasattr(df, "collect"):
        collected = df.collect()
        if hasattr(collected, "to_pandas"):
            return collected.to_pandas()
    raise TypeError(f"Unsupported dataframe type for materialization: {type(df)}")


def _safe_write_deltalake(path, data, **kwargs):
    """Safely execute write_deltalake handling across v0.17+ and v1.0+ engine signatures."""
    try:
        from deltalake import write_deltalake
    except ImportError as e:
        raise ImportError("Delta materialization requires the deltalake package: pip install deltalake") from e

    # Empty dataframes initialized into new Delta tables do not need schema merging
    # and dropping it prevents 'rust' engine errors in older deltalake versions.
    if hasattr(data, "__len__") and len(data) == 0:
        kwargs.pop("schema_mode", None)
        kwargs.pop("engine", None)

    # Strip schema_mode=None — passing None explicitly can cause unexpected
    # behaviour in some deltalake versions; omitting the arg entirely lets
    # Delta-RS enforce strict schema matching (the desired default).
    if "schema_mode" in kwargs and kwargs["schema_mode"] is None:
        del kwargs["schema_mode"]

    import inspect

    sig = inspect.signature(write_deltalake)
    if "engine" in sig.parameters:
        if kwargs.get("schema_mode") == "merge":
            kwargs["engine"] = "rust"
    elif "engine" in kwargs:
        del kwargs["engine"]

    try:
        return write_deltalake(path, data, **kwargs)
    except Exception as e:
        err_msg = str(e).lower()

        # Handle "Cannot merge types X and Y" — PyArrow raises this during schema
        # unification when schema_mode='merge' is requested but column types conflict
        # (e.g. existing=string, incoming=int64/long). Cast incoming columns to match
        # the committed table schema and retry.
        if "cannot merge types" in err_msg:
            try:
                from deltalake import DeltaTable
                import pyarrow as pa

                dt = DeltaTable(path, storage_options=kwargs.get("storage_options"))
                existing_schema = _get_pyarrow_schema(dt)
                if hasattr(data, "schema"):
                    new_arrays = []
                    new_fields = []
                    cast_log = []
                    for i in range(len(data.schema)):
                        field = data.schema.field(i)
                        existing_idx = existing_schema.get_field_index(field.name)
                        if existing_idx >= 0 and field.type != existing_schema.field(existing_idx).type:
                            target_type = existing_schema.field(existing_idx).type
                            try:
                                new_arrays.append(data.column(i).cast(target_type))
                                new_fields.append(existing_schema.field(existing_idx))
                                cast_log.append(f"'{field.name}': {field.type}→{target_type}")
                            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as cast_err:
                                raise ValueError(
                                    f"Schema merge failed: column '{field.name}' has type "
                                    f"{field.type} in incoming data but {target_type} in the "
                                    f"existing table, and the cast is not safe. "
                                    f"Fix the source data or update schema_policy.evolution. "
                                    f"Cast error: {cast_err}"
                                ) from e
                        else:
                            new_arrays.append(data.column(i))
                            new_fields.append(field)
                    if cast_log:
                        logger.debug(f"Schema merge type conflict auto-resolved by casting: {', '.join(cast_log)}")
                        data = pa.table(
                            {f.name: arr for f, arr in zip(new_fields, new_arrays)},
                            schema=pa.schema(new_fields),
                        )
                        return write_deltalake(path, data, **kwargs)
            except ValueError:
                raise
            except Exception:
                pass  # Fall through to original error if resolution itself fails

        if "schema" in err_msg and ("mismatch" in err_msg or "does not match" in err_msg):
            try:
                from deltalake import DeltaTable

                dt = DeltaTable(path, storage_options=kwargs.get("storage_options"))
                existing_schema = _get_pyarrow_schema(dt)
                existing_cols = {f.name: str(f.type) for f in existing_schema}

                incoming_cols = {}
                if hasattr(data, "schema"):
                    for field in data.schema:
                        incoming_cols[field.name] = str(field.type)

                if incoming_cols and existing_cols:
                    new_cols = [c for c in incoming_cols if c not in existing_cols]
                    dropped_cols = [c for c in existing_cols if c not in incoming_cols]
                    type_mismatches = [
                        f"{c} (table: {existing_cols[c]}, data: {incoming_cols[c]})"
                        for c in incoming_cols
                        if c in existing_cols and existing_cols[c] != incoming_cols[c]
                    ]

                    diff_parts = []
                    if new_cols:
                        diff_parts.append(f"New columns in data not in table: {', '.join(new_cols)}")
                    if dropped_cols:
                        diff_parts.append(f"Columns in table missing from data: {', '.join(dropped_cols)}")
                    if type_mismatches:
                        diff_parts.append(f"Type mismatches: {', '.join(type_mismatches)}")

                    if diff_parts:
                        friendly_msg = " | ".join(diff_parts)
                        raise ValueError(
                            f"Schema Evolution Blocked (policy=strict): {friendly_msg}. Original error: {str(e)}"
                        ) from e
            except ValueError:
                raise  # Re-raise our enriched error
            except Exception:
                pass  # Fallback to original error if diff parsing itself fails
        raise e


def _write_frame(
    df,
    path,
    output_format: str,
    storage_options: Optional[Dict[str, str]] = None,
    mode_override: Optional[str] = None,
) -> None:
    """
    Write a DataFrame-like object to disk or remote storage (ADLS/S3/GCS).

    Args:
        df: pandas/polars DataFrame to write.
        path: Destination path.
        output_format: csv or parquet.
    """
    path_str = str(path)
    is_remote = _is_remote_path(path_str)
    opts = _build_storage_options(storage_options) if is_remote else None

    if output_format == "csv":
        if hasattr(df, "write_csv"):
            df.write_csv(path_str)
        elif hasattr(df, "to_csv"):
            df.to_csv(path_str, index=False)
        else:
            raise ValueError("Unsupported dataframe type for CSV materialization.")
    elif output_format == "parquet":
        if is_remote and hasattr(df, "write_parquet"):
            # Polars native write to remote (ADLS/S3/GCS) via storage_options
            logger.info(f"Writing Parquet to remote: {path_str}")
            df.write_parquet(path_str, storage_options=opts)
            return
        if hasattr(df, "write_parquet"):
            df.write_parquet(path)
        elif hasattr(df, "to_parquet"):
            try:
                df.to_parquet(path, index=False)
            except Exception as exc:
                # Fallback to DuckDB COPY for parquet without pyarrow/fastparquet.
                try:
                    import duckdb

                    owns_connection = False
                    con = None
                    if hasattr(df, "connection") and hasattr(df, "sql_query"):
                        con = df.connection
                    else:
                        con = duckdb.connect()
                        owns_connection = True

                    try:
                        if hasattr(df, "sql_query"):
                            con.execute(f"COPY ({df.sql_query()}) TO '{path}' (FORMAT PARQUET)")
                        else:
                            con.register("incoming_df", df)
                            con.execute(f"COPY incoming_df TO '{path}' (FORMAT PARQUET)")
                    finally:
                        if owns_connection and con is not None:
                            con.close()
                    return
                except Exception as duckdb_exc:
                    # Use a distinct name so we don't shadow the outer `exc` —
                    # the final `raise ... from exc` below needs the ORIGINAL
                    # to_parquet failure as the cause, not this fallback failure.
                    logger.debug(f"DuckDB parquet write fallback failed: {duckdb_exc}")
                try:
                    import polars as pl

                    pl.from_pandas(df).write_parquet(path)
                except Exception:
                    raise ValueError(
                        "Parquet materialization requires pyarrow/fastparquet, duckdb, or polars as a fallback."
                    ) from exc
        else:
            raise ValueError("Unsupported dataframe type for Parquet materialization.")
    elif output_format == "iceberg":
        try:
            import duckdb

            # Create a localized connection or use existing if it's a relation
            if hasattr(df, "connection") and hasattr(df, "sql_query"):
                con = df.connection
                con.execute("INSTALL iceberg; LOAD iceberg;")
                con.execute("INSTALL httpfs; LOAD httpfs;")
                con.execute(f"COPY ({df.sql_query()}) TO '{path}' (FORMAT ICEBERG)")
            else:
                con = duckdb.connect()
                con.execute("INSTALL iceberg; LOAD iceberg;")
                con.execute("INSTALL httpfs; LOAD httpfs;")
                con.register("incoming_df", df)
                con.execute(f"COPY incoming_df TO '{path}' (FORMAT ICEBERG)")
        except ImportError:
            raise ValueError("Iceberg materialization requires 'duckdb' installed.")
        except Exception as e:
            raise ValueError(f"Iceberg materialization failed: {e}")
    elif output_format == "delta":
        try:
            import deltalake  # noqa: F401  # availability probe — raises ImportError if not installed

            # Polars native write_delta for remote paths
            if is_remote and hasattr(df, "write_delta"):
                logger.info(f"Writing Delta to remote: {path_str}")
                df.write_delta(
                    path_str,
                    mode="overwrite",
                    storage_options=opts,
                )
                return

            # Convert to Arrow without pandas — Polars, DuckDB and PyArrow all
            # support zero-copy Arrow conversion. Never route through pandas.
            import pyarrow as pa

            data = df
            if hasattr(df, "to_arrow"):  # Polars DataFrame
                data = df.to_arrow()
            elif hasattr(df, "to_arrow_table"):  # DuckDB relation
                data = df.to_arrow_table()
            elif "pandas" in type(df).__module__:
                # Pandas DataFrame — convert to Arrow so the empty-table
                # fallback below isn't accidentally hit for non-empty input.
                # Without this branch a 1-row pandas frame became an empty
                # pa.Table.from_pydict({}) which then triggered
                # _safe_write_deltalake's empty-data schema_mode strip.
                data = pa.Table.from_pandas(df, preserve_index=False)

            if not isinstance(data, pa.Table):
                data = pa.Table.from_pydict({})  # last resort empty table
            if isinstance(data, pa.Table):
                data = _sanitize_arrow_nulls(data)

            delta_mode = mode_override or "overwrite"
            _engine = "rust" if is_remote else "pyarrow"
            # deltalake 1.x: schema_mode='overwrite' is only valid with
            # mode='overwrite'. For append we use 'merge' so new columns can be
            # added without rewriting existing data.
            _schema_mode = "overwrite" if delta_mode == "overwrite" else "merge"
            _safe_write_deltalake(
                path_str if is_remote else path,
                data,
                mode=delta_mode,
                schema_mode=_schema_mode,
                engine=_engine,
                storage_options=opts,
            )
        except ImportError:
            raise ValueError("Delta materialization requires 'deltalake' installed (pip install deltalake).")
        except Exception as e:
            raise ValueError(f"Delta materialization failed: {e}")
    elif output_format == "duckdb":
        try:
            import duckdb
            import re

            db_path = str(path_str) if not is_remote else path_str
            # Use path.stem as table name, stripping non-alphanumeric
            table_name = re.sub(r"[^a-zA-Z0-9_]", "_", Path(path).stem)
            if not table_name:
                table_name = "data"

            owns_connection = False
            if hasattr(df, "connection") and hasattr(df, "sql_query"):
                con = df.connection
            else:
                con = duckdb.connect()
                owns_connection = True

            try:
                # If path doesn't exist, duckdb creates it when we connect. Here we attach.
                con.execute(f"ATTACH '{db_path}' AS target_db")
                if hasattr(df, "sql_query"):
                    con.execute(f"CREATE OR REPLACE TABLE target_db.{table_name} AS SELECT * FROM ({df.sql_query()})")
                else:
                    con.register("incoming_df", df)
                    con.execute(f"CREATE OR REPLACE TABLE target_db.{table_name} AS SELECT * FROM incoming_df")
            finally:
                try:
                    con.execute("DETACH target_db")
                except Exception:
                    pass
                if owns_connection and con is not None:
                    con.close()
        except ImportError:
            raise ValueError("DuckDB materialization requires 'duckdb' installed.")
        except Exception as e:
            raise ValueError(f"DuckDB materialization failed: {e}")
    elif output_format == "dlt":
        # ── dlt Destination Materialization ──────────────────────────────────
        # Enables writing to any dlt-supported destination:
        #   postgres, snowflake, bigquery, redshift, mssql, databricks,
        #   motherduck, clickhouse, synapse, filesystem, and more.
        #
        # Contract YAML usage:
        #   materialization:
        #     format: dlt
        #     dlt_destination: postgres          # any dlt destination name
        #     dlt_credentials: "postgresql://user:pass@host:5432/db"
        #     dlt_dataset_name: analytics        # optional schema/dataset
        #     strategy: merge                    # append | merge | overwrite
        #     primary_key: [id]                  # required for merge
        try:
            import dlt as _dlt
            import pyarrow as pa
            import re

            # Extract dlt config from the path object or contract extras.
            # When called from materialize_dataframe, the path carries the
            # target table name.  The actual destination config is passed
            # via the contract's materialization extras (extra="allow").
            dlt_config = {}
            if hasattr(path, "_dlt_config"):
                dlt_config = path._dlt_config

            destination = dlt_config.get("dlt_destination", "duckdb")
            credentials = dlt_config.get("dlt_credentials")
            dataset_name = dlt_config.get("dlt_dataset_name", "lakelogic")
            write_disposition = dlt_config.get("write_disposition", "append")
            primary_key = dlt_config.get("primary_key")

            # Derive table name from path
            table_name = (
                re.sub(r"[^a-zA-Z0-9_]", "_", str(Path(path).stem))
                if not hasattr(path, "_dlt_table")
                else path._dlt_table
            )
            if not table_name:
                table_name = "data"

            # Convert DataFrame to Arrow for efficient transfer
            if hasattr(df, "to_arrow"):
                arrow_data = df.to_arrow()
            elif hasattr(df, "to_arrow_table"):
                arrow_data = df.to_arrow_table()
            elif isinstance(df, pa.Table):
                arrow_data = df
            else:
                # Pandas or other — go through Arrow
                arrow_data = pa.Table.from_pandas(df) if hasattr(df, "columns") else df

            @_dlt.resource(
                name=table_name,
                write_disposition=write_disposition,
                primary_key=primary_key,
            )
            def _dlt_sink():
                yield arrow_data

            # Build destination kwargs dynamically from any dlt_* config
            dest_kwargs = {}
            if credentials:
                dest_kwargs["credentials"] = credentials
            for k, v in dlt_config.items():
                if k.startswith("dlt_") and k not in ("dlt_destination", "dlt_credentials", "dlt_dataset_name"):
                    dest_kwargs[k[4:]] = v

            pipeline = _dlt.pipeline(
                pipeline_name=f"lakelogic_{table_name}",
                destination=_dlt.destinations.__dict__.get(destination, destination)(**dest_kwargs)
                if dest_kwargs
                else destination,
                dataset_name=dataset_name,
            )

            load_info = pipeline.run(_dlt_sink())
            logger.info(
                f"dlt materialization complete: {table_name} → {destination} ({write_disposition}) | {load_info}"
            )

        except ImportError:
            raise ValueError(
                "dlt materialization requires the 'dlt' package and the target destination extras. "
                "Install with: pip install dlt[postgres]  (or dlt[snowflake], dlt[bigquery], etc.)"
            )
        except Exception as e:
            raise ValueError(f"dlt materialization failed: {e}")
    else:
        raise ValueError(f"Unsupported output format: {output_format}")


def _pandas_available() -> bool:
    try:
        import pandas  # noqa: F401
    except Exception:
        return False
    return True


def _row_count(df: Any) -> Optional[int]:
    try:
        if hasattr(df, "height"):
            return int(df.height)
        return int(len(df))
    except Exception:
        return None


def _is_polars_frame(df: Any) -> bool:
    try:
        import polars as pl
    except Exception:
        return False
    return isinstance(df, (pl.DataFrame, pl.LazyFrame))


def _frame_has_columns(df: Any) -> bool:
    """
    Best-effort check whether a frame has defined columns.
    Used to avoid writing empty/invalid files (e.g., 0-column parquet).
    """
    if df is None:
        return False
    try:
        if hasattr(df, "columns"):
            return len(df.columns) > 0
    except Exception:
        pass
    try:
        if hasattr(df, "schema"):
            schema = df.schema
            if schema is not None:
                return len(schema) > 0
    except Exception:
        pass
    try:
        if hasattr(df, "collect_schema"):
            schema = df.collect_schema()
            return len(schema) > 0
    except Exception:
        pass
    if isinstance(df, dict):
        return len(df) > 0
    if isinstance(df, (list, tuple)):
        if not df:
            return False
        first = df[0]
        if isinstance(first, dict):
            return len(first) > 0
        return True
    return True


def _append_without_pandas(df: Any, target_file: Path, output_format: str) -> int:
    try:
        import polars as pl
    except Exception as exc:
        raise ValueError("Append without pandas requires polars installed.") from exc

    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    if not isinstance(df, pl.DataFrame):
        raise ValueError("Append without pandas requires a Polars DataFrame.")

    if target_file.exists():
        if output_format == "csv":
            existing = pl.read_csv(target_file)
            combined = pl.concat([existing, df], how="vertical")
            _write_frame(combined, target_file, output_format)
            return int(combined.height)
        elif output_format == "parquet":
            existing = pl.read_parquet(target_file)
            combined = pl.concat([existing, df], how="vertical")
            _write_frame(combined, target_file, output_format)
            return int(combined.height)
        elif output_format == "delta":
            # Delta append: write_deltalake with mode="append" handles this natively
            _write_frame(df, target_file, output_format, mode_override="append")
            return int(df.height)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

    _write_frame(df, target_file, output_format)
    return int(df.height)


def _read_frame(path: Path, output_format: str):
    """
    Read a pandas DataFrame from disk.

    Args:
        path: Source path.
        output_format: csv or parquet.

    Returns:
        pandas.DataFrame
    """
    import pandas as pd

    if output_format == "csv":
        return pd.read_csv(path)
    if output_format == "parquet":
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            try:
                import duckdb

                return duckdb.read_parquet(str(path)).df()
            except Exception as exc:
                logger.debug(f"DuckDB parquet read fallback failed: {exc}")
            try:
                import polars as pl

                return pl.read_parquet(path).to_pandas()
            except Exception:
                raise ValueError("Parquet reads require pyarrow/fastparquet, duckdb, or polars as a fallback.") from exc
    if output_format == "delta":
        try:
            import polars as pl

            return pl.read_delta(str(path)).to_pandas()
        except (ImportError, Exception):
            from deltalake import DeltaTable

            return DeltaTable(path).to_pandas()
    if output_format == "iceberg":
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
        return con.execute(f"SELECT * FROM iceberg_scan('{path}')").to_df()
    if output_format == "duckdb":
        import duckdb
        import re

        db_path = str(path)
        table_name = re.sub(r"[^a-zA-Z0-9_]", "_", path.stem)
        if not table_name:
            table_name = "data"

        con = duckdb.connect(db_path, read_only=True)
        try:
            return con.execute(f"SELECT * FROM {table_name}").df()
        finally:
            con.close()
    raise ValueError(f"Unsupported output format: {output_format}")


def _seed_soft_delete_columns_pandas(
    df,
    soft_delete_col: Optional[str] = None,
    soft_delete_time_col: Optional[str] = None,
    soft_delete_reason_col: Optional[str] = None,
    cdc_op_field: Optional[str] = None,
    cdc_delete_values: Optional[List[Any]] = None,
    soft_delete_val: Any = True,
    cdc_timestamp_field: Optional[str] = None,
):
    """Ensure soft-delete metadata columns exist in a pandas DataFrame.

    On the **first write** (no existing table) the merge path is skipped,
    so these columns would otherwise be absent from the output schema.
    This function adds them with sensible defaults (``False`` / ``None``),
    and — when CDC delete values are present — populates them for rows
    flagged as deletes.
    """
    if df is None or not soft_delete_col:
        return df

    from datetime import datetime, timezone

    df = df.copy()

    # Add columns if not present
    if soft_delete_col not in df.columns:
        df[soft_delete_col] = False
    if soft_delete_time_col and soft_delete_time_col not in df.columns:
        df[soft_delete_time_col] = None
    if soft_delete_reason_col and soft_delete_reason_col not in df.columns:
        df[soft_delete_reason_col] = None

    # If CDC delete signals are in this batch, mark those rows
    if cdc_op_field and cdc_delete_values and cdc_op_field in df.columns:
        delete_mask = df[cdc_op_field].isin(cdc_delete_values)
        df.loc[delete_mask, soft_delete_col] = soft_delete_val

        if soft_delete_time_col:
            now_ts = datetime.now(timezone.utc).isoformat()
            source_col = cdc_timestamp_field if cdc_timestamp_field and cdc_timestamp_field in df.columns else None
            if source_col:
                df.loc[delete_mask, soft_delete_time_col] = df.loc[delete_mask, source_col].fillna(now_ts)
            else:
                df.loc[delete_mask, soft_delete_time_col] = now_ts

        if soft_delete_reason_col:
            df.loc[delete_mask, soft_delete_reason_col] = df[soft_delete_reason_col].where(
                ~delete_mask | df[soft_delete_reason_col].notna(), "cdc_delete_signal"
            )

    return df


def _seed_soft_delete_columns_spark(  # pragma: no cover
    df,
    soft_delete_col: Optional[str] = None,
    soft_delete_time_col: Optional[str] = None,
    soft_delete_reason_col: Optional[str] = None,
    cdc_op_field: Optional[str] = None,
    cdc_delete_values: Optional[List[Any]] = None,
    soft_delete_val: Any = True,
    cdc_timestamp_field: Optional[str] = None,
):
    """Ensure soft-delete metadata columns exist in a Spark DataFrame.

    Mirrors ``_seed_soft_delete_columns_pandas`` for Spark DataFrames.
    Adds the columns with sensible defaults and populates them when CDC
    delete signals are present.
    """
    if df is None or not soft_delete_col:
        return df

    from pyspark.sql import functions as F

    # Add columns if not present
    if soft_delete_col not in df.columns:
        df = df.withColumn(soft_delete_col, F.lit(False).cast("boolean"))
    if soft_delete_time_col and soft_delete_time_col not in df.columns:
        df = df.withColumn(soft_delete_time_col, F.lit(None).cast("string"))
    if soft_delete_reason_col and soft_delete_reason_col not in df.columns:
        df = df.withColumn(soft_delete_reason_col, F.lit(None).cast("string"))

    # If CDC delete signals are in this batch, mark those rows
    if cdc_op_field and cdc_delete_values and cdc_op_field in df.columns:
        delete_cond = F.col(cdc_op_field).isin(cdc_delete_values)

        df = df.withColumn(
            soft_delete_col, F.when(delete_cond, F.lit(soft_delete_val)).otherwise(F.col(soft_delete_col))
        )

        if soft_delete_time_col:
            now_str = F.current_timestamp().cast("string")
            source_col = cdc_timestamp_field if cdc_timestamp_field else soft_delete_time_col
            df = df.withColumn(
                soft_delete_time_col,
                F.when(delete_cond, F.coalesce(F.col(source_col), now_str)).otherwise(F.col(soft_delete_time_col)),
            )

        if soft_delete_reason_col:
            df = df.withColumn(
                soft_delete_reason_col,
                F.when(delete_cond, F.coalesce(F.col(soft_delete_reason_col), F.lit("cdc_delete_signal"))).otherwise(
                    F.col(soft_delete_reason_col)
                ),
            )

    return df


def _merge_frames(
    existing,
    incoming,
    primary_key: List[str],
    soft_delete_col: Optional[str] = None,
    soft_delete_val: Any = True,
    soft_delete_time_col: Optional[str] = None,
    soft_delete_reason_col: Optional[str] = None,
    cdc_op_field: Optional[str] = None,
    cdc_delete_values: Optional[List[Any]] = None,
    cdc_timestamp_field: Optional[str] = None,
    scd1_cfg: Optional[Dict[str, Any]] = None,
    merge_dedup_guard: bool = False,
):
    """
    Merge incoming rows into existing rows using a primary key.
    Supports CDC delete signals and soft-deletion with metadata.

    Args:
        existing: Existing dataframe.
        incoming: Incoming dataframe (unfiltered).
        primary_key: Primary key columns.
        soft_delete_col: Optional column to flag as deleted.
        soft_delete_val: Value to set in soft_delete_col.
        soft_delete_time_col: Optional column for deletion timestamp.
        soft_delete_reason_col: Optional column for deletion reason.
        cdc_op_field: Column indicating the CDC operation (U, I, D).
        cdc_delete_values: List of values in cdc_op_field representing a delete.

    Returns:
        Merged dataframe.
    """
    if not primary_key:
        raise ValueError("primary_key is required for merge strategy.")
    import pandas as pd
    from datetime import datetime, timezone

    existing = existing.copy()
    incoming = incoming.copy()

    # ── Dedup guard: deduplicate incoming by PK before MERGE ──
    if merge_dedup_guard:
        _pre_count = len(incoming)
        incoming = incoming.drop_duplicates(subset=primary_key, keep="last")
        _post_count = len(incoming)
        if _pre_count != _post_count:
            logger.warning(
                f"Merge dedup guard: {_pre_count} → {_post_count} rows "
                f"(dropped {_pre_count - _post_count} duplicate PKs). "
            )

    # 1. Handle CDC Deletes
    deletes = pd.DataFrame()
    if cdc_op_field and cdc_delete_values and cdc_op_field in incoming.columns:
        delete_mask = incoming[cdc_op_field].isin(cdc_delete_values)
        deletes = incoming[delete_mask].copy()
        incoming = incoming[~delete_mask].copy()

    all_cols = list(dict.fromkeys(list(existing.columns) + list(incoming.columns)))

    # Ensure metadata columns exist in schema
    metadata_cols = []
    if soft_delete_col:
        metadata_cols.append(soft_delete_col)
    if soft_delete_time_col:
        metadata_cols.append(soft_delete_time_col)
    if soft_delete_reason_col:
        metadata_cols.append(soft_delete_reason_col)

    for col in metadata_cols:
        if col not in all_cols:
            all_cols.append(col)

    for col in all_cols:
        if col not in existing.columns:
            dtype = incoming[col].dtype if col in incoming.columns else "object"
            existing[col] = pd.Series(dtype=dtype)
        if col not in incoming.columns:
            dtype = existing[col].dtype if col in existing.columns else "object"
            incoming[col] = pd.Series(dtype=dtype)

    existing = existing.reindex(columns=all_cols)
    incoming = incoming.reindex(columns=all_cols)

    # Prepare indices
    existing = existing.set_index(primary_key)
    incoming = incoming.set_index(primary_key)

    # 2. Apply Updates/Inserts
    existing.update(incoming)
    new_rows = incoming.loc[~incoming.index.isin(existing.index)]

    # 3. Apply Soft Deletes
    if not deletes.empty and soft_delete_col:
        deletes = deletes.reindex(columns=all_cols)

        # Set metadata for the delete batch (Smart Fill)
        deletes[soft_delete_col] = soft_delete_val

        if soft_delete_time_col:
            source_time_col = cdc_timestamp_field if cdc_timestamp_field else soft_delete_time_col
            # Fill only where source didn't provide a timestamp
            now_ts = datetime.now(timezone.utc).isoformat()
            if source_time_col in deletes.columns:
                deletes[soft_delete_time_col] = deletes[source_time_col].fillna(now_ts)
            else:
                deletes[soft_delete_time_col] = now_ts

        if soft_delete_reason_col:
            # Fill only where source didn't provide a reason
            default_reason = "cdc_delete_signal"
            if soft_delete_reason_col in deletes.columns:
                deletes[soft_delete_reason_col] = deletes[soft_delete_reason_col].fillna(default_reason).astype(str)
            else:
                deletes[soft_delete_reason_col] = default_reason

        deletes = deletes.set_index(primary_key)

        # Update existing records with the delete flag and metadata
        existing.update(deletes)

        # If deleted record didn't exist, we might still want to insert it as a "tombstone"
        new_deletes = deletes.loc[~deletes.index.isin(existing.index)]
        if not new_deletes.empty:
            new_rows = pd.concat([new_rows, new_deletes])

    # 4. Apply Hard Deletes (if no soft delete col)
    elif not deletes.empty:
        # Filter existing by removing matching keys from deletes
        delete_keys = deletes.set_index(primary_key).index
        existing = existing.loc[~existing.index.isin(delete_keys)]

    merged = existing.reset_index()
    if not new_rows.empty:
        frames = [merged, new_rows.reset_index()]
        frames = [f for f in frames if not f.empty and not f.isna().all(axis=None)]
        merged = pd.concat(frames, ignore_index=True) if frames else merged

    # ── SCD1 Surrogate Key Injection ─────────────────────────────
    if scd1_cfg and scd1_cfg.get("surrogate_key"):
        sk_column = scd1_cfg["surrogate_key"]
        sk_strategy = scd1_cfg.get("surrogate_key_strategy", "hash")
        import hashlib

        def _compute_sk(row):
            pk_val = "|".join(str(row.get(c, "")) for c in primary_key)
            if sk_strategy == "uuid":
                import uuid

                return uuid.uuid4().hex[:16]
            return hashlib.sha256(pk_val.encode("utf-8")).hexdigest()[:16]

        merged[sk_column] = merged.apply(_compute_sk, axis=1)

    # NOTE: SCD1 Unknown member injection is NOT done here because _merge_frames
    # can be called per-partition. The caller injects it once on the combined result.

    # ── Enforce SCD1 Column Ordering ─────────────────────────────
    if scd1_cfg and scd1_cfg.get("surrogate_key") and scd1_cfg["surrogate_key"] in merged.columns:
        sk_column = scd1_cfg["surrogate_key"]
        front_cols = [sk_column]
        other_cols = [c for c in merged.columns if c not in front_cols]
        merged = merged[front_cols + other_cols]

    return merged


def _inject_unknown_member_pandas(
    df,
    primary_key: List[str],
    scd2_cfg: Dict[str, Any],
    unknown_cfg: Dict[str, Any],
):
    """
    Inject a Kimball 'Unknown Member' row into a pandas/polars SCD2 result.

    Idempotent: skips if the row already exists (matched by SK value).
    """
    import pandas as pd

    if unknown_cfg.get("enabled", True) is False:
        return df

    sk_column = scd2_cfg.get("surrogate_key", "_sk")
    sk_value = str(unknown_cfg.get("surrogate_key_value", "-1"))
    defaults = unknown_cfg.get("default_values", {})

    effective_from = scd2_cfg.get("effective_from_field", "effective_from")
    effective_to = scd2_cfg.get("effective_to_field", "effective_to")
    current_flag = scd2_cfg.get("current_flag_field", "is_current")
    effective_to_default = (
        scd2_cfg.get("end_date_default")
        if "end_date_default" in scd2_cfg
        else scd2_cfg.get("effective_to_default", "9999-12-31")
    )
    effective_from_default = (
        scd2_cfg.get("start_date_default")
        if "start_date_default" in scd2_cfg
        else scd2_cfg.get("effective_from_default", "1900-01-01")
    )
    version_column = scd2_cfg.get("version_column", "_version")
    change_reason_col = scd2_cfg.get("change_reason_column", "_change_reason")

    # Check if unknown member already exists (by SK value OR by change_reason)
    # Belt-and-suspenders: the SK may have been overwritten by hash recomputation
    # in _scd2_frames, so also check the change_reason column as a fallback.
    if sk_column in df.columns:
        existing_unknown = df[df[sk_column].astype(str) == sk_value]
        if not existing_unknown.empty:
            return df
    if change_reason_col and change_reason_col in df.columns:
        existing_by_reason = df[df[change_reason_col].astype(str) == "unknown_member"]
        if not existing_by_reason.empty:
            return df

    # Extract lineage column values from first row so the unknown member
    # gets the same run metadata as the rest of the batch.
    _lineage_cols = {
        "_lakelogic_processed_at",
        "_lakelogic_run_id",
        "_lakelogic_created_at",
        "_lakelogic_created_by",
        "_lakelogic_contract_name",
    }
    _lineage_values: Dict[str, Any] = {"_lakelogic_source": "Unknown"}
    if not df.empty:
        first_row = df.iloc[0]
        for lc in _lineage_cols:
            if lc in df.columns:
                _lineage_values[lc] = first_row[lc]

    # Build the unknown row from column defaults
    unknown_row = {}
    for col in df.columns:
        if col == sk_column:
            unknown_row[col] = sk_value
        elif col in defaults:
            unknown_row[col] = defaults[col]
        elif col == effective_from:
            unknown_row[col] = effective_from_default
        elif col == effective_to:
            unknown_row[col] = effective_to_default
        elif col == current_flag:
            unknown_row[col] = True
        elif col == version_column:
            unknown_row[col] = 0
        elif col == change_reason_col:
            unknown_row[col] = "unknown_member"
        elif col in _lineage_values:
            unknown_row[col] = _lineage_values[col]
        else:
            val = defaults.get(col)
            if val is None:
                from pandas.api.types import is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_any_dtype
                import pandas as pd

                dtype = df[col].dtype
                if is_string_dtype(dtype):
                    val = "Unknown"
                elif is_numeric_dtype(dtype):
                    val = -1
                elif is_bool_dtype(dtype):
                    val = False
                elif is_datetime64_any_dtype(dtype):
                    val = pd.to_datetime(effective_from_default)
            unknown_row[col] = val

    unknown_df = pd.DataFrame([unknown_row])
    merged = pd.concat([df, unknown_df], ignore_index=True)
    logger.info(f"Injected unknown member row (SK={sk_value}) into dimension")
    return merged


def _inject_unknown_member_spark(  # pragma: no cover
    result,
    primary_key: List[str],
    scd2_cfg: Dict[str, Any],
    unknown_cfg: Dict[str, Any],
):
    """
    Inject a Kimball 'Unknown Member' row into a Spark SCD2 result.

    Idempotent: skips if the row already exists (matched by SK value).
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Row

    if unknown_cfg.get("enabled", True) is False:
        return result

    sk_column = scd2_cfg.get("surrogate_key", "_sk")
    sk_value = str(unknown_cfg.get("surrogate_key_value", "-1"))
    defaults = unknown_cfg.get("default_values", {})

    effective_from = scd2_cfg.get("effective_from_field", "effective_from")
    effective_to = scd2_cfg.get("effective_to_field", "effective_to")
    current_flag = scd2_cfg.get("current_flag_field", "is_current")
    effective_to_default = (
        scd2_cfg.get("end_date_default")
        if "end_date_default" in scd2_cfg
        else scd2_cfg.get("effective_to_default", "9999-12-31")
    )
    effective_from_default = (
        scd2_cfg.get("start_date_default")
        if "start_date_default" in scd2_cfg
        else scd2_cfg.get("effective_from_default", "1900-01-01")
    )
    version_column = scd2_cfg.get("version_column", "_version")
    change_reason_col = scd2_cfg.get("change_reason_column", "_change_reason")

    # Check if unknown member already exists (by SK value OR by change_reason)
    if sk_column in result.columns:
        existing_count = result.filter(F.col(sk_column).cast("string") == sk_value).count()
        if existing_count > 0:
            return result
    if change_reason_col and change_reason_col in result.columns:
        reason_count = result.filter(F.col(change_reason_col) == "unknown_member").count()
        if reason_count > 0:
            return result

    # Extract lineage column values from the first row so the unknown member
    # gets the same run metadata as the rest of the batch.
    _lineage_cols = {
        "_lakelogic_processed_at",
        "_lakelogic_run_id",
        "_lakelogic_created_at",
        "_lakelogic_created_by",
        "_lakelogic_contract_name",
    }
    _lineage_values: Dict[str, Any] = {"_lakelogic_source": "Unknown"}
    if result.count() > 0:
        first_row = result.first()
        if first_row:
            for lc in _lineage_cols:
                if lc in result.columns:
                    _lineage_values[lc] = first_row[lc]

    # Build the unknown row dict
    spark = result.sparkSession
    unknown_row = {}
    for field in result.schema:
        col = field.name
        if col == sk_column:
            unknown_row[col] = sk_value
        elif col in defaults:
            unknown_row[col] = defaults[col]
        elif col == effective_from:
            unknown_row[col] = effective_from_default
        elif col == effective_to:
            unknown_row[col] = effective_to_default
        elif col == current_flag:
            unknown_row[col] = True
        elif col == version_column:
            unknown_row[col] = 0
        elif col == change_reason_col:
            unknown_row[col] = "unknown_member"
        elif col in _lineage_values:
            unknown_row[col] = _lineage_values[col]
        else:
            val = defaults.get(col)
            if val is None:
                dt = field.dataType.simpleString().lower()
                if "string" in dt or "char" in dt:
                    val = "Unknown"
                    import re

                    match = re.search(r"char\((\d+)\)", dt)
                    if match:
                        val = val[: int(match.group(1))]
                elif "int" in dt or "long" in dt or "short" in dt or "byte" in dt:
                    val = -1
                elif "float" in dt or "double" in dt or "decimal" in dt:
                    val = -1.0
                elif "bool" in dt:
                    val = False
                elif "date" in dt or "timestamp" in dt:
                    val = effective_from_default
            unknown_row[col] = val

    unknown_df = spark.createDataFrame([Row(**unknown_row)])

    # Align schema: cast columns to match result schema
    for field in result.schema:
        if field.name in unknown_df.columns:
            unknown_df = unknown_df.withColumn(field.name, F.col(field.name).cast(field.dataType))
        else:
            unknown_df = unknown_df.withColumn(field.name, F.lit(None).cast(field.dataType))
    unknown_df = unknown_df.select(*[c.name for c in result.schema])

    result = result.union(unknown_df)
    logger.info(f"Injected unknown member row (SK={sk_value}) into dimension")
    return result


def _inject_unknown_member_spark_table(  # pragma: no cover
    spark,
    table_name: str,
    primary_key: List[str],
    scd2_cfg: Dict[str, Any],
    unknown_cfg: Dict[str, Any],
):
    """
    Inject a Kimball 'Unknown Member' row into an already-written Spark table.

    Used for SCD1 (merge) dimensions where the table is written before this
    function is called. Reads the existing table schema to build the row.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Row

    if not unknown_cfg.get("enabled"):
        return

    sk_column = scd2_cfg.get("surrogate_key", "_sk") if scd2_cfg else None
    sk_value = str(unknown_cfg.get("surrogate_key_value", "-1"))
    defaults = unknown_cfg.get("default_values", {})

    try:
        existing = spark.table(table_name)
    except Exception:
        logger.debug(f"Table {table_name} not available for unknown member injection")
        return

    # Check if unknown member already exists
    if sk_column and sk_column in existing.columns:
        count = existing.filter(F.col(sk_column).cast("string") == sk_value).count()
        if count > 0:
            return
    elif primary_key:
        # For merge without SCD2 SK, check by primary key sentinel
        pk_col = primary_key[0]
        pk_sentinel = defaults.get(pk_col, "_UNKNOWN")
        count = existing.filter(F.col(pk_col).cast("string") == str(pk_sentinel)).count()
        if count > 0:
            return

    # Extract lineage column values from the first row so the unknown member
    # gets the same run metadata as the rest of the table.
    _lineage_cols = {
        "_lakelogic_processed_at",
        "_lakelogic_run_id",
        "_lakelogic_created_at",
        "_lakelogic_created_by",
        "_lakelogic_contract_name",
    }
    _lineage_values = {"_lakelogic_source": "Unknown"}
    if existing.count() > 0:
        first_row = existing.first()
        if first_row:
            for lc in _lineage_cols:
                if lc in existing.columns:
                    _lineage_values[lc] = first_row[lc]

    # Build the unknown row
    unknown_row = {}
    for field in existing.schema:
        col = field.name
        if col == sk_column:
            unknown_row[col] = sk_value
        elif col in defaults:
            unknown_row[col] = defaults[col]
        elif col in _lineage_values:
            unknown_row[col] = _lineage_values[col]
        else:
            unknown_row[col] = None

    unknown_df = spark.createDataFrame([Row(**unknown_row)])
    for field in existing.schema:
        if field.name in unknown_df.columns:
            unknown_df = unknown_df.withColumn(field.name, F.col(field.name).cast(field.dataType))
        else:
            unknown_df = unknown_df.withColumn(field.name, F.lit(None).cast(field.dataType))
    unknown_df = unknown_df.select(*[c.name for c in existing.schema])

    # Append the unknown member row
    unknown_df.write.format("delta").mode("append").saveAsTable(table_name)
    logger.info(f"Injected unknown member row (SK={sk_value}) into {table_name}")


def _scd2_frames(existing, incoming, primary_key: List[str], scd2_cfg: Dict[str, Any]):
    """
    Apply SCD2 changes by closing current records and appending new versions.

    Args:
        existing: Existing dataframe.
        incoming: Incoming dataframe.
        primary_key: Primary key columns.
        scd2_cfg: SCD2 configuration dict supporting keys:
            - effective_from_field (str): NAME of the version-start column in the
              destination dimension table (e.g. "effective_from"). This is purely a
              column-naming setting — it does NOT control which source field is read.
            - change_date_field (str): SOURCE column whose value is used as the start
              date when a tracked column actually changes (e.g. "updated_at").  Defaults
              to effective_from_field for backwards compatibility (when both share the
              same name, e.g. the old effective_from_field: updated_at pattern).
            - effective_to_field (str): column holding version-end date.
            - current_flag_field (str): boolean column marking the live row.
            - track_columns (list[str]): optional – only open a new version
              when at least one of these columns has changed.  When omitted,
              every incoming row for a known key triggers a new version
              (original behaviour).
            - effective_to_default (str): sentinel for current rows (default "9999-12-31").
            - effective_from_default (str): origin date for initial loads / first
              appearances (default "1900-01-01").
            - merge_dedup_guard (bool): if true, deduplicate incoming by PK (latest wins).

    Returns:
        Updated dataframe with SCD2 semantics.
    """
    if not primary_key:
        raise ValueError("primary_key is required for scd2 strategy.")

    effective_from = scd2_cfg.get("effective_from_field", "effective_from")  # destination column name
    effective_to = scd2_cfg.get("effective_to_field", "effective_to")
    current_flag = scd2_cfg.get("current_flag_field", "is_current")
    track_columns: Optional[List[str]] = scd2_cfg.get("track_columns")
    # change_date_field: which SOURCE column holds the actual change-event date.
    # Defaults to effective_from for backwards compat (old pattern: effective_from_field: updated_at).
    ts_field = scd2_cfg.get("timestamp_field")
    change_date_field: str = ts_field or scd2_cfg.get("change_date_field", effective_from)

    # Default values for SCD2 fields
    effective_to_default = (
        scd2_cfg.get("end_date_default")
        if "end_date_default" in scd2_cfg
        else scd2_cfg.get("effective_to_default", "9999-12-31")
    )
    effective_from_default = (
        scd2_cfg.get("start_date_default")
        if "start_date_default" in scd2_cfg
        else scd2_cfg.get("effective_from_default", "1900-01-01")
    )
    change_reason_col = scd2_cfg.get("change_reason_column", "_change_reason")

    # Timestamp for closing existing records (when a change occurs)
    now_value = scd2_cfg.get("default_effective_from")
    if not now_value:
        now_value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # ── Dedup guard: deduplicate incoming by PK before SCD2 processing ──
    if scd2_cfg.get("merge_dedup_guard"):
        _pre_count = len(incoming)
        # Sort by change_date_field to ensure 'last' is the latest change
        if change_date_field in incoming.columns:
            incoming = incoming.sort_values(primary_key + [change_date_field])
        incoming = incoming.drop_duplicates(subset=primary_key, keep="last")
        _post_count = len(incoming)
        if _pre_count != _post_count:
            logger.warning(
                f"SCD2 merge dedup guard: {_pre_count} → {_post_count} rows "
                f"(dropped {_pre_count - _post_count} duplicate PKs). "
            )

    existing = existing.copy()
    incoming = incoming.copy()

    # Ensure SCD2 control columns exist on incoming
    if effective_from not in incoming.columns:
        if change_date_field and change_date_field in incoming.columns:
            incoming[effective_from] = incoming[change_date_field]
        else:
            incoming[effective_from] = now_value

    if effective_to not in incoming.columns:
        import pandas as pd

        incoming[effective_to] = effective_to_default if effective_to_default is not None else pd.NaT

    if current_flag not in incoming.columns:
        incoming[current_flag] = True

    # Initial load – no existing data
    if existing.empty:
        # For initial load, all incoming records are new and start from effective_from_default
        incoming[effective_from] = effective_from_default
        import pandas as pd

        incoming[effective_to] = effective_to_default if effective_to_default is not None else pd.NaT
        incoming[current_flag] = True
        if change_reason_col:
            incoming[change_reason_col] = "initial_load"

        # ── Surrogate key injection (pandas/polars) initial load ─────
        sk_column = scd2_cfg.get("surrogate_key", "_sk")
        sk_strategy = scd2_cfg.get("surrogate_key_strategy", "hash")
        if sk_column:
            import hashlib

            def _compute_sk(row):
                pk_val = "|".join(str(row.get(c, "")) for c in primary_key)
                ef_val = str(row.get(effective_from, ""))
                raw = f"{pk_val}|{ef_val}"
                if sk_strategy == "uuid":
                    import uuid

                    return uuid.uuid4().hex[:16]
                return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

            incoming[sk_column] = incoming.apply(_compute_sk, axis=1)

        # ── Version number injection ─────────────────────────────────
        version_column = scd2_cfg.get("version_column", "_version")
        if version_column:
            incoming = incoming.sort_values(primary_key + [effective_from])
            incoming[version_column] = incoming.groupby(primary_key).cumcount() + 1
        else:
            # Need a stable order anyway
            incoming = incoming.sort_values(primary_key + [effective_from])

        # ── Fix is_current for duplicate primary keys ────────────────
        # Only the highest version per primary key should be current.
        # Older versions get is_current=False and effective_to chained.
        if version_column and version_column in incoming.columns:
            max_version = incoming.groupby(primary_key)[version_column].transform("max")
            incoming[current_flag] = incoming[version_column] == max_version
            # Chain effective_to: older versions point to the next version's effective_from
            incoming.loc[~incoming[current_flag], effective_to] = now_value

        return incoming

    # Ensure SCD2 control columns exist on existing
    if effective_from not in existing.columns:
        existing[effective_from] = (
            effective_from_default  # Assume existing records started from default if not specified
        )
    if effective_to not in existing.columns:
        import pandas as pd

        existing[effective_to] = (
            effective_to_default if effective_to_default is not None else pd.NaT
        )  # Assume existing current records end at default if not specified
    if current_flag not in existing.columns:
        if effective_to_default is not None:
            existing[current_flag] = (
                existing[effective_to] == effective_to_default
            )  # Infer current based on effective_to
        else:
            import pandas as pd

            existing[current_flag] = pd.isna(existing[effective_to])

    # Cast SCD2 control columns so pandas can accept mixed-type writes
    merged = existing.copy()
    # Ensure these columns are of object type to handle mixed string/None/datetime values
    merged[effective_to] = merged[effective_to].astype(object)
    merged[current_flag] = merged[current_flag].astype(object)

    # Columns to compare when track_columns is set; fall back to all
    # non-key, non-SCD2 data columns present in both frames.
    scd2_control = {effective_from, effective_to, current_flag}
    if track_columns:
        compare_cols = [c for c in track_columns if c in existing.columns and c in incoming.columns]
    else:
        # If no track_columns, compare all non-key, non-SCD2 columns
        all_data_cols = list(set(existing.columns) | set(incoming.columns) - set(primary_key) - scd2_control)
        compare_cols = [c for c in all_data_cols if c in existing.columns and c in incoming.columns]

    incoming_keys = incoming[primary_key].drop_duplicates()

    # Rows from incoming that will actually open a new version
    import pandas as pd

    new_versions = []

    for _, key_row in incoming_keys.iterrows():
        # Build boolean mask for this key in the merged (existing) frame
        key_filter = None
        for col in primary_key:
            cond = merged[col] == key_row[col]
            key_filter = cond if key_filter is None else key_filter & cond

        # Matching row(s) in incoming for this key
        incoming_key_filter = None
        for col in primary_key:
            cond = incoming[col] == key_row[col]
            incoming_key_filter = cond if incoming_key_filter is None else incoming_key_filter & cond

        inc_rows = incoming[incoming_key_filter]
        if inc_rows.empty:
            continue

        inc_row = inc_rows.iloc[0]  # single canonical incoming row per key

        # Find the currently active record for this key in the merged dataframe
        current_mask = key_filter & merged[current_flag]

        # Decide whether a change actually happened
        changed = True  # Assume change if no track_columns or no existing current record
        changed_fields = []  # Track which fields changed
        if current_mask.any():
            existing_current_row = merged[current_mask].iloc[0]
            if compare_cols:
                changed_fields = [c for c in compare_cols if str(existing_current_row.get(c)) != str(inc_row.get(c))]
                changed = len(changed_fields) > 0
            else:
                changed = True
                changed_fields = ["all"]
        else:
            # No current record exists for this key, so the incoming record is new
            changed = True
            changed_fields = ["initial_load"]

        if changed:
            # Resolve the change-event date from the designated SOURCE column.
            # change_date_field is set by the user (e.g. "updated_at"); falls back
            # to effective_from_field for backwards compat when both share a name.
            if change_date_field in inc_row.index and inc_row[change_date_field] is not None:
                change_date = inc_row[change_date_field]
            else:
                change_date = effective_from_default

            if current_mask.any():
                # Close the existing current record: set its effective_to to the
                # new version's start date (the real change-event date from source)
                merged.loc[current_mask, effective_to] = change_date
                merged.loc[current_mask, current_flag] = False

            # Prepare the new version row
            new_version_row = inc_row.copy()
            if not current_mask.any():
                # Brand-new key — first appearance → use origin sentinel
                new_version_row[effective_from] = effective_from_default
            else:
                # Real change event — use the source change-event date
                new_version_row[effective_from] = change_date
            new_version_row[effective_to] = effective_to_default
            new_version_row[current_flag] = True

            # Stamp the change reason
            if change_reason_col:
                new_version_row[change_reason_col] = ",".join(changed_fields)

            new_versions.append(new_version_row.to_dict())
        # else: no tracked columns changed → skip (no new version, no close)

    if new_versions:
        new_df = pd.DataFrame(new_versions)
        merged = pd.concat([merged, new_df], ignore_index=True)

    # ── Surrogate key injection ──────────────────────────────────
    sk_column = scd2_cfg.get("surrogate_key", "_sk")
    sk_strategy = scd2_cfg.get("surrogate_key_strategy", "hash")

    unknown_cfg = scd2_cfg.get("unknown_member") or {}
    unknown_sk_val = str(unknown_cfg.get("surrogate_key_value", "-1")) if unknown_cfg.get("enabled", True) else None

    if sk_column:
        import hashlib

        def _compute_sk(row):
            # Protect the unknown member SK from being overwritten by a hash
            if unknown_sk_val and str(row.get(sk_column, "")) == unknown_sk_val:
                return unknown_sk_val

            pk_val = "|".join(str(row.get(c, "")) for c in primary_key)
            ef_val = str(row.get(effective_from, ""))
            raw = f"{pk_val}|{ef_val}"
            if sk_strategy == "uuid":
                import uuid

                return uuid.uuid4().hex[:16]
            # Default: hash (deterministic)
            return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

        merged[sk_column] = merged.apply(_compute_sk, axis=1)

    # ── Version number injection ─────────────────────────────────
    version_column = scd2_cfg.get("version_column", "_version")
    if version_column:
        merged = merged.sort_values(primary_key + [effective_from])
        merged[version_column] = merged.groupby(primary_key).cumcount() + 1

    # NOTE: Unknown member injection is NOT done here because _scd2_frames
    # is called per-partition. The caller injects it once on the combined result.

    # ── Enforce SCD2 Column Ordering ─────────────────────────────
    front_cols = []
    if sk_column and sk_column in merged.columns:
        front_cols.append(sk_column)
    if effective_from in merged.columns:
        front_cols.append(effective_from)
    if effective_to in merged.columns:
        front_cols.append(effective_to)
    if current_flag in merged.columns:
        front_cols.append(current_flag)

    other_cols = [c for c in merged.columns if c not in front_cols]
    merged = merged[front_cols + other_cols]

    return merged


def _partition_groups(df, partition_by: List[str]) -> Iterable[Tuple[Dict[str, Any], Any]]:
    """
    Yield dataframe groups for each partition.

    Args:
        df: pandas.DataFrame to partition.
        partition_by: Partition columns.

    Yields:
        Tuple of partition values and group dataframe.
    """
    if not partition_by:
        yield {}, df
        return

    grouped = df.groupby(partition_by, dropna=False)
    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        values = dict(zip(partition_by, keys))
        yield values, group.reset_index(drop=True)


def _spark_merge_dataframe(  # pragma: no cover
    spark,
    incoming_df: Any,
    target: str,
    primary_key: List[str],
    output_format: str,
    location: Optional[str] = None,
    scd1_cfg: Optional[Dict[str, Any]] = None,
    cdc_op_field: Optional[str] = None,
    cdc_delete_values: Optional[List[Any]] = None,
    cdc_timestamp_field: Optional[str] = None,
    soft_delete_col: Optional[str] = None,
    soft_delete_val: Any = True,
    soft_delete_time_col: Optional[str] = None,
    soft_delete_reason_col: Optional[str] = None,
    merge_dedup_guard: bool = False,
) -> Dict[str, Any]:
    """
    Perform a native Spark merge (upsert) without collecting to pandas.

    Uses DataFrame joins to update existing records and insert new ones.
    For Delta Lake tables, uses MERGE INTO when available.

    Args:
        spark: SparkSession.
        incoming_df: New data to merge.
        target: Target path or table name.
        primary_key: Primary key columns for matching.
        output_format: Output format (delta, parquet, etc.).

    Returns:
        Metadata about the write.
    """
    from pyspark.sql import functions as F

    is_table = target.startswith("table:")
    table_or_path = target[6:] if is_table else target

    # For Delta tables, use native MERGE INTO
    if output_format == "delta" and is_table:
        # ── Pre-process incoming_df for soft deletes BEFORE checking target ──
        # This ensures the schema includes soft-delete columns even on first write.
        is_soft_delete = bool(cdc_op_field and cdc_delete_values and soft_delete_col)
        is_hard_delete = bool(cdc_op_field and cdc_delete_values and not soft_delete_col)

        if is_soft_delete and cdc_op_field in incoming_df.columns:
            delete_cond = F.col(cdc_op_field).isin(cdc_delete_values)

            if soft_delete_col not in incoming_df.columns:
                incoming_df = incoming_df.withColumn(soft_delete_col, F.lit(None).cast("boolean"))
            incoming_df = incoming_df.withColumn(
                soft_delete_col, F.when(delete_cond, F.lit(soft_delete_val)).otherwise(F.col(soft_delete_col))
            )

            if soft_delete_time_col:
                if soft_delete_time_col not in incoming_df.columns:
                    incoming_df = incoming_df.withColumn(soft_delete_time_col, F.lit(None).cast("string"))
                now_str = F.current_timestamp().cast("string")
                source_time_col = cdc_timestamp_field if cdc_timestamp_field else soft_delete_time_col
                incoming_df = incoming_df.withColumn(
                    soft_delete_time_col,
                    F.when(delete_cond, F.coalesce(F.col(source_time_col), now_str)).otherwise(
                        F.col(soft_delete_time_col)
                    ),
                )

            if soft_delete_reason_col:
                if soft_delete_reason_col not in incoming_df.columns:
                    incoming_df = incoming_df.withColumn(soft_delete_reason_col, F.lit(None).cast("string"))
                incoming_df = incoming_df.withColumn(
                    soft_delete_reason_col,
                    F.when(
                        delete_cond, F.coalesce(F.col(soft_delete_reason_col), F.lit("cdc_delete_signal"))
                    ).otherwise(F.col(soft_delete_reason_col)),
                )

        try:
            from delta.tables import DeltaTable

            if DeltaTable.isDeltaTable(spark, table_or_path) or spark.catalog.tableExists(table_or_path):
                delta_table = (
                    DeltaTable.forName(spark, table_or_path) if is_table else DeltaTable.forPath(spark, table_or_path)
                )
                merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_key])

                # ── Dedup guard: deduplicate incoming by PK before MERGE ──
                # Disabled by default. Enable via merge_dedup_guard: true.
                if merge_dedup_guard:
                    from pyspark.sql.window import Window as _W

                    _dedup_w = _W.partitionBy(*primary_key).orderBy(F.monotonically_increasing_id().desc())
                    _pre_count = incoming_df.count()
                    incoming_df = (
                        incoming_df.withColumn("_dedup_rn", F.row_number().over(_dedup_w))
                        .filter(F.col("_dedup_rn") == 1)
                        .drop("_dedup_rn")
                    )
                    _post_count = incoming_df.count()
                    if _pre_count != _post_count:
                        logger.warning(
                            f"Spark merge dedup guard: {_pre_count} → {_post_count} rows "
                            f"(dropped {_pre_count - _post_count} duplicate PKs). "
                            f"Consider adding explicit dedup in your contract SQL."
                        )

                update_cols = {col: f"source.{col}" for col in incoming_df.columns if col not in primary_key}
                insert_cols = {col: f"source.{col}" for col in incoming_df.columns}

                merge_builder = delta_table.alias("target").merge(incoming_df.alias("source"), merge_condition)

                if is_hard_delete:
                    in_vals = ",".join([f"'{v}'" for v in cdc_delete_values])
                    delete_cond_str = f"source.{cdc_op_field} IN ({in_vals})"
                    insert_cond_str = f"source.{cdc_op_field} NOT IN ({in_vals})"

                    merge_builder = (
                        merge_builder.whenMatchedDelete(condition=delete_cond_str)
                        .whenMatchedUpdate(set=update_cols)
                        .whenNotMatchedInsert(condition=insert_cond_str, values=insert_cols)
                    )
                else:
                    merge_builder = merge_builder.whenMatchedUpdate(set=update_cols).whenNotMatchedInsert(
                        values=insert_cols
                    )

                merge_builder.execute()

                rows_written = incoming_df.count()
                logger.info(f"Merged {rows_written} rows into Delta table {table_or_path}")
                return {
                    "target": table_or_path,
                    "rows_written": rows_written,
                    "format": output_format,
                }
        except ImportError:
            logger.debug("Delta Lake not available, falling back to DataFrame merge")
        except Exception as e:
            logger.debug(f"Delta MERGE failed, falling back to DataFrame merge: {e}")

    # Fallback: DataFrame-based merge for non-Delta or when Delta unavailable
    try:
        if is_table and spark.catalog.tableExists(table_or_path):
            existing_df = spark.table(table_or_path)
        elif not is_table and Path(table_or_path).exists():
            existing_df = spark.read.format(output_format).load(table_or_path)
        else:
            # No existing data — seed soft-delete columns before first write
            # so the table schema includes them from creation.
            incoming_df = _seed_soft_delete_columns_spark(
                incoming_df,
                soft_delete_col,
                soft_delete_time_col,
                soft_delete_reason_col,
                cdc_op_field,
                cdc_delete_values,
                soft_delete_val,
                cdc_timestamp_field,
            )
            writer = incoming_df.write.format(output_format)
            if is_table:
                _spark_save_as_table(writer, table_or_path, "overwrite", location)
            else:
                writer.mode("overwrite").save(table_or_path)
            rows_written = incoming_df.count()
            logger.info(f"Wrote {rows_written} rows to {table_or_path} (no existing data)")
            return {
                "target": table_or_path,
                "rows_written": rows_written,
                "format": output_format,
            }
    except Exception:
        # Target doesn't exist yet — seed soft-delete columns before first write
        incoming_df = _seed_soft_delete_columns_spark(
            incoming_df,
            soft_delete_col,
            soft_delete_time_col,
            soft_delete_reason_col,
            cdc_op_field,
            cdc_delete_values,
            soft_delete_val,
            cdc_timestamp_field,
        )
        writer = incoming_df.write.format(output_format)
        if is_table:
            _spark_save_as_table(writer, table_or_path, "overwrite", location)
        else:
            writer.mode("overwrite").save(table_or_path)
        rows_written = incoming_df.count()
        logger.info(f"Wrote {rows_written} rows to {table_or_path} (new target)")
        return {
            "target": table_or_path,
            "rows_written": rows_written,
            "format": output_format,
        }

    # Perform merge via anti-join + union
    # 1. Find rows in existing that DON'T match incoming (to keep unchanged)
    # 2. Union with all incoming rows (which are updates or inserts)

    # ── Dedup guard for DataFrame-based merge fallback ───────────
    # Disabled by default. Enable via merge_dedup_guard: true.
    if merge_dedup_guard:
        from pyspark.sql.window import Window as _W

        _dedup_w = _W.partitionBy(*primary_key).orderBy(F.monotonically_increasing_id().desc())
        incoming_df = (
            incoming_df.withColumn("_dedup_rn", F.row_number().over(_dedup_w))
            .filter(F.col("_dedup_rn") == 1)
            .drop("_dedup_rn")
        )

    _join_condition = [existing_df[col] == incoming_df[col] for col in primary_key]

    # Get non-matching existing rows (rows not being updated)
    unchanged = existing_df.join(incoming_df, on=primary_key, how="left_anti")

    # Align columns
    all_columns = list(
        dict.fromkeys(existing_df.columns + [c for c in incoming_df.columns if c not in existing_df.columns])
    )
    for col in all_columns:
        if col not in unchanged.columns:
            unchanged = unchanged.withColumn(col, F.lit(None))
        if col not in incoming_df.columns:
            incoming_df = incoming_df.withColumn(col, F.lit(None))

    unchanged = unchanged.select(*all_columns)
    incoming_df = incoming_df.select(*all_columns)

    merged = unchanged.union(incoming_df)

    # ── SCD1 Surrogate Key Injection ─────────────────────────────
    if scd1_cfg and scd1_cfg.get("surrogate_key"):
        sk_column = scd1_cfg["surrogate_key"]
        sk_strategy = scd1_cfg.get("surrogate_key_strategy", "hash")
        pk_concat = F.concat_ws("|", *[F.col(c).cast("string") for c in primary_key])
        if sk_strategy == "uuid":
            merged = merged.withColumn(sk_column, F.expr("substring(uuid(), 1, 16)"))
        else:
            merged = merged.withColumn(sk_column, F.substring(F.sha2(pk_concat, 256), 1, 16))

    # ── SCD1 Unknown Member Injection ────────────────────────────
    if scd1_cfg:
        unknown_cfg = scd1_cfg.get("unknown_member", {})
        if unknown_cfg.get("enabled", True):
            merged = _inject_unknown_member_spark(merged, primary_key, scd1_cfg, unknown_cfg)

    # ── Enforce SCD1 Column Ordering ─────────────────────────────
    if scd1_cfg and scd1_cfg.get("surrogate_key") and scd1_cfg["surrogate_key"] in merged.columns:
        sk_column = scd1_cfg["surrogate_key"]
        other_cols = [c for c in merged.columns if c != sk_column]
        merged = merged.select(sk_column, *other_cols)

    writer = merged.write.format(output_format)
    if is_table:
        _spark_save_as_table(writer, table_or_path, "overwrite", location)
    else:
        writer.mode("overwrite").save(table_or_path)

    rows_written = merged.count()
    logger.info(f"Merged {rows_written} rows to {table_or_path}")
    return {
        "target": table_or_path,
        "rows_written": rows_written,
        "format": output_format,
    }


def _spark_scd2_dataframe(  # pragma: no cover
    spark,
    incoming_df: Any,
    target: str,
    primary_key: List[str],
    scd2_cfg: Dict[str, Any],
    output_format: str,
    location: Optional[str] = None,
    merge_dedup_guard: bool = False,
) -> Dict[str, Any]:
    """
    Perform native Spark SCD2 (Slowly Changing Dimension Type 2) without collecting to pandas.

    Closes current records and appends new versions using Spark DataFrame operations.

    Args:
        spark: SparkSession.
        incoming_df: New data with updates.
        target: Target path or table name.
        primary_key: Primary key columns.
        scd2_cfg: SCD2 configuration (effective_from_field, effective_to_field, current_flag_field).
        output_format: Output format.

    Returns:
        Metadata about the write.
    """
    from pyspark.sql import functions as F

    effective_from = scd2_cfg.get("effective_from_field", "effective_from")
    effective_to = scd2_cfg.get("effective_to_field", "effective_to")
    current_flag = scd2_cfg.get("current_flag_field", "is_current")

    ts_field = scd2_cfg.get("timestamp_field")
    change_date_field: str = ts_field or scd2_cfg.get("change_date_field", effective_from)
    effective_to_default = (
        scd2_cfg.get("end_date_default")
        if "end_date_default" in scd2_cfg
        else scd2_cfg.get("effective_to_default", "9999-12-31")
    )
    effective_from_default = (
        scd2_cfg.get("start_date_default")
        if "start_date_default" in scd2_cfg
        else scd2_cfg.get("effective_from_default", "1900-01-01")
    )

    now_value = scd2_cfg.get("default_effective_from")
    if not now_value:
        now_value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # ── Dedup guard: deduplicate incoming by PK before SCD2 processing ──
    if merge_dedup_guard:
        from pyspark.sql.window import Window as _W

        _pre_count = incoming_df.count()
        # Order by change_date_field to keep latest
        _dedup_w = _W.partitionBy(*primary_key).orderBy(F.col(change_date_field).desc())
        incoming_df = (
            incoming_df.withColumn("_dedup_rn", F.row_number().over(_dedup_w))
            .filter(F.col("_dedup_rn") == 1)
            .drop("_dedup_rn")
        )
        _post_count = incoming_df.count()
        if _pre_count != _post_count:
            logger.warning(
                f"Spark SCD2 merge dedup guard: {_pre_count} → {_post_count} rows "
                f"(dropped {_pre_count - _post_count} duplicate PKs). "
            )

    is_table = target.startswith("table:")
    table_or_path = target[6:] if is_table else target

    # Always stamp SCD2 control columns on incoming — the materializer owns
    # these fields.  The columns may already exist (from DDL or contract
    # schema) but with NULL values which must be overwritten.
    if change_date_field and change_date_field in incoming_df.columns:
        incoming_df = incoming_df.withColumn(effective_from, F.col(change_date_field))
    else:
        incoming_df = incoming_df.withColumn(effective_from, F.to_timestamp(F.lit(now_value)))

    if effective_to_default is None:
        incoming_df = incoming_df.withColumn(effective_to, F.lit(None).cast("timestamp"))
    else:
        incoming_df = incoming_df.withColumn(effective_to, F.to_timestamp(F.lit(effective_to_default)))

    incoming_df = incoming_df.withColumn(current_flag, F.lit(True))

    # Try to read existing data
    existing_df = None
    try:
        if is_table and spark.catalog.tableExists(table_or_path):
            existing_df = spark.table(table_or_path)
        elif not is_table:
            # Probe via Spark for both local AND cloud paths (abfss://, s3://, etc.)
            existing_df = spark.read.format(output_format).load(table_or_path)
    except Exception:
        pass

    if existing_df is None or existing_df.count() == 0:
        # Initial load represents pre-existing history: stamp effective_from with
        # the configured start-of-time sentinel (default 1900-01-01) so the first
        # version reads as "valid since the beginning", matching the polars/pandas
        # engine (see _scd2_frames). Done BEFORE the surrogate key so the SK is
        # derived from the same effective_from value on every engine.
        # (Previously the Spark path left effective_from = the change/timestamp
        # column here, ignoring start_date_default — an engine inconsistency.)
        if effective_from_default is not None:
            incoming_df = incoming_df.withColumn(effective_from, F.to_timestamp(F.lit(effective_from_default)))

        # Generate surrogate key for initial load
        sk_column = scd2_cfg.get("surrogate_key", "_sk")
        sk_strategy = scd2_cfg.get("surrogate_key_strategy", "hash")
        if sk_column:
            pk_concat = F.concat_ws(
                "|", *[F.col(c).cast("string") for c in primary_key], F.col(effective_from).cast("string")
            )
            if sk_strategy == "uuid":
                incoming_df = incoming_df.withColumn(sk_column, F.expr("substring(uuid(), 1, 16)"))
            else:
                incoming_df = incoming_df.withColumn(sk_column, F.substring(F.sha2(pk_concat, 256), 1, 16))

        # Stamp version + change-reason here too, so the initial-load schema is
        # IDENTICAL to the merge path's (below). Otherwise a re-run overwrites the
        # table with a differently-shaped frame and fails with DELTA_METADATA_MISMATCH
        # under UC Table ACLs (auto schema migration disallowed).
        _init_version_col = scd2_cfg.get("version_column", "_version")
        if _init_version_col:
            from pyspark.sql.window import Window as _InitWindow

            _init_w = _InitWindow.partitionBy(*primary_key).orderBy(F.col(effective_from).asc())
            incoming_df = incoming_df.withColumn(_init_version_col, F.row_number().over(_init_w).cast("long"))
        _init_reason_col = scd2_cfg.get("change_reason_column", "_change_reason")
        if _init_reason_col:
            incoming_df = incoming_df.withColumn(_init_reason_col, F.lit("initial").cast("string"))

        # Inject the Kimball 'Unknown Member' (-1) on the INITIAL load too. The
        # merge path below does this, but the initial-load branch returns early —
        # so without this a freshly built dimension never gets its unknown member,
        # and facts have nothing to point unmatched keys at.
        _init_um_cfg = scd2_cfg.get("unknown_member") or {}
        if _init_um_cfg.get("enabled", True):
            incoming_df = _inject_unknown_member_spark(incoming_df, primary_key, scd2_cfg, _init_um_cfg)

        # No existing data, just write incoming
        writer = incoming_df.write.format(output_format)
        if is_table:
            _spark_save_as_table(writer, table_or_path, "overwrite", location)
        else:
            writer.mode("overwrite").save(table_or_path)
        rows_written = incoming_df.count()
        logger.info(f"Wrote {rows_written} SCD2 rows to {table_or_path} (initial load)")
        return {
            "target": table_or_path,
            "rows_written": rows_written,
            "format": output_format,
        }

    # Ensure existing has SCD2 columns
    if effective_from not in existing_df.columns:
        existing_df = existing_df.withColumn(effective_from, F.to_timestamp(F.lit(effective_from_default)))
    if effective_to not in existing_df.columns:
        if effective_to_default is None:
            existing_df = existing_df.withColumn(effective_to, F.lit(None).cast("timestamp"))
        else:
            existing_df = existing_df.withColumn(effective_to, F.to_timestamp(F.lit(effective_to_default)))
    if current_flag not in existing_df.columns:
        if effective_to_default is None:
            existing_df = existing_df.withColumn(current_flag, F.col(effective_to).isNull())
        else:
            existing_df = existing_df.withColumn(
                current_flag, F.col(effective_to) == F.to_timestamp(F.lit(effective_to_default))
            )

    # Get incoming keys
    incoming_keys = incoming_df.select(*primary_key).distinct()
    track_columns = scd2_cfg.get("track_columns")  # optional list of columns to watch

    # Candidate matches: existing current rows whose key appears in incoming
    candidates = existing_df.join(incoming_keys, on=primary_key, how="inner").filter(F.col(current_flag))

    if track_columns:
        # Join candidates with full incoming rows so we can compare field values
        inc_alias = incoming_df.select(
            *primary_key,
            *[F.col(c).alias(f"_inc_{c}") for c in track_columns if c in incoming_df.columns],
        ).distinct()
        candidates_with_inc = candidates.join(inc_alias, on=primary_key, how="left")

        # Build per-column change condition: NULLsafe inequality
        change_conditions = [
            F.col(c) != F.col(f"_inc_{c}")
            for c in track_columns
            if c in candidates.columns and c in incoming_df.columns
        ]
        if change_conditions:
            any_changed = change_conditions[0]
            for cond in change_conditions[1:]:
                any_changed = any_changed | cond
            records_to_close = candidates_with_inc.filter(any_changed).drop(*[f"_inc_{c}" for c in track_columns])
            # Incoming rows that actually triggered a change (used for new versions below)
            changed_keys = records_to_close.select(*primary_key).distinct()

            # Also keep completely new keys that aren't in the dimension yet
            existing_keys = existing_df.select(*primary_key).distinct()
            new_keys = incoming_keys.join(existing_keys, on=primary_key, how="left_anti").distinct()

            keys_to_keep = changed_keys.unionByName(new_keys)
            incoming_df = incoming_df.join(keys_to_keep, on=primary_key, how="inner")
        else:
            records_to_close = candidates
            # When track_columns has no changes, filter incoming to ONLY completely new keys!
            existing_keys = existing_df.select(*primary_key).distinct()
            new_keys = incoming_keys.join(existing_keys, on=primary_key, how="left_anti").distinct()
            incoming_df = incoming_df.join(new_keys, on=primary_key, how="inner")
    else:
        # Original behaviour: always close+version on any key match
        records_to_close = candidates

    # Records to keep unchanged: not matching incoming keys OR already closed
    unchanged = existing_df.join(incoming_keys, on=primary_key, how="left_anti")
    already_closed = existing_df.join(incoming_keys, on=primary_key, how="inner").filter(~F.col(current_flag))

    # Existing CURRENT rows whose key IS in incoming but is NOT being closed
    # (tracked columns unchanged) — retain them as-is. `unchanged` above only
    # covers keys ABSENT from incoming; without this, a re-run with unchanged data
    # drops every still-current row and the dimension collapses to just the
    # unknown-member row.
    _closing_keys = records_to_close.select(*primary_key).distinct()
    retained_current = candidates.join(_closing_keys, on=primary_key, how="left_anti")

    # Stamp effective_to on records being closed
    incoming_effective = incoming_df.select(*primary_key, F.col(effective_from).alias("_new_effective_from")).distinct()

    closed_records = (
        records_to_close.join(incoming_effective, on=primary_key, how="left")
        .withColumn(effective_to, F.col("_new_effective_from"))
        .withColumn(current_flag, F.lit(False))
        .drop("_new_effective_from")
    )

    # ── Change reason column (Spark) ────────────────────────────
    change_reason_col = scd2_cfg.get("change_reason_column", "_change_reason")
    if change_reason_col:
        # Incoming rows that matched existing keys: compute which fields changed
        if track_columns:
            # Join incoming with existing current rows to compare fields
            existing_current = existing_df.filter(F.col(current_flag))
            inc_with_existing = incoming_df.join(
                existing_current.select(
                    *primary_key,
                    *[F.col(c).alias(f"_old_{c}") for c in track_columns if c in existing_current.columns],
                ),
                on=primary_key,
                how="left",
            )
            # Build per-column change indicator
            change_parts = [
                F.when(
                    F.col(c) != F.col(f"_old_{c}"),
                    F.lit(c),
                )
                for c in track_columns
                if c in existing_current.columns
            ]
            if change_parts:
                reason_expr = F.concat_ws(
                    ",",
                    *change_parts,
                )
                # NULL _old_ columns mean new key → initial_load
                first_old = f"_old_{track_columns[0]}"
                incoming_df = inc_with_existing.withColumn(
                    change_reason_col,
                    F.when(
                        F.col(first_old).isNull(),
                        F.lit("initial_load"),
                    ).otherwise(reason_expr),
                ).drop(*[f"_old_{c}" for c in track_columns])
            else:
                incoming_df = incoming_df.withColumn(change_reason_col, F.lit("all"))
        else:
            # No track_columns → stamp "all" for existing keys, "initial_load" for new
            existing_keys = existing_df.select(*primary_key).distinct()
            incoming_df = (
                incoming_df.join(
                    existing_keys.withColumn("_existed", F.lit(True)),
                    on=primary_key,
                    how="left",
                )
                .withColumn(
                    change_reason_col,
                    F.when(
                        F.col("_existed").isNull(),
                        F.lit("initial_load"),
                    ).otherwise(F.lit("all")),
                )
                .drop("_existed")
            )

        # Unchanged and already_closed rows get NULL for change_reason
        unchanged = unchanged.withColumn(change_reason_col, F.lit(None).cast("string"))
        already_closed = already_closed.withColumn(change_reason_col, F.lit(None).cast("string"))
        closed_records = closed_records.withColumn(change_reason_col, F.lit(None).cast("string"))
        retained_current = retained_current.withColumn(change_reason_col, F.lit(None).cast("string"))

    # Align all columns
    all_columns = list(existing_df.columns)
    for col in incoming_df.columns:
        if col not in all_columns:
            all_columns.append(col)

    def align_columns(df, cols):
        for col in cols:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(None))
        return df.select(*cols)

    unchanged = align_columns(unchanged, all_columns)
    already_closed = align_columns(already_closed, all_columns)
    closed_records = align_columns(closed_records, all_columns)
    retained_current = align_columns(retained_current, all_columns)
    incoming_df = align_columns(incoming_df, all_columns)

    # Union all: unchanged (key not in incoming) + still-current (key in incoming,
    # unchanged) + already closed + newly closed + incoming (changed/new versions)
    result = unchanged.union(retained_current).union(already_closed).union(closed_records).union(incoming_df)

    # ── Surrogate key injection (Spark) ─────────────────────────
    sk_column = scd2_cfg.get("surrogate_key", "_sk")
    sk_strategy = scd2_cfg.get("surrogate_key_strategy", "hash")
    if sk_column:
        pk_concat = F.concat_ws(
            "|",
            *[F.col(c).cast("string") for c in primary_key],
            F.col(effective_from).cast("string"),
        )
        if sk_strategy == "uuid":
            result = result.withColumn(sk_column, F.expr("substring(uuid(), 1, 16)"))
        else:
            # Default: hash (deterministic, vectorized)
            result = result.withColumn(
                sk_column,
                F.substring(F.sha2(pk_concat, 256), 1, 16),
            )

    # ── Version number injection (Spark) ────────────────────────
    version_column = scd2_cfg.get("version_column", "_version")
    if version_column:
        from pyspark.sql.window import Window

        w = Window.partitionBy(*primary_key).orderBy(effective_from)
        # Cast to long: the initial-load path writes this column at the model's
        # declared type, and every integer model type maps to bigint. row_number()
        # is INT, so without this cast a re-run (merge path) collides with the
        # existing bigint column → DELTA_FAILED_TO_MERGE_FIELDS / METADATA_MISMATCH.
        result = result.withColumn(version_column, F.row_number().over(w).cast("long"))

    # ── Unknown member injection (Spark) ─────────────────────────
    unknown_cfg = scd2_cfg.get("unknown_member") or {}
    if unknown_cfg.get("enabled", True):
        result = _inject_unknown_member_spark(result, primary_key, scd2_cfg, unknown_cfg)

    # ── Enforce SCD2 Column Ordering (Spark / Kimball convention) ──
    # Surrogate key → natural keys / attributes → SCD2 control → internal metadata
    scd2_control_cols = []
    if effective_from in result.columns:
        scd2_control_cols.append(effective_from)
    if effective_to in result.columns:
        scd2_control_cols.append(effective_to)
    if current_flag in result.columns:
        scd2_control_cols.append(current_flag)
    if version_column and version_column in result.columns:
        scd2_control_cols.append(version_column)
    if change_reason_col and change_reason_col in result.columns:
        scd2_control_cols.append(change_reason_col)

    sk_head = [sk_column] if sk_column and sk_column in result.columns else []
    internal_tail = [c for c in result.columns if c.startswith("_lakelogic_")]
    tail_set = set(scd2_control_cols + internal_tail + sk_head)
    body = [c for c in result.columns if c not in tail_set]

    ordered_cols = sk_head + body + scd2_control_cols + internal_tail
    # Deduplicate while preserving order
    seen = set()
    ordered_dedup = []
    for c in ordered_cols:
        if c not in seen:
            seen.add(c)
            ordered_dedup.append(c)
    result = result.select(*ordered_dedup)

    writer = result.write.format(output_format)
    if is_table:
        _spark_save_as_table(writer, table_or_path, "overwrite", location)
    else:
        writer.mode("overwrite").save(table_or_path)

    rows_written = result.count()
    logger.info(f"Applied SCD2 with {rows_written} total rows to {table_or_path}")
    return {
        "target": table_or_path,
        "rows_written": rows_written,
        "format": output_format,
    }


def _spark_update_incremental_version(spark, table_name, version):  # pragma: no cover
    """Update the target table's properties with the last processed version."""
    try:
        # Use simple table name if it's a spark table reference
        clean_name = table_name[6:] if table_name.startswith("table:") else table_name
        spark.sql(f"ALTER TABLE {clean_name} SET TBLPROPERTIES ('lakelogic.last_source_version' = '{version}')")
        logger.info(f"Updated {clean_name} property lakelogic.last_source_version to {version}")
    except Exception as e:
        logger.warning(f"Failed to update table property for {clean_name}: {e}")


def _materialize_spark_dataframe(  # pragma: no cover
    df: Any,
    contract,
    target: Path,
    output_format: str,
    incremental_metadata: Optional[Dict[str, Any]] = None,
    is_reprocess: bool = False,
) -> Dict[str, Any]:
    """
    Materialize Spark DataFrames to a path (CSV, Parquet, Delta, Iceberg).

    Supports append, overwrite, merge, and scd2 strategies natively without
    collecting to pandas (avoiding driver memory issues at scale).

    Args:
        df: Spark DataFrame to write.
        contract: DataContract with materialization settings.
        target: Target path for output.
        output_format: Format to use for Spark write.

    Returns:
        Metadata about the write.
    """
    mat = contract.materialization
    strategy = (mat.strategy or "append").lower()
    partition_by = list(mat.partition_by or [])
    reprocess_policy = getattr(mat, "reprocess_policy", "overwrite_partition")
    primary_key = list(contract.primary_key or [])
    scd2_cfg = getattr(mat, "scd2", None)
    scd2_cfg = dict(scd2_cfg) if isinstance(scd2_cfg, dict) else {}
    # track_columns may be set at the materialization level (outside scd2:)
    if "track_columns" not in scd2_cfg:
        tc = getattr(mat, "track_columns", None)
        if tc:
            scd2_cfg["track_columns"] = tc
    # Pass unknown_member config into scd2_cfg for SCD2 functions
    unknown_member = getattr(mat, "unknown_member", None)
    if unknown_member and "unknown_member" not in scd2_cfg:
        scd2_cfg["unknown_member"] = dict(unknown_member) if isinstance(unknown_member, dict) else {}
    location = _resolve_external_location(getattr(mat, "location", None))

    spark = df.sparkSession
    target_str = str(target)

    # Handle merge strategy natively
    if strategy == "merge":
        if not primary_key:
            raise ValueError("primary_key is required for merge strategy.")

        scd1_cfg = getattr(mat, "scd1", None)
        scd1_cfg = dict(scd1_cfg) if isinstance(scd1_cfg, dict) else {}
        unknown_member = getattr(mat, "unknown_member", None)
        if unknown_member and "unknown_member" not in scd1_cfg:
            scd1_cfg["unknown_member"] = dict(unknown_member) if isinstance(unknown_member, dict) else {}

        cdc_op_field = getattr(contract.source, "cdc_op_field", None) if getattr(contract, "source", None) else None
        cdc_delete_values = (
            getattr(contract.source, "cdc_delete_values", None) if getattr(contract, "source", None) else None
        )
        cdc_timestamp_field = (
            getattr(contract.source, "cdc_timestamp_field", None) if getattr(contract, "source", None) else None
        )
        soft_delete_col = getattr(mat, "soft_delete_column", None)
        soft_delete_val = getattr(mat, "soft_delete_value", True)
        soft_delete_time_col = getattr(mat, "soft_delete_time_column", None)
        soft_delete_reason_col = getattr(mat, "soft_delete_reason_column", None)

        # ── Truncate a long transform lineage before the merge ───────────────
        # The DataFrame merge references `df` in BOTH the anti-join and the union,
        # duplicating its lineage within a single query plan. For contracts with
        # several transformations that makes Catalyst's plan grow super-linearly
        # and OOM the driver at plan-compile time. Materialising `df` once here
        # collapses it to a cache scan, so each reference downstream is trivial.
        # GATE: only when the contract has transformations — trivial pass-through
        # contracts have a short lineage that never explodes and shouldn't pay for
        # an extra materialisation pass.
        _merge_ckpt = None
        if getattr(contract, "transformations", None):
            try:
                df = df.localCheckpoint(eager=True)
                _merge_ckpt = df
                logger.debug("incoming_df checkpointed before merge (lineage truncated)")
            except Exception as _ck_err:  # pragma: no cover - no local checkpoint dirs
                logger.debug(f"localCheckpoint unavailable; using persist()+count(): {_ck_err}")
                df = df.persist()
                df.count()
                _merge_ckpt = df

        result = _spark_merge_dataframe(
            spark,
            df,
            target_str,
            primary_key,
            output_format,
            location=location,
            scd1_cfg=scd1_cfg,
            cdc_op_field=cdc_op_field,
            cdc_delete_values=cdc_delete_values,
            cdc_timestamp_field=cdc_timestamp_field,
            soft_delete_col=soft_delete_col,
            soft_delete_val=soft_delete_val,
            soft_delete_time_col=soft_delete_time_col,
            soft_delete_reason_col=soft_delete_reason_col,
            merge_dedup_guard=bool(getattr(mat, "merge_dedup_guard", False)),
        )

        # UNPERSIST: release the checkpoint's cached blocks now the merge has
        # committed, so they don't accumulate across contracts in a long
        # multi-contract SparkSession.
        if _merge_ckpt is not None:
            try:
                _merge_ckpt.unpersist()
            except Exception:  # pragma: no cover
                pass

        if target_str.startswith("table:"):
            table_name = target_str[6:]
            _spark_apply_table_metadata(spark, table_name, contract)
            if incremental_metadata and incremental_metadata.get("strategy") == "delta_version":
                tv = incremental_metadata.get("to_version")
                if tv is not None:
                    _spark_update_incremental_version(spark, table_name, tv)
        return result

    # Handle SCD2 strategy natively
    if strategy == "scd2":
        if not primary_key:
            raise ValueError("primary_key is required for scd2 strategy.")
        result = _spark_scd2_dataframe(
            spark,
            df,
            target_str,
            primary_key,
            scd2_cfg,
            output_format,
            location=location,
            merge_dedup_guard=bool(getattr(mat, "merge_dedup_guard", False)),
        )
        if target_str.startswith("table:"):
            table_name = target_str[6:]
            _spark_apply_table_metadata(spark, table_name, contract)
            if incremental_metadata and incremental_metadata.get("strategy") == "delta_version":
                tv = incremental_metadata.get("to_version")
                if tv is not None:
                    _spark_update_incremental_version(spark, table_name, tv)
        return result

    # Standard append/overwrite
    writer = df.write.format(output_format)
    if partition_by:
        # Prune partition columns not present in data — allows _system.yaml
        # to define a superset shared across multiple contracts.
        _spark_cols = set(df.columns)
        _missing_parts = [c for c in partition_by if c not in _spark_cols]
        if _missing_parts:
            logger.warning(
                f"Partition columns not present in data (pruned): {', '.join(_missing_parts)}. "
                f"This is expected when _system.yaml defines a superset of partition columns "
                f"shared across multiple contracts."
            )
            partition_by = [c for c in partition_by if c not in _missing_parts]
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    # Delta schema evolution — driven by contract server.schema_policy
    if output_format == "delta" and contract:
        server = contract.effective_server() if hasattr(contract, "effective_server") else None
        from lakelogic.core.models import SchemaPolicy as _SP

        _default_evo = _SP().evolution
        evolution = _default_evo
        if server and getattr(server, "schema_policy", None):
            evolution = getattr(server.schema_policy, "evolution", _default_evo) or _default_evo
        if evolution in ("append", "merge", "compatible", "allow"):
            writer = writer.option("mergeSchema", "true")
        elif evolution == "overwrite":
            writer = writer.option("overwriteSchema", "true")

    mode = "append" if strategy == "append" else "overwrite"
    if (
        strategy == "append"
        and is_reprocess
        and reprocess_policy
        in [
            "overwrite_partition",
            "overwrite_partition_safe",
        ]
    ):
        try:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        except Exception as exc:
            logger.warning(f"Could not set partitionOverwriteMode to dynamic: {exc}")
        mode = "overwrite"

    if target_str.startswith("table:"):
        table_name = target_str[len("table:") :]
        # For append mode on UC with external locations, the table must
        # already exist. On first run, fall back to overwrite to create it.
        if mode == "append":
            try:
                spark.table(table_name)
            except Exception:
                logger.info(f"Initialize `{table_name}` (overwrite mode)")
                mode = "overwrite"
        _spark_save_as_table(writer, table_name, mode, location)
        _spark_apply_table_metadata(spark, table_name, contract)
        if incremental_metadata and incremental_metadata.get("strategy") == "delta_version":
            tv = incremental_metadata.get("to_version")
            if tv is not None:
                _spark_update_incremental_version(spark, table_name, tv)
        logger.info(f"Materialized to `{table_name}` ({output_format})")
        return {
            "target": table_name,
            "rows_written": df.count(),
            "format": output_format,
        }

    writer.mode(mode).save(target_str)
    logger.info(f"Materialized Spark dataframe to {target_str} ({output_format})")
    return {"target": target_str, "rows_written": df.count(), "format": output_format}


def _partition_aware_merge(
    df: Any,
    contract,
    resolved_target: Path,
    resolved_format: str,
    strategy: str,
    partition_by: List[str],
    primary_key: List[str],
    mat,
    scd2_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    _delta_schema_mode = None
    if contract:
        _srv = contract.effective_server() if hasattr(contract, "effective_server") else None
        _evo = None
        if _srv and getattr(_srv, "schema_policy", None):
            _evo = getattr(_srv.schema_policy, "evolution", None)
        # Fallback: check _server_layer_defaults injected by runner from _system.yaml
        if _evo is None and hasattr(contract, "metadata") and contract.metadata:
            _layer_defaults = contract.metadata.get("_server_layer_defaults", {})
            _schema_policy = _layer_defaults.get("schema_policy", {})
            if isinstance(_schema_policy, dict):
                _evo = _schema_policy.get("evolution")
            if _evo is None:
                from lakelogic.core.models import SchemaPolicy as _SP

                _evo = _layer_defaults.get("schema_evolution", _SP().evolution)
        _evo = _evo or "allow"
        if _evo in ("append", "merge", "compatible", "allow"):
            _delta_schema_mode = "merge"
        elif _evo == "overwrite":
            _delta_schema_mode = "overwrite"
    """
    Perform merge or SCD2 within each affected partition independently.

    For **delta** format, writes a single root-level Delta table with Hive-style
    partition columns so that ``pl.read_delta(root)`` and
    ``spark.read.format("delta").load(root)`` work correctly.

    For all other formats, writes per-partition files under Hive-style
    subdirectories (original behaviour).

    Args:
        df: Incoming dataframe.
        contract: DataContract.
        resolved_target: Base target directory.
        resolved_format: Output format (csv, parquet, delta, …).
        strategy: "merge" or "scd2".
        partition_by: List of partition column names.
        primary_key: Primary key columns.
        mat: Materialization config.
        scd2_cfg: SCD2 configuration dict.

    Returns:
        Metadata dict with target, rows_written, format.
    """
    import pandas as pd

    pdf = _to_pandas(df)

    # ── Empty-frame guard ─────────────────────────────────────────────────────
    # On an incremental run where the watermark filter matched 0 new rows the
    # DataFrame has the correct schema but 0 rows.  There is nothing to write;
    # return a zero-rows metadata dict so callers don't raise on missing columns.
    if pdf.empty:
        import logging as _log

        _log.getLogger(__name__).info("materialize: empty DataFrame — no rows to write, skipping materialization.")
        return {
            "target": str(resolved_target),
            "rows_written": 0,
            "format": resolved_format,
        }

    if not primary_key:
        raise ValueError("primary_key is required for merge/scd2 strategy.")

    missing_parts = [c for c in partition_by if c not in pdf.columns]
    if missing_parts:
        logger.warning(
            f"Partition columns not present in data (pruned): {', '.join(missing_parts)}. "
            f"This is expected when _system.yaml defines a superset of partition columns "
            f"shared across multiple contracts."
        )
        partition_by = [c for c in partition_by if c not in missing_parts]
    missing_pk = [c for c in primary_key if c not in pdf.columns]
    if missing_pk:
        raise ValueError(f"Primary key columns missing from data: {', '.join(missing_pk)}")

    base_dir = resolved_target
    base_dir.mkdir(parents=True, exist_ok=True)
    total_rows = 0

    cdc_op_field = getattr(contract.source, "cdc_op_field", None) if contract.source else None
    cdc_delete_values = getattr(contract.source, "cdc_delete_values", None) if contract.source else None
    cdc_timestamp_field = getattr(contract.source, "cdc_timestamp_field", None) if contract.source else None
    soft_delete_col = getattr(mat, "soft_delete_column", None)
    soft_delete_val = getattr(mat, "soft_delete_value", True)
    soft_delete_time_col = getattr(mat, "soft_delete_time_column", None)
    soft_delete_reason_col = getattr(mat, "soft_delete_reason_column", None)

    # ── Delta format: single root-level Delta table ───────────────────────────
    # write_deltalake writes Hive-style partition dirs under a single _delta_log
    # at the root so pl.read_delta(base_dir) works correctly.
    #
    # Strategy:
    #   First write  → write_deltalake(mode="overwrite")  — creates the table
    #   Subsequent   → DeltaTable.merge() MERGE INTO keyed on primary_key
    #                  No partition_filters needed; stable across all versions.
    if resolved_format == "delta":
        try:
            from deltalake import write_deltalake, DeltaTable
            import pyarrow as pa
        except ImportError:
            raise ImportError(
                "Writing partitioned Delta tables requires deltalake and pyarrow: pip install deltalake pyarrow"
            )

        target_str = str(base_dir)
        # Build storage_options for cloud targets (ADLS, S3, GCS)
        _part_opts = _build_storage_options() if _is_remote_path(target_str) else None
        # Detect existing Delta table — local Path check won't work for cloud
        if _is_remote_path(target_str):
            try:
                DeltaTable(target_str, storage_options=_part_opts)
                table_exists = True
            except Exception:
                table_exists = False
        else:
            table_exists = base_dir.joinpath("_delta_log").exists()

        # Pre-merge all incoming partitions into their final state first,
        # then write in a single pass to minimise Delta log transactions.
        merged_parts: list = []

        for part_values, group in _partition_groups(pdf, partition_by):
            if group.empty:
                continue

            if strategy in ("merge", "scd2") and table_exists:
                existing = pd.DataFrame(columns=group.columns)
                try:
                    dt = DeltaTable(target_str, storage_options=_part_opts)
                    part_filter = [(col, "=", str(val)) for col, val in part_values.items()]
                    existing = dt.to_pandas(filters=part_filter)
                    logger.debug(
                        f"Partition-aware {strategy}: read {len(existing)} existing rows for partition {part_values}"
                    )
                except Exception as exc:
                    logger.warning(f"Could not read existing Delta partition for merge/scd2: {exc}")

                if strategy == "merge":
                    merged = _merge_frames(
                        existing,
                        group,
                        primary_key,
                        soft_delete_col=soft_delete_col,
                        soft_delete_val=soft_delete_val,
                        soft_delete_time_col=soft_delete_time_col,
                        soft_delete_reason_col=soft_delete_reason_col,
                        cdc_op_field=cdc_op_field,
                        cdc_delete_values=cdc_delete_values,
                        cdc_timestamp_field=cdc_timestamp_field,
                        merge_dedup_guard=bool(getattr(mat, "merge_dedup_guard", False)),
                    )
                else:
                    merged = _scd2_frames(existing, group, primary_key, scd2_cfg)
            else:
                if strategy == "scd2":
                    # First write with SCD2 — route through _scd2_frames so
                    # control columns (effective_from, effective_to, is_current,
                    # surrogate key, _version) are populated on initial load.
                    empty_existing = pd.DataFrame(columns=group.columns)
                    merged = _scd2_frames(empty_existing, group, primary_key, scd2_cfg)
                else:
                    # First write or plain append — seed soft-delete columns
                    # so the schema includes them from table creation.
                    merged = _seed_soft_delete_columns_pandas(
                        group,
                        soft_delete_col,
                        soft_delete_time_col,
                        soft_delete_reason_col,
                        cdc_op_field,
                        cdc_delete_values,
                        soft_delete_val,
                        cdc_timestamp_field,
                    )

            if not merged.empty:
                merged_parts.append(merged)

        if not merged_parts:
            logger.info(f"Partition-aware {strategy} (delta): no rows to write → {base_dir}")
            return {
                "target": str(base_dir),
                "rows_written": 0,
                "format": resolved_format,
            }

        combined = pd.concat(merged_parts, ignore_index=True)

        # ── Unknown member injection (once, after all partitions merged) ──
        if strategy == "scd2" and scd2_cfg:
            unknown_cfg = scd2_cfg.get("unknown_member")
            if unknown_cfg is not None and unknown_cfg.get("enabled", True):
                combined = _inject_unknown_member_pandas(combined, primary_key, scd2_cfg, unknown_cfg)

        # Spark / Kimball convention: surrogate key → natural keys →
        # dimension attributes → SCD2 control columns → internal metadata.
        if strategy == "scd2" and scd2_cfg:
            _sk = scd2_cfg.get("surrogate_key", "_sk")
            _ef = scd2_cfg.get("effective_from_field", "effective_from")
            _et = scd2_cfg.get("effective_to_field", "effective_to")
            _cf = scd2_cfg.get("current_flag_field", "is_current")
            _vc = scd2_cfg.get("version_column", "_version")
            _cr = scd2_cfg.get("change_reason_column", "_change_reason")

            # SCD2 control columns in canonical order
            scd2_tail = [c for c in [_ef, _et, _cf, _vc, _cr] if c in combined.columns]
            # Internal/lineage columns always last
            internal_tail = [c for c in combined.columns if c.startswith("_lakelogic_")]
            # Surrogate key first
            sk_head = [_sk] if _sk in combined.columns else []

            tail_set = set(scd2_tail + internal_tail + sk_head)
            body = [c for c in combined.columns if c not in tail_set]

            # Final order: SK → natural keys / attributes → SCD2 control → lineage
            ordered = sk_head + body + scd2_tail + internal_tail
            # Deduplicate while preserving order
            seen = set()
            ordered_dedup = []
            for c in ordered:
                if c not in seen:
                    seen.add(c)
                    ordered_dedup.append(c)
            combined = combined[ordered_dedup]

        arrow_table = pa.Table.from_pandas(combined, preserve_index=False)
        arrow_table = _sanitize_arrow_nulls(arrow_table)  # Delta rejects Arrow Null type
        total_rows = len(combined)

        if not table_exists:
            # ── First write: create the root Delta table ──────────────────
            _safe_write_deltalake(
                target_str,
                arrow_table,
                partition_by=partition_by,
                mode="overwrite",
                schema_mode=_delta_schema_mode,
                storage_options=_part_opts,
            )
        elif strategy == "scd2":
            # ── SCD2: partition-scoped overwrite ──────────────────────────
            # _scd2_frames returns the FULL desired state per partition
            # (existing + closed records + new versions).  We cannot use
            # Delta MERGE because a single PK can have multiple rows
            # (v1 closed + v2 current) which violates MERGE's unique-key
            # requirement.  Instead, build a partition predicate and
            # overwrite only the affected partitions.
            affected_parts = combined[partition_by].drop_duplicates()
            part_predicates = []
            for _, row in affected_parts.iterrows():
                clause = " AND ".join(f"{col} = '{row[col]}'" for col in partition_by)
                part_predicates.append(f"({clause})")
            partition_predicate = " OR ".join(part_predicates) if part_predicates else None

            _safe_write_deltalake(
                target_str,
                arrow_table,
                mode="overwrite",
                predicate=partition_predicate,
                schema_mode=_delta_schema_mode,
                storage_options=_part_opts,
            )
        elif primary_key:
            # ── Subsequent writes: MERGE INTO via DeltaTable.merge() ─────
            # Stable across all deltalake versions; no partition_filters API.
            pk_predicate = " AND ".join(f"source.{pk} = target.{pk}" for pk in primary_key)
            dt = DeltaTable(target_str, storage_options=_part_opts)
            (
                dt.merge(
                    source=arrow_table,
                    predicate=pk_predicate,
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        else:
            # No primary key defined → plain append
            write_deltalake(
                target_str,
                arrow_table,
                partition_by=partition_by,
                mode="append",
                schema_mode=_delta_schema_mode,
                storage_options=_part_opts,
            )

        logger.info(f"Partition-aware {strategy} (delta): materialized {total_rows} rows → {base_dir}")
        _maybe_compact_delta(str(base_dir), contract)
        return {
            "target": str(base_dir),
            "rows_written": total_rows,
            "format": resolved_format,
        }

    # ── Non-delta formats: per-partition file approach (original behaviour) ───
    for part_values, group in _partition_groups(pdf, partition_by):
        # Resolve the partition directory
        part_dir = base_dir
        for col, val in part_values.items():
            part_dir = part_dir / f"{col}={_safe_partition_value(val)}"

        part_dir.mkdir(parents=True, exist_ok=True)
        part_file = part_dir / f"data.{resolved_format}"

        # Load existing partition data (if any)
        if part_file.exists():
            existing = _read_frame(part_file, resolved_format)
        else:
            existing = pd.DataFrame(columns=group.columns)

        # Perform merge or SCD2 within this partition
        if strategy == "merge":
            merged = _merge_frames(
                existing,
                group,
                primary_key,
                soft_delete_col=soft_delete_col,
                soft_delete_val=soft_delete_val,
                soft_delete_time_col=soft_delete_time_col,
                soft_delete_reason_col=soft_delete_reason_col,
                cdc_op_field=cdc_op_field,
                cdc_delete_values=cdc_delete_values,
                merge_dedup_guard=bool(getattr(mat, "merge_dedup_guard", False)),
            )
        elif strategy == "scd2":
            merged = _scd2_frames(existing, group, primary_key, scd2_cfg)
            # Inject unknown member once (non-delta partition loop)
            unknown_cfg = scd2_cfg.get("unknown_member") if scd2_cfg else None
            if unknown_cfg is not None and unknown_cfg.get("enabled", True):
                merged = _inject_unknown_member_pandas(merged, primary_key, scd2_cfg, unknown_cfg)
        else:
            merged = group

        _write_frame(merged, part_file, resolved_format)
        total_rows += len(merged)

    logger.info(f"Partition-aware {strategy}: materialized {total_rows} rows across affected partitions → {base_dir}")
    return {
        "target": str(base_dir),
        "rows_written": total_rows,
        "format": resolved_format,
    }


def _run_secondary_targets(
    mat, contract, df, strategy: str, primary_key: list, rows_written: int, result: dict
) -> dict:
    """
    Execute secondary materialization targets (dual-write) after the primary write.

    Reads the ``secondary_targets`` list from the contract's materialization
    block and writes the same data to each target via dlt.

    Each secondary target supports:
      - ``fail_on_error`` (bool, default False): If True, a failed secondary
        write will re-raise the exception, failing the entire contract.
        Recommended for production targets.

    Args:
        mat: Materialization config.
        contract: DataContract instance.
        df: The processed DataFrame (pandas/polars/Arrow).
        strategy: Primary materialization strategy (merge/append/overwrite).
        primary_key: Primary key columns.
        rows_written: Number of rows written to the primary target.
        result: Primary write result dict (mutated in place).

    Returns:
        The enriched result dict with ``secondary_writes`` if any.
    """
    secondary_targets = getattr(mat, "secondary_targets", None)
    if not secondary_targets or not isinstance(secondary_targets, list):
        return result

    # Destinations that work without explicit credentials
    _LOCAL_DESTINATIONS = {"duckdb", "filesystem", "motherduck", "weaviate"}

    result["secondary_writes"] = []
    for i, sec in enumerate(secondary_targets):
        sec_format = sec.get("format", "dlt")
        _raw_table = sec.get("table_name")
        # "auto" or missing → derive from the contract's dataset name
        sec_table = getattr(contract, "dataset", "data") if (not _raw_table or _raw_table == "auto") else _raw_table
        fail_on_error = sec.get("fail_on_error", False)
        try:
            if sec_format == "dlt":
                import dlt as _dlt
                import pyarrow as pa

                destination = sec.get("dlt_destination", "duckdb")
                credentials = sec.get("dlt_credentials")
                dataset_name = sec.get("dlt_dataset_name", "lakelogic")

                # Build destination kwargs dynamically
                dest_kwargs = {}
                if credentials:
                    dest_kwargs["credentials"] = credentials
                for k, v in sec.items():
                    if k.startswith("dlt_") and k not in ("dlt_destination", "dlt_credentials", "dlt_dataset_name"):
                        dest_kwargs[k[4:]] = v

                # ── Credentials validation ──────────────────────────────
                if not credentials and destination not in _LOCAL_DESTINATIONS and "credentials" not in dest_kwargs:
                    # Check if dlt can resolve from env vars / secrets.toml
                    import os

                    env_key = f"DESTINATION__{destination.upper()}__CREDENTIALS"
                    env_cred = os.environ.get(env_key)
                    if not env_cred:
                        msg = (
                            f"Secondary target [{i}]: No credentials for '{destination}'. "
                            f"Set 'dlt_credentials' in the contract or the "
                            f"'{env_key}' environment variable."
                        )
                        if fail_on_error:
                            raise ValueError(msg)
                        logger.warning(f"⚠️ {msg} — the write may fail.")

                write_disp = {
                    "merge": "merge",
                    "append": "append",
                    "overwrite": "replace",
                }.get(strategy, "append")

                # Convert to Arrow for efficient transfer
                if isinstance(df, pa.Table):
                    arrow_data = df
                elif hasattr(df, "to_arrow"):
                    arrow_data = df.to_arrow()
                elif hasattr(df, "values"):  # pandas
                    arrow_data = pa.Table.from_pandas(df)
                else:
                    arrow_data = df

                pk = primary_key if primary_key else None

                @_dlt.resource(
                    name=sec_table,
                    write_disposition=write_disp,
                    primary_key=pk,
                )
                def _secondary_sink():
                    yield arrow_data

                pipeline = _dlt.pipeline(
                    pipeline_name=f"lakelogic_{sec_table}_secondary",
                    destination=_dlt.destinations.__dict__.get(destination, destination)(**dest_kwargs)
                    if dest_kwargs
                    else destination,
                    dataset_name=dataset_name,
                )

                pipeline.run(_secondary_sink())
                logger.info(
                    f"Secondary target [{i}]: {sec_table} \u2192 {destination} ({write_disp}, {rows_written} rows)"
                )
                result["secondary_writes"].append(
                    {
                        "target": f"{destination}:{dataset_name}.{sec_table}",
                        "format": "dlt",
                        "dlt_destination": destination,
                        "rows_written": rows_written,
                    }
                )
            else:
                logger.warning(f"Secondary target [{i}]: unsupported format '{sec_format}' (only 'dlt' is supported)")
        except Exception as e:
            logger.error(f"Secondary target [{i}] ({sec_format}\u2192{sec.get('dlt_destination', '?')}): {e}")
            result["secondary_writes"].append(
                {
                    "target": sec.get("dlt_destination", sec_format),
                    "error": str(e),
                }
            )
            if fail_on_error:
                raise

    return result


def write_to_secondary_targets(
    secondary_targets: list,
    df: Any,
    table_name: str,
    *,
    strategy: str = "append",
    primary_key: Optional[list] = None,
) -> list:
    """Public helper to write a DataFrame to secondary dlt targets.

    Used by run_log and quarantine modules to fan out writes to the
    same secondary databases configured globally via ``materialization._all``.

    Args:
        secondary_targets: List of secondary target dicts from contract config.
        df: DataFrame (pandas/polars/Arrow) to write.
        table_name: Destination table name for the secondary write.
        strategy: Write strategy (append/merge/overwrite).
        primary_key: Primary key columns for merge.

    Returns:
        List of secondary write result dicts.
    """
    if not secondary_targets or not isinstance(secondary_targets, list):
        return []

    _LOCAL_DESTINATIONS = {"duckdb", "filesystem", "motherduck", "weaviate"}
    results = []

    for i, sec in enumerate(secondary_targets):
        sec_format = sec.get("format", "dlt")
        fail_on_error = sec.get("fail_on_error", False)
        try:
            if sec_format == "dlt":
                import dlt as _dlt
                import pyarrow as pa

                destination = sec.get("dlt_destination", "duckdb")
                credentials = sec.get("dlt_credentials")
                dataset_name = sec.get("dlt_dataset_name", "lakelogic")

                # Build destination kwargs dynamically
                dest_kwargs = {}
                if credentials:
                    dest_kwargs["credentials"] = credentials
                for k, v in sec.items():
                    if k.startswith("dlt_") and k not in ("dlt_destination", "dlt_credentials", "dlt_dataset_name"):
                        dest_kwargs[k[4:]] = v

                # Credentials validation
                if not credentials and destination not in _LOCAL_DESTINATIONS and "credentials" not in dest_kwargs:
                    import os

                    env_key = f"DESTINATION__{destination.upper()}__CREDENTIALS"
                    env_cred = os.environ.get(env_key)
                    if not env_cred:
                        msg = (
                            f"Secondary target [{i}]: No credentials for '{destination}'. "
                            f"Set 'dlt_credentials' in the contract or the "
                            f"'{env_key}' environment variable."
                        )
                        if fail_on_error:
                            raise ValueError(msg)
                        logger.warning(f"\u26a0\ufe0f {msg} \u2014 the write may fail.")

                write_disp = {
                    "merge": "merge",
                    "append": "append",
                    "overwrite": "replace",
                }.get(strategy, "append")

                # Convert to Arrow
                if isinstance(df, pa.Table):
                    arrow_data = df
                elif hasattr(df, "to_arrow"):
                    arrow_data = df.to_arrow()
                elif hasattr(df, "values"):  # pandas
                    arrow_data = pa.Table.from_pandas(df)
                else:
                    arrow_data = df

                pk = primary_key if primary_key else None
                rows = arrow_data.num_rows if hasattr(arrow_data, "num_rows") else 0
                sec_table = sec.get("table_name", table_name)

                @_dlt.resource(
                    name=sec_table,
                    write_disposition=write_disp,
                    primary_key=pk,
                )
                def _sec_sink():
                    yield arrow_data

                pipeline = _dlt.pipeline(
                    pipeline_name=f"lakelogic_{sec_table}_secondary",
                    destination=_dlt.destinations.__dict__.get(destination, destination)(**dest_kwargs)
                    if dest_kwargs
                    else destination,
                    dataset_name=dataset_name,
                )

                pipeline.run(_sec_sink())
                logger.info(f"Secondary target [{i}]: {sec_table} \u2192 {destination} ({write_disp}, {rows} rows)")
                results.append(
                    {
                        "target": f"{destination}:{dataset_name}.{sec_table}",
                        "format": "dlt",
                        "dlt_destination": destination,
                        "rows_written": rows,
                    }
                )
            else:
                logger.warning(f"Secondary target [{i}]: unsupported format '{sec_format}'")
        except Exception as e:
            logger.error(f"Secondary target [{i}] ({table_name}\u2192{sec.get('dlt_destination', '?')}): {e}")
            results.append({"target": sec.get("dlt_destination", sec_format), "error": str(e)})
            if fail_on_error:
                raise

    return results


def _write_iceberg_via_catalog(df, table_identifier: str, contract, mat, strategy: str = "append") -> Dict[str, Any]:
    """Create/update a **catalog-registered** Iceberg table via pyiceberg.

    Lets non-Spark engines (polars/duckdb) write the SAME catalog-registered
    Iceberg tables Spark uses — the open, multi-engine promise of Iceberg — rather
    than falling back to a path-based (Hadoop-catalog) table that no catalog
    (e.g. AWS Glue / Athena) can see.

    ``table_identifier`` is the target with the ``table:`` prefix stripped, e.g.
    ``glue.reference.dim_country`` — the first segment is the catalog name and the
    remainder is ``namespace.table``. Catalog connection props resolve from contract
    metadata (``iceberg_catalog_type|region|uri|warehouse``) then env
    (``ICEBERG_CATALOG_*`` / ``AWS_REGION`` / ``ICEBERG_WAREHOUSE``), defaulting to
    Glue. Write op follows the materialization ``strategy``: overwrite → overwrite,
    merge/scd2 → upsert on the primary key (pyiceberg), else append.
    """
    try:
        from pyiceberg.catalog import load_catalog
    except ImportError:
        raise ValueError("Catalog-registered Iceberg writes require pyiceberg: pip install pyiceberg")
    import pyarrow as pa

    metadata = getattr(contract, "metadata", None) or {}
    parts = [p for p in str(table_identifier).split(".") if p]
    if len(parts) < 2:
        raise ValueError(f"Iceberg catalog target must be 'catalog.namespace.table', got '{table_identifier}'")
    catalog_name = parts[0]
    identifier = ".".join(parts[1:])           # namespace.table (pyiceberg identifier)
    namespace = ".".join(parts[1:-1]) or "default"

    props: Dict[str, Any] = {"type": (metadata.get("iceberg_catalog_type") or os.getenv("ICEBERG_CATALOG_TYPE") or "glue").lower()}
    region = metadata.get("iceberg_catalog_region") or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if region:
        props["glue.region"] = region
        props["s3.region"] = region
    uri = (metadata.get("iceberg_catalog_uri") or os.getenv("ICEBERG_CATALOG_URI")
           or os.getenv("PYICEBERG_CATALOG__DEFAULT__URI"))
    if uri:
        props["uri"] = uri
    warehouse = (metadata.get("iceberg_catalog_warehouse") or os.getenv("ICEBERG_CATALOG_WAREHOUSE")
                 or os.getenv("ICEBERG_WAREHOUSE"))
    if warehouse:
        props["warehouse"] = warehouse

    catalog = load_catalog(catalog_name, **props)

    if hasattr(df, "to_arrow"):            # polars
        arrow = df.to_arrow()
    elif hasattr(df, "to_arrow_table"):    # duckdb relation
        arrow = df.to_arrow_table()
    else:
        arrow = pa.Table.from_pandas(df, preserve_index=False)
    arrow = _sanitize_arrow_nulls(arrow)

    try:
        catalog.create_namespace_if_not_exists(namespace)
    except AttributeError:  # older pyiceberg
        try:
            catalog.create_namespace(namespace)
        except Exception:
            pass
    except Exception:
        pass

    try:
        tbl = catalog.load_table(identifier)
        exists = True
    except Exception:
        tbl = catalog.create_table(identifier, schema=arrow.schema)
        exists = False

    primary_key = list(getattr(contract, "primary_key", None) or [])
    if not exists:
        tbl.append(arrow); op = "create+append"
    elif strategy == "overwrite":
        tbl.overwrite(arrow); op = "overwrite"
    elif strategy in ("merge", "scd2", "upsert") and primary_key and hasattr(tbl, "upsert"):
        tbl.upsert(arrow, join_cols=primary_key); op = "upsert"
    else:
        tbl.append(arrow); op = "append"

    logger.info(f"Materialized {arrow.num_rows} rows to Iceberg catalog table {catalog_name}.{identifier} (op={op})")
    return {"target": f"{catalog_name}.{identifier}", "rows_written": arrow.num_rows, "format": "iceberg", "op": op}


def _write_ducklake_via_catalog(df, table_identifier: str, contract, mat, strategy: str = "append") -> Dict[str, Any]:
    """Create/update a table in a **DuckLake** catalog (DuckDB's ``ducklake`` extension).

    DuckLake is DuckDB's open lakehouse format — a SQL-database catalog (SQLite/DuckDB/
    Postgres) plus Parquet data, with snapshots/time-travel. This materialises a batch
    into a DuckLake table so contracts land in DuckLake the same way they do in
    Delta/Iceberg — pure local DuckDB, no cloud or JVM.

    ``table_identifier`` is the target with the ``table:`` prefix stripped, e.g.
    ``rideflow_lake.marketplace.bronze_trips`` — first segment is the attached catalog
    name, then ``schema.table``. Catalog metadata + data paths resolve from contract
    metadata (``ducklake_metadata`` / ``ducklake_data_path``) then env
    (``DUCKLAKE_METADATA`` / ``DUCKLAKE_DATA_PATH``). Write op follows ``strategy``:
    overwrite → CREATE OR REPLACE, merge/scd2 → delete-by-PK + insert, else append.
    """
    import duckdb
    import pyarrow as pa

    metadata = getattr(contract, "metadata", None) or {}
    parts = [p for p in str(table_identifier).split(".") if p]
    if len(parts) < 2:
        raise ValueError(f"DuckLake target must be 'catalog.[schema.]table', got '{table_identifier}'")
    catalog = parts[0]
    schema = parts[1] if len(parts) >= 3 else "main"
    table = parts[-1]
    fq = f'"{catalog}"."{schema}"."{table}"'

    meta_path = (metadata.get("ducklake_metadata") or os.getenv("DUCKLAKE_METADATA")
                 or "./lakehouse/ducklake_catalog.ducklake")
    data_path = (metadata.get("ducklake_data_path") or os.getenv("DUCKLAKE_DATA_PATH")
                 or "./lakehouse/ducklake_data/")
    # Local catalog/data → ensure dirs exist. Remote targets (MotherDuck 'md:',
    # cloud object storage 's3://'/'gs://'/'abfss://') are managed server-side.
    def _is_remote(p: str) -> bool:
        p = str(p)
        return "://" in p or p.startswith("md:")

    if not _is_remote(data_path):
        os.makedirs(data_path, exist_ok=True)
    if not _is_remote(meta_path):
        _meta_dir = os.path.dirname(meta_path)
        if _meta_dir:
            os.makedirs(_meta_dir, exist_ok=True)

    if hasattr(df, "to_arrow"):
        arrow = df.to_arrow()
    elif hasattr(df, "to_arrow_table"):
        arrow = df.to_arrow_table()
    else:
        arrow = pa.Table.from_pandas(df, preserve_index=False)
    arrow = _sanitize_arrow_nulls(arrow)

    # Two DuckLake backends, shared write logic below:
    #  * MotherDuck — metadata 'md:...' (or ducklake_backend=motherduck): create a
    #    NATIVE DuckLake database. ('ATTACH ducklake:md:' is rejected in MotherDuck
    #    "workspace mode".) Requires the motherduck_token env var.
    #  * Local — attach a file-backed DuckLake catalog + local/S3 data path.
    _motherduck = str(meta_path).startswith("md:") or (metadata.get("ducklake_backend") or "").lower() == "motherduck"
    con = duckdb.connect("md:") if _motherduck else duckdb.connect()
    try:
        con.execute("INSTALL ducklake; LOAD ducklake;")
        if _motherduck:
            con.execute(f'CREATE DATABASE IF NOT EXISTS "{catalog}" (TYPE DUCKLAKE)')
        else:
            con.execute(f"ATTACH 'ducklake:{meta_path}' AS \"{catalog}\" (DATA_PATH '{data_path}')")
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."{schema}"')
        con.register("_ll_incoming", arrow)

        exists = con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? AND table_name = ?",
            [catalog, schema, table],
        ).fetchone()[0] > 0
        primary_key = list(getattr(contract, "primary_key", None) or [])

        if not exists:
            con.execute(f"CREATE TABLE {fq} AS SELECT * FROM _ll_incoming")
            op = "create"
        elif strategy == "overwrite":
            con.execute(f"CREATE OR REPLACE TABLE {fq} AS SELECT * FROM _ll_incoming")
            op = "overwrite"
        elif strategy in ("merge", "scd2", "upsert") and primary_key:
            _on = " AND ".join([f'{fq}."{k}" = i."{k}"' for k in primary_key])
            con.execute(f"DELETE FROM {fq} WHERE EXISTS (SELECT 1 FROM _ll_incoming i WHERE {_on})")
            con.execute(f"INSERT INTO {fq} SELECT * FROM _ll_incoming")
            op = "upsert"
        else:
            con.execute(f"INSERT INTO {fq} SELECT * FROM _ll_incoming")
            op = "append"
    finally:
        con.close()

    logger.info(f"Materialized {arrow.num_rows} rows to DuckLake table {catalog}.{schema}.{table} (op={op})")
    return {"target": f"{catalog}.{schema}.{table}", "rows_written": arrow.num_rows, "format": "ducklake", "op": op}


def materialize_dataframe(
    df: Any,
    contract,
    target_path: Optional[Path] = None,
    *,
    output_format: Optional[str] = None,
    engine_name: Optional[str] = None,
    storage_options: Optional[Dict[str, str]] = None,
    incremental_metadata: Optional[Dict[str, Any]] = None,
    is_reprocess: bool = False,
) -> Dict[str, Any]:
    """
    Materialize validated data to the configured target.

    Args:
        df: Engine dataframe (polars/pandas/duckdb/spark).
        contract: DataContract with materialization settings.
        target_path: Optional override target path.
        output_format: Optional override output format.
        engine_name: Optional engine name for engine-specific write paths.

    Returns:
        Metadata about the write (target, rows_written, format).
    """
    if contract is None or contract.materialization is None:
        return {}

    mat = contract.materialization
    resolved_target, resolved_format = _resolve_target(contract, target_path)
    _delta_schema_mode = None
    if contract:
        _srv = contract.effective_server() if hasattr(contract, "effective_server") else None
        _evo = None
        if _srv and getattr(_srv, "schema_policy", None):
            _evo = getattr(_srv.schema_policy, "evolution", None)
        # Fallback: check _server_layer_defaults injected by runner from _system.yaml
        if _evo is None and hasattr(contract, "metadata") and contract.metadata:
            _layer_defaults = contract.metadata.get("_server_layer_defaults", {})
            _schema_policy = _layer_defaults.get("schema_policy", {})
            if isinstance(_schema_policy, dict):
                _evo = _schema_policy.get("evolution")
            if _evo is None:
                _evo = _layer_defaults.get("schema_evolution", "strict")
        _evo = _evo or "strict"
        if _evo in ("append", "merge", "compatible", "allow"):
            _delta_schema_mode = "merge"
        elif _evo == "overwrite":
            _delta_schema_mode = "overwrite"

    if output_format:
        resolved_format = output_format
    if resolved_format:
        resolved_format = resolved_format.lower()

    if resolved_target is None or resolved_format is None:
        logger.warning("Materialization skipped: target path or format could not be resolved.")
        return {}

    # For non-Spark engines, 'table:' targets (Unity Catalog) are not
    # accessible.  Fall back to the materialization 'location' field which
    # contains the actual storage path (e.g. abfss://...).
    resolved_str = str(resolved_target)
    # Non-Spark + Iceberg + a catalog-qualified ('table:') target → write through
    # pyiceberg to the configured catalog (e.g. AWS Glue), so polars/duckdb create
    # and update the SAME catalog-registered Iceberg tables Spark uses, instead of
    # falling back to a path-based (Hadoop-catalog) table no catalog can see.
    if engine_name != "spark" and resolved_format == "iceberg" and resolved_str.startswith("table:"):
        return _write_iceberg_via_catalog(
            df, resolved_str[6:], contract, mat, strategy=(mat.strategy or "append").lower()
        )
    # DuckLake (DuckDB's SQL-catalog lakehouse) — a catalog write, not a file path.
    if resolved_format == "ducklake" and resolved_str.startswith("table:"):
        return _write_ducklake_via_catalog(
            df, resolved_str[6:], contract, mat, strategy=(mat.strategy or "append").lower()
        )
    if resolved_str.startswith("table:") and engine_name != "spark":
        location = getattr(mat, "location", None)
        if location:
            logger.info(f"Non-Spark engine '{engine_name}': falling back from table target to location: {location}")
            resolved_target = URIPath(str(location))
        else:
            logger.warning(
                f"Non-Spark engine '{engine_name}': table target '{resolved_str}' "
                f"is not supported and no 'location' fallback is configured."
            )
            return {}

    if engine_name == "spark" and hasattr(df, "sparkSession") and hasattr(df, "write"):  # pragma: no cover
        return _materialize_spark_dataframe(
            df,
            contract,
            resolved_target,
            resolved_format,
            incremental_metadata=incremental_metadata,
            is_reprocess=is_reprocess,
        )

    strategy = (mat.strategy or "append").lower()
    partition_by = list(mat.partition_by or [])
    reprocess_policy = getattr(mat, "reprocess_policy", "overwrite_partition")

    primary_key = list(contract.primary_key or [])
    scd2_cfg = getattr(mat, "scd2", None)
    scd2_cfg = dict(scd2_cfg) if isinstance(scd2_cfg, dict) else {}
    # track_columns may be set at the materialization level (outside scd2:)
    if "track_columns" not in scd2_cfg:
        tc = getattr(mat, "track_columns", None)
        if tc:
            scd2_cfg["track_columns"] = tc

    if partition_by and strategy in ["merge", "scd2"]:
        # Partition-aware merge: merge/scd2 within each affected partition
        return _partition_aware_merge(
            df,
            contract,
            resolved_target,
            resolved_format,
            strategy,
            partition_by,
            primary_key,
            mat,
            scd2_cfg,
        )

    is_dir_target = bool(partition_by) or resolved_target.suffix == ""
    if resolved_target.exists() and resolved_target.is_dir():
        is_dir_target = True

    if is_dir_target:
        resolved_target.mkdir(parents=True, exist_ok=True)
        # Delta tables ARE the directory — never append data.delta to the path.
        # The _delta_log lives directly inside resolved_target.
        if resolved_format == "delta":
            target_file = resolved_target
        else:
            target_file = resolved_target / f"data.{resolved_format}"
    else:
        target_file = resolved_target
        if target_file.suffix == "":
            target_file = target_file.with_suffix(f".{resolved_format}")
        target_file.parent.mkdir(parents=True, exist_ok=True)

    if not _frame_has_columns(df):
        logger.info("Materialization skipped: dataframe has no columns (empty incremental batch).")
        return {
            "target": str(target_file),
            "rows_written": 0,
            "format": resolved_format,
        }

    if not _pandas_available():
        if partition_by:
            raise ValueError("Partitioned materialization requires pandas (or Spark). Install pandas to proceed.")
        if resolved_format == "delta" and strategy == "merge" and primary_key:
            # Delta merge without pandas: use DeltaTable.merge() + pyarrow directly.
            import pyarrow as pa
            from deltalake import DeltaTable, write_deltalake

            target_str = str(target_file)
            if _is_polars_frame(df):
                arrow_data = (df.collect() if hasattr(df, "collect") else df).to_arrow()
            elif hasattr(df, "to_arrow"):
                arrow_data = df.to_arrow()
            else:
                raise ValueError("Delta merge without pandas requires a Polars or Arrow frame.")
            arrow_data = _sanitize_arrow_nulls(arrow_data)
            _dt_opts = _build_storage_options() if _is_remote_path(target_str) else None
            table_exists = Path(target_file / "_delta_log").exists() if not _is_remote_path(target_str) else False
            _local_engine = "rust" if _is_remote_path(target_str) else "pyarrow"
            if not table_exists:
                _safe_write_deltalake(
                    target_str,
                    arrow_data,
                    mode="overwrite",
                    schema_mode="overwrite",
                    storage_options=_dt_opts,
                    engine=_local_engine,
                )
            else:
                pk_predicate = " AND ".join(f"source.{pk} = target.{pk}" for pk in primary_key)
                dt = DeltaTable(target_str, storage_options=_dt_opts)
                dt.merge(
                    source=arrow_data,
                    predicate=pk_predicate,
                    source_alias="source",
                    target_alias="target",
                ).when_matched_update_all().when_not_matched_insert_all().execute()
            rows_written = len(arrow_data)
        elif strategy not in ["overwrite", "append"]:
            raise ValueError(
                f"Materialization strategy '{strategy}' for format '{resolved_format}' requires pandas "
                "(or Spark). Install pandas to proceed."
            )
        elif strategy == "append" and target_file.exists():
            rows_written = _append_without_pandas(df, target_file, resolved_format)
        else:
            _write_frame(df, target_file, resolved_format)
            rows_written = _row_count(df)
        logger.info(f"Materialized {rows_written if rows_written is not None else '?'} rows to {target_file}")
        return {
            "target": str(target_file),
            "rows_written": rows_written,
            "format": resolved_format,
        }

    # Prefer native Polars writes for csv/parquet to avoid pyarrow dependency.
    if (
        _is_polars_frame(df)
        and resolved_format in ["csv", "parquet"]
        and not partition_by
        and strategy in ["overwrite", "append"]
    ):
        if strategy == "append" and target_file.exists():
            rows_written = _append_without_pandas(df, target_file, resolved_format)
        else:
            _write_frame(df, target_file, resolved_format)
            rows_written = _row_count(df)
            if rows_written is None and hasattr(df, "collect"):
                try:
                    rows_written = int(df.collect().height)
                except Exception as exc:
                    logger.debug(f"Could not determine row count via collect: {exc}")
        logger.info(f"Materialized {rows_written if rows_written is not None else '?'} rows to {target_file}")
        return {
            "target": str(target_file),
            "rows_written": rows_written,
            "format": resolved_format,
        }

    pdf = _to_pandas(df)

    # Empty-frame guard — nothing to write, avoid partition-column validation errors
    if pdf.empty:
        is_delta_init = False
        if resolved_format == "delta":
            try:
                from deltalake import DeltaTable

                target_str = str(target_file)
                _dt_opts = _build_storage_options() if _is_remote_path(target_str) else None
                DeltaTable(target_str, storage_options=_dt_opts)
            except Exception:
                is_delta_init = True

        if is_delta_init:
            logger.info(
                f"materialize: empty DataFrame, but falling through to initialize empty Delta table schema at {target_file}"  # noqa: E501
            )
        else:
            logger.info("materialize: empty DataFrame — target exists or not Delta, skipping write.")
            return {
                "target": str(target_file),
                "rows_written": 0,
                "format": resolved_format,
            }

    if partition_by:
        missing = [col for col in partition_by if col not in pdf.columns]
        if missing:
            logger.warning(
                f"Partition columns not present in data (pruned): {', '.join(missing)}. "
                f"This is expected when _system.yaml defines a superset of partition columns "
                f"shared across multiple contracts."
            )
            partition_by = [col for col in partition_by if col not in missing]

    if strategy in ["merge", "scd2"]:
        missing_pk = [col for col in primary_key if col not in pdf.columns]
        if missing_pk:
            raise ValueError(f"Primary key columns missing from data: {', '.join(missing_pk)}")
    rows_written = 0

    # ── Delta: use write_deltalake natively (handles partitioning + schema evolution) ──
    # The Hive-style file loop below is for parquet/csv only.
    # Delta tables must be managed at the root path with partition_by passed
    # to write_deltalake, not via directory-per-partition file writes.
    if resolved_format == "delta":
        try:
            from deltalake import write_deltalake  # noqa: F401
            import pyarrow as pa
        except ImportError as exc:
            raise ImportError("Delta materialization requires the deltalake package: pip install deltalake") from exc

        # Resolve Arrow table from any engine frame
        if _is_polars_frame(pdf):
            # pdf may already be a Polars frame if we took the early path
            arrow_data = (pdf.collect() if hasattr(pdf, "collect") else pdf).to_arrow()
        elif hasattr(pdf, "to_arrow"):
            arrow_data = pdf.to_arrow()
        elif hasattr(pdf, "arrow"):
            arrow_data = pdf.arrow()  # DuckDB relations
        else:
            arrow_data = pa.Table.from_pandas(pdf if hasattr(pdf, "columns") else _to_pandas(pdf))

        arrow_data = _sanitize_arrow_nulls(arrow_data)  # Delta rejects Arrow Null type

        # ── Cast DataFrame schema to match target Delta table precisely ──
        # Polars defaults ints to Int64 and timestamps to strings, but the Delta
        # table (initialized by DDL_ONLY) expects exact types. This pre-write cast
        # aligns the arrow_data schema with the physical Delta schema, covering
        # both contract fields AND injected system lineage columns.
        resolved_target.mkdir(parents=True, exist_ok=True)
        target_str = str(resolved_target)

        # Check if target Delta table already exists
        try:
            from deltalake import DeltaTable as _DT

            _dt_opts = _build_storage_options() if _is_remote_path(target_str) else None
            _existing_dt = _DT(target_str, storage_options=_dt_opts)
            table_exists = True
        except Exception:
            _existing_dt = None
            table_exists = False

        # Phase 1: Try to cast against existing Delta table schema (most accurate)
        if _existing_dt is not None:
            try:
                import pyarrow as pa
                import pyarrow.compute as pc

                delta_schema = _get_pyarrow_schema(_existing_dt)
                delta_type_map = {f.name: f.type for f in delta_schema}
                cast_count = 0

                for i, f in enumerate(arrow_data.schema):
                    if f.name not in delta_type_map:
                        continue
                    target_type = delta_type_map[f.name]
                    if f.type == target_type:
                        continue

                    try:
                        col = arrow_data.column(i)
                        # Handle string → timestamp specially
                        if (pa.types.is_string(f.type) or pa.types.is_large_string(f.type)) and pa.types.is_timestamp(
                            target_type
                        ):
                            # Strategy: use pandas to parse ISO timestamps (handles all tz formats)
                            import pandas as pd

                            pd_series = col.to_pandas()
                            pd_ts = pd.to_datetime(pd_series, utc=True)
                            if target_type.tz is None:
                                # Strip timezone for timestamp[us] (no tz)
                                pd_ts = pd_ts.dt.tz_localize(None)
                            col = pa.array(pd_ts, type=target_type)
                            arrow_data = arrow_data.set_column(i, pa.field(f.name, target_type), col)
                            cast_count += 1
                            continue

                        # General cast for numeric / other types
                        casted_col = col.cast(target_type)
                        arrow_data = arrow_data.set_column(i, pa.field(f.name, target_type), casted_col)
                        cast_count += 1
                    except Exception as col_err:
                        logger.warning(f"Could not cast column '{f.name}' from {f.type} to {target_type}: {col_err}")

                if cast_count:
                    logger.info(f"Cast {cast_count} column(s) to match existing Delta table schema")
            except Exception as e:
                logger.warning(f"Could not cast arrow schema to Delta table schema: {e}")
        else:
            # Phase 2: No existing table — cast using contract field types only
            _srv = contract.effective_server() if hasattr(contract, "effective_server") else None
            _cast_to_string = getattr(_srv, "cast_to_string", False) if _srv else False

            if not _cast_to_string:
                try:
                    import pyarrow as pa
                    from lakelogic.core.ddl import _resolve_arrow_type, _get_fields

                    contract_fields = _get_fields(contract)
                    if contract_fields:
                        field_types = {f.name: _resolve_arrow_type(f.type) for f in contract_fields}
                        new_fields = []
                        for f in arrow_data.schema:
                            if f.name in field_types:
                                target_type = field_types[f.name]
                                if isinstance(target_type, pa.DataType) and f.type != target_type:
                                    f = pa.field(f.name, target_type)
                            new_fields.append(f)

                        target_schema = pa.schema(new_fields)
                        if target_schema != arrow_data.schema:
                            diffs_count = sum(1 for a, b in zip(arrow_data.schema, target_schema) if a.type != b.type)
                            msg_lines = [
                                f"Casting {diffs_count} column(s) to match contract types before Delta write (new table):",  # noqa: E501
                                f"{'Column Name':<35} | {'From Type':<15} | {'To Type':<15}",
                                "-" * 71,
                            ]
                            for a, b in zip(arrow_data.schema, target_schema):
                                if a.type != b.type:
                                    msg_lines.append(f"{a.name:<35} | {str(a.type):<15} | {str(b.type):<15}")

                            logger.info("\n" + "\n".join(msg_lines))
                            arrow_data = arrow_data.cast(target_schema)
                except Exception as e:
                    logger.warning(f"Could not cast arrow schema to contract types: {e}")

        # ── Merge / SCD2 strategy for non-partitioned Delta tables ────────
        if strategy in ("merge", "scd2") and primary_key:
            if table_exists:
                from deltalake import DeltaTable

                if strategy == "scd2":
                    # SCD2: apply full frame logic then overwrite
                    scd2_cfg_local = getattr(mat, "scd2", None)
                    scd2_cfg_local = dict(scd2_cfg_local) if isinstance(scd2_cfg_local, dict) else {}
                    # Pass the dedup guard flag into the local cfg
                    scd2_cfg_local["merge_dedup_guard"] = bool(getattr(mat, "merge_dedup_guard", False))
                    dt = DeltaTable(target_str, storage_options=_dt_opts)
                    existing = dt.to_pandas()
                    logger.info(f"SCD2 merge: existing={len(existing)} rows, incoming={len(_to_pandas(pdf))} rows")
                    merged = _scd2_frames(existing, _to_pandas(pdf), primary_key, scd2_cfg_local)
                    # Inject unknown member once (non-partitioned delta SCD2)
                    _um_cfg = scd2_cfg_local.get("unknown_member")
                    if _um_cfg is not None and _um_cfg.get("enabled", True):
                        merged = _inject_unknown_member_pandas(merged, primary_key, scd2_cfg_local, _um_cfg)
                    logger.info(f"SCD2 merge result: {len(merged)} rows (existing {len(existing)} + changes)")
                    arrow_data = pa.Table.from_pandas(merged, preserve_index=False)
                    arrow_data = _sanitize_arrow_nulls(arrow_data)
                    _safe_write_deltalake(
                        target_str,
                        arrow_data,
                        mode="overwrite",
                        schema_mode=_delta_schema_mode,
                        storage_options=_dt_opts,
                    )
                    rows_written = len(merged)
                else:
                    # Merge: use DeltaTable.merge() for upsert
                    # ── Dedup guard: incoming batch must not contain duplicate PKs ──
                    # Disabled by default. Enable via merge_dedup_guard: true.
                    _dedup_enabled = bool(getattr(mat, "merge_dedup_guard", False))
                    if _dedup_enabled:
                        _incoming_pdf = arrow_data.to_pandas()
                        _pre_dedup = len(_incoming_pdf)
                        _incoming_pdf = _incoming_pdf.drop_duplicates(subset=primary_key, keep="last")
                        _post_dedup = len(_incoming_pdf)
                        if _pre_dedup != _post_dedup:
                            logger.warning(
                                f"Merge dedup guard: {_pre_dedup} → {_post_dedup} rows "
                                f"(dropped {_pre_dedup - _post_dedup} duplicate PKs). "
                                f"Consider adding explicit dedup in your contract SQL."
                            )
                            arrow_data = pa.Table.from_pandas(_incoming_pdf, preserve_index=False)
                            arrow_data = _sanitize_arrow_nulls(arrow_data)
                    pk_predicate = " AND ".join(f"source.{pk} = target.{pk}" for pk in primary_key)
                    dt = DeltaTable(target_str, storage_options=_dt_opts)
                    (
                        dt.merge(
                            source=arrow_data,
                            predicate=pk_predicate,
                            source_alias="source",
                            target_alias="target",
                        )
                        .when_matched_update_all()
                        .when_not_matched_insert_all()
                        .execute()
                    )
                    rows_written = len(arrow_data)
            else:
                # First write — seed the table
                if strategy == "scd2":
                    scd2_cfg_local = getattr(mat, "scd2", None)
                    scd2_cfg_local = dict(scd2_cfg_local) if isinstance(scd2_cfg_local, dict) else {}
                    import pandas as _pd

                    empty_existing = _pd.DataFrame(columns=_to_pandas(pdf).columns)
                    merged = _scd2_frames(empty_existing, _to_pandas(pdf), primary_key, scd2_cfg_local)
                    # Inject unknown member once (initial load delta SCD2)
                    _um_cfg = scd2_cfg_local.get("unknown_member")
                    if _um_cfg is not None and _um_cfg.get("enabled", True):
                        merged = _inject_unknown_member_pandas(merged, primary_key, scd2_cfg_local, _um_cfg)
                    arrow_data = pa.Table.from_pandas(merged, preserve_index=False)
                    arrow_data = _sanitize_arrow_nulls(arrow_data)
                    rows_written = len(merged)
                else:
                    rows_written = len(arrow_data)
                _safe_write_deltalake(
                    target_str,
                    arrow_data,
                    mode="overwrite",
                    schema_mode=_delta_schema_mode,
                    storage_options=_dt_opts,
                )

            logger.info(
                f"Materialized {rows_written} rows to Delta table: {resolved_target} "
                f"(strategy={strategy}, pk={primary_key})"
            )
            _maybe_compact_delta(target_str, contract)
            _result = {
                "target": target_str,
                "rows_written": rows_written,
                "format": "delta",
            }
            return _run_secondary_targets(mat, contract, arrow_data, strategy, primary_key, rows_written, _result)

        # ── Append / Overwrite strategy ───────────────────────────────────
        delta_mode = "overwrite" if strategy == "overwrite" else "append"
        delta_partition_by = partition_by if partition_by else None

        # ── Diagnostic: log pre-write row count ──
        _pre_write_count = None
        if table_exists and delta_mode == "append":
            try:
                from deltalake import DeltaTable as _DT_pre

                _dt_pre_opts = _build_storage_options() if _is_remote_path(target_str) else None
                import pyarrow.compute as pc

                _dt_pre = _DT_pre(target_str, storage_options=_dt_pre_opts)
                _actions = _dt_pre.get_add_actions(flatten=True)
                if "num_records" in _actions.column_names:
                    _pre_write_count = pc.sum(_actions["num_records"]).as_py()
                else:
                    _pre_write_count = _dt_pre.to_pyarrow_dataset().scanner().count_rows()
                logger.info(
                    f"Delta pre-write state: {_pre_write_count} rows in {target_str} "
                    f"(appending {len(arrow_data)} new rows)"
                )
            except Exception as _pre_err:
                logger.debug(f"Could not read pre-write row count: {_pre_err}")

        # Build storage_options for cloud targets
        _write_opts = _build_storage_options() if _is_remote_path(target_str) else None
        _write_kwargs = {
            "mode": delta_mode,
            "partition_by": delta_partition_by,
            "schema_mode": _delta_schema_mode,  # schema evolution: new columns auto-added
        }
        if _write_opts:
            _write_kwargs["storage_options"] = _write_opts

        try:
            _safe_write_deltalake(
                target_str,
                arrow_data,
                **_write_kwargs,
            )
        except Exception as e:
            err_msg = str(e)
            if "DeletionVectors" in err_msg or "reader features required" in err_msg:
                raise RuntimeError(
                    f"\n{'=' * 80}\n"
                    f"❌ ENGINE INCOMPATIBILITY ERROR: DELTA PROTOCOL v3\n"
                    f"{'=' * 80}\n"
                    f"LakeLogic (Polars/Delta-RS) failed to write to the Delta table at:\n"
                    f"  {resolved_target}\n\n"
                    f"REASON:\n"
                    f"This table was likely created by Databricks/Spark, which enables 'DeletionVectors' \n"
                    f"by default. The open-source Python 'deltalake' package does not yet support \n"
                    f"writing to tables with this Databricks-specific feature enabled.\n\n"
                    f"HOW TO FIX:\n"
                    f"  Option 1 (Colab/Local): Add 'bronze' to the RESET_LAYERS list in your pipeline \n"
                    f"           runner block to wipe and recreate a clean OSS-compatible table.\n"
                    f"           Example: RESET_LAYERS = ['bronze']\n"
                    f"  Option 2 (Databricks) : Run this SQL in Databricks to downgrade the table:\n"
                    f"           ALTER TABLE <your-table> SET TBLPROPERTIES ('delta.enableDeletionVectors' = false);\n"
                    f"           REORG TABLE <your-table> APPLY (PURGE);\n"
                    f"{'=' * 80}"
                ) from e
            raise

        rows_written = len(arrow_data)

        # ── Diagnostic: log post-write row count ──
        if delta_mode == "append":
            try:
                from deltalake import DeltaTable as _DT_post

                _dt_post_opts = _build_storage_options() if _is_remote_path(target_str) else None
                _dt_post = _DT_post(target_str, storage_options=_dt_post_opts)
                _post_write_count = len(_dt_post.to_pyarrow_table())

                if _pre_write_count is not None and _post_write_count < _pre_write_count:
                    logger.error(
                        f"⚠️ DATA LOSS DETECTED: table went from {_pre_write_count} to "
                        f"{_post_write_count} rows after mode=append write!"
                    )
            except Exception as _post_err:
                logger.debug(f"Could not read post-write row count: {_post_err}")

        logger.info(
            f"Materialized {rows_written} rows to Delta table: {resolved_target} "
            f"(strategy={strategy}, mode={delta_mode})"
        )
        _maybe_compact_delta(target_str, contract)
        _result = {
            "target": target_str,
            "rows_written": rows_written,
            "format": "delta",
        }
        return _run_secondary_targets(mat, contract, arrow_data, strategy, primary_key, rows_written, _result)

    if partition_by:
        base_dir = resolved_target
        base_dir.mkdir(parents=True, exist_ok=True)
        timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

        for part_values, group in _partition_groups(pdf, partition_by):
            part_dir = base_dir
            for col, val in part_values.items():
                part_dir = part_dir / f"{col}={_safe_partition_value(val)}"

            safe_overwrite = reprocess_policy == "overwrite_partition_safe"
            overwrite_partition = (
                is_reprocess and (reprocess_policy == "overwrite_partition" or safe_overwrite)
            ) or strategy == "overwrite"

            if overwrite_partition and not safe_overwrite:
                if part_dir.exists():
                    shutil.rmtree(part_dir)
            part_dir.mkdir(parents=True, exist_ok=True)

            if strategy == "append" and not overwrite_partition:
                file_name = f"data_{timestamp_tag}.{resolved_format}"
            else:
                file_name = f"data.{resolved_format}"

            if safe_overwrite:
                tmp_dir = part_dir.parent / f".{part_dir.name}.tmp_{uuid4().hex}"
                if tmp_dir.exists():
                    shutil.rmtree(tmp_dir)
                tmp_dir.mkdir(parents=True, exist_ok=True)
                out_path = tmp_dir / file_name
                try:
                    _write_frame(group, out_path, resolved_format)
                except Exception:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                    raise
                if part_dir.exists():
                    shutil.rmtree(part_dir)
                shutil.move(str(tmp_dir), str(part_dir))
            else:
                out_path = part_dir / file_name
                _write_frame(group, out_path, resolved_format)
            rows_written += len(group)

        logger.info(f"Materialized {rows_written} rows to partitioned path: {base_dir}")
        return {
            "target": str(base_dir),
            "rows_written": rows_written,
            "format": resolved_format,
        }

    if strategy == "overwrite":
        _write_frame(pdf, target_file, resolved_format)
        rows_written = len(pdf)
    elif strategy == "append":
        if target_file.exists():
            import pandas as pd

            existing = _read_frame(target_file, resolved_format)
            combined = pd.concat([existing, pdf], ignore_index=True)
            _write_frame(combined, target_file, resolved_format)
            rows_written = len(combined)
        else:
            _write_frame(pdf, target_file, resolved_format)
            rows_written = len(pdf)
    elif strategy == "merge":
        # Extract CDC and Soft Delete settings (needed by both branches below)
        cdc_op_field = getattr(contract.source, "cdc_op_field", None) if contract.source else None
        cdc_delete_values = getattr(contract.source, "cdc_delete_values", None) if contract.source else None
        soft_delete_col = getattr(mat, "soft_delete_column", None)
        soft_delete_val = getattr(mat, "soft_delete_value", True)
        soft_delete_time_col = getattr(mat, "soft_delete_time_column", None)
        soft_delete_reason_col = getattr(mat, "soft_delete_reason_column", None)

        if target_file.exists():
            existing = _read_frame(target_file, resolved_format)

            scd1_cfg = getattr(mat, "scd1", None)
            scd1_cfg = dict(scd1_cfg) if isinstance(scd1_cfg, dict) else {}
            unknown_member = getattr(mat, "unknown_member", None)
            if unknown_member and "unknown_member" not in scd1_cfg:
                scd1_cfg["unknown_member"] = dict(unknown_member) if isinstance(unknown_member, dict) else {}

            merged = _merge_frames(
                existing,
                pdf,
                primary_key,
                soft_delete_col=soft_delete_col,
                soft_delete_val=soft_delete_val,
                soft_delete_time_col=soft_delete_time_col,
                soft_delete_reason_col=soft_delete_reason_col,
                cdc_op_field=cdc_op_field,
                cdc_delete_values=cdc_delete_values,
                scd1_cfg=scd1_cfg,
            )
            # Inject unknown member once
            unknown_cfg = scd1_cfg.get("unknown_member", {})
            if unknown_cfg.get("enabled", True):
                merged = _inject_unknown_member_pandas(merged, primary_key, scd1_cfg, unknown_cfg)
        else:
            scd1_cfg = getattr(mat, "scd1", None)
            scd1_cfg = dict(scd1_cfg) if isinstance(scd1_cfg, dict) else {}
            unknown_member = getattr(mat, "unknown_member", None)
            if unknown_member and "unknown_member" not in scd1_cfg:
                scd1_cfg["unknown_member"] = dict(unknown_member) if isinstance(unknown_member, dict) else {}

            # If target doesn't exist, process it against an empty dataframe to inject SK/Unknown Member
            merged = _merge_frames(
                _to_pandas(pdf)[:0],
                pdf,
                primary_key,
                soft_delete_col=soft_delete_col,
                soft_delete_val=soft_delete_val,
                soft_delete_time_col=soft_delete_time_col,
                soft_delete_reason_col=soft_delete_reason_col,
                cdc_op_field=cdc_op_field,
                cdc_delete_values=cdc_delete_values,
                scd1_cfg=scd1_cfg,
            )
            # Inject unknown member once
            unknown_cfg = scd1_cfg.get("unknown_member", {})
            if unknown_cfg.get("enabled", True):
                merged = _inject_unknown_member_pandas(merged, primary_key, scd1_cfg, unknown_cfg)
        _write_frame(merged, target_file, resolved_format)
        rows_written = len(merged)
    elif strategy == "scd2":
        if target_file.exists():
            existing = _read_frame(target_file, resolved_format)
            merged = _scd2_frames(existing, pdf, primary_key, scd2_cfg)
        else:
            merged = _scd2_frames(_to_pandas(pdf)[:0], pdf, primary_key, scd2_cfg)
        # Inject unknown member once (non-partitioned file-based SCD2)
        unknown_cfg = scd2_cfg.get("unknown_member") if scd2_cfg else None
        if unknown_cfg is not None and unknown_cfg.get("enabled", True):
            merged = _inject_unknown_member_pandas(merged, primary_key, scd2_cfg, unknown_cfg)
        _write_frame(merged, target_file, resolved_format)
        rows_written = len(merged)
    else:
        raise ValueError(f"Unsupported materialization strategy: {strategy}")

    logger.info(f"Materialized {rows_written} rows to {target_file}")

    result = {
        "target": str(target_file),
        "rows_written": rows_written,
        "format": resolved_format,
    }
    # Use the final data (merged/pdf) for secondary targets
    _sec_df = merged if "merged" in locals() else pdf if "pdf" in locals() else df
    return _run_secondary_targets(mat, contract, _sec_df, strategy, primary_key, rows_written, result)


# ── Delta Compaction ─────────────────────────────────────────────────────────


def optimize_delta(
    target: str,
    *,
    vacuum: bool = True,
    vacuum_retention_hours: int = 168,
    storage_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Compact small files in a Delta table and optionally vacuum old versions.

    Uses the ``deltalake`` Python package (delta-rs) — no Spark required.
    Safe to call from Azure Functions, containers, or any Python process.

    Args:
        target: Delta table path (local or ``abfss://...``).
        vacuum: Whether to run vacuum after compaction.
        vacuum_retention_hours: Minimum age of files to vacuum (default 7 days).
        storage_options: Azure/S3/GCS credentials for cloud storage.

    Returns:
        Metadata dict with compaction results.

    Usage::

        from lakelogic.core.materialization import optimize_delta

        # Manual call
        optimize_delta("abfss://bronze@storage.dfs.core.windows.net/orders")

        # Or from contract config (auto mode)
        # materialization:
        #   compaction:
        #     auto: true
        #     vacuum_retention_hours: 168
    """
    try:
        from deltalake import DeltaTable
    except ImportError:
        raise ImportError("Delta compaction requires the deltalake package: pip install deltalake")

    result: Dict[str, Any] = {"target": target}

    try:
        dt = DeltaTable(target, storage_options=storage_options)

        # ── Compact (bin-pack small files) ──────────────────────────────────
        compact_result = dt.optimize.compact()
        metrics = compact_result if isinstance(compact_result, dict) else {}
        result["compaction"] = {
            "status": "ok",
            "metrics": metrics,
        }
        logger.info(f"Compacted Delta table: {target}")

        # ── Vacuum (remove old file versions) ───────────────────────────────
        if vacuum:
            vacuum_result = dt.vacuum(
                retention_hours=vacuum_retention_hours,
                enforce_retention_duration=True,
                dry_run=False,
            )
            result["vacuum"] = {
                "status": "ok",
                "retention_hours": vacuum_retention_hours,
                "files_removed": len(vacuum_result) if isinstance(vacuum_result, list) else 0,
            }
            logger.info(f"Vacuumed Delta table: {target} (retention={vacuum_retention_hours}h)")

    except Exception as exc:
        logger.warning(f"Delta compaction failed for {target}: {exc}")
        result["error"] = str(exc)

    return result


def _maybe_compact_delta(target: str, contract) -> Optional[Dict[str, Any]]:
    """
    Run compaction if the contract has ``compaction.auto: true``.

    Called automatically after Delta materialization writes.
    """
    mat = getattr(contract, "materialization", None)
    compaction_cfg = getattr(mat, "compaction", None) if mat else None
    if not compaction_cfg or not compaction_cfg.get("auto"):
        return None

    vacuum = compaction_cfg.get("vacuum", True)
    retention = compaction_cfg.get("vacuum_retention_hours", 168)

    return optimize_delta(
        target,
        vacuum=vacuum,
        vacuum_retention_hours=retention,
    )


# ── Re-exports for backwards compatibility ──────────────────────────────────
# Quarantine and run-log persistence have been extracted to focused modules.
# The functions below are re-exported so existing imports continue to work.

from lakelogic.core.quarantine import materialize_quarantine  # noqa: F401, E402
from lakelogic.core.run_log import write_run_log, get_last_run_watermark  # noqa: F401, E402
