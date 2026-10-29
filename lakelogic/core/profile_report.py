"""Profile a contract's data where it lives, and report the profile to LakeLogic.

``lakelogic profile --contract <file>``: the contract's ``profiling`` block says how (full or a
sample, findings or not); its ``source`` says where, unless a location is passed. The profile is
statistics only, never rows (``lakelogic.core.profile``), with ``pii`` / ``classification``
columns' min and max withheld.

With an endpoint and API key it is sent to the platform's run-log ingest exactly as
``lakelogic check`` sends a rule check (``rule_check.send``: the same header, the same retry
buffer), tagged ``record_type: profile``. The platform files it as an observation, not a
pipeline run.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from lakelogic.core.rule_check import dataset_name, load_contract_doc, send

RECORD_TYPE = "profile"
ENGINE = "profile"


def profiling_settings(doc: Dict[str, Any]) -> Dict[str, Any]:
    """The contract's ``profiling`` block as the profiler's settings (full scan when absent)."""
    block = doc.get("profiling") or {}
    if block.get("enabled") is False:
        raise ValueError("This contract turns profiling off (profiling.enabled: false).")
    if block.get("mode") == "sample":
        return {
            "mode": "sample",
            "sample_unit": "percent" if block.get("sample_percent") else "rows",
            "sample_rows": block.get("sample_rows"),
            "sample_percent": block.get("sample_percent"),
            "sample_order": block.get("sample_order") or "newest",
        }
    return {"mode": "full"}


def contract_location(doc: Dict[str, Any]) -> Optional[str]:
    """Where the contract's data is: ``source.path``, when it names a concrete location."""
    path = str(((doc.get("source") or {}).get("path")) or "").strip()
    return path if path and "{" not in path else None


def build_payload(
    profile: Dict[str, Any],
    *,
    dataset: str,
    started_at: str,
    finished_at: str,
    environment: Optional[str] = None,
    contract_version: Optional[str] = None,
) -> Dict[str, Any]:
    """The ``RunLogIngest`` body for one profile, shaped like a rule check's."""
    counts = profile.get("finding_counts") or {}
    return {
        "contract_name": dataset,
        "dataset": dataset,
        "status": "success",
        "engine": ENGINE,
        "started_at": started_at,
        "finished_at": finished_at,
        # ``assess_files`` wraps the statistics: {name, status, profile, findings, metrics}.
        "rows_input": (profile.get("metrics") or {}).get("row_count"),
        "metadata": {
            "record_type": RECORD_TYPE,
            "engine": ENGINE,
            "source": "client",
            "dataset": dataset,
            "environment": environment,
            "contract_version": contract_version,
            "finding_counts": counts,
            "profile": profile,
        },
    }


#: Columns of the local profiles table (``lakelogic_profiles``): one row per column per profile,
#: the table-level figures repeated on each row so a column's history is one filter away.
PROFILE_COLUMNS = (
    ("profile_id", "string"),
    ("profiled_at", "string"),
    ("dataset", "string"),
    ("table_name", "string"),
    ("environment", "string"),
    ("source", "string"),
    ("row_count", "bigint"),
    ("sampling", "string"),
    ("confirmed_findings", "bigint"),
    ("potential_findings", "bigint"),
    ("control_gaps", "bigint"),
    ("column_name", "string"),
    ("data_type", "string"),
    ("null_count", "bigint"),
    ("distinct_count", "bigint"),
    ("min_value", "string"),
    ("max_value", "string"),
    ("sensitive", "string"),
    ("contract_version", "string"),
    ("lakelogic_version", "string"),
)


def profiles_table_name(schema: str) -> str:
    """``<schema>.lakelogic_profiles``, from the ONE naming module (``metadata_names``)."""
    from lakelogic.core.metadata_names import metadata_table_name

    return f"{schema}.{metadata_table_name('profiles')}"


def profile_rows(payload: Dict[str, Any], *, table: str, profile_id: str) -> List[Dict[str, Any]]:
    """The ``lakelogic_profiles`` rows for one profile: the SAME statistics sent to LakeLogic.
    A sensitive column's min and max are already withheld by the profiler and stay empty."""
    try:
        from lakelogic import __version__ as ll_version
    except Exception:  # noqa: BLE001
        ll_version = None
    meta = payload.get("metadata") or {}
    doc = meta.get("profile") or {}
    counts = doc.get("finding_counts") or {}
    sampling = ((doc.get("metrics") or {}).get("sampling") or {}).get("method") or "full"

    def text(v: Any) -> Optional[str]:
        return None if v is None else str(v)

    return [
        {
            "profile_id": profile_id,
            "profiled_at": payload.get("finished_at"),
            "dataset": payload.get("dataset"),
            "table_name": table,
            "environment": meta.get("environment"),
            "source": meta.get("source"),
            "row_count": payload.get("rows_input"),
            "sampling": sampling,
            "confirmed_findings": counts.get("confirmed"),
            "potential_findings": counts.get("potential"),
            "control_gaps": counts.get("gap"),
            "column_name": c.get("name"),
            "data_type": c.get("type") or c.get("data_type"),
            "null_count": c.get("null_count"),
            "distinct_count": c.get("distinct_count"),
            "min_value": text(c.get("min")),
            "max_value": text(c.get("max")),
            "sensitive": text(c.get("sensitive")),
            "contract_version": meta.get("contract_version"),
            "lakelogic_version": ll_version,
        }
        for c in (doc.get("profile") or {}).get("columns") or []
    ]


def write_profile_rows(rows: List[Dict[str, Any]], results_table: str, *, spark: Any) -> Optional[str]:
    """Append ``rows`` to ``results_table`` (created on first write). Returns an error sentence,
    or None when written. Never raises: the table never fails the profile it records."""
    if not rows:
        return None
    try:
        from pyspark.sql.types import LongType, StringType, StructField, StructType

        schema = StructType(
            [StructField(n, LongType() if t == "bigint" else StringType(), True) for n, t in PROFILE_COLUMNS]
        )
        cols = [n for n, _ in PROFILE_COLUMNS]
        (
            spark.createDataFrame([tuple(r.get(c) for c in cols) for r in rows], schema)
            .write.mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(results_table)
        )
        return None
    except Exception as exc:  # noqa: BLE001
        return f"The profile was not written to {results_table}: {exc}"


def active_spark() -> Any:
    """The Spark session already running here (a Databricks or Fabric notebook or job), or None.
    Never starts one: locally, files are profiled with DuckDB."""
    try:
        from pyspark.sql import SparkSession
    except Exception:  # noqa: BLE001
        return None
    try:
        return SparkSession.getActiveSession()
    except Exception:  # noqa: BLE001
        return None


def snowflake_connection() -> Any:
    """A Snowflake connection from the SNOWFLAKE_* variables the Snowflake engine reads."""
    import os

    import snowflake.connector

    keys = ("account", "user", "password", "warehouse", "database", "schema", "role")
    params = {k: os.getenv(f"SNOWFLAKE_{k.upper()}") for k in keys}
    return snowflake.connector.connect(**{k: v for k, v in params.items() if v})


def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "'true'" if v else "'false'"
    if isinstance(v, int):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def write_profile_rows_sql(rows: List[Dict[str, Any]], results_table: str, *, connection: Any) -> Optional[str]:
    """``write_profile_rows`` for a SQL warehouse (Snowflake): CREATE TABLE IF NOT EXISTS, then one
    INSERT of literal rows. Same columns, same never-raises contract."""
    if not rows:
        return None
    try:
        cols = [n for n, _ in PROFILE_COLUMNS]
        ddl = ", ".join(f"{n} {'BIGINT' if t == 'bigint' else 'STRING'}" for n, t in PROFILE_COLUMNS)
        values = ", ".join("(" + ", ".join(_sql_literal(r.get(c)) for c in cols) + ")" for r in rows)
        cur = getattr(connection, "conn", connection).cursor()  # a DbApiExecutor, or its connection
        try:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {results_table} ({ddl})")
            cur.execute(f"INSERT INTO {results_table} ({', '.join(cols)}) VALUES {values}")
        finally:
            cur.close()
        return None
    except Exception as exc:  # noqa: BLE001
        return f"The profile was not written to {results_table}: {exc}"


def spark_files_view(spark: Any, location: str, fmt: Optional[str]) -> str:
    """A temporary view over the files at ``location``, read by Spark; returns its name.

    CSV and JSON are read as written (header, quoted line breaks); folders nested by partition
    are read whole. The view lives only in this Spark session."""
    import re

    kind = (fmt or "").lower() or ("parquet" if location.rstrip("/").endswith(".parquet") else "csv")
    reader = spark.read.option("recursiveFileLookup", "true")
    if kind in ("csv", "tsv"):
        reader = (reader.option("header", "true").option("multiLine", "true").option("escape", '"')
                  .option("sep", "\t" if kind == "tsv" else ","))
        kind = "csv"
    elif kind in ("json", "jsonl", "ndjson"):
        kind = "json"
    view = "_lakelogic_profile_" + re.sub(r"\W", "_", location.rstrip("/").rsplit("/", 1)[-1] or "files")[:40]
    reader.format(kind).load(location).createOrReplaceTempView(view)
    return view


def profile_contract(
    contract: Any,
    *,
    location: Optional[str] = None,
    fmt: Optional[str] = None,
    table: Optional[str] = None,
    spark: Any = None,
    connection: Any = None,
    results_table: Optional[str] = None,
    endpoint: Optional[str] = None,
    api_key: Optional[str] = None,
    environment: Optional[str] = None,
    dataset: Optional[str] = None,
) -> Dict[str, Any]:
    """Profile the contract's data and send the result when ``endpoint`` is set.

    With ``spark`` it profiles a TABLE (``table``, else the contract's ``info.table_name``) where
    it lives; with ``connection`` (a Snowflake connection, or any ``profile.executor_for`` accepts)
    it profiles that TABLE in the warehouse. With ``results_table`` it also appends one row per column to that table
    (``profiles_table_name``). Without, files at ``location``, else ``source.path``. Returns
    ``{"profile", "payload", "sent", "results_table", "results_table_error"}``. Sending never
    raises: an unreachable platform is buffered for retry, as a pipeline run is."""
    import uuid

    from lakelogic.core.profile import SparkExecutor
    from lakelogic.core.profile_assess import assess_files, assess_table

    doc = load_contract_doc(contract)
    started = datetime.now(timezone.utc).isoformat()
    if connection is not None:
        # A TABLE in a SQL warehouse (Snowflake): one aggregate query, pushed down.
        target = table or str((doc.get("info") or {}).get("table_name") or "")
        if not target or "{" in target:
            raise ValueError("Pass --table: the contract's info.table_name is not a concrete table name.")
        profile = assess_table(target, connection, settings=profiling_settings(doc), contract=doc)
    elif spark is not None and (table or not (location or contract_location(doc))):
        # A TABLE where it lives (Unity Catalog, Hive, a lakehouse), read through Spark.
        target = table or str((doc.get("info") or {}).get("table_name") or "")
        if not target or "{" in target:
            raise ValueError("Pass --table: the contract's info.table_name is not a concrete table name.")
        profile = assess_table(target, SparkExecutor(spark), settings=profiling_settings(doc), contract=doc)
    elif spark is not None:
        # (Spark is wrapped in its executor, never sniffed by type: a Databricks job hands the
        # script a proxy session that the type check did not recognise.)
        # FILES in a Volume or cloud storage (/Volumes/..., abfss://, s3://, gs://): Spark reads
        # the folder (partition folders included) into a temporary view, profiled as a table.
        target = str(location or contract_location(doc))
        view = spark_files_view(spark, target, fmt or (doc.get("source") or {}).get("format"))
        profile = assess_table(view, SparkExecutor(spark), settings=profiling_settings(doc), contract=doc)
        profile["name"] = target
    else:
        where = location or contract_location(doc)
        if not where:
            raise ValueError("Pass the data's location: the contract's source.path is not a concrete path.")
        target = where
        profile = assess_files(
            where,
            fmt=fmt or ((doc.get("source") or {}).get("format")),
            settings=profiling_settings(doc),
            contract=doc,
        )
    if (doc.get("profiling") or {}).get("findings") is False:
        profile.pop("findings", None)
        profile.pop("finding_counts", None)
    name = dataset or dataset_name(doc)
    payload = build_payload(
        profile,
        dataset=name,
        started_at=started,
        finished_at=datetime.now(timezone.utc).isoformat(),
        environment=environment,
        contract_version=str(doc.get("version") or "") or None,
    )
    sent = bool(endpoint) and send(payload, endpoint, api_key)
    error = None
    if results_table and connection is not None:
        error = write_profile_rows_sql(profile_rows(payload, table=target, profile_id=str(uuid.uuid4())),
                                       results_table, connection=connection)
    elif results_table and spark is not None:
        error = write_profile_rows(profile_rows(payload, table=target, profile_id=str(uuid.uuid4())),
                                   results_table, spark=spark)
    return {"profile": profile, "payload": payload, "sent": sent,
            "results_table": results_table if results_table and not error else None, "results_table_error": error}
