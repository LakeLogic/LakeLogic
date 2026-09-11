"""Keep a Delta table's liquid clustering in step with its contract.

WHY THIS EXISTS

``materialization.cluster_by`` reached a Databricks table in exactly one way: the ``CREATE TABLE``
that DDL mode emits (``ddl.py`` -> ``CLUSTER BY (...)``). Two ordinary routes never got it:

* A table created by its FIRST PIPELINE WRITE. The Spark writer applies ``partitionBy`` and
  nothing else — there is no ``clusterBy`` anywhere on the write path — so the table was
  created unclustered.
* A table that ALREADY EXISTED. Creation is ``CREATE TABLE IF NOT EXISTS``, so it is skipped,
  and schema evolution (``generate_alter_ddl``) only adds columns. Declaring ``cluster_by`` on
  a live table therefore changed nothing, silently.

So this runs AFTER every Spark write to a catalog table (``_spark_apply_table_metadata``) and
reconciles: read the table's actual clustering with ``DESCRIBE DETAIL``, and only if it differs
from the contract, ``ALTER TABLE ... CLUSTER BY (...)``. Delta supports adding and changing
liquid clustering on an existing table (verified against delta-spark 4.0 in
``tests/test_delta_clustering_reconcile.py``); it does not support it on a PARTITIONED table
(``DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED``), which is reported instead.

It is an optimisation, so it NEVER fails a write: every outcome is returned and logged, and an
older runtime without liquid clustering is a warning.
"""

from __future__ import annotations

from typing import Any, List, Optional

from loguru import logger

#: What ``reconcile_delta_clustering`` did, for callers and tests.
APPLIED = "applied"
UNCHANGED = "unchanged"
NOT_DECLARED = "not_declared"
PARTITIONED = "partitioned"
NOT_DELTA = "not_delta"
NO_COLUMNS = "no_columns"
UNSUPPORTED = "unsupported"


def _quote(table_ref: str) -> str:
    """Backtick each part of a catalog name, leaving an already-quoted or path ref alone."""
    if "`" in table_ref:
        return table_ref
    return ".".join(f"`{part}`" for part in table_ref.split("."))


def _detail(spark: Any, ref: str) -> Optional[dict]:
    rows = spark.sql(f"DESCRIBE DETAIL {ref}").collect()
    return rows[0].asDict() if rows else None


def reconcile_delta_clustering(
    spark: Any,
    table_ref: str,
    cluster_by: Optional[List[str]],
    *,
    table_label: Optional[str] = None,
) -> str:
    """Make ``table_ref``'s liquid clustering match ``cluster_by``; return what happened.

    ``table_ref`` is a catalog name (``cat.schema.table``) or a path ref (``delta.`/p```).
    Columns the table does not have are pruned first — registry defaults are a superset shared
    across a layer, exactly as the writers and ``ddl.py`` treat ``partition_by``.
    """
    label = table_label or table_ref
    wanted = [str(c) for c in (cluster_by or []) if c]
    if not wanted:
        return NOT_DECLARED
    ref = _quote(table_ref)
    try:
        detail = _detail(spark, ref)
        if not detail:
            return UNSUPPORTED
        if str(detail.get("format") or "").lower() != "delta":
            return NOT_DELTA  # Hive bucketing for parquet / orc is ddl.py's business, not this

        columns = set(spark.sql(f"SELECT * FROM {ref} LIMIT 0").columns)
        present = [c for c in wanted if c in columns]
        missing = [c for c in wanted if c not in columns]
        if missing:
            logger.warning(
                f"Cluster columns not present in {label} (pruned): {', '.join(missing)}. Expected when "
                f"_system.yaml defines a superset of cluster columns shared across contracts."
            )
        if not present:
            return NO_COLUMNS

        if detail.get("partitionColumns"):
            logger.warning(
                f"{label} is partitioned by [{', '.join(detail['partitionColumns'])}]; Delta cannot add "
                f"liquid clustering to a partitioned table, so cluster_by [{', '.join(present)}] was not "
                f"applied. Cluster within partitions with OPTIMIZE {label} ZORDER BY ({', '.join(present)})."
            )
            return PARTITIONED

        current = [str(c) for c in (detail.get("clusteringColumns") or [])]
        if current == present:
            return UNCHANGED

        spark.sql(f"ALTER TABLE {ref} CLUSTER BY ({', '.join(f'`{c}`' for c in present)})")
        logger.info(
            f"Liquid clustering on {label}: [{', '.join(current) or 'none'}] -> [{', '.join(present)}]. "
            f"Existing files are clustered by the next OPTIMIZE."
        )
        if not current:
            # Measured against delta-spark 4.0 + deltalake 1.6.3: adding clustering raises the
            # table to writer version 7 (features clustering + domainMetadata), and delta-rs then
            # refuses to WRITE it ("Unsupported table features required: [ClusteredTable,
            # DomainMetadata]"). Reads are unaffected. So a table shared with the polars / duckdb
            # engines becomes Spark-write-only — say so, once, when clustering is first added.
            logger.warning(
                f"{label} now requires Delta writer version 7 (liquid clustering). Spark and "
                f"Databricks write it normally, but delta-rs — the writer behind the polars and "
                f"duckdb engines — can only READ a clustered table. Remove cluster_by if another "
                f"engine also writes this table."
            )
        return APPLIED
    except Exception as exc:  # an optimisation must never fail the write that preceded it
        logger.warning(
            f"Could not apply liquid clustering [{', '.join(wanted)}] to {label}: {type(exc).__name__}: "
            f"{str(exc)[:200]}. The runtime may predate liquid clustering (Delta 3.1+ / DBR 13.3+)."
        )
        return UNSUPPORTED
