"""Make an existing table match the contract's model before a write.

The contract is the table's schema. When the contract changes, the table follows it:

* a column the contract ADDS is added to the table (``ALTER TABLE ... ADD COLUMNS``);
* a column the contract REMOVES is kept, with a warning: nothing is deleted;
* a type that WIDENS (``int`` -> ``bigint``, ``float`` -> ``double``, a larger decimal) is changed
  in place (``ALTER COLUMN ... TYPE``), with no data rewritten;
* any OTHER type change needs every row rewritten, which a scheduled pipeline never does on its own:
  the run stops with ``SchemaChangeNeedsMigration`` and names ``lakelogic migrate``. The migration
  casts strictly (a value that does not convert fails it, never becomes null) and is one commit,
  so the previous version stays in the table's history.

Whether a change is ALLOWED (breaking or not) is the platform's question, not this module's.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

#: Type changes Delta and Iceberg can make in place: the stored values still read as the new type.
_WIDENING = {
    ("tinyint", "smallint"),
    ("tinyint", "int"),
    ("tinyint", "bigint"),
    ("smallint", "int"),
    ("smallint", "bigint"),
    ("int", "bigint"),
    ("float", "double"),
}

_DECIMAL = re.compile(r"decimal\((\d+),\s*(\d+)\)")


class SchemaChangeNeedsMigration(RuntimeError):
    """A contract type change the table can only take by rewriting its data."""


@dataclass
class AlignmentPlan:
    add: List[Tuple[str, str]] = field(default_factory=list)  # (column, type)
    widen: List[Tuple[str, str, str]] = field(default_factory=list)  # (column, from, to)
    rewrite: List[Tuple[str, str, str]] = field(default_factory=list)  # (column, from, to)
    kept: List[str] = field(default_factory=list)  # in the table, no longer in the contract

    @property
    def changes_table(self) -> bool:
        return bool(self.add or self.widen)


def is_widening(old: str, new: str) -> bool:
    if (old, new) in _WIDENING:
        return True
    a, b = _DECIMAL.fullmatch(old), _DECIMAL.fullmatch(new)
    if a and b:
        (p1, s1), (p2, s2) = map(int, a.groups()), map(int, b.groups())
        return s2 >= s1 and (p2 - s2) >= (p1 - s1)  # room for every digit on both sides of the point
    return False


def plan_alignment(
    table_types: Dict[str, str], contract_types: Dict[str, str], managed: Optional[set] = None
) -> AlignmentPlan:
    """What the table needs, given its column types and the contract's (Spark type names).

    Columns are matched case-insensitively, as Spark resolves them. A contract column with no known
    type is left alone. Columns whose names start with ``_lakelogic`` are the engine's own.
    """
    managed_l = {m.lower() for m in (managed or set())}
    have = {k.lower(): (k, v) for k, v in table_types.items()}
    want = {k.lower(): (k, v) for k, v in contract_types.items() if v}
    plan = AlignmentPlan()
    for key, (name, new) in want.items():
        if key not in have:
            plan.add.append((name, new))
            continue
        old = have[key][1]
        if old == new or key in managed_l:
            # A column the ENGINE manages (SCD2 effective dates, version, current flag, surrogate
            # key) is stored in the engine's own type, whatever the contract declares for it.
            continue
        (plan.widen if is_widening(old, new) else plan.rewrite).append((have[key][0], old, new))
    plan.kept = [name for key, (name, _) in have.items() if key not in want and not key.startswith("_lakelogic")]
    return plan


def contract_spark_types(contract: Any) -> Dict[str, str]:
    """``{column: spark type}`` for every declared field whose type maps to Spark."""
    from lakelogic.core.types import spark_type_object

    out: Dict[str, str] = {}
    for f in getattr(getattr(contract, "model", None), "fields", None) or []:
        try:
            t = spark_type_object(str(getattr(f, "type", "") or ""))
        except Exception:  # noqa: BLE001 - an unmapped type is left alone, never guessed
            t = None
        if t is not None:
            out[f.name] = t.simpleString()
    return out


def managed_columns(contract: Any) -> set:
    """Columns the engine writes and types itself: SCD2 history columns and surrogate keys."""
    mat = getattr(contract, "materialization", None)
    if mat is None:
        return set()
    out: set = set()
    scd2 = getattr(mat, "scd2", None)
    scd2 = dict(scd2) if isinstance(scd2, dict) else {}
    if (getattr(mat, "strategy", None) or "").lower() == "scd2":
        out |= {
            scd2.get("effective_from_field", "effective_from"),
            scd2.get("effective_to_field", "effective_to"),
            scd2.get("current_flag_field", "is_current"),
            scd2.get("version_column", "_version"),
            scd2.get("change_reason_column", "_change_reason"),
        }
    for cfg in (scd2, getattr(mat, "scd1", None)):
        if isinstance(cfg, dict) and cfg.get("surrogate_key"):
            out.add(cfg["surrogate_key"])
    return {c for c in out if c}


def _strict(contract: Any) -> bool:
    try:
        from lakelogic.core.models import effective_schema_policy

        return (getattr(effective_schema_policy(contract), "evolution", None) or "").lower() == "strict"
    except Exception:  # noqa: BLE001
        return False


def _refuse_if_strict(table: str, plan: AlignmentPlan, contract: Any) -> None:
    """``strict`` (OLC): the incoming schema must match the table exactly; nothing changes on its own,
    declared or not. Every other policy lets the table follow the contract."""
    if _strict(contract) and (plan.add or plan.widen or plan.rewrite):
        changes = [f"add {c}" for c, _ in plan.add] + [f"{c}: {o} -> {n}" for c, o, n in plan.widen + plan.rewrite]
        raise ValueError(
            f"Schema Evolution Blocked (policy=strict): {table} differs from the contract ({'; '.join(changes)}). "
            "Under strict nothing changes the table automatically: "
            "relax schema_policy.evolution, or run lakelogic migrate."
        )


def _needs_migration_message(table: str, rewrite: List[Tuple[str, str, str]]) -> str:
    cols = "; ".join(f"`{c}` is {old} in the table and {new} in the contract" for c, old, new in rewrite)
    return (
        f"{table}: {cols}. This change needs every row rewritten, which a pipeline run does not do on "
        f"its own. Run `lakelogic migrate --contract <contract>` to apply it (a strict cast: a value "
        f"that does not convert fails the migration, never becomes null)."
    )


# ── Spark ──────────────────────────────────────────────────────────────────────────────────────


def align_spark_table(
    spark: Any, table_ref: str, contract: Any, table_format: str = "delta"
) -> AlignmentPlan:  # pragma: no cover
    """Bring an existing Spark table (Delta or Iceberg) to the contract's model, or stop.

    ``table_ref`` is a catalog name, or ``delta.`<path>``` for a Delta table written by path.
    """
    schema = spark.table(table_ref).schema
    plan = plan_alignment(
        {f.name: f.dataType.simpleString() for f in schema}, contract_spark_types(contract), managed_columns(contract)
    )
    _refuse_if_strict(table_ref, plan, contract)

    if plan.rewrite:
        raise SchemaChangeNeedsMigration(_needs_migration_message(table_ref, plan.rewrite))
    for col in plan.kept:
        logger.warning(
            f"{table_ref}: `{col}` is in the table but not in the contract; it is kept, and new rows get null."
        )
    if plan.add:
        spark.sql(f"ALTER TABLE {table_ref} ADD COLUMNS (" + ", ".join(f"`{c}` {t}" for c, t in plan.add) + ")")
        for c, t in plan.add:
            logger.info(f"{table_ref}: added `{c}` {t} (declared in the contract)")
    if plan.widen:
        if table_format == "delta":
            spark.sql(f"ALTER TABLE {table_ref} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
        for c, old, new in plan.widen:
            try:
                spark.sql(f"ALTER TABLE {table_ref} ALTER COLUMN `{c}` TYPE {new}")
            except Exception as exc:  # the runtime cannot widen in place: that is a migration
                raise SchemaChangeNeedsMigration(
                    _needs_migration_message(table_ref, [(c, old, new)])
                    + f" (this runtime could not widen it in place: {str(exc).splitlines()[0]})"
                ) from exc
            logger.info(f"{table_ref}: `{c}` {old} -> {new} in place (no data rewritten)")
    return plan


def migrate_spark_table(
    spark: Any, table_ref: str, contract: Any, table_format: str = "delta"
) -> Dict[str, Any]:  # pragma: no cover
    """The deliberate rewrite: cast each changed column strictly, then one overwrite commit."""
    from pyspark.sql import functions as F

    if table_format != "delta":
        raise NotImplementedError(
            f"lakelogic migrate rewrites Delta tables. {table_ref} is {table_format}: change its column "
            "types with the catalog's own ALTER, or rewrite it with your engine."
        )
    df = spark.table(table_ref)
    plan = plan_alignment({f.name: f.dataType.simpleString() for f in df.schema}, contract_spark_types(contract))
    changes = plan.widen + plan.rewrite
    if not changes and not plan.add:
        return {"table": table_ref, "changed": []}

    try:
        version: Optional[int] = spark.sql(f"DESCRIBE HISTORY {table_ref} LIMIT 1").collect()[0]["version"]
    except Exception:  # noqa: BLE001 - history is a convenience for the log line
        version = None

    # Strict cast: on a failed conversion Spark's ANSI cast raises; nothing is committed.
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        for c, _old, new in changes:
            df = df.withColumn(c, F.col(c).cast(new))
        for c, t in plan.add:
            df = df.withColumn(c, F.lit(None).cast(t))
        parts = _spark_partition_columns(spark, table_ref)
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if parts:
            writer = writer.partitionBy(*parts)
        if table_ref.startswith("delta.`"):
            writer.save(table_ref[len("delta.`") : -1])
        else:
            writer.saveAsTable(table_ref)
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)

    changed = [f"{c}: {old} -> {new}" for c, old, new in changes] + [f"{c}: added {t}" for c, t in plan.add]
    logger.info(
        f"{table_ref}: migrated ({'; '.join(changed)})"
        + (
            f"; previous version {version} (RESTORE TABLE {table_ref} TO VERSION AS OF {version} to undo)"
            if version is not None
            else ""
        )
    )
    return {"table": table_ref, "changed": changed, "previous_version": version}


def _spark_partition_columns(spark: Any, table_ref: str) -> List[str]:  # pragma: no cover
    try:
        rows = spark.sql(f"DESCRIBE DETAIL {table_ref}").collect()
        return list(rows[0]["partitionColumns"] or [])
    except Exception:  # noqa: BLE001 - an unpartitioned table, or no DESCRIBE DETAIL
        return []


# ── Delta written by Polars / DuckDB (delta-rs) ───────────────────────────────────────────────


def delta_table_types(dt: Any) -> Dict[str, str]:
    """``{column: spark type name}`` of a delta-rs ``DeltaTable``; nested types left as JSON."""
    import json

    out: Dict[str, str] = {}
    for f in json.loads(dt.schema().to_json())["fields"]:
        t = f["type"]
        out[f["name"]] = _spark_name(t) if isinstance(t, str) else json.dumps(t, sort_keys=True)
    return out


def align_delta_rs_table(path: str, contract: Any, storage_options: Optional[Dict[str, str]] = None) -> AlignmentPlan:
    """Bring an existing Delta table (written by Polars/DuckDB through delta-rs) to the contract.

    delta-rs can add columns in place but cannot change a column's type in place: a WIDENING is
    made by rewriting (lossless, every value fits); any other type change stops the run and names
    ``lakelogic migrate``.
    """
    import pyarrow as pa
    from deltalake import DeltaTable, Schema

    dt = DeltaTable(path, storage_options=storage_options)
    plan = plan_alignment(delta_table_types(dt), contract_spark_types(contract), managed_columns(contract))
    _refuse_if_strict(path, plan, contract)

    if plan.rewrite:
        raise SchemaChangeNeedsMigration(_needs_migration_message(path, plan.widen + plan.rewrite))
    for col in plan.kept:
        logger.warning(f"{path}: `{col}` is in the table but not in the contract; it is kept, and new rows get null.")
    if plan.widen:
        # delta-rs has no in-place type change, so a widening is a rewrite here. Every value fits
        # the wider type (nothing can be lost), so the run makes it rather than stopping.
        migrate_delta_rs_table(path, contract, storage_options)
        return plan
    if plan.add:
        dt.alter.add_columns(Schema.from_arrow(pa.schema([pa.field(c, _arrow_type(t)) for c, t in plan.add])).fields)
        for c, t in plan.add:
            logger.info(f"{path}: added `{c}` {t} (declared in the contract)")
    return plan


def migrate_delta_rs_table(
    path: str, contract: Any, storage_options: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """The deliberate rewrite for a delta-rs table: a strict cast per changed column, one commit."""
    import pyarrow as pa
    import pyarrow.compute as pc
    from deltalake import DeltaTable, write_deltalake

    dt = DeltaTable(path, storage_options=storage_options)
    plan = plan_alignment(delta_table_types(dt), contract_spark_types(contract))
    changes = plan.widen + plan.rewrite
    if not changes and not plan.add:
        return {"table": path, "changed": []}

    data = dt.to_pyarrow_table()
    version = dt.version()
    for c, _old, new in changes:
        # safe=True: a value that does not convert raises; nothing is written.
        data = data.set_column(data.schema.get_field_index(c), c, pc.cast(data[c], _arrow_type(new), safe=True))
    for c, t in plan.add:
        data = data.append_column(c, pa.nulls(data.num_rows, _arrow_type(t)))
    write_deltalake(
        path,
        data,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=dt.metadata().partition_columns or None,
        storage_options=storage_options,
    )
    changed = [f"{c}: {old} -> {new}" for c, old, new in changes] + [f"{c}: added {t}" for c, t in plan.add]
    logger.info(f"{path}: migrated ({'; '.join(changed)}); previous version {version} (load it to undo)")
    return {"table": path, "changed": changed, "previous_version": version}


def _spark_name(type_name: str) -> str:
    """Delta's or Spark's name for a type, as Spark spells it (``long`` -> ``bigint``), via the registry."""
    if _DECIMAL.fullmatch(type_name):
        return type_name
    try:
        from lakelogic.core.types import spark_type_object

        t = spark_type_object(type_name)
        return t.simpleString() if t is not None else type_name
    except Exception:  # noqa: BLE001 - a name the registry does not know is compared as written
        return type_name


def _arrow_type(spark_type: str) -> Any:
    """The Arrow type Delta stores for a Spark type name, via the registry."""
    import pyarrow as pa

    from lakelogic.core.types import arrow_type

    m = _DECIMAL.fullmatch(spark_type)
    if m:
        return pa.decimal128(int(m.group(1)), int(m.group(2)))
    if spark_type == "timestamp":  # Delta's `timestamp` is UTC-adjusted
        return pa.timestamp("us", tz="UTC")
    if spark_type == "timestamp_ntz":
        return pa.timestamp("us")
    return pa.type_for_alias(arrow_type(spark_type))
