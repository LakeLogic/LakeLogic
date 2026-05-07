"""
GDPR compliance utilities for LakeLogic.

Provides right-to-forget (erasure) and PII masking/nullification capabilities
driven by the contract's ``pii: true`` field annotations.

Usage (Python API):
    from lakelogic.core.gdpr import forget_subjects, mask_pii_columns

    # Erase specific subjects from a dataframe
    cleaned_df = forget_subjects(df, contract, subject_column="customer_id",
                                  subject_ids=["cust_123", "cust_456"])

    # Mask all PII columns (nullify, hash, or redact)
    masked_df = mask_pii_columns(df, contract, strategy="nullify")

Usage via DataProcessor:
    proc = DataProcessor("contract.yaml")
    cleaned = proc.forget(subject_column="customer_id", subject_ids=["cust_123"])
    masked = proc.mask_pii(strategy="hash")
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from loguru import logger

from lakelogic.core.constants import (
    DELETE_REASON_GDPR_ART17,
    ERASURE_NULLIFY,
    META_DELETE_REASON,
    META_DELETED_AT,
    META_IS_DELETED,
    META_UPDATED_AT,
    VALID_ERASURE_STRATEGIES,
)
from lakelogic.core.models import DataContract, FieldDefinition


def _get_pii_fields(contract: DataContract) -> List[FieldDefinition]:
    """Extract field definitions marked as PII from the contract."""
    if not contract.model or not contract.model.fields:
        return []
    return [f for f in contract.model.fields if f.pii]


def _get_pii_column_names(contract: DataContract) -> List[str]:
    """Get the names of PII-marked columns."""
    return [f.name for f in _get_pii_fields(contract)]


def _hash_value(value: Any, salt: str = "") -> Optional[str]:
    """
    One-way SHA-256 hash of a value. Returns None for null values.

    Args:
        value: Value to hash.
        salt: Optional salt for the hash.

    Returns:
        Hex digest string or None.
    """
    if value is None or (isinstance(value, float) and value != value):  # NaN check
        return None
    raw = f"{salt}{value}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _redact_value(value: Any, replacement: str = "***REDACTED***") -> Optional[str]:
    """Replace a value with a redaction marker. Returns None for null values."""
    if value is None or (isinstance(value, float) and value != value):
        return None
    return replacement


# ── Polars implementations ───────────────────────────────────────────────────


def _forget_polars(
    df,
    pii_columns: List[str],
    subject_column: str,
    subject_ids: List[str],
    erasure_strategy: str,
    hash_salt: str,
    partition_filter: Optional[Dict[str, str]] = None,
    delete_reason: str = DELETE_REASON_GDPR_ART17,
    strategy_per_field: Optional[Dict[str, str]] = None,
):
    """Erase/mask PII for specific subjects in a Polars DataFrame."""
    import polars as pl

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    present_pii = [c for c in pii_columns if c in df.columns]
    if not present_pii:
        logger.info("No PII columns found in dataframe; nothing to erase.")
        return df

    if subject_column not in df.columns:
        raise ValueError(f"Subject column '{subject_column}' not found in dataframe.")

    # Cast subject_ids to match column type
    subject_ids_set = set(str(s) for s in subject_ids)
    mask = df[subject_column].cast(pl.Utf8).is_in(list(subject_ids_set))

    # Scope erasure to a specific partition (e.g. country_code = 'FR')
    if partition_filter:
        pcol, pval = partition_filter["column"], partition_filter["value"]
        if pcol in df.columns:
            mask = mask & (df[pcol].cast(pl.Utf8) == pval)
        else:
            logger.warning(f"Partition column '{pcol}' not found in dataframe; ignoring partition filter.")

    strategy_map = strategy_per_field or {}
    for col in present_pii:
        col_strategy = strategy_map.get(col, erasure_strategy)

        if col == subject_column and col_strategy == "nullify":
            # Don't nullify the subject column itself in nullify mode
            # (would lose the key needed for audit)
            continue

        if col_strategy == "nullify":
            df = df.with_columns(pl.when(mask).then(pl.lit(None)).otherwise(pl.col(col)).alias(col))
        elif col_strategy == "hash":
            # Hash values for matching subjects
            df = df.with_columns(
                pl.when(mask)
                .then(
                    pl.col(col)
                    .cast(pl.Utf8)
                    .map_elements(
                        lambda v, _salt=hash_salt: _hash_value(v, _salt),
                        return_dtype=pl.Utf8,
                    )
                )
                .otherwise(pl.col(col))
                .alias(col)
            )
        elif col_strategy == "redact":
            df = df.with_columns(pl.when(mask).then(pl.lit("***REDACTED***")).otherwise(pl.col(col)).alias(col))

    affected_count = int(mask.sum())

    # ── Set compliance metadata on affected rows ─────────────────────────────
    now = datetime.now(timezone.utc).isoformat()
    if META_IS_DELETED not in df.columns:
        df = df.with_columns(pl.lit(False).alias(META_IS_DELETED))
    if META_DELETED_AT not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(META_DELETED_AT))
    if META_DELETE_REASON not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(META_DELETE_REASON))
    if META_UPDATED_AT not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(META_UPDATED_AT))

    df = df.with_columns(
        pl.when(mask).then(pl.lit(True)).otherwise(pl.col(META_IS_DELETED)).alias(META_IS_DELETED),
        pl.when(mask).then(pl.lit(now)).otherwise(pl.col(META_DELETED_AT)).alias(META_DELETED_AT),
        pl.when(mask).then(pl.lit(delete_reason)).otherwise(pl.col(META_DELETE_REASON)).alias(META_DELETE_REASON),
        pl.when(mask).then(pl.lit(now)).otherwise(pl.col(META_UPDATED_AT)).alias(META_UPDATED_AT),
    )

    logger.info(f"GDPR erasure ({erasure_strategy}): affected {affected_count} rows, {len(present_pii)} PII columns.")
    return df


def _forget_pandas(
    df,
    pii_columns: List[str],
    subject_column: str,
    subject_ids: List[str],
    erasure_strategy: str,
    hash_salt: str,
    partition_filter: Optional[Dict[str, str]] = None,
    delete_reason: str = DELETE_REASON_GDPR_ART17,
    strategy_per_field: Optional[Dict[str, str]] = None,
):
    """Erase/mask PII for specific subjects in a Pandas DataFrame."""

    present_pii = [c for c in pii_columns if c in df.columns]
    if not present_pii:
        logger.info("No PII columns found in dataframe; nothing to erase.")
        return df

    if subject_column not in df.columns:
        raise ValueError(f"Subject column '{subject_column}' not found in dataframe.")

    df = df.copy()
    subject_ids_set = set(str(s) for s in subject_ids)
    mask = df[subject_column].astype(str).isin(subject_ids_set)

    # Scope erasure to a specific partition (e.g. country_code = 'FR')
    if partition_filter:
        pcol, pval = partition_filter["column"], partition_filter["value"]
        if pcol in df.columns:
            mask = mask & (df[pcol].astype(str) == pval)
        else:
            logger.warning(f"Partition column '{pcol}' not found in dataframe; ignoring partition filter.")

    affected_count = mask.sum()

    for col in present_pii:
        if col == subject_column and erasure_strategy == "nullify":
            continue

        if erasure_strategy == "nullify":
            df.loc[mask, col] = None
        elif erasure_strategy == "hash":
            df.loc[mask, col] = df.loc[mask, col].apply(lambda v: _hash_value(v, hash_salt))
        elif erasure_strategy == "redact":
            df.loc[mask, col] = "***REDACTED***"

    logger.info(f"GDPR erasure ({erasure_strategy}): affected {affected_count} rows, {len(present_pii)} PII columns.")

    # ── Set compliance metadata on affected rows ─────────────────────────────
    now = datetime.now(timezone.utc).isoformat()
    if META_IS_DELETED not in df.columns:
        df[META_IS_DELETED] = False
    if META_DELETED_AT not in df.columns:
        df[META_DELETED_AT] = None
    if META_DELETE_REASON not in df.columns:
        df[META_DELETE_REASON] = None
    if META_UPDATED_AT not in df.columns:
        df[META_UPDATED_AT] = None

    df.loc[mask, META_IS_DELETED] = True
    df.loc[mask, META_DELETED_AT] = now
    df.loc[mask, META_DELETE_REASON] = delete_reason
    df.loc[mask, META_UPDATED_AT] = now

    return df


def _mask_polars(df, pii_columns: List[str], strategy: str, hash_salt: str):
    """Mask all PII columns in a Polars DataFrame (all rows)."""
    import polars as pl

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    present_pii = [c for c in pii_columns if c in df.columns]
    if not present_pii:
        logger.info("No PII columns found in dataframe; nothing to mask.")
        return df

    for col in present_pii:
        if strategy == "nullify":
            df = df.with_columns(pl.lit(None).alias(col))
        elif strategy == "hash":
            df = df.with_columns(
                pl.col(col)
                .cast(pl.Utf8)
                .map_elements(
                    lambda v, _salt=hash_salt: _hash_value(v, _salt),
                    return_dtype=pl.Utf8,
                )
                .alias(col)
            )
        elif strategy == "redact":
            df = df.with_columns(pl.lit("***REDACTED***").alias(col))

    logger.info(f"PII masking ({strategy}): masked {len(present_pii)} columns across all rows.")
    return df


def _mask_pandas(df, pii_columns: List[str], strategy: str, hash_salt: str):
    """Mask all PII columns in a Pandas DataFrame (all rows)."""
    df = df.copy()
    present_pii = [c for c in pii_columns if c in df.columns]
    if not present_pii:
        logger.info("No PII columns found in dataframe; nothing to mask.")
        return df

    for col in present_pii:
        if strategy == "nullify":
            df[col] = None
        elif strategy == "hash":
            df[col] = df[col].apply(lambda v: _hash_value(v, hash_salt))
        elif strategy == "redact":
            df[col] = "***REDACTED***"

    logger.info(f"PII masking ({strategy}): masked {len(present_pii)} columns across {len(df)} rows.")
    return df


# ── PySpark implementations ─────────────────────────────────────────────


def _forget_pyspark(
    df,
    pii_columns: List[str],
    subject_column: str,
    subject_ids: List[str],
    erasure_strategy: str,
    hash_salt: str,
    partition_filter: Optional[Dict[str, str]] = None,
    delete_reason: str = DELETE_REASON_GDPR_ART17,
):
    """Erase/mask PII for specific subjects in a PySpark DataFrame."""
    from pyspark.sql import functions as F

    present_pii = [c for c in pii_columns if c in df.columns]
    if not present_pii:
        logger.info("No PII columns found in dataframe; nothing to erase.")
        return df

    if subject_column not in df.columns:
        raise ValueError(f"Subject column '{subject_column}' not found in dataframe.")

    # Build the subject mask
    mask = F.col(subject_column).cast("string").isin([str(s) for s in subject_ids])

    # Scope erasure to a specific partition
    if partition_filter:
        pcol, pval = partition_filter["column"], partition_filter["value"]
        if pcol in df.columns:
            mask = mask & (F.col(pcol).cast("string") == pval)
        else:
            logger.warning(f"Partition column '{pcol}' not found in dataframe; ignoring partition filter.")

    for col in present_pii:
        if col == subject_column and erasure_strategy == "nullify":
            continue

        if erasure_strategy == "nullify":
            df = df.withColumn(col, F.when(mask, F.lit(None)).otherwise(F.col(col)))
        elif erasure_strategy == "hash":
            # SHA-256 hash with salt
            df = df.withColumn(
                col,
                F.when(mask, F.sha2(F.concat(F.lit(hash_salt), F.col(col).cast("string")), 256)).otherwise(F.col(col)),
            )
        elif erasure_strategy == "redact":
            df = df.withColumn(col, F.when(mask, F.lit("***REDACTED***")).otherwise(F.col(col)))

    # ── Set compliance metadata on affected rows ─────────────────────────
    now = datetime.now(timezone.utc).isoformat()
    for meta_col, default_val in [
        (META_IS_DELETED, False),
        (META_DELETED_AT, None),
        (META_DELETE_REASON, None),
        (META_UPDATED_AT, None),
    ]:
        if meta_col not in df.columns:
            df = df.withColumn(meta_col, F.lit(default_val))

    df = df.withColumn(META_IS_DELETED, F.when(mask, F.lit(True)).otherwise(F.col(META_IS_DELETED)))
    df = df.withColumn(META_DELETED_AT, F.when(mask, F.lit(now)).otherwise(F.col(META_DELETED_AT)))
    df = df.withColumn(META_DELETE_REASON, F.when(mask, F.lit(delete_reason)).otherwise(F.col(META_DELETE_REASON)))
    df = df.withColumn(META_UPDATED_AT, F.when(mask, F.lit(now)).otherwise(F.col(META_UPDATED_AT)))

    logger.info(f"GDPR erasure ({erasure_strategy}): processed PySpark DataFrame, {len(present_pii)} PII columns.")
    return df


def _mask_pyspark(df, pii_columns: List[str], strategy: str, hash_salt: str):
    """Mask all PII columns in a PySpark DataFrame (all rows)."""
    from pyspark.sql import functions as F

    present_pii = [c for c in pii_columns if c in df.columns]
    if not present_pii:
        logger.info("No PII columns found in dataframe; nothing to mask.")
        return df

    for col in present_pii:
        if strategy == "nullify":
            df = df.withColumn(col, F.lit(None))
        elif strategy == "hash":
            df = df.withColumn(col, F.sha2(F.concat(F.lit(hash_salt), F.col(col).cast("string")), 256))
        elif strategy == "redact":
            df = df.withColumn(col, F.lit("***REDACTED***"))

    logger.info(f"PII masking ({strategy}): masked {len(present_pii)} PySpark columns across all rows.")
    return df


# ── Public API ───────────────────────────────────────────────────────────────


def forget_subjects(
    df: Any,
    contract: DataContract,
    subject_column: str,
    subject_ids: List[str],
    *,
    erasure_strategy: str = ERASURE_NULLIFY,
    hash_salt: str = "",
    audit: bool = True,
    partition_filter: Optional[Dict[str, str]] = None,
    delete_reason: str = DELETE_REASON_GDPR_ART17,
    compliance_event: Optional[Dict[str, Any]] = None,
    audit_report_out: Optional[List[Dict[str, Any]]] = None,
) -> Any:
    """
    GDPR Right-to-be-Forgotten: erase PII for specific data subjects.

    Identifies PII columns from the contract's ``pii: true`` field annotations
    and nullifies, hashes, or redacts values for matching subjects.

    Args:
        df: Input dataframe (Polars, Pandas, or DuckDB relation).
        contract: DataContract with PII-annotated fields.
        subject_column: Column containing the data subject identifier
                        (e.g., "customer_id", "email").
        subject_ids: List of subject identifiers to erase.
        erasure_strategy: How to erase PII values:
            - "nullify" (default): Set PII fields to NULL.
            - "hash": Replace with one-way SHA-256 hash.
            - "redact": Replace with "***REDACTED***".
        hash_salt: Salt for hashing (only used with "hash" strategy).
        audit: If True, log an audit entry about the erasure.
        partition_filter: Optional dict with 'column' and 'value' keys to scope
                          erasure to a specific partition (e.g. {"column": "country_code", "value": "FR"}).
                          When provided, only rows matching BOTH the subject_ids AND
                          the partition predicate are erased. Critical for multi-region
                          lakehouses where IDs may be reused across partitions.

    Returns:
        DataFrame with PII erased for the specified subjects.

    Raises:
        ValueError: If subject_column not in dataframe or no PII fields defined.
    """
    if not compliance_event and getattr(contract, "compliance", None):
        compliance_event = contract.compliance

    strategy_per_field = None
    if compliance_event:
        erasure_strategy = compliance_event.get("strategy", erasure_strategy)
        strategy_per_field = compliance_event.get("strategy_per_field")

    if erasure_strategy not in VALID_ERASURE_STRATEGIES:
        raise ValueError(f"Invalid erasure_strategy: {erasure_strategy}. Must be one of {VALID_ERASURE_STRATEGIES}.")

    pii_columns = _get_pii_column_names(contract)
    if not pii_columns:
        logger.warning(
            "No PII fields defined in contract. "
            "Mark fields with 'pii: true' in the model section to enable GDPR erasure."
        )
        return df

    # ── Build audit report (written AFTER erasure succeeds) ─────────────
    _audit_report = None
    _audit_engine = None
    if audit:
        partition_msg = ""
        if partition_filter:
            partition_msg = f", partition={partition_filter['column']}='{partition_filter['value']}'"

        timestamp_iso = datetime.now(timezone.utc).isoformat()
        logger.info(
            f"GDPR erasure request: strategy={erasure_strategy}, "
            f"subjects={len(subject_ids)}, pii_columns={pii_columns}{partition_msg}, "
            f"timestamp={timestamp_iso}"
        )

        try:
            import uuid

            # Build engine name from dataframe type since we don't have processor context here
            _audit_engine = "pandas"
            df_type = str(type(df)).lower()
            if "polars" in df_type:
                _audit_engine = "polars"
            elif "pyspark" in df_type:
                _audit_engine = "spark"
            elif "duckdb" in df_type or hasattr(df, "fetchdf"):
                _audit_engine = "duckdb"

            metadata = getattr(contract, "metadata", {}) or {}
            erasure_report = generate_erasure_report(
                contract,
                subject_column,
                subject_ids,
                erasure_strategy,
                partition_filter=partition_filter,
                compliance_event=compliance_event,
            )
            if strategy_per_field:
                erasure_report["strategy_per_field"] = strategy_per_field

            run_id = f"erasure_{uuid.uuid4().hex[:8]}"
            _audit_report = {
                "run_id": run_id,
                "pipeline_run_id": run_id,  # standalone event
                "timestamp": timestamp_iso,
                "start_time": timestamp_iso,
                "end_time": timestamp_iso,
                "run_duration_seconds": 0.0,
                "engine": _audit_engine,
                "contract": contract.info.title if getattr(contract, "info", None) else None,
                "dataset": getattr(contract, "dataset", None),
                "domain": metadata.get("domain", ""),
                "system": metadata.get("system", ""),
                "stage": "gdpr_erasure",
                "compliance_event": compliance_event,
                "status": "ok",
                "counts": {
                    "total": len(subject_ids),
                    "good": len(subject_ids),
                    "quarantined": len(subject_ids),  # repurpose to mean 'erased count' for easy dashboarding
                    "quarantine_ratio": 1.0,
                },
                "erasure_report": erasure_report,
            }
            if audit_report_out is not None:
                audit_report_out.append(_audit_report)
        except Exception as e:
            logger.warning(f"Failed to build GDPR erasure audit report: {e}")

    # ── Detect frame type and dispatch ────────────────────────────────
    result = None

    try:
        try:
            import polars as pl

            if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
                result = _forget_polars(
                    df,
                    pii_columns,
                    subject_column,
                    subject_ids,
                    erasure_strategy,
                    hash_salt,
                    partition_filter,
                    delete_reason,
                    strategy_per_field=strategy_per_field,
                )
        except ImportError:
            pass

        if result is None:
            try:
                import pandas as pd

                if isinstance(df, pd.DataFrame):
                    result = _forget_pandas(
                        df,
                        pii_columns,
                        subject_column,
                        subject_ids,
                        erasure_strategy,
                        hash_salt,
                        partition_filter,
                        delete_reason,
                        strategy_per_field=strategy_per_field,
                    )
            except ImportError:
                pass

        if result is None:
            # PySpark DataFrame
            try:
                from pyspark.sql import DataFrame as SparkDataFrame

                if isinstance(df, SparkDataFrame):
                    result = _forget_pyspark(
                        df,
                        pii_columns,
                        subject_column,
                        subject_ids,
                        erasure_strategy,
                        hash_salt,
                        partition_filter,
                        delete_reason,
                    )
            except ImportError:
                pass

        if result is None:
            # DuckDB relation → convert to pandas, process, convert back
            if hasattr(df, "fetchdf"):
                import pandas as pd

                pdf = df.fetchdf()
                pdf_result = _forget_pandas(
                    pdf,
                    pii_columns,
                    subject_column,
                    subject_ids,
                    erasure_strategy,
                    hash_salt,
                    partition_filter,
                    delete_reason,
                )
                import duckdb

                result = duckdb.from_df(pdf_result)

        if result is None:
            raise TypeError(f"Unsupported dataframe type: {type(df)}")

    except Exception as exc:
        # Record failure in audit log before re-raising
        if _audit_report is not None:
            try:
                from lakelogic.core.run_log import write_run_log

                _audit_report["status"] = "error"
                _audit_report["error_message"] = str(exc)
                _audit_report["end_time"] = datetime.now(timezone.utc).isoformat()
                write_run_log(_audit_report, contract, engine_name=_audit_engine)
            except Exception:
                pass
        raise

    # ── Write audit log after successful erasure ──────────────────────
    if _audit_report is not None:
        try:
            from lakelogic.core.run_log import write_run_log

            _audit_report["end_time"] = datetime.now(timezone.utc).isoformat()
            write_run_log(_audit_report, contract, engine_name=_audit_engine)
        except Exception as e:
            logger.warning(f"Failed to record GDPR erasure to run_log: {e}")

    return result


def mask_pii_columns(
    df: Any,
    contract: DataContract,
    *,
    strategy: str = "nullify",
    hash_salt: str = "",
    columns: Optional[List[str]] = None,
) -> Any:
    """
    Mask all PII columns across all rows in a dataframe.

    Useful for creating anonymised datasets for development, testing,
    or analytics where PII is not needed.

    Args:
        df: Input dataframe (Polars, Pandas, or DuckDB relation).
        contract: DataContract with PII-annotated fields.
        strategy: Masking strategy:
            - "nullify" (default): Set to NULL.
            - "hash": One-way SHA-256 hash (preserves referential integrity).
            - "redact": Replace with "***REDACTED***".
        hash_salt: Salt for hashing.
        columns: Optional list of specific columns to mask (overrides
                 contract PII annotations).

    Returns:
        DataFrame with PII columns masked.
    """
    if strategy not in ("nullify", "hash", "redact"):
        raise ValueError(f"Invalid strategy: {strategy}. Must be 'nullify', 'hash', or 'redact'.")

    pii_columns = columns or _get_pii_column_names(contract)
    if not pii_columns:
        logger.warning("No PII columns to mask.")
        return df

    logger.info(f"Masking PII columns: {pii_columns} with strategy '{strategy}'")

    try:
        import polars as pl

        if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return _mask_polars(df, pii_columns, strategy, hash_salt)
    except ImportError:
        pass

    try:
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            return _mask_pandas(df, pii_columns, strategy, hash_salt)
    except ImportError:
        pass

    # PySpark DataFrame
    try:
        from pyspark.sql import DataFrame as SparkDataFrame

        if isinstance(df, SparkDataFrame):
            return _mask_pyspark(df, pii_columns, strategy, hash_salt)
    except ImportError:
        pass

    if hasattr(df, "fetchdf"):
        import pandas as pd

        pdf = df.fetchdf()
        result = _mask_pandas(pdf, pii_columns, strategy, hash_salt)
        import duckdb

        return duckdb.from_df(result)

    raise TypeError(f"Unsupported dataframe type: {type(df)}")


def generate_erasure_report(
    contract: DataContract,
    subject_column: str,
    subject_ids: List[str],
    erasure_strategy: str = "nullify",
    affected_rows: Optional[int] = None,
    partition_filter: Optional[Dict[str, str]] = None,
    compliance_event: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate a GDPR-compliant audit report for an erasure operation.

    Args:
        contract: DataContract.
        subject_column: Column used for subject identification.
        subject_ids: List of erased subject IDs.
        erasure_strategy: Strategy used.
        affected_rows: Number of rows affected (if known).
        partition_filter: Optional partition scope dict.

    Returns:
        Audit report dictionary.
    """
    pii_columns = _get_pii_column_names(contract)
    contract_name = contract.info.title if contract.info else "unknown"

    report: Dict[str, Any] = {
        "report_type": "gdpr_erasure",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "contract": contract_name,
        "subject_column": subject_column,
        "subjects_erased": len(subject_ids),
        "erasure_strategy": erasure_strategy,
        "pii_columns_affected": pii_columns,
        "affected_rows": affected_rows,
        "compliance_note": (
            "PII data has been erased in accordance with GDPR Article 17 "
            "(Right to Erasure). This report should be retained for audit purposes."
        ),
    }

    if compliance_event:
        report["compliance_event"] = compliance_event

    if partition_filter:
        report["partition_filter"] = partition_filter

    return report
