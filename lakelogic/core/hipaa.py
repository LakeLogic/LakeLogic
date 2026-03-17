"""
HIPAA compliance utilities for LakeLogic.

Provides right-to-forget (erasure/deletion) and PHI (Protected Health Information) 
masking/nullification capabilities driven by the contract's ``phi: true`` field annotations.

Usage (Python API):
    from lakelogic.core.hipaa import forget_patients, mask_phi_columns

    # Erase specific patients from a dataframe
    cleaned_df = forget_patients(df, contract, patient_column="patient_id",
                                  patient_ids=["P_123", "P_456"])

    # Mask all PHI columns (nullify, hash, or redact) for Safe Harbor
    masked_df = mask_phi_columns(df, contract, strategy="nullify")
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from loguru import logger

from lakelogic.core.models import DataContract, FieldDefinition


def _get_phi_fields(contract: DataContract) -> List[FieldDefinition]:
    """Extract field definitions marked as PHI or PII from the contract."""
    if not contract.model or not contract.model.fields:
        return []
    return [f for f in contract.model.fields if f.phi or f.pii]


def _get_phi_column_names(contract: DataContract) -> List[str]:
    """Get the names of PHI-marked columns."""
    return [f.name for f in _get_phi_fields(contract)]


def _hash_value(value: Any, salt: str = "") -> Optional[str]:
    """One-way SHA-256 hash of a value."""
    if value is None or (isinstance(value, float) and value != value):
        return None
    raw = f"{salt}{value}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# ── Polars implementations ───────────────────────────────────────────────────

def _forget_polars(
    df,
    phi_columns: List[str],
    patient_column: str,
    patient_ids: List[str],
    erasure_strategy: str,
    hash_salt: str,
    partition_filter: Optional[Dict[str, str]] = None,
):
    import polars as pl
    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    present_phi = [c for c in phi_columns if c in df.columns]
    if not present_phi:
        logger.info("No PHI columns found in dataframe; nothing to erase.")
        return df

    if patient_column not in df.columns:
        raise ValueError(f"Patient column '{patient_column}' not found in dataframe.")

    patient_ids_set = set(str(s) for s in patient_ids)
    mask = df[patient_column].cast(pl.Utf8).is_in(list(patient_ids_set))

    # Scope erasure to a specific partition (e.g. region = 'US-EAST')
    if partition_filter:
        pcol, pval = partition_filter["column"], partition_filter["value"]
        if pcol in df.columns:
            mask = mask & (df[pcol].cast(pl.Utf8) == pval)
        else:
            logger.warning(f"Partition column '{pcol}' not found in dataframe; ignoring partition filter.")

    for col in present_phi:
        if col == patient_column and erasure_strategy == "nullify":
            continue

        if erasure_strategy == "nullify":
            df = df.with_columns(pl.when(mask).then(pl.lit(None)).otherwise(pl.col(col)).alias(col))
        elif erasure_strategy == "hash":
            df = df.with_columns(
                pl.when(mask).then(
                    pl.col(col).cast(pl.Utf8).map_elements(lambda v, _s=hash_salt: _hash_value(v, _s), return_dtype=pl.Utf8)
                ).otherwise(pl.col(col)).alias(col)
            )
        elif erasure_strategy == "redact":
            df = df.with_columns(pl.when(mask).then(pl.lit("***REDACTED_PHI***")).otherwise(pl.col(col)).alias(col))

    return df


def _forget_pandas(
    df,
    phi_columns: List[str],
    patient_column: str,
    patient_ids: List[str],
    erasure_strategy: str,
    hash_salt: str,
    partition_filter: Optional[Dict[str, str]] = None,
):
    present_phi = [c for c in phi_columns if c in df.columns]
    if not present_phi:
        logger.info("No PHI columns found in dataframe; nothing to erase.")
        return df

    if patient_column not in df.columns:
        raise ValueError(f"Patient column '{patient_column}' not found in dataframe.")

    df = df.copy()
    patient_ids_set = set(str(s) for s in patient_ids)
    mask = df[patient_column].astype(str).isin(patient_ids_set)

    # Scope erasure to a specific partition (e.g. region = 'US-EAST')
    if partition_filter:
        pcol, pval = partition_filter["column"], partition_filter["value"]
        if pcol in df.columns:
            mask = mask & (df[pcol].astype(str) == pval)
        else:
            logger.warning(f"Partition column '{pcol}' not found in dataframe; ignoring partition filter.")

    affected_count = mask.sum()

    for col in present_phi:
        if col == patient_column and erasure_strategy == "nullify":
            continue
        if erasure_strategy == "nullify":
            df.loc[mask, col] = None
        elif erasure_strategy == "hash":
            df.loc[mask, col] = df.loc[mask, col].apply(lambda v: _hash_value(v, hash_salt))
        elif erasure_strategy == "redact":
            df.loc[mask, col] = "***REDACTED_PHI***"

    logger.info(f"HIPAA erasure ({erasure_strategy}): affected {affected_count} rows, {len(present_phi)} PHI columns.")
    return df


def _mask_polars(df, phi_columns: List[str], strategy: str, hash_salt: str):
    import polars as pl
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    present_phi = [c for c in phi_columns if c in df.columns]
    if not present_phi:
        return df
    for col in present_phi:
        if strategy == "nullify":
            df = df.with_columns(pl.lit(None).alias(col))
        elif strategy == "hash":
            df = df.with_columns(pl.col(col).cast(pl.Utf8).map_elements(lambda v, _s=hash_salt: _hash_value(v, _s), return_dtype=pl.Utf8).alias(col))
        elif strategy == "redact":
            df = df.with_columns(pl.lit("***REDACTED_PHI***").alias(col))
    return df


def _mask_pandas(df, phi_columns: List[str], strategy: str, hash_salt: str):
    df = df.copy()
    present_phi = [c for c in phi_columns if c in df.columns]
    if not present_phi:
        return df
    for col in present_phi:
        if strategy == "nullify":
            df[col] = None
        elif strategy == "hash":
            df[col] = df[col].apply(lambda v: _hash_value(v, hash_salt))
        elif strategy == "redact":
            df[col] = "***REDACTED_PHI***"
    return df


# ── Public API ───────────────────────────────────────────────────────────────

def forget_patients(
    df: Any,
    contract: DataContract,
    patient_column: str,
    patient_ids: List[str],
    *,
    erasure_strategy: str = "nullify",
    hash_salt: str = "",
    audit: bool = True,
    partition_filter: Optional[Dict[str, str]] = None,
) -> Any:
    """HIPAA Right-to-be-Forgotten: erase PHI for specific patients."""
    if erasure_strategy not in ("nullify", "hash", "redact"):
        raise ValueError(f"Invalid erasure_strategy: {erasure_strategy}. Must be 'nullify', 'hash', or 'redact'.")

    phi_columns = _get_phi_column_names(contract)
    if not phi_columns:
        logger.warning("No PHI fields defined in contract.")
        return df

    if audit:
        partition_msg = ""
        if partition_filter:
            partition_msg = f", partition={partition_filter['column']}='{partition_filter['value']}'"
        logger.info(
            f"HIPAA erasure request: strategy={erasure_strategy}, "
            f"patients={len(patient_ids)}, phi_columns={phi_columns}{partition_msg}, "
            f"timestamp={datetime.now(timezone.utc).isoformat()}"
        )

    try:
        import polars as pl
        if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return _forget_polars(df, phi_columns, patient_column, patient_ids, erasure_strategy, hash_salt, partition_filter)
    except ImportError:
        pass

    try:
        import pandas as pd
        if isinstance(df, pd.DataFrame):
            return _forget_pandas(df, phi_columns, patient_column, patient_ids, erasure_strategy, hash_salt, partition_filter)
    except ImportError:
        pass

    if hasattr(df, "fetchdf"):
        pdf = df.fetchdf()
        result = _forget_pandas(pdf, phi_columns, patient_column, patient_ids, erasure_strategy, hash_salt, partition_filter)
        import duckdb
        return duckdb.from_df(result)

    raise TypeError(f"Unsupported dataframe type: {type(df)}")


def mask_phi_columns(
    df: Any,
    contract: DataContract,
    *,
    strategy: str = "nullify",
    hash_salt: str = "",
    columns: Optional[List[str]] = None,
) -> Any:
    """Mask all PHI columns across all rows (e.g. for Safe Harbor)."""
    if strategy not in ("nullify", "hash", "redact"):
        raise ValueError(f"Invalid strategy: {strategy}.")

    phi_columns = columns or _get_phi_column_names(contract)
    if not phi_columns:
        logger.warning("No PHI columns to mask.")
        return df

    logger.info(f"Masking PHI columns: {phi_columns} with strategy '{strategy}'")

    try:
        import polars as pl
        if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return _mask_polars(df, phi_columns, strategy, hash_salt)
    except ImportError:
        pass

    try:
        import pandas as pd
        if isinstance(df, pd.DataFrame):
            return _mask_pandas(df, phi_columns, strategy, hash_salt)
    except ImportError:
        pass

    if hasattr(df, "fetchdf"):
        pdf = df.fetchdf()
        result = _mask_pandas(pdf, phi_columns, strategy, hash_salt)
        import duckdb
        return duckdb.from_df(result)

    raise TypeError(f"Unsupported dataframe type: {type(df)}")


def generate_hipaa_erasure_report(
    contract: DataContract,
    patient_column: str,
    patient_ids: List[str],
    erasure_strategy: str = "nullify",
    affected_rows: Optional[int] = None,
    partition_filter: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Generate a HIPAA-compliant audit report for an erasure operation."""
    phi_columns = _get_phi_column_names(contract)
    contract_name = contract.info.title if contract.info else "unknown"

    report: Dict[str, Any] = {
        "report_type": "hipaa_erasure",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "contract": contract_name,
        "patient_column": patient_column,
        "patients_erased": len(patient_ids),
        "erasure_strategy": erasure_strategy,
        "phi_columns_affected": phi_columns,
        "affected_rows": affected_rows,
        "compliance_note": (
            "Protected Health Information (PHI) has been erased or de-identified "
            "in accordance with the HIPAA Privacy Rule / Safe Harbor guidelines. "
            "This cryptographic report should be retained for compliance audits."
        ),
    }

    if partition_filter:
        report["partition_filter"] = partition_filter

    return report
