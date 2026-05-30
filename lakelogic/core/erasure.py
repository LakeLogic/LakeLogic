"""
Regulation-agnostic erasure engine for LakeLogic.

All right-to-be-forgotten regimes (GDPR Art. 17, HIPAA Safe Harbor,
CCPA right-to-delete, LGPD, PDPA, ...) share the same mechanics:

  1. Select a set of "sensitive" columns from the contract.
  2. For rows matching a subject identifier (+ optional partition),
     transform each sensitive column with one of three strategies:
     nullify | hash | redact.
  3. Stamp four compliance-metadata columns on affected rows so the
     row itself carries proof it was processed.
  4. Emit a structured run-log audit record.

The regulation-specific bits — which field annotation marks a column
as sensitive, what redaction marker to use, what reason code to write
to ``_lakelogic_delete_reason``, what the audit report calls itself —
are isolated behind an :class:`ErasureProfile`. ``core.gdpr`` and
``core.hipaa`` are now thin facades that bind their public API verbs
(``forget_subjects`` / ``forget_patients``) to the relevant profile.

Adding CCPA / LGPD / PDPA means adding a profile, not duplicating the
backend dispatchers.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from loguru import logger

from lakelogic.core.constants import (
    DELETE_REASON_CCPA_DELETE,
    DELETE_REASON_GDPR_ART17,
    DELETE_REASON_HIPAA_PHI,
    ERASURE_NULLIFY,
    META_DELETE_REASON,
    META_DELETED_AT,
    META_IS_DELETED,
    META_UPDATED_AT,
    VALID_ERASURE_STRATEGIES,
)
from lakelogic.core.models import DataContract, FieldDefinition

# ── Profile ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ErasureProfile:
    """The regulation-specific knobs the engine needs to do its job."""

    name: str
    field_selector: Callable[[DataContract], List[FieldDefinition]]
    redaction_marker: str
    default_reason: str
    report_type: str
    compliance_note: str
    subject_noun: str  # "subject" (GDPR), "patient" (HIPAA), "consumer" (CCPA)
    sensitive_noun: str  # "PII", "PHI", "personal information"


def _pii_fields(contract: DataContract) -> List[FieldDefinition]:
    if not contract.model or not contract.model.fields:
        return []
    return [f for f in contract.model.fields if f.pii]


def _phi_fields(contract: DataContract) -> List[FieldDefinition]:
    if not contract.model or not contract.model.fields:
        return []
    return [f for f in contract.model.fields if f.phi or f.pii]


GDPR_PROFILE = ErasureProfile(
    name="gdpr",
    field_selector=_pii_fields,
    redaction_marker="***REDACTED***",
    default_reason=DELETE_REASON_GDPR_ART17,
    report_type="gdpr_erasure",
    compliance_note=(
        "PII data has been erased in accordance with GDPR Article 17 "
        "(Right to Erasure). This report should be retained for audit purposes."
    ),
    subject_noun="subject",
    sensitive_noun="PII",
)

HIPAA_PROFILE = ErasureProfile(
    name="hipaa",
    field_selector=_phi_fields,
    redaction_marker="***REDACTED_PHI***",
    default_reason=DELETE_REASON_HIPAA_PHI,
    report_type="hipaa_erasure",
    compliance_note=(
        "Protected Health Information (PHI) has been erased or de-identified "
        "in accordance with the HIPAA Privacy Rule / Safe Harbor guidelines. "
        "This cryptographic report should be retained for compliance audits."
    ),
    subject_noun="patient",
    sensitive_noun="PHI",
)

CCPA_PROFILE = ErasureProfile(
    name="ccpa",
    field_selector=_pii_fields,
    redaction_marker="***REDACTED***",
    default_reason=DELETE_REASON_CCPA_DELETE,
    report_type="ccpa_erasure",
    compliance_note=(
        "Personal information has been deleted in accordance with the "
        "California Consumer Privacy Act (CCPA) right to delete."
    ),
    subject_noun="consumer",
    sensitive_noun="personal information",
)


def column_names(profile: ErasureProfile, contract: DataContract) -> List[str]:
    return [f.name for f in profile.field_selector(contract)]


# ── Primitive value transforms ──────────────────────────────────────────────


def _hash_value(value: Any, salt: str = "") -> Optional[str]:
    if value is None or (isinstance(value, float) and value != value):
        return None
    return hashlib.sha256(f"{salt}{value}".encode("utf-8")).hexdigest()


# ── Backend dispatchers ─────────────────────────────────────────────────────


def _forget_polars(
    df,
    profile: ErasureProfile,
    sensitive_columns: List[str],
    subject_column: str,
    subject_ids: List[str],
    erasure_strategy: str,
    hash_salt: str,
    partition_filter: Optional[Dict[str, str]],
    delete_reason: str,
    strategy_per_field: Optional[Dict[str, str]],
):
    import polars as pl

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    present = [c for c in sensitive_columns if c in df.columns]
    if not present:
        logger.info(f"No {profile.sensitive_noun} columns found in dataframe; nothing to erase.")
        return df

    if subject_column not in df.columns:
        raise ValueError(f"{profile.subject_noun.capitalize()} column '{subject_column}' not found in dataframe.")

    subject_ids_str = [str(s) for s in subject_ids]
    mask = df[subject_column].cast(pl.Utf8).is_in(subject_ids_str)

    if partition_filter:
        pcol, pval = partition_filter["column"], partition_filter["value"]
        if pcol in df.columns:
            mask = mask & (df[pcol].cast(pl.Utf8) == str(pval))
        else:
            logger.warning(f"Partition column '{pcol}' not found; ignoring partition filter.")

    strategy_map = strategy_per_field or {}
    for col in present:
        col_strategy = strategy_map.get(col, erasure_strategy)
        if col == subject_column and col_strategy == "nullify":
            continue
        if col_strategy == "nullify":
            df = df.with_columns(pl.when(mask).then(pl.lit(None)).otherwise(pl.col(col)).alias(col))
        elif col_strategy == "hash":
            df = df.with_columns(
                pl.when(mask)
                .then(
                    pl.col(col)
                    .cast(pl.Utf8)
                    .map_elements(lambda v, _s=hash_salt: _hash_value(v, _s), return_dtype=pl.Utf8)
                )
                .otherwise(pl.col(col))
                .alias(col)
            )
        elif col_strategy == "redact":
            df = df.with_columns(pl.when(mask).then(pl.lit(profile.redaction_marker)).otherwise(pl.col(col)).alias(col))

    affected = int(mask.sum())
    df = _stamp_metadata_polars(df, mask, delete_reason)
    logger.info(
        f"{profile.name.upper()} erasure ({erasure_strategy}): "
        f"affected {affected} rows, {len(present)} {profile.sensitive_noun} columns."
    )
    return df


def _stamp_metadata_polars(df, mask, delete_reason: str):
    import polars as pl

    now = datetime.now(timezone.utc).isoformat()
    if META_IS_DELETED not in df.columns:
        df = df.with_columns(pl.lit(False).alias(META_IS_DELETED))
    if META_DELETED_AT not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(META_DELETED_AT))
    if META_DELETE_REASON not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(META_DELETE_REASON))
    if META_UPDATED_AT not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(META_UPDATED_AT))

    return df.with_columns(
        pl.when(mask).then(pl.lit(True)).otherwise(pl.col(META_IS_DELETED)).alias(META_IS_DELETED),
        pl.when(mask).then(pl.lit(now)).otherwise(pl.col(META_DELETED_AT)).alias(META_DELETED_AT),
        pl.when(mask).then(pl.lit(delete_reason)).otherwise(pl.col(META_DELETE_REASON)).alias(META_DELETE_REASON),
        pl.when(mask).then(pl.lit(now)).otherwise(pl.col(META_UPDATED_AT)).alias(META_UPDATED_AT),
    )


def _forget_pandas(
    df,
    profile: ErasureProfile,
    sensitive_columns: List[str],
    subject_column: str,
    subject_ids: List[str],
    erasure_strategy: str,
    hash_salt: str,
    partition_filter: Optional[Dict[str, str]],
    delete_reason: str,
    strategy_per_field: Optional[Dict[str, str]],
):
    present = [c for c in sensitive_columns if c in df.columns]
    if not present:
        logger.info(f"No {profile.sensitive_noun} columns found in dataframe; nothing to erase.")
        return df

    if subject_column not in df.columns:
        raise ValueError(f"{profile.subject_noun.capitalize()} column '{subject_column}' not found in dataframe.")

    df = df.copy()
    subject_ids_str = {str(s) for s in subject_ids}
    mask = df[subject_column].astype(str).isin(subject_ids_str)

    if partition_filter:
        pcol, pval = partition_filter["column"], partition_filter["value"]
        if pcol in df.columns:
            mask = mask & (df[pcol].astype(str) == str(pval))
        else:
            logger.warning(f"Partition column '{pcol}' not found; ignoring partition filter.")

    affected = int(mask.sum())
    strategy_map = strategy_per_field or {}
    for col in present:
        col_strategy = strategy_map.get(col, erasure_strategy)
        if col == subject_column and col_strategy == "nullify":
            continue
        if col_strategy == "nullify":
            df.loc[mask, col] = None
        elif col_strategy == "hash":
            df.loc[mask, col] = df.loc[mask, col].apply(lambda v, _s=hash_salt: _hash_value(v, _s))
        elif col_strategy == "redact":
            df.loc[mask, col] = profile.redaction_marker

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

    logger.info(
        f"{profile.name.upper()} erasure ({erasure_strategy}): "
        f"affected {affected} rows, {len(present)} {profile.sensitive_noun} columns."
    )
    return df


def _forget_pyspark(  # pragma: no cover
    df,
    profile: ErasureProfile,
    sensitive_columns: List[str],
    subject_column: str,
    subject_ids: List[str],
    erasure_strategy: str,
    hash_salt: str,
    partition_filter: Optional[Dict[str, str]],
    delete_reason: str,
    strategy_per_field: Optional[Dict[str, str]],
):
    from pyspark.sql import functions as F

    present = [c for c in sensitive_columns if c in df.columns]
    if not present:
        logger.info(f"No {profile.sensitive_noun} columns found in dataframe; nothing to erase.")
        return df

    if subject_column not in df.columns:
        raise ValueError(f"{profile.subject_noun.capitalize()} column '{subject_column}' not found in dataframe.")

    mask = F.col(subject_column).cast("string").isin([str(s) for s in subject_ids])
    if partition_filter:
        pcol, pval = partition_filter["column"], partition_filter["value"]
        if pcol in df.columns:
            mask = mask & (F.col(pcol).cast("string") == str(pval))
        else:
            logger.warning(f"Partition column '{pcol}' not found; ignoring partition filter.")

    strategy_map = strategy_per_field or {}
    for col in present:
        col_strategy = strategy_map.get(col, erasure_strategy)
        if col == subject_column and col_strategy == "nullify":
            continue
        if col_strategy == "nullify":
            df = df.withColumn(col, F.when(mask, F.lit(None)).otherwise(F.col(col)))
        elif col_strategy == "hash":
            df = df.withColumn(
                col,
                F.when(mask, F.sha2(F.concat(F.lit(hash_salt), F.col(col).cast("string")), 256)).otherwise(F.col(col)),
            )
        elif col_strategy == "redact":
            df = df.withColumn(col, F.when(mask, F.lit(profile.redaction_marker)).otherwise(F.col(col)))

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

    logger.info(
        f"{profile.name.upper()} erasure ({erasure_strategy}): "
        f"processed PySpark DataFrame, {len(present)} {profile.sensitive_noun} columns."
    )
    return df


def _mask_polars(df, profile: ErasureProfile, columns: List[str], strategy: str, hash_salt: str):
    import polars as pl

    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    present = [c for c in columns if c in df.columns]
    if not present:
        return df
    for col in present:
        if strategy == "nullify":
            df = df.with_columns(pl.lit(None).alias(col))
        elif strategy == "hash":
            df = df.with_columns(
                pl.col(col)
                .cast(pl.Utf8)
                .map_elements(lambda v, _s=hash_salt: _hash_value(v, _s), return_dtype=pl.Utf8)
                .alias(col)
            )
        elif strategy == "redact":
            df = df.with_columns(pl.lit(profile.redaction_marker).alias(col))
    logger.info(f"{profile.sensitive_noun} masking ({strategy}): masked {len(present)} columns across all rows.")
    return df


def _mask_pandas(df, profile: ErasureProfile, columns: List[str], strategy: str, hash_salt: str):
    df = df.copy()
    present = [c for c in columns if c in df.columns]
    if not present:
        return df
    for col in present:
        if strategy == "nullify":
            df[col] = None
        elif strategy == "hash":
            df[col] = df[col].apply(lambda v, _s=hash_salt: _hash_value(v, _s))
        elif strategy == "redact":
            df[col] = profile.redaction_marker
    logger.info(f"{profile.sensitive_noun} masking ({strategy}): masked {len(present)} columns across {len(df)} rows.")
    return df


def _mask_pyspark(df, profile: ErasureProfile, columns: List[str], strategy: str, hash_salt: str):  # pragma: no cover
    from pyspark.sql import functions as F

    present = [c for c in columns if c in df.columns]
    if not present:
        return df
    for col in present:
        if strategy == "nullify":
            df = df.withColumn(col, F.lit(None))
        elif strategy == "hash":
            df = df.withColumn(col, F.sha2(F.concat(F.lit(hash_salt), F.col(col).cast("string")), 256))
        elif strategy == "redact":
            df = df.withColumn(col, F.lit(profile.redaction_marker))
    return df


# ── Public engine API ───────────────────────────────────────────────────────


def erase(
    df: Any,
    contract: DataContract,
    *,
    profile: ErasureProfile,
    subject_column: str,
    subject_ids: List[str],
    erasure_strategy: str = ERASURE_NULLIFY,
    hash_salt: str = "",
    audit: bool = True,
    partition_filter: Optional[Dict[str, str]] = None,
    delete_reason: Optional[str] = None,
    compliance_event: Optional[Dict[str, Any]] = None,
    audit_report_out: Optional[List[Dict[str, Any]]] = None,
    dispatchers: Optional[Dict[str, Callable]] = None,
) -> Any:
    """Erase sensitive columns for matching subjects under the given profile.

    See :func:`lakelogic.core.gdpr.forget_subjects` for the full kwarg
    semantics. This engine entrypoint adds ``profile`` to choose which
    field annotation defines "sensitive", which redaction marker is
    used, and which reason code is recorded.
    """
    delete_reason = delete_reason or profile.default_reason

    if not compliance_event and getattr(contract, "compliance", None):
        compliance_event = contract.compliance

    strategy_per_field = None
    if compliance_event:
        erasure_strategy = compliance_event.get("strategy", erasure_strategy)
        strategy_per_field = compliance_event.get("strategy_per_field")

    if erasure_strategy not in VALID_ERASURE_STRATEGIES:
        raise ValueError(f"Invalid erasure_strategy: {erasure_strategy}. Must be one of {VALID_ERASURE_STRATEGIES}.")

    sensitive_columns = column_names(profile, contract)
    if not sensitive_columns:
        logger.warning(
            f"No {profile.sensitive_noun} fields defined in contract. "
            f"Mark fields with 'pii: true'"
            + (" or 'phi: true'" if profile is HIPAA_PROFILE else "")
            + f" in the model section to enable {profile.name.upper()} erasure."
        )
        return df

    # ── Build audit report (written AFTER erasure succeeds) ────────────
    _audit_report = None
    _audit_engine = None
    if audit:
        partition_msg = ""
        if partition_filter:
            partition_msg = f", partition={partition_filter['column']}='{partition_filter['value']}'"
        timestamp_iso = datetime.now(timezone.utc).isoformat()
        logger.info(
            f"{profile.name.upper()} erasure request: strategy={erasure_strategy}, "
            f"{profile.subject_noun}s={len(subject_ids)}, "
            f"{profile.sensitive_noun.lower()}_columns={sensitive_columns}{partition_msg}, "
            f"timestamp={timestamp_iso}"
        )

        try:
            import uuid

            df_type = str(type(df)).lower()
            if "polars" in df_type:
                _audit_engine = "polars"
            elif "pyspark" in df_type:
                _audit_engine = "spark"
            elif "duckdb" in df_type or hasattr(df, "fetchdf"):
                _audit_engine = "duckdb"
            else:
                _audit_engine = "pandas"

            metadata = getattr(contract, "metadata", {}) or {}
            erasure_report = generate_report(
                profile,
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
                "pipeline_run_id": run_id,
                "timestamp": timestamp_iso,
                "start_time": timestamp_iso,
                "end_time": timestamp_iso,
                "run_duration_seconds": 0.0,
                "engine": _audit_engine,
                "contract": contract.info.title if getattr(contract, "info", None) else None,
                "dataset": getattr(contract, "dataset", None),
                "domain": metadata.get("domain", ""),
                "system": metadata.get("system", ""),
                "stage": f"{profile.name}_erasure",
                "compliance_event": compliance_event,
                "status": "ok",
                "counts": {
                    "total": len(subject_ids),
                    "good": len(subject_ids),
                    "quarantined": len(subject_ids),  # repurposed: erased count for dashboards
                    "quarantine_ratio": 1.0,
                },
                "erasure_report": erasure_report,
            }
            if audit_report_out is not None:
                audit_report_out.append(_audit_report)
        except Exception as e:
            logger.warning(f"Failed to build {profile.name.upper()} erasure audit report: {e}")

    # ── Detect frame type and dispatch ────────────────────────────────
    # ``dispatchers`` lets the facade modules (gdpr/hipaa) inject their own
    # forget_* shims so that test code monkeypatching ``gdpr._forget_polars``
    # is honored. Without it, calls would go straight to the engine helpers
    # and the patch would be silently bypassed.
    disp = dispatchers or {}
    dp_polars = disp.get("polars", _forget_polars)
    dp_pandas = disp.get("pandas", _forget_pandas)
    dp_pyspark = disp.get("pyspark", _forget_pyspark)

    def _call(fn, frame):
        # Engine helpers take (df, profile, ...); facade shims have the
        # profile baked in and take (df, cols, subj_col, ids, strategy, salt, ...).
        # We detect by checking the function's qualname / module — engine
        # helpers live in this module.
        if getattr(fn, "__module__", None) == __name__:
            return fn(
                frame,
                profile,
                sensitive_columns,
                subject_column,
                subject_ids,
                erasure_strategy,
                hash_salt,
                partition_filter,
                delete_reason,
                strategy_per_field,
            )
        return fn(
            frame,
            sensitive_columns,
            subject_column,
            subject_ids,
            erasure_strategy,
            hash_salt,
            partition_filter=partition_filter,
            delete_reason=delete_reason,
            strategy_per_field=strategy_per_field,
        )

    result = None
    try:
        try:
            import polars as pl

            if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
                result = _call(dp_polars, df)
        except ImportError:
            pass

        if result is None:
            try:
                import pandas as pd

                if isinstance(df, pd.DataFrame):
                    result = _call(dp_pandas, df)
            except ImportError:
                pass

        if result is None:
            try:
                from pyspark.sql import DataFrame as SparkDataFrame

                if isinstance(df, SparkDataFrame):
                    result = _call(dp_pyspark, df)
            except ImportError:
                pass

        if result is None and hasattr(df, "fetchdf"):
            import pandas as pd

            pdf = df.fetchdf()
            pdf_result = _call(dp_pandas, pdf)
            import duckdb

            result = duckdb.from_df(pdf_result)

        if result is None:
            raise TypeError(f"Unsupported dataframe type: {type(df)}")

    except Exception as exc:
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

    if _audit_report is not None:
        try:
            from lakelogic.core.run_log import write_run_log

            _audit_report["end_time"] = datetime.now(timezone.utc).isoformat()
            write_run_log(_audit_report, contract, engine_name=_audit_engine)
        except Exception as e:
            logger.warning(f"Failed to record {profile.name.upper()} erasure to run_log: {e}")

    return result


def mask(
    df: Any,
    contract: DataContract,
    *,
    profile: ErasureProfile,
    strategy: str = "nullify",
    hash_salt: str = "",
    columns: Optional[List[str]] = None,
) -> Any:
    """Mask sensitive columns across ALL rows (e.g. anonymised dev datasets, Safe Harbor)."""
    if strategy not in ("nullify", "hash", "redact"):
        raise ValueError(f"Invalid strategy: {strategy}. Must be 'nullify', 'hash', or 'redact'.")

    sensitive_columns = columns or column_names(profile, contract)
    if not sensitive_columns:
        logger.warning(f"No {profile.sensitive_noun} columns to mask.")
        return df

    logger.info(f"Masking {profile.sensitive_noun} columns: {sensitive_columns} with strategy '{strategy}'")

    try:
        import polars as pl

        if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            return _mask_polars(df, profile, sensitive_columns, strategy, hash_salt)
    except ImportError:
        pass

    try:
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            return _mask_pandas(df, profile, sensitive_columns, strategy, hash_salt)
    except ImportError:
        pass

    try:
        from pyspark.sql import DataFrame as SparkDataFrame

        if isinstance(df, SparkDataFrame):
            return _mask_pyspark(df, profile, sensitive_columns, strategy, hash_salt)
    except ImportError:
        pass

    if hasattr(df, "fetchdf"):
        import pandas as pd

        pdf = df.fetchdf()
        result = _mask_pandas(pdf, profile, sensitive_columns, strategy, hash_salt)
        import duckdb

        return duckdb.from_df(result)

    raise TypeError(f"Unsupported dataframe type: {type(df)}")


def generate_report(
    profile: ErasureProfile,
    contract: DataContract,
    subject_column: str,
    subject_ids: List[str],
    erasure_strategy: str = "nullify",
    affected_rows: Optional[int] = None,
    partition_filter: Optional[Dict[str, str]] = None,
    compliance_event: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Profile-shaped audit report.

    ``gdpr.generate_erasure_report`` / ``hipaa.generate_hipaa_erasure_report`` wrap this.
    """
    cols = column_names(profile, contract)
    contract_name = contract.info.title if contract.info else "unknown"

    report: Dict[str, Any] = {
        "report_type": profile.report_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "contract": contract_name,
        f"{profile.subject_noun}_column": subject_column,
        f"{profile.subject_noun}s_erased": len(subject_ids),
        "erasure_strategy": erasure_strategy,
        f"{profile.sensitive_noun.lower().replace(' ', '_')}_columns_affected": cols,
        "affected_rows": affected_rows,
        "compliance_note": profile.compliance_note,
    }
    if compliance_event:
        report["compliance_event"] = compliance_event
    if partition_filter:
        report["partition_filter"] = partition_filter
    return report
