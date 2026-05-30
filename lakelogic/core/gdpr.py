"""
GDPR compliance utilities for LakeLogic.

Thin facade over :mod:`lakelogic.core.erasure` — the regulation-agnostic
engine that handles every right-to-be-forgotten regime. This module binds
the GDPR profile (Article 17 reason code, ``pii: true`` field selector,
``***REDACTED***`` marker) to the public verbs LakeLogic users expect.

Usage (Python API):
    from lakelogic.core.gdpr import forget_subjects, mask_pii_columns

    cleaned = forget_subjects(df, contract, subject_column="customer_id",
                              subject_ids=["cust_123", "cust_456"])
    masked  = mask_pii_columns(df, contract, strategy="hash")

Usage via DataProcessor:
    proc = DataProcessor("contract.yaml")
    cleaned = proc.forget(subject_column="customer_id", subject_ids=["cust_123"])
    masked  = proc.mask_pii(strategy="hash")
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from typing import Any as _Any

from loguru import logger  # re-exported for tests that monkeypatch gdpr.logger

from lakelogic.core.constants import (
    DELETE_REASON_GDPR_ART17,
    ERASURE_NULLIFY,
    META_DELETE_REASON,
    META_DELETED_AT,
    META_IS_DELETED,
)
from lakelogic.core.erasure import (
    GDPR_PROFILE,
    _hash_value,
    erase,
    generate_report,
    mask,
)
from lakelogic.core.erasure import (
    _forget_pandas as _engine_forget_pandas,
)
from lakelogic.core.erasure import (
    _forget_polars as _engine_forget_polars,
)
from lakelogic.core.erasure import (
    _forget_pyspark as _engine_forget_pyspark,
)
from lakelogic.core.erasure import (
    _mask_pandas as _engine_mask_pandas,
)
from lakelogic.core.erasure import (
    _mask_polars as _engine_mask_polars,
)
from lakelogic.core.erasure import (
    _mask_pyspark as _engine_mask_pyspark,
)
from lakelogic.core.models import DataContract, FieldDefinition

__all__ = [
    "forget_subjects",
    "mask_pii_columns",
    "generate_erasure_report",
    "_get_pii_fields",
    "_get_pii_column_names",
    "_hash_value",
    "_redact_value",
    "logger",
    "DELETE_REASON_GDPR_ART17",
    "META_IS_DELETED",
    "META_DELETED_AT",
    "META_DELETE_REASON",
]


def _redact_value(value: _Any, replacement: str = "***REDACTED***"):
    """Replace a value with a redaction marker. Returns None for null values."""
    if value is None or (isinstance(value, float) and value != value):
        return None
    return replacement


# Back-compat shims for tests / external callers that import the legacy
# private dispatchers directly. They bind the GDPR profile to the engine
# entrypoint, preserving the historical positional-arg signature.


def _forget_polars(
    df,
    pii_columns,
    subject_column: str,
    subject_ids,
    erasure_strategy: str,
    hash_salt: str,
    partition_filter=None,
    delete_reason: str = DELETE_REASON_GDPR_ART17,
    strategy_per_field=None,
):
    return _engine_forget_polars(
        df,
        GDPR_PROFILE,
        list(pii_columns),
        subject_column,
        list(subject_ids),
        erasure_strategy,
        hash_salt,
        partition_filter,
        delete_reason,
        strategy_per_field,
    )


def _forget_pandas(
    df,
    pii_columns,
    subject_column: str,
    subject_ids,
    erasure_strategy: str,
    hash_salt: str,
    partition_filter=None,
    delete_reason: str = DELETE_REASON_GDPR_ART17,
    strategy_per_field=None,
):
    return _engine_forget_pandas(
        df,
        GDPR_PROFILE,
        list(pii_columns),
        subject_column,
        list(subject_ids),
        erasure_strategy,
        hash_salt,
        partition_filter,
        delete_reason,
        strategy_per_field,
    )


def _forget_pyspark(  # pragma: no cover
    df,
    pii_columns,
    subject_column,
    subject_ids,
    erasure_strategy,
    hash_salt,
    partition_filter=None,
    delete_reason: str = DELETE_REASON_GDPR_ART17,
):
    return _engine_forget_pyspark(
        df,
        GDPR_PROFILE,
        list(pii_columns),
        subject_column,
        list(subject_ids),
        erasure_strategy,
        hash_salt,
        partition_filter,
        delete_reason,
        None,
    )


def _mask_polars(df, pii_columns, strategy: str, hash_salt: str):
    return _engine_mask_polars(df, GDPR_PROFILE, list(pii_columns), strategy, hash_salt)


def _mask_pandas(df, pii_columns, strategy: str, hash_salt: str):
    return _engine_mask_pandas(df, GDPR_PROFILE, list(pii_columns), strategy, hash_salt)


def _mask_pyspark(df, pii_columns, strategy: str, hash_salt: str):  # pragma: no cover
    return _engine_mask_pyspark(df, GDPR_PROFILE, list(pii_columns), strategy, hash_salt)


def _get_pii_fields(contract: DataContract) -> List[FieldDefinition]:
    return GDPR_PROFILE.field_selector(contract)


def _get_pii_column_names(contract: DataContract) -> List[str]:
    return [f.name for f in _get_pii_fields(contract)]


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
    """GDPR Right-to-be-Forgotten (Article 17): erase PII for matching subjects.

    See :func:`lakelogic.core.erasure.erase` for full semantics.
    """
    return erase(
        df,
        contract,
        profile=GDPR_PROFILE,
        subject_column=subject_column,
        subject_ids=subject_ids,
        erasure_strategy=erasure_strategy,
        hash_salt=hash_salt,
        audit=audit,
        partition_filter=partition_filter,
        delete_reason=delete_reason,
        compliance_event=compliance_event,
        audit_report_out=audit_report_out,
        # Route dispatch through the local shims so tests that patch
        # ``gdpr._forget_polars`` etc. via monkeypatch see their fakes called.
        dispatchers={
            "polars": _forget_polars,
            "pandas": _forget_pandas,
            "pyspark": _forget_pyspark,
        },
    )


def mask_pii_columns(
    df: Any,
    contract: DataContract,
    *,
    strategy: str = "nullify",
    hash_salt: str = "",
    columns: Optional[List[str]] = None,
) -> Any:
    """Mask all PII columns across all rows (anonymised dev/test datasets)."""
    return mask(
        df,
        contract,
        profile=GDPR_PROFILE,
        strategy=strategy,
        hash_salt=hash_salt,
        columns=columns,
    )


def generate_erasure_report(
    contract: DataContract,
    subject_column: str,
    subject_ids: List[str],
    erasure_strategy: str = "nullify",
    affected_rows: Optional[int] = None,
    partition_filter: Optional[Dict[str, str]] = None,
    compliance_event: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """GDPR-shaped audit report. Output keys: ``pii_columns_affected``, ``subject_column``, ``subjects_erased``."""
    return generate_report(
        GDPR_PROFILE,
        contract,
        subject_column,
        subject_ids,
        erasure_strategy,
        affected_rows=affected_rows,
        partition_filter=partition_filter,
        compliance_event=compliance_event,
    )
