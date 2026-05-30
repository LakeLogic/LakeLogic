"""
HIPAA compliance utilities for LakeLogic.

Thin facade over :mod:`lakelogic.core.erasure`. Binds the HIPAA profile
(Safe Harbor reason code, ``phi: true | pii: true`` field selector,
``***REDACTED_PHI***`` marker) to the public verbs LakeLogic users expect.

By delegating to the engine, HIPAA erasure now inherits — for free —
pyspark / duckdb backend dispatchers, the four compliance-metadata
columns, ``compliance.strategy_per_field`` overrides, and run-log audit
integration. None of which the previous standalone implementation had.

Usage (Python API):
    from lakelogic.core.hipaa import forget_patients, mask_phi_columns

    cleaned = forget_patients(df, contract, patient_column="patient_id",
                              patient_ids=["P_123", "P_456"])
    masked  = mask_phi_columns(df, contract, strategy="nullify")
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from loguru import logger  # re-exported for tests that monkeypatch hipaa.logger

from lakelogic.core.constants import DELETE_REASON_HIPAA_PHI
from lakelogic.core.erasure import (
    HIPAA_PROFILE,
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
from lakelogic.core.models import DataContract, FieldDefinition

__all__ = [
    "forget_patients",
    "mask_phi_columns",
    "generate_hipaa_erasure_report",
    "_get_phi_fields",
    "_get_phi_column_names",
    "_hash_value",
    "logger",
    "DELETE_REASON_HIPAA_PHI",
]


def _forget_polars(
    df,
    phi_columns,
    patient_column,
    patient_ids,
    erasure_strategy,
    hash_salt,
    partition_filter=None,
    delete_reason=DELETE_REASON_HIPAA_PHI,
    strategy_per_field=None,
):
    return _engine_forget_polars(
        df,
        HIPAA_PROFILE,
        list(phi_columns),
        patient_column,
        list(patient_ids),
        erasure_strategy,
        hash_salt,
        partition_filter,
        delete_reason,
        strategy_per_field,
    )


def _forget_pandas(
    df,
    phi_columns,
    patient_column,
    patient_ids,
    erasure_strategy,
    hash_salt,
    partition_filter=None,
    delete_reason=DELETE_REASON_HIPAA_PHI,
    strategy_per_field=None,
):
    return _engine_forget_pandas(
        df,
        HIPAA_PROFILE,
        list(phi_columns),
        patient_column,
        list(patient_ids),
        erasure_strategy,
        hash_salt,
        partition_filter,
        delete_reason,
        strategy_per_field,
    )


def _forget_pyspark(  # pragma: no cover
    df,
    phi_columns,
    patient_column,
    patient_ids,
    erasure_strategy,
    hash_salt,
    partition_filter=None,
    delete_reason=DELETE_REASON_HIPAA_PHI,
):
    return _engine_forget_pyspark(
        df,
        HIPAA_PROFILE,
        list(phi_columns),
        patient_column,
        list(patient_ids),
        erasure_strategy,
        hash_salt,
        partition_filter,
        delete_reason,
        None,
    )


def _mask_polars(df, phi_columns, strategy, hash_salt):
    return _engine_mask_polars(df, HIPAA_PROFILE, list(phi_columns), strategy, hash_salt)


def _mask_pandas(df, phi_columns, strategy, hash_salt):
    return _engine_mask_pandas(df, HIPAA_PROFILE, list(phi_columns), strategy, hash_salt)


def _get_phi_fields(contract: DataContract) -> List[FieldDefinition]:
    return HIPAA_PROFILE.field_selector(contract)


def _get_phi_column_names(contract: DataContract) -> List[str]:
    return [f.name for f in _get_phi_fields(contract)]


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
    delete_reason: str = DELETE_REASON_HIPAA_PHI,
    compliance_event: Optional[Dict[str, Any]] = None,
    audit_report_out: Optional[List[Dict[str, Any]]] = None,
) -> Any:
    """HIPAA Right-to-be-Forgotten: erase PHI for matching patients."""
    return erase(
        df,
        contract,
        profile=HIPAA_PROFILE,
        subject_column=patient_column,
        subject_ids=patient_ids,
        erasure_strategy=erasure_strategy,
        hash_salt=hash_salt,
        audit=audit,
        partition_filter=partition_filter,
        delete_reason=delete_reason,
        compliance_event=compliance_event,
        audit_report_out=audit_report_out,
        dispatchers={
            "polars": _forget_polars,
            "pandas": _forget_pandas,
            "pyspark": _forget_pyspark,
        },
    )


def mask_phi_columns(
    df: Any,
    contract: DataContract,
    *,
    strategy: str = "nullify",
    hash_salt: str = "",
    columns: Optional[List[str]] = None,
) -> Any:
    """Mask all PHI columns across all rows (HIPAA Safe Harbor de-identification)."""
    return mask(
        df,
        contract,
        profile=HIPAA_PROFILE,
        strategy=strategy,
        hash_salt=hash_salt,
        columns=columns,
    )


def generate_hipaa_erasure_report(
    contract: DataContract,
    patient_column: str,
    patient_ids: List[str],
    erasure_strategy: str = "nullify",
    affected_rows: Optional[int] = None,
    partition_filter: Optional[Dict[str, str]] = None,
    compliance_event: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """HIPAA-shaped audit report. Output keys: ``phi_columns_affected``, ``patient_column``, ``patients_erased``."""
    return generate_report(
        HIPAA_PROFILE,
        contract,
        patient_column,
        patient_ids,
        erasure_strategy,
        affected_rows=affected_rows,
        partition_filter=partition_filter,
        compliance_event=compliance_event,
    )
