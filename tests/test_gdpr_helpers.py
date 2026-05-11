from __future__ import annotations

import sys
import types

import pytest

pd = pytest.importorskip("pandas")

from lakelogic.core import gdpr
from lakelogic.core.models import DataContract, FieldDefinition, Info, Model


def _contract_with_pii():
    return DataContract(
        version="1.0",
        info=Info(title="Customers", version="1.0"),
        dataset="customers",
        metadata={"domain": "commerce", "system": "crm"},
        model=Model(
            fields=[
                FieldDefinition(name="customer_id", type="string", required=True),
                FieldDefinition(name="email", type="string", pii=True),
                FieldDefinition(name="phone", type="string", pii=True),
                FieldDefinition(name="country_code", type="string"),
            ]
        ),
    )


def test_gdpr_hash_redact_and_pii_helpers():
    contract = _contract_with_pii()
    assert gdpr._get_pii_column_names(contract) == ["email", "phone"]
    assert gdpr._hash_value("abc", "salt") == gdpr._hash_value("abc", "salt")
    assert gdpr._hash_value(None) is None
    assert gdpr._redact_value("abc") == "***REDACTED***"
    assert gdpr._redact_value(float("nan")) is None


def test_gdpr_forget_pandas_partition_filter_and_metadata(monkeypatch):
    contract = _contract_with_pii()
    df = pd.DataFrame(
        {
            "customer_id": ["c1", "c2"],
            "email": ["a@example.com", "b@example.com"],
            "phone": ["111", "222"],
            "country_code": ["FR", "GB"],
        }
    )
    warnings = []
    monkeypatch.setattr(gdpr.logger, "warning", warnings.append)

    result = gdpr._forget_pandas(
        df,
        ["email", "phone"],
        "customer_id",
        ["c1", "c2"],
        "nullify",
        "",
        partition_filter={"column": "country_code", "value": "FR"},
    )
    assert pd.isna(result.loc[0, "email"])
    assert result.loc[1, "email"] == "b@example.com"
    assert result.loc[0, gdpr.META_IS_DELETED] == True
    assert result.loc[1, gdpr.META_IS_DELETED] == False

    unchanged = gdpr._forget_pandas(
        df,
        ["email"],
        "customer_id",
        ["c1"],
        "redact",
        "",
        partition_filter={"column": "missing_partition", "value": "FR"},
    )
    assert unchanged.loc[0, "email"] == "***REDACTED***"
    assert any("ignoring partition filter" in message for message in warnings)


def test_gdpr_forget_and_mask_duckdb_dispatch(monkeypatch):
    contract = _contract_with_pii()
    pdf = pd.DataFrame({"customer_id": ["c1"], "email": ["a@example.com"], "phone": ["111"]})

    class FakeRelation:
        def fetchdf(self):
            return pdf

    monkeypatch.setitem(
        sys.modules,
        "duckdb",
        types.SimpleNamespace(from_df=lambda frame: {"rows": len(frame), "columns": list(frame.columns)}),
    )
    forgot = gdpr.forget_subjects(FakeRelation(), contract, "customer_id", ["c1"], audit=False)
    masked = gdpr.mask_pii_columns(FakeRelation(), contract, strategy="redact")

    assert forgot["rows"] == 1
    assert gdpr.META_IS_DELETED in forgot["columns"]
    assert masked["rows"] == 1
    assert "email" in masked["columns"]


def test_gdpr_audit_logging_success_and_failure(monkeypatch):
    contract = _contract_with_pii()
    df = pd.DataFrame({"customer_id": ["c1"], "email": ["a@example.com"], "phone": ["111"]})

    reports = []
    fake_run_log = types.ModuleType("lakelogic.core.run_log")
    fake_run_log.write_run_log = lambda payload, contract, engine_name=None: reports.append((payload, engine_name))
    monkeypatch.setitem(sys.modules, "lakelogic.core.run_log", fake_run_log)

    result = gdpr.forget_subjects(df, contract, "customer_id", ["c1"], audit=True)
    assert result.loc[0, gdpr.META_IS_DELETED] == True
    assert reports[-1][0]["status"] == "ok"
    assert reports[-1][1] == "pandas"

    monkeypatch.setattr(gdpr, "_forget_pandas", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    try:
        gdpr.forget_subjects(df, contract, "customer_id", ["c1"], audit=True)
    except RuntimeError as exc:
        assert str(exc) == "boom"
    else:
        raise AssertionError("Expected forget_subjects to re-raise processing failure")

    assert reports[-1][0]["status"] == "error"
    assert reports[-1][0]["error_message"] == "boom"


def test_gdpr_mask_paths_and_validation_guards(monkeypatch):
    contract = _contract_with_pii()
    df = pd.DataFrame({"customer_id": ["c1"], "email": ["a@example.com"], "phone": ["111"]})

    hashed = gdpr.mask_pii_columns(df, contract, strategy="hash", hash_salt="salt")
    assert hashed.loc[0, "email"] == gdpr._hash_value("a@example.com", "salt")

    selected = gdpr.mask_pii_columns(df, contract, strategy="redact", columns=["phone"])
    assert selected.loc[0, "phone"] == "***REDACTED***"
    assert selected.loc[0, "email"] == "a@example.com"

    no_pii_contract = DataContract(version="1.0", info=Info(title="Plain", version="1.0"), dataset="plain")
    warnings = []
    monkeypatch.setattr(gdpr.logger, "warning", warnings.append)
    unchanged = gdpr.mask_pii_columns(df, no_pii_contract)
    assert unchanged.equals(df)
    assert any("No PII columns to mask" in message for message in warnings)

    with pytest.raises(ValueError, match="Invalid strategy"):
        gdpr.mask_pii_columns(df, contract, strategy="encrypt")

    with pytest.raises(ValueError, match="Invalid erasure_strategy"):
        gdpr.forget_subjects(df, contract, "customer_id", ["c1"], erasure_strategy="encrypt", audit=False)


def test_gdpr_generate_erasure_report_and_polars_mask(monkeypatch):
    pl = pytest.importorskip("polars")
    contract = _contract_with_pii()
    polars_df = pl.DataFrame({"customer_id": ["c1"], "email": ["a@example.com"], "phone": ["111"]})

    masked = gdpr.mask_pii_columns(polars_df, contract, strategy="redact")
    assert masked["email"].to_list() == ["***REDACTED***"]
    assert masked["phone"].to_list() == ["***REDACTED***"]

    report = gdpr.generate_erasure_report(
        contract,
        "customer_id",
        ["c1", "c2"],
        erasure_strategy="hash",
        affected_rows=3,
        partition_filter={"column": "country_code", "value": "FR"},
    )
    assert report["report_type"] == "gdpr_erasure"
    assert report["subjects_erased"] == 2
    assert report["affected_rows"] == 3
    assert report["partition_filter"] == {"column": "country_code", "value": "FR"}
