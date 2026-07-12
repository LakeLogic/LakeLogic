"""`sensitive: true` — confidential-but-not-personal fields (bank account, salary,
API key). Masked exactly like `pii`, but NOT pulled into GDPR/HIPAA erasure."""

from __future__ import annotations

import pytest

from lakelogic.core import erasure
from lakelogic.core.contract_lint import check_pii_no_masking
from lakelogic.core.ddl import generate_ddl
from lakelogic.core.masking_engine import MaskingEngine
from lakelogic.core.models import DataContract

pl = pytest.importorskip("polars")


def _contract() -> DataContract:
    return DataContract(
        version="1.0.0",
        dataset="accounts",
        model={
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "email", "type": "string", "pii": True, "masking": "hash"},
                {"name": "bank_account", "type": "string", "sensitive": True, "masking": "hash"},
            ]
        },
    )


def test_sensitive_field_is_selected_for_masking():
    eng = MaskingEngine(_contract(), hash_salt="s")
    names = {f.name for f in eng.get_fields_to_mask(user_groups=[])}
    assert names == {"email", "bank_account"}  # pii AND sensitive are both masked


def test_sensitive_value_is_masked_in_dataframe():
    eng = MaskingEngine(_contract(), hash_salt="s")
    df = pl.DataFrame(
        {"id": [1], "email": ["a@b.com"], "bank_account": ["GB29NWBK60161331926819"]}
    )
    out = eng.apply(df, user_groups=[])
    assert out["bank_account"][0] != "GB29NWBK60161331926819"  # hashed away
    assert out["email"][0] != "a@b.com"


def test_sensitive_is_excluded_from_gdpr_and_hipaa_erasure():
    c = _contract()
    pii = {f.name for f in erasure._pii_fields(c)}
    phi = {f.name for f in erasure._phi_fields(c)}
    assert "bank_account" not in pii  # sensitive is confidential, not "right to be forgotten"
    assert "bank_account" not in phi
    assert "email" in pii  # pii is still erased


def test_sensitive_column_tagged_in_ddl():
    ddl = generate_ddl(_contract(), "databricks")
    assert "SENSITIVE" in ddl  # bank_account tagged
    assert "PII" in ddl  # email still tagged


def test_lint_flags_sensitive_field_without_masking():
    raw = {"model": {"fields": [{"name": "bank_account", "type": "string", "sensitive": True}]}}
    findings = check_pii_no_masking(raw, "accounts", None)
    assert "SENS-001" in {f.check_id for f in findings}
