"""Tests for the polars erasure helper and the public forget_subjects /
mask_pii_columns / generate_erasure_report dispatch branches in gdpr.py.

Existing test_gdpr_helpers.py already covers pandas paths and DuckDB
dispatch; this file fills the polars hole (lines 94-169 of _forget_polars)
plus the contract/compliance_event / strategy_per_field branches in
forget_subjects, and a few mask_pii_columns / generate_erasure_report
branches that lacked coverage.

PySpark paths are pragma'd-no-cover and not exercised here.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pytest

pl = pytest.importorskip("polars")

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


# ──────────────────────────────────────────────────────────────────────────────
# _forget_polars — covers the previously-uncovered block 94-169
# ──────────────────────────────────────────────────────────────────────────────


class TestForgetPolars:
    def _df(self):
        return pl.DataFrame(
            {
                "customer_id": ["c1", "c2", "c3"],
                "email": ["a@example.com", "b@example.com", "c@example.com"],
                "phone": ["111", "222", "333"],
                "country_code": ["FR", "GB", "FR"],
            }
        )

    def test_lazyframe_input_is_collected(self):
        lazy = self._df().lazy()
        out = gdpr._forget_polars(lazy, ["email"], "customer_id", ["c1"], "nullify", "")
        assert isinstance(out, pl.DataFrame)
        # c1 is nullified; c2/c3 untouched
        emails = out.sort("customer_id")["email"].to_list()
        assert emails[0] is None
        assert emails[1] == "b@example.com"
        assert emails[2] == "c@example.com"

    def test_no_pii_columns_present_returns_unchanged(self):
        df = self._df()
        out = gdpr._forget_polars(
            df, ["ssn", "national_id"], "customer_id", ["c1"], "nullify", ""
        )
        # No matching PII columns → returned as-is; compliance metadata NOT injected.
        assert gdpr.META_IS_DELETED not in out.columns
        # All original rows preserved
        assert out.height == 3

    def test_missing_subject_column_raises(self):
        df = self._df()
        with pytest.raises(ValueError, match="Subject column 'missing' not found"):
            gdpr._forget_polars(df, ["email"], "missing", ["c1"], "nullify", "")

    def test_hash_strategy(self):
        df = self._df()
        out = gdpr._forget_polars(df, ["email"], "customer_id", ["c1"], "hash", "salt").sort(
            "customer_id"
        )
        expected = gdpr._hash_value("a@example.com", "salt")
        assert out["email"].to_list()[0] == expected
        # Non-matched rows unchanged
        assert out["email"].to_list()[1] == "b@example.com"

    def test_redact_strategy(self):
        df = self._df()
        out = gdpr._forget_polars(df, ["email"], "customer_id", ["c2"], "redact", "").sort(
            "customer_id"
        )
        assert out["email"].to_list()[1] == "***REDACTED***"

    def test_partition_filter_scopes_erasure(self):
        df = self._df()
        # c1 (FR) and c3 (FR) match the partition; c2 (GB) does not
        out = gdpr._forget_polars(
            df,
            ["email"],
            "customer_id",
            ["c1", "c2", "c3"],
            "nullify",
            "",
            partition_filter={"column": "country_code", "value": "FR"},
        ).sort("customer_id")
        emails = out["email"].to_list()
        assert emails[0] is None
        assert emails[1] == "b@example.com"  # c2 is GB → skipped
        assert emails[2] is None

    def test_partition_filter_missing_column_is_ignored(self, monkeypatch):
        warnings = []
        monkeypatch.setattr(gdpr.logger, "warning", warnings.append)
        df = self._df()
        out = gdpr._forget_polars(
            df,
            ["email"],
            "customer_id",
            ["c1"],
            "nullify",
            "",
            partition_filter={"column": "missing_partition", "value": "FR"},
        ).sort("customer_id")
        # Erasure still happens; partition filter is just dropped.
        assert out["email"].to_list()[0] is None
        assert any("ignoring partition filter" in m for m in warnings)

    def test_nullify_does_not_null_the_subject_column_itself(self):
        df = self._df()
        # If subject_column is in pii_columns AND strategy is nullify, that column
        # should be preserved (otherwise we lose the audit key).
        out = gdpr._forget_polars(
            df, ["customer_id", "email"], "customer_id", ["c1"], "nullify", ""
        ).sort("customer_id")
        # customer_id NOT nullified
        assert out["customer_id"].to_list()[0] == "c1"
        # email IS nullified
        assert out["email"].to_list()[0] is None

    def test_strategy_per_field_overrides_default(self):
        df = self._df()
        out = gdpr._forget_polars(
            df,
            ["email", "phone"],
            "customer_id",
            ["c1"],
            "nullify",
            "salt",
            strategy_per_field={"email": "hash", "phone": "redact"},
        ).sort("customer_id")
        assert out["email"].to_list()[0] == gdpr._hash_value("a@example.com", "salt")
        assert out["phone"].to_list()[0] == "***REDACTED***"

    def test_compliance_metadata_columns_are_set_on_affected_rows(self):
        df = self._df()
        out = gdpr._forget_polars(
            df, ["email"], "customer_id", ["c1"], "nullify", ""
        ).sort("customer_id")
        # Metadata columns added on the affected row only
        is_deleted = out[gdpr.META_IS_DELETED].to_list()
        assert is_deleted == [True, False, False]
        # Reason column is the GDPR Article 17 default
        reasons = out[gdpr.META_DELETE_REASON].to_list()
        assert reasons[0] == gdpr.DELETE_REASON_GDPR_ART17
        assert reasons[1] is None
        # Timestamps are ISO-formatted strings on affected rows, None elsewhere
        assert out[gdpr.META_DELETED_AT].to_list()[0] is not None
        assert out[gdpr.META_DELETED_AT].to_list()[1] is None


# ──────────────────────────────────────────────────────────────────────────────
# forget_subjects — public dispatcher branches not covered by existing tests
# ──────────────────────────────────────────────────────────────────────────────


class TestForgetSubjectsDispatch:
    def test_polars_dispatch_returns_polars(self, monkeypatch):
        # Stub run_log so audit logging doesn't error / write files
        fake_run_log = types.ModuleType("lakelogic.core.run_log")
        fake_run_log.write_run_log = lambda payload, contract, engine_name=None: None
        monkeypatch.setitem(sys.modules, "lakelogic.core.run_log", fake_run_log)

        contract = _contract_with_pii()
        df = pl.DataFrame(
            {
                "customer_id": ["c1", "c2"],
                "email": ["a@x.com", "b@x.com"],
                "phone": ["1", "2"],
                "country_code": ["FR", "GB"],
            }
        )
        result = gdpr.forget_subjects(df, contract, "customer_id", ["c1"], audit=True)
        assert isinstance(result, pl.DataFrame)
        assert result.sort("customer_id")["email"].to_list()[0] is None

    def test_unsupported_dataframe_type_raises_type_error(self):
        contract = _contract_with_pii()

        class WeirdFrame:
            pass

        with pytest.raises(TypeError, match="Unsupported dataframe type"):
            gdpr.forget_subjects(
                WeirdFrame(), contract, "customer_id", ["c1"], audit=False
            )

    def test_no_pii_in_contract_warns_and_returns_df_unchanged(self, monkeypatch):
        warnings = []
        monkeypatch.setattr(gdpr.logger, "warning", warnings.append)

        pd = pytest.importorskip("pandas")
        no_pii = DataContract(
            version="1.0", info=Info(title="Plain", version="1.0"), dataset="plain"
        )
        df = pd.DataFrame({"customer_id": ["c1"]})
        out = gdpr.forget_subjects(df, no_pii, "customer_id", ["c1"], audit=False)
        assert out is df  # untouched
        assert any("No PII fields defined in contract" in m for m in warnings)

    def test_compliance_event_overrides_erasure_strategy(self, monkeypatch):
        # When a compliance_event is passed, its strategy wins over the kwarg default
        captured = {}

        def fake_forget_polars(
            df, pii_columns, subject_column, subject_ids, erasure_strategy,
            hash_salt, partition_filter=None, delete_reason=gdpr.DELETE_REASON_GDPR_ART17,
            strategy_per_field=None,
        ):
            captured["strategy"] = erasure_strategy
            captured["strategy_per_field"] = strategy_per_field
            return df

        monkeypatch.setattr(gdpr, "_forget_polars", fake_forget_polars)
        fake_run_log = types.ModuleType("lakelogic.core.run_log")
        fake_run_log.write_run_log = lambda *a, **kw: None
        monkeypatch.setitem(sys.modules, "lakelogic.core.run_log", fake_run_log)

        contract = _contract_with_pii()
        df = pl.DataFrame({"customer_id": ["c1"], "email": ["a@x.com"], "phone": ["1"]})
        gdpr.forget_subjects(
            df,
            contract,
            "customer_id",
            ["c1"],
            erasure_strategy="nullify",
            compliance_event={"strategy": "hash", "strategy_per_field": {"email": "redact"}},
            audit=False,
        )
        assert captured["strategy"] == "hash"
        assert captured["strategy_per_field"] == {"email": "redact"}

    def test_contract_compliance_attr_is_used_when_no_event_passed(self, monkeypatch):
        # If forget_subjects is called without compliance_event AND contract has one,
        # the contract's compliance is used.
        captured = {}

        def fake_forget_polars(
            df, pii_columns, subject_column, subject_ids, erasure_strategy,
            hash_salt, partition_filter=None, delete_reason=gdpr.DELETE_REASON_GDPR_ART17,
            strategy_per_field=None,
        ):
            captured["strategy"] = erasure_strategy
            return df

        monkeypatch.setattr(gdpr, "_forget_polars", fake_forget_polars)
        fake_run_log = types.ModuleType("lakelogic.core.run_log")
        fake_run_log.write_run_log = lambda *a, **kw: None
        monkeypatch.setitem(sys.modules, "lakelogic.core.run_log", fake_run_log)

        contract = _contract_with_pii()
        # Attach a compliance dict directly on the contract object
        contract.compliance = {"strategy": "redact"}  # type: ignore[attr-defined]
        df = pl.DataFrame({"customer_id": ["c1"], "email": ["a@x.com"], "phone": ["1"]})
        gdpr.forget_subjects(df, contract, "customer_id", ["c1"], audit=False)
        assert captured["strategy"] == "redact"

    def test_audit_report_collected_into_out_list(self, monkeypatch):
        fake_run_log = types.ModuleType("lakelogic.core.run_log")
        fake_run_log.write_run_log = lambda *a, **kw: None
        monkeypatch.setitem(sys.modules, "lakelogic.core.run_log", fake_run_log)

        contract = _contract_with_pii()
        df = pl.DataFrame({"customer_id": ["c1"], "email": ["a@x.com"], "phone": ["1"]})
        out = []
        gdpr.forget_subjects(
            df, contract, "customer_id", ["c1"], audit=True, audit_report_out=out
        )
        assert len(out) == 1
        assert out[0]["status"] == "ok"
        assert out[0]["engine"] == "polars"
        assert out[0]["counts"]["total"] == 1

    def test_invalid_erasure_strategy_raises(self):
        contract = _contract_with_pii()
        df = pl.DataFrame({"customer_id": ["c1"], "email": ["a@x.com"], "phone": ["1"]})
        with pytest.raises(ValueError, match="Invalid erasure_strategy"):
            gdpr.forget_subjects(
                df, contract, "customer_id", ["c1"],
                erasure_strategy="evaporate", audit=False,
            )


# ──────────────────────────────────────────────────────────────────────────────
# mask_pii_columns — branches not covered by existing tests
# ──────────────────────────────────────────────────────────────────────────────


class TestMaskPiiColumnsBranches:
    def test_polars_hash_strategy(self):
        contract = _contract_with_pii()
        df = pl.DataFrame({"customer_id": ["c1"], "email": ["a@x.com"], "phone": ["1"]})
        out = gdpr.mask_pii_columns(df, contract, strategy="hash", hash_salt="s")
        assert out["email"].to_list()[0] == gdpr._hash_value("a@x.com", "s")
        assert out["phone"].to_list()[0] == gdpr._hash_value("1", "s")

    def test_polars_nullify_strategy(self):
        contract = _contract_with_pii()
        df = pl.DataFrame({"customer_id": ["c1"], "email": ["a@x.com"], "phone": ["1"]})
        out = gdpr.mask_pii_columns(df, contract, strategy="nullify")
        assert out["email"].to_list() == [None]
        assert out["phone"].to_list() == [None]

    def test_lazyframe_input_is_supported(self):
        contract = _contract_with_pii()
        lazy = pl.DataFrame(
            {"customer_id": ["c1"], "email": ["a@x.com"], "phone": ["1"]}
        ).lazy()
        out = gdpr.mask_pii_columns(lazy, contract, strategy="redact")
        # _mask_polars collects LazyFrames internally
        materialized = out.collect() if hasattr(out, "collect") else out
        assert materialized["email"].to_list() == ["***REDACTED***"]

    def test_unsupported_dataframe_type_raises(self):
        contract = _contract_with_pii()

        class WeirdFrame:
            pass

        with pytest.raises(TypeError, match="Unsupported dataframe type"):
            gdpr.mask_pii_columns(WeirdFrame(), contract)


# ──────────────────────────────────────────────────────────────────────────────
# generate_erasure_report — compliance_event branch
# ──────────────────────────────────────────────────────────────────────────────


class TestGenerateErasureReport:
    def test_compliance_event_included_when_provided(self):
        contract = _contract_with_pii()
        ce = {"framework": "GDPR", "article": "Article 17"}
        report = gdpr.generate_erasure_report(
            contract, "customer_id", ["c1"], compliance_event=ce
        )
        assert report["compliance_event"] == ce

    def test_compliance_event_omitted_when_not_provided(self):
        contract = _contract_with_pii()
        report = gdpr.generate_erasure_report(contract, "customer_id", ["c1"])
        assert "compliance_event" not in report

    def test_contract_without_info_uses_unknown_title(self):
        # Build a contract with info=None to exercise the fallback path
        plain = DataContract(
            version="1.0", dataset="plain", model=Model(fields=[
                FieldDefinition(name="customer_id", type="string"),
                FieldDefinition(name="email", type="string", pii=True),
            ])
        )
        # Pydantic may have populated info with defaults; force it to None
        plain.info = None  # type: ignore[assignment]
        report = gdpr.generate_erasure_report(plain, "customer_id", ["c1"])
        assert report["contract"] == "unknown"
