"""Tests for HIPAA erasure: the polars helper, public forget_patients /
mask_phi_columns / generate_hipaa_erasure_report, and HIPAA-specific
profile behavior (PHI marker, Safe Harbor reason code, phi-or-pii field
selection).

Parallels test_gdpr_polars_and_dispatch.py to give HIPAA the same
coverage GDPR has now that both regimes share the engine in
core/erasure.py.
"""

from __future__ import annotations

import sys
import types

import pytest

pl = pytest.importorskip("polars")
pd = pytest.importorskip("pandas")

from lakelogic.core import hipaa
from lakelogic.core.constants import (
    DELETE_REASON_HIPAA_PHI,
    META_DELETE_REASON,
    META_DELETED_AT,
    META_IS_DELETED,
    META_UPDATED_AT,
)
from lakelogic.core.models import DataContract, FieldDefinition, Info, Model


def _contract_with_phi():
    return DataContract(
        version="1.0",
        info=Info(title="Patients", version="1.0"),
        dataset="patients",
        metadata={"domain": "health", "system": "ehr"},
        model=Model(
            fields=[
                FieldDefinition(name="patient_id", type="string", required=True),
                FieldDefinition(name="diagnosis", type="string", phi=True),
                FieldDefinition(name="medication", type="string", phi=True),
                FieldDefinition(name="email", type="string", pii=True),  # PHI selector pulls PII too
                FieldDefinition(name="region", type="string"),
            ]
        ),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Profile-level: field selection (phi OR pii) and helper exports
# ──────────────────────────────────────────────────────────────────────────────


class TestPhiFieldSelection:
    def test_phi_fields_include_both_phi_and_pii_annotated(self):
        contract = _contract_with_phi()
        names = hipaa._get_phi_column_names(contract)
        # diagnosis & medication are phi=True; email is pii=True — HIPAA pulls all three
        assert set(names) == {"diagnosis", "medication", "email"}

    def test_empty_contract_returns_empty_field_list(self):
        empty = DataContract(version="1.0", info=Info(title="Empty", version="1.0"), dataset="empty")
        assert hipaa._get_phi_fields(empty) == []
        assert hipaa._get_phi_column_names(empty) == []


# ──────────────────────────────────────────────────────────────────────────────
# _forget_polars — the polars dispatcher reached via the HIPAA facade shim
# ──────────────────────────────────────────────────────────────────────────────


class TestForgetPolars:
    def _df(self):
        return pl.DataFrame(
            {
                "patient_id": ["p1", "p2", "p3"],
                "diagnosis": ["dx-A", "dx-B", "dx-C"],
                "medication": ["med-1", "med-2", "med-3"],
                "email": ["a@hosp.com", "b@hosp.com", "c@hosp.com"],
                "region": ["US-EAST", "US-WEST", "US-EAST"],
            }
        )

    def test_lazyframe_input_is_collected(self):
        lazy = self._df().lazy()
        out = hipaa._forget_polars(lazy, ["diagnosis"], "patient_id", ["p1"], "nullify", "")
        assert isinstance(out, pl.DataFrame)
        assert out.sort("patient_id")["diagnosis"].to_list()[0] is None

    def test_no_phi_columns_present_returns_unchanged(self):
        df = self._df()
        out = hipaa._forget_polars(df, ["ssn", "icd10"], "patient_id", ["p1"], "nullify", "")
        # No matching PHI columns → no transformation, no metadata cols injected
        assert META_IS_DELETED not in out.columns
        assert out.height == 3

    def test_missing_patient_column_raises(self):
        df = self._df()
        with pytest.raises(ValueError, match="Patient column 'missing' not found"):
            hipaa._forget_polars(df, ["diagnosis"], "missing", ["p1"], "nullify", "")

    def test_nullify_strategy(self):
        df = self._df()
        out = hipaa._forget_polars(df, ["diagnosis", "medication"], "patient_id", ["p1"], "nullify", "").sort(
            "patient_id"
        )
        assert out["diagnosis"].to_list() == [None, "dx-B", "dx-C"]
        assert out["medication"].to_list() == [None, "med-2", "med-3"]

    def test_hash_strategy(self):
        df = self._df()
        out = hipaa._forget_polars(df, ["diagnosis"], "patient_id", ["p1"], "hash", "phi-salt").sort("patient_id")
        expected = hipaa._hash_value("dx-A", "phi-salt")
        assert out["diagnosis"].to_list()[0] == expected
        assert out["diagnosis"].to_list()[1] == "dx-B"  # untouched

    def test_redact_strategy_uses_phi_marker(self):
        df = self._df()
        out = hipaa._forget_polars(df, ["diagnosis"], "patient_id", ["p2"], "redact", "").sort("patient_id")
        # HIPAA marker is distinct from GDPR's ***REDACTED***
        assert out["diagnosis"].to_list()[1] == "***REDACTED_PHI***"

    def test_partition_filter_scopes_erasure(self):
        df = self._df()
        out = hipaa._forget_polars(
            df,
            ["diagnosis"],
            "patient_id",
            ["p1", "p2", "p3"],
            "nullify",
            "",
            partition_filter={"column": "region", "value": "US-EAST"},
        ).sort("patient_id")
        diag = out["diagnosis"].to_list()
        assert diag[0] is None  # p1 in US-EAST → erased
        assert diag[1] == "dx-B"  # p2 in US-WEST → skipped
        assert diag[2] is None  # p3 in US-EAST → erased

    def test_partition_filter_missing_column_is_ignored(self, monkeypatch):
        warnings = []
        monkeypatch.setattr(hipaa.logger, "warning", warnings.append)
        df = self._df()
        out = hipaa._forget_polars(
            df,
            ["diagnosis"],
            "patient_id",
            ["p1"],
            "nullify",
            "",
            partition_filter={"column": "missing_col", "value": "X"},
        ).sort("patient_id")
        assert out["diagnosis"].to_list()[0] is None
        assert any("ignoring partition filter" in m for m in warnings)

    def test_nullify_does_not_null_the_patient_column_itself(self):
        df = self._df()
        out = hipaa._forget_polars(df, ["patient_id", "diagnosis"], "patient_id", ["p1"], "nullify", "").sort(
            "patient_id"
        )
        assert out["patient_id"].to_list()[0] == "p1"
        assert out["diagnosis"].to_list()[0] is None

    def test_strategy_per_field_overrides_default(self):
        df = self._df()
        out = hipaa._forget_polars(
            df,
            ["diagnosis", "medication"],
            "patient_id",
            ["p1"],
            "nullify",
            "phi-salt",
            strategy_per_field={"diagnosis": "hash", "medication": "redact"},
        ).sort("patient_id")
        assert out["diagnosis"].to_list()[0] == hipaa._hash_value("dx-A", "phi-salt")
        assert out["medication"].to_list()[0] == "***REDACTED_PHI***"

    def test_compliance_metadata_columns_use_hipaa_reason(self):
        df = self._df()
        out = hipaa._forget_polars(df, ["diagnosis"], "patient_id", ["p1"], "nullify", "").sort("patient_id")
        assert out[META_IS_DELETED].to_list() == [True, False, False]
        reasons = out[META_DELETE_REASON].to_list()
        # Reason is the HIPAA Safe Harbor code, NOT the GDPR Article 17 code
        assert reasons[0] == DELETE_REASON_HIPAA_PHI
        assert reasons[1] is None
        assert out[META_DELETED_AT].to_list()[0] is not None
        assert out[META_UPDATED_AT].to_list()[0] is not None


# ──────────────────────────────────────────────────────────────────────────────
# forget_patients — public dispatcher branches
# ──────────────────────────────────────────────────────────────────────────────


class TestForgetPatientsDispatch:
    def _stub_run_log(self, monkeypatch):
        fake = types.ModuleType("lakelogic.core.run_log")
        fake.write_run_log = lambda *a, **kw: None
        monkeypatch.setitem(sys.modules, "lakelogic.core.run_log", fake)

    def test_polars_dispatch_returns_polars(self, monkeypatch):
        self._stub_run_log(monkeypatch)
        contract = _contract_with_phi()
        df = pl.DataFrame(
            {
                "patient_id": ["p1", "p2"],
                "diagnosis": ["dx-A", "dx-B"],
                "medication": ["med-1", "med-2"],
                "email": ["a@hosp.com", "b@hosp.com"],
                "region": ["US-EAST", "US-WEST"],
            }
        )
        result = hipaa.forget_patients(df, contract, "patient_id", ["p1"], audit=True)
        assert isinstance(result, pl.DataFrame)
        assert result.sort("patient_id")["diagnosis"].to_list()[0] is None

    def test_pandas_dispatch(self, monkeypatch):
        self._stub_run_log(monkeypatch)
        contract = _contract_with_phi()
        df = pd.DataFrame(
            {
                "patient_id": ["p1", "p2"],
                "diagnosis": ["dx-A", "dx-B"],
                "medication": ["med-1", "med-2"],
                "email": ["a@hosp.com", "b@hosp.com"],
                "region": ["US-EAST", "US-WEST"],
            }
        )
        result = hipaa.forget_patients(df, contract, "patient_id", ["p1"], audit=False)
        assert isinstance(result, pd.DataFrame)
        out = result.set_index("patient_id")
        assert pd.isna(out.loc["p1", "diagnosis"])
        assert out.loc["p2", "diagnosis"] == "dx-B"
        assert out.loc["p1", META_IS_DELETED] == True  # noqa: E712
        assert out.loc["p1", META_DELETE_REASON] == DELETE_REASON_HIPAA_PHI

    def test_duckdb_dispatch_via_fetchdf(self, monkeypatch):
        duckdb = pytest.importorskip("duckdb")
        self._stub_run_log(monkeypatch)
        contract = _contract_with_phi()
        con = duckdb.connect(":memory:")
        rel = con.from_df(
            pd.DataFrame(
                {
                    "patient_id": ["p1", "p2"],
                    "diagnosis": ["dx-A", "dx-B"],
                    "medication": ["med-1", "med-2"],
                    "email": ["a@hosp.com", "b@hosp.com"],
                    "region": ["US-EAST", "US-WEST"],
                }
            )
        )
        result = hipaa.forget_patients(rel, contract, "patient_id", ["p1"], audit=False)
        out = result.fetchdf().set_index("patient_id")
        assert pd.isna(out.loc["p1", "diagnosis"])
        assert out.loc["p1", META_DELETE_REASON] == DELETE_REASON_HIPAA_PHI

    def test_unsupported_dataframe_type_raises(self):
        contract = _contract_with_phi()

        class WeirdFrame:
            pass

        with pytest.raises(TypeError, match="Unsupported dataframe type"):
            hipaa.forget_patients(WeirdFrame(), contract, "patient_id", ["p1"], audit=False)

    def test_no_phi_in_contract_warns_and_returns_unchanged(self, monkeypatch):
        warnings = []
        monkeypatch.setattr(hipaa.logger, "warning", warnings.append)
        no_phi = DataContract(version="1.0", info=Info(title="Plain", version="1.0"), dataset="plain")
        df = pd.DataFrame({"patient_id": ["p1"]})
        out = hipaa.forget_patients(df, no_phi, "patient_id", ["p1"], audit=False)
        assert out is df
        assert any("No PHI fields defined in contract" in m for m in warnings)

    def test_compliance_event_overrides_strategy(self, monkeypatch):
        self._stub_run_log(monkeypatch)
        captured = {}

        def fake_polars(
            df,
            phi_cols,
            patient_col,
            ids,
            strategy,
            salt,
            partition_filter=None,
            delete_reason=DELETE_REASON_HIPAA_PHI,
            strategy_per_field=None,
        ):
            captured["strategy"] = strategy
            captured["strategy_per_field"] = strategy_per_field
            return df

        monkeypatch.setattr(hipaa, "_forget_polars", fake_polars)

        contract = _contract_with_phi()
        df = pl.DataFrame({"patient_id": ["p1"], "diagnosis": ["dx-A"], "medication": ["m"], "email": ["a@x.com"]})
        hipaa.forget_patients(
            df,
            contract,
            "patient_id",
            ["p1"],
            erasure_strategy="nullify",
            compliance_event={"strategy": "hash", "strategy_per_field": {"diagnosis": "redact"}},
            audit=False,
        )
        assert captured["strategy"] == "hash"
        assert captured["strategy_per_field"] == {"diagnosis": "redact"}

    def test_invalid_strategy_raises(self):
        contract = _contract_with_phi()
        df = pl.DataFrame({"patient_id": ["p1"], "diagnosis": ["dx-A"], "medication": ["m"], "email": ["a@x.com"]})
        with pytest.raises(ValueError, match="Invalid erasure_strategy"):
            hipaa.forget_patients(df, contract, "patient_id", ["p1"], erasure_strategy="vaporize", audit=False)

    def test_audit_report_collected_into_out_list(self, monkeypatch):
        self._stub_run_log(monkeypatch)
        contract = _contract_with_phi()
        df = pl.DataFrame({"patient_id": ["p1"], "diagnosis": ["dx-A"], "medication": ["m"], "email": ["a@x.com"]})
        out = []
        hipaa.forget_patients(df, contract, "patient_id", ["p1"], audit=True, audit_report_out=out)
        assert len(out) == 1
        assert out[0]["status"] == "ok"
        assert out[0]["engine"] == "polars"
        assert out[0]["stage"] == "hipaa_erasure"


# ──────────────────────────────────────────────────────────────────────────────
# mask_phi_columns — Safe Harbor de-identification of all rows
# ──────────────────────────────────────────────────────────────────────────────


class TestMaskPhiColumns:
    def test_nullify_strategy_polars(self):
        df = pl.DataFrame({"patient_id": ["p1", "p2"], "diagnosis": ["dx-A", "dx-B"], "email": ["a", "b"]})
        contract = _contract_with_phi()
        out = hipaa.mask_phi_columns(df, contract, strategy="nullify")
        assert out["diagnosis"].to_list() == [None, None]
        assert out["email"].to_list() == [None, None]

    def test_redact_strategy_uses_phi_marker_polars(self):
        df = pl.DataFrame({"patient_id": ["p1"], "diagnosis": ["dx-A"], "email": ["a@x.com"]})
        contract = _contract_with_phi()
        out = hipaa.mask_phi_columns(df, contract, strategy="redact")
        assert out["diagnosis"].to_list()[0] == "***REDACTED_PHI***"
        assert out["email"].to_list()[0] == "***REDACTED_PHI***"

    def test_hash_strategy_pandas(self):
        df = pd.DataFrame({"patient_id": ["p1"], "diagnosis": ["dx-A"], "email": ["a@x.com"]})
        contract = _contract_with_phi()
        out = hipaa.mask_phi_columns(df, contract, strategy="hash", hash_salt="salt")
        assert out.loc[0, "diagnosis"] == hipaa._hash_value("dx-A", "salt")

    def test_invalid_strategy_raises(self):
        contract = _contract_with_phi()
        df = pl.DataFrame({"patient_id": ["p1"], "diagnosis": ["dx-A"]})
        with pytest.raises(ValueError, match="Invalid strategy"):
            hipaa.mask_phi_columns(df, contract, strategy="evaporate")

    def test_explicit_columns_override_contract(self):
        df = pl.DataFrame({"patient_id": ["p1"], "diagnosis": ["dx-A"], "notes": ["sensitive"]})
        contract = _contract_with_phi()  # 'notes' is NOT a PHI field in the contract
        out = hipaa.mask_phi_columns(df, contract, strategy="redact", columns=["notes"])
        assert out["notes"].to_list()[0] == "***REDACTED_PHI***"
        # diagnosis untouched because we explicitly overrode the column list
        assert out["diagnosis"].to_list()[0] == "dx-A"

    def test_no_phi_columns_warns_and_returns_unchanged(self, monkeypatch):
        warnings = []
        monkeypatch.setattr(hipaa.logger, "warning", warnings.append)
        contract = DataContract(version="1.0", info=Info(title="X", version="1.0"), dataset="x")
        df = pl.DataFrame({"patient_id": ["p1"]})
        out = hipaa.mask_phi_columns(df, contract, strategy="nullify")
        assert out is df
        assert any("No PHI columns to mask" in m for m in warnings)


# ──────────────────────────────────────────────────────────────────────────────
# generate_hipaa_erasure_report
# ──────────────────────────────────────────────────────────────────────────────


class TestGenerateHipaaReport:
    def test_report_shape_and_hipaa_specific_keys(self):
        contract = _contract_with_phi()
        report = hipaa.generate_hipaa_erasure_report(
            contract, "patient_id", ["p1", "p2"], erasure_strategy="nullify", affected_rows=2
        )
        assert report["report_type"] == "hipaa_erasure"
        # HIPAA-shaped keys (NOT GDPR's subject_*)
        assert report["patient_column"] == "patient_id"
        assert report["patients_erased"] == 2
        assert set(report["phi_columns_affected"]) == {"diagnosis", "medication", "email"}
        assert report["affected_rows"] == 2
        assert "HIPAA" in report["compliance_note"]
        assert "Safe Harbor" in report["compliance_note"]

    def test_report_includes_partition_filter_when_provided(self):
        contract = _contract_with_phi()
        report = hipaa.generate_hipaa_erasure_report(
            contract,
            "patient_id",
            ["p1"],
            partition_filter={"column": "region", "value": "US-EAST"},
        )
        assert report["partition_filter"] == {"column": "region", "value": "US-EAST"}

    def test_report_includes_compliance_event_when_provided(self):
        contract = _contract_with_phi()
        report = hipaa.generate_hipaa_erasure_report(
            contract, "patient_id", ["p1"], compliance_event={"strategy": "hash"}
        )
        assert report["compliance_event"] == {"strategy": "hash"}
