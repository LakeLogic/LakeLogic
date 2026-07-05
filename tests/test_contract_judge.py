"""Tests for the LLM judgment layer (lakelogic.ai.contract_judge).

The LLM call is injected via `caller=`, so these run with no API key and no network.
"""

from types import SimpleNamespace

from lakelogic.ai.contract_judge import judge_contract, _build_user_prompt
from lakelogic.core.contract_lint import ContractReviewReport, gate_severity

RAW = {
    "info": {"target_layer": "silver"},
    "primary_key": ["id"],
    "model": {
        "fields": [
            {"name": "rating", "type": "float"},
            {"name": "notes", "type": "string"},
        ]
    },
    "quality": {"row_rules": []},
}


def test_judge_maps_mock_findings():
    def caller(prompt):
        return [
            {
                "check_id": "SEM-001",
                "field": "rating",
                "severity": "warning",
                "message": "rating has no range rule",
                "suggestion": "add range 0-5",
            },
            {"check_id": "SEM-002", "field": "notes", "severity": "info", "message": "notes may contain PII"},
        ]

    fs = judge_contract(RAW, "silver_x", caller=caller)
    assert len(fs) == 2
    assert all(f.source == "llm" and f.category == "semantic" for f in fs)
    assert {f.check_id for f in fs} == {"SEM-001", "SEM-002"}
    by = {f.check_id: f for f in fs}
    assert by["SEM-001"].field == "rating" and by["SEM-001"].suggestion == "add range 0-5"


def test_judge_clamps_severity_below_critical():
    fs = judge_contract(RAW, "c", caller=lambda p: [{"check_id": "SEM-001", "severity": "critical", "message": "m"}])
    assert fs[0].severity == "warning"  # LLM never emits critical


def test_judge_graceful_without_api_key():
    cfg = SimpleNamespace(api_key_present=False, provider="none", model="none")
    assert judge_contract(RAW, "c", config=cfg) == []


def test_judge_graceful_on_llm_error():
    def boom(prompt):
        raise RuntimeError("network down")

    assert judge_contract(RAW, "c", caller=boom) == []


def test_judge_tolerates_garbage_items():
    fs = judge_contract(RAW, "c", caller=lambda p: [None, {"message": "ok"}, 42])
    assert len(fs) == 1 and fs[0].check_id == "SEM-000"  # default id, junk skipped


def test_llm_findings_never_gate_ci():
    fs = judge_contract(RAW, "c", caller=lambda p: [{"check_id": "SEM-001", "severity": "warning", "message": "m"}])
    rep = ContractReviewReport(contracts_scanned=1, findings=fs, summary={})
    assert gate_severity(rep) is None  # advisory only — never blocks


def test_prompt_includes_fields_and_rubric():
    p = _build_user_prompt(RAW, "silver_x", None)
    assert "rating" in p and "notes" in p and "silver_x" in p
