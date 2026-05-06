"""
Tests for the contract gates module.

Covers:
- lakelogic.gates.base (ContractGate, GateResult, GateStatus)
- lakelogic.gates.breaking_change (BreakingChangeGate)
- lakelogic.gates.pii_classification (PIIClassificationGate)
- lakelogic.gates.lineage_break (LineageBreakGate)
- lakelogic.gates.__init__ (re-exports)
"""

from types import SimpleNamespace

import pytest

from lakelogic.gates import ContractGate, GateResult
from lakelogic.gates.base import GateStatus
from lakelogic.gates.breaking_change import BreakingChangeGate
from lakelogic.gates.lineage_break import LineageBreakGate
from lakelogic.gates.pii_classification import PIIClassificationGate


# ── Helpers ──────────────────────────────────────────────────────────────────


def _field(name, type_="string", **kwargs):
    """Build a simple field-like object with arbitrary attributes."""
    return SimpleNamespace(name=name, type=type_, **kwargs)


def _contract(fields=None, version="1.0.0", upstream_refs=None):
    """Build a contract-like object matching what gates expect via getattr."""
    model = SimpleNamespace(fields=fields or [])
    info = SimpleNamespace(version=version, name="test_contract", owner="data-team")
    lineage = SimpleNamespace(upstream=upstream_refs or [])
    return SimpleNamespace(model=model, info=info, lineage=lineage)


# ── Base classes ─────────────────────────────────────────────────────────────


def test_gate_result_passes_is_truthy():
    result = GateResult(gate_name="g", status=GateStatus.PASSED, message="ok")
    assert bool(result) is True


def test_gate_result_warning_is_truthy():
    result = GateResult(gate_name="g", status=GateStatus.WARNING, message="warn")
    assert bool(result) is True


def test_gate_result_failed_is_falsy():
    result = GateResult(gate_name="g", status=GateStatus.FAILED, message="bad")
    assert bool(result) is False


def test_gate_result_skipped_is_falsy():
    result = GateResult(gate_name="g", status=GateStatus.SKIPPED, message="skip")
    assert bool(result) is False


def test_gate_result_default_collections():
    result = GateResult(gate_name="g", status=GateStatus.PASSED, message="ok")
    assert result.details == {}
    assert result.violations == []


def test_contract_gate_is_abstract():
    with pytest.raises(TypeError):
        ContractGate(name="x")  # cannot instantiate ABC directly


def test_gate_status_enum_values():
    assert GateStatus.PASSED.value == "passed"
    assert GateStatus.FAILED.value == "failed"
    assert GateStatus.WARNING.value == "warning"
    assert GateStatus.SKIPPED.value == "skipped"


# ── BreakingChangeGate ───────────────────────────────────────────────────────


def test_breaking_change_skipped_when_no_context():
    gate = BreakingChangeGate()
    contract = _contract(fields=[_field("id", "int")])
    result = gate.run(contract, context=None)

    assert result.status == GateStatus.SKIPPED
    assert "no_baseline" in result.details.get("reason", "")


def test_breaking_change_skipped_when_context_missing_previous_key():
    gate = BreakingChangeGate()
    contract = _contract(fields=[_field("id", "int")])
    result = gate.run(contract, context={"unrelated": "value"})

    assert result.status == GateStatus.SKIPPED


def test_breaking_change_passes_when_previous_is_none():
    gate = BreakingChangeGate()
    contract = _contract(fields=[_field("id", "int")])
    result = gate.run(contract, context={"previous_contract": None})

    assert result.status == GateStatus.PASSED
    assert result.details["version_count"] == 1


def test_breaking_change_passes_when_no_diff():
    gate = BreakingChangeGate()
    fields = [_field("id", "int"), _field("name", "string")]
    contract = _contract(fields=fields, version="1.1.0")
    previous = _contract(fields=list(fields), version="1.0.0")

    result = gate.run(contract, context={"previous_contract": previous})

    assert result.status == GateStatus.PASSED
    assert result.details["fields_checked"] == 2


def test_breaking_change_detects_field_removal():
    gate = BreakingChangeGate()
    current = _contract(fields=[_field("id", "int")], version="2.0.0")
    previous = _contract(
        fields=[_field("id", "int"), _field("removed_col", "string")],
        version="1.0.0",
    )

    result = gate.run(current, context={"previous_contract": previous})

    assert result.status == GateStatus.FAILED
    assert any("removed_col" in v for v in result.violations)


def test_breaking_change_detects_type_change():
    gate = BreakingChangeGate()
    current = _contract(fields=[_field("amount", "string")], version="2.0.0")
    previous = _contract(fields=[_field("amount", "int")], version="1.0.0")

    result = gate.run(current, context={"previous_contract": previous})

    assert result.status == GateStatus.FAILED
    assert any("amount" in v and "int" in v and "string" in v for v in result.violations)


def test_breaking_change_strict_mode_still_fails():
    gate = BreakingChangeGate(strict=True)
    current = _contract(fields=[_field("id", "int")], version="2.0.0")
    previous = _contract(
        fields=[_field("id", "int"), _field("dropped", "string")],
        version="1.0.0",
    )

    result = gate.run(current, context={"previous_contract": previous})

    assert result.status == GateStatus.FAILED


# ── PIIClassificationGate ────────────────────────────────────────────────────


def test_pii_passes_when_no_fields():
    gate = PIIClassificationGate()
    contract = _contract(fields=[])
    result = gate.run(contract)

    assert result.status == GateStatus.PASSED
    assert result.details["fields_checked"] == 0


def test_pii_passes_when_no_pii_fields():
    gate = PIIClassificationGate()
    contract = _contract(fields=[_field("id", "int"), _field("status", "string")])
    result = gate.run(contract)

    assert result.status == GateStatus.PASSED
    assert result.details["pii_fields_checked"] == 0


def test_pii_fails_when_pii_field_missing_masking():
    gate = PIIClassificationGate()
    contract = _contract(fields=[_field("email", "string", pii=True, classification="restricted")])
    result = gate.run(contract)

    assert result.status == GateStatus.FAILED
    assert any("masking" in v for v in result.violations)


def test_pii_fails_when_pii_field_missing_classification():
    gate = PIIClassificationGate()
    contract = _contract(fields=[_field("email", "string", pii=True, masking="hash")])
    result = gate.run(contract)

    assert result.status == GateStatus.FAILED
    assert any("classification" in v for v in result.violations)


def test_pii_fails_with_invalid_classification():
    gate = PIIClassificationGate()
    contract = _contract(fields=[_field("email", "string", pii=True, masking="hash", classification="bogus")])
    result = gate.run(contract)

    assert result.status == GateStatus.FAILED
    assert any("bogus" in v for v in result.violations)


def test_pii_fails_with_invalid_dict_masking_type():
    gate = PIIClassificationGate()
    contract = _contract(
        fields=[
            _field(
                "ssn",
                "string",
                pii=True,
                masking={"type": "not_a_real_strategy"},
                classification="restricted",
            )
        ]
    )
    result = gate.run(contract)

    assert result.status == GateStatus.FAILED
    assert any("not_a_real_strategy" in v for v in result.violations)


def test_pii_passes_with_valid_dict_masking_type():
    gate = PIIClassificationGate()
    contract = _contract(
        fields=[
            _field(
                "ssn",
                "string",
                pii=True,
                masking={"type": "hash"},
                classification="restricted",
            )
        ]
    )
    result = gate.run(contract)

    assert result.status == GateStatus.PASSED
    assert result.details["pii_fields"] == 1


def test_pii_passes_with_compliant_phi_field():
    gate = PIIClassificationGate()
    contract = _contract(
        fields=[_field("diagnosis", "string", phi=True, masking="encrypt", classification="confidential")]
    )
    result = gate.run(contract)

    assert result.status == GateStatus.PASSED
    assert result.details["pii_fields"] == 1
    assert result.details["compliant"] == 1


# ── LineageBreakGate ─────────────────────────────────────────────────────────


def test_lineage_passes_when_no_upstream():
    gate = LineageBreakGate()
    contract = _contract(upstream_refs=[])
    result = gate.run(contract)

    assert result.status == GateStatus.PASSED
    assert result.details["upstream_count"] == 0


def test_lineage_fails_when_source_contract_missing():
    gate = LineageBreakGate()
    bad_upstream = SimpleNamespace()  # no source_contract attribute
    contract = _contract(upstream_refs=[bad_upstream])

    result = gate.run(contract)

    assert result.status == GateStatus.FAILED
    assert any("source_contract" in v for v in result.violations)


def test_lineage_fails_when_upstream_unresolved():
    gate = LineageBreakGate()
    upstream = SimpleNamespace(source_contract="domains/missing.yaml")
    contract = _contract(upstream_refs=[upstream])

    result = gate.run(contract, context={"contract_root": "/tmp/nonexistent", "registry": {}})

    assert result.status == GateStatus.FAILED
    assert any("missing.yaml" in v for v in result.violations)


def test_lineage_passes_when_upstream_in_registry():
    gate = LineageBreakGate()
    upstream = SimpleNamespace(source_contract="domains/orders.yaml")
    contract = _contract(upstream_refs=[upstream])

    result = gate.run(
        contract,
        context={"registry": {"domains/orders.yaml": "<resolved>"}},
    )

    assert result.status == GateStatus.PASSED
    assert result.details["upstream_count"] == 1


def test_lineage_passes_when_upstream_resolves_on_filesystem(tmp_path):
    gate = LineageBreakGate()

    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    (contracts_dir / "orders.yaml").write_text("placeholder: true\n")

    upstream = SimpleNamespace(source_contract="orders.yaml")
    contract = _contract(upstream_refs=[upstream])

    result = gate.run(contract, context={"contract_root": str(contracts_dir)})

    assert result.status == GateStatus.PASSED
    assert result.details["all_resolved"] is True


# ── __init__ re-exports ──────────────────────────────────────────────────────


def test_module_reexports():
    from lakelogic.gates import ContractGate as ReexportedGate
    from lakelogic.gates import GateResult as ReexportedResult

    assert ReexportedGate is ContractGate
    assert ReexportedResult is GateResult


# ── CLI validate skip behaviour for data mesh config files ───────────────────


@pytest.mark.parametrize(
    "filename",
    [
        "_domain.yaml",
        "_system.yaml",
        "marketing_domain.yaml",
        "salesforce_system.yaml",
        "CRM_DOMAIN.YAML",
        "analytics_system.yml",
    ],
)
def test_validate_cli_skips_domain_and_system_files(tmp_path, filename):
    """The CLI validate command should skip *domain.yaml/*system.yaml files."""
    from typer.testing import CliRunner

    from lakelogic.cli.main import app

    target = tmp_path / filename
    target.write_text("invalid: { not a real contract\n")  # malformed on purpose

    runner = CliRunner()
    result = runner.invoke(app, ["validate", "--contract", str(target)])

    assert result.exit_code == 0, f"Expected skip exit 0, got {result.exit_code}\n{result.output}"
    assert "Skipping" in result.output
    assert filename in result.output
