"""Registry validator — structural + referential checks."""

from __future__ import annotations

from pathlib import Path

from lakelogic.registry.validator import (
    discover,
    summarize,
    validate_file,
    validate_tree,
)

_ASSETS = Path(__file__).resolve().parents[2] / "examples" / "colab" / "assets" / "domains_rideflow"


def _write(p: Path, text: str) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return p


# ── real files ───────────────────────────────────────────────────────────────


def test_real_tree_validates_clean():
    reports = validate_tree(_ASSETS / "marketplace")
    assert reports, "expected to discover registry files"
    files, errors, warnings = summarize(reports)
    assert errors == 0
    assert all(r.ok for r in reports)


# ── referential: missing contract file ───────────────────────────────────────


def test_missing_contract_file_is_error(tmp_path):
    _write(tmp_path / "d" / "_domain.yaml", "domain: d\n")
    _write(
        tmp_path / "d" / "s" / "_system.yaml",
        "system: s\ndomain: d\ncontracts:\n  - {layer: bronze, entity: orders, path: contracts/does_not_exist.yaml}\n",
    )
    reports = validate_tree(tmp_path)
    sys_report = next(r for r in reports if r.kind == "system")
    assert not sys_report.ok
    assert any("file not found" in f.message for f in sys_report.findings if f.is_error)


def test_present_contract_file_passes(tmp_path):
    _write(tmp_path / "d" / "s" / "contracts" / "bronze" / "orders.yaml", "version: 1.0.0\n")
    _write(tmp_path / "d" / "_domain.yaml", "domain: d\n")
    _write(
        tmp_path / "d" / "s" / "_system.yaml",
        "system: s\ndomain: d\n"
        "contracts:\n"
        '  - {layer: bronze, entity: orders, path: "contracts/{bronze_layer}/orders.yaml"}\n',
    )
    reports = validate_tree(tmp_path)
    sys_report = next(r for r in reports if r.kind == "system")
    assert sys_report.ok, [f.message for f in sys_report.findings]


# ── referential: domain identity mismatch is a warning (domain wins) ─────────


def test_domain_mismatch_is_warning(tmp_path):
    _write(tmp_path / "d" / "_domain.yaml", "domain: marketing\n")
    _write(tmp_path / "d" / "s" / "_system.yaml", "system: s\ndomain: sales\ncontracts: []\n")
    reports = validate_tree(tmp_path)
    sys_report = next(r for r in reports if r.kind == "system")
    assert sys_report.ok  # a mismatch is only a warning
    assert any(f.level == "warning" and "disagrees" in f.message for f in sys_report.findings)


# ── structural: unknown key surfaces as an error ─────────────────────────────


def test_unknown_key_is_structural_error(tmp_path):
    p = _write(tmp_path / "s" / "_system.yaml", "system: s\nserver_defaults: {}\n")
    report = validate_file(p)
    assert not report.ok
    assert any("server_defaults" in f.message for f in report.findings)


def test_discover_skips_non_registry_files(tmp_path):
    _write(tmp_path / "_domain.yaml", "domain: d\n")
    _write(tmp_path / "contracts" / "bronze" / "orders.yaml", "version: 1.0.0\n")
    found = {p.name for p in discover(tmp_path)}
    assert found == {"_domain.yaml"}
