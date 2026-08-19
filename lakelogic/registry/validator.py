"""Structural + referential validation for registry files.

Two layers:
  * **Structural** — parse a ``_domain.yaml`` / ``_system.yaml`` through the strict
    manifest model (:mod:`lakelogic.registry.models`): unknown keys, wrong types, missing
    required identity, duplicate contract entities.
  * **Referential** — checks that span files: every ``contracts:`` entry resolves to a
    file on disk, and a system's ``domain`` agrees with its sibling ``_domain.yaml``
    (a mismatch is a warning — under resolution the domain value wins, see
    ``docs/contracts/inheritance.md``).

The validator never executes a pipeline and never needs cloud credentials — it reads
YAML and the local filesystem only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

from lakelogic.registry.models import DomainManifestV1, SystemManifestV1

try:  # ruamel matches the runtime loader; fall back to PyYAML if unavailable
    from ruamel.yaml import YAML

    def _load_yaml(text: str):
        return YAML(typ="safe").load(text)
except Exception:  # pragma: no cover
    import yaml

    def _load_yaml(text: str):
        return yaml.safe_load(text)


@dataclass
class Finding:
    """One validation result. ``level`` is ``error`` | ``warning`` | ``info``."""

    level: str
    file: str
    message: str
    hint: Optional[str] = None

    @property
    def is_error(self) -> bool:
        return self.level == "error"


@dataclass
class FileReport:
    """The findings for a single registry file (plus the parsed manifest, if it parsed)."""

    path: str
    kind: str  # "domain" | "system" | "unknown"
    findings: List[Finding] = field(default_factory=list)
    manifest: object | None = None

    @property
    def ok(self) -> bool:
        return not any(f.is_error for f in self.findings)


# ── discovery ────────────────────────────────────────────────────────────────


def _kind_of(path: Path) -> str:
    name = path.name.lower()
    if name in ("_domain.yaml", "_domain.yml"):
        return "domain"
    if name in ("_system.yaml", "_system.yml"):
        return "system"
    return "unknown"


def discover(root: Path) -> List[Path]:
    """Every ``_domain.yaml`` / ``_system.yaml`` under ``root`` (or just ``root`` itself if
    it is one of those files). Skips VCS/backup/vendored trees."""
    if root.is_file():
        return [root] if _kind_of(root) != "unknown" else []
    skip = {".git", "node_modules", ".venv", "__pycache__"}
    out: List[Path] = []
    for p in sorted(root.rglob("_*.yaml")):
        if _kind_of(p) == "unknown":
            continue
        if any(part in skip or part.startswith(".bronze_source_backup") for part in p.parts):
            continue
        out.append(p)
    return out


# ── contract-path resolution (mirrors core.registry, minus env substitution) ──


def _resolve_contract_path(raw_path: str, domain: str, system: str) -> str:
    """Resolve ``{domain}``/``{system}``/``{*_layer}`` in a contract path. Layer aliases
    always resolve to the canonical literals for *file discovery* (see the resolution
    spec) — the alias values only rename tables/content, not files on disk."""
    return (
        raw_path.replace("{domain}", domain)
        .replace("{system}", system)
        .replace("{bronze_layer}", "bronze")
        .replace("{silver_layer}", "silver")
        .replace("{gold_layer}", "gold")
    )


# ── single-file validation ───────────────────────────────────────────────────


def validate_file(path: Path, *, sibling_domain: Optional[str] = None) -> FileReport:
    """Validate one registry file. For a ``_system.yaml``, also checks that each contract
    path exists and that its ``domain`` agrees with ``sibling_domain`` (the domain declared
    in the adjacent ``_domain.yaml``, when known)."""
    kind = _kind_of(path)
    report = FileReport(path=str(path), kind=kind)

    try:
        raw = _load_yaml(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # unparseable YAML
        report.findings.append(Finding("error", str(path), f"unparseable YAML: {exc}"))
        return report
    if not isinstance(raw, dict):
        report.findings.append(Finding("error", str(path), "top level must be a mapping"))
        return report

    model = DomainManifestV1 if kind == "domain" else SystemManifestV1 if kind == "system" else None
    if model is None:
        report.findings.append(Finding("info", str(path), "not a registry file — skipped"))
        return report

    try:
        manifest = model.model_validate(raw)
        report.manifest = manifest
    except Exception as exc:
        first = next((ln.strip() for ln in str(exc).splitlines()[1:] if ln.strip()), str(exc))
        report.findings.append(Finding("error", str(path), f"schema: {first}"))
        return report

    if kind == "system":
        _check_system_references(path, manifest, sibling_domain, report)

    return report


def _check_system_references(
    path: Path, manifest: SystemManifestV1, sibling_domain: Optional[str], report: FileReport
) -> None:
    domain = manifest.domain or sibling_domain or "{domain}"
    system = manifest.system

    # Domain identity should agree with the sibling _domain.yaml (domain wins on mismatch).
    if manifest.domain and sibling_domain and manifest.domain != sibling_domain:
        report.findings.append(
            Finding(
                "warning",
                str(path),
                f"domain '{manifest.domain}' disagrees with sibling _domain.yaml "
                f"('{sibling_domain}') — the domain value wins at resolution",
            )
        )

    # Every enabled contract must resolve to a file on disk.
    base = path.parent
    for c in manifest.contracts:
        if getattr(c, "enabled", True) is False:
            continue
        if not c.path:
            report.findings.append(Finding("error", str(path), f"contract '{c.entity}' has no path"))
            continue
        resolved = _resolve_contract_path(c.path, domain, system)
        candidate = Path(resolved)
        if not candidate.is_absolute():
            candidate = base / candidate
        if not candidate.exists():
            report.findings.append(
                Finding(
                    "error",
                    str(path),
                    f"contract '{c.entity}' → file not found: {resolved}",
                    hint=f"expected at {candidate}",
                )
            )


# ── tree validation ──────────────────────────────────────────────────────────


def validate_tree(root: Path) -> List[FileReport]:
    """Validate every registry file under ``root``. Each ``_system.yaml`` is given its
    sibling ``_domain.yaml``'s domain (one directory up) for the identity cross-check."""
    reports: List[FileReport] = []
    for p in discover(root):
        sibling_domain = None
        if _kind_of(p) == "system":
            sibling_domain = _sibling_domain(p)
        reports.append(validate_file(p, sibling_domain=sibling_domain))
    return reports


def _sibling_domain(system_path: Path) -> Optional[str]:
    """The ``domain`` declared in the ``_domain.yaml`` one directory above a system file."""
    domain_yaml = system_path.parent.parent / "_domain.yaml"
    if not domain_yaml.exists():
        return None
    try:
        d = _load_yaml(domain_yaml.read_text(encoding="utf-8")) or {}
        return d.get("domain") if isinstance(d, dict) else None
    except Exception:  # pragma: no cover
        return None


def summarize(reports: List[FileReport]) -> Tuple[int, int, int]:
    """(files, errors, warnings) across all reports."""
    errors = sum(1 for r in reports for f in r.findings if f.level == "error")
    warnings = sum(1 for r in reports for f in r.findings if f.level == "warning")
    return len(reports), errors, warnings
