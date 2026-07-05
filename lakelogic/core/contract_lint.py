"""Deterministic data-contract governance lint.

Rules-based checks over the **authored contract YAML** — no LLM, no network, and
(deliberately) no runtime resolution. This is the trustworthy backbone of the
Contract Review Agent: it powers ``lakelogic lint``, CI PR review, and (later)
the *critic* that verifies remediation agents' proposals before a human sees them.

It lints the *authored* artifact, so it does **not** require the registry /
template / run-log context the pipeline needs to *execute* a contract — a linter
must run on a raw PR diff, standalone. (Contrast: ``DataContract.from_yaml`` does
full runtime validation and will reject an authored-but-unresolved contract.)

Distinct from :mod:`lakelogic.ai.code_reviewer` (LLM code review). Every finding
here is structural and reproducible, so it can gate. LLM *judgment* checks live in
the AI layer and are advisory only. Findings align with
``ai.code_reviewer.ReviewFinding`` severities (critical | warning | info).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field

# ── Finding schema ──────────────────────────────────────────────────────────

SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}


class ContractFinding(BaseModel):
    """One governance finding on one contract. Reproducible; ``source='rules'``."""

    contract: str
    check_id: str
    severity: str  # critical | warning | info
    category: str
    message: str
    field: Optional[str] = None
    suggestion: Optional[str] = None
    source: str = "rules"  # rules | llm
    file: Optional[str] = None  # source file path (set by review_contract; used for annotations)
    line: Optional[int] = None  # 1-based line of the offending element (best-effort; None → whole file)


class ContractReviewReport(BaseModel):
    contracts_scanned: int
    findings: List[ContractFinding]
    summary: Dict[str, int] = Field(default_factory=dict)

    @property
    def worst_severity(self) -> Optional[str]:
        if not self.findings:
            return None
        return max((f.severity for f in self.findings), key=lambda s: SEVERITY_RANK.get(s, 0))


# ── Field-name PII heuristic (conservative; ambiguous cases → LLM layer) ─────

_PII_RE = re.compile(
    r"(e[-_]?mail|phone|mobile|msisdn|ssn|social_security|national_id|passport|"
    r"date_of_birth|(^|_)dob($|_)|birth|home_address|street_address|postcode|"
    r"licence|license|iban|bank_account|account_number|sort_code|routing|"
    r"card_number|cvv|tax_id|first_name|last_name|full_name)",
    re.I,
)

_ENTITY_LAYERS = {"silver", "gold"}
_KEYED_STRATEGIES = {"merge", "scd2", "upsert"}


# ── Accessors over the raw authored dict ────────────────────────────────────


def _d(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _layer(raw: Dict[str, Any]) -> str:
    return (raw.get("tier") or raw.get("layer") or _d(raw.get("info")).get("target_layer") or "").lower()


def _fields(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    fs = _d(raw.get("model")).get("fields")
    return [f for f in fs if isinstance(f, dict)] if isinstance(fs, list) else []


def _strategy(raw: Dict[str, Any]) -> str:
    return (_d(raw.get("materialization")).get("strategy") or "append").lower()


def _dedup(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the dedup transform's inner config dict, if any."""
    for t in raw.get("transformations") or []:
        if not isinstance(t, dict):
            continue
        for k, v in t.items():
            if str(k).startswith("deduplicate") or k == "dedup":
                return _d(v)
    return None


def _has_key(raw: Dict[str, Any], dedup: Optional[Dict[str, Any]]) -> bool:
    if raw.get("primary_key") or raw.get("natural_key"):
        return True
    return bool(dedup and dedup.get("key_columns"))


def _has_delete_strategy(raw: Dict[str, Any]) -> bool:
    mat = _d(raw.get("materialization"))
    if mat.get("soft_delete_column") or _d(raw.get("soft_deletes")).get("enabled"):
        return True
    if (_d(raw.get("source")).get("load_mode") or "").lower() == "cdc":
        return True
    if mat.get("cdc_delete_values") or mat.get("cdc_op_field"):
        return True
    return bool(raw.get("deletion"))  # proposed snapshot_reconcile block


def _has_any_quality(raw: Dict[str, Any]) -> bool:
    q = _d(raw.get("quality"))
    if q.get("row_rules") or q.get("dataset_rules"):
        return True
    return any(f.get("rules") for f in _fields(raw))


def _service_levels(raw: Dict[str, Any]) -> Dict[str, Any]:
    return _d(raw.get("service_levels") or raw.get("service_level") or raw.get("slo"))


# ── Domain/system governance context ────────────────────────────────────────
# Requirements can be declared at the domain (`_domain.yaml`) or system
# (`_system.yaml`) level and inherited by contracts. Precedence:
#   contract  >  system  >  domain   (the contract supersedes; checked per-field).
# When a requirement is satisfied at domain/system level, the check does NOT flag
# it as missing; when the domain declares a POLICY (GDPR, erasure), the check
# escalates severity.


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base or {})
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


class GovernanceContext(BaseModel):
    """Resolved domain+system governance policy (domain <- system merge)."""

    policy: Dict[str, Any] = Field(default_factory=dict)

    def _slo(self) -> Dict[str, Any]:
        return _d(self.policy.get("slo"))

    def provides_freshness(self, layer: str) -> bool:
        return bool(_d(self._slo().get("freshness")).get(layer))

    def provides_row_count(self, layer: str) -> bool:
        return bool(_d(self._slo().get("row_count")).get(layer))

    def provides_slo(self, layer: str) -> bool:
        return self.provides_freshness(layer) or self.provides_row_count(layer)

    @property
    def pii_required(self) -> bool:
        comp = _d(self.policy.get("compliance"))
        if "pii" in (comp.get("risk_triggers") or []):
            return True
        return bool(_d(_d(comp.get("frameworks")).get("gdpr")).get("applicable"))

    @property
    def erasure_required(self) -> bool:
        return bool(_d(_d(self.policy.get("compliance")).get("erasure")).get("strategy"))


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        d = yaml.safe_load(path.read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def load_context(contract_path: str | Path, *, max_up: int = 8) -> Optional[GovernanceContext]:
    """Walk up from a contract file to the nearest ``_system.yaml`` and
    ``_domain.yaml``; return the merged policy (domain <- system), or None
    (standalone mode — no domain files found)."""
    domain_cfg: Dict[str, Any] = {}
    system_cfg: Dict[str, Any] = {}
    d = Path(contract_path).resolve().parent
    for _ in range(max_up):
        if not system_cfg and (d / "_system.yaml").exists():
            system_cfg = _load_yaml(d / "_system.yaml")
        if (d / "_domain.yaml").exists():
            domain_cfg = _load_yaml(d / "_domain.yaml")
            break
        if d.parent == d:
            break
        d = d.parent
    if not domain_cfg and not system_cfg:
        return None
    return GovernanceContext(policy=_deep_merge(domain_cfg, system_cfg))


# ── Checks ──────────────────────────────────────────────────────────────────


def _c(contract, cid, sev, cat, msg, *, field=None, suggestion=None):
    return ContractFinding(
        contract=contract, check_id=cid, severity=sev, category=cat, message=msg, field=field, suggestion=suggestion
    )


# Every check takes (raw, name, ctx). ctx is the resolved domain/system policy
# (or None in standalone mode). Checks consult it for inheritance + severity.


def check_pk_missing_for_mutation(raw, name, ctx):  # PK-002
    dedup = _dedup(raw)
    if (_strategy(raw) in _KEYED_STRATEGIES or dedup is not None) and not _has_key(raw, dedup):
        return [
            _c(
                name,
                "PK-002",
                "critical",
                "keys",
                f"strategy '{_strategy(raw)}'/dedup needs a key but none is declared — "
                "merge/upsert/dedup are non-deterministic without one.",
                suggestion="Add `primary_key: [<id>]` (or `key_columns` on the dedup).",
            )
        ]
    return []


def check_dedup_no_timestamp(raw, name, ctx):  # KEY-001
    dedup = _dedup(raw)
    if dedup is not None and not dedup.get("timestamp_column"):
        return [
            _c(
                name,
                "KEY-001",
                "warning",
                "keys",
                "deduplicate has no timestamp_column — 'which row wins' is non-deterministic.",
                suggestion="Add `timestamp_column: <updated_at>` to the dedup transform.",
            )
        ]
    return []


def check_untagged_pii(raw, name, ctx):  # PII-001
    out = []
    for f in _fields(raw):
        fname = f.get("name") or ""
        if _PII_RE.search(fname) and not f.get("pii") and not f.get("phi"):
            out.append(
                _c(
                    name,
                    "PII-001",
                    "warning",
                    "pii",
                    f"field '{fname}' looks like PII but isn't tagged `pii: true`.",
                    field=fname,
                    suggestion=f"Tag `pii: true` + a `masking:` strategy on '{fname}', or confirm it's not PII.",
                )
            )
    return out


def check_pii_no_masking(raw, name, ctx):  # PII-002
    # Escalate to critical when the domain mandates PII protection (GDPR / risk_triggers=pii).
    sev = "critical" if (ctx and ctx.pii_required) else "warning"
    out = []
    for f in _fields(raw):
        if f.get("pii") and not f.get("masking") and not f.get("security_groups"):
            fname = f.get("name")
            extra = " (domain compliance mandates PII masking)" if sev == "critical" else ""
            out.append(
                _c(
                    name,
                    "PII-002",
                    sev,
                    "pii",
                    f"PII field '{fname}' has no masking strategy — it will surface unmasked.{extra}",
                    field=fname,
                    suggestion="Add `masking: hash|redact|partial|nullify` (or map security_groups).",
                )
            )
    return out


def check_no_delete_strategy(raw, name, ctx):  # DEL-001
    dedup = _dedup(raw)
    is_entity = (
        _layer(raw) in _ENTITY_LAYERS
        and _has_key(raw, dedup)
        and (_strategy(raw) in _KEYED_STRATEGIES or dedup is not None)
    )
    if is_entity and not _has_delete_strategy(raw):
        sev = "critical" if (ctx and ctx.erasure_required) else "warning"
        extra = " (domain declares an erasure policy — entities must support deletion)" if sev == "critical" else ""
        return [
            _c(
                name,
                "DEL-001",
                sev,
                "deletes",
                f"current-state entity with a key but no delete strategy — hard deletes "
                f"at source go undetected (stale rows persist).{extra}",
                suggestion="Declare `soft_deletes: {enabled: true}` + CDC, or "
                "`deletion: {strategy: snapshot_reconcile, ...}`.",
            )
        ]
    return []


def check_no_quality(raw, name, ctx):  # QLT-001 — Silver only (the enforcement layer)
    # Silver is where raw data is validated against rules. Gold is derived from
    # already-validated Silver, so row-level quality rules are NOT expected there.
    if _layer(raw) == "silver" and not _has_any_quality(raw):
        return [
            _c(
                name,
                "QLT-001",
                "warning",
                "quality",
                "silver table has no quality rules — Silver is the enforcement layer; "
                "with no rules nothing is validated and quarantine can never fire.",
                suggestion="Add `quality.row_rules` / `dataset_rules` (or field-level `rules`).",
            )
        ]
    return []


def check_scd2_no_track_columns(raw, name, ctx):  # SCD-001
    if _strategy(raw) == "scd2":
        scd2 = _d(_d(raw.get("materialization")).get("scd2"))
        if not scd2.get("track_columns"):
            return [
                _c(
                    name,
                    "SCD-001",
                    "critical",
                    "materialization",
                    "SCD2 with no track_columns — the engine cuts a NEW version on every load (version churn), "
                    "instead of only when a tracked attribute changes; the dimension's history becomes meaningless.",
                    suggestion="List the attributes under `materialization.scd2.track_columns`.",
                )
            ]
    return []


def check_unpartitioned_landing(raw, name, ctx):  # SRC-001 — Bronze only (landing ingest)
    # Landing zones are a Bronze concern; Silver/Gold read from upstream Delta.
    if _layer(raw) != "bronze":
        return []
    s = _d(raw.get("source"))
    if (s.get("type") or "").lower() == "landing" and not s.get("partition"):
        return [
            _c(
                name,
                "SRC-001",
                "warning",
                "source",
                "bronze landing source is unpartitioned — every load rescans the whole zone (no incremental pruning).",
                suggestion="Add `source.partition.format` (e.g. y_%Y/m_%m/d_%d/h_%H).",
            )
        ]
    return []


def check_no_volume_freshness_slo(raw, name, ctx):  # VOL-001
    layer = _layer(raw)
    if layer not in _ENTITY_LAYERS:
        return []
    sl = _service_levels(raw)
    if sl.get("freshness") or sl.get("row_count"):
        return []  # contract declares it → supersedes
    if ctx and ctx.provides_slo(layer):
        return []  # inherited from the domain/system
    return [
        _c(
            name,
            "VOL-001",
            "info",
            "reliability",
            f"{layer} table has no freshness or volume SLO (contract or domain) — "
            "blind to stalled feeds / missing data.",
            suggestion="Add contract `service_levels.freshness`/`row_count`, or an `slo.*` block at the domain.",
        )
    ]


_CHECKS: List[Callable[[Dict[str, Any], str, Optional[GovernanceContext]], List[ContractFinding]]] = [
    check_pk_missing_for_mutation,
    check_dedup_no_timestamp,
    check_untagged_pii,
    check_pii_no_masking,
    check_no_delete_strategy,
    check_no_quality,
    check_scd2_no_track_columns,
    check_unpartitioned_landing,
    check_no_volume_freshness_slo,
]


# ── Public API ──────────────────────────────────────────────────────────────


def review_contract_dict(
    raw: Dict[str, Any], name: str, ctx: Optional[GovernanceContext] = None
) -> List[ContractFinding]:
    """Run every check against a parsed contract dict + optional resolved domain
    policy. This is also the *critic-mode* entry point (pass a proposed diff's dict)."""
    findings: List[ContractFinding] = []
    for check in _CHECKS:
        try:
            findings.extend(check(raw, name, ctx))
        except Exception:  # a check bug must never crash the lint
            continue
    return findings


def review_contract(path: str | Path, ctx: Optional[GovernanceContext] = None) -> List[ContractFinding]:
    """Lint one contract file. Resolves its domain/system policy automatically
    (walks up to `_domain.yaml`/`_system.yaml`) unless a ``ctx`` is supplied."""
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as e:
        return [_c(path.stem, "PARSE-000", "critical", "parse", f"could not read YAML: {e}")]
    if not isinstance(raw, dict):
        return [_c(path.stem, "PARSE-000", "critical", "parse", "top-level YAML is not a mapping.")]
    if ctx is None:
        ctx = load_context(path)
    findings = review_contract_dict(raw, path.stem, ctx)
    fpath = str(path)
    for f in findings:
        f.file = fpath
    return findings


# ── Renderers ───────────────────────────────────────────────────────────────

_GH_LEVEL = {"critical": "error", "warning": "warning", "info": "notice"}


def _gh_escape(s: str) -> str:
    return s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def render_github(report: "ContractReviewReport") -> str:
    """Render findings as GitHub Actions workflow commands (inline PR annotations).

    Emits one ``::error|warning|notice file=...,line=...,title=...::message`` per
    finding, then a plain summary line. Anchors to the file (and line, when known).
    """
    lines: List[str] = []
    for f in report.findings:
        level = _GH_LEVEL.get(f.severity, "notice")
        props = []
        if f.file:
            props.append(f"file={f.file}")
            props.append(f"line={f.line or 1}")
        title = f.check_id + (f":{f.field}" if f.field else "")
        props.append(f"title={_gh_escape(title).replace(',', '%2C')}")
        msg = _gh_escape(f.message + (f"  → {f.suggestion}" if f.suggestion else ""))
        lines.append(f"::{level} {','.join(props)}::{msg}")
    s = report.summary
    lines.append(
        f"Contract governance: {report.contracts_scanned} contract(s) · "
        f"{s.get('critical', 0)} critical · {s.get('warning', 0)} warning · {s.get('info', 0)} info"
    )
    return "\n".join(lines)


def gate_severity(report: "ContractReviewReport", *, rules_only: bool = True) -> Optional[str]:
    """The worst severity that should gate CI. By default only ``source='rules'``
    findings count — LLM judgment findings are advisory and never block."""
    pool = [f for f in report.findings if (not rules_only or f.source == "rules")]
    if not pool:
        return None
    return max((f.severity for f in pool), key=lambda s: SEVERITY_RANK.get(s, 0))


def iter_contract_files(paths: List[str | Path]) -> List[Path]:
    """Expand files/dirs into a sorted, de-duplicated list of contract YAML files."""
    files: List[Path] = []
    for p in paths:
        p = Path(p)
        if p.is_dir():
            files.extend(sorted(set(p.rglob("*.yaml")) | set(p.rglob("*.yml"))))
        elif p.exists():
            files.append(p)
    return files


def review_paths(paths: List[str | Path]) -> ContractReviewReport:
    """Lint one or more files/directories (recursively finds *.yaml / *.yml)."""
    files = iter_contract_files(paths)
    all_findings: List[ContractFinding] = []
    for f in files:
        all_findings.extend(review_contract(f))
    summary: Dict[str, int] = {"critical": 0, "warning": 0, "info": 0}
    for fnd in all_findings:
        summary[fnd.severity] = summary.get(fnd.severity, 0) + 1
    return ContractReviewReport(contracts_scanned=len(files), findings=all_findings, summary=summary)
