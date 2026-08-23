"""Pre-flight materialization validation.

The subset of contract checks that are **materialization prerequisites** — without
them the engine doesn't error, it silently produces the WRONG output:

  * PK-002  a merge / upsert / SCD2 / dedup with no key → non-deterministic writes
  * KEY-001 a dedup with no timestamp → the deterministic tie-break picks the winner,
            which is stable across runs but is not necessarily the latest row
  * SCD-001 an SCD2 with no track_columns → a new version every load (history churn)

The same validator runs at **two moments** (one definition, no drift):

  * the **pipeline** runs it *before materializing* a contract (fail fast, or warn),
  * the **PR gate** (`lakelogic validate`) runs it *at merge time* (shift-left).

Operates on the authored contract **dict** (like the lint) — it does not require a
runtime-resolved ``DataContract`` (which needs registry/run-log context an authored
contract in a PR won't have). At runtime the pipeline already holds the raw dict
(``contract_dict``); pass that.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from lakelogic.core.contract_lint import (
    ContractFinding,
    GovernanceContext,
    check_dedup_no_timestamp,  # KEY-001
    check_pk_missing_for_mutation,  # PK-002
    check_scd2_no_track_columns,  # SCD-001
)

# The materialization-correctness checks. Violating any of these means the
# pipeline will run "green" and produce WRONG data — so they are blockers.
MATERIALIZATION_CHECKS = [
    check_pk_missing_for_mutation,
    check_dedup_no_timestamp,
    check_scd2_no_track_columns,
]


class PreflightError(RuntimeError):
    """Raised when a contract fails pre-flight materialization validation."""

    def __init__(self, contract: str, findings: List[ContractFinding]):
        self.contract = contract
        self.findings = findings
        detail = "\n".join(f"  ✖ {f.check_id}: {f.message}" for f in findings)
        super().__init__(f"Contract '{contract}' cannot materialize correctly (pre-flight):\n{detail}")


def _as_raw(contract: Union[Dict[str, Any], str, Path, Any]) -> Dict[str, Any]:
    """Coerce dict | path | DataContract into the authored-dict shape the checks read."""
    if isinstance(contract, dict):
        return contract
    if isinstance(contract, (str, Path)):
        import yaml

        d = yaml.safe_load(Path(contract).read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else {}
    # DataContract (or similar) — best-effort; prefer passing the raw dict at runtime.
    if hasattr(contract, "model_dump"):
        try:
            return contract.model_dump(by_alias=True, exclude_none=True)
        except Exception:
            return contract.model_dump()
    return {}


def preflight_check(
    contract: Union[Dict[str, Any], str, Path, Any],
    name: str = "",
    ctx: Optional[GovernanceContext] = None,
) -> List[ContractFinding]:
    """Return the materialization-blocking findings for a contract (empty = OK).

    All findings are forced to ``critical`` — a violated prerequisite is a
    correctness failure, not a style nit.
    """
    raw = _as_raw(contract)
    nm = name or (raw.get("info") or {}).get("table_name") or "contract"
    out: List[ContractFinding] = []
    for check in MATERIALIZATION_CHECKS:
        try:
            out.extend(check(raw, nm, ctx))
        except Exception:
            continue
    for f in out:
        f.severity = "critical"
        f.category = "materialization"
    return out


def assert_preflight(
    contract: Union[Dict[str, Any], str, Path, Any],
    name: str = "",
    ctx: Optional[GovernanceContext] = None,
) -> None:
    """Raise :class:`PreflightError` if the contract can't materialize correctly.

    Used by the pipeline in strict mode. In non-strict mode call
    :func:`preflight_check` and log the findings instead of raising.
    """
    findings = preflight_check(contract, name, ctx)
    if findings:
        raise PreflightError(name or ((_as_raw(contract).get("info") or {}).get("table_name") or "contract"), findings)
