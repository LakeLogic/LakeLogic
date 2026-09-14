"""
Privacy-action execution evidence (``privacy_action_execution_event`` v1).

An erasure pass used to report through ``RemoteObserver`` - off by default, addressed by an env
var nothing sets, posting a body the platform has no handler for - and to write a JSON file to
the Databricks-only ``/Workspace/Shared/lakelogic_logs``. Nothing reached the platform, so its
GDPR Evidence page showed no erasure events whatever ran (Fabric, 2026-09-14).

This module builds one event per asset an action ran against and posts it to the platform's
privacy-evidence ingest, using the SAME observatory configuration (endpoint + ``X-API-Key``) the
run logs use.

THE HARD RULE: an event never carries a subject identifier or a PII value. Subject IDs are used
here only to compute a COUNT and a SHA-256 digest, then dropped; ``columns`` are column NAMES; a
request is correlated by an opaque ``case_ref``. :func:`validate_privacy_action_event` rejects an
event that would break this, and the platform rejects it again.

Emission is BEST-EFFORT: a platform that is down never blocks or fails the erasure itself.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlsplit, urlunsplit

from loguru import logger

PRIVACY_ACTION_EVENT_VERSION = "1.0"
INGEST_PATH = "/api/v1/compliance/privacy-actions/ingest"
#: Overrides the derived ingest URL.
ENV_PRIVACY_ENDPOINT = "LAKELOGIC_PRIVACY_EVIDENCE_ENDPOINT"

_ACTIONS = {"nullify", "hash", "tokenize", "delete", "redact"}
#: Keys that could carry a subject identifier or a personal value. Rejected ANYWHERE in an event.
_PROHIBITED_KEYS = {
    "subject_ids", "subject_id", "identifier_values", "patient_ids", "patient_id",
    "subjects", "values", "raw_values", "pii_values", "emails", "email",
}


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def privacy_ingest_endpoint(run_log_endpoint: Optional[str]) -> Optional[str]:
    """The privacy-evidence ingest URL for an observatory configured with a run-log endpoint.

    ``https://host/api/v1/operations/run-logs/ingest`` -> ``https://host/api/v1/compliance/
    privacy-actions/ingest``. ``LAKELOGIC_PRIVACY_EVIDENCE_ENDPOINT`` overrides it.
    """
    override = os.getenv(ENV_PRIVACY_ENDPOINT, "").strip()
    if override:
        return override
    if not run_log_endpoint:
        return None
    parts = urlsplit(run_log_endpoint.strip())
    if not parts.scheme or not parts.netloc:
        return None
    path = parts.path
    marker = path.find("/api/v1")
    prefix = path[:marker] if marker >= 0 else ""
    return urlunsplit((parts.scheme, parts.netloc, f"{prefix}{INGEST_PATH}", "", ""))


def validate_privacy_action_event(event: Dict[str, Any], *, subject_ids: Iterable[Any] = ()) -> None:
    """Raise ``ValueError`` if the event could carry a subject identifier or PII value."""

    def walk(node: Any, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if str(key).lower() in _PROHIBITED_KEYS:
                    raise ValueError(f"privacy action event must not carry '{path}{key}'")
                walk(value, f"{path}{key}.")
        elif isinstance(node, list):
            for item in node:
                walk(item, path)

    walk(event, "")
    ids = {str(v) for v in subject_ids if v is not None and str(v) != ""}
    if ids:
        text = json.dumps(event, default=str)
        leaked = sorted(i for i in ids if len(i) >= 3 and i in text)
        if leaked:
            raise ValueError(f"privacy action event contains {len(leaked)} subject identifier value(s)")
    if event.get("action") not in _ACTIONS:
        raise ValueError(f"unsupported privacy action {event.get('action')!r}")


def build_privacy_action_event(
    *,
    framework: str,
    action: str,
    dry_run: bool,
    contract_name: str,
    subject_column: str,
    subject_ids: Iterable[Any],
    columns: Iterable[str],
    rows_affected: int,
    case_ref: Optional[str] = None,
    status: str = "completed",
    tier: Optional[str] = None,
    domain: Optional[str] = None,
    system: Optional[str] = None,
    environment: Optional[str] = None,
    run_id: Optional[str] = None,
    engine: Optional[str] = None,
    started_at: Optional[datetime] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    """One validated v1 event. Subject IDs become a count and a digest here and go no further."""
    ids = sorted({str(v) for v in subject_ids if v is not None and str(v) != ""})
    mode = "dry_run" if dry_run else "executed"
    now = datetime.now(timezone.utc)
    event: Dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "schema_version": PRIVACY_ACTION_EVENT_VERSION,
        "emitted_at": now.isoformat(),
        "framework": framework,
        "case_ref": (case_ref or "").strip() or (f"dry-run-{run_id}" if dry_run else f"run-{run_id}"),
        "action": action,
        "mode": mode,
        "status": status,
        "asset": {"contract_name": contract_name, "tier": tier, "domain": domain,
                  "system": system, "environment": environment},
        "subject_column": subject_column,
        "columns": sorted({str(c) for c in columns}),
        "rows_affected": max(0, int(rows_affected or 0)),
        "subject_count": len(ids),
        "subjects_sha256": _sha256("\n".join(ids)) if ids else None,
        "pipeline_run": {"run_id": run_id, "engine": engine,
                         "started_at": started_at.isoformat() if started_at else None,
                         "finished_at": now.isoformat()},
    }
    if notes:
        event["notes"] = notes[:500]
    # TAMPER-EVIDENCE: a digest over what the event asserts, so a stored copy can be checked.
    asserted = {k: event[k] for k in ("framework", "case_ref", "action", "mode", "status", "asset",
                                      "subject_column", "columns", "rows_affected", "subject_count",
                                      "subjects_sha256")}
    event["verification"] = {
        "method": "row_count" if mode == "executed" else "none",
        "verified": mode == "executed" and status == "completed",
        "evidence_digest": _sha256(json.dumps(asserted, sort_keys=True, default=str)),
    }
    validate_privacy_action_event(event, subject_ids=ids)
    return event


def emit_privacy_action_events(
    registry: Any,
    events: List[Dict[str, Any]],
    *,
    observatory: Optional[Dict[str, Any]] = None,
    timeout: float = 15.0,
) -> int:
    """Post events to the platform. Returns how many were accepted. NEVER raises."""
    if not events:
        return 0
    try:
        import requests as _requests

        from .observatory_spool import resolve_observatory_config

        cfg = resolve_observatory_config(
            observatory if observatory is not None else getattr(registry, "observatory", None)
        )
        if not (cfg and cfg.get("enabled")):
            logger.info("Privacy evidence not sent: observatory is not enabled.")
            return 0
        endpoint = privacy_ingest_endpoint(cfg.get("endpoint"))
        if not endpoint:
            logger.warning("Privacy evidence not sent: observatory is enabled but has no endpoint.")
            return 0
        headers = {"Content-Type": "application/json"}
        if cfg.get("api_key"):
            headers["X-API-Key"] = cfg["api_key"]

        accepted = 0
        for event in events:
            try:
                validate_privacy_action_event(event)
            except ValueError as exc:
                logger.error(f"Privacy evidence dropped (would disclose subject data): {exc}")
                continue
            for attempt in (1, 2):
                try:
                    resp = _requests.post(endpoint, json=event, headers=headers, timeout=timeout)
                except Exception as exc:
                    if attempt == 2:
                        logger.warning(f"Privacy evidence not delivered ({type(exc).__name__}): {exc}")
                    continue
                if resp.status_code < 300:
                    accepted += 1
                else:
                    logger.warning(f"Platform rejected privacy evidence: {resp.status_code} {resp.text[:200]}")
                break
        logger.info(f"Privacy evidence: {accepted}/{len(events)} event(s) accepted by the platform.")
        return accepted
    except Exception as exc:  # emission must never fail the privacy action it records
        logger.warning(f"Privacy evidence emission failed: {exc}")
        return 0


__all__ = [
    "PRIVACY_ACTION_EVENT_VERSION",
    "build_privacy_action_event",
    "emit_privacy_action_events",
    "privacy_ingest_endpoint",
    "validate_privacy_action_event",
]
