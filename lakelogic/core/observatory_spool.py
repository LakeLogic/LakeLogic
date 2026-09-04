"""Local spool + bounded retry for Observatory telemetry pushes.

The Observatory push (see ``run_log.py``) is fire-and-forget. Without a buffer,
any transient SaaS outage, network blip, timeout, or ``5xx``/``429`` silently
drops that run's telemetry — it's at-most-once and one event per run, so a brief
SaaS deploy punches a permanent hole in the customer's estate-health views.

The SaaS ingest endpoint is idempotent on ``run_id``, so re-sending is always
safe. This module persists failed pushes to a small, bounded local spool and
replays them on a later run once the SaaS is reachable again.

Design constraints (all deliberate):
  * **Never raises** — telemetry must never break or block a pipeline. Every
    public function swallows its own errors.
  * **Bounded** — max file count + TTL so a long outage can't fill the disk.
  * **Time-budgeted flush** — draining the backlog can never add more than a few
    seconds to a run (stops on the first still-failing attempt, and on a wall
    clock budget).
  * **Privacy** — the rule-attribution list is stripped before anything is
    written to disk; only run metadata is spooled.
  * **Portable** — stdlib + ``requests`` (already a dependency). Works on local
    and Databricks job clusters; on ephemeral serverless filesystems, point
    ``spool.dir`` at persistent storage to survive container restarts.

Config (under the contract/registry ``observatory.spool`` block, all optional)::

    observatory:
      enabled: true
      endpoint: https://api.lakelogic.io/api/v1/operations/run-logs/ingest
      api_key: llc_sk_...
      spool:
        enabled: true          # default true when observatory is enabled
        dir: ~/.lakelogic/observatory_spool
        max_files: 500          # ring-buffer cap (oldest dropped)
        ttl_days: 7             # discard stale buffered logs
        batch: 20               # max files replayed per run
        max_seconds: 5.0        # wall-clock budget for a flush
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

try:
    import requests
except Exception:  # pragma: no cover - requests is a hard dep, but never explode
    requests = None  # type: ignore

_DEFAULT_DIR = "~/.lakelogic/observatory_spool"
_DEFAULT_MAX_FILES = 500
_DEFAULT_TTL_DAYS = 7
_DEFAULT_BATCH = 20
_DEFAULT_MAX_SECONDS = 5.0
_DEFAULT_TIMEOUT = 3.0

# ── Frictionless connection (env-var convenience) ────────────────────────────
# The hosted SaaS ingest endpoint. A user who sets LAKELOGIC_CLOUD_API_KEY (and
# nothing else) pushes here — no YAML, no endpoint to look up.
DEFAULT_CLOUD_ENDPOINT = "https://api.lakelogic.io/api/v1/operations/run-logs/ingest"
ENV_API_KEY = "LAKELOGIC_CLOUD_API_KEY"
ENV_ENDPOINT = "LAKELOGIC_CLOUD_ENDPOINT"

_ENV_REF = re.compile(r"\$\{(\w+)\}")


def _interpolate_env(value: str) -> str:
    """Expand ``${VAR}`` references from the environment. An unset var expands to
    empty, so a committed ``api_key: ${LAKELOGIC_CLOUD_API_KEY}`` is inert (and
    safe) until the env var is present — keeping secrets out of git."""
    return _ENV_REF.sub(lambda m: os.getenv(m.group(1), ""), value)


def resolve_observatory_config(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge env-var convenience settings into the YAML ``observatory`` block.

    Precedence is **explicit YAML > env vars**. The env path exists so a single
    ``export LAKELOGIC_CLOUD_API_KEY=llc_sk_…`` is enough to start pushing to the
    hosted control plane with zero YAML — the frictionless connection.

      * ``${VAR}`` in ``api_key`` / ``endpoint`` is expanded from the env.
      * ``LAKELOGIC_CLOUD_API_KEY`` fills a missing key (and enables push).
      * ``LAKELOGIC_CLOUD_ENDPOINT`` (or the hosted default) fills a missing endpoint.
      * An explicit ``enabled: false`` in YAML is always honored (never overridden).
    """
    cfg = dict(cfg) if isinstance(cfg, dict) else {}

    for k in ("api_key", "endpoint"):
        if isinstance(cfg.get(k), str):
            cfg[k] = _interpolate_env(cfg[k]).strip()

    # Track provenance: was the KEY supplied by env (the convenience path) rather
    # than declared in YAML? That's what distinguishes a frictionless connection.
    key_from_env = False
    env_key = os.getenv(ENV_API_KEY)
    if not cfg.get("api_key") and env_key:
        cfg["api_key"] = env_key.strip()
        key_from_env = True

    endpoint_from_env = False
    if not cfg.get("endpoint"):
        env_ep = os.getenv(ENV_ENDPOINT)
        if env_ep:
            cfg["endpoint"] = env_ep.strip()
            endpoint_from_env = True
        elif cfg.get("api_key"):
            cfg["endpoint"] = DEFAULT_CLOUD_ENDPOINT

    # Auto-enable when the connection was established via env (YAML `enabled` wins).
    if "enabled" not in cfg and (key_from_env or endpoint_from_env):
        cfg["enabled"] = True

    # Env-key connections default rule attribution ON — knowing WHICH rule failed
    # is what makes cloud diagnosis good. Explicit YAML users keep the metadata-
    # only default (False) unless they opt in. The list is capped and always
    # stripped before anything is written to the local spool.
    #
    # NB: this carries no customer data. It is built from the rule-annotation
    # columns only (see ChainProcessor._extract_row_rule_failures) — rule name,
    # SQL, category, count. Failing source rows are never captured or sent.
    if key_from_env and "include_quarantine_sample" not in cfg:
        cfg["include_quarantine_sample"] = True

    return cfg


def _is_retryable(status_code: int) -> bool:
    """Transient/server-side failures worth retrying. 4xx (except 408/429) means
    the request itself is bad (auth/payload) — retrying won't help."""
    return status_code in (408, 429) or status_code >= 500


def _spool_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    s = cfg.get("spool") if isinstance(cfg, dict) else None
    return s if isinstance(s, dict) else {}


def _enabled(cfg: Dict[str, Any]) -> bool:
    # Default-on when observatory is enabled; allow explicit opt-out.
    return bool(_spool_cfg(cfg).get("enabled", True))


def _dir(cfg: Dict[str, Any]) -> Path:
    raw = _spool_cfg(cfg).get("dir", _DEFAULT_DIR)
    return Path(os.path.expanduser(str(raw)))


def strip_for_spool(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Copy of ``payload`` safe to persist: the rule-attribution list is removed
    so the spool stays run-metadata only.

    The list holds no source rows — it never has — so this is size hygiene and
    defence in depth, not the control that keeps data local. What keeps data
    local is that failing rows are never collected in the first place."""
    safe = dict(payload)
    safe.pop("quarantined_rows", None)
    safe["_spooled"] = True
    return safe


def _enforce_caps(cfg: Dict[str, Any], d: Path) -> None:
    """Purge stale (TTL) and excess (ring-buffer) spool files. Best-effort."""
    ttl_days = int(_spool_cfg(cfg).get("ttl_days", _DEFAULT_TTL_DAYS))
    max_files = int(_spool_cfg(cfg).get("max_files", _DEFAULT_MAX_FILES))
    cutoff = time.time() - ttl_days * 86400

    files = sorted(d.glob("*.json"), key=lambda f: f.stat().st_mtime)
    # TTL purge
    for f in list(files):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                files.remove(f)
        except Exception:
            pass
    # Ring-buffer: keep newest `max_files` (leave room for one incoming write)
    excess = len(files) - max_files + 1
    for f in files[: max(0, excess)]:
        try:
            f.unlink()
        except Exception:
            pass


def spool_payload(cfg: Dict[str, Any], payload: Dict[str, Any]) -> bool:
    """Persist a failed push for later retry (rule attribution stripped).
    Returns True if buffered. Never raises."""
    try:
        if not _enabled(cfg):
            return False
        d = _dir(cfg)
        d.mkdir(parents=True, exist_ok=True)
        _enforce_caps(cfg, d)
        run_id = ""
        meta = payload.get("metadata")
        if isinstance(meta, dict):
            run_id = str(meta.get("run_id") or "")
        safe_run = "".join(c for c in run_id if c.isalnum() or c in "-_")[:40] or "run"
        name = f"{int(time.time())}_{safe_run}_{uuid.uuid4().hex[:8]}.json"
        tmp = d / (name + ".tmp")
        final = d / name
        tmp.write_text(json.dumps(strip_for_spool(payload)), encoding="utf-8")
        os.replace(tmp, final)  # atomic publish
        logger.info(f"📡 [spool] buffered run log for retry → {final.name}")
        return True
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(f"📡 [spool] could not buffer run log: {exc}")
        return False


def flush_spool(cfg: Dict[str, Any], endpoint: str, headers: Dict[str, str]) -> int:
    """Replay buffered run logs to ``endpoint``. Bounded by batch size and a
    wall-clock budget; stops on the first still-failing attempt so it never
    stalls a pipeline. Returns the number successfully replayed. Never raises.

    Call this only when the SaaS has just been confirmed reachable (e.g. right
    after a successful live push) so a still-down backend costs ~one attempt.
    """
    sent = 0
    try:
        if requests is None or not _enabled(cfg) or not endpoint:
            return 0
        d = _dir(cfg)
        if not d.exists():
            return 0
        _enforce_caps(cfg, d)
        batch = int(_spool_cfg(cfg).get("batch", _DEFAULT_BATCH))
        budget = float(_spool_cfg(cfg).get("max_seconds", _DEFAULT_MAX_SECONDS))
        timeout = float(_spool_cfg(cfg).get("timeout", _DEFAULT_TIMEOUT))
        deadline = time.time() + budget

        files = sorted(d.glob("*.json"), key=lambda f: f.stat().st_mtime)[:batch]
        for f in files:
            if time.time() > deadline:
                break
            try:
                payload = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                # Corrupt/partial file — drop it, don't get stuck.
                try:
                    f.unlink()
                except Exception:
                    pass
                continue
            try:
                resp = requests.post(endpoint, json=payload, headers=headers, timeout=timeout)
            except Exception:
                break  # still unreachable — keep remaining for next time
            if resp.status_code < 300:
                try:
                    f.unlink()
                except Exception:
                    pass
                sent += 1
            elif _is_retryable(resp.status_code):
                break  # backend still failing — stop, retry on a later run
            else:
                # 4xx (bad payload/auth) — won't ever succeed; drop it.
                try:
                    f.unlink()
                except Exception:
                    pass
        if sent:
            logger.info(f"📡 [spool] replayed {sent} buffered run log(s)")
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(f"📡 [spool] flush skipped: {exc}")
    return sent
