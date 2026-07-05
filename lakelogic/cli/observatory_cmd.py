"""``lakelogic observatory`` — utilities for the SaaS telemetry (Observatory) link.

Currently exposes ``flush``: replay run logs that were buffered locally while the
SaaS ingest endpoint was unreachable (see ``core/observatory_spool.py``). Useful
after an outage to drain the backlog without waiting for the next pipeline run.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import typer
from loguru import logger

observatory_app = typer.Typer(
    name="observatory",
    help="SaaS telemetry (Observatory) utilities — e.g. replay buffered run logs.",
    no_args_is_help=True,
)


def _resolve_cfg(
    registry: Optional[Path],
    endpoint: Optional[str],
    api_key: Optional[str],
    spool_dir: Optional[Path],
) -> Dict[str, Any]:
    """Build the observatory config dict from a registry YAML, with CLI overrides."""
    cfg: Dict[str, Any] = {}
    if registry:
        try:
            from lakelogic.core.registry import DomainRegistry

            reg = DomainRegistry.from_yaml(str(registry))
            obs = getattr(reg, "observatory", None)
            if isinstance(obs, dict):
                cfg = dict(obs)
        except Exception as exc:
            raise typer.BadParameter(
                f"Could not read observatory config from registry '{registry}': {exc}"
            )
    if endpoint:
        cfg["endpoint"] = endpoint
    if api_key:
        cfg["api_key"] = api_key
    if spool_dir:
        spool = dict(cfg.get("spool") or {})
        spool["dir"] = str(spool_dir)
        cfg["spool"] = spool
    # Fold in env-var convenience config (${VAR} + LAKELOGIC_CLOUD_* one-liner),
    # so `flush`/`status` connect the same way a pipeline run does.
    from lakelogic.core.observatory_spool import resolve_observatory_config

    return resolve_observatory_config(cfg)


def _mask(secret: str) -> str:
    if not secret:
        return "—"
    return (secret[:6] + "…" + secret[-4:]) if len(secret) > 12 else "set"


@observatory_app.command("status")
def status(
    registry: Optional[Path] = typer.Option(
        None, "--registry", "-r",
        help="Registry YAML to read observatory settings from (optional; env vars alone work).",
    ),
    endpoint: Optional[str] = typer.Option(None, "--endpoint", help="Override the ingest endpoint URL."),
    api_key: Optional[str] = typer.Option(None, "--api-key", help="Override the API key."),
    spool_dir: Optional[Path] = typer.Option(None, "--spool-dir", help="Override the local spool directory."),
    check: bool = typer.Option(
        False, "--check", help="Also probe the endpoint host for reachability (no data sent)."
    ),
) -> None:
    """Report whether this runtime is connected to the Observatory — the
    'is my pipe connected?' one-liner. Shows the resolved endpoint, whether an
    API key is present (masked), the buffered-spool depth, and (with --check) a
    reachability probe. Reads the same env-var config a pipeline run does, so
    running it after ``export LAKELOGIC_CLOUD_API_KEY=…`` confirms the link."""
    from lakelogic.core import observatory_spool as sp

    cfg = _resolve_cfg(registry, endpoint, api_key, spool_dir)
    endpoint_url = cfg.get("endpoint")
    key = cfg.get("api_key")
    connected = bool(endpoint_url and key and cfg.get("enabled", bool(key)))

    typer.echo("LakeLogic Observatory — connection status")
    typer.echo(f"  connected : {'yes' if connected else 'no'}")
    typer.echo(f"  endpoint  : {endpoint_url or '— (set LAKELOGIC_CLOUD_ENDPOINT or observatory.endpoint)'}")
    typer.echo(f"  api key   : {_mask(key)}")
    typer.echo(f"  enabled   : {cfg.get('enabled', bool(key))}")

    d = sp._dir(cfg)
    buffered = len(list(d.glob('*.json'))) if d.exists() else 0
    typer.echo(f"  spool     : {buffered} buffered run log(s) at {d}")

    if not connected:
        typer.echo(
            "\nNot connected. The fastest path: mint an operations:ingest key in the "
            "app (Settings → Telemetry) and run:\n  export LAKELOGIC_CLOUD_API_KEY=llc_sk_…"
        )

    if check and endpoint_url:
        try:
            import requests as _rq
            from urllib.parse import urlsplit

            base = urlsplit(endpoint_url)
            host = f"{base.scheme}://{base.netloc}"
            r = _rq.head(host, timeout=5.0)
            typer.echo(f"  reach     : {host} → HTTP {r.status_code}")
        except Exception as exc:
            typer.echo(f"  reach     : unreachable ({type(exc).__name__})")

    if buffered:
        typer.echo("\nBuffered logs are waiting — run `lakelogic observatory flush` to drain them.")


@observatory_app.command("flush")
def flush(
    registry: Optional[Path] = typer.Option(
        None, "--registry", "-r",
        help="Registry YAML to read observatory endpoint/api_key/spool settings from.",
    ),
    endpoint: Optional[str] = typer.Option(
        None, "--endpoint", help="Override the ingest endpoint URL."
    ),
    api_key: Optional[str] = typer.Option(
        None, "--api-key", help="Override the API key (sent as X-API-Key)."
    ),
    spool_dir: Optional[Path] = typer.Option(
        None, "--spool-dir", help="Override the local spool directory."
    ),
) -> None:
    """Replay locally-buffered run logs to the Observatory ingest endpoint.

    Run logs are spooled to disk when a push fails during a pipeline run. This
    drains that backlog on demand (e.g. after a SaaS outage). Re-sends are
    idempotent on the SaaS side, so this is always safe to run.
    """
    from lakelogic.core import observatory_spool as sp

    cfg = _resolve_cfg(registry, endpoint, api_key, spool_dir)
    endpoint_url = cfg.get("endpoint")
    if not endpoint_url:
        raise typer.BadParameter(
            "No endpoint. Pass --endpoint, or --registry pointing at a YAML with "
            "observatory.endpoint set."
        )

    headers = {"Content-Type": "application/json"}
    if cfg.get("api_key"):
        headers["X-API-Key"] = cfg["api_key"]

    # Manual drain: large batch + generous budget (vs the small, time-boxed flush
    # that runs inline at pipeline end), looped until nothing more is sent.
    spool = dict(cfg.get("spool") or {})
    spool.setdefault("batch", 1000)
    spool.setdefault("max_seconds", 60.0)
    cfg["spool"] = spool

    total = 0
    while True:
        sent = sp.flush_spool(cfg, endpoint_url, headers)
        total += sent
        if sent == 0:
            break

    d = sp._dir(cfg)
    remaining = len(list(d.glob("*.json"))) if d.exists() else 0
    typer.echo(f"Replayed {total} buffered run log(s); {remaining} still buffered.")
    if remaining:
        typer.echo(
            "Some logs remain buffered — the endpoint may still be unreachable. "
            "Re-run this command once it's back."
        )
