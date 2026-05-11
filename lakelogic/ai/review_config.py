"""
lakelogic.ai.review_config
--------------------------
Loader for ``.lakelogic-review.toml`` and the precedence resolver
(CLI flag > config file > env-var auto-detect).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Optional

from loguru import logger
from pydantic import BaseModel, Field

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


_DEFAULT_MODELS = {
    "anthropic": "claude-sonnet-4-6",
    "openai": "gpt-4o",
}


class ReviewConfig(BaseModel):
    """Resolved review configuration."""

    provider: str = "none"
    model: str = "none"
    max_files: int = 50
    max_tokens_per_batch: int = 30_000
    fail_on: str = "critical"
    include: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)
    custom_rules: list[str] = Field(default_factory=list)
    severity_overrides: dict[str, str] = Field(default_factory=dict)
    api_key_present: bool = False


def _detect_env_provider() -> tuple[str, str, bool]:
    if os.environ.get("ANTHROPIC_API_KEY"):
        return ("anthropic", _DEFAULT_MODELS["anthropic"], True)
    if os.environ.get("OPENAI_API_KEY"):
        return ("openai", _DEFAULT_MODELS["openai"], True)
    return ("none", "none", False)


def _load_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        logger.warning(f"Failed to parse {path}: {e}; ignoring config file")
        return {}
    return data.get("review", {}) or {}


def load_config(
    config_path: Optional[Path] = None,
    *,
    cli_provider: Optional[str] = None,
    cli_model: Optional[str] = None,
    cli_fail_on: Optional[str] = None,
    cli_max_files: Optional[int] = None,
    cli_include: Optional[list[str]] = None,
    cli_exclude: Optional[list[str]] = None,
) -> ReviewConfig:
    """Resolve config from defaults, file, env, and CLI flags.

    Precedence (later wins): defaults < env-detect < file < CLI flags.
    """
    file_data = _load_toml(config_path or Path(".lakelogic-review.toml"))

    env_provider, env_model, key_present = _detect_env_provider()

    provider = cli_provider or file_data.get("provider") or env_provider
    if provider not in {"anthropic", "openai", "none"}:
        logger.warning(f"Unknown provider '{provider}', falling back to env detect")
        provider = env_provider

    model = cli_model or file_data.get("model") or _DEFAULT_MODELS.get(provider, "none")

    severity = file_data.get("severity") or {}
    if not isinstance(severity, dict):
        severity = {}

    return ReviewConfig(
        provider=provider,
        model=model,
        max_files=cli_max_files or file_data.get("max_files", 50),
        max_tokens_per_batch=file_data.get("max_tokens_per_batch", 30_000),
        fail_on=cli_fail_on or file_data.get("fail_on", "critical"),
        include=list(cli_include or file_data.get("include") or []),
        exclude=list(cli_exclude or file_data.get("exclude") or []),
        custom_rules=list(file_data.get("custom_rules") or []),
        severity_overrides={str(k): str(v) for k, v in severity.items()},
        api_key_present=key_present,
    )


def render_check_config(cfg: ReviewConfig) -> str:
    """Human-readable dump of the resolved config. Never prints key values."""
    lines = [
        "lakelogic review — resolved configuration",
        "─" * 50,
        f"  provider              {cfg.provider}",
        f"  model                 {cfg.model}",
        f"  api_key_present       {'yes' if cfg.api_key_present else 'no'}",
        f"  fail_on               {cfg.fail_on}",
        f"  max_files             {cfg.max_files}",
        f"  max_tokens_per_batch  {cfg.max_tokens_per_batch}",
        f"  include               {cfg.include or '(none)'}",
        f"  exclude               {cfg.exclude or '(none)'}",
        f"  custom_rules          {len(cfg.custom_rules)} rule(s)",
        f"  severity_overrides    {cfg.severity_overrides or '(none)'}",
    ]
    return "\n".join(lines)
