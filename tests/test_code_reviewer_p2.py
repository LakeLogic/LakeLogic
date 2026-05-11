"""P2 tests: config loader, prompt assembly, severity overrides, mocked LLM."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lakelogic.ai.code_reviewer import (
    ReviewFinding,
    _apply_severity_overrides,
    run_review,
)
from lakelogic.ai.review_config import load_config, render_check_config
from lakelogic.ai.review_prompts import build_review_prompt


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------


def test_config_defaults_when_no_file_no_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    cfg = load_config(tmp_path / "missing.toml")
    assert cfg.provider == "none"
    assert cfg.api_key_present is False
    assert cfg.fail_on == "critical"
    assert cfg.max_files == 50


def test_config_env_detect_anthropic(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    cfg = load_config(tmp_path / "missing.toml")
    assert cfg.provider == "anthropic"
    assert cfg.model == "claude-sonnet-4-6"
    assert cfg.api_key_present is True


def test_config_env_detect_openai_when_no_anthropic(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    cfg = load_config(tmp_path / "missing.toml")
    assert cfg.provider == "openai"


def test_config_file_overrides_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    f = tmp_path / ".lakelogic-review.toml"
    f.write_text(
        "[review]\n"
        'provider = "openai"\n'
        'model = "gpt-4o-mini"\n'
        'fail_on = "warning"\n'
        'custom_rules = ["No SELECT * in marts"]\n'
        "\n"
        "[review.severity]\n"
        'select_star = "info"\n'
    )
    cfg = load_config(f)
    assert cfg.provider == "openai"
    assert cfg.model == "gpt-4o-mini"
    assert cfg.fail_on == "warning"
    assert cfg.custom_rules == ["No SELECT * in marts"]
    assert cfg.severity_overrides == {"select_star": "info"}


def test_config_cli_overrides_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    f = tmp_path / ".lakelogic-review.toml"
    f.write_text('[review]\nprovider = "openai"\nfail_on = "warning"\n')
    cfg = load_config(f, cli_provider="anthropic", cli_fail_on="critical")
    assert cfg.provider == "anthropic"
    assert cfg.fail_on == "critical"


def test_config_render_never_prints_key(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-DO-NOT-LEAK")
    cfg = load_config(tmp_path / "missing.toml")
    rendered = render_check_config(cfg)
    assert "sk-DO-NOT-LEAK" not in rendered
    assert "api_key_present" in rendered
    assert "yes" in rendered


# ---------------------------------------------------------------------------
# Prompt assembly — Tier 1 findings show up in the user prompt
# ---------------------------------------------------------------------------


def test_build_prompt_includes_tier1_findings_section() -> None:
    files = [{"path": "a.py", "type": "python", "content": "x = 1"}]
    tier1 = [
        {"file": "a.py", "line": 1, "rule": "ruff_e501", "message": "line too long"},
    ]
    prompt = build_review_prompt(files, tier1_findings=tier1)
    assert "Already-Reported Tier 1 Findings (1)" in prompt
    assert "ruff_e501" in prompt
    assert "Do not duplicate" in prompt


def test_build_prompt_omits_tier1_section_when_empty() -> None:
    prompt = build_review_prompt([{"path": "a.py", "type": "python", "content": "x = 1"}])
    assert "Tier 1 Findings" not in prompt


def test_build_prompt_includes_custom_rules() -> None:
    prompt = build_review_prompt(
        [{"path": "a.py", "type": "python", "content": "x"}],
        custom_rules=["No bare excepts"],
    )
    assert "Additional Project-Specific Rules" in prompt
    assert "No bare excepts" in prompt


# ---------------------------------------------------------------------------
# Severity overrides
# ---------------------------------------------------------------------------


def test_severity_overrides_remap_known_rule() -> None:
    findings = [
        ReviewFinding(file="a", severity="warning", category="sql", rule="select_star", message="m"),
        ReviewFinding(file="a", severity="critical", category="sec", rule="hardcoded_secret", message="m"),
    ]
    out = _apply_severity_overrides(findings, {"select_star": "info"})
    assert out[0].severity == "info"
    assert out[1].severity == "critical"  # untouched


def test_severity_overrides_ignore_invalid_value() -> None:
    findings = [ReviewFinding(file="a", severity="warning", category="x", rule="r", message="m")]
    out = _apply_severity_overrides(findings, {"r": "BOGUS"})
    assert out[0].severity == "warning"


def test_severity_overrides_noop_when_empty() -> None:
    findings = [ReviewFinding(file="a", severity="warning", category="x", rule="r", message="m")]
    assert (
        _apply_severity_overrides(findings, {}) is findings
        or _apply_severity_overrides(findings, {})[0].severity == "warning"
    )


# ---------------------------------------------------------------------------
# Tier 2 wiring — mocked LLM client
# ---------------------------------------------------------------------------


def test_run_review_calls_llm_when_key_present(tmp_path: Path) -> None:
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")
    fake_finding = ReviewFinding(
        file=str(f), severity="warning", category="python_quality", rule="from_llm", message="LLM said so"
    )
    with patch("lakelogic.ai.llm_client.review_batch", return_value=([fake_finding], {"total": 123})) as mock_call:
        report = run_review(
            [f],
            api_key_present=True,
            provider="anthropic",
            model="claude-sonnet-4-6",
        )
    mock_call.assert_called_once()
    assert any(x.rule == "from_llm" for x in report.findings)
    assert report.token_usage["total"] == 123


def test_run_review_passes_tier1_findings_into_llm_call(tmp_path: Path) -> None:
    f = tmp_path / "leak.py"
    f.write_text("EMAIL = 'alice@company.com'\n")  # triggers PII Tier 1 finding
    captured: dict = {}

    def fake_review_batch(files, **kwargs):
        captured.update(kwargs)
        return ([], {"total": 0})

    with patch("lakelogic.ai.llm_client.review_batch", side_effect=fake_review_batch):
        run_review([f], api_key_present=True, provider="anthropic", model="claude-sonnet-4-6")

    assert "tier1_findings" in captured
    assert any(t["rule"] == "pii_email" for t in captured["tier1_findings"])


def test_run_review_no_llm_skips_call_even_with_key(tmp_path: Path) -> None:
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")
    with patch("lakelogic.ai.llm_client.review_batch") as mock_call:
        report = run_review([f], no_llm=True, api_key_present=True, provider="anthropic", model="m")
    mock_call.assert_not_called()
    assert not any(x.rule == "llm_review_skipped" for x in report.findings)


# ---------------------------------------------------------------------------
# Token budget enforcement (no real LLM call needed)
# ---------------------------------------------------------------------------


def test_review_batch_skips_when_estimated_tokens_exceed_budget(tmp_path: Path) -> None:
    from lakelogic.ai.llm_client import review_batch

    big = tmp_path / "huge.py"
    big.write_text("x = 1\n" * 50_000)  # ~300KB → ~75K tokens at 4 chars/token

    findings, usage = review_batch([big], provider="anthropic", model="claude-sonnet-4-6", max_tokens=1000)
    assert any(x.rule == "batch_too_large" for x in findings)
    assert usage["total"] == 0


def test_review_batch_returns_empty_when_sdk_missing(tmp_path: Path) -> None:
    """When instructor isn't installed, gracefully degrade (no crash)."""
    from lakelogic.ai.llm_client import review_batch

    f = tmp_path / "x.py"
    f.write_text("x = 1\n")
    findings, usage = review_batch([f], provider="anthropic", model="claude-sonnet-4-6")
    # Without instructor installed, returns ([], {"total": 0}) and logs a warning
    assert findings == []
    assert usage["total"] == 0
