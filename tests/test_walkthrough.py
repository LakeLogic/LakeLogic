"""P6 tests: walkthrough module + summary integration."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lakelogic.ai.code_reviewer import ReviewFinding, run_review
from lakelogic.ai.review_formatters import _build_summary_markdown
from lakelogic.ai.walkthrough import (
    WalkthroughResult,
    generate_walkthrough,
    render_walkthrough_markdown,
)


# ---------------------------------------------------------------------------
# render_walkthrough_markdown
# ---------------------------------------------------------------------------


def test_render_walkthrough_includes_summary_and_highlights() -> None:
    w = WalkthroughResult(
        summary="Refactors the bronze ingestion path.",
        highlights=["pipeline/runner.py: parallel wave scheduler", "ai/code_reviewer.py: tier wiring"],
        mermaid="",
    )
    md = render_walkthrough_markdown(w)
    assert "📝 Walkthrough" in md
    assert "Refactors the bronze" in md
    assert "parallel wave scheduler" in md
    assert "```mermaid" not in md  # no mermaid block when empty


def test_render_walkthrough_includes_mermaid_when_present() -> None:
    w = WalkthroughResult(
        summary="Changes flow.",
        highlights=[],
        mermaid="sequenceDiagram\n    A->>B: hi",
    )
    md = render_walkthrough_markdown(w)
    assert "```mermaid" in md
    assert "sequenceDiagram" in md


def test_render_walkthrough_handles_empty_summary() -> None:
    md = render_walkthrough_markdown(WalkthroughResult())
    assert "no summary" in md


# ---------------------------------------------------------------------------
# generate_walkthrough — diff collection + LLM mocking
# ---------------------------------------------------------------------------


def test_generate_walkthrough_returns_none_when_diff_empty() -> None:
    """No diff content → no walkthrough."""
    with patch("lakelogic.ai.walkthrough._collect_diff", return_value=""):
        assert generate_walkthrough("main", provider="anthropic", model="m") is None


def test_generate_walkthrough_returns_none_when_diff_collection_fails() -> None:
    with patch("lakelogic.ai.walkthrough._collect_diff", return_value=None):
        assert generate_walkthrough("main", provider="anthropic", model="m") is None


def test_generate_walkthrough_returns_none_when_sdk_missing() -> None:
    """When instructor isn't installed, gracefully degrade (no crash)."""
    with patch("lakelogic.ai.walkthrough._collect_diff", return_value="x" * 200):
        # instructor isn't installed in the test venv — _build_client raises
        result = generate_walkthrough("main", provider="anthropic", model="m")
        assert result is None


def test_generate_walkthrough_returns_result_when_llm_succeeds() -> None:
    """Happy path: mocked LLM returns a WalkthroughResult."""
    fake_result = WalkthroughResult(
        summary="Adds Tier 2 review.",
        highlights=["lakelogic/ai/llm_client.py: instructor wiring"],
        mermaid="",
    )
    fake_client = MagicMock()
    fake_client.chat.completions.create.return_value = fake_result

    with (
        patch("lakelogic.ai.walkthrough._collect_diff", return_value="diff --git a/foo b/foo\n..." * 50),
        patch("lakelogic.ai.walkthrough._build_client", return_value=fake_client),
    ):
        result = generate_walkthrough("main", provider="anthropic", model="claude-sonnet-4-6")

    assert result is not None
    assert result.summary == "Adds Tier 2 review."
    assert "instructor wiring" in result.highlights[0]


# ---------------------------------------------------------------------------
# Orchestrator integration — walkthrough flag + report population
# ---------------------------------------------------------------------------


def test_run_review_skips_walkthrough_without_base_ref(tmp_path: Path) -> None:
    """Walkthrough requires --diff (base_ref) — None means skip."""
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")
    with (
        patch("lakelogic.ai.llm_client.review_batch", return_value=([], {"total": 0})),
        patch("lakelogic.ai.walkthrough.generate_walkthrough") as mock_wt,
    ):
        report = run_review(
            [f],
            api_key_present=True,
            provider="anthropic",
            model="m",
            base_ref=None,
        )
    mock_wt.assert_not_called()
    assert report.walkthrough is None


def test_run_review_skips_walkthrough_when_flag_off(tmp_path: Path) -> None:
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")
    with (
        patch("lakelogic.ai.llm_client.review_batch", return_value=([], {"total": 0})),
        patch("lakelogic.ai.walkthrough.generate_walkthrough") as mock_wt,
    ):
        run_review(
            [f],
            api_key_present=True,
            provider="anthropic",
            model="m",
            base_ref="main",
            walkthrough=False,
        )
    mock_wt.assert_not_called()


def test_run_review_populates_walkthrough_when_enabled(tmp_path: Path) -> None:
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")
    fake_wt = WalkthroughResult(summary="A change.", highlights=["x.py: tweak"], mermaid="")

    with (
        patch("lakelogic.ai.llm_client.review_batch", return_value=([], {"total": 0})),
        patch("lakelogic.ai.walkthrough.generate_walkthrough", return_value=fake_wt),
    ):
        report = run_review(
            [f],
            api_key_present=True,
            provider="anthropic",
            model="m",
            base_ref="main",
        )
    assert report.walkthrough is not None
    assert report.walkthrough["summary"] == "A change."


# ---------------------------------------------------------------------------
# Summary markdown rendering with walkthrough
# ---------------------------------------------------------------------------


def _report_with_wt(walkthrough: dict | None, findings: list | None = None) -> dict:
    return {
        "findings": findings or [],
        "summary": {"critical": 0, "warning": 0, "info": 0},
        "files_scanned": 1,
        "ai_provider": "anthropic",
        "ai_model": "m",
        "duration_seconds": 0.5,
        "token_usage": {"total": 100},
        "walkthrough": walkthrough,
    }


def test_summary_markdown_renders_walkthrough_section() -> None:
    md = _build_summary_markdown(
        _report_with_wt({"summary": "Did a thing.", "highlights": ["files.py: bits"], "mermaid": ""})
    )
    assert "📝 Walkthrough" in md
    assert "Did a thing." in md


def test_summary_markdown_no_walkthrough_section_when_absent() -> None:
    md = _build_summary_markdown(_report_with_wt(None))
    assert "Walkthrough" not in md


def test_summary_markdown_walkthrough_appears_above_findings() -> None:
    finding = ReviewFinding(
        file="x.py", line=1, severity="warning", category="x", rule="r", message="m"
    ).model_dump()
    md = _build_summary_markdown(
        _report_with_wt(
            {"summary": "S.", "highlights": [], "mermaid": ""},
            findings=[finding],
        )
    )
    assert md.index("Walkthrough") < md.index("Findings by file")
