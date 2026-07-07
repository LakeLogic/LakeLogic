"""Targeted tests for registry.py branches that the existing suite misses.

Focus is on small, testable branches: validators, error paths, and the
helpers used during system-level config merging. Heavy contract-loading
paths (which require fixture YAML on disk + a registry directory layout)
are intentionally left out; they're already exercised by the integration
tests and the marginal coverage gain doesn't justify the fixture overhead.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from lakelogic.core.registry import (
    CloudReporting,
    DomainRegistry,
    EnvironmentConfig,
    _deep_merge,
    _resolve_env_or_secret,
    _resolve_placeholders,
)


# ---------------------------------------------------------------------------
# _resolve_env_or_secret
# ---------------------------------------------------------------------------


def test_resolve_env_or_secret_passes_through_non_string() -> None:
    """Non-string inputs return as-is (line 130 branch)."""
    assert _resolve_env_or_secret(42) == 42  # type: ignore[arg-type]
    assert _resolve_env_or_secret(None) is None  # type: ignore[arg-type]
    assert _resolve_env_or_secret(["a"]) == ["a"]  # type: ignore[arg-type]


def test_resolve_env_or_secret_resolves_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAKELOGIC_TEST_VAR", "resolved")
    assert _resolve_env_or_secret("${LAKELOGIC_TEST_VAR}") == "resolved"


def test_resolve_env_or_secret_returns_empty_for_missing_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LAKELOGIC_NEVER_SET", raising=False)
    assert _resolve_env_or_secret("${LAKELOGIC_NEVER_SET}") == ""


def test_resolve_env_or_secret_returns_plain_string_unchanged() -> None:
    assert _resolve_env_or_secret("plain-value") == "plain-value"


# ---------------------------------------------------------------------------
# CloudReporting / EnvironmentConfig validators
# ---------------------------------------------------------------------------


def test_cloud_reporting_validator_passes_through_falsy() -> None:
    """Empty string / None should bypass env resolution (line 195 branch)."""
    cr = CloudReporting()
    assert cr.api_key is None
    assert cr.report_url is None

    cr2 = CloudReporting(api_key="", report_url="")
    assert cr2.api_key == ""
    assert cr2.report_url == ""


def test_environment_config_validator_passes_through_non_dict() -> None:
    """When pydantic passes a non-dict (e.g. an instance) the validator returns it (line 210)."""
    cfg = EnvironmentConfig(catalog="main")
    # Re-validate from the model itself — Pydantic passes the model, not a dict
    cfg2 = EnvironmentConfig.model_validate(cfg)
    assert cfg2.catalog == "main"


def test_environment_config_resolves_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAKELOGIC_TEST_CATALOG", "prod_catalog")
    cfg = EnvironmentConfig.model_validate({"catalog": "${LAKELOGIC_TEST_CATALOG}", "region": "eu-west"})
    assert cfg.catalog == "prod_catalog"
    assert cfg.region == "eu-west"


def test_environment_config_keeps_non_template_values_unchanged() -> None:
    cfg = EnvironmentConfig.model_validate({"catalog": "literal", "storage_account": "acct"})
    assert cfg.catalog == "literal"
    assert cfg.storage_account == "acct"


# ---------------------------------------------------------------------------
# _deep_merge
# ---------------------------------------------------------------------------


def test_deep_merge_override_wins_on_scalar_conflict() -> None:
    base = {"a": 1, "b": 2}
    override = {"a": 99}
    assert _deep_merge(base, override) == {"a": 99, "b": 2}


def test_deep_merge_lists_replaced_not_concatenated() -> None:
    """Documented behaviour: lists are replaced, not appended."""
    assert _deep_merge({"x": [1, 2]}, {"x": [3]}) == {"x": [3]}


def test_deep_merge_nested_dicts_recursively_merged() -> None:
    base = {"outer": {"inner": {"a": 1, "b": 2}}}
    override = {"outer": {"inner": {"b": 99, "c": 3}}}
    assert _deep_merge(base, override) == {"outer": {"inner": {"a": 1, "b": 99, "c": 3}}}


def test_deep_merge_does_not_mutate_inputs() -> None:
    base = {"a": {"b": 1}}
    override = {"a": {"c": 2}}
    _deep_merge(base, override)
    assert base == {"a": {"b": 1}}
    assert override == {"a": {"c": 2}}


# ---------------------------------------------------------------------------
# _resolve_placeholders
# ---------------------------------------------------------------------------


def test_resolve_placeholders_leaves_unknown_keys_intact() -> None:
    out = _resolve_placeholders({"path": "{known}/{unknown}"}, {"known": "X"})
    assert out == {"path": "X/{unknown}"}


def test_resolve_placeholders_walks_nested_lists_and_dicts() -> None:
    obj = {"a": ["{x}", {"b": "{x}"}], "c": "literal"}
    assert _resolve_placeholders(obj, {"x": "Y"}) == {"a": ["Y", {"b": "Y"}], "c": "literal"}


def test_resolve_placeholders_passes_through_non_string_scalars() -> None:
    assert _resolve_placeholders(42, {"x": "y"}) == 42
    assert _resolve_placeholders(None, {"x": "y"}) is None
    assert _resolve_placeholders(True, {"x": "y"}) is True


def test_resolve_placeholders_no_op_on_string_without_braces() -> None:
    assert _resolve_placeholders("plain", {"x": "y"}) == "plain"


# ---------------------------------------------------------------------------
# DomainRegistry.from_yaml — error paths
# ---------------------------------------------------------------------------


def test_from_yaml_raises_on_missing_file(tmp_path: Path) -> None:
    """Line 430: FileNotFoundError when the registry path doesn't exist."""
    missing = tmp_path / "nope.yaml"
    with pytest.raises(FileNotFoundError, match="Registry not found"):
        DomainRegistry.from_yaml(missing)


def test_from_yaml_loads_minimal_registry(tmp_path: Path) -> None:
    """Smoke: a minimal valid registry parses without error."""
    reg_path = tmp_path / "_system.yaml"
    reg_path.write_text(
        yaml.safe_dump(
            {
                "domain": "sales",
                "system": "crm",
                "storage": {"catalog": "main"},
                "contracts": [],
            }
        )
    )
    reg = DomainRegistry.from_yaml(reg_path)
    assert reg.domain == "sales"
    assert reg.system == "crm"
    assert reg.contracts == []


def test_from_yaml_handles_storage_placeholder_keyerror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Line 580-581: KeyError during storage placeholder format → warning, no crash."""
    reg_path = tmp_path / "_system.yaml"
    # Deliberately reference an unknown placeholder in a storage field
    reg_path.write_text(
        yaml.safe_dump(
            {
                "domain": "sales",
                "system": "crm",
                "storage": {
                    "catalog": "main",
                    "landing_root": "{nonexistent_var}/landing",
                },
                "contracts": [],
            }
        )
    )
    # Should not raise — should warn and leave the unresolved value
    reg = DomainRegistry.from_yaml(reg_path)
    assert reg.domain == "sales"
