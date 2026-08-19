"""Domain → system resolution + provenance.

Verifies the merge rules from docs/contracts/inheritance.md, each tied to the provenance
it should produce. Uses tmp fixtures for exactness plus a real-file smoke check.
"""

from __future__ import annotations

from pathlib import Path

from lakelogic.registry import Origin, Reason, resolve_system

_ASSETS = Path(__file__).resolve().parents[2] / "examples" / "colab" / "assets" / "domains_rideflow"


def _mesh(tmp_path: Path, domain_yaml: str, system_yaml: str) -> Path:
    (tmp_path / "d").mkdir(parents=True, exist_ok=True)
    (tmp_path / "d" / "_domain.yaml").write_text(domain_yaml, encoding="utf-8")
    sysdir = tmp_path / "d" / "s"
    sysdir.mkdir(parents=True, exist_ok=True)
    sp = sysdir / "_system.yaml"
    sp.write_text(system_yaml, encoding="utf-8")
    return sp


# ── inheritance rules ────────────────────────────────────────────────────────


def test_system_absent_inherits_domain(tmp_path):
    sp = _mesh(tmp_path, "domain: d\nslo: {freshness: {bronze: {max_delay_minutes: 60}}}\n", "system: s\n")
    r = resolve_system(sp)
    assert r.config["slo"] == {"freshness": {"bronze": {"max_delay_minutes": 60}}}
    assert r.provenance["slo"].origin == Origin.DOMAIN
    assert r.provenance["slo"].reason == Reason.INHERITED


def test_inheritable_dicts_deep_merge_system_wins(tmp_path):
    sp = _mesh(
        tmp_path,
        "domain: d\nquarantine: {enabled: true, format: delta}\n",
        "system: s\nquarantine: {format: parquet}\n",
    )
    r = resolve_system(sp)
    # system overrides format; domain supplies the untouched `enabled`.
    assert r.config["quarantine"] == {"enabled": True, "format": "parquet"}
    assert r.provenance["quarantine"].origin == Origin.BOTH
    assert r.provenance["quarantine"].reason == Reason.DEEP_MERGED


def test_inheritable_lists_concatenate(tmp_path):
    sp = _mesh(
        tmp_path,
        "domain: d\nnotifications: [{type: slack, target: '#a'}]\n",
        "system: s\nnotifications: [{type: email, target: 'x@y.z'}]\n",
    )
    r = resolve_system(sp)
    assert [n["type"] for n in r.config["notifications"]] == ["slack", "email"]
    assert r.provenance["notifications"].reason == Reason.CONCATENATED


def test_identity_scalar_domain_wins_on_mismatch(tmp_path):
    sp = _mesh(tmp_path, "domain: d\ngold_layer: gold\n", "system: s\ngold_layer: curated\n")
    r = resolve_system(sp)
    assert r.config["gold_layer"] == "gold"  # domain-locked
    assert r.provenance["gold_layer"].origin == Origin.DOMAIN
    assert r.provenance["gold_layer"].reason == Reason.DOMAIN_LOCKED


def test_cost_currency_domain_authoritative(tmp_path):
    sp = _mesh(
        tmp_path,
        "domain: d\ncost: {currency: GBP, budget: {daily_limit: 40}}\n",
        "system: s\ncost: {currency: USD, rates: {dbu_per_hour: 0.22}}\n",
    )
    r = resolve_system(sp)
    assert r.config["cost"]["currency"] == "GBP"  # domain currency enforced
    assert r.config["cost"]["rates"] == {"dbu_per_hour": 0.22}  # system rates kept
    assert r.provenance["cost"].reason == Reason.CURRENCY_NORMALISED


def test_system_only_key_is_declared(tmp_path):
    sp = _mesh(tmp_path, "domain: d\n", "system: s\nstorage: {domain_catalog: c}\n")
    r = resolve_system(sp)
    assert r.provenance["storage"].origin == Origin.SYSTEM
    assert r.provenance["storage"].reason == Reason.DECLARED


def test_per_leaf_provenance_inside_deep_merge(tmp_path):
    """A deep-merged block records origin down to each leaf — the "why is this value
    what it is?" lookup."""
    domain_yaml = (
        "domain: d\n"
        "slo:\n"
        "  freshness:\n"
        "    bronze: {max_delay_minutes: 60, check_column: _p}\n"
        "    silver: {max_delay_minutes: 240}\n"
    )
    sp = _mesh(
        tmp_path,
        domain_yaml,
        "system: s\nslo:\n  freshness:\n    bronze: {max_delay_minutes: 30}\n",
    )
    r = resolve_system(sp)
    assert r.config["slo"]["freshness"]["bronze"]["max_delay_minutes"] == 30
    # the overridden leaf is system; the untouched sibling leaf is domain-inherited
    assert r.provenance["slo.freshness.bronze.max_delay_minutes"].origin == Origin.SYSTEM
    assert r.provenance["slo.freshness.bronze.max_delay_minutes"].reason == Reason.OVERRIDDEN
    assert r.provenance["slo.freshness.bronze.check_column"].origin == Origin.DOMAIN
    assert r.provenance["slo.freshness.silver"].origin == Origin.DOMAIN


# ── environment-layer provenance ─────────────────────────────────────────────


def test_environment_bindings_and_substitution(tmp_path):
    sp = _mesh(
        tmp_path,
        "domain: d\n",
        (
            "system: s\n"
            "storage:\n"
            "  domain_catalog: '{catalog}.{domain}'\n"  # references an env var → substituted
            "  static_root: /fixed/path\n"  # no env var → not annotated
            "environments:\n"
            "  dev: {catalog: rideflow_dev, storage_account: acct}\n"
        ),
    )
    r = resolve_system(sp, environment="dev")
    # the env's own fields are bindings
    assert r.provenance["environments.dev.catalog"].origin == Origin.ENVIRONMENT
    assert r.provenance["environments.dev.catalog"].reason == Reason.ENV_BINDING
    # a value referencing {catalog} is flagged env-substituted…
    assert r.provenance["storage.domain_catalog"].origin == Origin.ENVIRONMENT
    assert r.provenance["storage.domain_catalog"].reason == Reason.ENV_SUBSTITUTED
    # …a static value is not
    assert "storage.static_root" not in r.provenance


def test_no_environment_means_no_env_provenance(tmp_path):
    sp = _mesh(tmp_path, "domain: d\n", "system: s\nstorage: {domain_catalog: '{catalog}'}\n")
    r = resolve_system(sp)  # no environment
    assert not any(p.origin == Origin.ENVIRONMENT for p in r.provenance.values())


def test_unknown_environment_is_safe(tmp_path):
    sp = _mesh(tmp_path, "domain: d\n", "system: s\nenvironments: {dev: {catalog: c}}\n")
    r = resolve_system(sp, environment="prod")  # not defined
    assert not any(p.reason == Reason.ENV_BINDING for p in r.provenance.values())


# ── real file ────────────────────────────────────────────────────────────────


def test_real_system_resolves_with_provenance():
    r = resolve_system(_ASSETS / "marketplace" / "rideflow" / "_system.yaml")
    assert r.domain and r.system
    # Layer aliases come from the domain; the contract index is the system's own.
    assert r.provenance["silver_layer"].origin == Origin.DOMAIN
    assert r.provenance["contracts"].origin == Origin.SYSTEM
    assert r.keys_from(Origin.DOMAIN)  # something was inherited
