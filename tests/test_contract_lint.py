"""Tests for the deterministic contract governance lint (lakelogic.core.contract_lint)."""

from lakelogic.core.contract_lint import (
    review_contract,
    review_contract_dict,
    review_paths,
    render_github,
    gate_severity,
    ContractFinding,
    ContractReviewReport,
    GovernanceContext,
    load_context,
)


def _ids(raw, name="t"):
    return {f.check_id for f in review_contract_dict(raw, name)}


def _silver(**over):
    base = {
        "info": {"target_layer": "silver"},
        "primary_key": ["id"],
        "model": {"fields": [{"name": "id", "type": "string"}]},
        "quality": {"row_rules": [{"name": "r", "sql": "id is not null"}]},
        "materialization": {"strategy": "merge"},
        "soft_deletes": {"enabled": True},
        "service_levels": {"freshness": "6h"},
        "source": {"type": "delta", "load_mode": "incremental"},
    }
    base.update(over)
    return base


def test_clean_contract_has_no_findings():
    assert _ids(_silver()) == set()


def test_pk_missing_for_merge():  # PK-002
    raw = _silver(primary_key=[])
    assert "PK-002" in _ids(raw)
    # ...but present PK clears it
    assert "PK-002" not in _ids(_silver())


def test_dedup_without_timestamp():  # KEY-001
    raw = _silver(transformations=[{"deduplicate_by_latest": {"key_columns": ["id"]}}])
    assert "KEY-001" in _ids(raw)
    ok = _silver(transformations=[{"deduplicate_by_latest": {"key_columns": ["id"], "timestamp_column": "updated_at"}}])
    assert "KEY-001" not in _ids(ok)


def test_untagged_pii_field():  # PII-001
    raw = _silver(model={"fields": [{"name": "email", "type": "string"}]})
    assert "PII-001" in _ids(raw)


def test_pii_without_masking():  # PII-002
    raw = _silver(model={"fields": [{"name": "email", "type": "string", "pii": True}]})
    assert "PII-002" in _ids(raw)
    masked = _silver(model={"fields": [{"name": "email", "type": "string", "pii": True, "masking": "hash"}]})
    assert "PII-002" not in _ids(masked)


def test_missing_delete_strategy():  # DEL-001
    raw = _silver()
    raw.pop("soft_deletes")  # keyed merge entity, no delete strategy
    assert "DEL-001" in _ids(raw)
    # cdc load_mode satisfies it
    cdc = _silver(source={"type": "delta", "load_mode": "cdc"})
    cdc.pop("soft_deletes")
    assert "DEL-001" not in _ids(cdc)
    # the proposed deletion block satisfies it
    snap = _silver(deletion={"strategy": "snapshot_reconcile"})
    snap.pop("soft_deletes")
    assert "DEL-001" not in _ids(snap)


def test_no_quality_rules():  # QLT-001 — Silver only (enforcement layer)
    assert "QLT-001" in _ids(_silver(quality={}))
    # Gold derives from already-validated Silver → row-rules NOT expected there.
    gold = {
        "info": {"target_layer": "gold"},
        "primary_key": ["id"],
        "materialization": {"strategy": "scd2", "scd2": {"track_columns": ["x"]}},
        "model": {"fields": [{"name": "id", "type": "string"}]},
    }
    assert "QLT-001" not in _ids(gold)


def test_scd2_without_track_columns():  # SCD-001
    raw = _silver(materialization={"strategy": "scd2", "scd2": {"surrogate_key": "sk"}})
    assert "SCD-001" in _ids(raw)
    ok = _silver(materialization={"strategy": "scd2", "scd2": {"track_columns": ["status"]}})
    assert "SCD-001" not in _ids(ok)


def test_unpartitioned_landing():  # SRC-001 — Bronze only
    raw = {"info": {"target_layer": "bronze"}, "source": {"type": "landing"}, "model": {"fields": []}}
    assert "SRC-001" in _ids(raw)
    ok = {
        "info": {"target_layer": "bronze"},
        "source": {"type": "landing", "partition": {"format": "y_%Y"}},
        "model": {"fields": []},
    }
    assert "SRC-001" not in _ids(ok)
    # A non-bronze contract with a landing source is not a SRC-001 concern.
    gold_landing = {"info": {"target_layer": "gold"}, "source": {"type": "landing"}, "model": {"fields": []}}
    assert "SRC-001" not in _ids(gold_landing)


def test_no_volume_freshness_slo():  # VOL-001
    raw = _silver(service_levels={})
    assert "VOL-001" in _ids(raw)
    assert "VOL-001" not in _ids(_silver())  # has freshness


def test_append_on_streaming_source():  # STREAM-001
    # A Kafka source materialized with bare `append` → at-least-once replays dup.
    raw = {
        "info": {"target_layer": "bronze"},
        "source": {"type": "kafka"},
        "materialization": {"strategy": "append"},
        "model": {"fields": [{"name": "id", "type": "string"}]},
    }
    assert "STREAM-001" in _ids(raw)
    # merge clears it …
    merged = dict(raw, materialization={"strategy": "merge"}, primary_key=["id"])
    assert "STREAM-001" not in _ids(merged)
    # … and a plain (non-streaming) batch append is fine.
    batch = dict(raw, source={"type": "landing"})
    assert "STREAM-001" not in _ids(batch)


def test_append_on_resumable_trigger():  # STREAM-001 via trigger
    raw = {
        "info": {"target_layer": "bronze"},
        "source": {"type": "delta"},
        "trigger": "available_now",
        "materialization": {"strategy": "append"},
        "model": {"fields": [{"name": "id", "type": "string"}]},
    }
    assert "STREAM-001" in _ids(raw)


def test_continuous_trigger_advisory():  # STREAM-002
    raw = {
        "info": {"target_layer": "bronze"},
        "source": {"type": "kafka"},
        "trigger": "continuous",
        "materialization": {"strategy": "merge"},
        "primary_key": ["id"],
        "model": {"fields": [{"name": "id", "type": "string"}]},
    }
    ids = _ids(raw)
    assert "STREAM-002" in ids
    assert "STREAM-001" not in ids  # merge, so no dup warning
    # available_now doesn't trip the always-on advisory.
    an = dict(raw, trigger="available_now")
    assert "STREAM-002" not in _ids(an)


def test_bronze_landing_not_flagged_for_entity_checks():
    # Bronze landing shouldn't get silver/gold-only entity findings (DEL/QLT/VOL).
    raw = {
        "info": {"target_layer": "bronze"},
        "source": {"type": "landing", "partition": {"format": "y_%Y"}},
        "model": {"fields": [{"name": "id", "type": "string"}]},
    }
    ids = _ids(raw)
    assert {"DEL-001", "QLT-001", "VOL-001"}.isdisjoint(ids)


# ── Domain-aware behaviour (contract > system > domain) ─────────────────────


def _sev(raw, cid, ctx=None):
    hits = [f for f in review_contract_dict(raw, "t", ctx) if f.check_id == cid]
    return hits[0].severity if hits else None


def test_contract_slo_supersedes_domain():
    # A contract that declares its own SLO is never flagged, with or without ctx.
    assert "VOL-001" not in _ids(_silver())  # _silver() declares service_levels.freshness


def test_vol_suppressed_when_domain_provides_slo():
    raw = _silver(service_levels={})
    assert "VOL-001" in _ids(raw)  # standalone → fires
    ctx = GovernanceContext(policy={"slo": {"freshness": {"silver": {"max_delay_minutes": 120}}}})
    assert "VOL-001" not in {f.check_id for f in review_contract_dict(raw, "t", ctx)}  # inherited → suppressed


def test_pii_escalates_to_critical_under_gdpr():
    raw = _silver(model={"fields": [{"name": "email", "type": "string", "pii": True}]})
    assert _sev(raw, "PII-002") == "warning"
    ctx = GovernanceContext(policy={"compliance": {"frameworks": {"gdpr": {"applicable": True}}}})
    assert _sev(raw, "PII-002", ctx) == "critical"
    # risk_triggers also triggers it
    ctx2 = GovernanceContext(policy={"compliance": {"risk_triggers": ["pii"]}})
    assert _sev(raw, "PII-002", ctx2) == "critical"


def test_del_escalates_to_critical_under_erasure_policy():
    raw = _silver()
    raw.pop("soft_deletes")
    assert _sev(raw, "DEL-001") == "warning"
    ctx = GovernanceContext(policy={"compliance": {"erasure": {"strategy": "nullify"}}})
    assert _sev(raw, "DEL-001", ctx) == "critical"


def test_load_context_walks_up_the_tree(tmp_path):
    dom = tmp_path / "marketplace"
    cdir = dom / "rideflow" / "contracts" / "silver"
    cdir.mkdir(parents=True)
    (dom / "_domain.yaml").write_text(
        "compliance: {frameworks: {gdpr: {applicable: true}}, erasure: {strategy: nullify}}\n"
        "slo: {freshness: {silver: {max_delay_minutes: 120}}}\n",
        encoding="utf-8",
    )
    (dom / "rideflow" / "_system.yaml").write_text("system: rideflow\n", encoding="utf-8")
    cfile = cdir / "silver_x.yaml"
    cfile.write_text("info: {target_layer: silver}\n", encoding="utf-8")
    ctx = load_context(cfile)
    assert ctx is not None
    assert ctx.pii_required is True and ctx.erasure_required is True
    assert ctx.provides_slo("silver") is True


# ── CI: annotations, file path, gate ────────────────────────────────────────


def test_file_path_populated_on_findings(tmp_path):
    c = tmp_path / "silver_x.yaml"
    c.write_text(
        "info: {target_layer: silver}\nprimary_key: [id]\n"
        "materialization: {strategy: merge}\nmodel: {fields: [{name: id, type: string}]}\n",
        encoding="utf-8",
    )
    fs = review_contract(c)
    assert fs and all(f.file == str(c) for f in fs)


def test_render_github_annotations():
    rep = ContractReviewReport(
        contracts_scanned=1,
        findings=[
            ContractFinding(
                contract="c",
                check_id="PII-002",
                severity="critical",
                category="pii",
                message="m1",
                field="email",
                file="a/b.yaml",
            ),
            ContractFinding(
                contract="c", check_id="VOL-001", severity="info", category="rel", message="m2", file="a/b.yaml"
            ),
        ],
        summary={"critical": 1, "warning": 0, "info": 1},
    )
    out = render_github(rep)
    assert "::error file=a/b.yaml,line=1,title=PII-002:email::m1" in out
    assert "::notice file=a/b.yaml,line=1,title=VOL-001::m2" in out
    assert "1 critical" in out


def test_gate_ignores_llm_findings():
    rep = ContractReviewReport(
        contracts_scanned=1,
        findings=[
            ContractFinding(
                contract="c", check_id="SEM-001", severity="critical", category="semantic", message="m", source="llm"
            )
        ],
        summary={},
    )
    assert gate_severity(rep) is None  # an LLM critical must NOT gate CI
    rep.findings.append(
        ContractFinding(
            contract="c", check_id="PK-002", severity="warning", category="keys", message="m", source="rules"
        )
    )
    assert gate_severity(rep) == "warning"  # ...but a rules finding does


def test_report_summary_and_severity(tmp_path):
    c = tmp_path / "silver_x.yaml"
    c.write_text(
        "info: {target_layer: silver}\n"
        "primary_key: [id]\n"
        "materialization: {strategy: merge}\n"
        "model: {fields: [{name: id, type: string}]}\n",
        encoding="utf-8",
    )
    report = review_paths([tmp_path])
    assert report.contracts_scanned == 1
    assert report.summary["warning"] >= 1  # DEL-001 + QLT-001 at least
    assert report.worst_severity in {"warning", "critical", "info"}
    assert all(isinstance(f, ContractFinding) for f in report.findings)
