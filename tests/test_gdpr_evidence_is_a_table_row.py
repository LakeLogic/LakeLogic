"""GDPR erasure evidence is a TABLE row, never a file.

The runner's GDPR path wrote a JSON report to a hardcoded Databricks folder,
``/Workspace/Shared/lakelogic_logs/gdpr_reports`` (``./logs/gdpr_reports`` elsewhere) - outside
the domain, and unlike the profile erasure path, which already wrote
``_lakelogic_erasure_evidence``. Owner decision 2026-09-30: evidence lives in tables only.
"""

from types import SimpleNamespace

from lakelogic.core import evidence_tables
from lakelogic.pipeline.runner import LakehousePipeline


def _runner():
    r = LakehousePipeline.__new__(LakehousePipeline)
    r.run_id = "run-1"
    r.engine = "spark"
    r.registry = SimpleNamespace(domain="payments", system="stripe")
    return r


def _record(runner, dc, **kw):
    events = []
    args = dict(
        subject_col="customer_id",
        subject_ids=["c1", "c2"],
        strategy="nullify",
        affected=2,
        dry_run=False,
        partition_filter=None,
        pii_cols=["email"],
        case_ref="DSR-7",
        status="completed",
    )
    args.update(kw)
    runner._record_gdpr_evidence(SimpleNamespace(entity="silver_stripe_charges", layer="silver",
                                                 contract_dict={}), dc, None, events, **args)
    return events


def _dc(run_log_table="`cat`.payments._pipeline_run_log"):
    md = {"table_name": "silver_stripe_charges", "domain": "payments", "system": "stripe"}
    if run_log_table:
        md["run_log_table"] = run_log_table
    return SimpleNamespace(metadata=md, dataset="silver_stripe_charges",
                           info=SimpleNamespace(title="Stripe charges", version="1.0", table_name=None))


def test_writes_one_evidence_row_and_no_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    written = []
    monkeypatch.setattr(evidence_tables, "write_evidence_rows",
                        lambda kind, rows, md, engine_name=None: written.append((kind, rows, md)))

    _record(_runner(), _dc())

    assert len(written) == 1
    kind, rows, md = written[0]
    assert kind == "erasure_evidence"
    assert md["run_log_table"] == "`cat`.payments._pipeline_run_log"  # beside the run log
    row = rows[0]
    assert row["profile"] == "gdpr" and row["status"] == "completed"
    assert row["subject_count"] == 2 and row["rows_affected"] == 2
    assert row["domain"] == "payments" and row["system"] == "stripe"
    # Counts only - never a subject identifier.
    assert "c1" not in str(row)
    assert not (tmp_path / "logs").exists(), "a report file was written"


def test_a_dry_run_is_recorded_as_one(monkeypatch):
    written = []
    monkeypatch.setattr(evidence_tables, "write_evidence_rows",
                        lambda kind, rows, md, engine_name=None: written.append(rows[0]))
    _record(_runner(), _dc(), dry_run=True)
    assert written[0]["status"] == "dry_run"


def test_no_run_log_destination_writes_nothing(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    written = []
    monkeypatch.setattr(evidence_tables, "write_evidence_rows",
                        lambda *a, **k: written.append(a))
    _record(_runner(), _dc(run_log_table=None))
    assert written == []
    assert not (tmp_path / "logs").exists()
