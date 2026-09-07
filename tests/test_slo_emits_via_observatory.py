"""SLO results must travel the SAME route as pipeline run logs.

THE DEFECT
    The standalone SLO check reported through `RemoteObserver`:
      * disabled unless `LAKELOGIC_REMOTE_OBSERVER=true`
      * addressed by `LINEAGELOGIC_REPORT_URL`, an env var nothing sets
      * posting `{"type": "slo", "report": ...}` — a shape the platform has no
        handler for (its telemetry module contains no SLO handling at all)
      * and the notebook checked `registry.cloud.report_url`, then constructed the
        observer WITHOUT passing it, so a configured endpoint was ignored anyway

    Measured consequence: 0 of 2,082 recorded runs across three orgs carried an
    `slo` key, so every data product read "Not evaluated" while the objectives were
    declared and (once the crash was fixed) actually being evaluated.

THE ROUTE THAT WORKS
    `RunLogIngest.metadata` is a free-form dict that lands in `run_metadata`, and
    the platform reads `run_metadata.slo`. So no platform change is needed — the
    results just have to go through the pipeline's own observatory path.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakelogic.core.run_log import emit_slo_report
from lakelogic.core.slo import SLOCheckResult


class _Registry:
    domain = "marketplace"
    system = "rideflow"
    observatory = {
        "enabled": True,
        "endpoint": "https://example.invalid/api/v1/operations/run-logs/ingest",
        "api_key": "llc_sk_test",
    }


def _results():
    return [
        SLOCheckResult(
            layer="bronze",
            entity="trips",
            check_type="freshness",
            status="PASS",
            passed=True,
            delay_minutes=10,
            slo_max_minutes=60,
        ),
        SLOCheckResult(
            layer="silver",
            entity="charges",
            check_type="row_count",
            status="FAIL",
            passed=False,
            row_count=0,
            slo_min_rows=1,
        ),
    ]


def _posts(mock_post):
    return [c.kwargs["json"] for c in mock_post.call_args_list]


def test_it_posts_to_the_observatory_endpoint_with_the_api_key_header():
    """The pipeline's route: same endpoint, same X-API-Key header.

    `flush_spool` is patched out because a successful send also drains any run logs
    this machine buffered during a past outage — 12 of them here — which would be
    counted as posts made by this function.
    """
    with patch("requests.post") as post, patch("lakelogic.core.observatory_spool.flush_spool", return_value=0):
        post.return_value = MagicMock(status_code=200, text="ok")
        emit_slo_report(_Registry(), _results(), environment="dev")

    assert post.call_count == 2
    url = post.call_args_list[0].args[0]
    assert url == _Registry.observatory["endpoint"]
    assert post.call_args_list[0].kwargs["headers"]["X-API-Key"] == "llc_sk_test"


def test_the_results_land_where_the_platform_reads_them():
    """`run_metadata.slo_json` — the field `_slo_signal_counts` inspects."""
    with patch("requests.post") as post, patch("lakelogic.core.observatory_spool.flush_spool", return_value=0):
        post.return_value = MagicMock(status_code=200, text="ok")
        emit_slo_report(_Registry(), _results(), environment="dev")

    bodies = {b["dataset"]: b for b in _posts(post)}
    assert "freshness" in bodies["trips"]["metadata"]["slo_json"]
    assert bodies["trips"]["metadata"]["slo_json"]["freshness"]["pass"] is True
    # A threshold is what makes the platform count a section as CONFIGURED.
    assert bodies["trips"]["metadata"]["slo_json"]["freshness"]["threshold_seconds"] == 3600.0


def test_one_row_per_entity_so_each_product_gets_its_own_verdict():
    """Data Products is keyed by dataset; one combined row could not say which
    product met its objective."""
    with patch("requests.post") as post, patch("lakelogic.core.observatory_spool.flush_spool", return_value=0):
        post.return_value = MagicMock(status_code=200, text="ok")
        emit_slo_report(_Registry(), _results(), environment="dev")

    assert {b["dataset"] for b in _posts(post)} == {"trips", "charges"}


def test_a_failing_check_is_reported_as_failed():
    with patch("requests.post") as post, patch("lakelogic.core.observatory_spool.flush_spool", return_value=0):
        post.return_value = MagicMock(status_code=200, text="ok")
        emit_slo_report(_Registry(), _results(), environment="dev")

    bodies = {b["dataset"]: b for b in _posts(post)}
    assert bodies["charges"]["status"] == "failed"
    assert bodies["charges"]["metadata"]["slo_json"]["row_count"]["pass"] is False


def test_the_row_is_marked_as_a_check_not_a_data_load():
    """An SLO check reads tables and writes none. Without this the row would be
    indistinguishable from a pipeline run that produced zero rows."""
    with patch("requests.post") as post, patch("lakelogic.core.observatory_spool.flush_spool", return_value=0):
        post.return_value = MagicMock(status_code=200, text="ok")
        emit_slo_report(_Registry(), _results(), environment="dev")

    body = _posts(post)[0]
    assert body["metadata"]["record_type"] == "slo_check"
    assert body["engine"] == "slo"


def test_nothing_is_sent_when_the_observatory_is_disabled():
    class _Off(_Registry):
        observatory = {"enabled": False}

    with patch("requests.post") as post:
        assert emit_slo_report(_Off(), _results()) == 0
    post.assert_not_called()


def test_a_transient_failure_is_buffered_rather_than_dropped():
    """Same spool-on-5xx behaviour the pipeline's run logs get."""
    with patch("requests.post") as post, patch("lakelogic.core.observatory_spool.spool_payload") as spool:
        post.return_value = MagicMock(status_code=503, text="unavailable")
        emit_slo_report(_Registry(), _results(), environment="dev")
    assert spool.call_count == 2


def test_the_result_lands_under_the_key_the_platform_actually_reads():
    """THE KEY IS THE WHOLE FEATURE.

    The SaaS reads a run's SLO result with

        def _metadata_slo(metadata):
            slo = metadata.get("slo_json") or metadata.get("slos") or {}

    and every other `get("slo")` in that codebase reads the DECLARED objective from
    contract config, not a run result. Emitting `slo` therefore wrote 36 records that
    no code path could see — the products drawer went on saying "No objectives
    configured · not evaluated" with the evidence one key away in the same row.

    This test previously asserted `metadata["slo"]`, so it stayed green while the
    feature did nothing: it pinned the emitter to its author's assumption instead of
    to the consumer. It now names the consumer's key, which is the only thing that
    makes the record readable.
    """
    with patch("requests.post") as post, patch("lakelogic.core.observatory_spool.flush_spool", return_value=0):
        post.return_value = MagicMock(status_code=200, text="ok")
        emit_slo_report(_Registry(), _results(), environment="dev")
    body = _posts(post)[0]
    assert "slo_json" in body["metadata"], "the platform reads slo_json, not slo"
    assert "slo" not in body["metadata"], "emitting the old key too would leave a second, unread copy of the verdict"
