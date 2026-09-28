"""The run summary records and prints how long each contract took (2026-09-27)."""

from lakelogic.pipeline.runner import PipelineRunSummary, format_duration


def test_format_duration():
    assert format_duration(None) == "-"
    assert format_duration(4.26) == "4.3s"
    assert format_duration(185) == "3m 05s"
    assert format_duration(3725) == "1h 02m"


def test_summary_carries_and_prints_duration():
    s = PipelineRunSummary(run_id="r", environment="dev", dry_run=False)
    s.append("trips", "bronze", "success", rows=10, table_name="bronze_trips", duration_seconds=185.0)
    s.append("drivers", "bronze", "skipped_checkpoint", table_name="bronze_drivers")
    assert s.results[0]["duration_seconds"] == 185.0 and s.results[1]["duration_seconds"] is None
    text = str(s)
    assert "Duration" in text and "3m 05s" in text
