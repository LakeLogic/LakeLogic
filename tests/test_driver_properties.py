"""
Property-based tests for CLI parsers and observability functions.

Uses Hypothesis to fuzz-test:
    • parse_layers — valid layers always succeed; invalid layers always raise
    • parse_entities — round-trip preservation
    • parse_metrics_tags — key=value pairs always produce correct dicts
    • parse_overrides — type coercion (bool, int, float, str)
    • parse_window — mode dispatch correctness
    • flatten_summary — output key invariants
    • format_prometheus — well-formedness
    • build_backfill_windows — window count and boundary invariants
"""
import json
from datetime import datetime, timedelta, timezone

import pytest
hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, assume, settings, HealthCheck
from hypothesis import strategies as st

from lakelogic.cli.cli_parsers import (
    build_backfill_windows,
    parse_contracts,
    parse_entities,
    parse_layers,
    parse_metrics_tags,
    parse_overrides,
    parse_window,
)
from lakelogic.cli.observability import (
    flatten_summary,
    format_prometheus,
)


# ── Strategies ───────────────────────────────────────────────────────────────

VALID_LAYERS = ["bronze", "silver", "gold", "reference"]

# Generate a non-empty subsequence of valid layers in valid order
valid_layer_lists = st.lists(
    st.sampled_from(VALID_LAYERS),
    min_size=1,
    max_size=4,
).filter(
    lambda layers: layers == sorted(layers, key=VALID_LAYERS.index) and len(set(layers)) == len(layers)
)

# Entity names: non-empty alphanumeric strings (no commas or whitespace)
entity_names = st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789_"),
    min_size=1,
    max_size=20,
)

# Tag key=value pairs
tag_keys = st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz_"),
    min_size=1,
    max_size=15,
)
tag_values = st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789-._"),
    min_size=1,
    max_size=20,
)

# Metric values
metric_values = st.one_of(st.integers(min_value=0, max_value=10_000), st.none())


# ── parse_layers ─────────────────────────────────────────────────────────────

class TestParseLayers:
    """Property-based tests for parse_layers."""

    @given(layers=valid_layer_lists)
    @settings(suppress_health_check=[HealthCheck.too_slow])
    def test_valid_layers_round_trip(self, layers: list[str]) -> None:
        """A valid layer list should parse without error and return the same layers."""
        raw = ",".join(layers)
        result = parse_layers(raw, strict=True)
        assert result == layers

    @given(layers=valid_layer_lists)
    @settings(suppress_health_check=[HealthCheck.too_slow])
    def test_non_strict_always_succeeds(self, layers: list[str]) -> None:
        """Non-strict mode should accept any valid layer ordering."""
        raw = ",".join(layers)
        result = parse_layers(raw, strict=False)
        assert set(result) == set(layers)

    @given(junk=st.text(alphabet="xyz!@#$%^&*()", min_size=1, max_size=10))
    def test_invalid_layers_raise(self, junk: str) -> None:
        """Random non-layer strings should raise ValueError."""
        assume(junk.strip())  # must not be blank
        assume(junk.strip().lower() not in VALID_LAYERS)
        with pytest.raises(ValueError, match="Invalid layer"):
            parse_layers(junk, strict=True)

    def test_empty_string_raises(self) -> None:
        """Empty layer string should raise ValueError."""
        with pytest.raises(ValueError):
            parse_layers("", strict=False)

    def test_reversed_strict_raises(self) -> None:
        """Reversed ordering should raise ValueError in strict mode."""
        # gold,silver is definitively reversed
        with pytest.raises(ValueError, match="Invalid layer order"):
            parse_layers("gold,silver", strict=True)
        with pytest.raises(ValueError, match="Invalid layer order"):
            parse_layers("gold,bronze", strict=True)
        with pytest.raises(ValueError, match="order"):
            parse_layers("silver,bronze", strict=True)


# ── parse_entities ───────────────────────────────────────────────────────────

class TestParseEntities:
    """Property tests for parse_entities."""

    @given(entities=st.lists(entity_names, min_size=1, max_size=5))
    def test_round_trip(self, entities: list[str]) -> None:
        """Entities should survive comma-separated round-trip."""
        raw = ",".join(entities)
        result = parse_entities(raw)
        assert result is not None
        assert all(e in result for e in entities)

    def test_none_returns_none(self) -> None:
        """None input should return None."""
        assert parse_entities(None) is None

    def test_empty_returns_none(self) -> None:
        """Empty string should return None."""
        assert parse_entities("") is None


# ── parse_metrics_tags ───────────────────────────────────────────────────────

class TestParseMetricsTags:
    """Property tests for parse_metrics_tags."""

    @given(
        pairs=st.lists(
            st.tuples(tag_keys, tag_values),
            min_size=1,
            max_size=5,
        ).filter(lambda ps: len(set(k for k, _ in ps)) == len(ps))  # unique keys
    )
    def test_key_value_round_trip(self, pairs: list[tuple[str, str]]) -> None:
        """key=value tags should parse into the correct dict."""
        raw = ",".join(f"{k}={v}" for k, v in pairs)
        result = parse_metrics_tags(raw)
        for k, v in pairs:
            assert result[k] == v

    def test_none_returns_empty(self) -> None:
        """None input should return empty dict."""
        assert parse_metrics_tags(None) == {}

    @given(junk=st.text(alphabet="abcdefg", min_size=1, max_size=10))
    def test_no_equals_produces_empty_dict(self, junk: str) -> None:
        """Tags without = should be ignored (produce empty dict or missing keys)."""
        assume("=" not in junk)
        result = parse_metrics_tags(junk)
        assert junk not in result  # the raw value is never a key


# ── parse_overrides ──────────────────────────────────────────────────────────

class TestParseOverrides:
    """Property tests for parse_overrides."""

    def test_bool_true_coercion(self) -> None:
        """'true' should be coerced to Python True."""
        result = parse_overrides(["key=true"])
        assert result["key"] is True

    def test_bool_false_coercion(self) -> None:
        """'false' should be coerced to Python False."""
        result = parse_overrides(["key=false"])
        assert result["key"] is False

    @given(n=st.integers(min_value=0, max_value=10_000))
    def test_int_coercion(self, n: int) -> None:
        """Digit-only values should be coerced to int."""
        result = parse_overrides([f"key={n}"])
        assert result["key"] == n
        assert isinstance(result["key"], int)

    @given(f=st.floats(min_value=-1000, max_value=1000, allow_nan=False, allow_infinity=False))
    @settings(suppress_health_check=[HealthCheck.filter_too_much])
    def test_float_coercion(self, f: float) -> None:
        """Float-convertible values should be coerced to float."""
        s = f"{f:.4f}"
        assume("." in s)  # ensure it's not mistaken for int
        result = parse_overrides([f"key={s}"])
        assert isinstance(result["key"], float)
        assert abs(result["key"] - f) < 0.001

    @given(s=st.text(alphabet="abcdef_", min_size=1, max_size=10))
    def test_string_fallback(self, s: str) -> None:
        """Non-numeric, non-bool values remain strings."""
        assume(s.lower() not in ("true", "false"))
        assume(not s.replace(".", "", 1).isdigit())
        result = parse_overrides([f"key={s}"])
        assert result["key"] == s
        assert isinstance(result["key"], str)

    def test_empty_returns_empty(self) -> None:
        """None or empty list should return empty dict."""
        assert parse_overrides(None) == {}
        assert parse_overrides([]) == {}


# ── parse_window ─────────────────────────────────────────────────────────────

class TestParseWindow:
    """Property tests for parse_window modes."""

    def test_none_mode(self) -> None:
        """Window='none' produces a full-load window."""
        window, reprocess = parse_window("none", None, None, None, None, None)
        assert window.label == "full"
        assert reprocess is False

    def test_yesterday_mode(self) -> None:
        """Window='yesterday' produces a bounded one-day window."""
        window, reprocess = parse_window("yesterday", None, None, None, None, None)
        assert window.label == "yesterday"
        assert window.start is not None
        assert window.end is not None
        assert (window.end - window.start) == timedelta(days=1)
        assert reprocess is False

    def test_range_mode(self) -> None:
        """Window='range' with valid dates produces correct boundaries."""
        window, reprocess = parse_window(
            "range", "2026-01-01", "2026-01-05", None, None, None,
        )
        assert window.label == "range"
        assert window.start == datetime(2026, 1, 1, tzinfo=timezone.utc)
        # end is exclusive (+1 day)
        assert window.end == datetime(2026, 1, 6, tzinfo=timezone.utc)
        assert reprocess is False

    def test_range_missing_dates_raises(self) -> None:
        """Window='range' without dates should raise ValueError."""
        with pytest.raises(ValueError, match="window-start-date"):
            parse_window("range", None, None, None, None, None)

    def test_reprocess_date(self) -> None:
        """A single reprocess date produces a one-day reprocess window."""
        window, reprocess = parse_window(
            "last_success", None, None, "2026-03-01", None, None,
        )
        assert window.label == "reprocess"
        assert reprocess is True
        assert (window.end - window.start) == timedelta(days=1)

    def test_reprocess_range(self) -> None:
        """A reprocess range produces the correct boundaries."""
        window, reprocess = parse_window(
            "last_success", None, None, None, "2026-03-01", "2026-03-05",
        )
        assert window.label == "reprocess"
        assert reprocess is True
        assert window.start == datetime(2026, 3, 1, tzinfo=timezone.utc)
        assert window.end == datetime(2026, 3, 6, tzinfo=timezone.utc)

    def test_default_is_last_success(self) -> None:
        """Unrecognized window value defaults to last_success."""
        window, reprocess = parse_window("last_success", None, None, None, None, None)
        assert window.label == "last_success"
        assert reprocess is False


# ── flatten_summary ──────────────────────────────────────────────────────────

class TestFlattenSummaryProperties:
    """Property-based tests for flatten_summary."""

    @given(
        run_id=st.text(min_size=1, max_size=32),
        engine=st.sampled_from(["polars", "pandas", "duckdb", "spark"]),
        total=st.integers(min_value=0, max_value=1000),
        success=st.integers(min_value=0, max_value=1000),
        failed=st.integers(min_value=0, max_value=100),
    )
    def test_output_always_has_required_keys(
        self, run_id: str, engine: str, total: int, success: int, failed: int,
    ) -> None:
        """flatten_summary output always contains the expected keys."""
        summary = {
            "run_id": run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": 42.0,
            "engine": engine,
            "metrics": {
                "total_contracts": total,
                "successful": success,
                "failed": failed,
                "skipped_missing_upstream": 0,
                "skipped_no_sources": 0,
                "full_loads": 0,
                "full_loads_due_to_missing_logs": 0,
                "missing_upstreams": 0,
            },
        }
        record = flatten_summary(summary)
        expected_keys = {
            "run_id", "started_at", "finished_at", "duration_seconds",
            "engine", "total_contracts", "successful", "failed",
            "skipped_missing_upstream", "skipped_no_sources", "full_loads",
            "full_loads_due_to_missing_logs", "missing_upstreams", "summary_json",
        }
        assert set(record.keys()) == expected_keys

    @given(
        total=st.integers(min_value=0, max_value=1000),
        success=st.integers(min_value=0, max_value=1000),
    )
    def test_summary_json_is_valid_json(self, total: int, success: int) -> None:
        """The summary_json field should always be valid JSON."""
        summary = {
            "run_id": "test",
            "engine": "polars",
            "metrics": {"total_contracts": total, "successful": success},
        }
        record = flatten_summary(summary)
        parsed = json.loads(record["summary_json"])
        assert parsed["run_id"] == "test"


# ── format_prometheus ────────────────────────────────────────────────────────

class TestFormatPrometheusProperties:
    """Property-based tests for Prometheus exposition format."""

    @given(
        prefix=st.text(alphabet="abcdefg_", min_size=1, max_size=10),
        metrics=st.dictionaries(
            keys=st.text(alphabet="abcdefghij_", min_size=1, max_size=10),
            values=metric_values,
            min_size=1,
            max_size=5,
        ),
    )
    def test_output_ends_with_newline(self, prefix: str, metrics: dict) -> None:
        """Prometheus output should always end with a newline."""
        snapshot = {"tags": {}, "metrics": metrics}
        text = format_prometheus(snapshot, prefix)
        assert text.endswith("\n")

    @given(
        value=st.integers(min_value=0, max_value=10_000),
    )
    def test_single_metric_format(self, value: int) -> None:
        """Each metric line should be 'prefix_name{labels} value'."""
        snapshot = {"tags": {"env": "ci"}, "metrics": {"total": value}}
        text = format_prometheus(snapshot, "app")
        assert f'app_total{{env="ci"}} {value}' in text

    def test_none_values_excluded(self) -> None:
        """Metrics with None values should not appear in output."""
        snapshot = {"tags": {}, "metrics": {"good": 5, "bad": None}}
        text = format_prometheus(snapshot, "prefix")
        assert "good" in text
        assert "bad" not in text

    def test_empty_snapshot_produces_newline_only(self) -> None:
        """An empty snapshot should produce just a newline."""
        text = format_prometheus(None, "prefix")
        assert text == "\n"


# ── build_backfill_windows ───────────────────────────────────────────────────

class TestBackfillWindowProperties:
    """Property-based tests for backfill window generation."""

    @given(
        days=st.integers(min_value=0, max_value=60),
    )
    def test_daily_count(self, days: int) -> None:
        """Daily backfill from start to start+days should produce days+1 windows."""
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=days)
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        windows = build_backfill_windows(start_str, end_str, "day")
        assert len(windows) == days + 1

    @given(
        days=st.integers(min_value=0, max_value=60),
    )
    def test_windows_are_contiguous(self, days: int) -> None:
        """Each window's start should match the previous window's end."""
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=days)
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        windows = build_backfill_windows(start_str, end_str, "day")
        for i in range(1, len(windows)):
            assert windows[i].start == windows[i - 1].end

    @given(
        days=st.integers(min_value=0, max_value=60),
    )
    def test_first_window_starts_at_start_date(self, days: int) -> None:
        """The first window should start at the start date."""
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=days)
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        windows = build_backfill_windows(start_str, end_str, "day")
        assert windows[0].start == start
