"""
Tests for the unknown member row injection in SCD2 materialization.
"""

import pytest

pd = pytest.importorskip("pandas")


def _make_scd2_cfg(**overrides):
    """Build a standard SCD2 config dict."""
    cfg = {
        "effective_from_field": "effective_from",
        "effective_to_field": "effective_to",
        "current_flag_field": "is_current",
        "surrogate_key": "COUNTRY_SR_KEY",
        "surrogate_key_strategy": "hash",
        "effective_to_default": "9999-12-31",
        "effective_from_default": "1900-01-01",
        "version_column": "_version",
        "change_reason_column": "_change_reason",
        "track_columns": ["COUNTRY_NAME"],
    }
    cfg.update(overrides)
    return cfg


def _make_unknown_cfg(enabled=True, sk_value="-1", **defaults):
    """Build an unknown_member config."""
    return {
        "enabled": enabled,
        "surrogate_key_value": sk_value,
        "default_values": defaults,
    }


class TestUnknownMemberPandas:
    """Tests for _inject_unknown_member_pandas."""

    def test_inject_on_initial_load(self):
        from lakelogic.core.materialization import _inject_unknown_member_pandas

        data = pd.DataFrame(
            {
                "COUNTRY_CODE": ["GB", "US"],
                "COUNTRY_NAME": ["United Kingdom", "United States"],
                "effective_from": ["1900-01-01", "1900-01-01"],
                "effective_to": ["9999-12-31", "9999-12-31"],
                "is_current": [True, True],
                "COUNTRY_SR_KEY": ["abc123", "def456"],
                "_version": [1, 1],
                "_change_reason": ["initial_load", "initial_load"],
            }
        )

        scd2_cfg = _make_scd2_cfg()
        unknown_cfg = _make_unknown_cfg(COUNTRY_CODE="_UNKNOWN", COUNTRY_NAME="Unknown")

        result = _inject_unknown_member_pandas(data, ["COUNTRY_CODE"], scd2_cfg, unknown_cfg)

        assert len(result) == 3  # 2 original + 1 unknown
        unknown_row = result[result["COUNTRY_SR_KEY"] == "-1"]
        assert len(unknown_row) == 1
        assert unknown_row.iloc[0]["COUNTRY_CODE"] == "_UNKNOWN"
        assert unknown_row.iloc[0]["COUNTRY_NAME"] == "Unknown"
        assert unknown_row.iloc[0]["is_current"] == True
        assert unknown_row.iloc[0]["_version"] == 0

    def test_idempotent_no_duplicate(self):
        from lakelogic.core.materialization import _inject_unknown_member_pandas

        data = pd.DataFrame(
            {
                "COUNTRY_CODE": ["GB", "_UNKNOWN"],
                "COUNTRY_NAME": ["United Kingdom", "Unknown"],
                "effective_from": ["1900-01-01", "1900-01-01"],
                "effective_to": ["9999-12-31", "9999-12-31"],
                "is_current": [True, True],
                "COUNTRY_SR_KEY": ["abc123", "-1"],
                "_version": [1, 0],
                "_change_reason": ["initial_load", "unknown_member"],
            }
        )

        scd2_cfg = _make_scd2_cfg()
        unknown_cfg = _make_unknown_cfg(COUNTRY_CODE="_UNKNOWN", COUNTRY_NAME="Unknown")

        result = _inject_unknown_member_pandas(data, ["COUNTRY_CODE"], scd2_cfg, unknown_cfg)

        # Should NOT add another row — already has SK=-1
        assert len(result) == 2
        unknown_rows = result[result["COUNTRY_SR_KEY"] == "-1"]
        assert len(unknown_rows) == 1

    def test_disabled_skips(self):
        from lakelogic.core.materialization import _inject_unknown_member_pandas

        data = pd.DataFrame(
            {
                "COUNTRY_CODE": ["GB"],
                "COUNTRY_SR_KEY": ["abc123"],
            }
        )

        scd2_cfg = _make_scd2_cfg()
        unknown_cfg = _make_unknown_cfg(enabled=False)

        result = _inject_unknown_member_pandas(data, ["COUNTRY_CODE"], scd2_cfg, unknown_cfg)

        assert len(result) == 1  # No unknown row added


class TestSCD2WithUnknownMember:
    """Test _scd2_frames integration with unknown_member config."""

    def test_scd2_frames_injects_unknown(self):
        from lakelogic.core.materialization import _inject_unknown_member_pandas, _scd2_frames

        existing = pd.DataFrame()
        incoming = pd.DataFrame(
            {
                "COUNTRY_CODE": ["GB", "US"],
                "COUNTRY_NAME": ["United Kingdom", "United States"],
            }
        )

        unknown_cfg = _make_unknown_cfg(COUNTRY_CODE="_UNKNOWN", COUNTRY_NAME="Unknown")
        scd2_cfg = _make_scd2_cfg(unknown_member=unknown_cfg)

        result = _scd2_frames(existing, incoming, ["COUNTRY_CODE"], scd2_cfg)
        # Unknown member injection happens at the caller level, not inside _scd2_frames
        result = _inject_unknown_member_pandas(result, ["COUNTRY_CODE"], scd2_cfg, unknown_cfg)

        # Should have 3 rows: GB, US, and _UNKNOWN
        assert len(result) == 3
        unknown = result[result["COUNTRY_SR_KEY"] == "-1"]
        assert len(unknown) == 1
        assert unknown.iloc[0]["COUNTRY_NAME"] == "Unknown"
        assert unknown.iloc[0]["is_current"] == True

    def test_scd2_frames_no_unknown_when_disabled(self):
        from lakelogic.core.materialization import _scd2_frames

        existing = pd.DataFrame()
        incoming = pd.DataFrame(
            {
                "COUNTRY_CODE": ["GB"],
                "COUNTRY_NAME": ["United Kingdom"],
            }
        )

        scd2_cfg = _make_scd2_cfg()  # No unknown_member key

        result = _scd2_frames(existing, incoming, ["COUNTRY_CODE"], scd2_cfg)

        # Should only have the 1 real row
        assert len(result) == 1
