from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from lakelogic.cli import cli_parsers as cp


@dataclass
class FakeWindow:
    start: datetime | None
    end: datetime | None
    name: str


def test_parse_layers_entities_contracts_tags_and_overrides(monkeypatch, tmp_path):
    warnings = []
    monkeypatch.setattr(cp.logger, "warning", warnings.append)

    assert cp.parse_layers("bronze, ref") == ["bronze", "reference"]
    with pytest.raises(ValueError, match="No layers specified"):
        cp.parse_layers("  ,  ")
    with pytest.raises(ValueError, match=r"Invalid layer\(s\): platinum"):
        cp.parse_layers("platinum")
    with pytest.raises(ValueError, match="Invalid layer order"):
        cp.parse_layers("gold,bronze", strict=True)

    assert cp.parse_entities(None) is None
    assert cp.parse_entities("orders, customers , orders") == {"orders", "customers"}
    assert cp.parse_contracts(None) is None
    expected = {(tmp_path / "a.yaml").resolve(), (tmp_path / "b.yaml").resolve()}
    raw = f"{tmp_path / 'a.yaml'}, {tmp_path / 'b.yaml'}"
    assert cp.parse_contracts(raw) == expected

    assert cp.parse_metrics_tags(None) == {}
    assert cp.parse_metrics_tags("env=dev, invalid , team = data") == {"env": "dev", "team": "data"}
    assert any("Ignoring unparseable tag: invalid" in message for message in warnings)

    overrides = cp.parse_overrides(["enabled=true", "count=5", "ratio=3.5", "name=orders", "disabled=false", "bad"])
    assert overrides == {
        "enabled": True,
        "count": 5,
        "ratio": 3.5,
        "name": "orders",
        "disabled": False,
    }
    assert cp.parse_overrides(None) == {}
    assert any("Ignoring unparseable override: bad" in message for message in warnings)


def test_build_backfill_windows_and_parse_window(monkeypatch):
    fake_driver = types.ModuleType("lakelogic.cli.driver")
    fake_driver.Window = FakeWindow
    monkeypatch.setitem(sys.modules, "lakelogic.cli.driver", fake_driver)

    windows = cp.build_backfill_windows("2026-01-01", "2026-01-03", "day")
    assert [window.name for window in windows] == ["backfill_20260101", "backfill_20260102", "backfill_20260103"]

    weekly = cp.build_backfill_windows("2026-01-01", "2026-01-08", "week")
    assert len(weekly) == 2

    with pytest.raises(ValueError, match="Backfill end date"):
        cp.build_backfill_windows("2026-01-03", "2026-01-01", "day")

    window, reprocess = cp.parse_window("none", None, None, None, None, None)
    assert window.name == "full"
    assert reprocess is False

    class FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 1, 3, 12, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(cp, "datetime", FakeDateTime)
    window, reprocess = cp.parse_window("yesterday", None, None, None, None, None)
    assert window.name == "yesterday"
    assert window.start == datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc)
    assert reprocess is False

    window, reprocess = cp.parse_window("range", "2026-02-01", "2026-02-03", None, None, None)
    assert window.name == "range"
    assert window.end == datetime(2026, 2, 4, 0, 0, tzinfo=timezone.utc)
    assert reprocess is False

    with pytest.raises(ValueError, match="Both --window-start-date and --window-end-date"):
        cp.parse_window("range", None, None, None, None, None)
    with pytest.raises(ValueError, match="Window end date must be on or after start date"):
        cp.parse_window("range", "2026-02-03", "2026-02-01", None, None, None)

    window, reprocess = cp.parse_window("ignored", None, None, "2026-03-05", None, None)
    assert window.name == "reprocess"
    assert window.end == datetime(2026, 3, 6, 0, 0, tzinfo=timezone.utc)
    assert reprocess is True

    with pytest.raises(ValueError, match="Both --reprocess-start-date and --reprocess-end-date"):
        cp.parse_window("ignored", None, None, None, "2026-03-01", None)
    with pytest.raises(ValueError, match="Reprocess end date must be on or after start date"):
        cp.parse_window("ignored", None, None, None, "2026-03-05", "2026-03-04")

    window, reprocess = cp.parse_window("ignored", None, None, None, "2026-03-01", "2026-03-02")
    assert window.start == datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
    assert window.end == datetime(2026, 3, 3, 0, 0, tzinfo=timezone.utc)
    assert reprocess is True

    window, reprocess = cp.parse_window("anything", None, None, None, None, None)
    assert window.name == "last_success"
    assert reprocess is False
