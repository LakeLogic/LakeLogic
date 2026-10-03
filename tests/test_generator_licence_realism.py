"""Generated rows for a document-source contract read like the documents (2026-10-03).

A bronze contract over `{landing_root}/driver_licences/**/*.pdf` produced `file_path`
values like `/series/sing.bmp`, birth dates last month, lapsed expiries, `VEH-4486`
vehicle classes, and rule-breaking rows that garbled fields no rule covers.
"""

import fnmatch
from datetime import date

import yaml

from lakelogic.core.generator import DataGenerator

CONTRACT = {
    "version": "1.0.0",
    "info": {"title": "Bronze - Driver Licences"},
    "dataset": "driver_licences",
    "source": {"type": "landing", "path": "{landing_root}/driver_licences/**/*.pdf", "format": "pdf"},
    "model": {
        "fields": [
            {"name": "file_path", "type": "string"},
            {"name": "full_name", "type": "string"},
            {"name": "licence_number", "type": "string"},
            {"name": "date_of_birth", "type": "date"},
            {"name": "expiry_date", "type": "date"},
            {"name": "vehicle_classes", "type": "string"},
        ]
    },
    "quality": {"row_rules": [{"not_null": "licence_number"}]},
}

CATEGORIES = {"AM", "A1", "A2", "A", "B", "BE", "C1", "C1E", "C", "CE", "D1", "D1E", "D", "DE"}


def _rows(contract=CONTRACT, rows=200, invalid_ratio=0.25):
    return DataGenerator(yaml.safe_dump(contract), seed=11).generate(rows=rows, invalid_ratio=invalid_ratio).to_dicts()


def _years(d: str) -> float:
    return (date.today() - date.fromisoformat(str(d)[:10])).days / 365.25


def test_file_path_matches_the_declared_glob_and_format():
    for r in _rows():
        assert r["file_path"] is not None
        assert fnmatch.fnmatch(r["file_path"], "{landing_root}/driver_licences/*/*.pdf"), r["file_path"]
        assert "/y_" in r["file_path"]


def test_file_path_without_a_file_source_is_left_to_faker():
    contract = {k: v for k, v in CONTRACT.items() if k != "source"}
    paths = [r["file_path"] for r in _rows(contract, invalid_ratio=0.0) if r["file_path"]]
    assert not any("driver_licences" in p for p in paths)


def test_date_of_birth_is_an_adult_and_expiry_lies_ahead():
    for r in _rows(invalid_ratio=0.0):
        if r["date_of_birth"]:
            assert 18 <= _years(r["date_of_birth"]) <= 86
        if r["expiry_date"]:
            assert date.fromisoformat(str(r["expiry_date"])[:10]) > date.today()
        if r["date_of_birth"] and r["expiry_date"]:
            assert str(r["expiry_date"]) > str(r["date_of_birth"])


def test_string_typed_date_of_birth_is_still_an_adult():
    contract = yaml.safe_load(yaml.safe_dump(CONTRACT))
    for f in contract["model"]["fields"]:
        if f["name"] in ("date_of_birth", "expiry_date"):
            f["type"] = "string"
    for r in _rows(contract, invalid_ratio=0.0):
        if r["date_of_birth"]:
            assert 18 <= _years(r["date_of_birth"]) <= 86
        if r["expiry_date"]:
            assert r["expiry_date"] > date.today().isoformat()


def test_vehicle_classes_are_licence_categories():
    seen = [r["vehicle_classes"] for r in _rows(invalid_ratio=0.0) if r["vehicle_classes"]]
    assert seen
    for v in seen:
        assert {c.strip() for c in v.split(",")} <= CATEGORIES, v


def test_rule_breaking_rows_break_only_the_declared_rule():
    rows = _rows()
    bad = [r for r in rows if r["_is_invalid"]]
    good = [r for r in rows if not r["_is_invalid"]]
    assert len(bad) == 50
    # Every rule-breaking row breaks the one declared rule, and nothing else is garbled.
    assert all(r["licence_number"] is None for r in bad)
    for r in bad:
        assert r["full_name"] and r["full_name"].isascii()
        assert fnmatch.fnmatch(r["file_path"], "{landing_root}/driver_licences/*/*.pdf")
        if r["date_of_birth"]:
            assert 18 <= _years(r["date_of_birth"]) <= 86
    # The declared not_null shorthand holds on valid rows.
    assert all(r["licence_number"] for r in good)


def test_last_four_plates_and_json_columns_are_not_code_fallbacks():
    contract = {
        "version": "1.0.0",
        "info": {"title": "x"},
        "model": {
            "fields": [
                {"name": n, "type": "string", "required": True}
                for n in ("card_last_four", "licence_plate", "metadata_json")
            ]
        },
    }
    import json
    import re

    for r in _rows(contract, rows=30, invalid_ratio=0.0):
        assert re.fullmatch(r"\d{4}", r["card_last_four"]), r
        assert not re.fullmatch(r"[A-Z]{3}-\d{4}", r["licence_plate"]), r
        json.loads(r["metadata_json"])
