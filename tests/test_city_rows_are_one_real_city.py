"""A generated city row describes ONE real city, in time order, with no invented codes.

THE LIVE FAILURE (Fabric, 2026-09-13), a `city_master` seeded from its contract:

    city_code | city_name      | country_code | timezone | default_currency
    TYO       | NULL           | ES           | TIM-3689 | GBP
    MEX-2     | David Gonzalez | ZA           | TIM-6358 | GBP

Each column was filled on its own: the code, name, country, timezone and currency of one
"city" named five different places, `sunset_at` fell before `launched_at`, and 200 rows of a
23-code key were made unique by inventing `PAR-3`. These tests pin the rules that fix it, with
and without Faker (the notebooks install `lakelogic` without the `synthetic` extra).
"""

from datetime import datetime

import pytest
import yaml

from lakelogic.core.generator import _CITY_LOOKUP_BY_CODE, _CITY_RECORDS, DataGenerator

CONTRACT = {
    "version": "1.0.0",
    "info": {"title": "city_master"},
    "primary_key": ["city_code"],
    "model": {
        "fields": [
            {"name": "city_code", "type": "string", "required": True},
            {"name": "city_name", "type": "string"},
            {"name": "country_code", "type": "string"},
            {"name": "timezone", "type": "string"},
            {"name": "default_currency", "type": "string"},
            {"name": "launched_at", "type": "timestamp"},
            {"name": "sunset_at", "type": "timestamp"},
        ]
    },
}


def _rows(tmp_path, *, use_faker, rows=200, contract=CONTRACT):
    path = tmp_path / "city_master.yaml"
    path.write_text(yaml.safe_dump(contract), encoding="utf-8")
    df = DataGenerator(str(path), seed=7, use_faker=use_faker).generate(rows=rows, invalid_ratio=0.0)
    return df.to_dicts()


@pytest.mark.parametrize("use_faker", [False, True], ids=["no_faker", "faker"])
def test_every_row_is_one_city(tmp_path, use_faker):
    for row in _rows(tmp_path, use_faker=use_faker):
        record = _CITY_LOOKUP_BY_CODE[row["city_code"]]
        for column in ("country_code", "timezone", "default_currency"):
            assert row[column] == record[column], (row["city_code"], column, row[column])
        # A nullable name may be empty; when present it is THE city's name, never a person.
        assert row["city_name"] in (None, record["city_name"])


@pytest.mark.parametrize("use_faker", [False, True], ids=["no_faker", "faker"])
def test_a_fixed_key_domain_caps_the_rows_instead_of_inventing_codes(tmp_path, use_faker):
    codes = [row["city_code"] for row in _rows(tmp_path, use_faker=use_faker, rows=200)]
    assert len(codes) == len(_CITY_RECORDS)
    assert len(set(codes)) == len(codes)
    assert all("-" not in code for code in codes), "a uniqueness suffix invented a city code"


@pytest.mark.parametrize("use_faker", [False, True], ids=["no_faker", "faker"])
def test_a_city_is_retired_after_it_launched(tmp_path, use_faker):
    rows = _rows(tmp_path, use_faker=use_faker)
    for row in rows:
        if row["sunset_at"] is not None and row["launched_at"] is not None:
            launched, sunset = (
                v if isinstance(v, datetime) else datetime.fromisoformat(str(v))
                for v in (row["launched_at"], row["sunset_at"])
            )
            assert sunset >= launched
    assert sum(row["sunset_at"] is None for row in rows) > len(rows) / 2, "most cities are still live"


def test_without_faker_a_timezone_is_a_real_zone(tmp_path):
    contract = {**CONTRACT, "primary_key": [], "model": {"fields": [{"name": "timezone", "type": "string"}]}}
    zones = {row["timezone"] for row in _rows(tmp_path, use_faker=False, rows=40, contract=contract)} - {None}
    assert zones and all("/" in zone and not zone.startswith("TIM-") for zone in zones)


def test_a_column_the_contract_constrains_is_not_overwritten(tmp_path):
    fields = [dict(f) for f in CONTRACT["model"]["fields"]]
    for f in fields:
        if f["name"] == "country_code":
            f["accepted_values"] = ["GB"]
    rows = _rows(tmp_path, use_faker=False, contract={**CONTRACT, "model": {"fields": fields}})
    assert {row["country_code"] for row in rows} <= {"GB", None}
