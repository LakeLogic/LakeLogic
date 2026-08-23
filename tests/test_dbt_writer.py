"""``lakelogic scaffold --target dbt`` — contracts in, a Snowflake-first dbt project out.

The correctness bar has two halves: (1) the emitted project is a plausible dbt project
(project file, per-layer models + schema.yml, sources for bronze/entry-layer inputs,
contract enforcement, tests) and (2) **the round-trip closes** — the existing
read-direction adapter (:mod:`lakelogic.adapters.dbt`) imports the generated
``schema.yml`` back into a contract whose fields, required flags, and rules match the
canonical contract. That round-trip is the drift gate. Plus the standing scaffold bars:
byte-for-byte contracts, determinism, provenance only in generated files, all-or-nothing.
"""

from __future__ import annotations

import textwrap

import pytest
import yaml

from lakelogic.adapters.dbt import DbtAdapter
from lakelogic.scaffold.dbt_project import scaffold_dbt_project
from lakelogic.scaffold.project import Provenance, ScaffoldError


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text), encoding="utf-8", newline="\n")
    return path


@pytest.fixture()
def contract_dir(tmp_path):
    src = tmp_path / "contracts_src"
    _write(
        src / "trips_bronze.yaml",
        """\
        version: "1.0.0"
        info:
          title: "Trips — raw landing"
          target_layer: "bronze"
          domain: "marketplace"
          system: "rideflow"
        dataset: "bronze_trips"
        """,
    )
    _write(
        src / "trips_silver.yaml",
        """\
        version: "1.0.0"
        info:
          title: "Trips — cleansed"
          target_layer: "silver"
          domain: "marketplace"
          system: "rideflow"
        dataset: "silver_trips"
        primary_key: ["trip_id"]
        upstream:
          - "bronze_trips"
        model:
          fields:
            - {name: "trip_id", type: "string", required: true}
            - {name: "rider_id", type: "string", required: true}
            - {name: "fare_amount", type: "float", required: false}
        transformations:
          - deduplicate:
              by: ["trip_id"]
              sort_by: ["trip_id"]
        quality:
          row_rules:
            - name: "fare_amount_range"
              sql: "fare_amount BETWEEN 0 AND 10000"
        """,
    )
    _write(
        src / "trips_gold.yaml",
        """\
        version: "1.0.0"
        info:
          title: "Trips — daily fact"
          target_layer: "gold"
          domain: "marketplace"
          system: "rideflow"
        dataset: "fact_trips"
        upstream:
          - "silver_trips"
        """,
    )
    return src


def test_dbt_layout_and_project_file(contract_dir, tmp_path):
    out = tmp_path / "dbt"
    result = scaffold_dbt_project(contract_dir, out)

    for rel in (
        "dbt_project.yml",
        "profiles.yml.example",
        "models/sources.yml",
        "models/silver/silver_trips.sql",
        "models/silver/schema.yml",
        "models/gold/fact_trips.sql",
        "models/gold/schema.yml",
        "contracts/bronze/bronze_trips.yaml",
        "contracts/silver/silver_trips.yaml",
        "contracts/gold/fact_trips.yaml",
        "README.md",
    ):
        assert (out / rel).is_file(), rel
    assert set(result.files) >= {out / "dbt_project.yml", out / "README.md"}

    project = yaml.safe_load((out / "dbt_project.yml").read_text(encoding="utf-8"))
    assert project["name"] == "marketplace_rideflow"
    assert project["models"]["marketplace_rideflow"]["silver"]["+schema"] == "silver"
    # dbt_utils needed (expression_is_true from the range rule) → packages.yml emitted.
    packages = yaml.safe_load((out / "packages.yml").read_text(encoding="utf-8"))
    assert packages["packages"][0]["package"] == "dbt-labs/dbt_utils"


def test_bronze_is_a_source_not_a_model(contract_dir, tmp_path):
    out = tmp_path / "dbt"
    scaffold_dbt_project(contract_dir, out)
    assert not (out / "models" / "bronze").exists()
    sources = yaml.safe_load((out / "models" / "sources.yml").read_text(encoding="utf-8"))
    tables = {t["name"] for t in sources["sources"][0]["tables"]}
    assert "bronze_trips" in tables
    # Silver reads from the bronze SOURCE; gold refs the silver MODEL.
    silver_sql = (out / "models" / "silver" / "silver_trips.sql").read_text(encoding="utf-8")
    assert "{{ source('raw', 'bronze_trips') }}" in silver_sql
    gold_sql = (out / "models" / "gold" / "fact_trips.sql").read_text(encoding="utf-8")
    assert "{{ ref('silver_trips') }}" in gold_sql


def test_silver_sql_casts_and_dedups_per_the_contract(contract_dir, tmp_path):
    out = tmp_path / "dbt"
    scaffold_dbt_project(contract_dir, out)
    sql = (out / "models" / "silver" / "silver_trips.sql").read_text(encoding="utf-8")
    assert "cast(trip_id as varchar) as trip_id" in sql
    assert "cast(fare_amount as float) as fare_amount" in sql
    # The ORDER BY must come from the contract's `sort_by`, not `order by 1`.
    # `order by 1` orders by the first projected column, so the generated dbt model
    # would keep a different row than LakeLogic keeps for the same contract — the
    # two implementations of one contract silently disagreeing.
    assert "qualify row_number() over (partition by trip_id order by trip_id desc) = 1" in sql
    assert "order by 1)" not in sql


def test_schema_yml_tests_and_contract_enforcement(contract_dir, tmp_path):
    out = tmp_path / "dbt"
    scaffold_dbt_project(contract_dir, out)
    schema = yaml.safe_load((out / "models" / "silver" / "schema.yml").read_text(encoding="utf-8"))
    (model,) = schema["models"]
    assert model["name"] == "silver_trips"
    assert model["config"]["contract"]["enforced"] is True
    cols = {c["name"]: c for c in model["columns"]}
    assert cols["trip_id"]["data_type"] == "varchar"
    assert "not_null" in cols["trip_id"]["tests"]
    assert "unique" in cols["trip_id"]["tests"]  # single-column dedup key
    assert "not_null" not in cols.get("fare_amount", {}).get("tests", [])
    # The range row rule lands on ITS column as expression_is_true (row-rule shape the
    # read adapter maps back).
    fare_tests = cols["fare_amount"]["tests"]
    assert {"dbt_utils.expression_is_true": {"expression": "fare_amount BETWEEN 0 AND 10000"}} in fare_tests
    # Thin gold model: no fields → no contract enforcement, but still declared.
    gold_schema = yaml.safe_load((out / "models" / "gold" / "schema.yml").read_text(encoding="utf-8"))
    (gold_model,) = gold_schema["models"]
    assert gold_model["name"] == "fact_trips"
    assert "config" not in gold_model


def test_drift_gate_roundtrip_through_the_read_adapter(contract_dir, tmp_path):
    """THE bar: the read-direction adapter imports the generated schema.yml back into a
    contract that matches the canonical one (fields, required, uniqueness, row rule)."""
    out = tmp_path / "dbt"
    scaffold_dbt_project(contract_dir, out)
    readback = DbtAdapter(out / "models" / "silver" / "schema.yml").model_to_contract("silver_trips")

    fields = {f.name: f for f in readback.model.fields}
    assert set(fields) == {"trip_id", "rider_id", "fare_amount"}
    assert fields["trip_id"].type == "string" and fields["trip_id"].required
    assert fields["rider_id"].required
    # float emits as Snowflake `float`; the adapter canonicalises that family to double.
    assert fields["fare_amount"].type == "double" and not fields["fare_amount"].required
    # unique → dataset rule + primary key; range rule → row rule with the same SQL.
    assert readback.primary_key == ["trip_id"]
    assert any("COUNT(DISTINCT trip_id)" in r.sql for r in readback.quality.dataset_rules)
    assert any(r.sql == "fare_amount BETWEEN 0 AND 10000" for r in readback.quality.row_rules)


def test_contracts_copied_byte_for_byte_and_provenance_isolated(contract_dir, tmp_path):
    out = tmp_path / "dbt"
    prov = Provenance(generator="LakeLogic Medallion Builder", revision_hash="sha256:abc123", seal_hash="seal:def456")
    scaffold_dbt_project(contract_dir, out, provenance=prov)
    assert (out / "contracts" / "silver" / "silver_trips.yaml").read_bytes() == (
        contract_dir / "trips_silver.yaml"
    ).read_bytes()
    for rel in ("dbt_project.yml", "models/silver/silver_trips.sql", "models/silver/schema.yml", "README.md"):
        assert "sha256:abc123" in (out / rel).read_text(encoding="utf-8"), rel
    schema = yaml.safe_load((out / "models" / "silver" / "schema.yml").read_text(encoding="utf-8"))
    assert schema["models"][0]["meta"]["lakelogic"]["revision_hash"] == "sha256:abc123"
    assert "sha256:abc123" not in (out / "contracts" / "silver" / "silver_trips.yaml").read_text(encoding="utf-8")


def test_output_is_deterministic(contract_dir, tmp_path):
    r1 = scaffold_dbt_project(contract_dir, tmp_path / "p1")
    r2 = scaffold_dbt_project(contract_dir, tmp_path / "p2")
    for f1, f2 in zip(sorted(r1.files), sorted(r2.files)):
        assert f1.read_bytes() == f2.read_bytes(), f"nondeterministic: {f1.name}"


def test_entry_layer_model_without_upstream_gets_an_implicit_source(tmp_path):
    src = tmp_path / "src"
    _write(
        src / "orders.yaml",
        """\
        version: "1.0.0"
        info: {title: "Orders", target_layer: "silver"}
        dataset: "silver_orders"
        model:
          fields:
            - {name: "order_id", type: "string", required: true}
        """,
    )
    out = tmp_path / "out"
    scaffold_dbt_project(src, out)
    sql = (out / "models" / "silver" / "silver_orders.sql").read_text(encoding="utf-8")
    assert "{{ source('raw', 'silver_orders') }}" in sql
    sources = yaml.safe_load((out / "models" / "sources.yml").read_text(encoding="utf-8"))
    assert {t["name"] for t in sources["sources"][0]["tables"]} == {"silver_orders"}


def test_logic_sql_is_used_verbatim(tmp_path):
    src = tmp_path / "src"
    _write(
        src / "a.yaml",
        """\
        version: "1.0.0"
        info: {title: "A", target_layer: "silver"}
        dataset: "silver_a"
        """,
    )
    _write(
        src / "b.yaml",
        """\
        version: "1.0.0"
        info: {title: "B", target_layer: "silver"}
        dataset: "silver_b"
        """,
    )
    _write(
        src / "joined.yaml",
        """\
        version: "1.0.0"
        info: {title: "Joined", target_layer: "gold"}
        dataset: "gold_joined"
        upstream: ["silver_a", "silver_b"]
        logic: "select a.*, b.x from {{ ref('silver_a') }} a join {{ ref('silver_b') }} b on a.id = b.id"
        """,
    )
    out = tmp_path / "out"
    scaffold_dbt_project(src, out)
    sql = (out / "models" / "gold" / "gold_joined.sql").read_text(encoding="utf-8")
    assert "join {{ ref('silver_b') }} b" in sql


def test_multi_upstream_without_logic_is_refused_all_or_nothing(tmp_path):
    src = tmp_path / "src"
    _write(
        src / "a.yaml",
        """\
        version: "1.0.0"
        info: {title: "A", target_layer: "silver"}
        dataset: "silver_a"
        """,
    )
    _write(
        src / "b.yaml",
        """\
        version: "1.0.0"
        info: {title: "B", target_layer: "silver"}
        dataset: "silver_b"
        """,
    )
    _write(
        src / "bad.yaml",
        """\
        version: "1.0.0"
        info: {title: "Bad", target_layer: "gold"}
        dataset: "gold_bad"
        upstream: ["silver_a", "silver_b"]
        """,
    )
    out = tmp_path / "out"
    with pytest.raises(ScaffoldError, match="never invents a join"):
        scaffold_dbt_project(src, out)
    assert not out.exists()  # nothing written on failure


def test_columnless_row_rule_falls_back_to_model_level_and_reclassifies_on_readback(tmp_path):
    """A row rule naming no declared field emits model-level; the read adapter then
    reclassifies it as a DATASET rule (SQL intact, category flips) — the documented
    mapping edge, pinned here so a silent behavior change is caught."""
    src = tmp_path / "src"
    _write(
        src / "orders.yaml",
        """\
        version: "1.0.0"
        info: {title: "Orders", target_layer: "silver"}
        dataset: "silver_orders"
        model:
          fields:
            - {name: "order_id", type: "string", required: true}
        quality:
          row_rules:
            - name: "row_sanity"
              sql: "1 = 1"
        """,
    )
    out = tmp_path / "out"
    scaffold_dbt_project(src, out)
    schema = yaml.safe_load((out / "models" / "silver" / "schema.yml").read_text(encoding="utf-8"))
    (model,) = schema["models"]
    assert model["tests"] == [{"dbt_utils.expression_is_true": {"expression": "1 = 1"}}]
    assert "tests" not in {c["name"]: c for c in model["columns"]}["order_id"] or {
        "dbt_utils.expression_is_true": {"expression": "1 = 1"}
    } not in {c["name"]: c for c in model["columns"]}["order_id"].get("tests", [])
    readback = DbtAdapter(out / "models" / "silver" / "schema.yml").model_to_contract("silver_orders")
    assert any(r.sql == "1 = 1" for r in readback.quality.dataset_rules)
    assert not any(r.sql == "1 = 1" for r in (readback.quality.row_rules or []))


def test_composite_dedup_key_uses_unique_combination(tmp_path):
    src = tmp_path / "src"
    _write(
        src / "events.yaml",
        """\
        version: "1.0.0"
        info: {title: "Events", target_layer: "silver"}
        dataset: "silver_events"
        model:
          fields:
            - {name: "user_id", type: "string", required: true}
            - {name: "event_ts", type: "timestamp", required: true}
        transformations:
          - deduplicate:
              by: ["user_id", "event_ts"]
              sort_by: ["user_id"]
        """,
    )
    out = tmp_path / "out"
    scaffold_dbt_project(src, out)
    schema = yaml.safe_load((out / "models" / "silver" / "schema.yml").read_text(encoding="utf-8"))
    (model,) = schema["models"]
    cols = {c["name"]: c for c in model["columns"]}
    assert "unique" not in cols["user_id"].get("tests", [])  # not per-column
    assert model["tests"] == [
        {"dbt_utils.unique_combination_of_columns": {"combination_of_columns": ["user_id", "event_ts"]}}
    ]
    sql = (out / "models" / "silver" / "silver_events.sql").read_text(encoding="utf-8")
    assert "partition by user_id, event_ts" in sql
