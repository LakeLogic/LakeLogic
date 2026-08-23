"""``lakelogic scaffold`` — contracts in, a runnable medallion project out.

The correctness bar: the ENGINE ITSELF accepts the scaffold — ``DomainRegistry.from_yaml``
round-trips the generated ``_registry.yaml`` and resolves every contract. Plus: contracts
are copied byte-for-byte (the contract stays canonical, never re-serialized), output is
deterministic (same inputs → same bytes), provenance appears only when supplied, and
invalid inputs fail all-or-nothing with a clear error.
"""

from __future__ import annotations

import json
import textwrap

import pytest
import yaml

from lakelogic.core.registry import DomainRegistry
from lakelogic.scaffold.project import Provenance, ScaffoldError, scaffold_project


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text), encoding="utf-8", newline="\n")
    return path


@pytest.fixture()
def contract_dir(tmp_path):
    src = tmp_path / "contracts_src"
    _write(src / "trips_bronze.yaml", """\
        version: "1.0.0"
        info:
          title: "Trips — raw landing"
          target_layer: "bronze"
          domain: "marketplace"
          system: "rideflow"
        dataset: "bronze_trips"
        """)
    _write(src / "trips_silver.yaml", """\
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
        transformations:
          - cast:
              columns:
                fare_amount: "float"
          - deduplicate:
              on: ["trip_id"]
              sort_by: ["trip_id"]
        quality:
          row_rules:
            - name: "trip_id_not_null"
              sql: "trip_id IS NOT NULL"
        """)
    _write(src / "trips_gold.yaml", """\
        version: "1.0.0"
        info:
          title: "Trips — daily fact"
          target_layer: "gold"
          domain: "marketplace"
          system: "rideflow"
        dataset: "fact_trips"
        upstream:
          - "silver_trips"
        """)
    return src


def test_scaffold_layout_and_engine_roundtrip(contract_dir, tmp_path):
    out = tmp_path / "project"
    result = scaffold_project(contract_dir, out)

    # Layout: per-layer contracts + registry + entrypoint + README.
    assert (out / "contracts" / "bronze" / "bronze_trips.yaml").is_file()
    assert (out / "contracts" / "silver" / "silver_trips.yaml").is_file()
    assert (out / "contracts" / "gold" / "fact_trips.yaml").is_file()
    assert (out / "run_pipeline.py").is_file()
    assert (out / "README.md").is_file()
    assert result.registry_path == out / "_registry.yaml"

    # THE correctness bar: the engine loads the generated registry and resolves
    # every contract — the scaffold is a valid project by the runner's own rules.
    registry = DomainRegistry.from_yaml(str(result.registry_path), storage_mode="direct")
    assert registry.domain == "marketplace" and registry.system == "rideflow"
    entities = {c.entity: c.layer for c in registry.contracts}
    assert entities == {
        "bronze_trips": "bronze", "silver_trips": "silver", "fact_trips": "gold",
    }


def test_contracts_are_copied_byte_for_byte(contract_dir, tmp_path):
    out = tmp_path / "project"
    scaffold_project(contract_dir, out)
    src = (contract_dir / "trips_silver.yaml").read_bytes()
    dst = (out / "contracts" / "silver" / "silver_trips.yaml").read_bytes()
    assert src == dst  # the contract stays canonical — no lossy re-serialization


def test_output_is_deterministic(contract_dir, tmp_path):
    out1, out2 = tmp_path / "p1", tmp_path / "p2"
    r1 = scaffold_project(contract_dir, out1)
    r2 = scaffold_project(contract_dir, out2)
    for f1, f2 in zip(sorted(r1.files), sorted(r2.files)):
        assert f1.relative_to(out1) == f2.relative_to(out2)
        assert f1.read_bytes() == f2.read_bytes(), f"nondeterministic: {f1.name}"


def test_provenance_stamped_when_supplied_absent_otherwise(contract_dir, tmp_path):
    plain = tmp_path / "plain"
    scaffold_project(contract_dir, plain)
    assert "revision" not in (plain / "_registry.yaml").read_text(encoding="utf-8")
    assert "Provenance" not in (plain / "README.md").read_text(encoding="utf-8")

    stamped = tmp_path / "stamped"
    scaffold_project(
        contract_dir, stamped,
        provenance=Provenance(
            generator="LakeLogic Medallion Builder",
            revision_hash="sha256:abc123",
            seal_hash="seal:def456",
            source="snapshot snap-1",
        ),
    )
    for name in ("_registry.yaml", "run_pipeline.py", "README.md"):
        text = (stamped / name).read_text(encoding="utf-8")
        assert "sha256:abc123" in text, f"provenance missing from {name}"
    # ... but NEVER stamped into the contracts themselves (they stay canonical).
    contract = (stamped / "contracts" / "silver" / "silver_trips.yaml").read_text(encoding="utf-8")
    assert "sha256:abc123" not in contract


def test_missing_target_layer_fails_clearly(tmp_path):
    src = tmp_path / "src"
    _write(src / "no_layer.yaml", """\
        version: "1.0.0"
        dataset: "mystery"
        """)
    with pytest.raises(ScaffoldError, match="target_layer"):
        scaffold_project(src, tmp_path / "out")


def test_invalid_contract_fails_all_or_nothing(contract_dir, tmp_path):
    _write(contract_dir / "broken.yaml", """\
        version: "1.0.0"
        info: {title: "Broken", target_layer: "silver"}
        dataset: "broken"
        primary_key: "not-a-list"
        """)
    out = tmp_path / "out"
    with pytest.raises(ScaffoldError, match="broken.yaml"):
        scaffold_project(contract_dir, out)
    assert not out.exists()  # nothing written on failure


def test_duplicate_entities_rejected(tmp_path):
    src = tmp_path / "src"
    _write(src / "a.yaml", """\
        version: "1.0.0"
        info: {title: "A", target_layer: "bronze"}
        dataset: "trips"
        """)
    _write(src / "b.yaml", """\
        version: "1.0.0"
        info: {title: "B", target_layer: "silver"}
        dataset: "trips"
        """)
    with pytest.raises(ScaffoldError, match="duplicate entity"):
        scaffold_project(src, tmp_path / "out")


def test_generated_entrypoint_is_valid_python(contract_dir, tmp_path):
    out = tmp_path / "project"
    scaffold_project(contract_dir, out, engine="polars")
    code = (out / "run_pipeline.py").read_text(encoding="utf-8")
    compile(code, "run_pipeline.py", "exec")  # syntactically valid
    assert 'engine=\'polars\'' in code or 'engine="polars"' in code


# ── Target flavors: fabric / databricks notebooks ───────────────────────────


def test_fabric_target_emits_per_layer_notebooks(contract_dir, tmp_path):
    out = tmp_path / "project"
    result = scaffold_project(contract_dir, out, target="fabric")

    # One notebook per layer, ordinal-prefixed; NO python entrypoint.
    for name in ("01_bronze", "02_silver", "03_gold"):
        assert (out / "notebooks" / f"{name}.ipynb").is_file()
    assert not (out / "run_pipeline.py").exists()

    # Valid nbformat JSON; the run cell targets exactly its own layer on Spark,
    # reads the project from the OneLake default-lakehouse mount, and compiles.
    for layer, name in (("bronze", "01_bronze"), ("silver", "02_silver"), ("gold", "03_gold")):
        nb = json.loads((out / "notebooks" / f"{name}.ipynb").read_text(encoding="utf-8"))
        assert nb["nbformat"] == 4
        code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
        assert any("%pip install lakelogic" in "".join(c["source"]) for c in code_cells)
        run_src = "".join(code_cells[-1]["source"])
        assert f"target_layers='{layer}'" in run_src
        assert 'engine="spark", spark=spark' in run_src
        assert "/lakehouse/default/Files/marketplace_rideflow" in run_src
        compile(run_src, f"{name}.ipynb", "exec")

    # Contracts + registry are the SAME shape as the python target; the engine still
    # round-trips the registry, whose storage root is the OneLake mount.
    registry = DomainRegistry.from_yaml(str(result.registry_path), storage_mode="direct")
    assert {c.entity for c in registry.contracts} == {"bronze_trips", "silver_trips", "fact_trips"}
    reg_text = result.registry_path.read_text(encoding="utf-8")
    assert "/lakehouse/default/Files/lake" in reg_text


def test_databricks_target_emits_source_notebooks_and_bundle(contract_dir, tmp_path):
    out = tmp_path / "project"
    scaffold_project(contract_dir, out, target="databricks")

    for layer, name in (("bronze", "01_bronze"), ("silver", "02_silver"), ("gold", "03_gold")):
        text = (out / "notebooks" / f"{name}.py").read_text(encoding="utf-8")
        assert text.startswith("# Databricks notebook source")
        assert "# MAGIC %pip install lakelogic" in text
        assert f"target_layers='{layer}'" in text
        assert 'engine="spark", spark=spark' in text
        compile(text, f"{name}.py", "exec")  # MAGIC lines are comments — whole file compiles
    assert not (out / "run_pipeline.py").exists()

    # The bundle chains bronze -> silver -> gold and points at the emitted notebooks.
    bundle = yaml.safe_load((out / "databricks.yml").read_text(encoding="utf-8"))
    tasks = bundle["resources"]["jobs"]["medallion_run"]["tasks"]
    assert [t["task_key"] for t in tasks] == ["bronze", "silver", "gold"]
    assert "depends_on" not in tasks[0]
    assert tasks[1]["depends_on"] == [{"task_key": "bronze"}]
    assert tasks[2]["depends_on"] == [{"task_key": "silver"}]
    for task in tasks:
        rel = task["notebook_task"]["notebook_path"]
        assert (out / rel).is_file(), f"bundle references missing notebook {rel}"


def test_notebook_targets_only_emit_layers_present(tmp_path):
    src = tmp_path / "src"
    _write(src / "a.yaml", """\
        version: "1.0.0"
        info: {title: "A", target_layer: "bronze"}
        dataset: "bronze_a"
        """)
    _write(src / "b.yaml", """\
        version: "1.0.0"
        info: {title: "B", target_layer: "silver"}
        dataset: "silver_b"
        """)
    out = tmp_path / "out"
    scaffold_project(src, out, target="databricks")
    assert (out / "notebooks" / "01_bronze.py").is_file()
    assert (out / "notebooks" / "02_silver.py").is_file()
    assert not (out / "notebooks" / "03_gold.py").exists()
    bundle = yaml.safe_load((out / "databricks.yml").read_text(encoding="utf-8"))
    tasks = bundle["resources"]["jobs"]["medallion_run"]["tasks"]
    assert [t["task_key"] for t in tasks] == ["bronze", "silver"]


def test_notebook_targets_are_deterministic_and_stamp_provenance(contract_dir, tmp_path):
    prov = Provenance(
        generator="LakeLogic Medallion Builder",
        revision_hash="sha256:abc123",
        seal_hash="seal:def456",
    )
    out1, out2 = tmp_path / "p1", tmp_path / "p2"
    r1 = scaffold_project(contract_dir, out1, target="databricks", provenance=prov)
    r2 = scaffold_project(contract_dir, out2, target="databricks", provenance=prov)
    for f1, f2 in zip(sorted(r1.files), sorted(r2.files)):
        assert f1.read_bytes() == f2.read_bytes(), f"nondeterministic: {f1.name}"

    # Provenance in every generated file — never in the contracts.
    for rel in ("notebooks/01_bronze.py", "databricks.yml", "_registry.yaml", "README.md"):
        assert "sha256:abc123" in (out1 / rel).read_text(encoding="utf-8"), rel
    fabric_out = tmp_path / "fab"
    scaffold_project(contract_dir, fabric_out, target="fabric", provenance=prov)
    nb = json.loads((fabric_out / "notebooks" / "01_bronze.ipynb").read_text(encoding="utf-8"))
    assert "sha256:abc123" in "".join(nb["cells"][0]["source"])
    contract = (out1 / "contracts" / "silver" / "silver_trips.yaml").read_text(encoding="utf-8")
    assert "sha256:abc123" not in contract


def test_invalid_target_and_engine_combos_rejected(contract_dir, tmp_path):
    with pytest.raises(ScaffoldError, match="target must be one of"):
        scaffold_project(contract_dir, tmp_path / "o1", target="snowflake")
    with pytest.raises(ScaffoldError, match="engine must be 'spark'"):
        scaffold_project(contract_dir, tmp_path / "o2", target="fabric", engine="duckdb")
    # python target keeps its duckdb default.
    scaffold_project(contract_dir, tmp_path / "o3")
    code = (tmp_path / "o3" / "run_pipeline.py").read_text(encoding="utf-8")
    assert "engine='duckdb'" in code or 'engine="duckdb"' in code
