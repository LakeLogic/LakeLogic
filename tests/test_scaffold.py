"""``lakelogic scaffold`` — contracts in, a runnable medallion project out.

The correctness bar: the ENGINE ITSELF accepts the scaffold — ``DomainRegistry.from_yaml``
round-trips the generated ``_registry.yaml`` and resolves every contract. Plus: contracts
are copied byte-for-byte (the contract stays canonical, never re-serialized), output is
deterministic (same inputs → same bytes), provenance appears only when supplied, and
invalid inputs fail all-or-nothing with a clear error.
"""

from __future__ import annotations

import textwrap

import pytest

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
