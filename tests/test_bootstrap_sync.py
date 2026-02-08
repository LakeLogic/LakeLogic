from pathlib import Path

import yaml

from lakelogic.cli import main


def test_bootstrap_sync_adds_new_entities(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "customers.csv").write_text("id,name\n1,Alice\n", encoding="utf-8")

    output_dir = tmp_path / "contracts"
    registry_path = output_dir / "_registry.yaml"

    main.bootstrap(
        landing=landing,
        output_dir=output_dir,
        registry=registry_path,
        format="csv",
        pattern="*.csv",
        layer="bronze",
        sample_rows=10,
    )

    (landing / "orders.csv").write_text("id,amount\n1,10.0\n", encoding="utf-8")

    main.bootstrap(
        landing=landing,
        output_dir=output_dir,
        registry=registry_path,
        format="csv",
        pattern="*.csv",
        layer="bronze",
        sample_rows=10,
        sync=True,
    )

    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    entries = registry.get("entries", [])
    entities = {e.get("entity") for e in entries}
    assert "customers" in entities
    assert "orders" in entities
