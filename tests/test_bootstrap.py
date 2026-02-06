from pathlib import Path

import yaml

from lakeguard.cli import main


def test_bootstrap_generates_contracts(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "customers.csv").write_text("id,name\n1,Alice\n", encoding="utf-8")
    (landing / "orders.csv").write_text("id,amount\n1,10.0\n", encoding="utf-8")

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

    assert registry_path.exists()
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    assert len(registry.get("entries", [])) == 2
    for entry in registry["entries"]:
        contract_path = output_dir / entry["contracts"]["bronze"]
        assert contract_path.exists()
