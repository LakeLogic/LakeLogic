"""`storage.quarantine_table_name: "{domain}_{table}"` is valid: `{table}` is filled per
contract by the runner. Loading the system file must keep it, fill `{domain}`, and not warn
(it warned in every Colab notebook, 2026-10-19)."""
import textwrap

from loguru import logger

from lakelogic.core.registry import DomainRegistry


def test_per_contract_placeholder_is_kept_without_a_warning(tmp_path):
    p = tmp_path / "_system.yaml"
    p.write_text(textwrap.dedent("""
        domain: marketplace
        system: rideflow
        storage:
          quarantine_table_name: "{domain}_{table}"
        environments:
          dev: {catalog: main}
    """), encoding="utf-8")
    messages = []
    sink = logger.add(lambda m: messages.append(str(m)), level="WARNING")
    try:
        reg = DomainRegistry.from_yaml(str(p), environment="dev")
    finally:
        logger.remove(sink)
    assert reg.storage.quarantine_table_name == "marketplace_{table}"
    assert not [m for m in messages if "Could not resolve template var" in m]
