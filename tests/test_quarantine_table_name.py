"""Quarantine table naming is configurable, default unchanged (2026-09-26: domain-centric
quarantine — rejects in the domain's own schema as `quarantine_<table>`)."""
from types import SimpleNamespace as NS

from lakelogic.core.registry import RegistryStorage
from lakelogic.pipeline.runner import quarantine_table_name


def test_default_is_the_historical_name():
    s = RegistryStorage()
    assert s.quarantine_table_name == "{domain}_{table}"
    assert quarantine_table_name(s, {"domain": "payments"}, "silver_stripe_charges") == "payments_silver_stripe_charges"
    assert quarantine_table_name(s, {}, "silver_stripe_charges") == "silver_stripe_charges"


def test_domain_centric_prefix():
    s = RegistryStorage(quarantine_table_name="quarantine_{table}")
    assert quarantine_table_name(s, {"domain": "payments"}, "silver_stripe_charges") == "quarantine_silver_stripe_charges"


def test_system_placeholder():
    s = NS(quarantine_table_name="{system}__{table}")
    assert quarantine_table_name(s, {"domain": "d", "system": "stripe"}, "t") == "stripe__t"
