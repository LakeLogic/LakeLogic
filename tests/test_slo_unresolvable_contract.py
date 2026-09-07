"""A contract with no storage root and no materialization path must be SKIPPED.

FOUND ON DATABRICKS (2026-09-06). The `marketplace / rideflow — Service Level Checks`
job logged, in three consecutive lines:

    SLO Freshness Check: scanning 18 contracts in marketplace/rideflow
    SLO Freshness Summary: 0 checks | 0 passed | 0 failed
    AttributeError: 'NoneType' object has no attribute 'replace'

THE DEFECT
    `check_freshness` guarded the unresolvable case with

        if not self.polars and not self.duckdb_con and not schema_root and not polars_path:
            continue

    The first two terms ask WHICH ENGINE is active, which has nothing to do with
    whether a table reference can be built. With `polars=True` (or duckdb) they are
    False, the guard never fires, and the contract falls through to

        to_sql_table_ref(None, "spark")   ->  None.replace("`", "")

    On Spark the same guard did fire — for EVERY contract — which is how one run
    reported "scanning 18 contracts" and "0 checks" in the same breath. One wrong
    condition, two opposite symptoms: a crash on one engine, silence on another.

    `check_quality`'s twin site had NO guard at all and crashed on any engine.

The rule is engine-independent: no root and no path means there is no table to name.
"""
from __future__ import annotations

from lakelogic.core.registry import (
    DomainRegistry,
    RegistryContract,
    RegistrySLO,
    RegistryStorage,
    SLOFreshnessConfig,
)
from lakelogic.core.slo import SLOValidator


def _unresolvable_registry() -> DomainRegistry:
    """A registry whose storage resolves NO roots — the shape the live mesh had."""
    return DomainRegistry(
        domain="marketplace",
        system="rideflow",
        # Freshness IS configured; the contracts simply cannot be located.
        slo=RegistrySLO(freshness={"bronze": SLOFreshnessConfig(max_delay_minutes=60)}),
        storage=RegistryStorage(),  # no bronze/silver/gold root, no run_log_table
        contracts=[
            RegistryContract(
                layer="bronze", entity="trips", path="dummy.yaml",
                enabled=True, contract_dict={"info": {}},
            ),
        ],
    )


def test_freshness_skips_an_unresolvable_contract_on_polars():
    """The engine that crashed. `polars=True` made the old guard unreachable."""
    results = SLOValidator(_unresolvable_registry(), polars=True).check_freshness()
    assert results == []


def test_freshness_skips_an_unresolvable_contract_on_duckdb():
    """Same guard, the other engine it was disabled for."""
    results = SLOValidator(_unresolvable_registry(), duckdb_con=object()).check_freshness()
    assert results == []


def test_run_checks_does_not_raise_when_nothing_can_be_resolved():
    """End to end: the job failed here, not in an isolated helper."""
    report = SLOValidator(_unresolvable_registry(), polars=True).run_checks(environment="dev")
    assert report is not None


def test_the_guard_does_not_ask_which_engine_is_active():
    """Pins the REASON, not just the outcome.

    Re-introducing `self.polars` / `self.duckdb_con` into this condition brings the
    crash back, because whether a reference can be built has nothing to do with who
    would execute it.
    """
    import inspect

    src = inspect.getsource(SLOValidator.check_freshness)
    # Skip comments: the fix's own comment QUOTES the old broken guard verbatim, so a
    # naive scan finds the documentation of the bug instead of the code.
    guard = next(
        line for line in src.splitlines()
        if "not schema_root" in line and "not polars_path" in line
        and not line.lstrip().startswith("#")
    )
    assert "self.polars" not in guard, "the guard must not depend on the active engine"
    assert "self.duckdb_con" not in guard, "the guard must not depend on the active engine"


# ── run_log_table: one meaning, two homes ────────────────────────────────────


def _registry_with_metadata_run_log() -> DomainRegistry:
    """The shape every mesh domain actually has: run_log_table under `metadata`."""
    return DomainRegistry(
        domain="marketplace",
        system="rideflow",
        slo=RegistrySLO(),
        storage=RegistryStorage(),           # storage does NOT carry it
        metadata={"run_log_table": "`cat`.marketplace._pipeline_run_log"},
        contracts=[],
    )


def test_the_validator_finds_the_run_log_the_pipeline_writes():
    """The pipeline reads `metadata.run_log_table`; the validator read
    `storage.run_log_table` and found nothing. Every domain declares the first, so
    every run logged "No run_log_table configured in storage; cannot check row
    counts" while the pipeline was writing that very table."""
    v = SLOValidator(_registry_with_metadata_run_log(), polars=True)
    assert v._run_log_table() == "`cat`.marketplace._pipeline_run_log"


def test_an_explicit_storage_value_still_wins():
    """`storage` is the override, so setting it deliberately must not be ignored."""
    reg = _registry_with_metadata_run_log()
    reg.storage.run_log_table = "`cat`.other._explicit"
    assert SLOValidator(reg, polars=True)._run_log_table() == "`cat`.other._explicit"


def test_neither_declared_is_still_none():
    """Absent stays absent — the checks that need it must skip, not guess a name."""
    reg = _registry_with_metadata_run_log()
    reg.metadata = {}
    assert SLOValidator(reg, polars=True)._run_log_table() is None
