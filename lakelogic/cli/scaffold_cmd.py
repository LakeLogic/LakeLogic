"""``lakelogic scaffold`` — contracts in, a runnable medallion project out."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from loguru import logger

from lakelogic.scaffold.project import Provenance, ScaffoldError, scaffold_project


def scaffold_command(
    contracts: Path = typer.Argument(
        ..., help="A contract YAML, or a directory of contracts (searched recursively)."
    ),
    out: Path = typer.Option(
        Path("./lakehouse_project"), "--out", "-o", help="Output project directory."
    ),
    domain: Optional[str] = typer.Option(
        None, help="Registry domain (default: first contract's info.domain)."
    ),
    system: Optional[str] = typer.Option(
        None, help="Registry system (default: first contract's info.system)."
    ),
    engine: str = typer.Option(
        "duckdb", help="Engine for the generated entrypoint (duckdb | polars | spark)."
    ),
    environment: str = typer.Option("dev", help="Environment the entrypoint resolves."),
    generator: Optional[str] = typer.Option(
        None, help="Provenance: what generated this project."
    ),
    revision_hash: Optional[str] = typer.Option(
        None, help="Provenance: the approved design revision hash."
    ),
    seal_hash: Optional[str] = typer.Option(
        None, help="Provenance: the governance review seal hash."
    ),
    source: Optional[str] = typer.Option(
        None, help="Provenance: the estate/snapshot the design came from."
    ),
):
    """Scaffold a runnable medallion project (registry + contracts + entrypoint).

    Contracts are validated, then copied byte-for-byte into a per-layer layout with a
    generated ``_registry.yaml``, ``run_pipeline.py``, and README. Deterministic:
    the same contracts always produce the same bytes.
    """
    provenance = Provenance(
        generator=generator, revision_hash=revision_hash, seal_hash=seal_hash, source=source
    )
    try:
        result = scaffold_project(
            contracts,
            out,
            domain=domain,
            system=system,
            engine=engine,
            environment=environment,
            provenance=provenance if provenance.lines() else None,
        )
    except ScaffoldError as exc:
        logger.error("scaffold failed: {}", exc)
        raise typer.Exit(code=1)
    typer.echo(f"Scaffolded {len(result.files)} file(s) into {result.out_dir}")
    typer.echo(f"Run it: cd {result.out_dir} && python run_pipeline.py")
