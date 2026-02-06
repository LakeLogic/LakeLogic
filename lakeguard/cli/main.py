import typer
from pathlib import Path
from typing import Optional, Any, Dict, List
from loguru import logger
import sys
import yaml

from lakeguard.core.processor import DataProcessor

app = typer.Typer(
    help="LakeGuard - Consistent Data Contracts across engines.",
    add_completion=False,
)

@app.command()
def run(
    contract: Path = typer.Option(
        ..., "--contract", "-c", help="Path to the contract YAML file."
    ),
    source: Path = typer.Option(
        ..., "--source", "-s", help="Path to the source data file (CSV/Parquet) or table name for warehouse engines."
    ),
    engine: str = typer.Option(
        "polars", "--engine", "-e", help="Execution engine (polars, pandas, duckdb, spark, snowflake, bigquery)."
    ),
    output_good: Optional[Path] = typer.Option(
        None, "--output-good", help="Path to save good records (CSV/Parquet)."
    ),
    output_bad: Optional[Path] = typer.Option(
        None, "--output-bad", help="Path to save quarantined records (CSV/Parquet)."
    ),
    output_format: Optional[str] = typer.Option(
        None,
        "--output-format",
        help="Format for --output-good/--output-bad (csv|parquet). Defaults to CSV or inferred from file extension.",
    ),
    materialize: bool = typer.Option(
        False, "--materialize/--no-materialize", help="Write good data to the contract materialization target."
    ),
    materialize_target: Optional[Path] = typer.Option(
        None, "--materialize-target", help="Override materialization target path."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging."),
):
    """
    Run a data contract against a source file.

    Args:
        contract: Path to the contract YAML file.
        source: Path to the source data file.
        engine: Execution engine name.
        output_good: Optional CSV output for good records.
        output_bad: Optional CSV output for quarantined records.
        output_format: Format for output_good/output_bad (csv|parquet).
        materialize: Whether to write good data to the contract target.
        materialize_target: Optional override materialization target path.
        verbose: Enable debug logging.
    """
    def _resolve_output_format(path: Path, fmt: Optional[str]) -> str:
        """
        Resolve the output format based on CLI option or file extension.

        Args:
            path: Output path.
            fmt: Optional format override.

        Returns:
            Output format string.
        """
        if fmt:
            value = fmt.strip().lower().lstrip(".")
            if value not in ["csv", "parquet"]:
                raise typer.BadParameter("output_format must be csv or parquet.")
            return value
        if path and path.suffix:
            ext = path.suffix.lower().lstrip(".")
            if ext in ["csv", "parquet"]:
                return ext
        return "csv"

    def _write_output(df: Any, path: Path, fmt: str, engine_name: str) -> None:
        """
        Write output dataframe to disk in the requested format.

        Args:
            df: Dataframe to write.
            path: Destination path.
            fmt: Output format (csv|parquet).
            engine_name: Engine name for backend-specific writers.
        """
        if engine_name == "spark":
            if fmt == "csv":
                df.write.mode("overwrite").option("header", "true").csv(str(path))
            else:
                df.write.mode("overwrite").parquet(str(path))
            return

        if fmt == "csv":
            if hasattr(df, "write_csv"):
                df.write_csv(path)
            elif hasattr(df, "to_csv"):
                df.to_csv(path, index=False)
            else:
                raise ValueError("Unsupported dataframe type for CSV output.")
            return

        if fmt == "parquet":
            if hasattr(df, "write_parquet"):
                df.write_parquet(path)
            elif hasattr(df, "to_parquet"):
                df.to_parquet(path, index=False)
            else:
                raise ValueError("Unsupported dataframe type for Parquet output.")
            return

        raise ValueError(f"Unsupported output format: {fmt}")
    # Configure logging
    logger.remove()
    log_level = "DEBUG" if verbose else "INFO"
    logger.add(sys.stderr, level=log_level, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")

    if engine not in ["snowflake", "bigquery"]:
        if not source.exists():
            logger.error(f"Source file not found: {source}")
            raise typer.Exit(code=1)

    try:
        processor = DataProcessor(engine=engine, contract=contract)
        good_df, bad_df = processor.run_source(source)

        if materialize:
            processor.materialize(good_df, bad_df, target_path=materialize_target)
        
        # Save results
        if output_good:
            out_fmt = _resolve_output_format(output_good, output_format)
            _write_output(good_df, output_good, out_fmt, engine)
            logger.info(f"Saved good records to {output_good}")
        
        if output_bad:
            out_fmt = _resolve_output_format(output_bad, output_format)
            _write_output(bad_df, output_bad, out_fmt, engine)
            logger.info(f"Saved quarantined records to {output_bad}")
            
    except Exception as e:
        logger.exception(f"Fatal error during execution: {e}")
        raise typer.Exit(code=1)


@app.command()
def bootstrap(
    landing: Path = typer.Option(..., "--landing", help="Landing zone root path."),
    output_dir: Path = typer.Option(..., "--output-dir", help="Directory to write generated contracts."),
    registry: Path = typer.Option(..., "--registry", help="Path to write registry YAML."),
    format: str = typer.Option("csv", "--format", help="Input file format (csv|parquet|json)."),
    pattern: str = typer.Option("*.csv", "--pattern", help="File glob pattern."),
    layer: str = typer.Option("bronze", "--layer", help="Layer name prefix for datasets."),
    sample_rows: int = typer.Option(1000, "--sample-rows", help="Rows to sample for schema inference."),
):
    """
    Bootstrap contracts and registry from a landing zone.

    Args:
        landing: Landing zone root path.
        output_dir: Output directory for generated contracts.
        registry: Registry YAML output path.
        format: Input file format.
        pattern: File glob pattern.
        layer: Dataset layer prefix.
        sample_rows: Rows to sample for schema inference.
    """
    if not landing.exists():
        logger.error(f"Landing path not found: {landing}")
        raise typer.Exit(code=1)

    def _infer_fields(file_path: Path) -> List[Dict[str, Any]]:
        import pandas as pd

        fmt = format.lower()
        if fmt == "csv":
            df = pd.read_csv(file_path, nrows=sample_rows)
        elif fmt == "parquet":
            df = pd.read_parquet(file_path)
        elif fmt == "json":
            df = pd.read_json(file_path, lines=True)
        else:
            raise typer.BadParameter("format must be csv, parquet, or json.")

        type_map = {
            "int64": "integer",
            "int32": "integer",
            "float64": "double",
            "float32": "double",
            "bool": "boolean",
        }
        fields = []
        for col, dtype in df.dtypes.items():
            dtype_name = str(dtype).lower()
            if "datetime" in dtype_name:
                field_type = "timestamp"
            else:
                field_type = type_map.get(dtype_name, "string")
            fields.append({"name": col, "type": field_type})
        return fields

    def _discover_entities(root: Path) -> Dict[str, List[Path]]:
        subdirs = [p for p in root.iterdir() if p.is_dir()]
        if subdirs:
            entities: Dict[str, List[Path]] = {}
            for subdir in subdirs:
                files = sorted(subdir.glob(pattern))
                if files:
                    entities[subdir.name] = files
            return entities

        files = sorted(root.glob(pattern))
        entities = {}
        for file_path in files:
            stem = file_path.stem
            key = stem.split("_")[0] if "_" in stem else stem
            entities.setdefault(key, []).append(file_path)
        return entities

    entities = _discover_entities(landing)
    if not entities:
        logger.error("No files discovered in landing zone.")
        raise typer.Exit(code=1)

    output_dir.mkdir(parents=True, exist_ok=True)
    registry_entries = []

    for entity, files in entities.items():
        sample_file = files[0]
        fields = _infer_fields(sample_file)
        dataset = f"{layer}_{entity}"
        contract = {
            "version": "1.0.0",
            "info": {
                "title": f"Bootstrap {entity} ({layer})",
                "version": "1.0.0",
                "description": "Generated by lakeguard bootstrap.",
            },
            "server": {
                "type": "local",
                "path": str(sample_file),
                "format": format,
                "mode": "ingest",
                "schema_evolution": "strict",
                "allow_schema_drift": False,
            },
            "source": {
                "type": "landing",
                "path": str(sample_file.parent if sample_file.parent != landing else landing),
                "load_mode": "full",
                "pattern": pattern,
            },
            "dataset": dataset,
            "model": {"fields": fields},
            "materialization": {
                "strategy": "append",
                "target_path": str(output_dir / "output" / layer / entity),
                "format": "parquet",
            },
            "quarantine": {
                "enabled": True,
                "target": str(output_dir / "output" / "quarantine" / entity),
            },
        }

        contract_path = output_dir / f"{dataset}.yaml"
        contract_path.write_text(
            yaml.safe_dump(contract, sort_keys=False),
            encoding="utf-8",
        )

        registry_entries.append(
            {
                "entity": entity,
                "enabled": True,
                "contracts": {layer: contract_path.name},
            }
        )

    registry.parent.mkdir(parents=True, exist_ok=True)
    registry.write_text(
        yaml.safe_dump({"entries": registry_entries}, sort_keys=False),
        encoding="utf-8",
    )

    logger.info(f"Generated {len(registry_entries)} contracts in {output_dir}")
    logger.info(f"Registry written to {registry}")

if __name__ == "__main__":
    app()
