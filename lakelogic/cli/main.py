import typer
from pathlib import Path
from typing import Optional, Any, Dict, List
from loguru import logger
import sys
import yaml
import json

from lakelogic.core.processor import DataProcessor

app = typer.Typer(
    help="LakeLogic - Consistent Data Contracts across engines.",
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
    stage: Optional[str] = typer.Option(
        None, "--stage", help="Apply contract stage overrides (e.g., bronze or silver)."
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
    trace: bool = typer.Option(False, "--trace", help="Display detailed execution trace."),
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
    
    # Configure logging with multi-line splitting for long messages
    logger.remove()
    log_level = "DEBUG" if verbose else "INFO"
    max_line_length = 120  # Maximum characters per line
    
    def split_long_message(record):
        """Split long log messages into multiple lines for readability."""
        message = record["message"]
        if len(message) <= max_line_length:
            return True
        
        # Split at word boundaries
        words = message.split()
        lines = []
        current_line = ""
        
        for word in words:
            if len(current_line) + len(word) + 1 <= max_line_length:
                current_line += (word + " ")
            else:
                if current_line:
                    lines.append(current_line.rstrip())
                current_line = "  " + word + " "  # Indent continuation lines
        
        if current_line:
            lines.append(current_line.rstrip())
        
        record["message"] = "\n".join(lines)
        return True
    
    logger.add(
        sys.stderr, 
        level=log_level, 
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        filter=split_long_message
    )

    if engine not in ["snowflake", "bigquery"]:
        if not source.exists():
            logger.error(f"Source file not found: {source}")
            raise typer.Exit(code=1)

    try:
        processor = DataProcessor(engine=engine, contract=contract, stage=stage)
        result = processor.run_source(source)
        good_df, bad_df = result.good, result.bad

        if trace and result.trace:
            _display_trace(result.trace)

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

def _display_trace(trace: Any):
    """Display execution trace in a formatted table."""
    import typer
    from datetime import datetime
    
    typer.echo("")
    typer.echo(typer.style(" 🔍 EXECUTION TRACE", fg=typer.colors.CYAN, bold=True))
    typer.echo(typer.style(f" Run ID: {trace.run_id}", dim=True))
    typer.echo(" " + "─" * 110)
    
    header = f" {'STEP':<30} | {'STATUS':<8} | {'IN':>10} | {'OUT':>10} | {'DURATION':>12} | {'DETAILS'}"
    typer.echo(typer.style(header, bold=True))
    typer.echo(" " + "─" * 110)
    
    for step in trace.steps:
        status_color = typer.colors.GREEN if step.status == "ok" else typer.colors.RED
        status_text = typer.style(f"{step.status.upper():<8}", fg=status_color)
        
        in_rows = f"{step.input_rows:,}" if step.input_rows is not None else "-"
        out_rows = f"{step.output_rows:,}" if step.output_rows is not None else "-"
        duration = f"{step.duration_ms:,.2f}ms" if step.duration_ms is not None else "-"
        
        details = ""
        if step.details:
            if "sql" in step.details:
                sql = step.details["sql"].strip().replace("\n", " ")
                if len(sql) > 40:
                    sql = sql[:37] + "..."
                details = f"SQL: {sql}"
            elif "errors" in step.details and step.details["errors"]:
                details = f"Errors: {len(step.details['errors'])}"
            elif "path" in step.details:
                details = f"Path: {step.details['path']}"
        
        row = f" {step.step:<30} | {status_text} | {in_rows:>10} | {out_rows:>10} | {duration:>12} | {details}"
        typer.echo(row)
    
    typer.echo(" " + "─" * 110)
    total_dur = f"{trace.total_duration_ms:,.2f}ms" if trace.total_duration_ms else "n/a"
    typer.echo(typer.style(f" TOTAL DURATION: {total_dur}", bold=True))
    typer.echo("")


@app.command("setup-oss")
def setup_oss():
    """
    Setup the OSS engine environment by pre-installing required extensions and checking dependencies.
    """
    logger.info("Setting up LakeLogic OSS environment...")
    
    # Check deltalake
    try:
        import deltalake
        logger.info("✅ deltalake is installed.")
    except ImportError:
        logger.warning("❌ deltalake is NOT installed. Run: pip install \"lakelogic[duckdb]\" or pip install deltalake")

    # Setup DuckDB extensions
    try:
        from lakelogic.engines.duckdb import DuckDBAdapter
        DuckDBAdapter.setup_extensions()
        logger.info("✅ DuckDB extensions setup complete.")
    except Exception as e:
        logger.error(f"❌ Failed to setup DuckDB extensions: {e}")
        
    logger.info("OSS environment setup finished.")


@app.command()
def bootstrap(
    landing: Path = typer.Option(..., "--landing", help="Landing zone root path."),
    output_dir: Path = typer.Option(..., "--output-dir", help="Directory to write generated contracts."),
    registry: Path = typer.Option(..., "--registry", help="Path to write registry YAML."),
    format: str = typer.Option("csv", "--format", help="Input file format (csv|parquet|json)."),
    pattern: str = typer.Option("*.csv", "--pattern", help="File glob pattern."),
    layer: str = typer.Option("bronze", "--layer", help="Layer name prefix for datasets."),
    sample_rows: int = typer.Option(1000, "--sample-rows", help="Rows to sample for schema inference."),
    sync: bool = typer.Option(False, "--sync", help="Sync registry/contracts with landing zone."),
    sync_update_schema: bool = typer.Option(False, "--sync-update-schema", help="Update schema for existing contracts."),
    sync_overwrite: bool = typer.Option(False, "--sync-overwrite", help="Overwrite existing contracts."),
    profile: bool = typer.Option(False, "--profile", help="Generate a data profile for each entity."),
    detect_pii: bool = typer.Option(False, "--detect-pii", help="Detect PII using Presidio."),
    suggest_rules: bool = typer.Option(False, "--suggest-rules", help="Suggest quality rules from profile."),
    profile_output_dir: Optional[Path] = typer.Option(None, "--profile-output-dir", help="Directory for profile reports."),
    pii_sample_size: int = typer.Option(50, "--pii-sample-size", help="Number of sample values per column for PII detection."),
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
    def _flag(value: Any) -> bool:
        return True if value is True else False

    sync = _flag(sync)
    sync_update_schema = _flag(sync_update_schema)
    sync_overwrite = _flag(sync_overwrite)
    profile = _flag(profile)
    detect_pii = _flag(detect_pii)
    suggest_rules = _flag(suggest_rules)
    if not isinstance(profile_output_dir, Path):
        profile_output_dir = None
    if not isinstance(pii_sample_size, int):
        pii_sample_size = 50

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

    def _load_dataframe(file_path: Path):
        import pandas as pd

        fmt = format.lower()
        if fmt == "csv":
            return pd.read_csv(file_path, nrows=sample_rows)
        if fmt == "parquet":
            return pd.read_parquet(file_path)
        if fmt == "json":
            return pd.read_json(file_path, lines=True)
        raise typer.BadParameter("format must be csv, parquet, or json.")

    def _profile_dataframe(df) -> Dict[str, Any]:
        try:
            from dataprofiler import Profiler
        except Exception as exc:
            raise typer.BadParameter(
                "DataProfiler not installed. Install with: pip install \"lakelogic[profiling]\""
            ) from exc
        profiler = Profiler(df)
        return profiler.profile

    def _detect_pii_for_fields(df) -> Dict[str, str]:
        try:
            from presidio_analyzer import AnalyzerEngine
        except Exception as exc:
            raise typer.BadParameter(
                "Presidio not installed. Install with: pip install \"lakelogic[profiling]\""
            ) from exc

        analyzer = AnalyzerEngine()
        pii_map: Dict[str, str] = {}
        for col in df.columns:
            series = df[col].dropna().astype(str).head(pii_sample_size)
            found = []
            for value in series.tolist():
                try:
                    results = analyzer.analyze(text=value, language="en")
                except Exception:
                    results = []
                for result in results:
                    found.append(result.entity_type)
            if found:
                # Choose the most common entity type
                entity = max(set(found), key=found.count)
                pii_map[col] = entity
        return pii_map

    def _suggest_quality_rules(df) -> Dict[str, Any]:
        rules = {"row_rules": [], "dataset_rules": []}
        total = len(df)
        if total == 0:
            return rules

        for col in df.columns:
            series = df[col]
            null_ratio = series.isna().mean()
            distinct = series.nunique(dropna=True)
            if null_ratio < 0.01:
                rules["row_rules"].append({"not_null": col})
            if distinct == total and total > 1:
                rules["dataset_rules"].append({"unique": col})
            if distinct > 0 and distinct <= 20 and series.dtype == "object":
                values = [v for v in series.dropna().unique().tolist() if v is not None]
                rules["row_rules"].append({"accepted_values": {"field": col, "values": values}})
        return rules

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
    existing_registry = {}
    existing_entries = {}
    if sync and registry.exists():
        existing_registry = yaml.safe_load(registry.read_text(encoding="utf-8")) or {}
        for entry in existing_registry.get("entries", []):
            name = str(entry.get("entity") or "").strip()
            if name:
                existing_entries[name] = entry

    for entity, files in entities.items():
        sample_file = files[0]
        fields = _infer_fields(sample_file)
        df = None
        profile_data = None
        pii_map: Dict[str, str] = {}
        if profile or detect_pii or suggest_rules:
            df = _load_dataframe(sample_file)
        if profile:
            profile_data = _profile_dataframe(df)
        if detect_pii:
            pii_map = _detect_pii_for_fields(df)
        suggested_rules = _suggest_quality_rules(df) if suggest_rules else None
        dataset = f"{layer}_{entity}"
        contract_path = output_dir / f"{dataset}.yaml"

        if sync and entity in existing_entries and contract_path.exists():
            if sync_overwrite:
                pass
            elif sync_update_schema:
                try:
                    existing_contract = yaml.safe_load(contract_path.read_text(encoding="utf-8")) or {}
                    model = existing_contract.get("model") or {}
                    existing_fields = model.get("fields") or []
                    existing_names = {f.get("name") for f in existing_fields if isinstance(f, dict)}
                    for field in fields:
                        if field["name"] not in existing_names:
                            existing_fields.append(field)
                    existing_contract["model"] = {"fields": existing_fields}
                    contract_path.write_text(
                        yaml.safe_dump(existing_contract, sort_keys=False),
                        encoding="utf-8",
                    )
                    registry_entries.append(existing_entries[entity])
                    continue
                except Exception as exc:
                    logger.warning(f"Failed to update schema for {entity}: {exc}")
                    registry_entries.append(existing_entries[entity])
                    continue
            else:
                registry_entries.append(existing_entries[entity])
                continue

        model_fields = []
        for field in fields:
            field_name = field.get("name")
            if field_name and field_name in pii_map:
                field["pii"] = True
                field["classification"] = pii_map[field_name].lower()
            model_fields.append(field)

        contract = {
            "version": "1.0.0",
            "info": {
                "title": f"Bootstrap {entity} ({layer})",
                "version": "1.0.0",
                "description": "Generated by lakelogic bootstrap.",
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
            "model": {"fields": model_fields},
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

        if suggested_rules:
            contract["quality"] = suggested_rules

        if profile_data is not None:
            profile_dir = profile_output_dir or (output_dir / "profiles")
            profile_dir.mkdir(parents=True, exist_ok=True)
            profile_path = profile_dir / f"{entity}_profile.json"
            profile_path.write_text(json.dumps(profile_data, indent=2, default=str), encoding="utf-8")

        contract_path.write_text(
            yaml.safe_dump(contract, sort_keys=False),
            encoding="utf-8",
        )
        if entity in existing_entries:
            entry = existing_entries[entity]
            contracts_block = entry.get("contracts") or {}
            if isinstance(contracts_block, dict):
                contracts_block[layer] = contract_path.name
                entry["contracts"] = contracts_block
            registry_entries.append(entry)
        else:
            registry_entries.append(
                {
                    "entity": entity,
                    "enabled": True,
                    "contracts": {layer: contract_path.name},
                }
            )

    if sync and existing_registry.get("entries"):
        seen = {e.get("entity") for e in registry_entries}
        for entry in existing_registry.get("entries", []):
            if entry.get("entity") not in seen:
                registry_entries.append(entry)

    registry.parent.mkdir(parents=True, exist_ok=True)
    registry.write_text(
        yaml.safe_dump({"entries": registry_entries}, sort_keys=False),
        encoding="utf-8",
    )

    logger.info(f"Generated {len(registry_entries)} contracts in {output_dir}")
    logger.info(f"Registry written to {registry}")


@app.command("help")
def help_command(topic: Optional[str] = typer.Argument(None)):
    """
    Show contextual help for LakeLogic commands.
    """
    base = """LakeLogic Help

Commands:
  run          Run a contract against a source file.
  bootstrap    Generate starter contracts and a registry from a landing zone.
  help         Show help for a command (driver, bootstrap).

Examples:
  lakelogic help
  lakelogic help bootstrap
"""
    driver = """LakeLogic Driver Help

Use lakelogic-driver to run registry-driven Bronze -> Silver -> Gold pipelines.

Examples:
  lakelogic-driver --registry contracts/_registry.yaml --layers bronze
  lakelogic-driver --window range --window-start-date 2026-02-01 --window-end-date 2026-02-05
  lakelogic-driver --policy-pack baseline_silver --policy-pack-dir policy_packs
"""
    bootstrap_text = """LakeLogic Bootstrap Help

Generate contracts and registry from a landing zone.

Example:
  lakelogic bootstrap --landing data/landing --output-dir contracts/new --registry contracts/new/_registry.yaml

Sync mode:
  lakelogic bootstrap --landing data/landing --output-dir contracts/new --registry contracts/new/_registry.yaml --sync
"""
    if not topic:
        typer.echo(base)
        return
    topic = topic.lower()
    if topic in ["driver", "lakelogic-driver"]:
        typer.echo(driver)
        return
    if topic in ["bootstrap", "boot"]:
        typer.echo(bootstrap_text)
        return
    typer.echo(base)

if __name__ == "__main__":
    app()
