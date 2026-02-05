import typer
from pathlib import Path
from typing import Optional
from loguru import logger
import sys

from lakeguard.core.processor import DataProcessor

app = typer.Typer(
    help="LakeGuard 🛡️ - Consistent Data Contracts across engines.",
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
            logger.info(f"💾 Saved good records to {output_good}")
        
        if output_bad:
            out_fmt = _resolve_output_format(output_bad, output_format)
            _write_output(bad_df, output_bad, out_fmt, engine)
            logger.info(f"💾 Saved quarantined records to {output_bad}")
            
    except Exception as e:
        logger.exception(f"Fatal error during execution: {e}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
