import typer
from pathlib import Path
from typing import Optional
from loguru import logger
import polars as pl
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
        ..., "--source", "-s", help="Path to the source data file (CSV/Parquet)."
    ),
    engine: str = typer.Option(
        "polars", "--engine", "-e", help="Execution engine (polars, pandas, duckdb, spark)."
    ),
    output_good: Optional[Path] = typer.Option(
        None, "--output-good", help="Path to save good records (CSV)."
    ),
    output_bad: Optional[Path] = typer.Option(
        None, "--output-bad", help="Path to save quarantined records (CSV)."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging."),
):
    """
    Run a data contract against a source file.
    """
    # Configure logging
    logger.remove()
    log_level = "DEBUG" if verbose else "INFO"
    logger.add(sys.stderr, level=log_level, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")

    if not source.exists():
        logger.error(f"Source file not found: {source}")
        raise typer.Exit(code=1)

    # Load data
    # We use polars for initial loading as it's fast and engine-neutral for file reading
    try:
        if source.suffix == ".csv":
            df = pl.read_csv(source)
        elif source.suffix == ".parquet":
            df = pl.read_parquet(source)
        else:
            logger.error(f"Unsupported file format {source.suffix}. Use .csv or .parquet")
            raise typer.Exit(code=1)
            
        # If engine is pandas, convert now (or let the adapter do it)
        if engine == "pandas":
            df = df.to_pandas()
        elif engine == "spark":
            # For Spark we need an active session
            try:
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.appName("LakeGuardCLI").getOrCreate()
                df = spark.createDataFrame(df.to_pandas())
            except ImportError:
                logger.error("pyspark is not installed. Required for --engine spark")
                raise typer.Exit(code=1)

        processor = DataProcessor(engine=engine, contract=contract)
        good_df, bad_df = processor.run(df)
        
        # Save results
        if output_good:
            # Handle different DF types for saving
            if hasattr(good_df, "write_csv"): # Polars
                good_df.write_csv(output_good)
            elif hasattr(good_df, "to_csv"): # Pandas
                good_df.to_csv(output_good, index=False)
            elif engine == "spark":
                good_df.toPandas().to_csv(output_good, index=False)
            logger.info(f"💾 Saved good records to {output_good}")
        
        if output_bad:
            if hasattr(bad_df, "write_csv"):
                bad_df.write_csv(output_bad)
            elif hasattr(bad_df, "to_csv"):
                bad_df.to_csv(output_bad, index=False)
            elif engine == "spark":
                bad_df.toPandas().to_csv(output_bad, index=False)
            logger.info(f"💾 Saved quarantined records to {output_bad}")
            
    except Exception as e:
        logger.exception(f"Fatal error during execution: {e}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
