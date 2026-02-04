import typer
import polars as pl
from pathlib import Path
from typing import Optional
from lakeguard.core.processor import DataProcessor

app = typer.Typer(help="LakeGuard CLI - Consistent Data Contracts across engines.")

@app.command()
def run(
    contract: Path = typer.Option(..., "--contract", "-c", help="Path to the contract YAML file."),
    source: Path = typer.Option(..., "--source", "-s", help="Path to the source data file (CSV/Parquet)."),
    engine: str = typer.Option("polars", "--engine", "-e", help="Execution engine (polars, pandas)."),
    output_good: Optional[Path] = typer.Option(None, "--output-good", help="Path to save good records."),
    output_bad: Optional[Path] = typer.Option(None, "--output-bad", help="Path to save quarantined records."),
):
    """
    Run a data contract against a source file.
    """
    if not source.exists():
        typer.echo(f"Error: Source file not found: {source}")
        raise typer.Exit(code=1)

    # Load data based on extension
    # For MVP, we support CSV and Parquet via Polars
    typer.echo(f"🚀 Loading data from {source}...")
    if source.suffix == ".csv":
        df = pl.read_csv(source)
    elif source.suffix == ".parquet":
        df = pl.read_parquet(source)
    else:
        typer.echo(f"Error: Unsupported file format {source.suffix}. Use .csv or .parquet")
        raise typer.Exit(code=1)

    typer.echo(f"🛡️  Applying contract {contract} using {engine} engine...")
    
    try:
        processor = DataProcessor(engine=engine, contract=contract)
        good_df, bad_df = processor.run(df)
        
        good_count = len(good_df)
        bad_count = len(bad_df)
        
        typer.echo(f"✅ Processed {len(df)} records.")
        typer.echo(f"   - Good: {good_count}")
        typer.echo(f"   - Quarantined: {bad_count}")
        
        if output_good:
            good_df.write_csv(output_good)
            typer.echo(f"💾 Saved good records to {output_good}")
        
        if output_bad:
            bad_df.write_csv(output_bad)
            typer.echo(f"💾 Saved quarantined records to {output_bad}")
            
        if bad_count > 0 and not output_bad:
            typer.echo("⚠️  Warning: Found invalid records but no --output-bad was specified.")

    except Exception as e:
        typer.echo(f"❌ Error: {str(e)}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
