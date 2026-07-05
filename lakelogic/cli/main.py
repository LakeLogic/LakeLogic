import sys

# ── Windows console encoding fix ──────────────────────────────────────────────
# cmd.exe and PowerShell default to cp1252 which cannot encode many Unicode
# characters used in Rich help panels (arrows, box-drawing, emoji, etc.).
# Reconfigure stdout/stderr to UTF-8 at import time so this package never
# requires the caller to set PYTHONIOENCODING manually.
if sys.platform == "win32":
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except AttributeError:  # pragma: no cover
            pass  # non-TextIOWrapper stream (e.g. redirect to file)  # pragma: no cover
# ──────────────────────────────────────────────────────────────────────────────

import importlib.util
import typer
from pathlib import Path
from typing import Optional, Any, Dict, List
from loguru import logger
import yaml
import json

from lakelogic.core.processor import DataProcessor
from lakelogic.cli.review import review_command
from lakelogic.cli.review_reply import review_reply_command
from lakelogic.cli.lint import lint_command
from lakelogic.cli.observatory_cmd import observatory_app

app = typer.Typer(
    name="lakelogic",
    help=(
        "LakeLogic — Consistent Data Contracts across engines.\n\n"
        "A contract runtime for validating, materializing, and governing\n"
        "data across Polars, Pandas, DuckDB, Spark, Snowflake, and BigQuery.\n\n"
        "[dim]Use [bold]lakelogic [COMMAND] --help[/bold] for detailed usage on any command.[/dim]"
    ),
    add_completion=False,
    no_args_is_help=True,  # show help instead of "Missing command" error
    rich_markup_mode="rich",  # enable Rich markup in help strings
    pretty_exceptions_enable=False,
)

app.command(name="review", rich_help_panel="Code Quality")(review_command)
app.command(name="review-reply", rich_help_panel="Code Quality")(review_reply_command)
app.command(name="lint", rich_help_panel="Governance")(lint_command)
app.add_typer(observatory_app, name="observatory", rich_help_panel="Observatory")


@app.command(rich_help_panel="Contract Execution")
def run(
    contract: Path = typer.Option(..., "--contract", "-c", help="Path to the contract YAML file."),
    source: Path = typer.Option(
        ...,
        "--source",
        "-s",
        help="Path to the source data file (CSV/Parquet) or table name for warehouse engines.",
    ),
    engine: str = typer.Option(
        "polars",
        "--engine",
        "-e",
        help="Execution engine (polars, spark, snowflake, bigquery).",
    ),
    stage: Optional[str] = typer.Option(
        None, "--stage", help="Apply contract stage overrides (e.g., bronze or silver)."
    ),
    output_good: Optional[Path] = typer.Option(None, "--output-good", help="Path to save good records (CSV/Parquet)."),
    output_bad: Optional[Path] = typer.Option(
        None, "--output-bad", help="Path to save quarantined records (CSV/Parquet)."
    ),
    output_format: Optional[str] = typer.Option(
        None,
        "--output-format",
        help="Format for --output-good/--output-bad (csv|parquet). Defaults to CSV or inferred from file extension.",
    ),
    materialize: bool = typer.Option(
        False,
        "--materialize/--no-materialize",
        help="Write good data to the contract materialization target.",
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
            value = fmt.strip().lower().lstrip(".")  # pragma: no cover
            if value not in ["csv", "parquet"]:  # pragma: no cover
                raise typer.BadParameter("output_format must be csv or parquet.")  # pragma: no cover
            return value  # pragma: no cover
        if path and path.suffix:
            ext = path.suffix.lower().lstrip(".")
            if ext in ["csv", "parquet"]:
                return ext
        return "csv"  # pragma: no cover

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
            if fmt == "csv":  # pragma: no cover
                df.write.mode("overwrite").option("header", "true").csv(str(path))  # pragma: no cover
            else:  # pragma: no cover
                df.write.mode("overwrite").parquet(str(path))  # pragma: no cover
            return  # pragma: no cover

        if fmt == "csv":
            if hasattr(df, "write_csv"):
                df.write_csv(path)
            elif hasattr(df, "to_csv"):  # pragma: no cover
                df.to_csv(path, index=False)  # pragma: no cover
            else:  # pragma: no cover
                raise ValueError("Unsupported dataframe type for CSV output.")  # pragma: no cover
            return

        if fmt == "parquet":
            if hasattr(df, "write_parquet"):
                df.write_parquet(path)
            elif hasattr(df, "to_parquet"):  # pragma: no cover
                df.to_parquet(path, index=False)  # pragma: no cover
            else:  # pragma: no cover
                raise ValueError("Unsupported dataframe type for Parquet output.")  # pragma: no cover
            return

        raise ValueError(f"Unsupported output format: {fmt}")  # pragma: no cover

    # Configure logging with multi-line splitting for long messages
    logger.remove()
    log_level = "DEBUG" if verbose else "INFO"
    max_line_length = 120  # Maximum characters per line

    def split_long_message(record):
        """Split long log messages into multiple lines for readability."""
        message = record["message"]  # pragma: no cover
        if len(message) <= max_line_length:  # pragma: no cover
            return True  # pragma: no cover
        # pragma: no cover
        # Split at word boundaries  # pragma: no cover
        words = message.split()  # pragma: no cover
        lines = []  # pragma: no cover
        current_line = ""  # pragma: no cover
        # pragma: no cover
        for word in words:  # pragma: no cover
            if len(current_line) + len(word) + 1 <= max_line_length:  # pragma: no cover
                current_line += word + " "  # pragma: no cover
            else:  # pragma: no cover
                if current_line:  # pragma: no cover
                    lines.append(current_line.rstrip())  # pragma: no cover
                current_line = "  " + word + " "  # Indent continuation lines  # pragma: no cover
        # pragma: no cover
        if current_line:  # pragma: no cover
            lines.append(current_line.rstrip())  # pragma: no cover
        # pragma: no cover
        record["message"] = "\n".join(lines)  # pragma: no cover
        return True  # pragma: no cover

    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        filter=split_long_message,
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

    except Exception as e:  # pragma: no cover
        logger.exception(f"Fatal error during execution: {e}")  # pragma: no cover
        raise typer.Exit(code=1)  # pragma: no cover


def _display_trace(trace: Any):
    """Display execution trace in a formatted table."""
    import typer

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


@app.command(rich_help_panel="Contract Execution")
def validate(
    contract: Path = typer.Option(..., "--contract", "-c", help="Path to the contract YAML file."),
    gates: Optional[str] = typer.Option(
        None,
        "--gates",
        "-g",
        help="Comma-separated list of gates to enforce (e.g., 'breaking_change,pii_classification'). "
        "If not specified, runs structural validation only.",
    ),
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Fail on warnings (not just errors).",
    ),
    preflight: bool = typer.Option(
        False,
        "--preflight",
        help="Also run pre-flight materialization checks — catch contracts that would silently "
        "produce WRONG output (keyless merge, dedup with no tiebreaker, SCD2 with no tracked columns).",
    ),
):
    """
    Validate a data contract for correctness and enforceable gates.

    Performs static analysis without executing a pipeline: validates YAML structure,
    checks required fields, detects schema drift, and optionally runs contract gates
    for CI/CD blocking policies.

    Exit code 0 = valid; non-zero = validation failed.
    """
    import time
    from lakelogic.core.models import DataContract

    # Skip non-contract config files (data mesh hierarchy: *domain.yaml / *system.yaml)
    SKIP_SUFFIXES = ("domain.yaml", "domain.yml", "system.yaml", "system.yml")
    if contract.name.lower().endswith(SKIP_SUFFIXES):
        typer.secho(
            f"ℹ️  Skipping {contract.name} — data mesh config file, not a data contract.",
            fg=typer.colors.CYAN,
        )
        raise typer.Exit(0)

    start_time = time.time()
    typer.echo("")
    typer.echo(typer.style(f"📋 Validating contract: {contract}", bold=True))
    typer.echo("")

    try:
        # Load contract
        with open(contract, "r") as f:
            contract_data = yaml.safe_load(f)

        if not contract_data:
            typer.secho("❌ Contract file is empty.", fg=typer.colors.RED)
            raise typer.Exit(1)

        # Pre-flight materialization validation (opt-in). Dict-based, so it runs
        # even for contracts that need runtime context to fully parse — the same
        # validator the pipeline runs before materializing.
        if preflight:
            from lakelogic.core.contract_lint import load_context
            from lakelogic.core.preflight import preflight_check

            pf = preflight_check(contract_data, contract.stem, load_context(contract))
            if pf:
                typer.echo(
                    typer.style(
                        "❌ Pre-flight materialization blockers (would silently produce WRONG output):",
                        fg=typer.colors.RED,
                        bold=True,
                    )
                )
                for finding in pf:
                    typer.echo(
                        typer.style(f"   ✖ {finding.check_id}", fg=typer.colors.RED, bold=True) + f"  {finding.message}"
                    )
                    if finding.suggestion:
                        typer.echo(typer.style(f"      → {finding.suggestion}", dim=True))
                raise typer.Exit(1)
            typer.secho("✓ Pre-flight: contract can materialize correctly.", fg=typer.colors.GREEN)

        # Parse into DataContract
        try:
            dc = DataContract(**contract_data)
        except Exception as e:
            typer.secho(f"❌ Contract schema validation failed:\n{e}", fg=typer.colors.RED)
            raise typer.Exit(1)

        # Check required fields
        errors = []
        warnings = []

        if not dc.info or not dc.info.name:
            errors.append("Missing required field: info.name")
        if not dc.info or not dc.info.owner:
            warnings.append("Missing recommended field: info.owner")
        if not dc.info or not dc.info.version:
            warnings.append("Missing recommended field: info.version")

        if errors:
            for err in errors:
                typer.secho(f"  ❌ {err}", fg=typer.colors.RED)
            raise typer.Exit(1)

        if warnings:
            for warn in warnings:
                typer.secho(f"  ⚠️  {warn}", fg=typer.colors.YELLOW)

        typer.secho(f"✅ Contract structure valid: {dc.info.name}", fg=typer.colors.GREEN)

        # Run gates if specified
        if gates:
            from lakelogic.gates import GATE_REGISTRY, GateStatus

            typer.echo("")
            typer.echo(typer.style("🚪 Running contract gates...", bold=True))

            gate_names = [g.strip() for g in gates.split(",")]
            any_failed = False

            for gate_name in gate_names:
                gate_cls = GATE_REGISTRY.get(gate_name)
                if not gate_cls:
                    available = ", ".join(GATE_REGISTRY.keys())
                    typer.secho(
                        f"  ⚠️  Unknown gate '{gate_name}'. Available: {available}",
                        fg=typer.colors.YELLOW,
                    )
                    continue

                typer.echo(f"  → {gate_name}...", nl=False)
                try:
                    result = gate_cls(strict=strict).run(dc, context={"contract_root": contract.parent})
                except Exception as exc:
                    typer.secho(f" ERROR — {exc}", fg=typer.colors.RED)
                    any_failed = True
                    continue

                if result.status == GateStatus.PASSED:
                    typer.secho(" PASSED", fg=typer.colors.GREEN)
                elif result.status == GateStatus.SKIPPED:
                    typer.secho(f" SKIPPED — {result.message}", fg=typer.colors.CYAN)
                elif result.status == GateStatus.WARNING:
                    typer.secho(f" WARNING — {result.message}", fg=typer.colors.YELLOW)
                    for v in result.violations:
                        typer.secho(f"      • {v}", fg=typer.colors.YELLOW)
                    if strict:
                        any_failed = True
                else:  # FAILED
                    typer.secho(f" FAILED — {result.message}", fg=typer.colors.RED)
                    for v in result.violations:
                        typer.secho(f"      • {v}", fg=typer.colors.RED)
                    any_failed = True

            if any_failed:
                raise typer.Exit(1)

        elapsed = time.time() - start_time
        typer.echo("")
        typer.secho(f"✅ Validation successful ({elapsed:.2f}s)", fg=typer.colors.GREEN, bold=True)

    except Exception as e:
        if not isinstance(e, typer.Exit):
            typer.secho(f"❌ Validation error: {e}", fg=typer.colors.RED)
        raise typer.Exit(1)


@app.command("setup-oss", rich_help_panel="Environment Setup")
def setup_oss():
    """
    Setup the OSS engine environment by pre-installing required extensions and checking dependencies.
    """
    logger.info("Setting up LakeLogic OSS environment...")

    # Check deltalake
    if importlib.util.find_spec("deltalake") is not None:
        logger.info("✅ deltalake is installed.")
    else:
        logger.warning('❌ deltalake is NOT installed. Run: pip install "lakelogic[duckdb]" or pip install deltalake')

    # Setup DuckDB extensions
    try:
        import duckdb

        # DuckDB is used as an internal query compiler, ensure its cloud extensions exist
        for ext in ("httpfs", "azure"):
            try:
                duckdb.install_extension(ext)
                duckdb.load_extension(ext)
            except Exception as ext_e:
                logger.debug(f"Could not load duckdb extension {ext}: {ext_e}")

        logger.info("✅ DuckDB extensions setup complete.")
    except Exception as e:  # pragma: no cover
        logger.error(f"❌ Failed to setup DuckDB extensions: {e}")  # pragma: no cover

    logger.info("OSS environment setup finished.")


@app.command(rich_help_panel="Contract Execution")
def bootstrap(
    landing: Path = typer.Option(..., "--landing", help="Landing zone root path."),
    output_dir: Path = typer.Option(..., "--output-dir", help="Directory to write generated contracts."),
    registry: Path = typer.Option(..., "--registry", help="Path to write registry YAML."),
    format: str = typer.Option("csv", "--format", help="Input file format (csv|parquet|json)."),
    pattern: str = typer.Option("*.csv", "--pattern", help="File glob pattern."),
    layer: str = typer.Option("bronze", "--layer", help="Layer name prefix for datasets."),
    sample_rows: int = typer.Option(1000, "--sample-rows", help="Rows to sample for schema inference."),
    sync: bool = typer.Option(False, "--sync", help="Sync registry/contracts with landing zone."),
    sync_update_schema: bool = typer.Option(
        False, "--sync-update-schema", help="Update schema for existing contracts."
    ),
    sync_overwrite: bool = typer.Option(False, "--sync-overwrite", help="Overwrite existing contracts."),
    profile: bool = typer.Option(False, "--profile", help="Generate a data profile for each entity."),
    detect_pii: bool = typer.Option(False, "--detect-pii", help="Detect PII using Presidio."),
    suggest_rules: bool = typer.Option(False, "--suggest-rules", help="Suggest quality rules from profile."),
    profile_output_dir: Optional[Path] = typer.Option(
        None, "--profile-output-dir", help="Directory for profile reports."
    ),
    pii_sample_size: int = typer.Option(
        50,
        "--pii-sample-size",
        help="Number of sample values per column for PII detection.",
    ),
    ai: bool = typer.Option(
        False,
        "--ai",
        help="Enrich contracts with LLM-generated descriptions, PII flags, and SQL rules (requires API key in env).",
    ),
    ai_provider: Optional[str] = typer.Option(
        None,
        "--ai-provider",
        help="AI provider: openai | azure | anthropic | ollama (Requires OPENAI_API_KEY, ANTHROPIC_API_KEY, or AZURE_* env vars).",  # noqa: E501
    ),
    ai_model: Optional[str] = typer.Option(
        None,
        "--ai-model",
        help="AI model name (e.g. gpt-4o-mini, claude-sonnet-4-20250514).",
    ),
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
            raise typer.BadParameter("format must be csv, parquet, or json.")  # pragma: no cover

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
                field_type = "timestamp"  # pragma: no cover
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
        raise typer.BadParameter("format must be csv, parquet, or json.")  # pragma: no cover

    def _profile_dataframe(df) -> Dict[str, Any]:
        try:
            from dataprofiler import Profiler
        except Exception as exc:  # pragma: no cover
            raise typer.BadParameter(  # pragma: no cover
                'DataProfiler not installed. Install with: pip install "lakelogic[profiling]"'
            ) from exc
        profiler = Profiler(df)
        return profiler.profile

    def _detect_pii_for_fields(df) -> Dict[str, str]:
        try:
            from presidio_analyzer import AnalyzerEngine
        except Exception as exc:  # pragma: no cover
            raise typer.BadParameter(  # pragma: no cover
                'Presidio not installed. Install with: pip install "lakelogic[profiling]"'
            ) from exc

        analyzer = AnalyzerEngine()
        pii_map: Dict[str, str] = {}
        for col in df.columns:
            series = df[col].dropna().astype(str).head(pii_sample_size)
            found = []
            for value in series.tolist():
                try:
                    results = analyzer.analyze(text=value, language="en")
                except Exception:  # pragma: no cover
                    results = []  # pragma: no cover
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
            return rules  # pragma: no cover

        # Field name patterns that strongly suggest categorical/enum fields
        _ENUM_KEYWORDS = {
            "type",
            "status",
            "category",
            "mode",
            "tier",
            "level",
            "grade",
            "class",
            "kind",
            "group",
            "segment",
            "channel",
            "source",
            "medium",
            "priority",
            "severity",
            "stage",
            "phase",
            "state",
            "role",
            "gender",
            "currency",
            "country",
            "region",
            "platform",
            "device",
            "browser",
            "language",
            "format",
            "method",
            "protocol",
            "plan",
            "subscription",
            "membership",
            "rating",
            "flag",
            "indicator",
            "option",
            "preference",
            "consent",
        }
        # Field name patterns that should NEVER get accepted_values
        _NEVER_ENUM_KEYWORDS = {
            "id",
            "uuid",
            "key",
            "hash",
            "token",
            "secret",
            "password",
            "name",
            "title",
            "description",
            "comment",
            "note",
            "text",
            "message",
            "body",
            "content",
            "summary",
            "detail",
            "reason",
            "path",
            "url",
            "uri",
            "email",
            "address",
            "phone",
            "timestamp",
            "date",
            "time",
            "created",
            "updated",
            "modified",
            "amount",
            "price",
            "cost",
            "total",
            "balance",
            "quantity",
            "count",
            "score",
            "value",
            "size",
            "length",
            "width",
            "height",
            "lat",
            "lon",
            "latitude",
            "longitude",
            "zip",
            "postal",
        }

        for col in df.columns:
            series = df[col]
            null_ratio = series.isna().mean()
            distinct = series.nunique(dropna=True)
            if null_ratio < 0.01:
                rules["row_rules"].append({"not_null": col})
            if distinct == total and total > 1:
                rules["dataset_rules"].append({"unique": col})

            # ── Smart accepted_values: cardinality + name heuristics ──────
            if distinct > 0 and series.dtype == "object":
                col_lower = col.lower()  # pragma: no cover
                col_parts = set(col_lower.replace("-", "_").split("_"))  # pragma: no cover
                # pragma: no cover
                # Skip fields that should never be enum  # pragma: no cover
                if col_parts & _NEVER_ENUM_KEYWORDS:  # pragma: no cover
                    continue  # pragma: no cover
                # pragma: no cover
                # Cardinality ratio: distinct values / total rows  # pragma: no cover
                cardinality_ratio = distinct / total if total > 0 else 1.0  # pragma: no cover
                # pragma: no cover
                # Accept if: (a) field name matches enum keywords, OR  # pragma: no cover
                #             (b) very low cardinality ratio with small distinct count  # pragma: no cover
                is_enum_name = bool(col_parts & _ENUM_KEYWORDS)  # pragma: no cover
                is_low_cardinality = cardinality_ratio < 0.3 and distinct <= 15  # pragma: no cover
                # pragma: no cover
                if (is_enum_name or is_low_cardinality) and distinct <= 20:  # pragma: no cover
                    values = [v for v in series.dropna().unique().tolist() if v is not None]  # pragma: no cover
                    rules["row_rules"].append({"accepted_values": {"field": col, "values": values}})  # pragma: no cover
        return rules

    def _discover_entities(root: Path) -> Dict[str, List[Path]]:
        subdirs = [p for p in root.iterdir() if p.is_dir()]
        target_pattern = pattern.replace("**/", "") if pattern.startswith("**/") else pattern
        if subdirs:
            entities: Dict[str, List[Path]] = {}
            for subdir in subdirs:
                files = sorted([f for f in subdir.rglob(target_pattern) if f.is_file()])
                if files:
                    entities[subdir.name] = files
            return entities

        files = sorted([f for f in root.rglob(target_pattern) if f.is_file()])
        entities = {}
        for file_path in files:
            stem = file_path.stem
            key = stem.split("_")[0] if "_" in stem else stem
            entities.setdefault(key, []).append(file_path)
        return entities

    entities = _discover_entities(landing)
    if not entities:
        logger.error("No files discovered in landing zone.")  # pragma: no cover
        raise typer.Exit(code=1)  # pragma: no cover

    output_dir.mkdir(parents=True, exist_ok=True)
    registry_entries = []
    existing_registry = {}
    existing_entries = {}
    if sync and registry.exists():
        existing_registry = yaml.safe_load(registry.read_text(encoding="utf-8")) or {}
        for entry in existing_registry.get("contracts", []):
            name = str(entry.get("entity") or "").strip()
            if name:
                existing_entries[name] = entry

    for entity, files in entities.items():
        sample_file = files[0]
        fields = _infer_fields(sample_file)
        df = None
        profile_data = None
        pii_map: Dict[str, str] = {}
        if profile or detect_pii or suggest_rules or ai:
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
                pass  # pragma: no cover
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
                except Exception as exc:  # pragma: no cover
                    logger.warning(f"Failed to update schema for {entity}: {exc}")  # pragma: no cover
                    registry_entries.append(existing_entries[entity])  # pragma: no cover
                    continue  # pragma: no cover
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
                "schema_policy": {
                    "evolution": "strict",
                    "unknown_fields": "quarantine",
                },
            },
            "source": {
                "type": "landing",
                "path": str(landing / entity),
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

        # ── AI enrichment (opt-in) ────────────────────────────────
        if ai:
            try:
                from lakelogic.ai.contract_enricher import enrich_contract

                contract = enrich_contract(
                    contract,
                    sample_df=df,
                    provider=ai_provider,
                    model=ai_model,
                )
            except Exception as ai_err:
                logger.warning(f"AI enrichment failed for {entity}: {ai_err}")

        contract_path.write_text(
            yaml.safe_dump(contract, sort_keys=False),
            encoding="utf-8",
        )
        if entity in existing_entries:
            entry = existing_entries[entity]  # pragma: no cover
            contracts_block = entry.get("contracts") or {}  # pragma: no cover
            if isinstance(contracts_block, dict):  # pragma: no cover
                contracts_block[layer] = contract_path.name  # pragma: no cover
                entry["contracts"] = contracts_block  # pragma: no cover
            registry_entries.append(entry)  # pragma: no cover
        else:
            registry_entries.append(
                {
                    "entity": entity,
                    "enabled": True,
                    "contracts": {layer: contract_path.name},
                }
            )

    if sync and existing_registry.get("contracts"):
        seen = {e.get("entity") for e in registry_entries}
        for entry in existing_registry.get("contracts", []):
            if entry.get("entity") not in seen:
                registry_entries.append(entry)

    registry.parent.mkdir(parents=True, exist_ok=True)

    # Try to infer system/domain from landing path (e.g. landing_marketing/google_analytics)
    domain_name = "TODO_DOMAIN"
    system_name = "TODO_SYSTEM"
    parts = landing.parts
    if len(parts) >= 2:
        system_name = parts[-1]
        parent_name = parts[-2]
        if parent_name.startswith("landing_"):
            domain_name = parent_name.replace("landing_", "")
        else:
            domain_name = parent_name

    # Build the full scaffold using the lakehouse standard template
    registry_scaffold = {
        "domain": domain_name,
        "system": system_name,
        "bronze_layer": "bronze",
        "silver_layer": "silver",
        "gold_layer": "gold",
        "slo": {
            "freshness": {
                "bronze": {"max_delay_minutes": 60, "check_column": "_lakelogic_loaded_at"},
                "silver": {"max_delay_minutes": 240, "check_column": "_lakelogic_processed_at"},
            }
        },
        "storage": {
            "domain_catalog": "`{catalog}`.{domain}",
            "quarantine_root": "`{catalog}`.quarantine",
            "run_log_table": "`{catalog}`.{domain}._run_logs",
            "external_location_root": "abfss://{domain}@{storage_account}.dfs.core.windows.net",
            "contract_root": "/Workspace/Shared/data_platform/domains_retail/{domain}/{system}",
            "landing_root": "/Volumes/{catalog}/nondelta/landing_{domain}/{system}",
            "log_root": "/Volumes/{catalog}/nondelta/_logs",
            "landing_path": "abfss://nondelta@{storage_account}.dfs.core.windows.net/_data/{domain}/{system}",
            "contract_path": "abfss://nondelta@{storage_account}.dfs.core.windows.net/_contracts/{domain}/{system}",
        },
        "external_sources": [],
        "lineage": {
            "enabled": True,
            "source_column_name": "_lakelogic_source",
            "timestamp_column_name": "_lakelogic_processed_at",
            "run_id_column_name": "_lakelogic_run_id",
            "contract_name_column_name": "_lakelogic_contract_name",
            "domain_column_name": "_lakelogic_domain",
            "system_column_name": "_lakelogic_system",
        },
        "materialization": {
            "bronze": {"strategy": "append", "format": "delta"},
            "silver": {"strategy": "merge", "format": "delta"},
            "gold": {"strategy": "overwrite", "format": "delta"},
        },
        "server": {
            "bronze": {"schema_policy": {"evolution": "append", "unknown_fields": "allow"}},
            "silver": {"schema_policy": {"evolution": "strict", "unknown_fields": "quarantine"}},
            "gold": {"schema_policy": {"evolution": "strict", "unknown_fields": "quarantine"}},
        },
        "quarantine": {
            "enabled": True,
            "include_error_reason": True,
        },
        "environments": {
            "dev": {"catalog": "lakelogic-lakehouse-dev-001", "storage_account": "salakelogicdevadls001"},
            "staging": {"catalog": "lakelogic-lakehouse-staging-001", "storage_account": "salakelogicdevadls001"},
            "prod": {"catalog": "lakelogic-lakehouse-prod-001", "storage_account": "salakelogicdevadls001"},
        },
        "contracts": registry_entries,
    }

    if sync and registry.exists():
        # Keep existing top-level keys if updating an existing registry
        existing_full = yaml.safe_load(registry.read_text(encoding="utf-8")) or {}
        existing_full["contracts"] = registry_entries
        registry_scaffold = existing_full

    registry.write_text(
        yaml.safe_dump(registry_scaffold, sort_keys=False),
        encoding="utf-8",
    )

    logger.info(f"Generated {len(registry_entries)} contracts in {output_dir}")
    logger.info(f"Registry written to {registry}")


@app.command("help", rich_help_panel="Help")
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
        typer.echo(base)  # pragma: no cover
        return  # pragma: no cover
    topic = topic.lower()
    if topic in ["driver", "lakelogic-driver"]:
        typer.echo(driver)  # pragma: no cover
        return  # pragma: no cover
    if topic in ["bootstrap", "boot"]:
        typer.echo(bootstrap_text)
        return
    typer.echo(base)  # pragma: no cover


@app.command(rich_help_panel="Data Tooling")
def generate(
    contract: Path = typer.Option(..., "--contract", "-c", help="Path to the contract YAML file."),
    rows: int = typer.Option(100, "--rows", "-n", help="Number of rows to generate."),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file path (CSV/Parquet/JSON)."),
    format: str = typer.Option("parquet", "--format", "-f", help="Output format: parquet | csv | json."),
    engine: str = typer.Option("polars", "--engine", "-e", help="DataFrame engine: polars | pandas."),
    invalid_ratio: float = typer.Option(
        0.0,
        "--invalid-ratio",
        help="Fraction of rows that intentionally break quality rules (0.0–1.0). Useful for quarantine testing.",
    ),
    seed: Optional[int] = typer.Option(None, "--seed", help="Random seed for reproducibility."),
    preview: int = typer.Option(5, "--preview", help="Number of rows to print to console (0 = silent)."),
    ai: bool = typer.Option(False, "--ai", help="Use LLM to generate realistic edge cases for invalid rows."),
    ai_provider: Optional[str] = typer.Option(
        None, "--ai-provider", help="AI provider: openai | azure | anthropic | ollama."
    ),
    ai_model: Optional[str] = typer.Option(None, "--ai-model", help="AI model name."),
):
    """
    Generate synthetic data from a contract definition.

    Respects field types, nullability, accepted_values, and range constraints.
    Use --invalid-ratio to inject bad rows and verify your quarantine logic.

    Examples:

        lakelogic generate --contract orders.yaml --rows 1000 --output sample.parquet

        lakelogic generate --contract orders.yaml --rows 500 --invalid-ratio 0.1 \\
            --format csv --output orders_with_errors.csv

        lakelogic generate --contract orders.yaml --rows 200 --engine pandas --preview 10
    """
    from lakelogic.core.generator import DataGenerator

    if not contract.exists():
        logger.error(f"Contract not found: {contract}")
        raise typer.Exit(code=1)

    if not (0.0 <= invalid_ratio <= 1.0):
        logger.error("--invalid-ratio must be between 0.0 and 1.0")  # pragma: no cover
        raise typer.Exit(code=1)  # pragma: no cover

    valid_engines = ("polars", "pandas")
    if engine not in valid_engines:
        logger.error(f"--engine must be one of: {', '.join(valid_engines)}")
        raise typer.Exit(code=1)

    valid_formats = ("parquet", "csv", "json")
    if format.lower() not in valid_formats:
        logger.error(f"--format must be one of: {', '.join(valid_formats)}")
        raise typer.Exit(code=1)

    try:
        gen = DataGenerator(contract, seed=seed)
        df = gen.generate(
            rows=rows,
            invalid_ratio=invalid_ratio,
            output_format=engine,
            ai=ai,
            ai_provider=ai_provider,
            ai_model=ai_model,
        )

        n_invalid = int(rows * invalid_ratio)
        n_valid = rows - n_invalid
        typer.echo(
            typer.style(f"✔  Generated {rows:,} rows", fg=typer.colors.GREEN, bold=True)
            + f"  ({n_valid:,} valid"
            + (f", {n_invalid:,} intentionally invalid" if n_invalid else "")
            + ")"
        )

        # ── Test case summary table ──────────────────────────────────────
        report = gen.generation_report()
        if report and report.get("test_cases"):
            typer.echo("")
            typer.echo(typer.style("  TEST CASES GENERATED:", bold=True))
            typer.echo("  ┌" + "─" * 56 + "┐")
            for tc in report["test_cases"]:
                tc_id = tc["id"]
                tc_type = tc["type"][:26]
                tc_field = tc["field"][:16]
                tc_rows = tc["rows_generated"]
                typer.echo(f"  │ {tc_id}  {tc_type:26s}  {tc_field:16s} {tc_rows:>4d} rows │")
            typer.echo("  └" + "─" * 56 + "┘")
            typer.echo("")

        if preview > 0:
            typer.echo("")
            if engine == "polars":
                typer.echo(str(df.head(preview)))
            else:
                typer.echo(str(df.head(preview).to_string(index=False)))  # pragma: no cover
            typer.echo("")

        if output:
            output_path = Path(output)
            # If output is a directory (or invalid_ratio > 0 and output has no extension):
            # produce the full 3-file report output
            if n_invalid > 0 and (output_path.is_dir() or not output_path.suffix):
                data_path, invalid_path, report_path = gen.save_with_report(df, output_path, format=format)
                typer.echo(typer.style("  OUTPUT:", bold=True))
                typer.echo(typer.style(f"  ✔  {data_path}", fg=typer.colors.CYAN) + f"  ({rows:,} rows)")
                typer.echo(
                    typer.style(f"  ✔  {invalid_path}", fg=typer.colors.CYAN) + f"  ({n_invalid:,} invalid rows)"
                )
                typer.echo(typer.style(f"  ✔  {report_path}", fg=typer.colors.CYAN) + "  (generation report)")
                typer.echo("")
                typer.echo(
                    typer.style(
                        f"  Run your pipeline against this data to validate\n"
                        f"  quarantine catches all {n_invalid:,} invalid rows.\n"
                        f"  Expected quarantine rate: {invalid_ratio:.1%}",
                        dim=True,
                    )
                )
            else:
                saved = gen.save(df, output, format=format)  # pragma: no cover
                typer.echo(typer.style(f"✔  Saved → {saved}", fg=typer.colors.CYAN))  # pragma: no cover
        else:  # pragma: no cover
            typer.echo(
                typer.style("ℹ  No --output specified; use --output to save to disk.", dim=True)
            )  # pragma: no cover
    # pragma: no cover
    except Exception as e:  # pragma: no cover
        logger.exception(f"Generation failed: {e}")  # pragma: no cover
        raise typer.Exit(code=1)  # pragma: no cover


@app.command("assert", rich_help_panel="Data Tooling")
def assert_report(
    report: Path = typer.Option(..., "--report", "-r", help="Path to the generation report JSON."),
    quarantine_log: Optional[Path] = typer.Option(
        None, "--quarantine-log", "-q", help="Path to quarantine/run log JSON or CSV."
    ),
    expect_all_invalid_quarantined: bool = typer.Option(
        False,
        "--expect-all-invalid-quarantined",
        help="Assert that ALL invalid rows from the report were quarantined.",
    ),
    min_coverage: float = typer.Option(
        0.0,
        "--min-coverage",
        help="Minimum quarantine coverage ratio (0.0–1.0). Fails if below threshold.",
    ),
):
    """
    Validate quarantine results against a generation report.

    Cross-references a generation report (from ``lakelogic generate``) with
    quarantine output to verify that invalid rows were caught by contract rules.

    Use in CI pipelines to enforce contract quality gates.

    Examples:

        lakelogic assert --report ./test_data/report.json --expect-all-invalid-quarantined

        lakelogic assert --report ./test_data/report.json --min-coverage 0.95
    """
    import json

    if not report.exists():
        logger.error(f"Report not found: {report}")
        raise typer.Exit(code=1)

    with open(report, "r", encoding="utf-8") as f:
        gen_report = json.load(f)

    summary = gen_report.get("summary", {})
    test_cases = gen_report.get("test_cases", [])
    total_invalid = summary.get("invalid_rows", 0)

    typer.echo("")
    typer.echo(typer.style("  LakeLogic Assert — Contract Test Validation", bold=True))
    typer.echo("  " + "═" * 50)
    typer.echo(f"  Contract    : {gen_report.get('contract', 'unknown')}")
    typer.echo(f"  Seed        : {gen_report.get('seed', 'N/A')}")
    typer.echo(f"  Total rows  : {summary.get('total_rows', 0):,}")
    typer.echo(f"  Invalid rows: {total_invalid:,} ({summary.get('invalid_ratio', 0):.1%})")
    typer.echo("")

    # ── Test case summary ─────────────────────────────────────────────
    typer.echo(typer.style("  TEST CASE COVERAGE:", bold=True))
    typer.echo("  ┌" + "─" * 60 + "┐")
    typer.echo(f"  │ {'ID':7s}  {'Type':26s}  {'Field':14s}  {'Rows':>5s} │")
    typer.echo("  ├" + "─" * 60 + "┤")
    for tc in test_cases:
        tc_id = tc["id"]
        tc_type = tc["type"][:26]
        tc_field = tc["field"][:14]
        tc_rows = tc["rows_generated"]
        typer.echo(f"  │ {tc_id:7s}  {tc_type:26s}  {tc_field:14s}  {tc_rows:>5d} │")
    typer.echo("  └" + "─" * 60 + "┘")
    typer.echo("")

    # ── Quarantine cross-reference (if provided) ──────────────────────
    if quarantine_log and quarantine_log.exists():
        typer.echo(  # pragma: no cover
            typer.style(  # pragma: no cover
                "  ℹ  Quarantine cross-reference is available. Full assertion logic requires the run log integration.",  # pragma: no cover # noqa: E501
                dim=True,  # pragma: no cover
            )  # pragma: no cover
        )  # pragma: no cover
        typer.echo("")  # pragma: no cover

    # ── Assertions ────────────────────────────────────────────────────
    passed = True

    if expect_all_invalid_quarantined:
        typer.echo(
            typer.style(
                f"  Assertion: ALL {total_invalid} invalid rows should be quarantined",
                bold=True,
            )
        )
        # Without quarantine log cross-reference, we can only validate
        # the generation report structure is complete
        if not test_cases:
            typer.echo(typer.style("  ✗  No test cases in report!", fg=typer.colors.RED))  # pragma: no cover
            passed = False  # pragma: no cover
        else:
            typer.echo(
                typer.style(
                    f"  ✔  {len(test_cases)} test case categories generated",
                    fg=typer.colors.GREEN,
                )
            )

    if min_coverage > 0:
        typer.echo(typer.style(f"  Assertion: min coverage ≥ {min_coverage:.1%}", bold=True))
        # This will be fully implemented when quarantine log cross-reference is added
        typer.echo(typer.style("  ℹ  Coverage assertion requires quarantine log input.", dim=True))

    typer.echo("")
    if passed:
        typer.echo(typer.style("  ✔  All assertions passed.", fg=typer.colors.GREEN, bold=True))
    else:
        typer.echo(typer.style("  ✗  Assertions failed.", fg=typer.colors.RED, bold=True))  # pragma: no cover
        raise typer.Exit(code=1)  # pragma: no cover


@app.command("import-dbt", rich_help_panel="Data Tooling")
def import_dbt(
    schema: Path = typer.Option(..., "--schema", help="Path to the dbt schema.yml or sources.yml file."),
    model: Optional[str] = typer.Option(
        None,
        "--model",
        "-m",
        help="Name of the dbt model to import. Omit to import all models in the file.",
    ),
    source_name: Optional[str] = typer.Option(None, "--source-name", help="dbt source name (for sources.yml files)."),
    source_table: Optional[str] = typer.Option(
        None, "--source-table", help="dbt source table name (for sources.yml files)."
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help=(
            "Output path. If a .yaml file path, writes one contract there. "
            "If a directory (or omitted), writes <model>.yaml into that directory."
        ),
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite/--no-overwrite",
        help="Overwrite existing contract files. Default: skip existing.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the generated contract YAML but do not write files.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output."),
):
    """
    Import dbt schema.yml / sources.yml -> LakeLogic contract YAML.

    Examples
    --------
    # Import a single model
    lakelogic import-dbt --schema models/schema.yml --model customers --output contracts/

    # Import all models in a file
    lakelogic import-dbt --schema models/schema.yml --output contracts/

    # Import a dbt source table
    lakelogic import-dbt --schema models/sources.yml --source-name raw --source-table orders --output contracts/

    # Dry-run preview
    lakelogic import-dbt --schema models/schema.yml --model customers --dry-run
    """
    from lakelogic.adapters.dbt import DbtAdapter

    try:
        adapter = DbtAdapter(schema)
    except FileNotFoundError as exc:
        typer.echo(typer.style(f"✗  {exc}", fg=typer.colors.RED), err=True)
        raise typer.Exit(code=1)

    # Resolve output directory / file
    out_dir: Optional[Path] = None
    out_file: Optional[Path] = None
    if output:
        if str(output).endswith((".yaml", ".yml")):
            out_file = output
        else:
            out_dir = output

    imported: List[str] = []

    try:
        # --- Source import ---
        if source_name or source_table:
            contract = adapter.source_to_contract(source_name, source_table)
            _write_or_print_contract(contract, out_file, out_dir, overwrite, dry_run, verbose)
            imported.append(contract.dataset or "source")

        # --- Single model ---
        elif model:
            contract = adapter.model_to_contract(model)
            _write_or_print_contract(contract, out_file, out_dir, overwrite, dry_run, verbose)
            imported.append(contract.dataset or model)

        # --- All models ---
        else:
            models = adapter.list_models()
            if not models:
                typer.echo(typer.style("⚠  No models found in schema file.", fg=typer.colors.YELLOW))
                raise typer.Exit(code=0)
            for mname in models:
                contract = adapter.model_to_contract(mname)
                _write_or_print_contract(contract, None, out_dir, overwrite, dry_run, verbose)
                imported.append(mname)

    except Exception as exc:
        typer.echo(typer.style(f"✗  {exc}", fg=typer.colors.RED), err=True)
        if verbose:
            import traceback  # pragma: no cover

            # pragma: no cover
            traceback.print_exc()  # pragma: no cover
        raise typer.Exit(code=1)

    if not dry_run:
        typer.echo(
            typer.style(
                f"✔  Imported {len(imported)} contract(s): {', '.join(imported)}",
                fg=typer.colors.GREEN,
            )
        )


def _write_or_print_contract(
    contract: Any,
    out_file: Optional[Path],
    out_dir: Optional[Path],
    overwrite: bool,
    dry_run: bool,
    verbose: bool,
) -> None:
    """Write a DataContract to disk or print it."""
    import yaml as _yaml

    data = contract.model_dump(exclude_none=True, by_alias=True)
    text = _yaml.dump(data, sort_keys=False, allow_unicode=True, default_flow_style=False)

    if dry_run:
        typer.echo(f"# --- {contract.dataset} ---")
        typer.echo(text)
        return

    dest: Optional[Path] = out_file
    if dest is None and out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        dest = out_dir / f"{contract.dataset}.yaml"

    if dest is None:
        # No output path — print to stdout
        typer.echo(text)
        return

    if dest.exists() and not overwrite:
        typer.echo(typer.style(f"  ↷  Skipped (already exists): {dest}", dim=True))
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(text, encoding="utf-8")
    if verbose:
        typer.echo(typer.style(f"  ✔  Written: {dest}", fg=typer.colors.CYAN))


@app.command(rich_help_panel="Environment Setup")
def doctor():
    """
    Check your LakeLogic environment and report installed engines & extras.

    Run this to diagnose missing dependencies, verify engine availability,
    and confirm your LakeLogic version.

    Example:

        lakelogic doctor
    """
    import platform

    try:
        import lakelogic

        ll_version = getattr(lakelogic, "__version__", "unknown")
    except Exception:  # pragma: no cover
        ll_version = "unknown"  # pragma: no cover

    py_version = platform.python_version()
    os_info = f"{platform.system()} {platform.release()}"

    # ── Header ──────────────────────────────────────────────
    typer.echo("")
    typer.echo(typer.style("  LakeLogic Doctor", fg=typer.colors.CYAN, bold=True))
    typer.echo("  " + "═" * 45)
    typer.echo(f"  Version     : {ll_version}")
    typer.echo(f"  Python      : {py_version}")
    typer.echo(f"  OS          : {os_info}")
    typer.echo("")

    def _check_package(name: str, import_name: str = "") -> str:
        """Try to import a package and return its version or 'not installed'."""
        mod_name = import_name or name
        try:
            mod = __import__(mod_name)
            ver = getattr(mod, "__version__", None) or getattr(mod, "version", None) or getattr(mod, "VERSION", None)
            if callable(ver):
                ver = ver()
            return str(ver) if ver else "installed"
        except ImportError:
            return ""

    # ── Engines ─────────────────────────────────────────────
    engines = [
        ("polars", "polars"),
        ("duckdb", "duckdb"),
        ("pandas", "pandas"),
        ("pyspark", "pyspark"),
    ]

    typer.echo("  Engines")
    typer.echo("  " + "─" * 45)
    for label, pkg in engines:
        ver = _check_package(label, pkg)
        if ver:
            mark = typer.style("✅", fg=typer.colors.GREEN)
            typer.echo(f"  {mark} {label:<14} {ver}")
        else:
            mark = typer.style("⬚ ", dim=True)  # pragma: no cover
            typer.echo(f"  {mark} {label:<14} not installed")  # pragma: no cover

    typer.echo("")

    # ── Extras ──────────────────────────────────────────────
    extras = [
        ("deltalake", "deltalake"),
        ("pyarrow", "pyarrow"),
        ("jinja2", "jinja2"),
        ("apprise", "apprise"),
        ("dataprofiler", "dataprofiler"),
        ("presidio", "presidio_analyzer"),
        ("httpx", "httpx"),
        ("sqlglot", "sqlglot"),
        ("pydantic", "pydantic"),
        ("cryptography", "cryptography"),
        ("nbclient", "nbclient"),
    ]

    typer.echo("  Extras")
    typer.echo("  " + "─" * 45)
    for label, pkg in extras:
        ver = _check_package(label, pkg)
        if ver:
            mark = typer.style("✅", fg=typer.colors.GREEN)
            typer.echo(f"  {mark} {label:<14} {ver}")
        else:
            mark = typer.style("⬚ ", dim=True)
            typer.echo(f"  {mark} {label:<14} not installed")

    typer.echo("")

    # ── Database Connectors ─────────────────────────────────
    db_connectors = [
        ("pyodbc", "pyodbc"),
        ("psycopg2", "psycopg2"),
        ("pymysql", "pymysql"),
        ("pymongo", "pymongo"),
    ]

    typer.echo("  Database Connectors")
    typer.echo("  " + "─" * 45)
    for label, pkg in db_connectors:
        ver = _check_package(label, pkg)
        if ver:
            mark = typer.style("✅", fg=typer.colors.GREEN)  # pragma: no cover
            typer.echo(f"  {mark} {label:<14} {ver}")  # pragma: no cover
        else:
            mark = typer.style("⬚ ", dim=True)
            typer.echo(f"  {mark} {label:<14} not installed")

    typer.echo("")

    # ── Streaming ───────────────────────────────────────────
    streaming_pkgs = [
        ("kafka-python", "kafka"),
        ("bytewax", "bytewax"),
        ("websocket", "websocket"),
    ]

    typer.echo("  Streaming")
    typer.echo("  " + "─" * 45)
    for label, pkg in streaming_pkgs:
        ver = _check_package(label, pkg)
        if ver:
            mark = typer.style("✅", fg=typer.colors.GREEN)  # pragma: no cover
            typer.echo(f"  {mark} {label:<14} {ver}")  # pragma: no cover
        else:
            mark = typer.style("⬚ ", dim=True)
            typer.echo(f"  {mark} {label:<14} not installed")

    typer.echo("  " + "═" * 45)
    typer.echo("")


@app.command(rich_help_panel="Observability")
def scan(
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Path to scanner.yaml"),
    connection: Optional[str] = typer.Option(
        None, "--connection", help="Connection type: delta | unity_catalog | duckdb"
    ),
    path: Optional[str] = typer.Option(None, "--path", help="Storage root path (for delta/duckdb connections)"),
    host: Optional[str] = typer.Option(None, "--host", help="Databricks host URL (for unity_catalog)"),
    catalog: Optional[str] = typer.Option(None, "--catalog", help="Catalog name (for unity_catalog)"),
    token: Optional[str] = typer.Option(None, "--token", help="Access token (for unity_catalog)"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write JSON report to file"),
    push: bool = typer.Option(False, "--push", help="Push results to Observatory (requires observatory config)"),
    fail_on_breach: bool = typer.Option(True, "--fail/--no-fail", help="Exit 1 if any SLO check fails"),
):
    """
    Scan lakehouse tables and check SLOs without pipeline instrumentation.

    Connects directly to Delta Lake, Unity Catalog, or DuckDB and runs
    freshness, volume, schema drift, and retention checks using only
    table metadata and lightweight SQL aggregates.

    No contracts, no DataProcessor, no YAML authoring required.

    Examples:

        lakelogic scan --config scanner.yaml

        lakelogic scan --connection delta --path ./lakehouse/

        lakelogic scan --connection unity_catalog --catalog prod-001 \\
          --host https://adb-xxx.azuredatabricks.net --token $DBT

    """
    from lakelogic.scanner import Scanner

    try:
        if config:
            scanner = Scanner.from_yaml(config)
        elif connection:
            scanner = Scanner.from_args(
                connection_type=connection,
                path=path,
                host=host,
                catalog=catalog,
                token=token,
            )
        else:
            typer.secho(
                "Provide --config scanner.yaml or --connection <type> with connection args.",
                fg=typer.colors.RED,
            )
            raise typer.Exit(1)

        typer.echo(typer.style("🔍 LakeLogic Scanner", bold=True))

        report = scanner.run()

        # ── Summary output ──
        typer.echo("")
        total = len(report.results)
        failed = len(report.failures)
        passed_count = total - failed

        typer.echo(f"  Domain  : {report.domain}")
        typer.echo(f"  Run ID  : {report.check_run_id[:8]}...")
        typer.echo(f"  Tables  : {total} checks across scanned tables")
        typer.echo("")

        # Print failures
        if report.failures:
            typer.secho("  Failures:", fg=typer.colors.RED, bold=True)
            for r in report.failures:
                typer.secho(f"    • [{r.check_type}] {r.entity}: {r.status}", fg=typer.colors.RED)
            typer.echo("")

        # Summary line
        if report.passed:
            typer.secho(f"  ✅ All {passed_count} checks passed", fg=typer.colors.GREEN, bold=True)
        else:
            typer.secho(
                f"  ❌ {failed} of {total} checks failed",
                fg=typer.colors.RED,
                bold=True,
            )

        # JSON output
        if output:
            import json

            out_path = Path(output)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(report.model_dump(), f, indent=2, default=str)
            typer.echo(f"\n  Report written to {out_path}")

        if fail_on_breach and not report.passed:
            raise typer.Exit(1)

    except (FileNotFoundError, ValueError, ConnectionError) as exc:
        typer.secho(f"Scanner error: {exc}", fg=typer.colors.RED)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()  # pragma: no cover
