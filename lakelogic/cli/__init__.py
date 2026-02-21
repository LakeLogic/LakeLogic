"""
LakeLogic CLI — pipeline driver and supporting modules.

Public API:
    PipelineDriver  — main orchestrator for registry-driven pipeline runs
    Window          — time boundaries for incremental processing
    ContractLoader  — YAML contract loader
    RunLogReader    — run-log timestamp reader (Spark / DuckDB / SQLite)
    main            — CLI entrypoint

Parser helpers (re-exported for convenience):
    parse_layers, parse_entities, parse_contracts,
    parse_metrics_tags, parse_overrides, parse_window,
    build_backfill_windows

Observability helpers (used internally; importable for custom drivers):
    flatten_summary, finalize_summary, write_summary_table,
    emit_metrics, format_prometheus,
    start_prometheus_server, stop_prometheus_server
"""

from lakelogic.cli.driver import (  # noqa: F401
    ContractLoader,
    PipelineDriver,
    Window,
    main,
)

from lakelogic.cli.run_log_reader import RunLogReader  # noqa: F401

from lakelogic.cli.cli_parsers import (  # noqa: F401
    build_backfill_windows,
    parse_contracts,
    parse_entities,
    parse_layers,
    parse_metrics_tags,
    parse_overrides,
    parse_window,
)

from lakelogic.cli.observability import (  # noqa: F401
    emit_metrics,
    finalize_summary,
    flatten_summary,
    format_prometheus,
    start_prometheus_server,
    stop_prometheus_server,
    write_summary_table,
)

__all__ = [
    # Core
    "PipelineDriver",
    "Window",
    "ContractLoader",
    "RunLogReader",
    "main",
    # Parsers
    "parse_layers",
    "parse_entities",
    "parse_contracts",
    "parse_metrics_tags",
    "parse_overrides",
    "parse_window",
    "build_backfill_windows",
    # Observability
    "flatten_summary",
    "finalize_summary",
    "write_summary_table",
    "emit_metrics",
    "format_prometheus",
    "start_prometheus_server",
    "stop_prometheus_server",
]
