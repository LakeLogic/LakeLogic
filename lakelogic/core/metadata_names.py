"""One place that names the tables and files LakeLogic creates for its own use.

The rule
--------
A table or file whose contents are 100% created by LakeLogic core — run log,
SLO checks, system logs, pipeline runs, erasure and retention evidence, the erasure
request queue (people insert its rows; LakeLogic owns its schema and status) — is named
``lakelogic_<kind>`` (for example ``lakelogic_run_log``) — on EVERY platform and storage
(2026-10-20, owner decision, replacing the 2026-09-26 ``_lakelogic_`` table prefix). These
tables sit in their own schema (``lakelogic``, or the domain's schema), so a leading
underscore added nothing, and it made Databricks (``_lakelogic_run_log``) and Fabric
(``lakelogic_run_log``) disagree. The ``lakelogic_`` prefix still groups LakeLogic's tables and
says the framework owns them; no leading underscore also keeps path segments visible (Spark
and Hadoop hide ``_``-prefixed directories).

Quarantine is deliberately NOT covered: it holds the client's own rejected rows,
not LakeLogic's bookkeeping, so it keeps its original names (``_quarantine``,
``quarantine_rows``, ``quarantine.duckdb``, ``{quarantine_root}.{domain}_{table}``).

Local files LakeLogic creates (DuckDB/SQLite databases) use
:func:`metadata_file_name`: ``lakelogic_<kind>.<ext>``.

A USER-CONFIGURED name is always used verbatim. These helpers only supply the
default when nothing is configured; they never rewrite a name someone chose.

Existing estates
----------------
Names used before this standard are listed in :data:`LEGACY_NAMES` — including the
``_lakelogic_<kind>`` form of the 2026-09-26 standard. Where a legacy table/file already
exists, writers keep using it (see :func:`resolve_existing`) so history is not split; only new
estates get the new name. Readers that cannot check existence fall back through the legacy
names. A table that sits BESIDE another (evidence beside the run log) follows that table's
style (:func:`sibling_table_name`), so an estate is never half old, half new.

Covered by the directory
------------------------
Local state files under ``.lakelogic/`` (``watermark_*.json``,
``dlt_pipelines/``, ``observatory_spool/``, ``scanner_baselines.json``,
``input_*.csv``) are already namespaced by the
``.lakelogic`` directory and keep their names.
"""

from __future__ import annotations

from typing import Callable, Dict, Iterable, Optional, Tuple

METADATA_KINDS: Tuple[str, ...] = (
    "run_log",
    "slo_checks",
    "logs",
    "pipeline_runs",
    "erasure_evidence",
    "retention_evidence",
    "erasure_requests",
    # Per-rule results of `lakelogic check` (#570): one row per rule per check.
    "quality_checks",
)

# Kept for callers that still pass ``backend=``: every backend now gets the same name.
UNDERSCORE_UNSAFE_BACKENDS = frozenset({"fabric"})

PREFIX = "lakelogic_"

# Every name used for each kind before this standard (tables, fallback tables
# inside local DB files, path segments, and file names).
LEGACY_NAMES: Dict[str, Tuple[str, ...]] = {
    # `_lakelogic_<kind>` first: the 2026-09-26 standard, the most recent legacy form.
    "run_log": ("_lakelogic_run_log", "_run_logs", "run_logs", "lakelogic_run_logs.duckdb", "lakelogic_run_logs.sqlite"),
    "slo_checks": ("_lakelogic_slo_checks", "_slo_checks", "slo_checks"),
    "logs": ("_lakelogic_logs", "_logs"),
    "pipeline_runs": ("_lakelogic_pipeline_runs", "pipeline_runs"),
    "erasure_evidence": ("_lakelogic_erasure_evidence",),
    "retention_evidence": ("_lakelogic_retention_evidence",),
    "erasure_requests": ("_lakelogic_erasure_requests",),
    "quality_checks": ("_lakelogic_quality_checks", "_quality_checks"),
}


def _check_kind(kind: str) -> None:
    if kind not in METADATA_KINDS:
        raise ValueError(f"Unknown LakeLogic metadata kind {kind!r}; expected one of {METADATA_KINDS}")


def metadata_table_name(kind: str, *, path_based: bool = False, backend: Optional[str] = None) -> str:
    """Default name for a LakeLogic-owned table of ``kind``: ``lakelogic_<kind>``, the same on
    every platform. ``path_based`` / ``backend`` are accepted for older callers and no longer
    change the name."""
    _check_kind(kind)
    return f"{PREFIX}{kind}"


def is_old_prefix_style(name: str) -> bool:
    """True for a ``_lakelogic_<kind>`` name (the 2026-09-26 standard)."""
    return str(name or "").rsplit(".", 1)[-1].rsplit("/", 1)[-1].startswith(f"_{PREFIX}")


def sibling_table_name(kind: str, beside: str) -> str:
    """The name for a ``kind`` table written BESIDE ``beside`` (e.g. evidence beside the run
    log): the old ``_lakelogic_`` style when ``beside`` uses it, so an existing estate keeps one
    convention and its history; otherwise the standard name."""
    _check_kind(kind)
    return f"_{PREFIX}{kind}" if is_old_prefix_style(beside) else f"{PREFIX}{kind}"


def metadata_file_name(kind: str, ext: str) -> str:
    """Default name for a local file LakeLogic creates: ``lakelogic_<kind>.<ext>``."""
    _check_kind(kind)
    return f"{PREFIX}{kind}.{ext.lstrip('.')}"


def legacy_table_names(kind: str) -> Tuple[str, ...]:
    """Legacy table-like names for ``kind`` (file names excluded)."""
    _check_kind(kind)
    return tuple(n for n in LEGACY_NAMES[kind] if "." not in n)


def resolve_existing(kind: str, candidates: Iterable[str], exists_fn: Callable[[str], bool]) -> str:
    """Pick the name to use: the first *legacy* candidate that already exists, else the new one.

    ``candidates`` lists the new name first, then legacy names. If a legacy
    object exists it is kept (history is not split); otherwise the first
    candidate (the new name) is returned. Errors from ``exists_fn`` count as
    "does not exist".
    """
    _check_kind(kind)
    cands = [c for c in candidates if c]
    if not cands:
        raise ValueError("resolve_existing needs at least one candidate")
    new_name = cands[0]
    for cand in cands[1:]:
        try:
            if exists_fn(cand):
                return cand
        except Exception:
            continue
    return new_name


def resolve_local_file(kind: str, directory, ext: str):
    """Default local DB file for ``kind`` in ``directory``, preferring an existing legacy file.

    Returns a :class:`pathlib.Path`. If a legacy file (e.g.
    ``lakelogic_run_logs.duckdb``) exists and the new one does not, the legacy
    file is kept so history is not split.
    """
    from pathlib import Path

    _check_kind(kind)
    directory = Path(directory)
    new_path = directory / metadata_file_name(kind, ext)
    ext = ext.lstrip(".")
    legacy = [n for n in LEGACY_NAMES[kind] if n.endswith(f".{ext}")]
    return Path(
        resolve_existing(kind, [str(new_path)] + [str(directory / n) for n in legacy], lambda p: Path(p).exists())
    )


def is_legacy_name(kind: str, name: str) -> bool:
    """True when ``name`` (a table name, path or file) is one of ``kind``'s legacy names."""
    from pathlib import PurePath

    _check_kind(kind)
    return PurePath(str(name)).name in LEGACY_NAMES[kind]


def default_local_db_path(kind: str, ext: str, base: str = "logs") -> str:
    """Default ``logs/lakelogic_<kind>.<ext>``, or the legacy file there when it already exists."""
    return str(resolve_local_file(kind, base, ext)).replace("\\", "/")


def resolve_path_dir(kind: str, root: str) -> str:
    """``{root}/lakelogic_<kind>``, or the legacy ``{root}/_<kind>`` directory when it already exists.

    Existence is only checked for local paths; for cloud URIs (``abfss://``,
    ``s3://`` ...) the check is not cheap, so the new name is used.
    """
    import os

    _check_kind(kind)
    root = str(root).rstrip("/\\")
    new_dir = f"{root}/{metadata_table_name(kind, path_based=True)}"
    legacy = [f"{root}/{n}" for n in legacy_table_names(kind) if n.startswith("_")]

    def _local_exists(p: str) -> bool:
        return "://" not in p and os.path.isdir(p)

    return resolve_existing(kind, [new_dir] + legacy, _local_exists)


#: The dlt dataset the run log used before this standard. A dlt DATASET is a schema (a
#: BigQuery dataset, a Snowflake/Postgres schema), and a BigQuery dataset whose name starts
#: with ``_`` is hidden — so the new name drops the underscore, like a path does.
DLT_LEGACY_RUN_LOG_DATASET = "run_logs"


def resolve_dlt_run_log_dataset(has_dataset: Callable[[str], bool]) -> str:
    """The dlt dataset to write the run log to.

    Keeps the legacy ``run_logs`` dataset when it already exists: switching an existing estate
    to a new dataset would split its history and leave the watermark reader looking at an
    empty log — a full reload. ``has_dataset`` asks the destination; if it cannot answer
    (unreachable, or a destination with no SQL client), the legacy name is kept rather than
    risk that reload. Only a destination that says "no such dataset" gets the new name.
    """
    try:
        if has_dataset(DLT_LEGACY_RUN_LOG_DATASET):
            return DLT_LEGACY_RUN_LOG_DATASET
    except Exception:
        return DLT_LEGACY_RUN_LOG_DATASET
    return metadata_table_name("run_log", path_based=True)
