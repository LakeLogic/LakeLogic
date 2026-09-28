"""One place that names the tables and files LakeLogic creates for its own use.

The rule
--------
A table or file whose contents are 100% created by LakeLogic core — run log,
SLO checks, system logs, pipeline runs, erasure and retention evidence — is named
``_lakelogic_<kind>`` (for example ``_lakelogic_run_log``). The prefix groups
LakeLogic's tables together, sorts them away from user tables, and makes it
obvious which objects the framework owns.

Quarantine is deliberately NOT covered: it holds the client's own rejected rows,
not LakeLogic's bookkeeping, so it keeps its original names (``_quarantine``,
``quarantine_rows``, ``quarantine.duckdb``, ``{quarantine_root}.{domain}_{table}``).

Where a leading underscore is unsafe, the same name is used without it:
``lakelogic_<kind>``. That applies to:

* **Path-based storage** — a table written to a directory or URI (Delta/Parquet
  paths, ``.../lakelogic_logs/...``). Spark and Hadoop treat path segments
  starting with ``_`` (or ``.``) as hidden and skip them when listing, so an
  ``_``-prefixed directory can silently vanish from reads.
* **Backends in** :data:`UNDERSCORE_UNSAFE_BACKENDS` — see the comment there.

Local files LakeLogic creates (DuckDB/SQLite databases) use
:func:`metadata_file_name`: ``lakelogic_<kind>.<ext>``.

A USER-CONFIGURED name is always used verbatim. These helpers only supply the
default when nothing is configured; they never rewrite a name someone chose.

Existing estates
----------------
Names used before this standard are listed in :data:`LEGACY_NAMES`. Where the
legacy table/file already exists, writers keep using it (see
:func:`resolve_existing`) so history is not split; only new estates get the new
name. Readers that cannot check existence fall back through the legacy names.

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
)

# Backends where an ``_``-prefixed table name is not safe.
# Fabric: lakehouse tables are folders under ``Tables/``, and Spark/Hadoop treat
# ``_``-prefixed paths as hidden. Unverified on Fabric — conservative until tested.
UNDERSCORE_UNSAFE_BACKENDS = frozenset({"fabric"})

PREFIX = "lakelogic_"

# Every name used for each kind before this standard (tables, fallback tables
# inside local DB files, path segments, and file names).
LEGACY_NAMES: Dict[str, Tuple[str, ...]] = {
    "run_log": ("_run_logs", "run_logs", "lakelogic_run_logs.duckdb", "lakelogic_run_logs.sqlite"),
    "slo_checks": ("_slo_checks", "slo_checks"),
    "logs": ("_logs",),
    "pipeline_runs": ("pipeline_runs",),
    "erasure_evidence": (),
    "retention_evidence": (),
}


def _check_kind(kind: str) -> None:
    if kind not in METADATA_KINDS:
        raise ValueError(f"Unknown LakeLogic metadata kind {kind!r}; expected one of {METADATA_KINDS}")


def metadata_table_name(kind: str, *, path_based: bool = False, backend: Optional[str] = None) -> str:
    """Default name for a LakeLogic-owned table of ``kind``."""
    _check_kind(kind)
    if path_based or (backend or "").lower() in UNDERSCORE_UNSAFE_BACKENDS:
        return f"{PREFIX}{kind}"
    return f"_{PREFIX}{kind}"


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
