"""What happens to landing files once a Bronze load has finished with them.

One definition, imported by both callers (``PipelineRunner`` and the standalone
``DataProcessor.run_source``), so a contract cleans up the same way however it is run.

Layout. ``archive_path`` is a ROOT, not a folder. The contract's TABLE name (what the end user
sees: ``info.table_name``) and the outcome are added beneath it, so ONE system-level root serves
every table::

    <archive_path>/<table>/success/<path relative to the landing folder>
    <archive_path>/<table>/error/<path relative to the landing folder>

Settings come from ``source.post_ingestion`` (the contract) over
``server.bronze.post_ingestion`` (the system default), field by field, so a contract that
only sets ``on_error`` still inherits the system's ``action`` and ``archive_path``.

Only the files THIS load read are touched (``DataProcessor._source_files``), never the whole
landing folder: a file that arrived while the load was running is left for the next one.

An error is a file that could not be READ or PARSED. Rows that fail quality rules are not
file errors; they go to quarantine and the file is cleaned up as a success.
"""

from __future__ import annotations

import json
import os
import posixpath
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from loguru import logger

SUCCESS = "success"
ERROR = "error"

_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_GLOB = re.compile(r"[*?\[]")


@dataclass(frozen=True)
class LandingPolicy:
    action: str = "retain"  # delete | archive | retain
    on_error: str = "retain"  # retain | move
    archive_root: Optional[str] = None
    cleanup_is_blocking: bool = False
    #: ``retry_orphaned_files``: remember files a cleanup left behind, retry them first next run and
    #: never read them again. Off: they stay in landing and a later run may read them again.
    retry_orphans: bool = True


def _as_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    dump = getattr(value, "model_dump", None)
    return dump(exclude_none=True) if callable(dump) else {}


def resolve_policy(
    source: Any = None,
    server: Any = None,
    storage_archive_path: Optional[str] = None,
) -> LandingPolicy:
    """The contract's ``post_ingestion`` laid over the system's, field by field."""
    merged: Dict[str, Any] = {}
    merged.update(_as_dict(_as_dict(server).get("post_ingestion")))
    merged.update(_as_dict(_as_dict(source).get("post_ingestion")))
    return LandingPolicy(
        action=str(merged.get("action") or "retain"),
        on_error=str(merged.get("on_error") or "retain"),
        archive_root=merged.get("archive_path") or storage_archive_path or None,
        cleanup_is_blocking=bool(merged.get("cleanup_is_blocking", False)),
        retry_orphans=merged.get("retry_orphaned_files") is not False,
    )


def entity_folder(entity: str) -> str:
    """The entity as one safe folder name."""
    name = re.sub(r"[^\w.\-]+", "_", str(entity or "").strip()).strip("._")
    return name or "unnamed"


def is_uri(path: str) -> bool:
    return bool(_URI.match(str(path)))


def _join(root: str, *parts: str) -> str:
    return posixpath.join(root.rstrip("/"), *parts) if is_uri(root) else os.path.join(root, *parts)


def destination_dir(archive_root: str, entity: str, outcome: str) -> str:
    """``<root>/<table>/<outcome>`` - the one place the layout is spelled out."""
    return _join(archive_root, entity_folder(entity), outcome)


def _landing_base(landing_path: Optional[str]) -> Optional[str]:
    """The folder a (possibly glob) landing path starts in."""
    if not landing_path:
        return None
    text = str(landing_path)
    m = _GLOB.search(text)
    if m:
        text = text[: m.start()]
        return text.rstrip("/\\") or None
    # A bare file path: its folder. A bare folder: itself.
    return (
        text if not os.path.splitext(text)[1] else (posixpath.dirname(text) if is_uri(text) else os.path.dirname(text))
    )


def relative_paths(files: List[str], landing_path: Optional[str]) -> Dict[str, str]:
    """``{file: path relative to the landing folder}``, so date folders survive the move.

    Falls back to the files' common folder when the landing path does not contain a file
    (a storage-relative path the processor resolved), and to the bare name for a lone file.
    """
    base = _landing_base(landing_path)
    uri = any(is_uri(f) for f in files)

    def norm(p: str) -> str:
        return p.replace("\\", "/").rstrip("/") if not uri else p.rstrip("/")

    out: Dict[str, str] = {}
    common = None
    if files:
        parents = [norm(os.path.dirname(f) if not uri else posixpath.dirname(f)) for f in files]
        try:
            common = os.path.commonpath(parents) if not uri else posixpath.commonpath(parents)
        except ValueError:
            common = None
    for f in files:
        nf = norm(f)
        rel = None
        for root in (base, common):
            if not root:
                continue
            nr = norm(root)
            if nf.startswith(nr + "/"):
                rel = nf[len(nr) + 1 :]
                break
        out[f] = rel or posixpath.basename(nf)
    return out


def _get_dbutils(spark: Any) -> Any:
    if spark is None:
        return None
    try:
        return spark._jvm.com.databricks.service.DBUtils(spark._jsc.sc())
    except Exception:
        try:
            import IPython

            return IPython.get_ipython().user_ns.get("dbutils")
        except Exception:
            return None


class _Backend:
    """Move / delete one file on one storage kind."""

    def __init__(self, move: Callable[[str, str], None], delete: Callable[[str], None], exists: Callable[[str], bool]):
        self.move, self.delete, self.exists = move, delete, exists


def _local_backend() -> _Backend:
    def move(src: str, dst: str) -> None:
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
        shutil.move(src, dst)

    return _Backend(move, os.unlink, os.path.exists)


def _cloud_backend(sample_path: str, spark: Any) -> _Backend:
    dbu = _get_dbutils(spark)
    if dbu is not None:
        return _Backend(
            lambda s, d: dbu.fs.mv(s, d),
            lambda p: dbu.fs.rm(p, False),
            lambda p: _dbutils_exists(dbu, p),
        )
    try:
        import fsspec
    except ImportError as exc:
        raise RuntimeError(
            "fsspec is required for cloud post-ingestion cleanup but is not installed. "
            "Install it with: pip install fsspec adlfs  (or s3fs / gcsfs)"
        ) from exc
    opts: Dict[str, str] = {}
    if sample_path.startswith(("abfss://", "abfs://")):
        for env_key, opt_key in (
            ("AZURE_STORAGE_ACCOUNT_NAME", "account_name"),
            ("AZURE_STORAGE_ACCOUNT", "account_name"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "account_key"),
            ("AZURE_STORAGE_SAS_TOKEN", "sas_token"),
            ("AZURE_CLIENT_ID", "client_id"),
            ("AZURE_CLIENT_SECRET", "client_secret"),
            ("AZURE_TENANT_ID", "tenant_id"),
        ):
            val = os.environ.get(env_key)
            if val:
                opts[opt_key] = val
    fs, _ = fsspec.core.url_to_fs(sample_path, **opts)

    def move(src: str, dst: str) -> None:
        # copy + delete: a move across containers is not atomic on object stores.
        fs.copy(src, dst)
        fs.rm(src)

    return _Backend(move, fs.rm, fs.exists)


def _dbutils_exists(dbu: Any, path: str) -> bool:
    try:
        dbu.fs.ls(path)
        return True
    except Exception:
        return False


def _unused_name(dst: str, exists: Callable[[str], bool], stamp: str) -> str:
    """``dst`` unless a file is already there, else ``name.<stamp>.ext``: never overwrite."""
    if not exists(dst):
        return dst
    stem, ext = os.path.splitext(dst)
    n = 0
    while True:
        cand = f"{stem}.{stamp}{'' if n == 0 else '-' + str(n)}{ext}"
        if not exists(cand):
            return cand
        n += 1


def _backend_for(files: List[str], spark: Any) -> _Backend:
    return _cloud_backend(files[0], spark) if is_uri(files[0]) else _local_backend()


def apply(
    files: Iterable[str],
    *,
    action: str,
    landing_path: Optional[str],
    entity: str,
    outcome: str,
    archive_root: Optional[str] = None,
    spark: Any = None,
    failed_paths: Optional[List[str]] = None,
) -> Tuple[int, List[str]]:
    """Delete or move ``files``; returns ``(files handled, failures)``.

    ``failed_paths``, when given, collects the paths that could not be handled, so a caller can
    remember them (``record_orphans``).

    ``action`` is ``delete`` or ``move`` (the caller maps ``archive`` and ``on_error: move``
    onto it). A move needs ``archive_root``. One file failing does not stop the rest.
    """
    paths = [str(f) for f in files]
    if not paths:
        return 0, []
    if action == "move" and not archive_root:
        raise ValueError("a move needs an archive_path (or the system's storage.archive_path)")
    backend = _backend_for(paths, spark)
    rels = relative_paths(paths, landing_path)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    done, failed = 0, []
    for src in paths:
        try:
            if not backend.exists(src):
                continue
            if action == "delete":
                backend.delete(src)
            else:
                dest = _join(destination_dir(archive_root or "", entity, outcome), rels[src])
                backend.move(src, _unused_name(dest, backend.exists, stamp))
            done += 1
        except Exception as exc:  # one bad file must not strand the others
            failed.append(f"{src}: {exc}")
            if failed_paths is not None:
                failed_paths.append(src)
    return done, failed


class CleanupFailed(RuntimeError):
    """A cleanup that left files behind; ``failed_paths`` are the ones to remember (``record_orphans``)."""

    def __init__(self, message: str, failed_paths: Iterable[str] = ()):
        super().__init__(message)
        self.failed_paths = list(failed_paths)


#: Files a Bronze load READ but could not clean up: remembered so the next run retries the cleanup
#: and never reads them again (a full load would otherwise duplicate their rows).
ORPHANS_FILE = "_orphaned_files.json"


def manifest_path(policy: "LandingPolicy", entity: str, fallback_root: Optional[str] = None) -> Optional[str]:
    """``<archive root>/<table>/_orphaned_files.json``, beside the files it describes; with no archive
    root (``action: delete``) the run-log folder holds it. ``None`` when neither exists."""
    root = policy.archive_root or fallback_root
    return _join(str(root), entity_folder(entity), ORPHANS_FILE) if root else None


def _read_text(path: str, spark: Any) -> Optional[str]:
    if not is_uri(path):
        p = Path(path)
        return p.read_text(encoding="utf-8") if p.is_file() else None
    dbu = _get_dbutils(spark)
    if dbu is not None:
        try:
            return str(dbu.fs.head(path, 10_000_000))
        except Exception as exc:
            # Missing is "none remembered"; any other failure must not be read as an empty list.
            if "FileNotFound" in str(exc) or "does not exist" in str(exc) or "NOT_FOUND" in str(exc):
                return None
            raise
    import fsspec

    fs, _ = fsspec.core.url_to_fs(path)
    if not fs.exists(path):
        return None
    with fs.open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def _write_text(path: str, text: str, spark: Any) -> None:
    if not is_uri(path):
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
        return
    dbu = _get_dbutils(spark)
    if dbu is not None:
        dbu.fs.put(path, text, True)
        return
    import fsspec

    fs, _ = fsspec.core.url_to_fs(path)
    with fs.open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def read_orphans(manifest: str, spark: Any = None) -> List[str]:
    """The paths a manifest remembers; ``[]`` for none. An unreadable manifest raises: guessing
    ``[]`` would let an orphan be read twice."""
    text = _read_text(manifest, spark)
    if not text or not text.strip():
        return []
    return [str(f) for f in (json.loads(text).get("files") or [])]


def write_orphans(manifest: str, files: Iterable[str], spark: Any = None) -> None:
    _write_text(manifest, json.dumps({"files": sorted(set(str(f) for f in files))}, indent=2), spark)


def record_orphans(manifest: str, paths: Iterable[str], spark: Any = None) -> None:
    """Add ``paths`` to the manifest (kept as a union, so a stuck file stays remembered)."""
    new = [str(p) for p in paths]
    if new:
        write_orphans(manifest, list(read_orphans(manifest, spark)) + new, spark)


def retry_orphans(
    manifest: str,
    policy: "LandingPolicy",
    *,
    entity: str,
    landing_path: Optional[str],
    spark: Any = None,
) -> List[str]:
    """Retry the cleanup of every file the manifest remembers; returns the ones STILL stuck.

    A file that is gone counts as done. The manifest is rewritten with what is left. With
    ``action: retain`` there is nothing to retry, and nothing is held back.
    """
    if policy.action == "retain":
        return []
    remembered = read_orphans(manifest, spark)
    if not remembered:
        return []
    if policy.action == "archive" and not policy.archive_root:
        return remembered  # cannot move them yet; keep them out of the read
    stuck: List[str] = []
    apply(
        remembered,
        action="delete" if policy.action == "delete" else "move",
        landing_path=landing_path,
        entity=entity,
        outcome=SUCCESS,
        archive_root=policy.archive_root,
        spark=spark,
        failed_paths=stuck,
    )
    write_orphans(manifest, stuck, spark)
    return stuck


def clean_after_success(
    files: Iterable[str],
    policy: LandingPolicy,
    *,
    entity: str,
    landing_path: Optional[str],
    spark: Any = None,
) -> int:
    """The policy's ``action`` for the files a committed load read. Raises when it must."""
    if policy.action == "retain":
        return 0
    if policy.action == "delete":
        mode = "delete"
    elif policy.action == "archive":
        if not policy.archive_root:
            logger.warning(
                f"  post_ingestion.action=archive for {entity} but no archive_path "
                f"is set (contract, system or storage.archive_path) - leaving the files in place"
            )
            return 0
        mode = "move"
    else:
        logger.warning(f"  Unknown post_ingestion action '{policy.action}' for {entity} - skipping")
        return 0
    failed_paths: List[str] = []
    done, failed = apply(
        files,
        action=mode,
        landing_path=landing_path,
        entity=entity,
        outcome=SUCCESS,
        archive_root=policy.archive_root,
        spark=spark,
        failed_paths=failed_paths,
    )
    if failed:
        raise CleanupFailed(
            f"{len(failed)} of {done + len(failed)} landing file(s) could not be cleaned up: {failed[:3]}",
            failed_paths,
        )
    return done


def list_folder(folder: str) -> List[str]:
    """Every file under a landing folder that Auto Loader would read: all but hidden ones.

    Auto Loader tracks its files in the checkpoint and does not say which a run read, so the
    pipeline lists the folder BEFORE the run: a file that arrives during it is not in the list
    and stays for the next one. Hidden files (``_x``, ``.x``) are skipped, as Auto Loader skips them.
    Returns ``[]`` for a folder it cannot list (a missing path, or a cloud URI without fsspec).
    """
    folder = str(folder or "").strip()
    if not folder:
        return []

    def visible(name: str) -> bool:
        return not name.startswith(("_", "."))

    if not is_uri(folder):
        root = Path(folder)
        if not root.is_dir():
            return []
        return sorted(
            str(p)
            for p in root.rglob("*")
            if p.is_file() and visible(p.name) and not any(not visible(part) for part in p.relative_to(root).parts)
        )
    try:
        import fsspec

        fs, _ = fsspec.core.url_to_fs(folder)
        scheme = folder.split("://", 1)[0]
        found = [str(f) for f in fs.find(folder)]
    except Exception as exc:
        logger.debug(f"  cannot list {folder} for post-ingestion cleanup: {exc}")
        return []
    return sorted((f if "://" in f else f"{scheme}://{f.lstrip('/')}") for f in found if visible(f.rsplit("/", 1)[-1]))


def named_in_error(files: Iterable[str], message: str) -> List[str]:
    """The files whose path the error message names.

    Spark reads lazily, so a corrupt file fails after the read stage began, and the error says
    which file: ``Error while reading file dbfs:/Volumes/.../bad.parquet``. Matching on the full
    path (``dbfs:`` / ``file:`` prefixes are a substring of it) attributes the failure to the
    file, which a batch read cannot do. A message that names no tracked file names none.
    """
    text = str(message or "").replace("\\", "/")
    return [str(f) for f in files if str(f).replace("\\", "/") in text]


def move_failed(
    files: Iterable[str],
    policy: LandingPolicy,
    *,
    entity: str,
    landing_path: Optional[str],
    spark: Any = None,
) -> int:
    """``on_error: move``: send the files of a load that failed to read to ``.../error/``.

    Never raises: the load already failed and that failure is what the caller re-raises.
    Moves the files of the whole load, because the read is one batch and does not say which
    file broke it; the caller logs that.
    """
    if policy.on_error != "move":
        return 0
    if not policy.archive_root:
        logger.warning(
            f"  post_ingestion.on_error=move for {entity} but no archive_path is set - "
            f"the failed files stay in the landing folder"
        )
        return 0
    try:
        done, failed = apply(
            files,
            action="move",
            landing_path=landing_path,
            entity=entity,
            outcome=ERROR,
            archive_root=policy.archive_root,
            spark=spark,
        )
    except Exception as exc:
        logger.warning(f"  Could not move the failed files of {entity} to the error folder: {exc}")
        return 0
    if failed:
        logger.warning(f"  {len(failed)} failed file(s) of {entity} could not be moved: {failed[:3]}")
    return done
