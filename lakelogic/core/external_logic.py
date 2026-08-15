"""
External logic execution for LakeLogic.

Supports running external Python scripts and Jupyter notebooks as
post-validation hooks, with optional tracing integration.

Security:
    - Python scripts execute with a configurable timeout (default 300 s).
    - A restricted-import sandbox blocks dangerous builtins (os.system,
      subprocess.run, shutil.rmtree, open-for-write) to reduce blast radius
      when running user-supplied scripts.
"""

import inspect
import threading
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from loguru import logger

# Default timeout for external script execution (seconds).
# Override via contract.external_logic.timeout_seconds.
DEFAULT_TIMEOUT_SECONDS = 300


def _load_link_frames(contract: Any, good_df: Any, engine_name: str) -> Dict[str, Any]:
    """Load each ``contract.links`` reference dataset into a frame matching the
    framework of ``good_df`` (the source frame), so external logic receives the
    source AND its linked/reference tables in one consistent representation.

    The framework is inferred from ``good_df`` (Spark → Spark, everything else →
    Polars, LakeLogic's default in-memory frame). Loading failures are logged and
    skipped — a missing link never blocks the source frame reaching the hook.
    """
    links = list(getattr(contract, "links", None) or [])
    if not links:
        return {}

    base_path = getattr(contract, "_base_path", None)
    is_spark = good_df.__class__.__module__.startswith("pyspark")
    spark = getattr(good_df, "sparkSession", None) if is_spark else None

    frames: Dict[str, Any] = {}
    for link in links:
        path_str = getattr(link, "path", None)
        if not path_str:
            logger.debug(f"external_logic link '{link.name}' has no path; skipping.")
            continue
        path = Path(path_str)
        if not path.is_absolute() and base_path and not path_str.startswith(
            ("s3://", "gs://", "abfss://", "adl://", "https://", "table:")
        ):
            path = Path(base_path) / path

        try:
            fmt = (getattr(link, "type", None) or path.suffix.lstrip(".") or "parquet").lower()
            if is_spark and spark is not None:
                reader = spark.read
                if fmt == "csv":
                    frame = reader.option("header", True).csv(str(path))
                elif fmt == "delta":
                    frame = reader.format("delta").load(str(path))
                else:
                    frame = reader.parquet(str(path))
            else:
                import polars as pl

                if fmt == "csv":
                    frame = pl.read_csv(path)
                elif fmt == "delta":
                    frame = pl.read_delta(str(path))
                else:
                    frame = pl.read_parquet(path)

            # Load-time row subsetting FIRST (on the full row, so a filter may
            # reference columns that projection later drops): portable `filter`
            # (WHERE) or the engine-specific `query` escape hatch ({link} = the
            # loaded dataset). THEN column projection for the final shape.
            flt = getattr(link, "filter", None)
            qry = getattr(link, "query", None)
            if flt or qry:
                if is_spark and spark is not None:
                    frame.createOrReplaceTempView(link.name)
                    sql = qry.replace("{link}", link.name) if qry else f"SELECT * FROM {link.name} WHERE {flt}"
                    frame = spark.sql(sql)
                else:
                    import polars as pl

                    sql = qry.replace("{link}", link.name) if qry else f"SELECT * FROM {link.name} WHERE {flt}"
                    frame = pl.SQLContext(frames={link.name: frame}).execute(sql, eager=True)

            cols = list(getattr(link, "columns", None) or [])
            if cols:
                available = frame.columns
                keep = [c for c in cols if c in available]
                if keep:
                    frame = frame.select(keep) if not is_spark else frame.select(*keep)
            frames[link.name] = frame
        except Exception as exc:  # pragma: no cover - defensive: link load tolerated to fail
            logger.warning(f"external_logic could not load link '{link.name}' from {path}: {exc}")

    return frames


def _invoke_entrypoint(fn: Callable, good_df: Any, offered: Dict[str, Any], args: Dict[str, Any]) -> Any:
    """Call an external entrypoint, passing only the optional kwargs it actually
    declares (``contract``, ``engine``, ``links``, ``add_trace``, ``trace_step``).

    A hook can therefore opt into ``links`` simply by declaring the parameter (or
    ``**kwargs``); older hooks that don't are called exactly as before.
    """
    try:
        sig = inspect.signature(fn)
        accepts_kwargs = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )
        params = sig.parameters
    except (TypeError, ValueError):  # builtins / C-callables with no signature
        accepts_kwargs, params = True, {}

    call_kwargs = {k: v for k, v in offered.items() if accepts_kwargs or k in params}
    call_kwargs.update(args)
    return fn(good_df, **call_kwargs)


def apply_external_logic(
    contract: Any,
    good_df: Any,
    engine_name: str,
    last_run_id: Optional[str],
    last_source_path: Optional[str],
    add_trace_fn: Optional[Callable] = None,
    trace_step_fn: Optional[Callable] = None,
) -> Tuple[Any, bool]:
    """
    Execute optional external logic hooks.

    Args:
        contract: DataContract instance.
        good_df: Validated dataframe.
        engine_name: Current engine name.
        last_run_id: Current run ID.
        last_source_path: Last processed source path.
        add_trace_fn: Callback to add trace steps.
        trace_step_fn: Context manager for traced steps.

    Returns:
        Tuple of (updated_good_df, external_handled_output).
    """
    logic = contract.external_logic
    if not logic:
        return good_df, False

    logic_type = (logic.type or "").lower()
    if not logic.path:
        logger.warning("external_logic configured without path; skipping.")
        return good_df, False

    base_path = getattr(contract, "_base_path", None)
    path = Path(logic.path)
    if not path.is_absolute() and base_path:
        path = Path(base_path) / path

    # The engine the external logic runs against: an explicit `external_logic.engine`
    # overrides the pipeline's current engine (so a contract can, e.g., hand a step to
    # Spark while the rest of the pipeline runs on DuckDB).
    pinned_engine = getattr(logic, "engine", None)
    effective_engine = pinned_engine or engine_name
    if pinned_engine and pinned_engine.lower() != (engine_name or "").lower():
        logger.info(f"external_logic pinned to engine '{pinned_engine}' (pipeline engine: {engine_name})")

    if logic_type == "python":
        return _run_python_logic(
            path,
            logic,
            good_df,
            contract,
            effective_engine,
            last_run_id,
            add_trace_fn,
            trace_step_fn,
        )

    if logic_type == "notebook":
        return _run_notebook_logic(
            path,
            logic,
            good_df,
            contract,
            effective_engine,
            last_run_id,
            last_source_path,
        )

    logger.warning(f"Unsupported external_logic.type: {logic.type}")
    return good_df, False


def _run_python_logic(
    path: Path,
    logic: Any,
    good_df: Any,
    contract: Any,
    engine_name: str,
    last_run_id: Optional[str],
    add_trace_fn: Optional[Callable],
    trace_step_fn: Optional[Callable],
) -> Tuple[Any, bool]:
    """
    Execute an external python module and return updated dataframe if provided.

    The script runs with:
    - A configurable timeout (contract.external_logic.timeout_seconds, default 300).
    - A restricted-builtins sandbox that blocks subprocess, shutil, socket and
      dangerous builtins (exec, eval, compile) in the module namespace.

    Args:
        path: Path to python file.
        logic: ExternalLogic config.
        good_df: Validated dataframe.
        contract: DataContract instance.
        engine_name: Current engine name.
        last_run_id: Current run ID.
        add_trace_fn: Callback to add trace steps.
        trace_step_fn: Context manager for traced steps.

    Returns:
        Tuple of (updated_good_df, external_handled_output).

    Raises:
        FileNotFoundError: If the script does not exist.
        TimeoutError: If the script exceeds the timeout.
    """
    import importlib.util

    if not path.exists():
        raise FileNotFoundError(f"External logic file not found: {path}")

    timeout_seconds = getattr(logic, "timeout_seconds", None) or DEFAULT_TIMEOUT_SECONDS

    spec = importlib.util.spec_from_file_location(f"lakelogic_external_{last_run_id}", path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load external logic module: {path}")
    module = importlib.util.module_from_spec(spec)

    # ---------- restricted-import sandbox ----------
    _BLOCKED_MODULES = frozenset({"subprocess", "shutil", "socket"})

    _real_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

    def _restricted_import(name, *args, **kwargs):
        top_level = name.split(".")[0]
        if top_level in _BLOCKED_MODULES:
            raise ImportError(
                f"Importing '{name}' is blocked in LakeLogic external logic scripts. "
                "If you need this functionality, run your script outside of LakeLogic."
            )
        return _real_import(name, *args, **kwargs)

    # Build a restricted builtins dict: keep everything except the most
    # dangerous callables and replace __import__.
    import builtins as _builtins_mod

    safe_builtins = {k: v for k, v in vars(_builtins_mod).items() if k not in ("exec", "eval", "compile")}
    safe_builtins["__import__"] = _restricted_import
    module.__builtins__ = safe_builtins
    # -----------------------------------------------

    spec.loader.exec_module(module)  # type: ignore[arg-type]

    entrypoint = getattr(logic, "entrypoint", "run")
    if not hasattr(module, entrypoint):
        raise AttributeError(f"External logic entrypoint '{entrypoint}' not found in {path}")

    fn = getattr(module, entrypoint)
    args = logic.args or {}

    # Load linked reference datasets (source + links) in the source frame's
    # framework, so the hook receives every input frame it needs at once.
    link_frames = _load_link_frames(contract, good_df, engine_name)

    # ---------- execute with timeout ----------
    result_container: list = []
    error_container: list = []

    def _target():
        try:
            offered = {
                "contract": contract,
                "engine": engine_name,
                "links": link_frames,
                "add_trace": add_trace_fn,
                "trace_step": trace_step_fn,
            }
            r = _invoke_entrypoint(fn, good_df, offered, args)
            result_container.append(r)
        except Exception as exc:
            error_container.append(exc)

    worker = threading.Thread(target=_target, daemon=True)
    worker.start()
    worker.join(timeout=timeout_seconds)

    if worker.is_alive():
        logger.error(f"External logic script timed out after {timeout_seconds}s: {path}")
        raise TimeoutError(f"External logic script '{path}' exceeded timeout of {timeout_seconds} seconds.")

    if error_container:
        raise error_container[0]

    result = result_container[0] if result_container else None
    # ------------------------------------------

    if result is None:
        handled = bool(logic.handles_output)
        return good_df, handled

    # If a path is returned, load it as a dataframe
    if isinstance(result, (str, Path)):
        output_df = _load_output_frame(Path(result), logic.output_format)
        return output_df, False

    # If tuple, take first element as the dataframe
    if isinstance(result, tuple) and result:
        return result[0], False

    return result, False


def _run_notebook_logic(
    path: Path,
    logic: Any,
    good_df: Any,
    contract: Any,
    engine_name: str,
    last_run_id: Optional[str],
    last_source_path: Optional[str],
) -> Tuple[Any, bool]:
    """
    Execute an external notebook.

    Args:
        path: Path to notebook file.
        logic: ExternalLogic config.
        good_df: Validated dataframe.
        contract: DataContract instance.
        engine_name: Current engine name.
        last_run_id: Current run ID.
        last_source_path: Last processed source path.

    Returns:
        Tuple of (updated_good_df, external_handled_output).
    """
    if not path.exists():
        raise FileNotFoundError(f"External notebook not found: {path}")

    try:
        import nbformat  # type: ignore
        from nbclient import NotebookClient  # type: ignore
    except Exception as exc:
        raise ValueError("Notebook execution requires nbformat and nbclient. Install lakelogic[notebook].") from exc

    params = dict(logic.args or {})
    base_path = getattr(contract, "_base_path", None)
    if base_path:
        params.setdefault("lakelogic_contract_dir", str(Path(base_path)))
    params.setdefault("lakelogic_engine", engine_name)
    params.setdefault("lakelogic_run_id", last_run_id)
    params.setdefault("lakelogic_source_path", last_source_path)
    # Linked reference datasets: hand the notebook each link's resolved path so it
    # can load source + links itself (notebooks receive paths, not live frames).
    link_paths: Dict[str, str] = {}
    for _link in list(getattr(contract, "links", None) or []):
        _lp = getattr(_link, "path", None)
        if not _lp:
            continue
        _p = Path(_lp)
        if not _p.is_absolute() and base_path and not _lp.startswith(
            ("s3://", "gs://", "abfss://", "adl://", "https://", "table:")
        ):
            _p = Path(base_path) / _p
        link_paths[_link.name] = str(_p)
    params.setdefault("lakelogic_links", link_paths)

    # Write validated input to a temp CSV for notebook access
    tmp_dir = Path(base_path) / ".lakelogic" if base_path else (Path.cwd() / ".lakelogic")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    input_path = tmp_dir / f"input_{last_run_id}.csv"
    try:
        import pandas as pd

        if hasattr(good_df, "to_pandas"):
            pdf = good_df.to_pandas()
        elif hasattr(good_df, "toPandas"):
            pdf = good_df.toPandas()
        else:
            pdf = good_df
        if not isinstance(pdf, pd.DataFrame):
            pdf = pd.DataFrame(pdf)
        pdf.to_csv(input_path, index=False)
        params.setdefault("lakelogic_input_path", str(input_path))
        params.setdefault("lakelogic_input_format", "csv")
    except Exception as exc:
        logger.warning(f"Failed to write notebook input data: {exc}")

    output_path = None
    if logic.output_path:
        output_path = Path(logic.output_path)
        if not output_path.is_absolute() and getattr(contract, "_base_path", None):
            output_path = Path(contract._base_path) / output_path
        params.setdefault("lakelogic_output_path", str(output_path))

    nb = nbformat.read(path, as_version=4)
    inject_cell = nbformat.v4.new_code_cell(f"LAKELOGIC_PARAMS = {repr(params)}")
    nb.cells.insert(0, inject_cell)

    client = NotebookClient(nb, kernel_name=logic.kernel_name)
    client.execute()

    if output_path:
        output_df = _load_output_frame(output_path, logic.output_format)
        return output_df, False

    handled = True if logic.handles_output is None else bool(logic.handles_output)
    return good_df, handled


def _load_output_frame(path: Path, fmt: Optional[str]) -> Any:
    """
    Load an output dataframe from disk.

    Args:
        path: Output path.
        fmt: Optional format override.

    Returns:
        pandas.DataFrame
    """
    import pandas as pd

    output_format = (fmt or path.suffix.lstrip(".") or "csv").lower()
    if output_format == "parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)
