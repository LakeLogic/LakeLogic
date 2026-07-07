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

import threading
from pathlib import Path
from typing import Any, Callable, Optional, Tuple

from loguru import logger

# Default timeout for external script execution (seconds).
# Override via contract.external_logic.timeout_seconds.
DEFAULT_TIMEOUT_SECONDS = 300


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

    if logic_type == "python":
        return _run_python_logic(
            path,
            logic,
            good_df,
            contract,
            engine_name,
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
            engine_name,
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

    # ---------- execute with timeout ----------
    result_container: list = []
    error_container: list = []

    def _target():
        try:
            # Inject tracing callback if the user is interested
            try:
                r = fn(
                    good_df,
                    contract=contract,
                    engine=engine_name,
                    add_trace=add_trace_fn,
                    trace_step=trace_step_fn,
                    **args,
                )
            except TypeError as exc:
                if "trace_step" in str(exc) or "add_trace" in str(exc):
                    r = fn(good_df, contract=contract, engine=engine_name, **args)
                else:
                    raise
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
