from __future__ import annotations

import builtins
import importlib.util
import sys
import types
from pathlib import Path

import pytest

from lakelogic.core import external_logic as ext


def _logic(**overrides):
    base = {
        "type": "python",
        "path": "logic.py",
        "entrypoint": "run",
        "args": {},
        "handles_output": False,
        "output_format": None,
        "output_path": None,
        "kernel_name": "python3",
        "timeout_seconds": None,
    }
    base.update(overrides)
    return types.SimpleNamespace(**base)


def _contract(logic=None, base_path=None):
    return types.SimpleNamespace(external_logic=logic, _base_path=base_path)


def _write_script(path: Path, source: str) -> Path:
    path.write_text(source, encoding="utf-8")
    return path


def test_apply_external_logic_short_circuits_and_resolves_paths(monkeypatch, tmp_path):
    good_df = {"value": 1}
    assert ext.apply_external_logic(_contract(None), good_df, "engine", "run-1", None) == (good_df, False)

    warnings = []
    monkeypatch.setattr(ext.logger, "warning", warnings.append)
    missing_path_logic = _logic(path="")
    assert ext.apply_external_logic(_contract(missing_path_logic), good_df, "engine", "run-1", None) == (good_df, False)
    assert warnings[-1] == "external_logic configured without path; skipping."

    called = {}

    def fake_run_python_logic(path, logic, *args):
        called["path"] = path
        return ("updated", True)

    monkeypatch.setattr(ext, "_run_python_logic", fake_run_python_logic)
    resolved = ext.apply_external_logic(_contract(_logic(), base_path=tmp_path), good_df, "engine", "run-1", None)
    assert resolved == ("updated", True)
    assert called["path"] == tmp_path / "logic.py"

    monkeypatch.setattr(ext, "_run_notebook_logic", lambda path, logic, *args: ("notebook", True))
    notebook_result = ext.apply_external_logic(
        _contract(_logic(type="notebook", path="logic.ipynb"), base_path=tmp_path),
        good_df,
        "engine",
        "run-1",
        None,
    )
    assert notebook_result == ("notebook", True)

    unsupported = ext.apply_external_logic(_contract(_logic(type="shell")), good_df, "engine", "run-1", None)
    assert unsupported == (good_df, False)
    assert any("Unsupported external_logic.type" in message for message in warnings)


def test_run_python_logic_main_paths(monkeypatch, tmp_path):
    path = tmp_path / "logic.py"
    _write_script(
        path,
        "def run(good_df, contract, engine, suffix='!'):\n    return {'value': f'{good_df}-{engine}{suffix}'}\n",
    )
    contract = _contract(_logic(args={"suffix": "?"}), base_path=tmp_path)

    result = ext._run_python_logic(path, contract.external_logic, "df", contract, "polars", "run-2", None, None)
    assert result == ({"value": "df-polars?"}, False)

    tuple_script = _write_script(
        tmp_path / "tuple_logic.py",
        "def run(good_df, contract, engine, add_trace=None, trace_step=None):\n"
        "    return ('tuple-df', {'ignored': True})\n",
    )
    tuple_logic = _logic(path=str(tuple_script), handles_output=True)
    tuple_result = ext._run_python_logic(tuple_script, tuple_logic, "df", contract, "spark", "run-3", None, None)
    assert tuple_result == ("tuple-df", False)

    none_script = _write_script(
        tmp_path / "none_logic.py",
        "def run(good_df, contract, engine, add_trace=None, trace_step=None):\n    return None\n",
    )
    none_logic = _logic(path=str(none_script), handles_output=True)
    none_result = ext._run_python_logic(none_script, none_logic, "df", contract, "spark", "run-4", None, None)
    assert none_result == ("df", True)

    output_script = _write_script(
        tmp_path / "output_logic.py",
        "def run(good_df, contract, engine, add_trace=None, trace_step=None):\n"
        f"    return r'{tmp_path / 'output.csv'}'\n",
    )
    monkeypatch.setattr(ext, "_load_output_frame", lambda path, fmt: {"path": str(path), "fmt": fmt})
    output_logic = _logic(path=str(output_script), output_format="csv")
    output_result = ext._run_python_logic(output_script, output_logic, "df", contract, "spark", "run-5", None, None)
    assert output_result == ({"path": str(tmp_path / "output.csv"), "fmt": "csv"}, False)


def test_run_python_logic_errors_and_restricted_imports(monkeypatch, tmp_path):
    contract = _contract(_logic(), base_path=tmp_path)
    with pytest.raises(FileNotFoundError):
        ext._run_python_logic(
            tmp_path / "missing.py", contract.external_logic, "df", contract, "spark", "run", None, None
        )

    real_thread = ext.threading.Thread
    monkeypatch.setattr(
        ext.threading,
        "Thread",
        lambda *args, **kwargs: types.SimpleNamespace(
            start=lambda: None, join=lambda timeout=None: None, is_alive=lambda: True
        ),
    )
    timeout_script = _write_script(tmp_path / "timeout.py", "def run(good_df, contract, engine):\n    return good_df\n")
    with pytest.raises(TimeoutError):
        ext._run_python_logic(
            timeout_script,
            _logic(path=str(timeout_script), timeout_seconds=1),
            "df",
            contract,
            "spark",
            "run",
            None,
            None,
        )

    monkeypatch.setattr(ext.threading, "Thread", real_thread)
    blocked = _write_script(
        tmp_path / "blocked.py", "import subprocess\n\ndef run(good_df, contract, engine):\n    return good_df\n"
    )
    with pytest.raises(ImportError):
        ext._run_python_logic(blocked, _logic(path=str(blocked)), "df", contract, "spark", "run", None, None)

    allowed = _write_script(
        tmp_path / "allowed.py",
        "import math\n\ndef run(good_df, contract, engine):\n    return {'pi': round(math.pi, 2)}\n",
    )
    assert ext._run_python_logic(allowed, _logic(path=str(allowed)), "df", contract, "spark", "run", None, None) == (
        {"pi": 3.14},
        False,
    )

    missing_entrypoint = _write_script(tmp_path / "missing_entry.py", "value = 1\n")
    with pytest.raises(AttributeError):
        ext._run_python_logic(
            missing_entrypoint,
            _logic(path=str(missing_entrypoint), entrypoint="custom"),
            "df",
            contract,
            "spark",
            "run",
            None,
            None,
        )

    exploding = _write_script(
        tmp_path / "explode.py",
        "def run(good_df, contract, engine, add_trace=None, trace_step=None):\n    raise RuntimeError('boom')\n",
    )
    with pytest.raises(RuntimeError, match="boom"):
        ext._run_python_logic(exploding, _logic(path=str(exploding)), "df", contract, "spark", "run", None, None)

    type_error = _write_script(
        tmp_path / "type_error.py",
        "def run(good_df, contract, engine, add_trace=None, trace_step=None):\n"
        "    raise TypeError('different problem')\n",
    )
    with pytest.raises(TypeError, match="different problem"):
        ext._run_python_logic(type_error, _logic(path=str(type_error)), "df", contract, "spark", "run", None, None)


def test_run_python_logic_invalid_spec(monkeypatch, tmp_path):
    path = _write_script(tmp_path / "logic.py", "def run(good_df, contract, engine):\n    return good_df\n")
    contract = _contract(_logic(path=str(path)), base_path=tmp_path)
    monkeypatch.setattr(importlib.util, "spec_from_file_location", lambda *args, **kwargs: None)

    with pytest.raises(ValueError):
        ext._run_python_logic(path, contract.external_logic, "df", contract, "spark", "run", None, None)


def test_run_python_logic_with_none_args_and_loader_missing(monkeypatch, tmp_path):
    path = _write_script(tmp_path / "logic.py", "def run(good_df, contract, engine):\n    return good_df\n")
    contract = _contract(_logic(path=str(path), args=None), base_path=tmp_path)

    original = importlib.util.spec_from_file_location

    class LoaderlessSpec:
        loader = None

    monkeypatch.setattr(importlib.util, "spec_from_file_location", lambda *args, **kwargs: LoaderlessSpec())
    with pytest.raises(ValueError):
        ext._run_python_logic(path, contract.external_logic, "df", contract, "spark", "run", None, None)

    monkeypatch.setattr(importlib.util, "spec_from_file_location", original)
    monkeypatch.setattr(ext, "_load_output_frame", lambda path, fmt: {"loaded": str(path), "fmt": fmt})
    final_path = _write_script(
        tmp_path / "path_logic.py", "def run(good_df, contract, engine):\n    return {'ok': True}\n"
    )
    contract = _contract(_logic(path=str(final_path), args=None), base_path=tmp_path)
    assert ext._run_python_logic(final_path, contract.external_logic, "df", contract, "spark", "run", None, None) == (
        {"ok": True},
        False,
    )


def test_run_notebook_logic_and_output_loading(monkeypatch, tmp_path):
    notebook_path = _write_script(tmp_path / "logic.ipynb", "{}")
    output_path = tmp_path / "output.csv"
    logic = _logic(type="notebook", path=str(notebook_path), output_path="output.csv", handles_output=None)
    contract = _contract(logic, base_path=tmp_path)

    class FakeNotebook:
        def __init__(self):
            self.cells = []

    executed = {}

    class FakeNotebookClient:
        def __init__(self, nb, kernel_name):
            executed["kernel_name"] = kernel_name
            self.nb = nb

        def execute(self):
            executed["executed"] = True

    fake_nbformat = types.SimpleNamespace(
        read=lambda path, as_version=4: FakeNotebook(),
        v4=types.SimpleNamespace(new_code_cell=lambda code: {"cell_type": "code", "source": code}),
    )

    class FakeDataFrame:
        def __init__(self, data):
            self.data = data

        def to_csv(self, path, index=False):
            Path(path).write_text("id\n1\n", encoding="utf-8")

    fake_pandas = types.SimpleNamespace(
        DataFrame=FakeDataFrame,
        read_csv=lambda path: {"csv": str(path)},
        read_parquet=lambda path: {"parquet": str(path)},
    )

    monkeypatch.setitem(sys.modules, "nbformat", fake_nbformat)
    monkeypatch.setitem(sys.modules, "nbclient", types.SimpleNamespace(NotebookClient=FakeNotebookClient))
    monkeypatch.setitem(sys.modules, "pandas", fake_pandas)

    result = ext._run_notebook_logic(notebook_path, logic, [{"id": 1}], contract, "spark", "run-9", "source.csv")
    assert result == ({"csv": str(output_path)}, False)
    assert executed["kernel_name"] == "python3"
    assert executed["executed"] is True

    no_output_logic = _logic(type="notebook", path=str(notebook_path), output_path=None, handles_output=None)
    no_output = ext._run_notebook_logic(
        notebook_path, no_output_logic, [{"id": 1}], contract, "spark", "run-10", "source.csv"
    )
    assert no_output == ([{"id": 1}], True)

    class WithToPandas:
        def toPandas(self):
            return FakeDataFrame([{"id": 2}])

    no_output_false_logic = _logic(type="notebook", path=str(notebook_path), output_path=None, handles_output=False)
    to_pandas_input = WithToPandas()
    no_output_false = ext._run_notebook_logic(
        notebook_path,
        no_output_false_logic,
        to_pandas_input,
        contract,
        "spark",
        "run-11",
        "source.csv",
    )
    assert no_output_false == (to_pandas_input, False)

    assert ext._load_output_frame(tmp_path / "result.csv", None) == {"csv": str(tmp_path / "result.csv")}
    assert ext._load_output_frame(tmp_path / "result.parquet", None) == {"parquet": str(tmp_path / "result.parquet")}


def test_run_notebook_logic_logs_input_write_failures(monkeypatch, tmp_path):
    notebook_path = _write_script(tmp_path / "logic.ipynb", "{}")
    contract = _contract(
        _logic(type="notebook", path=str(notebook_path), output_path=None, handles_output=None), base_path=tmp_path
    )

    class FakeNotebook:
        def __init__(self):
            self.cells = []

    class FakeNotebookClient:
        def __init__(self, nb, kernel_name):
            self.nb = nb

        def execute(self):
            return None

    class FailingFrame:
        def __init__(self, data):
            raise ValueError("bad frame")

    warnings = []
    monkeypatch.setattr(ext.logger, "warning", warnings.append)
    monkeypatch.setitem(
        sys.modules,
        "nbformat",
        types.SimpleNamespace(
            read=lambda path, as_version=4: FakeNotebook(),
            v4=types.SimpleNamespace(new_code_cell=lambda code: {"source": code}),
        ),
    )
    monkeypatch.setitem(sys.modules, "nbclient", types.SimpleNamespace(NotebookClient=FakeNotebookClient))
    monkeypatch.setitem(
        sys.modules,
        "pandas",
        types.SimpleNamespace(DataFrame=FailingFrame, read_csv=lambda path: {}, read_parquet=lambda path: {}),
    )

    class WithToPandas:
        def to_pandas(self):
            return {"rows": [1]}

    failing_input = WithToPandas()
    result = ext._run_notebook_logic(
        notebook_path, contract.external_logic, failing_input, contract, "spark", "run-12", None
    )
    assert result == (failing_input, True)
    assert any("Failed to write notebook input data" in message for message in warnings)


def test_run_notebook_logic_errors(monkeypatch, tmp_path):
    contract = _contract(_logic(type="notebook", path="logic.ipynb"), base_path=tmp_path)
    with pytest.raises(FileNotFoundError):
        ext._run_notebook_logic(tmp_path / "missing.ipynb", contract.external_logic, [], contract, "spark", "run", None)

    notebook_path = _write_script(tmp_path / "logic.ipynb", "{}")
    original_import = builtins.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"nbformat", "nbclient"}:
            raise ImportError("missing")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    with pytest.raises(ValueError, match="Notebook execution requires nbformat and nbclient"):
        ext._run_notebook_logic(notebook_path, contract.external_logic, [], contract, "spark", "run", None)
