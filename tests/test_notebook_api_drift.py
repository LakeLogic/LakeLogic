"""Notebooks must call the pipeline API that actually exists.

The failure this guards against is silent and slow: a parameter gets renamed in
``LakehousePipeline`` — ``gdpr_column`` became ``subject_col`` — every unit test
stays green because none of them call it the old way, and the drift is only
discovered when somebody executes the notebook. That takes twenty minutes for the
full gallery, needs optional extras installed, and for the ``08*`` chain needs the
earlier notebooks to have run first. So in practice nobody ran it, and
``08g_compliance_gdpr_rtbf`` shipped calling a signature that had not existed for
some time.

This test reads the notebooks and compares their keyword arguments against
``inspect.signature``. No execution, no extras, no lakehouse — it runs in
milliseconds on any machine, and it catches exactly the class of breakage that the
expensive nbmake job was the only thing catching.

It cannot catch behavioural drift (a parameter that still exists but means
something else). That is what the nbmake workflow is for. This is the cheap net
underneath it.
"""

from __future__ import annotations

import ast
import inspect
import json
import pathlib

import pytest

from lakelogic.pipeline.runner import LakehousePipeline

NOTEBOOK_DIR = pathlib.Path(__file__).resolve().parents[1] / "examples" / "colab"


def _notebooks() -> list[pathlib.Path]:
    return sorted(NOTEBOOK_DIR.glob("*.ipynb"))


def _code_source(nb_path: pathlib.Path) -> str:
    """Concatenate a notebook's code cells into one parseable module.

    ``source`` is allowed to be either a list of lines or a single string — a bulk
    edit script here once assumed the list form and silently skipped every cell
    that used the string form, so both are handled.
    """
    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    chunks = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", "")
        text = "".join(src) if isinstance(src, list) else src
        # Notebook-only syntax (%magics, !shell) is not valid Python. Drop those
        # lines rather than abandoning the whole cell, which would blind the check.
        kept = [ln for ln in text.splitlines() if not ln.lstrip().startswith(("%", "!"))]
        chunks.append("\n".join(kept))
    return "\n".join(chunks)


def _pipeline_variables(tree: ast.AST) -> set[str]:
    """Names bound to a ``LakehousePipeline(...)`` instance.

    Matching on the method name alone is not good enough: ``run`` is the most
    common method name in these notebooks, and the simulator (``sim.run``), the
    processor (``proc.run``) and the pipeline all answer to it with completely
    different signatures. Checking those against the pipeline's parameters
    produced four failures of which three were nonsense.
    """
    names = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        call = node.value
        if not isinstance(call, ast.Call):
            continue
        fn = call.func
        ctor = fn.id if isinstance(fn, ast.Name) else fn.attr if isinstance(fn, ast.Attribute) else None
        if ctor != "LakehousePipeline":
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                names.add(target.id)
    return names


def _pipeline_calls(source: str):
    """Yield (method_name, {keyword names}, lineno) for calls on a pipeline object.

    Methods are resolved against the real class, so a rename automatically widens
    or narrows what is checked — the test cannot drift out of step with the API it
    is guarding.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return

    pipelines = _pipeline_variables(tree)
    if not pipelines:
        return

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        if not isinstance(fn, ast.Attribute):
            continue
        if not (isinstance(fn.value, ast.Name) and fn.value.id in pipelines):
            continue
        if not callable(getattr(LakehousePipeline, fn.attr, None)):
            continue
        kwargs = {kw.arg for kw in node.keywords if kw.arg is not None}
        yield fn.attr, kwargs, node.lineno


@pytest.mark.parametrize("nb_path", _notebooks(), ids=lambda p: p.name)
def test_notebook_uses_real_pipeline_parameters(nb_path):
    source = _code_source(nb_path)

    problems = []
    for method_name, kwargs, lineno in _pipeline_calls(source):
        sig = inspect.signature(getattr(LakehousePipeline, method_name))
        # A **kwargs-accepting method takes anything; nothing to assert.
        if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
            continue
        unknown = kwargs - set(sig.parameters)
        if unknown:
            problems.append(
                f"  line ~{lineno}: {method_name}() got {sorted(unknown)}; "
                f"accepts {sorted(p for p in sig.parameters if p != 'self')}"
            )

    assert not problems, f"{nb_path.name} calls the pipeline API with parameters that do not exist:\n" + "\n".join(
        problems
    )


@pytest.mark.parametrize("nb_path", _notebooks(), ids=lambda p: p.name)
def test_notebook_code_cells_parse(nb_path):
    """Every code cell must be syntactically valid Python.

    Editing a notebook means writing Python source through a JSON escaping layer,
    where one wrong backslash turns ``\\n`` inside a string literal into a real
    newline. That produced an unterminated-string-literal in this very file's
    subject notebook, and nothing but a full nbmake execution caught it. Parsing
    is free and catches it instantly.
    """
    source = _code_source(nb_path)
    try:
        ast.parse(source)
    except SyntaxError as exc:
        pytest.fail(f"{nb_path.name} has a code cell that does not parse: line {exc.lineno}: {exc.msg}")


def test_the_check_can_actually_see_pipeline_calls():
    """A guard on the guard.

    Every assertion above passes vacuously if ``_pipeline_calls`` finds nothing —
    one refactor to how notebooks import the pipeline and this file would go green
    forever while checking nothing at all.
    """
    total = sum(len(list(_pipeline_calls(_code_source(nb)))) for nb in _notebooks())
    assert total > 0, "found no LakehousePipeline calls in any notebook — the parser has stopped matching"
