from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_cli_module_exports_app_and_main_executes_once(monkeypatch):
    calls = []
    # `sys.modules.setdefault(...)` inserted a bare stub when nothing had imported
    # `lakelogic.cli` yet — and `lakelogic.cli` is a REAL package, so that stub
    # shadowed it for the rest of the session depending only on test ordering.
    # Insert through monkeypatch so it is removed again.
    if "lakelogic.cli" in sys.modules:
        cli_root = sys.modules["lakelogic.cli"]
    else:
        cli_root = types.ModuleType("lakelogic.cli")
        monkeypatch.setitem(sys.modules, "lakelogic.cli", cli_root)
    main_module = types.ModuleType("lakelogic.cli.main")

    def fake_app():
        calls.append("called")

    main_module.app = fake_app
    # Via monkeypatch, NOT bare assignment. These were `sys.modules[...] = ...` and
    # `cli_root.main = ...`, so the stub outlived the test: every later-sorting file
    # that imported the CLI got a fake `app` and died with
    # "'function' object has no attribute '_add_completion'". The tests passed in
    # isolation and failed in the suite, which is the worst way for this to present.
    monkeypatch.setitem(sys.modules, "lakelogic.cli.main", main_module)
    monkeypatch.setattr(cli_root, "main", main_module, raising=False)

    module = _load_module("test_lakelogic_cli", ROOT / "lakelogic" / "cli.py")

    assert module.app is fake_app
    assert module.__all__ == ["app"]

    cli_path = ROOT / "lakelogic" / "cli.py"
    globals_dict = {"__name__": "__main__"}
    exec(compile(cli_path.read_text(encoding="utf-8"), str(cli_path), "exec"), globals_dict)

    assert calls == ["called"]


def test_pipeline_init_exports_lakehouse_pipeline():
    pipeline_root = sys.modules.setdefault("lakelogic.pipeline", types.ModuleType("lakelogic.pipeline"))
    runner_module = types.ModuleType("lakelogic.pipeline.runner")

    class FakePipeline:
        pass

    runner_module.LakehousePipeline = FakePipeline
    sys.modules["lakelogic.pipeline.runner"] = runner_module
    pipeline_root.runner = runner_module

    module = _load_module(
        "test_lakelogic_pipeline_init",
        ROOT / "lakelogic" / "pipeline" / "__init__.py",
    )

    assert module.LakehousePipeline is FakePipeline
    assert module.__all__ == ["LakehousePipeline"]


def test_tools_init_exports_template_helpers():
    from lakelogic.tools import __all__, apply_contract_template, collect_contract_paths
    from lakelogic.tools.template_apply import TemplateApplyResult

    assert __all__ == [
        "TemplateApplyResult",
        "apply_contract_template",
        "collect_contract_paths",
    ]
    assert callable(apply_contract_template)
    assert callable(collect_contract_paths)
    assert TemplateApplyResult.__name__ == "TemplateApplyResult"
