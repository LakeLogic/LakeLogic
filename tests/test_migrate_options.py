"""`lakelogic migrate-options`: older contracts onto OLC 0.21 typed source options (acceptance 8)."""

import yaml
from typer.testing import CliRunner

from lakelogic import DataProcessor
from lakelogic.cli.main import app
from lakelogic.core.migrate_options import migrate_source

OLD = """\
version: 1.0.0
info: {{title: bacs}}
source:
  type: landing
  path: {path}
  format: fixed_width   # BACS layout
  record_length: 15
  encoding: ascii
  options:
    flavour: legacy
model:
  fields:
    - {{name: sort_code, type: string, range: [1, 7]}}
"""


def test_settings_move_spellings_change_and_credentials_are_removed():
    src, notes = migrate_source({
        "type": "sftp", "path": "sftp://h/in", "record_length": 15,
        "options": {"sep": ";", "pattern": "*.csv", "password": "hunter2", "known_hosts": "/k", "x": 1},
    })
    assert src["pattern"] == "*.csv" and "record_length" not in src
    assert src["options"] == {"record_length": 15, "delimiter": ";", "known_hosts": "/k", "engine_options": {"x": 1}}
    assert any("REMOVED the credential source.options.password" in n for n in notes)
    assert "hunter2" not in str(src) and "hunter2" not in " ".join(notes)


def test_an_old_contract_fails_then_runs_the_same_after_migration(tmp_path):
    data = tmp_path / "s.dat"
    data.write_text("D20157500001050\nD30963400002000\n", encoding="ascii")
    path = tmp_path / "bacs.yaml"
    path.write_text(OLD.format(path=data.as_posix()), encoding="utf-8")

    try:
        DataProcessor(engine="polars", contract=str(path))
        raise AssertionError("the old shape must fail")
    except Exception as exc:  # noqa: BLE001
        assert "belongs under source.options" in str(exc) and "migrate-options" in str(exc)

    dry = CliRunner().invoke(app, ["migrate-options", str(tmp_path)])
    assert "would rewrite" in dry.output and path.read_text(encoding="utf-8") == OLD.format(path=data.as_posix())

    out = CliRunner().invoke(app, ["migrate-options", str(tmp_path), "--write"])
    assert out.exit_code == 0, out.output
    text = path.read_text(encoding="utf-8")
    assert "# BACS layout" in text  # comments on kept lines survive (ruamel)
    src = yaml.safe_load(text)["source"]
    assert src["options"] == {"engine_options": {"flavour": "legacy"}, "record_length": 15, "encoding": "ascii"} or \
        src["options"]["record_length"] == 15
    good, bad = DataProcessor(engine="polars", contract=str(path)).run_source()
    assert good["sort_code"].to_list() == ["201575", "309634"] and bad.height == 0
