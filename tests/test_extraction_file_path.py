"""Guards for file-provider extraction (pdfplumber / easyocr).

Both tests reproduce a live Databricks incident: 90 PDFs were "processed", every
row opened ``''`` instead of its real path, and the run still reported success.
No PDFs, no Spark — the frame and the provider are faked, as elsewhere in the suite.
"""

import pytest

from lakelogic.core.models import ExtractionConfig
from lakelogic.core import extraction as extraction_mod
from lakelogic.engines import llm as llm_mod


class _Contract:
    def __init__(self, config):
        self.extraction = config


def _config(**over):
    base = {"provider": "pdfplumber", "output_schema": []}
    base.update(over)
    return ExtractionConfig(**base)


def test_file_provider_uses_the_row_path_not_an_empty_text_column(monkeypatch):
    """A binaryFile-style row has `path`, not a text column — extract from `path`."""
    seen = []

    def _fake_extract_file(file_path, config):
        seen.append(file_path)
        return [{"licence_no": "AB-1"}]

    monkeypatch.setattr(llm_mod, "extract_file", _fake_extract_file)

    row = {"path": "/Volumes/demo/landing/driver_licences/a.pdf", "length": 1234}
    result = llm_mod.extract_row(row, _config())

    assert seen == ["/Volumes/demo/landing/driver_licences/a.pdf"]
    assert result["licence_no"] == "AB-1"


def test_file_provider_without_any_path_column_raises():
    """No path anywhere must fail loudly instead of silently opening ''."""
    with pytest.raises(ValueError) as exc:
        llm_mod.extract_row({"length": 1234}, _config())
    assert "file path" in str(exc.value)


def test_batch_where_every_row_fails_is_a_run_level_error(monkeypatch):
    """100% extraction failure is a broken run, not a green run over empty columns."""
    import pandas as pd

    def _boom(row, config, client=None):
        raise FileNotFoundError("[Errno 2] No such file or directory: ''")

    monkeypatch.setattr(llm_mod, "extract_row", _boom)

    df = pd.DataFrame([{"path": "a.pdf"}, {"path": "b.pdf"}])
    with pytest.raises(RuntimeError) as exc:
        extraction_mod.apply_extraction(_Contract(_config()), df, "polars")

    msg = str(exc.value)
    assert "all 2 row(s)" in msg
    assert "No such file or directory" in msg


def test_partial_extraction_failure_still_returns_rows(monkeypatch):
    """Only a total failure fails the run; partial failures stay quarantine-shaped."""
    import pandas as pd

    calls = {"n": 0}

    def _half(row, config, client=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise FileNotFoundError("boom")
        return {**row, "licence_no": "AB-2"}

    monkeypatch.setattr(llm_mod, "extract_row", _half)

    df = pd.DataFrame([{"path": "a.pdf"}, {"path": "b.pdf"}])
    out = extraction_mod.apply_extraction(_Contract(_config()), df, "polars")
    assert len(out) == 2
