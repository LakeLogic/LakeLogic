"""Document-extraction contracts get synthetic documents that their own extraction reads back.

THE LIVE FAILURE (Fabric, 2026-09-14): `bronze_checkr_driver_licences` extracts fields from PDFs
with pdfplumber regexes. `DataGenerator` refuses extraction contracts, so the synthetic estate
had no PDFs, bronze failed with `0 pdf files matched glob`, and `shared` never ran behind it.
"""

import pytest

from lakelogic.core.synthetic_documents import (
    extract_metadata,
    generate_documents,
    line_label,
    supports_documents,
)

pytest.importorskip("reportlab")
pytest.importorskip("pdfplumber")

LICENCES = {
    "dataset": "bronze_checkr_driver_licences",
    "version": "1.0.0",
    "info": {"title": "Bronze - Checkr Driver Licences"},
    "source": {"type": "landing", "path": "/lakehouse/default/{landing_root}/driver_licences/**/*.pdf",
               "format": "pdf", "load_mode": "full"},
    "extraction": {"provider": "pdfplumber"},
    "model": {"fields": [
        {"name": "file_path", "type": "string"},
        {"name": "full_name", "type": "string", "extraction_task": "metadata",
         "extraction_examples": [r"Name:\s+(.+)"]},
        {"name": "licence_number", "type": "string", "extraction_task": "metadata",
         "extraction_examples": [r"Licence No:\s+(\S+)"]},
        {"name": "date_of_birth", "type": "date", "extraction_task": "metadata",
         "extraction_examples": [r"DOB:\s+(\d{4}-\d{2}-\d{2})"]},
        {"name": "expiry_date", "type": "date", "extraction_task": "metadata",
         "extraction_examples": [r"EXP:\s+(\d{4}-\d{2}-\d{2})"]},
        {"name": "vehicle_classes", "type": "string", "extraction_task": "metadata",
         "extraction_examples": [r"Classes:\s+(.+)"]},
    ]},
    "quality": {"row_rules": [{"not_null": "licence_number"}]},
}


def test_the_line_label_is_the_literal_text_before_the_capture_group():
    assert line_label(r"Name:\s+(.+)") == "Name:"
    assert line_label(r"Licence No:\s+(\S+)") == "Licence No:"
    assert line_label(r"^EXP:\s*(\d{4}-\d{2}-\d{2})") == "EXP:"
    assert line_label(r"no group here") is None


def test_every_field_the_contract_extracts_reads_back_from_its_document(tmp_path):
    paths = generate_documents(LICENCES, tmp_path, rows=6, seed=3)
    assert len(paths) == 6 and all(p.suffix == ".pdf" for p in paths)
    for path in paths:
        values = extract_metadata(path, LICENCES)
        for field in ("full_name", "licence_number", "date_of_birth", "expiry_date", "vehicle_classes"):
            assert values[field], (path.name, field, values)
        assert len(values["date_of_birth"]) == 10 and values["date_of_birth"][4] == "-"


def test_invalid_documents_leave_out_a_required_field_so_quality_quarantines_them(tmp_path):
    paths = generate_documents(LICENCES, tmp_path, rows=20, seed=5, invalid_ratio=0.25)
    missing = [p for p in paths if extract_metadata(p, LICENCES)["licence_number"] is None]
    assert len(missing) == 5, "a quarter of the documents should lack the not_null field"


def test_the_same_seed_writes_the_same_documents(tmp_path):
    first = [extract_metadata(p, LICENCES) for p in generate_documents(LICENCES, tmp_path / "a", rows=3, seed=9)]
    second = [extract_metadata(p, LICENCES) for p in generate_documents(LICENCES, tmp_path / "b", rows=3, seed=9)]
    assert first == second


def test_an_llm_extraction_contract_is_refused_not_faked(tmp_path):
    llm = {**LICENCES, "extraction": {"provider": "openai"}}
    assert supports_documents(LICENCES) and not supports_documents(llm)
    with pytest.raises(ValueError, match="supports PDF sources"):
        generate_documents(llm, tmp_path)
