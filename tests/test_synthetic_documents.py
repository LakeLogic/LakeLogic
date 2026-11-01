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
    "source": {
        "type": "landing",
        "path": "/lakehouse/default/{landing_root}/driver_licences/**/*.pdf",
        "format": "pdf",
        "load_mode": "full",
    },
    "extraction": {"provider": "pdfplumber"},
    "model": {
        "fields": [
            {"name": "file_path", "type": "string"},
            {
                "name": "full_name",
                "type": "string",
                "extraction_task": "metadata",
                "extraction_examples": [r"Name:\s+(.+)"],
            },
            {
                "name": "licence_number",
                "type": "string",
                "extraction_task": "metadata",
                "extraction_examples": [r"Licence No:\s+(\S+)"],
            },
            {
                "name": "date_of_birth",
                "type": "date",
                "extraction_task": "metadata",
                "extraction_examples": [r"DOB:\s+(\d{4}-\d{2}-\d{2})"],
            },
            {
                "name": "expiry_date",
                "type": "date",
                "extraction_task": "metadata",
                "extraction_examples": [r"EXP:\s+(\d{4}-\d{2}-\d{2})"],
            },
            {
                "name": "vehicle_classes",
                "type": "string",
                "extraction_task": "metadata",
                "extraction_examples": [r"Classes:\s+(.+)"],
            },
        ]
    },
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
    with pytest.raises(ValueError, match="supports document sources"):
        generate_documents(llm, tmp_path)


def test_rows_extracted_from_a_synthetic_document_are_marked(tmp_path):
    """A seeded document's rows carry `_synthetic=true`, as seeded table rows do; a real
    document's rows do not, even when its text mentions the mark."""
    from reportlab.pdfgen import canvas

    from lakelogic.core.models import DataContract
    from lakelogic.core.synthetic_documents import SYNTHETIC_DOCUMENT_MARK, is_synthetic_document
    from lakelogic.engines.llm import extract_file

    synthetic = generate_documents(LICENCES, tmp_path, rows=2, seed=3)
    real = tmp_path / "real.pdf"
    pdf = canvas.Canvas(str(real))
    pdf.drawString(72, 740, f"Name: Real Person {SYNTHETIC_DOCUMENT_MARK}")
    pdf.save()

    assert all(is_synthetic_document(p) for p in synthetic)
    assert not is_synthetic_document(real)
    config = DataContract(**LICENCES).extraction
    assert all(r.get("_synthetic") is True for p in synthetic for r in extract_file(str(p), config))
    assert all("_synthetic" not in r for r in extract_file(str(real), config))


def test_word_and_image_documents_carry_the_mark(tmp_path):
    pytest.importorskip("docx")
    pytest.importorskip("PIL")
    from lakelogic.core.synthetic_documents import _write_docx, _write_image, is_synthetic_document

    assert is_synthetic_document(_write_docx(tmp_path / "a", "T", ["Name: X"]))
    assert is_synthetic_document(_write_image(tmp_path / "b", "T", ["Name: X"]))
