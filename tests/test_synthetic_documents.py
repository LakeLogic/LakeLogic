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


def test_image_documents_use_the_extension_the_contract_reads(tmp_path):
    """A contract reading `*.jpg` gets JPEGs, still marked synthetic; no extension means PNG."""
    pytest.importorskip("PIL")
    from lakelogic.core.synthetic_documents import _image_ext, _write_image, is_synthetic_document

    assert _image_ext({"source": {"path": "receipts/*.jpg"}}) == "jpg"
    assert _image_ext({"source": {"format": "image"}}) == "png"
    jpg = _write_image(tmp_path / "r", "T", ["Total: 9.99"], "jpg")
    assert jpg.suffix == ".jpg" and is_synthetic_document(jpg)


# ── layout patterns: the value comes from the pattern (2026-11-22) ─────────────────────────────
#
# RideFlow's document contracts describe their documents' LAYOUT — `([A-Z]{3}\d{6}[A-Z])`, a value
# on the line after "Receipt:", a merchant on a line of its own. The writer rendered only
# "Label: value" lines and left every other field out, so the extraction found nothing and a
# correct contract read as broken in the Test stage.

import random as _random
import re as _re

from lakelogic.core.synthetic_documents import text_from_pattern, value_from_pattern


def test_a_layout_value_is_made_from_its_pattern():
    v = value_from_pattern(r"([A-Z]{3}\d{6}[A-Z])", _random.Random(1))
    assert _re.fullmatch(r"[A-Z]{3}\d{6}[A-Z]", v)


def test_a_labelled_pattern_keeps_its_label_and_a_value_that_fits():
    text = text_from_pattern(r"Licence\s*No:\s*([A-Z]{2}-\d{8})", _random.Random(1), "LIC-123")
    found = _re.search(r"Licence\s*No:\s*([A-Z]{2}-\d{8})", text)
    assert found and _re.fullmatch(r"[A-Z]{2}-\d{8}", found.group(1))


def test_a_typed_value_that_fits_is_kept():
    pattern = r"Date:\s*(\d{2}/\d{2}/\d{4})"
    text = text_from_pattern(pattern, _random.Random(1), "14/03/2026")
    assert _re.search(pattern, text).group(1) == "14/03/2026"


def test_a_value_that_depends_on_the_line_before_is_written_with_it():
    pattern = r"Receipt:\s*[A-Z]\d{6}\s*\n([^\n]+)"
    text = text_from_pattern(pattern, _random.Random(1), "Airport transfer")
    assert _re.search(pattern, text).group(1) == "Airport transfer"


def test_layout_documents_read_back_every_field(tmp_path):
    from lakelogic.core.synthetic_documents import generate_documents

    contract = {
        "version": "1.0.0",
        "info": {"title": "Licences"},
        "source": {"type": "landing", "format": "pdf", "path": "landing/*.pdf"},
        "extraction": {"provider": "pdfplumber"},
        "model": {"fields": [
            {"name": "licence_no", "type": "string", "extraction_task": "metadata",
             "extraction_examples": [r"([A-Z]{3}\d{6}[A-Z])"]},
            {"name": "service", "type": "string", "extraction_task": "metadata",
             "extraction_examples": [r"Receipt:\s*[A-Z]\d{6}\s*\n([^\n]+)"]},
        ]},
    }
    paths = generate_documents(contract, tmp_path, rows=3)
    import pdfplumber

    for p in paths:
        with pdfplumber.open(p) as pdf:
            text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        assert _re.search(r"([A-Z]{3}\d{6}[A-Z])", text)
        assert _re.search(r"Receipt:\s*[A-Z]\d{6}\s*\n([^\n]+)", text)


# ── a sample a person can read: labels, real dates in order, one space (2026-11-22) ──────────────


def _layout_contract():
    return {
        "version": "1.0.0",
        "info": {"title": "Licences"},
        "source": {"type": "landing", "format": "pdf", "path": "landing/*.pdf"},
        "extraction": {"provider": "pdfplumber"},
        "model": {"fields": [
            {"name": "licence_no", "type": "string", "extraction_task": "metadata",
             "extraction_examples": [r"([A-Z]{3}\d{6}[A-Z])"]},
            {"name": "issuing_city", "type": "string", "extraction_task": "metadata",
             "extraction_examples": [r"^([A-Z]{3})$"]},
            {"name": "date_of_birth", "type": "string", "extraction_task": "metadata",
             "extraction_examples": [r"(\d{2}/\d{2}/\d{4})(?=[\s\S]*\d{2}/\d{2}/\d{4})"]},
            {"name": "expiry_date", "type": "string", "extraction_task": "metadata",
             "extraction_examples": [r"\d{2}/\d{2}/\d{4}[\s\S]*?(\d{2}/\d{2}/\d{4})"]},
        ]},
    }


def _pdf_text(path):
    import pdfplumber

    with pdfplumber.open(path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def test_every_layout_value_is_labelled_with_its_field(tmp_path):
    from lakelogic.core.synthetic_documents import generate_documents

    text = _pdf_text(generate_documents(_layout_contract(), tmp_path, rows=1)[0])
    assert "Licence no:" in text
    assert "Issuing city:" in text


def test_labels_never_change_what_the_reader_extracts(tmp_path):
    """Checked the way the READER matches — re.MULTILINE: `^([A-Z]{3})$` still finds the code alone."""
    from lakelogic.core.synthetic_documents import READER_FLAGS, generate_documents

    for path in generate_documents(_layout_contract(), tmp_path, rows=5):
        text = _pdf_text(path)
        city = _re.search(r"^([A-Z]{3})$", text, READER_FLAGS)
        assert city and city.group(1) != "Iss"


def test_dates_are_real_and_in_order(tmp_path):
    from datetime import datetime

    from lakelogic.core.synthetic_documents import READER_FLAGS, generate_documents

    for path in generate_documents(_layout_contract(), tmp_path, rows=5):
        text = _pdf_text(path)
        born = _re.search(r"(\d{2}/\d{2}/\d{4})(?=[\s\S]*\d{2}/\d{2}/\d{4})", text, READER_FLAGS).group(1)
        expires = _re.search(r"\d{2}/\d{2}/\d{4}[\s\S]*?(\d{2}/\d{2}/\d{4})", text, READER_FLAGS).group(1)
        b, e = datetime.strptime(born, "%d/%m/%Y"), datetime.strptime(expires, "%d/%m/%Y")
        assert b < e


def test_a_space_pattern_writes_one_space():
    from lakelogic.core.synthetic_documents import text_from_pattern

    text = text_from_pattern(r"Licence\s*No:\s*([A-Z]{2}-\d{8})", _random.Random(3), "x")
    assert text.startswith("Licence No: ")


def test_a_pattern_label_is_not_said_twice(tmp_path):
    from lakelogic.core.synthetic_documents import generate_documents

    contract = {**_layout_contract(), "model": {"fields": [
        {"name": "incident_id", "type": "string", "extraction_task": "metadata",
         "extraction_examples": [r"Incident\s*ID\s*([A-Z]{3}-\d{8}-\d{3})"]},
    ]}}
    text = _pdf_text(generate_documents(contract, tmp_path, rows=1)[0])
    assert "Incident id:" not in text and "Incident ID " in text
