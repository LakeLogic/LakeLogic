"""`provider: document` — structured fields from any document, offline (2026-10-23).

Before it, a deployed pipeline read text PDFs only: a scanned PDF gave nothing, an image gave
its raw OCR text in every field, and a Word file had no reader. Lakehouse Studio previewed all
of them with `extract_text`, so the preview promised fields the pipeline could not produce.
`document` reads every file with that same reader, then applies one pattern per field.
"""
from __future__ import annotations

import pytest

from lakelogic.core.models import ExtractionConfig
from lakelogic.core.synthetic_documents import generate_documents, supports_documents
from lakelogic.engines.llm import FILE_PROVIDERS, extract_file

pytest.importorskip("rapidocr_onnxruntime")
pytest.importorskip("docx")

PATTERNS = {
    "licence_number": r"Licence No:\s*(\S+)",
    "expiry_date": r"Expires:\s*(\d{4}-\d{2}-\d{2})",
}


def _font(size=44):
    from PIL import ImageFont

    for name in ("arial.ttf", "DejaVuSans.ttf", "LiberationSans-Regular.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


def _lines():
    return ["DRIVING LICENCE (SPECIMEN)", "Licence No: RF-482913", "Expires: 2027-03-31"]


def _image(path):
    from PIL import Image, ImageDraw

    img = Image.new("RGB", (1300, 420), "white")
    d = ImageDraw.Draw(img)
    for i, line in enumerate(_lines()):
        d.text((50, 40 + i * 120), line, fill="black", font=_font())
    img.save(path)
    return img


def _config(**over):
    fields = [
        {"name": "licence_number", "type": "string", "extraction_task": "metadata",
         "extraction_examples": [PATTERNS["licence_number"]]},
        {"name": "expiry_date", "type": "date", "extraction_task": "metadata",
         "extraction_examples": [PATTERNS["expiry_date"]]},
    ]
    return ExtractionConfig(**{"provider": "document", "output_schema": fields, **over})


def test_document_is_a_file_provider():
    assert "document" in FILE_PROVIDERS


@pytest.mark.parametrize("kind", ["image", "scanned_pdf", "word"])
def test_every_format_yields_the_same_fields(tmp_path, kind):
    if kind == "image":
        f = tmp_path / "licence.png"
        _image(f)
    elif kind == "scanned_pdf":
        f = tmp_path / "licence.pdf"
        _image(tmp_path / "page.png").save(f, "PDF", resolution=150)  # no text layer
    else:
        import docx

        f = tmp_path / "licence.docx"
        d = docx.Document()
        for line in _lines():
            d.add_paragraph(line)
        d.save(str(f))

    [row] = extract_file(str(f), _config())
    assert row["licence_number"] == "RF-482913"
    assert row["expiry_date"] == "2027-03-31"
    assert row["_lakelogic_llm_provider"] == "document"
    assert row["_lakelogic_llm_cost_usd"] == 0.0


def test_extraction_patterns_work_as_well_as_examples(tmp_path):
    f = tmp_path / "licence.png"
    _image(f)
    plain = [{"name": "licence_number", "type": "string"}, {"name": "expiry_date", "type": "string"}]
    [row] = extract_file(str(f), ExtractionConfig(provider="document", output_schema=plain, patterns=PATTERNS))
    assert (row["licence_number"], row["expiry_date"]) == ("RF-482913", "2027-03-31")


def test_an_ocr_misread_label_is_still_found_and_the_value_is_untouched():
    """OCR read "cost" as "c0st" and "Total" as "Tota1" on a clean image (2026-10-23)."""
    from lakelogic.engines.llm import _ocr_tolerant
    import re

    tolerant = _ocr_tolerant(r"Estimated cost:\s*([\d.]+)")
    assert re.search(tolerant, "Estimated c0st: 4080.83").group(1) == "4080.83"
    assert re.search(_ocr_tolerant(r"Total:\s*([\d.]+)"), "Tota1: 82.15").group(1) == "82.15"
    # The value is not loosened: a misread digit in the VALUE is not "corrected".
    assert re.search(_ocr_tolerant(r"Total:\s*(\d+\.\d\d)"), "Total: 8O.15") is None
    assert _ocr_tolerant(r"(\d+)") is None  # no label to loosen


def test_a_pattern_that_does_not_match_is_none_and_a_bad_one_fails(tmp_path):
    f = tmp_path / "licence.png"
    _image(f)
    [row] = extract_file(str(f), ExtractionConfig(
        provider="document", output_schema=[{"name": "policy", "type": "string"}],
        patterns={"policy": r"Policy:\s*(\S+)"}))
    assert row["policy"] is None
    with pytest.raises(ValueError, match="not a valid regex"):
        extract_file(str(f), ExtractionConfig(
            provider="document", output_schema=[{"name": "x", "type": "string"}], patterns={"x": "("}))


@pytest.mark.parametrize("fmt, path, ext", [
    ("docx", "landing/incidents/*.docx", ".docx"),
    ("image", "landing/receipts/*.png", ".png"),
    ("pdf", "landing/invoices/*.pdf", ".pdf"),
])
def test_synthetic_documents_for_a_document_contract_read_back(tmp_path, fmt, path, ext):
    """The Test stage's fake documents: written in the contract's own format and read back by
    the provider the deployed pipeline will use."""
    contract = {
        "dataset": "licences",
        "info": {"title": "Driving licences"},
        "source": {"type": "landing", "path": path, "format": fmt},
        "extraction": {"provider": "document"},
        "model": {"fields": [
            {"name": "licence_number", "type": "string", "extraction_task": "metadata",
             "extraction_examples": [r"Licence No:\s*(\S+)"]},
        ]},
    }
    assert supports_documents(contract)
    files = generate_documents(contract, tmp_path, rows=2, seed=3)
    assert [f.suffix for f in files] == [ext, ext]
    for f in files:
        [row] = extract_file(str(f), _config())
        assert row["licence_number"], f"{f.name}: nothing read back"


def test_pdfplumber_still_reads_pdf_only():
    contract = {"source": {"format": "docx"}, "extraction": {"provider": "pdfplumber"}}
    assert not supports_documents(contract)
