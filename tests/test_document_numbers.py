"""A number printed in a document for people ("GBP 1,250.00") is read as its contract type reads it."""

import pytest

from lakelogic.engines.llm import _as_document_number


@pytest.mark.parametrize("value,typ,expected", [
    ("1,250.00", "decimal", "1250.00"),
    ("12,040", "integer", "12040"),
    ("1 250 000.5", "double", "1250000.5"),
    ("-3,000", "decimal(18,2)", "-3000"),
    ("830.50", "decimal", "830.50"),
    # Not plainly grouped digits: left as read, so the cast quarantines the real value.
    ("1.250,00", "decimal", "1.250,00"),
    ("12,5", "double", "12,5"),
    ("1,25,000", "integer", "1,25,000"),
    # Text fields keep what the document says.
    ("1,250.00", "string", "1,250.00"),
    (None, "decimal", None),
])
def test_grouped_numbers_are_read_as_numbers(value, typ, expected):
    assert _as_document_number(value, typ) == expected


def test_a_word_invoice_total_reads_as_a_decimal(tmp_path):
    docx = pytest.importorskip("docx")
    from lakelogic.core.models import ExtractionConfig, FieldDefinition
    from lakelogic.engines.llm import extract_file

    doc = docx.Document()
    doc.add_paragraph("Invoice number: INV-1001")
    doc.add_paragraph("Total due: GBP 1,250.00")
    path = tmp_path / "inv.docx"
    doc.save(path)
    config = ExtractionConfig(provider="document", output_schema=[
        FieldDefinition(name="total_due", type="decimal", extraction_task="metadata",
                        extraction_examples=[r"Total due:\s*GBP\s*([\d,.]+)"]),
    ])

    (row,) = extract_file(str(path), config)

    assert row["total_due"] == "1250.00"
