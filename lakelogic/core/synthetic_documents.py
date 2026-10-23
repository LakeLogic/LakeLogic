"""
Synthetic documents for document-extraction contracts.

A contract that reads PDFs and extracts fields from their text (``extraction.provider:
pdfplumber`` with per-field ``extraction_examples`` regexes) cannot be seeded with rows: its
source is files. ``DataGenerator`` refuses such contracts, so a synthetic estate had no documents
for them and the bronze run failed with ``0 pdf files matched glob`` (Fabric, 2026-09-14).

This module writes the documents instead, derived entirely from the contract:

  - field VALUES come from ``DataGenerator`` over the contract's own fields and types;
  - each field becomes one line built from its OWN extraction regex — ``Name:\\s+(.+)`` renders
    as ``Name: <value>`` — and every line is checked against that regex before it is written,
    so the real extraction reads back exactly what was generated;
  - ``invalid_ratio`` leaves a required / ``not_null`` field's line out of some documents, so
    the extracted row is null there and the contract's quality rules quarantine it.

Only local, rule-based extraction is supported (``pdfplumber``). A contract that extracts with an
LLM is not: its fields have no pattern to render against.

Usage::

    from lakelogic.core.synthetic_documents import generate_documents

    paths = generate_documents("bronze_driver_licences.yaml", "landing/driver_licences", rows=30)

Requires ``reportlab`` (``pip install lakelogic[synthetic]``).
"""

from __future__ import annotations

import random
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

#: Extraction providers whose fields are read by a regex over the document text.
SUPPORTED_PROVIDERS = {"pdfplumber", "document"}
#: The formats each provider reads, and so the files this writes for it. ``document`` reads any
#: document through ``extract_text``: a Word file, an image (OCR) or a PDF (2026-10-23).
PROVIDER_FORMATS = {"pdfplumber": {"pdf"}, "document": {"pdf", "docx", "image"}}
_IMAGE_EXTS = {"png", "jpg", "jpeg", "tif", "tiff", "bmp", "webp"}

_CAPTURE_GROUP = re.compile(r"(?<!\\)\((?!\?)")


def _load(contract: Union[str, Path, Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(contract, dict):
        return contract
    text = Path(contract).read_text(encoding="utf-8") if Path(str(contract)).is_file() else str(contract)
    return yaml.safe_load(text) or {}


def supports_documents(contract: Union[str, Path, Dict[str, Any]]) -> bool:
    """True when the contract reads PDFs and extracts its fields with a supported local provider."""
    doc = _load(contract)
    extraction = doc.get("extraction") or {}
    source = doc.get("source") or {}
    if not isinstance(extraction, dict) or not isinstance(source, dict):
        return False
    provider = str(extraction.get("provider") or "").lower()
    return provider in SUPPORTED_PROVIDERS and _format_of(doc) in PROVIDER_FORMATS[provider]


def _format_of(doc: Dict[str, Any]) -> str:
    """``pdf`` / ``docx`` / ``image`` — from ``source.format``, else the path's extension."""
    source = doc.get("source") or {}
    fmt = str(source.get("format") or "").lower()
    if not fmt:
        fmt = str(source.get("path") or "").rsplit(".", 1)[-1].lower()
    return "image" if fmt in _IMAGE_EXTS else fmt


def _patterns(doc: Dict[str, Any]) -> Dict[str, str]:
    """Each field's pattern: ``extraction.patterns``, else a metadata field's first example."""
    out = dict(((doc.get("extraction") or {}).get("patterns")) or {})
    for f in (doc.get("model") or {}).get("fields") or []:
        if isinstance(f, dict) and f.get("name") and f["name"] not in out:
            examples = f.get("extraction_examples") or []
            if str(f.get("extraction_task") or "").lower() == "metadata" and examples:
                out[f["name"]] = str(examples[0])
    return out


def line_label(pattern: str) -> Optional[str]:
    """The literal text a line starts with for a metadata regex: ``Name:\\s+(.+)`` -> ``Name:``.

    Everything before the first capture group, with whitespace classes read as a space and
    escapes removed. ``None`` when the pattern has no capture group to put a value in.
    """
    match = _CAPTURE_GROUP.search(pattern or "")
    if not match:
        return None
    prefix = pattern[: match.start()].lstrip("^")
    prefix = re.sub(r"\\s(?:[+*?]|\{\d*,?\d*\})?", " ", prefix)
    prefix = re.sub(r"\\(.)", r"\1", prefix)
    return prefix.rstrip()


def _as_text(value: Any, ftype: str) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return (
            value.isoformat()[:10]
            if ftype == "date" or isinstance(value, date) and not isinstance(value, datetime)
            else value.isoformat()
        )
    text = str(value)
    if ftype == "date" and len(text) >= 10:
        return text[:10]
    return text


def _render_line(pattern: str, value: str) -> Optional[str]:
    """A line that ``pattern`` matches with group 1 == ``value``, or None if none can be built."""
    label = line_label(pattern)
    if label is None:
        return None
    for candidate in (value, re.sub(r"\s+", "-", value)):
        line = f"{label} {candidate}" if label else candidate
        found = re.search(pattern, line)
        if found and found.group(1) == candidate:
            return line
    return None


def _required_fields(doc: Dict[str, Any], names: List[str]) -> List[str]:
    required = {f.get("name") for f in (doc.get("model") or {}).get("fields") or [] if f.get("required")}
    for rule in (doc.get("quality") or {}).get("row_rules") or []:
        if isinstance(rule, dict) and isinstance(rule.get("not_null"), str):
            required.add(rule["not_null"])
    return [n for n in names if n in required]


def generate_documents(
    contract: Union[str, Path, Dict[str, Any]],
    output_dir: Union[str, Path],
    *,
    rows: int = 30,
    seed: int = 7,
    invalid_ratio: float = 0.0,
    file_prefix: Optional[str] = None,
) -> List[Path]:
    """Write ``rows`` synthetic PDFs for a document-extraction contract. Returns the paths.

    Raises ``ValueError`` for a contract this cannot serve (not a PDF source, or an extraction
    provider other than a local regex one) and ``ImportError`` when ``reportlab`` is missing.
    """
    doc = _load(contract)
    if not supports_documents(doc):
        raise ValueError(
            "generate_documents supports document sources extracted with "
            f"{sorted(SUPPORTED_PROVIDERS)} (pdfplumber: PDF; document: PDF, Word, image); "
            "this contract is not one "
            f"(extraction.provider={((doc.get('extraction') or {}).get('provider'))!r}, "
            f"source.format={((doc.get('source') or {}).get('format'))!r})"
        )
    fmt = _format_of(doc)
    from lakelogic.core.generator import DataGenerator

    # The fields a document carries: those extracted by a pattern. Values are generated from the
    # contract's own names and types, with the extraction block removed (the generator refuses it).
    patterns = _patterns(doc)
    fields = [
        f
        for f in (doc.get("model") or {}).get("fields") or []
        if isinstance(f, dict) and f.get("name") and f["name"] in patterns
    ]
    if not fields:
        raise ValueError(
            "the contract declares no field with an extraction pattern "
            "(`extraction.patterns`, or `extraction_task: metadata` with extraction_examples)"
        )
    value_contract = {
        "version": doc.get("version", "1.0.0"),
        "info": {"title": ((doc.get("info") or {}).get("title") or doc.get("dataset") or "documents")},
        "model": {
            "fields": [
                # Every document carries every line (required): a missing value is made ONLY by
                # leaving a line out on purpose below, never by the generator's null share.
                {
                    **{k: v for k, v in f.items() if k not in ("extraction_task", "extraction_examples")},
                    "required": True,
                }
                for f in fields
            ]
        },
    }
    frame = DataGenerator(yaml.safe_dump(value_contract), seed=seed).generate(rows=max(1, int(rows)), invalid_ratio=0.0)
    records = frame.to_dicts()

    rng = random.Random(f"{seed}:documents")
    names = [f["name"] for f in fields]
    droppable = _required_fields(doc, names)
    n_invalid = int(len(records) * max(0.0, float(invalid_ratio))) if droppable else 0
    invalid_rows = set(rng.sample(range(len(records)), n_invalid)) if n_invalid else set()

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    title = str((doc.get("info") or {}).get("title") or doc.get("dataset") or "Document")
    prefix = file_prefix or re.sub(r"[^A-Za-z0-9]+", "_", str(doc.get("dataset") or "document")).strip("_").lower()
    written: List[Path] = []
    for index, record in enumerate(records):
        omitted = rng.choice(droppable) if index in invalid_rows else None
        lines: List[str] = []
        for field in fields:
            if field["name"] == omitted:
                continue
            value = _as_text(record.get(field["name"]), str(field.get("type") or "string").lower())
            if value is None:
                continue
            line = _render_line(patterns[field["name"]], value)
            if line is not None:
                lines.append(line)
        stem = out / f"{prefix}_{seed}_{index:04d}"
        written.append(_WRITERS[fmt](stem, f"{title} (Synthetic - Test Data)", lines))
    return written


def _write_pdf(stem: Path, title: str, lines: List[str]) -> Path:
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "reportlab is required to write synthetic documents: pip install lakelogic[synthetic]"
        ) from exc
    path = stem.with_suffix(".pdf")
    pdf = canvas.Canvas(str(path), pagesize=letter)
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(72, 740, title)
    pdf.setFont("Helvetica", 12)
    y = 710
    for line in lines:
        pdf.drawString(72, y, line)
        y -= 20
    pdf.save()
    return path


def _write_docx(stem: Path, title: str, lines: List[str]) -> Path:
    try:
        import docx
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError("python-docx is required for Word documents: pip install 'lakelogic[documents]'") from exc
    path = stem.with_suffix(".docx")
    d = docx.Document()
    d.add_heading(title, level=1)
    for line in lines:
        d.add_paragraph(line)
    d.save(str(path))
    return path


def _write_image(stem: Path, title: str, lines: List[str]) -> Path:
    """A clean, large-type PNG that OCR reads back: a stand-in for a photographed or scanned
    document, so the Test stage proves the contract — not OCR on a poor scan."""
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError("Pillow is required for image documents: pip install 'lakelogic[documents]'") from exc

    def _font(size: int):
        for name in ("arial.ttf", "DejaVuSans.ttf", "LiberationSans-Regular.ttf"):
            try:
                return ImageFont.truetype(name, size)
            except OSError:
                continue
        return ImageFont.load_default()

    rows = [title, *lines]
    img = Image.new("RGB", (1400, 90 * len(rows) + 80), "white")
    draw = ImageDraw.Draw(img)
    for i, text in enumerate(rows):
        draw.text((50, 40 + i * 90), text, fill="black", font=_font(44))
    path = stem.with_suffix(".png")
    img.save(path)
    return path


_WRITERS = {"pdf": _write_pdf, "docx": _write_docx, "image": _write_image}


def extract_metadata(pdf_path: Union[str, Path], contract: Union[str, Path, Dict[str, Any]]) -> Dict[str, Any]:
    """Read a document back the way the ``pdfplumber`` extraction does: text joined across pages,
    each metadata field's first ``extraction_examples`` pattern, group 1. For round-trip checks."""
    import pdfplumber

    doc = _load(contract)
    with pdfplumber.open(str(pdf_path)) as handle:
        text = "\n".join(page.extract_text() or "" for page in handle.pages)
    values: Dict[str, Any] = {}
    for field in (doc.get("model") or {}).get("fields") or []:
        examples = field.get("extraction_examples") or []
        if str(field.get("extraction_task") or "").lower() == "metadata" and examples:
            found = re.search(str(examples[0]), text)
            values[field["name"]] = found.group(1) if found else None
    return values


__all__ = ["PROVIDER_FORMATS", "SUPPORTED_PROVIDERS", "extract_metadata", "generate_documents", "line_label", "supports_documents"]
