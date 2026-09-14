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
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml

#: Extraction providers whose fields are read by a regex over the document text.
SUPPORTED_PROVIDERS = {"pdfplumber"}

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
    return (
        isinstance(extraction, dict)
        and str(extraction.get("provider") or "").lower() in SUPPORTED_PROVIDERS
        and str(source.get("format") or "").lower() == "pdf"
    )


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
        return value.isoformat()[:10] if ftype == "date" or isinstance(value, date) and not isinstance(value, datetime) else value.isoformat()
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
            "generate_documents supports PDF sources extracted with "
            f"{sorted(SUPPORTED_PROVIDERS)}; this contract is not one "
            f"(extraction.provider={((doc.get('extraction') or {}).get('provider'))!r}, "
            f"source.format={((doc.get('source') or {}).get('format'))!r})"
        )
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError("reportlab is required to write synthetic documents: pip install lakelogic[synthetic]") from exc

    from lakelogic.core.generator import DataGenerator

    # The fields a document carries: those extracted by a pattern. Values are generated from the
    # contract's own names and types, with the extraction block removed (the generator refuses it).
    fields = [
        f for f in (doc.get("model") or {}).get("fields") or []
        if isinstance(f, dict) and f.get("name") and (f.get("extraction_examples") or [])
        and str(f.get("extraction_task") or "").lower() == "metadata"
    ]
    if not fields:
        raise ValueError("the contract declares no `extraction_task: metadata` field with an extraction_examples pattern")
    value_contract = {
        "version": doc.get("version", "1.0.0"),
        "info": {"title": ((doc.get("info") or {}).get("title") or doc.get("dataset") or "documents")},
        "model": {"fields": [
            {k: v for k, v in f.items() if k not in ("extraction_task", "extraction_examples")}
            for f in fields
        ]},
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
            line = _render_line(str(field["extraction_examples"][0]), value)
            if line is not None:
                lines.append(line)
        path = out / f"{prefix}_{seed}_{index:04d}.pdf"
        pdf = canvas.Canvas(str(path), pagesize=letter)
        pdf.setFont("Helvetica-Bold", 14)
        pdf.drawString(72, 740, f"{title} (Synthetic - Test Data)")
        pdf.setFont("Helvetica", 12)
        y = 710
        for line in lines:
            pdf.drawString(72, y, line)
            y -= 20
        pdf.save()
        written.append(path)
    return written


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


__all__ = ["SUPPORTED_PROVIDERS", "extract_metadata", "generate_documents", "line_label", "supports_documents"]
