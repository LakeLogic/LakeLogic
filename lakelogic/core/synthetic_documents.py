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

#: Stamped into every document this writes (PDF keywords, Word keywords, PNG text chunk), so the
#: rows extracted from it carry ``_synthetic=true`` like a seeded row does. The visible title says
#: "Synthetic - Test Data"; this is the part a reader can check.
SYNTHETIC_DOCUMENT_MARK = "lakelogic:synthetic"

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


def _regex_parser():
    try:  # Python 3.11+
        from re import _constants as c  # type: ignore[attr-defined]
        from re import _parser as p
    except ImportError:  # pragma: no cover - older Pythons
        import sre_constants as c  # type: ignore[no-redef]
        import sre_parse as p  # type: ignore[no-redef]
    return p, c


_CATEGORY_CHARS = {
    "CATEGORY_DIGIT": "0123456789",
    "CATEGORY_WORD": "ABCDEFGHJKLMNPRSTUVWXYZabcdefghkmnprstuvwxyz0123456789",
    "CATEGORY_SPACE": " ",
}


def _emit(tree: Any, rng: random.Random, c: Any) -> str:
    """One string the parsed regex ``tree`` matches. Assertions and anchors emit nothing."""
    out: List[str] = []
    for op, arg in tree:
        name = str(op)
        if name == "LITERAL":
            out.append(chr(arg))
        elif name == "NOT_LITERAL":
            out.append("X" if chr(arg) != "X" else "Y")
        elif name == "ANY":
            out.append(rng.choice("ABCDEFGHJKLMNPRSTUVWXYZ"))
        elif name == "IN":
            out.append(_pick_class(arg, rng))
        elif name in ("MAX_REPEAT", "MIN_REPEAT", "POSSESSIVE_REPEAT") and _is_space_class(arg[2]):
            # `\s*` / `\s+` is ONE space, as a person types it: random 0-3 wrote "LicenceNo:" and
            # ran a date into its time ("51/22/807652:49"), so neither read as written.
            out.append(" " * max(1, min(arg[0], 1)) if arg[1] != 0 else "")
        elif name in ("MAX_REPEAT", "MIN_REPEAT", "POSSESSIVE_REPEAT"):
            low, high, sub = arg
            high = low + 3 if high == c.MAXREPEAT else high
            out.extend(_emit(sub, rng, c) for _ in range(rng.randint(low, max(low, min(high, low + 3)))))
        elif name == "SUBPATTERN":
            out.append(_emit(arg[-1], rng, c))
        elif name == "BRANCH":
            out.append(_emit(rng.choice(arg[1]), rng, c))
        elif name == "CATEGORY":
            out.append(rng.choice(_CATEGORY_CHARS.get(str(arg), "A")))
        # AT (anchors), ASSERT / ASSERT_NOT (lookarounds), GROUPREF: nothing to emit.
    return "".join(out)


def _is_space_class(sub: Any) -> bool:
    """True for a repeated item that is only whitespace: ``\\s``, ``[ \\t]``, a literal space."""
    if len(sub) != 1:
        return False
    op, arg = sub[0]
    name = str(op)
    if name == "CATEGORY":
        return str(arg) == "CATEGORY_SPACE"
    if name == "LITERAL":
        return chr(arg) in " \t"
    if name == "IN":
        return all((str(o) == "CATEGORY" and str(a) == "CATEGORY_SPACE")
                   or (str(o) == "LITERAL" and chr(a) in " \t") for o, a in arg)
    return False


def _pick_class(items: Any, rng: random.Random) -> str:
    """A character from a ``[...]`` class (a negated class gets a letter it does not exclude)."""
    chars: List[str] = []
    negate = False
    for op, arg in items:
        name = str(op)
        if name == "NEGATE":
            negate = True
        elif name == "LITERAL":
            chars.append(chr(arg))
        elif name == "RANGE":
            chars.extend(chr(i) for i in range(arg[0], arg[1] + 1))
        elif name == "CATEGORY":
            chars.extend(_CATEGORY_CHARS.get(str(arg), ""))
    if negate:
        return next((ch for ch in "XQZK9" if ch not in chars), "X")
    return rng.choice(chars) if chars else "X"


def value_from_pattern(pattern: str, rng: random.Random) -> Optional[str]:
    """A value the pattern's FIRST capture group matches — for a pattern that describes a
    document's layout (``([A-Z]{3}\\d{6}[A-Z])``) rather than a ``Label: (value)`` line, where a
    value generated from the field's type would not match and the line was left out."""
    p, c = _regex_parser()
    try:
        tree = p.parse(pattern)
    except Exception:
        return None

    def first_group(node: Any) -> Any:
        for op, arg in node:
            name = str(op)
            if name == "SUBPATTERN" and arg[0] is not None:
                return arg[-1]
            for child in _children(name, arg):
                found = first_group(child)
                if found is not None:
                    return found
        return None

    group = first_group(tree)
    if group is None:
        return None
    value = _emit(group, rng, c).strip()
    return value or None


def text_from_pattern(pattern: str, rng: random.Random, value: Optional[str] = None) -> Optional[str]:
    r"""Text the WHOLE pattern matches — its labels, line breaks and context included — with the
    first capture group holding ``value`` when that matches, else a value made from the group.

    For a pattern whose value depends on what surrounds it: ``Receipt:\s*[A-Z]\d{6}\s*\n([^\n]+)``
    (the service is the line after the receipt number) or ``\n([A-Z]+)\n[A-Z]{3} [a-z]+`` (the
    merchant is a line of its own above the address). ``None`` when no attempt matches."""
    p, c = _regex_parser()
    try:
        tree = p.parse(pattern)
    except Exception:
        return None

    def emit(node: Any, fill: Optional[str], filled: List[bool]) -> str:
        out: List[str] = []
        for op, arg in node:
            name = str(op)
            if name == "SUBPATTERN" and arg[0] is not None and not filled[0]:
                filled[0] = True
                out.append(fill if fill is not None else _emit(arg[-1], rng, c))
            elif name == "SUBPATTERN":
                out.append(emit(arg[-1], fill, filled))
            elif name in ("MAX_REPEAT", "MIN_REPEAT", "POSSESSIVE_REPEAT") and _has_group(arg[2]):
                out.append(emit(arg[2], fill, filled))  # a repeated group: once, holding the value
            elif name == "BRANCH" and any(_has_group(b) for b in arg[1]):
                out.append(emit(next(b for b in arg[1] if _has_group(b)), fill, filled))
            else:
                out.append(_emit([(op, arg)], rng, c))
        return "".join(out)

    # The typed value first (a real date stays a real date), then values made from the pattern.
    for fill in ([value] if value else []) + [None, None, None]:
        text = emit(tree, fill, [False])
        found = re.search(pattern, text, READER_FLAGS)
        if found and found.groups() and found.group(1) and (fill is None or found.group(1) == fill):
            return text
    return None


def _has_group(node: Any) -> bool:
    for op, arg in node:
        name = str(op)
        if name == "SUBPATTERN" and arg[0] is not None:
            return True
        if any(_has_group(ch) for ch in _children(name, arg)):
            return True
    return False


def _children(name: str, arg: Any) -> List[Any]:
    if name == "SUBPATTERN":
        return [arg[-1]]
    if name in ("MAX_REPEAT", "MIN_REPEAT", "POSSESSIVE_REPEAT"):
        return [arg[2]]
    if name == "BRANCH":
        return list(arg[1])
    if name in ("ASSERT", "ASSERT_NOT"):
        return []  # a lookaround's groups are not the value
    return []


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
        doc_title = f"{title} (Synthetic - Test Data)"
        # Each field: (name, pattern, the value its pattern must extract, the text that carries
        # it, whether it can take a label). Built in contract order, against the text so far.
        parts: List[Dict[str, Any]] = []
        so_far = doc_title
        dates: Dict[str, Any] = {}  # this document's dates, kept in order
        for field in fields:
            if field["name"] == omitted:
                continue
            value = _as_text(record.get(field["name"]), str(field.get("type") or "string").lower())
            if value is None:
                continue
            pattern = patterns[field["name"]]
            line = _render_line(pattern, value)
            labelled = line is not None and line_label(pattern)
            if line is None:
                # A LAYOUT pattern (no "Label:" before the value): the type's value does not fit
                # it, and the line was silently left out — so the extraction found nothing and a
                # correct contract read as broken (RideFlow licences, 2026-11-22). The value comes
                # from the pattern itself, with the pattern's own labels and line breaks around it.
                line = _layout_text(pattern, value, so_far, rng, dates)
            if line is None:
                continue
            found = re.search(pattern, so_far + "\n" + line, READER_FLAGS)
            expected = found.group(1) if found and found.groups() else None
            # No second label where the pattern already writes one ("Date:", "Receipt:"):
            # "Receipt date: Date: 26/11/1969" says it twice.
            # Any words the pattern writes BEFORE the value are its label, colon or not
            # ("Incident ID MSI-…", "Location BOA …").
            before = line.split(expected, 1)[0] if expected and expected in line else ""
            has_label = bool(labelled) or bool(re.search(r"[A-Za-z]{2,}", before))
            parts.append({"name": field["name"], "pattern": pattern, "expected": expected,
                          "text": line, "label": None if has_label else _field_label(field["name"])})
            so_far += "\n" + line
        lines = _labelled_lines(doc_title, parts)
        stem = out / f"{prefix}_{seed}_{index:04d}"
        written.append(_write_image(stem, doc_title, lines, _image_ext(doc)) if fmt == "image"
                       else _WRITERS[fmt](stem, doc_title, lines))
    return written


# ── a document a person can read (2026-11-22) ──────────────────────────────────────────────────
#
# A sample is downloaded and LOOKED AT. Bare values ("ODN526808E", "SLN") say nothing about which
# field they are, and digits made from `\d{2}/\d{2}/\d{4}` ("50/67/5605") read as broken. So each
# value is labelled with its field and every date-shaped value is a real date — and the WHOLE
# document is checked: every field must still extract exactly its own value, or the label goes.

#: THE READER'S FLAGS. The document reader matches each pattern with `re.MULTILINE`
#: (engines/llm.py), so `^` / `$` hold at every line: a check without it passed a label that broke
#: `^([A-Z]{3})$` for the reader (Onboarding Driving Licences, 2026-11-22).
READER_FLAGS = re.MULTILINE

_DATE_SHAPES = [
    (re.compile(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)"), "%Y-%m-%d"),
    (re.compile(r"(?<!\d)(\d{2})/(\d{2})/(\d{4})(?!\d)"), "%d/%m/%Y"),
    (re.compile(r"(?<!\d)(\d{2})-(\d{2})-(\d{4})(?!\d)"), "%d-%m-%Y"),
    (re.compile(r"(?<!\d)(\d{2})\.(\d{2})\.(\d{4})(?!\d)"), "%d.%m.%Y"),
]
_TIME_SHAPE = re.compile(r"(?<![\d:])(\d{2}):(\d{2})(?![\d:])")
_MONTH_SHAPE = re.compile(r"(?<!\d)(\d{1,2})(\s*)([A-Z][a-z]{2,8})(\s*)(\d{4})(?!\d)")


def _with_spacing(day_month_year: str, gap1: str, gap2: str) -> str:
    """``"06 June 2021"`` laid out with the original gaps (``"06June2021"`` stays unspaced)."""
    day, month, year = day_month_year.split(" ")
    return f"{day}{gap1}{month}{gap2}{year}"


def _real_dates(text: str, rng: random.Random, state: Optional[Dict[str, Any]] = None) -> str:
    """Every date- and time-shaped run of digits in ``text`` made a real date / time.

    ``state`` (one per document) keeps the dates IN ORDER: each date falls after the last one, so
    a licence's expiry is never before its holder's birth."""
    from datetime import date as _date
    from datetime import timedelta

    state = state if state is not None else {}

    def next_date(_m: Any, fmt: str) -> str:
        last = state.get("last")
        if last is None:
            day = _date(1950, 1, 1) + timedelta(days=rng.randint(0, 20000))
        else:
            day = min(last + timedelta(days=rng.randint(365, 7300)), _date(2035, 12, 31))
        state["last"] = day
        return day.strftime(fmt)

    for shape, fmt in _DATE_SHAPES:
        text = shape.sub(lambda m, f=fmt: next_date(m, f), text)
    # "6 June 2021": a day, a month NAME and a year, with the spacing the text already had.
    text = _MONTH_SHAPE.sub(
        lambda m: _with_spacing(next_date(m, "%d %B %Y"), m.group(2), m.group(4)), text)
    return _TIME_SHAPE.sub(lambda _m: f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}", text)


def _layout_text(pattern: str, value: str, so_far: str, rng: random.Random,
                 dates: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """The text a layout pattern needs for one field, written into the document so far.

    The bare value first, when the document already holds the context the pattern wants before
    it (an expiry date read as "the date after the first one" needs no invented first date);
    then the whole pattern's text. Date shapes become real dates when the result still matches."""
    candidates: List[str] = []
    made = value_from_pattern(pattern, rng)
    for bare in (value, made):
        if bare:
            candidates.append(bare)
    full = text_from_pattern(pattern, rng, value)
    if full:
        candidates.append(full)
    for text in candidates:
        # A trial copy of the date order: only the version that is kept moves it on.
        for trial in ({**(dates or {})}, None):
            attempt = _real_dates(text, rng, trial) if trial is not None else text
            found = re.search(pattern, so_far + "\n" + attempt, READER_FLAGS)
            if found and found.groups() and found.group(1) and found.group(1) in attempt:
                if trial is not None and dates is not None:
                    dates.update(trial)
                return attempt
    # Nothing matched yet: the pattern wants context still to come ("a date with another after
    # it" — the expiry is written next). A real date has the same digit shape, so it still fits.
    fallback = full or made
    return _real_dates(fallback, rng, dates) if fallback else None


def _field_label(name: str) -> str:
    """``licence_no`` -> ``Licence no:`` — the field, as a person reads it."""
    words = re.sub(r"[_\s]+", " ", str(name)).strip()
    return (words[:1].upper() + words[1:] + ":") if words else ""


def _extracts_all(text: str, parts: List[Dict[str, Any]]) -> bool:
    """Every field's pattern still finds exactly the value it was written to carry."""
    for part in parts:
        if part["expected"] is None:
            continue
        found = re.search(part["pattern"], text, READER_FLAGS)
        if not (found and found.groups() and found.group(1) == part["expected"]):
            return False
    return True


def _labelled_lines(title: str, parts: List[Dict[str, Any]]) -> List[str]:
    """The document's lines, each value labelled with its field where that changes nothing.

    A label goes on the value's own line ("Licence no: ODN526808E") or, when that breaks the
    pattern (``^([A-Z]{3})$`` wants the code alone), on the line above. Each is kept only if the
    whole document still extracts every field exactly — a label such as "Holder name" must never
    become the holder's name."""
    def render(choice: Dict[str, str]) -> List[str]:
        out: List[str] = []
        for part in parts:
            body = [ln for ln in part["text"].split("\n") if ln.strip()]
            how, label = choice.get(part["name"]), part["label"]
            if how == "inline" and label and body:
                body[0] = f"{label} {body[0]}"
            elif how == "above" and label:
                body = [label, *body]
            out.extend(body)
        return out

    def ok(choice: Dict[str, str]) -> bool:
        return _extracts_all("\n".join([title, *render(choice)]), parts)

    choice: Dict[str, str] = {}
    for part in parts:
        if not part["label"]:
            continue
        for how in ("inline", "above"):
            if ok({**choice, part["name"]: how}):
                choice[part["name"]] = how
                break
    return render(choice)


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
    pdf.setKeywords(SYNTHETIC_DOCUMENT_MARK)
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
    d.core_properties.keywords = SYNTHETIC_DOCUMENT_MARK
    d.add_heading(title, level=1)
    for line in lines:
        d.add_paragraph(line)
    d.save(str(path))
    return path


def _image_ext(doc: Dict[str, Any]) -> str:
    """The image extension the contract's source path reads (``receipts/*.jpg`` -> ``jpg``); PNG
    when the path names none. Seeding always wrote PNG, so a contract reading ``*.jpg`` matched
    none of its own test documents (2026-11-18)."""
    ext = str((doc.get("source") or {}).get("path") or "").rsplit(".", 1)[-1].lower()
    return ext if ext in _IMAGE_EXTS else "png"


def _write_image(stem: Path, title: str, lines: List[str], ext: str = "png") -> Path:
    """A clean, large-type image that OCR reads back: a stand-in for a photographed or scanned
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
    path = stem.with_suffix(f".{ext}")
    if ext == "png":
        from PIL.PngImagePlugin import PngInfo

        info = PngInfo()
        info.add_text("Keywords", SYNTHETIC_DOCUMENT_MARK)
        img.save(path, pnginfo=info)
    else:
        # Other formats carry the mark in EXIF ImageDescription (0x010E), which JPEG, TIFF and WebP keep.
        exif = Image.Exif()
        exif[0x010E] = SYNTHETIC_DOCUMENT_MARK
        img.save(path, exif=exif)
    return path


_WRITERS = {"pdf": _write_pdf, "docx": _write_docx, "image": _write_image}


def is_synthetic_document(path: Union[str, Path]) -> bool:
    """True when ``path`` is a document :func:`generate_documents` wrote (it carries the mark).

    Reads only the document's properties, never its text: a real document that happens to
    mention the mark in its body is not synthetic. Any read failure means "not synthetic"."""
    p = Path(path)
    suffix = p.suffix.lower()
    try:
        if suffix == ".pdf":
            # reportlab writes the info dictionary uncompressed: `/Keywords (lakelogic:synthetic)`.
            return f"/Keywords ({SYNTHETIC_DOCUMENT_MARK})".encode() in p.read_bytes()
        if suffix == ".docx":
            import zipfile

            with zipfile.ZipFile(p) as z:
                return SYNTHETIC_DOCUMENT_MARK in z.read("docProps/core.xml").decode("utf-8", "ignore")
        if suffix.lstrip(".") in _IMAGE_EXTS:
            from PIL import Image

            with Image.open(p) as img:
                if img.info.get("Keywords") == SYNTHETIC_DOCUMENT_MARK:
                    return True
                return img.getexif().get(0x010E) == SYNTHETIC_DOCUMENT_MARK
    except Exception:
        return False
    return False


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


__all__ = [
    "PROVIDER_FORMATS",
    "SUPPORTED_PROVIDERS",
    "SYNTHETIC_DOCUMENT_MARK",
    "extract_metadata",
    "generate_documents",
    "is_synthetic_document",
    "line_label",
    "supports_documents",
]
