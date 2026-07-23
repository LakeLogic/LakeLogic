"""
lakelogic.engines.llm
----------------------
Engine adapter for unstructured data processing via LLM extraction.

Processes raw text, PDFs, images, audio, and video through LLM providers
(cloud or local), governed by the same YAML data contract used for
structured data.

Supported providers::

    provider: openai          # GPT-4o, GPT-4o-mini
    provider: anthropic       # Claude 3.5 Sonnet, Haiku
    provider: azure_openai    # Azure-hosted OpenAI
    provider: google          # Gemini 2.0 Flash, Gemini Pro
    provider: bedrock         # AWS Bedrock (Claude, Llama, Titan)
    provider: ollama          # Local models via Ollama
    provider: local           # Direct HuggingFace transformers
    provider: unstructured    # Document parsing (PDF, DOCX, HTML)
    provider: spacy           # NER + text classification (local, fast)
    provider: pdfplumber      # PDF table extraction (pure Python)
    provider: easyocr         # Image OCR via PyTorch

Usage — LLM with prompting::

    from lakelogic import DataContract, DataProcessor
    contract = DataContract.from_yaml("contracts/support_tickets.yaml")
    result = DataProcessor.run(contract, df)

Usage — PDF table extraction (pdfplumber)::

    from lakelogic.engines.llm import extract_file
    from lakelogic.core.models import ExtractionConfig
    import yaml

    config = ExtractionConfig(**yaml.safe_load('''
      provider: pdfplumber
      output_schema:
        - name: description
          type: string
        - name: hours
          type: integer
        - name: amount
          type: string
    '''))
    rows = extract_file("invoice.pdf", config)  # list of dicts

Usage — Image OCR (easyocr)::

    config = ExtractionConfig(**yaml.safe_load('''
      provider: easyocr
      output_schema:
        - name: full_text
          type: string
          extraction_task: all
    '''))
    rows = extract_file("receipt.png", config)

Usage — NER + classification (spacy)::

    result = extract_row(
        {"text": "Sarah Chen from Microsoft called about billing."},
        ExtractionConfig(**yaml.safe_load('''
          provider: spacy
          text_column: text
          output_schema:
            - name: persons
              type: string
              extraction_task: ner
            - name: organizations
              type: string
              extraction_task: ner
              extraction_examples: [ORG]
            - name: sentiment
              type: string
              extraction_task: sentiment
        ''')),
    )
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Dict, List, Optional

from loguru import logger

from lakelogic.core.models import DataContract, ExtractionConfig

# ── Provider Clients ──────────────────────────────────────────────────────────


def _init_openai(config: ExtractionConfig) -> Any:
    """Initialize OpenAI client."""
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError("openai is required for provider='openai'. Install with: pip install lakelogic[llm]")
    return OpenAI()


def _init_anthropic(config: ExtractionConfig) -> Any:
    """Initialize Anthropic client."""
    try:
        from anthropic import Anthropic
    except ImportError:
        raise ImportError("anthropic is required for provider='anthropic'. Install with: pip install lakelogic[llm]")
    return Anthropic()


def _init_google(config: ExtractionConfig) -> Any:
    """Initialize Google Generative AI client."""
    try:
        import google.generativeai as genai
    except ImportError:
        raise ImportError(
            "google-generativeai is required for provider='google'. Install with: pip install google-generativeai"
        )
    return genai


_PROVIDER_INIT = {
    "openai": _init_openai,
    "anthropic": _init_anthropic,
    "azure_openai": _init_openai,  # same SDK, different base_url
    "google": _init_google,
}


# ── Prompt Rendering ──────────────────────────────────────────────────────────


def _render_prompt(template: str, row: Dict[str, Any]) -> str:
    """
    Render a Jinja2 prompt template with row data.

    Parameters
    ----------
    template : str
        Jinja2 template string from ``extraction.prompt_template``.
    row : dict
        Row data (column_name → value).

    Returns
    -------
    str
        Rendered prompt.
    """
    try:
        from jinja2 import Template
    except ImportError:
        raise ImportError("jinja2 is required for prompt templates. Install with: pip install jinja2")
    return Template(template).render(**row)


def _prompt_hash(template: str) -> str:
    """SHA-256 hash of the prompt template for lineage reproducibility."""
    return hashlib.sha256(template.encode("utf-8")).hexdigest()[:16]


# ── LLM Extraction ───────────────────────────────────────────────────────────


def _extract_openai(
    client: Any,
    config: ExtractionConfig,
    prompt: str,
    system_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    """Call OpenAI API and return parsed JSON + usage metadata."""
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    kwargs: Dict[str, Any] = {
        "model": config.model,
        "messages": messages,
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
    }
    if config.response_format == "json":
        kwargs["response_format"] = {"type": "json_object"}

    start = time.perf_counter()
    response = client.chat.completions.create(**kwargs)
    elapsed_ms = int((time.perf_counter() - start) * 1000)

    choice = response.choices[0]
    usage = response.usage

    # Parse JSON response
    raw_text = choice.message.content or ""
    try:
        extracted = json.loads(raw_text)
    except json.JSONDecodeError:
        extracted = {"_raw_response": raw_text, "_parse_error": True}

    # Lineage metadata
    extracted["_lakelogic_llm_model"] = response.model
    extracted["_lakelogic_llm_provider"] = "openai"
    extracted["_lakelogic_llm_prompt_hash"] = _prompt_hash(config.prompt_template)
    extracted["_lakelogic_llm_token_input"] = usage.prompt_tokens if usage else 0
    extracted["_lakelogic_llm_token_output"] = usage.completion_tokens if usage else 0
    extracted["_lakelogic_llm_latency_ms"] = elapsed_ms

    # Estimate cost (approximate, based on public pricing)
    input_cost = (extracted["_lakelogic_llm_token_input"] / 1_000_000) * _get_input_price(config.model)
    output_cost = (extracted["_lakelogic_llm_token_output"] / 1_000_000) * _get_output_price(config.model)
    extracted["_lakelogic_llm_cost_usd"] = round(input_cost + output_cost, 6)

    return extracted


def _extract_anthropic(
    client: Any,
    config: ExtractionConfig,
    prompt: str,
    system_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    """Call Anthropic API and return parsed JSON + usage metadata."""
    kwargs: Dict[str, Any] = {
        "model": config.model,
        "max_tokens": config.max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system_prompt:
        kwargs["system"] = system_prompt

    start = time.perf_counter()
    response = client.messages.create(**kwargs)
    elapsed_ms = int((time.perf_counter() - start) * 1000)

    raw_text = response.content[0].text if response.content else ""
    try:
        extracted = json.loads(raw_text)
    except json.JSONDecodeError:
        extracted = {"_raw_response": raw_text, "_parse_error": True}

    extracted["_lakelogic_llm_model"] = response.model
    extracted["_lakelogic_llm_provider"] = "anthropic"
    extracted["_lakelogic_llm_prompt_hash"] = _prompt_hash(config.prompt_template)
    extracted["_lakelogic_llm_token_input"] = response.usage.input_tokens
    extracted["_lakelogic_llm_token_output"] = response.usage.output_tokens
    extracted["_lakelogic_llm_latency_ms"] = elapsed_ms
    extracted["_lakelogic_llm_cost_usd"] = 0.0  # TODO: pricing table

    return extracted


def _extract_local(
    config: ExtractionConfig,
    prompt: str,
    system_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Extract using a local HuggingFace model.

    If ``config.model == 'auto'``, uses the per-field ``extraction_task``
    routing.  Otherwise loads the specified model as a text-generation
    pipeline.
    """
    from lakelogic.engines.model_registry import load_model

    full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt

    start = time.perf_counter()
    pipe = load_model("local_llm", model_override=config.model if config.model != "auto" else None)
    result = pipe(full_prompt, max_new_tokens=config.max_tokens, return_full_text=False)
    elapsed_ms = int((time.perf_counter() - start) * 1000)

    raw_text = result[0]["generated_text"] if result else ""
    try:
        extracted = json.loads(raw_text)
    except json.JSONDecodeError:
        extracted = {"_raw_response": raw_text, "_parse_error": True}

    extracted["_lakelogic_llm_model"] = config.model
    extracted["_lakelogic_llm_provider"] = "local"
    extracted["_lakelogic_llm_prompt_hash"] = _prompt_hash(config.prompt_template)
    extracted["_lakelogic_llm_token_input"] = 0
    extracted["_lakelogic_llm_token_output"] = 0
    extracted["_lakelogic_llm_latency_ms"] = elapsed_ms
    extracted["_lakelogic_llm_cost_usd"] = 0.0

    return extracted


def _extract_unstructured(
    config: ExtractionConfig,
    prompt: str,
    system_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Extract structured elements from text or documents using ``unstructured``.

    Maps partitioned element categories (Title, NarrativeText, Table, etc.)
    to output_schema fields via the ``extraction_task`` hint on each field.

    Supported ``extraction_task`` values:
      - title       — Title elements
      - narrative   — NarrativeText / body paragraphs
      - table       — Table elements
      - list        — ListItem elements
      - header      — Header elements
      - all         — concatenation of all elements (default)
    """
    try:
        from unstructured.partition.auto import partition as partition_auto
        from unstructured.partition.text import partition_text
    except ImportError:
        raise ImportError(
            "unstructured is required for provider='unstructured'. Install with: pip install unstructured"
        )

    start = time.perf_counter()

    # Partition: if the prompt looks like a file path, use auto partition;
    # otherwise treat it as raw text.
    import os

    if os.path.isfile(prompt):
        elements = partition_auto(filename=prompt)
    else:
        elements = partition_text(text=prompt)

    # Map element categories to extraction_task names
    _CATEGORY_MAP: Dict[str, List[str]] = {
        "title": ["Title"],
        "narrative": ["NarrativeText"],
        "table": ["Table"],
        "list": ["ListItem"],
        "header": ["Header"],
        "image": ["Image"],
        "all": [],  # sentinel — matches everything
    }

    extracted: Dict[str, Any] = {}
    for field in config.output_schema:
        task = (field.extraction_task or "all").lower()
        target_cats = _CATEGORY_MAP.get(task)
        if target_cats is None:
            # Unknown task — try matching category name directly
            target_cats = [task.title()]
        if not target_cats:  # "all"
            matched = [el.text for el in elements if el.text]
        else:
            matched = [el.text for el in elements if el.category in target_cats and el.text]
        extracted[field.name] = "\n".join(matched) if matched else None

    elapsed_ms = int((time.perf_counter() - start) * 1000)

    # Lineage metadata
    extracted["_lakelogic_llm_model"] = "unstructured"
    extracted["_lakelogic_llm_provider"] = "unstructured"
    extracted["_lakelogic_llm_prompt_hash"] = _prompt_hash(config.prompt_template or "")
    extracted["_lakelogic_llm_token_input"] = 0
    extracted["_lakelogic_llm_token_output"] = 0
    extracted["_lakelogic_llm_latency_ms"] = elapsed_ms
    extracted["_lakelogic_llm_cost_usd"] = 0.0

    return extracted


def _extract_pdfplumber(
    file_path: str,
    config: ExtractionConfig,
) -> List[Dict[str, Any]]:
    """
    Extract table data from a PDF using ``pdfplumber``.

    Maps table columns to ``output_schema`` fields by name matching.
    Fields with ``extraction_task: metadata`` are extracted from page text
    using a regex pattern supplied in ``extraction_examples[0]``.

    Returns a list of dicts (one per table row).
    """
    try:
        import pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber is required for provider='pdfplumber'. Install with: pip install lakelogic[extraction-ocr]"
        )

    import re as _re

    start = time.perf_counter()

    with pdfplumber.open(file_path) as doc:
        all_text = "\n".join(page.extract_text() or "" for page in doc.pages)
        all_tables: list = []
        for page in doc.pages:
            for tbl in page.extract_tables() or []:
                if tbl:
                    all_tables.append(tbl)

    # Separate metadata fields (from text) vs table fields
    metadata_values: Dict[str, Any] = {}
    table_field_names: List[str] = []

    for field in config.output_schema:
        task = (field.extraction_task or "").lower()
        if task == "metadata":
            # Use first extraction_example as regex pattern
            pattern = field.extraction_examples[0] if field.extraction_examples else None
            if pattern:
                match = _re.search(pattern, all_text)
                metadata_values[field.name] = match.group(1) if match else None
            else:
                metadata_values[field.name] = None
        else:
            table_field_names.append(field.name)

    # Map table columns to schema fields by name similarity
    rows: List[Dict[str, Any]] = []
    for table in all_tables:
        if len(table) < 2:
            continue
        header_raw = table[0]
        header = [h.lower().replace(" ", "_") if h else "" for h in header_raw]

        # Build column mapping: schema_field_name → table_column_index
        col_map: Dict[str, int] = {}
        for fn in table_field_names:
            fn_lower = fn.lower()
            for idx, col_name in enumerate(header):
                if fn_lower == col_name or fn_lower in col_name or col_name in fn_lower:
                    col_map[fn] = idx
                    break

        # Skip pseudo-tables that don't actually map to any schema field —
        # pdfplumber's table detector over-fires on structured prose (e.g.
        # "Name: X / Licence No: Y / DOB: Z" PDFs that aren't really tables),
        # which previously emitted N duplicate metadata-only rows per PDF.
        # If no schema columns matched, treat this "table" as noise.
        if not col_map:
            continue

        for data_row in table[1:]:
            row: Dict[str, Any] = {}
            row.update(metadata_values)
            for fn, col_idx in col_map.items():
                row[fn] = data_row[col_idx] if col_idx < len(data_row) else None
            rows.append(row)

    elapsed_ms = int((time.perf_counter() - start) * 1000)

    # Lineage metadata on every row
    for row in rows:
        row["_lakelogic_llm_model"] = "pdfplumber"
        row["_lakelogic_llm_provider"] = "pdfplumber"
        row["_lakelogic_llm_latency_ms"] = elapsed_ms
        row["_lakelogic_llm_cost_usd"] = 0.0

    # Fallback: if no tables found, return full text as a single row
    if not rows:
        fallback: Dict[str, Any] = {}
        fallback.update(metadata_values)
        for fn in table_field_names:
            fallback[fn] = all_text if fn in ("full_text", "text", "body") else None
        fallback["_lakelogic_llm_model"] = "pdfplumber"
        fallback["_lakelogic_llm_provider"] = "pdfplumber"
        fallback["_lakelogic_llm_latency_ms"] = elapsed_ms
        fallback["_lakelogic_llm_cost_usd"] = 0.0
        rows.append(fallback)

    return rows


def _extract_easyocr(
    file_path: str,
    config: ExtractionConfig,
) -> List[Dict[str, Any]]:
    """
    Extract text from an image using ``easyocr`` (PyTorch).

    Returns a list of dicts (typically a single row with extracted text).
    """
    try:
        import easyocr
    except ImportError as e:
        if "easyocr" in str(e):
            raise ImportError(
                "easyocr is required for provider='easyocr'. Install with: pip install lakelogic[extraction-ocr]"
            ) from e
        raise e

    start = time.perf_counter()

    # Suppress verbose easyocr startup prints if possible
    import logging

    logging.getLogger("easyocr").setLevel(logging.ERROR)

    # Note: caching the reader in production is recommended, but for single-shot:
    reader = easyocr.Reader(["en"], gpu=False, verbose=False)
    ocr_results = reader.readtext(file_path)
    # easyocr returns list of tuples: (bbox, text, prob)
    ocr_text = "\n".join([r[1] for r in ocr_results]) if ocr_results else ""

    # Map to output_schema fields
    extracted: Dict[str, Any] = {}
    for field in config.output_schema:
        task = (field.extraction_task or "all").lower()
        if task == "all":
            extracted[field.name] = ocr_text
        elif task == "lines":
            extracted[field.name] = [r[1] for r in ocr_results] if ocr_results else []
        elif task == "confidence":
            extracted[field.name] = [round(float(r[2]), 3) for r in ocr_results] if ocr_results else []
        else:
            extracted[field.name] = ocr_text

    elapsed_ms = int((time.perf_counter() - start) * 1000)

    extracted["_lakelogic_llm_model"] = "easyocr"
    extracted["_lakelogic_llm_provider"] = "easyocr"
    extracted["_lakelogic_llm_latency_ms"] = elapsed_ms
    extracted["_lakelogic_llm_cost_usd"] = 0.0

    return [extracted]


def _extract_spacy(
    config: ExtractionConfig,
    prompt: str,
    system_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Extract named entities and classify text using spaCy.

    Dispatches per-field based on the ``extraction_task`` hint:

      - **ner** — extract entities matching the field name
        (e.g. field ``persons`` → PERSON entities,
        ``organizations`` → ORG entities).  Custom entity labels
        can be specified via ``extraction_examples: ["LABEL"]``.

      - **classification** — keyword-frequency classification into
        the field's ``accepted_values`` list.

      - **sentiment** — polarity via TextBlob (positive / neutral /
        negative).  Falls back to ``"neutral"`` if TextBlob is not
        installed.

    Parameters
    ----------
    config : ExtractionConfig
        Must have ``output_schema`` with per-field ``extraction_task``.
    prompt : str
        The input text to analyse.
    """
    try:
        import spacy
    except ImportError:
        raise ImportError(
            "spacy is required for provider='spacy'. "
            "Install with: pip install spacy && python -m spacy download en_core_web_sm"
        )

    start = time.perf_counter()

    model_name = config.model if config.model != "auto" else "en_core_web_sm"
    try:
        nlp = spacy.load(model_name)
    except OSError:
        # spaCy language models ship separately from the `spacy` package (they are
        # not pip dependencies of lakelogic[nlp]), so `spacy.load` raises OSError
        # [E050] the first time a model is used. Fetch it once, then retry — this
        # makes `provider: spacy` work out of the box in Colab/CI without a manual
        # `python -m spacy download` step.
        logger.info(
            f"spaCy model '{model_name}' not found — downloading it once "
            f"(this happens only on first use)…"
        )
        try:
            from spacy.cli import download as _spacy_download

            _spacy_download(model_name)
            nlp = spacy.load(model_name)
        except Exception as exc:  # pragma: no cover - network/model-name failures
            raise OSError(
                f"spaCy model '{model_name}' is not installed and could not be "
                f"downloaded automatically ({exc}). Install it manually with: "
                f"python -m spacy download {model_name}"
            ) from exc
    doc = nlp(prompt)

    # Map field names to spaCy entity labels
    _ENTITY_MAP: Dict[str, str] = {
        "person": "PERSON",
        "persons": "PERSON",
        "people": "PERSON",
        "organization": "ORG",
        "organizations": "ORG",
        "org": "ORG",
        "company": "ORG",
        "location": "GPE",
        "locations": "GPE",
        "place": "GPE",
        "city": "GPE",
        "date": "DATE",
        "dates": "DATE",
        "money": "MONEY",
        "amounts": "MONEY",
        "amount": "MONEY",
        "email": "EMAIL",
        "product": "PRODUCT",
        "products": "PRODUCT",
        "event": "EVENT",
        "events": "EVENT",
    }

    extracted: Dict[str, Any] = {}

    for field in config.output_schema:
        task = (field.extraction_task or "ner").lower()

        if task == "ner":
            # Determine which spaCy label to extract
            if field.extraction_examples:
                label = field.extraction_examples[0].upper()
            else:
                label = _ENTITY_MAP.get(field.name.lower(), field.name.upper())
            entities = list(
                dict.fromkeys(ent.text for ent in doc.ents if ent.label_ == label)
            )  # deduplicated, order-preserving
            extracted[field.name] = ", ".join(entities) if entities else None

        elif task == "classification":
            if field.accepted_values:
                text_lower = prompt.lower()
                scores = {cat: text_lower.count(cat.lower()) for cat in field.accepted_values}
                best = max(scores, key=scores.get) if any(scores.values()) else None
                extracted[field.name] = best
            else:
                extracted[field.name] = None

        elif task == "sentiment":
            # Keyword-boosted sentiment: TextBlob baseline + domain-aware keywords
            _NEG_KEYWORDS = {
                "error",
                "fail",
                "failed",
                "failure",
                "broken",
                "cracked",
                "bug",
                "crash",
                "block",
                "blocking",
                "wrong",
                "unhelpful",
                "terrible",
                "awful",
                "horrible",
                "charged",
                "cancelled",
                "canceled",
                "refund",
                "complaint",
                "urgent",
                "warning",
                "outage",
                "down",
                "fix",
                "unacceptable",
                "disappointed",
                "frustrat",
                "angry",
            }
            _POS_KEYWORDS = {
                "thank",
                "thanks",
                "helpful",
                "great",
                "excellent",
                "love",
                "awesome",
                "amazing",
                "perfect",
                "resolved",
                "happy",
                "pleased",
                "recommend",
                "fantastic",
                "wonderful",
                "impressed",
            }

            text_lower = prompt.lower()

            neg_hits = sum(1 for kw in _NEG_KEYWORDS if kw in text_lower)
            pos_hits = sum(1 for kw in _POS_KEYWORDS if kw in text_lower)

            try:
                from textblob import TextBlob  # type: ignore[import-untyped]

                polarity = TextBlob(prompt).sentiment.polarity
            except ImportError:
                polarity = 0.0

            # Boost polarity with keyword signals
            polarity += (pos_hits - neg_hits) * 0.3

            if polarity > 0.1:
                extracted[field.name] = "positive"
            elif polarity < -0.1:
                extracted[field.name] = "negative"
            else:
                extracted[field.name] = "neutral"

        else:
            logger.warning(f"Unknown extraction_task '{task}' for field '{field.name}'")
            extracted[field.name] = None

    elapsed_ms = int((time.perf_counter() - start) * 1000)

    # Lineage metadata
    extracted["_lakelogic_llm_model"] = model_name
    extracted["_lakelogic_llm_provider"] = "spacy"
    extracted["_lakelogic_llm_prompt_hash"] = _prompt_hash(config.prompt_template or "")
    extracted["_lakelogic_llm_token_input"] = 0
    extracted["_lakelogic_llm_token_output"] = 0
    extracted["_lakelogic_llm_latency_ms"] = elapsed_ms
    extracted["_lakelogic_llm_cost_usd"] = 0.0

    return extracted


_PROVIDER_EXTRACT = {
    "openai": _extract_openai,
    "azure_openai": _extract_openai,
    "anthropic": _extract_anthropic,
}


# ── Cost Pricing Tables ──────────────────────────────────────────────────────

_INPUT_PRICES: Dict[str, float] = {
    # USD per 1M input tokens
    "gpt-4o": 2.50,
    "gpt-4o-mini": 0.15,
    "gpt-4-turbo": 10.00,
    "o1": 15.00,
    "o1-mini": 3.00,
}

_OUTPUT_PRICES: Dict[str, float] = {
    # USD per 1M output tokens
    "gpt-4o": 10.00,
    "gpt-4o-mini": 0.60,
    "gpt-4-turbo": 30.00,
    "o1": 60.00,
    "o1-mini": 12.00,
}


def _get_input_price(model: str) -> float:
    """Get per-1M-token input price for a model."""
    for key, price in _INPUT_PRICES.items():
        if key in model:
            return price
    return 0.0


def _get_output_price(model: str) -> float:
    """Get per-1M-token output price for a model."""
    for key, price in _OUTPUT_PRICES.items():
        if key in model:
            return price
    return 0.0


# ── Confidence Scoring ────────────────────────────────────────────────────────


def _score_confidence(
    extracted: Dict[str, Any],
    config: ExtractionConfig,
    prompt: str,
) -> float:
    """
    Score extraction confidence.

    Methods:
      - self_assessment: asks the LLM "rate your confidence 0-1"
      - field_completeness: % of expected fields that are non-null
    """
    confidence_cfg = config.confidence
    if confidence_cfg is None or not confidence_cfg.enabled:
        return 1.0

    method = confidence_cfg.method

    if method == "field_completeness":
        expected = [f.name for f in config.output_schema if not f.nullable]
        if not expected:
            return 1.0
        present = sum(1 for f in expected if extracted.get(f) is not None and str(extracted.get(f)).strip() != "")
        return round(present / len(expected), 4)

    if method == "self_assessment":
        # Check if the LLM included a confidence field
        if "_confidence" in extracted:
            try:
                return float(extracted["_confidence"])
            except (ValueError, TypeError):
                pass
        return 0.5  # default if LLM didn't self-assess

    # Default: field completeness
    return 1.0


# ── Preprocessing (file → text) ───────────────────────────────────────────────


def _preprocess_pdf(file_path: str, config: Dict[str, Any]) -> str:
    """Extract text from a PDF file using OCR or text extraction."""
    engine = config.get("engine", "easyocr")

    if engine == "easyocr":
        try:
            import easyocr
        except ImportError:
            raise ImportError("easyocr is required for PDF OCR. Install with: pip install lakelogic[ocr]")
        reader = easyocr.Reader([config.get("language", "en")])
        results = reader.readtext(file_path)
        return " ".join([text for _, text, _ in results])

    # Fallback: try pdfplumber for text-based PDFs
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("pdfplumber or easyocr required for PDF processing. Install with: pip install pdfplumber")
    text_parts = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
    return "\n\n".join(text_parts)


def _preprocess_audio(file_path: str, config: Dict[str, Any]) -> str:
    """Transcribe audio file using Whisper."""
    from lakelogic.engines.model_registry import load_model

    model_name = config.get("model")
    pipe = load_model("transcription", model_override=model_name)
    result = pipe(file_path)
    return result.get("text", "") if isinstance(result, dict) else str(result)


def _preprocess_image(file_path: str, config: Dict[str, Any]) -> str:
    """Extract text from image via OCR or generate a caption."""
    engine = config.get("engine", "easyocr")

    if engine == "easyocr":
        try:
            import easyocr
        except ImportError:
            raise ImportError("easyocr is required for image OCR. Install with: pip install lakelogic[ocr]")
        reader = easyocr.Reader([config.get("language", "en")])
        results = reader.readtext(file_path)
        return " ".join([text for _, text, _ in results])

    if engine in ("blip", "blip2", "caption"):
        from lakelogic.engines.model_registry import load_model

        pipe = load_model("image_captioning")
        result = pipe(file_path)
        return result[0].get("generated_text", "") if result else ""

    return ""


_PREPROCESSORS = {
    "pdf": _preprocess_pdf,
    "audio": _preprocess_audio,
    "image": _preprocess_image,
    "video": _preprocess_audio,  # extract audio track → transcribe
}


def preprocess_row(
    row: Dict[str, Any],
    config: ExtractionConfig,
) -> Dict[str, Any]:
    """
    Run preprocessing on a row to convert raw files to text.

    Updates the row dict in-place with the extracted text column.
    """
    prep = config.preprocessing
    if prep is None:
        return row

    file_col = prep.file_column or "file_path"
    file_path = str(row.get(file_col, ""))
    if not file_path:
        return row

    content_type = prep.content_type
    preprocessor = _PREPROCESSORS.get(content_type)
    if preprocessor is None:
        logger.warning(f"No preprocessor for content_type='{content_type}'")
        return row

    ocr_config = prep.ocr or prep.transcription or {}
    extracted_text = preprocessor(file_path, ocr_config)

    row[prep.text_output_column] = extracted_text
    return row


# ── Main Extraction Pipeline ─────────────────────────────────────────────────


def extract_row(
    row: Dict[str, Any],
    config: ExtractionConfig,
    client: Any = None,
) -> Dict[str, Any]:
    """
    Extract structured data from a single row using the configured LLM.

    Steps:
      1. Preprocess (OCR / transcription) if needed
      2. Render prompt template with row data
      3. Call LLM provider
      4. Parse JSON response
      5. Score confidence
      6. Add lineage columns

    Parameters
    ----------
    row : dict
        Input row data.
    config : ExtractionConfig
        Extraction configuration from the contract.
    client : Any, optional
        Pre-initialized provider client.

    Returns
    -------
    dict
        Original row columns + extracted fields + lineage metadata.
    """
    # Step 1: Preprocess if needed
    row = preprocess_row(row, config)

    # Step 2: Render prompt (local providers may not have a template)
    provider = config.provider
    if config.prompt_template and provider not in ("unstructured", "spacy"):
        prompt = _render_prompt(config.prompt_template, row)
    else:
        # For unstructured/spacy: pass the text column value directly
        text_col = config.text_column or "text"
        prompt = str(row.get(text_col, ""))

    # Step 3: Call provider
    if provider == "unstructured":
        extracted = _extract_unstructured(config, prompt, config.system_prompt)
    elif provider == "spacy":
        extracted = _extract_spacy(config, prompt, config.system_prompt)
    elif provider == "local":
        extracted = _extract_local(config, prompt, config.system_prompt)
    elif provider in ("pdfplumber", "easyocr"):
        # File-based providers — prompt is typically a file path
        # For single-row usage, return the first row from extract_file
        file_rows = extract_file(prompt, config)
        extracted = file_rows[0] if file_rows else {}
    elif provider in _PROVIDER_EXTRACT:
        if client is None:
            init_fn = _PROVIDER_INIT.get(provider)
            if init_fn:
                client = init_fn(config)
        extract_fn = _PROVIDER_EXTRACT[provider]
        extracted = extract_fn(client, config, prompt, config.system_prompt)
    else:
        raise ValueError(
            f"Unknown extraction provider: {provider!r}. "
            f"Supported: openai, anthropic, azure_openai, google, local, "
            f"unstructured, spacy, pdfplumber, easyocr"
        )

    # Step 4: Score confidence
    confidence = _score_confidence(extracted, config, prompt)
    conf_col = config.confidence.column if config.confidence else "_extraction_confidence"
    extracted[conf_col] = confidence

    # Step 5: Merge with original row
    result = {**row, **extracted}

    # Remove internal keys
    result.pop("_parse_error", None)
    result.pop("_raw_response", None)
    result.pop("_confidence", None)

    return result


def extract_file(
    file_path: str,
    config: ExtractionConfig,
) -> List[Dict[str, Any]]:
    """
    Extract structured data from a file using the configured provider.

    Unlike ``extract_row`` (single row in → single row out), ``extract_file``
    reads an entire file and can return **multiple rows** — for example one
    row per table row in a PDF, or a single row with OCR text from an image.

    Supported providers:
      - ``pdfplumber`` — PDF table extraction (one row per table row)
      - ``easyocr``   — Image OCR via PyTorch (one row with text)

    Parameters
    ----------
    file_path : str
        Path to the file to extract from.
    config : ExtractionConfig
        Extraction configuration from the contract.

    Returns
    -------
    list of dict
        Extracted rows, each containing the output_schema fields
        plus ``_lakelogic_llm_*`` lineage columns.

    Examples
    --------
    PDF table extraction::

        rows = extract_file("invoice.pdf", config)
        df = pl.DataFrame(rows)

    Image OCR::

        rows = extract_file("receipt.png", config)
        df = pl.DataFrame(rows)
    """
    provider = config.provider

    if provider == "pdfplumber":
        return _extract_pdfplumber(file_path, config)
    elif provider == "easyocr":
        return _extract_easyocr(file_path, config)
    else:
        raise ValueError(
            f"extract_file does not support provider={provider!r}. "
            f"Use extract_row() for text-based providers, or use "
            f"provider='pdfplumber' (PDFs) or 'easyocr' (images)."
        )


def extract_batch(
    rows: List[Dict[str, Any]],
    contract: DataContract,
) -> List[Dict[str, Any]]:
    """
    Extract structured data from a batch of rows.

    Returns all rows with extracted fields + confidence scores.
    Quarantine decisions are handled by quality rules in the contract,
    not by the extraction engine.

    Parameters
    ----------
    rows : list of dict
        Input rows.
    contract : DataContract
        Contract with ``extraction`` config.

    Returns
    -------
    list of dict
        Rows with extracted fields + lineage metadata.
        Quality rules (e.g. ``_lakelogic_extraction_confidence >= 0.7``)
        handle quarantine splitting downstream.
    """
    config = contract.extraction
    if config is None:
        raise ValueError("Contract has no extraction config")

    # Initialize client once for the batch
    client = None
    if config.provider in _PROVIDER_INIT:
        client = _PROVIDER_INIT[config.provider](config)

    results: List[Dict[str, Any]] = []
    total_cost = 0.0

    for i, row in enumerate(rows):
        # Cost ceiling check
        if config.max_cost_per_run and total_cost >= config.max_cost_per_run:
            logger.warning(
                f"Cost ceiling reached (${total_cost:.2f} >= "
                f"${config.max_cost_per_run}). Stopping at row {i}/{len(rows)}."
            )
            break

        # Row limit check
        if config.max_rows_per_run and i >= config.max_rows_per_run:
            logger.warning(f"Row limit reached ({config.max_rows_per_run}). Stopping at row {i}/{len(rows)}.")
            break

        try:
            result = extract_row(row, config, client)
            total_cost += result.get("_lakelogic_llm_cost_usd", 0.0)
            results.append(result)

        except Exception as exc:
            logger.error(f"Extraction failed for row {i}: {exc}")
            row_copy = dict(row)
            conf_col = config.confidence.column if config.confidence else "_lakelogic_extraction_confidence"
            row_copy["_lakelogic_errors"] = f"Extraction error: {exc}"
            row_copy[conf_col] = 0.0
            results.append(row_copy)

    logger.info(f"Extraction complete: {len(results)} rows processed, total cost: ${total_cost:.4f}")

    return results
