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

Usage::

    from lakelogic import DataContract, DataProcessor

    contract = DataContract.from_yaml("contracts/support_tickets.yaml")
    # contract.extraction is populated → DataProcessor routes to LLMAdapter
    result = DataProcessor.run(contract, df)
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

    # Step 2: Render prompt
    prompt = _render_prompt(config.prompt_template, row)

    # Step 3: Call LLM
    provider = config.provider

    if provider == "local":
        extracted = _extract_local(config, prompt, config.system_prompt)
    elif provider in _PROVIDER_EXTRACT:
        if client is None:
            init_fn = _PROVIDER_INIT.get(provider)
            if init_fn:
                client = init_fn(config)
        extract_fn = _PROVIDER_EXTRACT[provider]
        extracted = extract_fn(client, config, prompt, config.system_prompt)
    else:
        raise ValueError(
            f"Unknown extraction provider: {provider!r}. Supported: local, openai, anthropic, azure_openai, google"
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
