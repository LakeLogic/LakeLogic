"""
lakelogic.engines.model_registry
---------------------------------
Registry of free, commercially-licensed local models for unstructured
data processing.  Models auto-download on first use via HuggingFace
``transformers`` and cache in ``~/.cache/huggingface/``.

Install extras as needed::

    pip install lakelogic[local]     # text NER, classification, summarization
    pip install lakelogic[ocr]       # PDF / image text extraction
    pip install lakelogic[vision]    # image captioning, classification
    pip install lakelogic[audio]     # speech-to-text (Whisper)
    pip install lakelogic[llm]       # cloud LLM providers (OpenAI, Anthropic)

Usage::

    from lakelogic.engines.model_registry import load_model

    # First call downloads; subsequent calls use cache
    pipe = load_model("classification")
    result = pipe("This is a billing complaint", candidate_labels=["billing", "technical", "shipping"])
"""

from __future__ import annotations

import functools
from typing import Any, Dict, Optional

from loguru import logger

# ── Model Registry ────────────────────────────────────────────────────────────
# One default model per task.  Users can override via contract YAML.
# All defaults are commercially licensed (MIT / Apache 2.0 / BSD).

LOCAL_MODEL_REGISTRY: Dict[str, Dict[str, Any]] = {
    # ── Text ──────────────────────────────────────────────────────────────
    "ner": {
        "model": "urchade/gliner_medium-v2.1",
        "task": "ner",
        "licence": "Apache 2.0",
        "size_mb": 420,
        "extra": "local",
        "description": "Named Entity Recognition — finds names, dates, amounts, locations",
    },
    "classification": {
        "model": "facebook/bart-large-mnli",
        "task": "zero-shot-classification",
        "licence": "MIT",
        "size_mb": 1600,
        "extra": "local",
        "description": "Zero-shot text classification — picks a label from a list you define",
    },
    "summarization": {
        "model": "facebook/bart-large-cnn",
        "task": "summarization",
        "licence": "MIT",
        "size_mb": 1600,
        "extra": "local",
        "description": "Text summarization — condenses long text into 1-2 sentences",
    },
    "question_answering": {
        "model": "deepset/roberta-base-squad2",
        "task": "question-answering",
        "licence": "CC BY 4.0",
        "size_mb": 500,
        "extra": "local",
        "description": "Extractive QA — answers a question from a text passage",
    },
    "embeddings": {
        "model": "sentence-transformers/all-MiniLM-L6-v2",
        "task": "feature-extraction",
        "licence": "Apache 2.0",
        "size_mb": 80,
        "extra": "local",
        "description": "Text embeddings — converts text to vectors for search/matching",
    },
    # ── Document / OCR ────────────────────────────────────────────────────
    "document_qa": {
        "model": "impira/layoutlm-document-qa",
        "task": "document-question-answering",
        "licence": "MIT",
        "size_mb": 500,
        "extra": "ocr",
        "description": "Document QA — extracts field values from scanned forms",
    },
    "table_detection": {
        "model": "microsoft/table-transformer-detection",
        "task": "object-detection",
        "licence": "MIT",
        "size_mb": 115,
        "extra": "ocr",
        "description": "Table detection — finds tables in document images",
    },
    # ── Vision ────────────────────────────────────────────────────────────
    "image_captioning": {
        "model": "Salesforce/blip-image-captioning-large",
        "task": "image-to-text",
        "licence": "BSD-3",
        "size_mb": 990,
        "extra": "vision",
        "description": "Image captioning — describes what's in an image",
    },
    "image_classification": {
        "model": "openai/clip-vit-large-patch14",
        "task": "zero-shot-image-classification",
        "licence": "MIT",
        "size_mb": 1700,
        "extra": "vision",
        "description": "Zero-shot image classification — labels images from a list you define",
    },
    "object_detection": {
        "model": "facebook/detr-resnet-50",
        "task": "object-detection",
        "licence": "Apache 2.0",
        "size_mb": 167,
        "extra": "vision",
        "description": "Object detection — finds and labels objects in images",
    },
    # ── Audio ─────────────────────────────────────────────────────────────
    "transcription": {
        "model": "openai/whisper-medium",
        "task": "automatic-speech-recognition",
        "licence": "MIT",
        "size_mb": 1500,
        "extra": "audio",
        "description": "Speech-to-text — transcribes audio/video to text",
    },
    # ── Local LLM (for full extraction without cloud API) ─────────────────
    "local_llm": {
        "model": "microsoft/Phi-3-mini-4k-instruct",
        "task": "text-generation",
        "licence": "MIT",
        "size_mb": 7600,
        "extra": "local",
        "description": "General-purpose local LLM — complex multi-field extraction",
    },
}


# ── Lazy Model Loader ─────────────────────────────────────────────────────────


@functools.lru_cache(maxsize=None)
def load_model(
    task: str,
    model_override: Optional[str] = None,
    device: Optional[str] = None,
):
    """
    Load a HuggingFace model lazily.

    Downloads on first call, serves from ``~/.cache/huggingface/`` on
    subsequent calls.  Uses ``@lru_cache`` so the pipeline stays in
    memory for the session lifetime.

    Parameters
    ----------
    task : str
        Registry task key (e.g. ``"classification"``, ``"ner"``,
        ``"transcription"``).
    model_override : str, optional
        HuggingFace model ID to use instead of the registry default.
    device : str, optional
        Device hint (``"cpu"``, ``"cuda"``, ``"mps"``).
        Defaults to ``"auto"`` (GPU if available).

    Returns
    -------
    transformers.Pipeline
        Ready-to-use HuggingFace pipeline.

    Raises
    ------
    ValueError
        If ``task`` is not in the registry.
    ImportError
        If ``transformers`` or ``torch`` is not installed.
    """
    entry = LOCAL_MODEL_REGISTRY.get(task)
    if entry is None:
        known = sorted(LOCAL_MODEL_REGISTRY.keys())
        raise ValueError(f"Unknown extraction task: {task!r}. Known tasks: {known}")

    model_name = model_override or entry["model"]
    hf_task = entry["task"]

    logger.info(f"Loading model '{model_name}' for task '{task}' (licence: {entry['licence']}, ~{entry['size_mb']} MB)")
    logger.info("First run downloads the model from HuggingFace; subsequent runs load from local cache.")

    try:
        from transformers import pipeline as hf_pipeline
    except ImportError:
        raise ImportError(
            f"transformers is required for local model '{model_name}'. "
            f"Install with: pip install lakelogic[{entry['extra']}]"
        )

    pipe = hf_pipeline(
        hf_task,
        model=model_name,
        device_map=device or "auto",
    )

    logger.info(f"Model '{model_name}' loaded successfully.")
    return pipe


def list_models() -> Dict[str, Dict[str, Any]]:
    """Return the full model registry for inspection."""
    return dict(LOCAL_MODEL_REGISTRY)


def get_model_info(task: str) -> Dict[str, Any]:
    """Return metadata for a single task's default model."""
    entry = LOCAL_MODEL_REGISTRY.get(task)
    if entry is None:
        raise ValueError(f"Unknown task: {task!r}")
    return dict(entry)


"""
from lakelogic.engines.model_registry import load_model

# ── Text tasks ──────────────────────────────────────────
ner = load_model("ner")
result = ner("John Smith paid $1,250 on March 5th")
# → [{"entity": "PER", "word": "John Smith"}, {"entity": "MONEY", "word": "$1,250"}, ...]

clf = load_model("classification")
result = clf("My invoice is wrong", candidate_labels=["billing", "technical", "shipping"])
# → {"labels": ["billing", ...], "scores": [0.94, ...]}

summary = load_model("summarization")
result = summary("Long support ticket text here...")
# → [{"summary_text": "Customer reports duplicate charge..."}]

# ── Vision tasks ────────────────────────────────────────
caption = load_model("image_captioning")
result = caption("path/to/product_photo.jpg")
# → [{"generated_text": "a red leather handbag on a white background"}]

# ── Audio tasks ─────────────────────────────────────────
transcribe = load_model("transcription")
result = transcribe("path/to/call_recording.mp3")
# → {"text": "Hello, I'm calling about my order..."}

# ── Override with a different model ─────────────────────
clf = load_model("classification", model_override="cross-encoder/nli-MiniLM2")
"""
