"""
Standard LakeLogic Hook for PII masking.

Two modes — pick based on whether your PII columns are known in advance:

Mode 1 — Direct replace (DEFAULT)
----------------------------------
Use when the PII columns are explicitly listed in the contract.
Replaces the **entire value** in each named column.
Zero external dependencies beyond the dataframe engine.

    args:
      columns:   [patient_name, ssn, email]   # required
      mask_with: "[PROTECTED]"                 # default: <MASKED>

Mode 2 — NLP detect-and-replace (use_nlp: true)
------------------------------------------------
Use when PII is **embedded inside free-text fields** (notes, tickets, emails)
and you want partial masking — only the PII spans are replaced, the rest of
the sentence is preserved.

Requires Microsoft Presidio + a spaCy language model:

    pip install presidio-analyzer presidio-anonymizer
    python -m spacy download en_core_web_sm   # 12 MB — sufficient for most use cases
    python -m spacy download en_core_web_lg   # 800 MB — higher NER accuracy for PERSON/LOCATION

    args:
      columns:     [notes]                         # columns to scan
      use_nlp:     true
      entities:    [PERSON, EMAIL_ADDRESS, US_SSN] # presidio entity types
      mask_with:   "[REDACTED]"
      spacy_model: en_core_web_sm                  # optional — default: en_core_web_sm

Presidio entity reference
--------------------------
Pattern-based (no NER, en_core_web_sm sufficient):
  EMAIL_ADDRESS, PHONE_NUMBER, US_SSN, CREDIT_CARD, IBAN_CODE, IP_ADDRESS, DATE_TIME

NER-based (requires spaCy NER — en_core_web_sm or lg):
  PERSON, LOCATION, ORGANIZATION, NRP, GPE
"""

from typing import Any, Dict, List, Optional

from loguru import logger

try:
    from presidio_analyzer import AnalyzerEngine
    from presidio_anonymizer import AnonymizerEngine
    from presidio_anonymizer.entities import OperatorConfig

    _PRESIDIO_AVAILABLE = True
except ImportError:
    AnalyzerEngine = None  # type: ignore[assignment,misc]
    AnonymizerEngine = None  # type: ignore[assignment,misc]
    _PRESIDIO_AVAILABLE = False


# ---------------------------------------------------------------------------
# Mode 1 — Direct replace
# ---------------------------------------------------------------------------


def _direct_replace(df: Any, columns: List[str], mask_with: str) -> Any:
    """Replace entire column values. No NLP or Presidio required."""
    try:
        import polars as pl

        if isinstance(df, pl.DataFrame):
            logger.info(f"PII masking (direct): {len(columns)} column(s) → '{mask_with}'")
            return df.with_columns([pl.lit(mask_with).alias(c) for c in columns if c in df.columns])
    except ImportError:
        pass

    try:
        from pyspark.sql import DataFrame as SparkDF
        from pyspark.sql import functions as F

        if isinstance(df, SparkDF):
            logger.info(f"PII masking (direct/Spark): {len(columns)} column(s) → '{mask_with}'")
            for col in columns:
                if col in df.columns:
                    df = df.withColumn(col, F.lit(mask_with))
            return df
    except ImportError:
        pass

    # Pandas fallback
    logger.info(f"PII masking (direct/Pandas): {len(columns)} column(s) → '{mask_with}'")
    for col in columns:
        if col in df.columns:
            df[col] = mask_with
    return df


# ---------------------------------------------------------------------------
# Mode 2 — NLP detect-and-replace (Presidio + spaCy)
# ---------------------------------------------------------------------------


def _nlp_replace(
    df: Any,
    columns: List[str],
    entities: List[str],
    mask_with: str,
    spacy_model: str,
) -> Any:
    """
    Presidio NLP detect-and-replace.

    Finds PII *spans* within free text and replaces only those spans,
    preserving the rest of the sentence. Always requires:
      - presidio-analyzer + presidio-anonymizer
      - a spaCy language model (en_core_web_sm minimum)

    en_core_web_sm (~12 MB) is sufficient for all pattern-based entities
    (EMAIL, SSN, PHONE) and reasonable NER. Use en_core_web_lg (~800 MB)
    for highest accuracy on PERSON/LOCATION detection.
    """
    if not _PRESIDIO_AVAILABLE:
        logger.error(
            "PII masking (NLP mode) requires presidio packages. Install with:\n"
            "  pip install presidio-analyzer presidio-anonymizer\n"
            f"  python -m spacy download {spacy_model}"
        )
        return df

    try:
        from presidio_analyzer.nlp_engine import NlpEngineProvider

        nlp_config = {
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": spacy_model}],
        }
        nlp_engine = NlpEngineProvider(nlp_configuration=nlp_config).create_engine()
        analyzer = AnalyzerEngine(nlp_engine=nlp_engine)
        logger.info(f"PII masking (NLP): using spaCy '{spacy_model}', entities={entities}")
    except Exception as exc:
        logger.error(
            f"PII masking (NLP): failed to load spaCy model '{spacy_model}' — {exc}\n"
            f"Run: python -m spacy download {spacy_model}"
        )
        return df

    anonymizer = AnonymizerEngine()
    operators = {e: OperatorConfig("replace", {"new_value": mask_with}) for e in entities}

    def _anonymize(text: Any) -> Any:
        if not isinstance(text, str) or not text:
            return text
        results = analyzer.analyze(text=text, entities=entities, language="en")
        return anonymizer.anonymize(text=text, analyzer_results=results, operators=operators).text

    try:
        import polars as pl

        if isinstance(df, pl.DataFrame):
            logger.info(f"PII masking (NLP/Polars): scanning {len(columns)} column(s)")
            return df.with_columns(
                [pl.col(c).map_elements(_anonymize, return_dtype=pl.Utf8).alias(c) for c in columns if c in df.columns]
            )
    except ImportError:
        pass

    try:
        from pyspark.sql import DataFrame as SparkDF
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        if isinstance(df, SparkDF):
            logger.info(f"PII masking (NLP/Spark): scanning {len(columns)} column(s)")
            mask_udf = udf(_anonymize, StringType())
            for col in columns:
                if col in df.columns:
                    df = df.withColumn(col, mask_udf(df[col]))
            return df
    except ImportError:
        pass

    # Pandas fallback
    logger.info(f"PII masking (NLP/Pandas): scanning {len(columns)} column(s)")
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(_anonymize)
    return df


# ---------------------------------------------------------------------------
# Public hook — called by LakeLogic external_logic
# ---------------------------------------------------------------------------


def mask_pii(
    df: Any,
    args: Optional[Dict[str, Any]] = None,
    contract: Any = None,
    engine: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    """
    Mask PII in a dataframe.

    See module docstring for full usage and dependency requirements.
    """
    if args is None:
        args = {}
    if kwargs:
        args = {**args, **kwargs}

    mask_with: str = args.get("mask_with", "<MASKED>")
    use_nlp: bool = bool(args.get("use_nlp", False))
    entities: List[str] = args.get("entities", ["PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER"])
    spacy_model: str = args.get("spacy_model", "en_core_web_sm")

    # Resolve target columns
    target_columns: Optional[List[str]] = args.get("columns")
    if not target_columns:
        # No explicit columns — fall back to all string columns
        try:
            import polars as pl

            if isinstance(df, pl.DataFrame):
                target_columns = [c for c in df.columns if df[c].dtype == pl.Utf8]
        except ImportError:
            pass
        if not target_columns:
            try:
                target_columns = [c for c in df.columns if df[c].dtype == object]
            except Exception:
                target_columns = []

    if not target_columns:
        logger.warning("PII masking: no columns to mask — skipping.")
        return df

    if use_nlp:
        return _nlp_replace(df, target_columns, entities, mask_with, spacy_model)
    else:
        return _direct_replace(df, target_columns, mask_with)
