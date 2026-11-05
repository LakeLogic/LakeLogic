"""
Wire the extraction engine into the processing pipeline.

A contract may declare an ``extraction:`` block (``ExtractionConfig``) that turns a
free-text column into structured fields — via an LLM (openai/anthropic/google),
a local model (spacy/local), file readers (unstructured/pdfplumber/easyocr), or the
deterministic offline ``regex`` provider. The extraction engine (``engines.llm``)
operates on rows (list of dict); this module bridges the engine-native frame to
rows, runs ``extract_batch``, and rebuilds the frame — so extracted fields are then
validated, quality-checked, and materialized by the contract like any other column.

Runs BEFORE schema/quality enforcement (same rationale as external_logic), so the
contract governs the extracted output.
"""

from typing import Any

from loguru import logger


def _frame_to_rows(df: Any) -> list:
    """Engine-native frame -> list[dict], for the row-based extraction engine."""
    mod = type(df).__module__
    if mod.startswith("pyspark"):
        return df.toPandas().to_dict("records")
    if mod.startswith("polars"):
        import polars as pl

        d = df.collect() if isinstance(df, pl.LazyFrame) else df
        return d.to_dicts()
    if hasattr(df, "to_dict"):  # pandas
        return df.to_dict("records")
    return list(df)


def _rows_to_frame(rows: list, like: Any) -> Any:
    """list[dict] -> a frame in the SAME framework as ``like``."""
    mod = type(like).__module__
    if mod.startswith("pyspark"):
        # An explicit schema: a column null in every row (an unpriced model's cost, a field no
        # document stated) is un-inferrable on Spark Connect — CANNOT_DETERMINE_TYPE.
        from pyspark.sql.types import DoubleType, StructField, StructType

        from lakelogic.core.processor import DataProcessor

        schema, keys = DataProcessor._spark_schema_for_extracted_rows(rows)
        schema = StructType([
            StructField(f.name, DoubleType(), True) if f.name.endswith("_cost_usd") else f for f in schema.fields
        ])
        data = [
            tuple((float(r[k]) if k.endswith("_cost_usd") and r.get(k) is not None else r.get(k)) for k in keys)
            for r in rows
        ]
        return like.sparkSession.createDataFrame(data, schema)
    if mod.startswith("polars"):
        import polars as pl

        return pl.DataFrame(rows)
    import pandas as pd

    return pd.DataFrame(rows)


def apply_extraction(contract: Any, df: Any, engine_name: str) -> Any:
    """Run the contract's ``extraction`` config over ``df`` and return the enriched
    frame (unchanged if no extraction is configured). Engine-agnostic."""
    config = getattr(contract, "extraction", None)
    if config is None:
        return df

    from lakelogic.engines.llm import extract_batch

    rows = _frame_to_rows(df)
    if not rows:
        return df

    logger.info(
        f"Extraction: provider='{config.provider}' over {len(rows)} rows "
        f"(text_column='{getattr(config, 'text_column', None)}')."
    )
    enriched = extract_batch(rows, contract)

    # A batch where EVERY row failed used to be N ERROR log lines followed by a
    # green run over empty columns — the exact shape of the "90 PDFs, 0 fields"
    # incident. Total failure is a run-level failure, so fail the run with the
    # count and the first reason rather than materializing empty output.
    failures = [
        str(r.get("_lakelogic_errors") or "")
        for r in enriched
        if str(r.get("_lakelogic_errors") or "").startswith("Extraction error")
    ]
    if failures and len(failures) == len(enriched):
        raise RuntimeError(
            f"Extraction failed for all {len(enriched)} row(s) using provider '{config.provider}': {failures[0]}"
        )
    if failures:
        logger.warning(f"Extraction failed for {len(failures)}/{len(enriched)} rows: {failures[0]}")

    drop = _read_text_column_to_drop(contract, config)
    if drop:
        for r in enriched:
            r.pop(drop, None)
    return _rows_to_frame(enriched, df)


def _read_text_column_to_drop(contract: Any, config: Any) -> str:
    """The column of text the engine READ from each document (``preprocessing.text_output_column``)
    — dropped after extraction unless ``extraction.keep_document_text: true``, or the contract
    declares that column as a field. A document's full text can carry personal data the
    extracted fields were chosen to leave out, and it is the bulk of the row (2026-11-04).
    A text column the SOURCE supplied (a ticket body) is never dropped: only read text is."""
    prep = getattr(config, "preprocessing", None)
    column = getattr(prep, "text_output_column", None) if prep else None
    if not column:
        return ""
    keep = getattr(config, "keep_document_text", None)
    if keep is None and getattr(config, "model_extra", None):
        keep = config.model_extra.get("keep_document_text")
    if keep is True or str(keep).lower() == "true":
        return ""
    fields = getattr(getattr(contract, "model", None), "fields", None) or []
    if any((getattr(f, "name", None) or (f.get("name") if isinstance(f, dict) else None)) == column for f in fields):
        return ""
    return column
