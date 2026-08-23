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
        import pandas as pd

        # Round-trip via pandas so mixed/None extracted columns get a stable schema.
        return like.sparkSession.createDataFrame(pd.DataFrame(rows))
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
            f"Extraction failed for all {len(enriched)} row(s) using provider "
            f"'{config.provider}': {failures[0]}"
        )
    if failures:
        logger.warning(f"Extraction failed for {len(failures)}/{len(enriched)} rows: {failures[0]}")

    return _rows_to_frame(enriched, df)
