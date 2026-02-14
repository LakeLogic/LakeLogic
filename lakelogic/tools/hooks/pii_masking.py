"""
Standard LakeLogic Hook for PII masking using Microsoft Presidio.
"""
from typing import Any, Dict, List, Optional
import os
from loguru import logger

try:
    from presidio_analyzer import AnalyzerEngine
    from presidio_anonymizer import AnonymizerEngine
    from presidio_anonymizer.entities import OperatorConfig
except ImportError:
    AnalyzerEngine = None
    AnonymizerEngine = None

def mask_pii(df: Any, args: Dict[str, Any]) -> Any:
    """
    Analyzes and masks PII in a dataframe.
    
    Args:
        df: Input dataframe (Polars, Pandas, or PySpark).
        args: Configuration for masking.
            - entities: List of entities to mask (e.g. ["PERSON", "EMAIL_ADDRESS"]).
            - mask_with: String to replace PII with (default: "<MASKED>").
            - columns: Specific columns to scan (optional).
    """
    if AnalyzerEngine is None:
        logger.error("PII masking requires 'presidio-analyzer' and 'presidio-anonymizer'.")
        return df

    entities = args.get("entities", ["PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "LOCATION"])
    mask_with = args.get("mask_with", "<MASKED>")
    target_columns = args.get("columns")

    entities = args.get("entities", ["PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "LOCATION"])
    mask_with = args.get("mask_with", "<MASKED>")
    target_columns = args.get("columns")

    analyzer = AnalyzerEngine()
    anonymizer = AnonymizerEngine()
    operators = {
        entity: OperatorConfig("replace", {"new_value": mask_with})
        for entity in entities
    }

    def anonymize_text(text):
        if not isinstance(text, str) or not text:
            return text
        results = analyzer.analyze(text=text, entities=entities, language='en')
        anonymized = anonymizer.anonymize(
            text=text,
            analyzer_results=results,
            operators=operators
        )
        return anonymized.text

    # --- POLARS OPTIMIZATION ---
    try:
        import polars as pl
        if isinstance(df, pl.DataFrame):
            if not target_columns:
                target_columns = [col for col in df.columns if df[col].dtype == pl.Utf8]
            
            logger.info(f"Polars Engine: Masking {len(target_columns)} columns.")
            
            # Apply masking natively in Polars
            for col in target_columns:
                df = df.with_columns([
                    pl.col(col).map_elements(anonymize_text, return_dtype=pl.Utf8).alias(col)
                ])
            return df
    except ImportError:
        pass

    # --- PYSPARK OPTIMIZATION ---
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        
        if isinstance(df, SparkDataFrame):
            if not target_columns:
                target_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
            
            logger.info(f"Spark Engine: Distributing PII masking for {len(target_columns)} columns.")
            
            # Register UDF
            mask_udf = udf(anonymize_text, StringType())
            
            for col in target_columns:
                df = df.withColumn(col, mask_udf(df[col]))
            return df
    except ImportError:
        pass

    # --- PANDAS FALLBACK ---
    if not target_columns:
        target_columns = [col for col in df.columns if df[col].dtype == object]
    
    logger.info(f"Pandas/Generic Engine: Masking {len(target_columns)} columns.")
    for col in target_columns:
        df[col] = df[col].apply(anonymize_text)
        
    return df
