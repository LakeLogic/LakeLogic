from typing import Tuple

import pandas as pd
from loguru import logger

from lakelogic.engines.base import EngineAdapter


class PandasAdapter(EngineAdapter):
    """
    Pandas execution engine for LakeLogic.
    Uses DuckDB as a high-performance SQL backend to process Pandas DataFrames.
    """

    def execute(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Executes the contract by delegating to DuckDBAdapter.
        Input and Output are Pandas DataFrames.

        Args:
            df: Pandas DataFrame to validate.

        Returns:
            Tuple of (good_df, bad_df).
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected Pandas DataFrame, got {type(df)}")

        logger.info("Pandas engine selected; using DuckDB backend for SQL execution.")

        # We leverage the DuckDBAdapter logic directly
        try:
            from lakelogic.engines.duckdb import DuckDBAdapter
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "Pandas engine requires DuckDB for SQL execution. "
                "Install with `pip install lakelogic[pandas]` or `pip install duckdb`."
            ) from exc

        duck_adapter = DuckDBAdapter(self.contract)
        good_df, bad_df = duck_adapter.execute(df)
        self.dataset_rule_results = duck_adapter.dataset_rule_results
        self.trace = duck_adapter.trace

        # DuckDBAdapter materialises to Polars internally — convert back to pandas.
        import polars as pl

        if isinstance(good_df, pl.DataFrame):
            good_df = good_df.to_pandas()
        if isinstance(bad_df, pl.DataFrame):
            bad_df = bad_df.to_pandas()

        return good_df, bad_df
