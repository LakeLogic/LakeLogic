from __future__ import annotations

from typing import Any
import pandas as pd


def build_sales_gold(df: Any, contract=None, engine: str | None = None, **kwargs):
    """
    Aggregate sales by salesperson to create a Gold fact table.

    Args:
        df: Input dataframe (polars/pandas/spark).
        contract: Optional DataContract instance.
        engine: Engine name used by LakeGuard.
        **kwargs: Optional extra parameters.

    Returns:
        pandas.DataFrame with aggregated results.
    """
    if hasattr(df, "to_pandas"):
        pdf = df.to_pandas()
    elif hasattr(df, "toPandas"):
        pdf = df.toPandas()
    else:
        pdf = df

    if not isinstance(pdf, pd.DataFrame):
        pdf = pd.DataFrame(pdf)

    agg = pdf.groupby("salesperson_id", as_index=False)["amount"].sum()
    agg = agg.rename(columns={"amount": "total_amount"})
    agg["processed_at"] = pd.Timestamp.utcnow().tz_localize("UTC")
    return agg
