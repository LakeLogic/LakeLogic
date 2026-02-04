"""
Additional HA Color Change Detection
"""

from typing import Union, List
from loguru import logger
import polars as pl


def calculate_ha_color_change(
    df,  # Union[pl.DataFrame, SparkFrame]
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
):
    """
    Calculate HA color (simple bar color) and color change events.
    
    HA Color:
    - Green: ha_close > ha_open
    - Red: ha_close < ha_open
    
    HA Color Change:
    - 'green_to_red': Previous bar was green, current is red (potential sell)
    - 'red_to_green': Previous bar was red, current is green (potential buy)
    - None: No color change
    
    Args:
        df: Input DataFrame with HA columns
        partition_cols: Columns to partition by
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with ha_color and ha_color_change columns
    """
    logger.info("Calculating HA color and color change detection")
    
    if engine == "polars":
        # Calculate HA color
        df = df.with_columns([
            pl.when(pl.col("ha_close") > pl.col("ha_open"))
              .then(pl.lit("green"))
              .when(pl.col("ha_close") < pl.col("ha_open"))
              .then(pl.lit("red"))
              .otherwise(pl.lit("neutral"))
              .alias("ha_color")
        ])
        
        # Calculate color change
        df = df.with_columns([
            pl.col("ha_color").shift(1).over(partition_cols, order_by="timestamp").alias("_prev_ha_color")
        ])
        
        df = df.with_columns([
            pl.when(
                (pl.col("_prev_ha_color") == "green") & (pl.col("ha_color") == "red")
            ).then(pl.lit("green_to_red"))
            .when(
                (pl.col("_prev_ha_color") == "red") & (pl.col("ha_color") == "green")
            ).then(pl.lit("red_to_green"))
            .otherwise(None)
            .alias("ha_color_change")
        ])
        
        # Drop temporary column
        df = df.drop("_prev_ha_color")
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp")
        
        # Calculate HA color
        df = df.withColumn("ha_color",
                          F.when(F.col("ha_close") > F.col("ha_open"), "green")
                           .when(F.col("ha_close") < F.col("ha_open"), "red")
                           .otherwise("neutral"))
        
        # Calculate color change
        df = df.withColumn("_prev_ha_color", F.lag("ha_color", 1).over(window_spec))
        
        df = df.withColumn("ha_color_change",
                          F.when(
                              (F.col("_prev_ha_color") == "green") & (F.col("ha_color") == "red"),
                              "green_to_red"
                          ).when(
                              (F.col("_prev_ha_color") == "red") & (F.col("ha_color") == "green"),
                              "red_to_green"
                          ).otherwise(None))
        
        # Drop temporary column
        df = df.drop("_prev_ha_color")
    
    return df


# Add this to the process_all_signals function after calculate_ha_trend:
# df = calculate_ha_color_change(df, partition_cols=partition_cols, engine=engine)
