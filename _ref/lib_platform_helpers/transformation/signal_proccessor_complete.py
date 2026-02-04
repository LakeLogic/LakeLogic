"""
Signal Processor Module - COMPLETE MERGE
=========================================
Combines ALL functions from part 1 and part 2 with conflicts resolved.

Includes:
- Technical indicators (RSI, MACD, Volume)
- Two signal strategies (Crossover & Reversal)
- Helper functions
"""

from typing import Union, List, Optional, Any
from loguru import logger
import polars as pl

# Type hints
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
SparkFrame = Any


# ==============================================================================
# TECHNICAL INDICATORS
# ==============================================================================

def calculate_rsi(
    df: Union[PolarsFrame, SparkFrame],
    column: str = "close",
    period: int = 14,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    output_name: Optional[str] = None,
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """Calculate RSI indicator."""
    if output_name is None:
        output_name = f"rsi_{column}_{period}"
    
    logger.info(f"Calculating RSI for {column} (period={period}, output={output_name})")
    
    if engine == "polars":
        df = df.with_columns([
            (pl.col(column) - pl.col(column).shift(1).over(partition_cols, order_by="timestamp"))
            .alias("_price_change")
        ])
        
        df = df.with_columns([
            pl.when(pl.col("_price_change") > 0)
              .then(pl.col("_price_change"))
              .otherwise(0.0)
              .alias("_gain"),
            pl.when(pl.col("_price_change") < 0)
              .then(-pl.col("_price_change"))
              .otherwise(0.0)
              .alias("_loss")
        ])
        
        df = df.with_columns([
            pl.col("_gain")
              .rolling_mean(window_size=period)
              .over(partition_cols, order_by="timestamp")
              .alias("_avg_gain"),
            pl.col("_loss")
              .rolling_mean(window_size=period)
              .over(partition_cols, order_by="timestamp")
              .alias("_avg_loss")
        ])
        
        df = df.with_columns([
            pl.when(pl.col("_avg_loss") == 0)
              .then(100.0)
              .when(pl.col("_avg_gain") == 0)
              .then(0.0)
              .otherwise(
                  100.0 - (100.0 / (1.0 + (pl.col("_avg_gain") / pl.col("_avg_loss"))))
              )
              .alias(output_name)
        ])
        
        df = df.drop(["_price_change", "_gain", "_loss", "_avg_gain", "_avg_loss"])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp")
        
        df = df.withColumn("_price_change", 
                          F.col(column) - F.lag(column, 1).over(window_spec))
        
        df = df.withColumn("_gain", 
                          F.when(F.col("_price_change") > 0, F.col("_price_change")).otherwise(0.0))
        df = df.withColumn("_loss",
                          F.when(F.col("_price_change") < 0, -F.col("_price_change")).otherwise(0.0))
        
        window_rolling = Window.partitionBy(partition_cols).orderBy("timestamp").rowsBetween(-period + 1, 0)
        df = df.withColumn("_avg_gain", F.avg("_gain").over(window_rolling))
        df = df.withColumn("_avg_loss", F.avg("_loss").over(window_rolling))
        
        df = df.withColumn(output_name,
                          F.when(F.col("_avg_loss") == 0, 100.0)
                           .when(F.col("_avg_gain") == 0, 0.0)
                           .otherwise(100.0 - (100.0 / (1.0 + (F.col("_avg_gain") / F.col("_avg_loss"))))))
        
        df = df.drop("_price_change", "_gain", "_loss", "_avg_gain", "_avg_loss")
    
    return df


def calculate_macd(
    df: Union[PolarsFrame, SparkFrame],
    column: str = "close",
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    prefix: str = "macd",
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """Calculate MACD indicator."""
    logger.info(f"Calculating MACD for {column} (fast={fast_period}, slow={slow_period}, signal={signal_period})")
    
    if engine == "polars":
        alpha_fast = 2.0 / (fast_period + 1)
        alpha_slow = 2.0 / (slow_period + 1)
        alpha_signal = 2.0 / (signal_period + 1)
        
        df = df.with_columns([
            pl.col(column).ewm_mean(alpha=alpha_fast).over(partition_cols, order_by="timestamp").alias("_ema_fast"),
            pl.col(column).ewm_mean(alpha=alpha_slow).over(partition_cols, order_by="timestamp").alias("_ema_slow")
        ])
        
        df = df.with_columns([
            (pl.col("_ema_fast") - pl.col("_ema_slow")).alias(f"{prefix}_line")
        ])
        
        df = df.with_columns([
            pl.col(f"{prefix}_line").ewm_mean(alpha=alpha_signal).over(partition_cols, order_by="timestamp").alias(f"{prefix}_signal")
        ])
        
        df = df.with_columns([
            (pl.col(f"{prefix}_line") - pl.col(f"{prefix}_signal")).alias(f"{prefix}_hist")
        ])
        
        df = df.drop(["_ema_fast", "_ema_slow"])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_fast = Window.partitionBy(partition_cols).orderBy("timestamp").rowsBetween(-fast_period + 1, 0)
        window_slow = Window.partitionBy(partition_cols).orderBy("timestamp").rowsBetween(-slow_period + 1, 0)
        window_signal = Window.partitionBy(partition_cols).orderBy("timestamp").rowsBetween(-signal_period + 1, 0)
        
        df = df.withColumn("_ema_fast", F.avg(column).over(window_fast))
        df = df.withColumn("_ema_slow", F.avg(column).over(window_slow))
        df = df.withColumn(f"{prefix}_line", F.col("_ema_fast") - F.col("_ema_slow"))
        df = df.withColumn(f"{prefix}_signal", F.avg(f"{prefix}_line").over(window_signal))
        df = df.withColumn(f"{prefix}_hist", F.col(f"{prefix}_line") - F.col(f"{prefix}_signal"))
        df = df.drop("_ema_fast", "_ema_slow")
    
    return df


def calculate_volume_indicators(
    df: Union[PolarsFrame, SparkFrame],
    volume_col: str = "volume",
    price_col: str = "close",
    window: int = 20,
    spike_threshold: float = 2.0,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """Calculate volume-based indicators."""
    logger.info(f"Calculating volume indicators (window={window}, threshold={spike_threshold})")
    
    if engine == "polars":
        df = df.with_columns([
            pl.col(volume_col)
              .rolling_mean(window_size=window)
              .over(partition_cols, order_by="timestamp")
              .alias("volume_sma")
        ])
        
        df = df.with_columns([
            (pl.col(volume_col) / pl.col("volume_sma")).alias("volume_ratio")
        ])
        
        df = df.with_columns([
            pl.when(pl.col("volume_ratio") > spike_threshold)
              .then(pl.lit(1))
              .otherwise(pl.lit(0))
              .alias("volume_spike")
        ])
        
        df = df.with_columns([
            (pl.col(price_col) * pl.col(volume_col)).alias("dollar_volume")
        ])
        
        df = df.with_columns([
            pl.col("dollar_volume")
              .rolling_mean(window_size=window)
              .over(partition_cols, order_by="timestamp")
              .alias("dollar_volume_sma")
        ])
        
        df = df.with_columns([
            (pl.col("dollar_volume") > (pl.col("dollar_volume_sma") * spike_threshold))
              .cast(pl.Int64)
              .alias("dollar_volume_spike")
        ])
        
        df = df.with_columns([
            pl.col("volume_ratio").alias("relative_volume")
        ])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp").rowsBetween(-window + 1, 0)
        
        df = df.withColumn("volume_sma", F.avg(volume_col).over(window_spec))
        df = df.withColumn("volume_ratio", F.col(volume_col) / F.col("volume_sma"))
        df = df.withColumn("volume_spike", 
                          F.when(F.col("volume_ratio") > spike_threshold, 1).otherwise(0))
        
        df = df.withColumn("dollar_volume", F.col(price_col) * F.col(volume_col))
        df = df.withColumn("dollar_volume_sma", F.avg("dollar_volume").over(window_spec))
        df = df.withColumn("dollar_volume_spike",
                          F.when(F.col("dollar_volume") > (F.col("dollar_volume_sma") * spike_threshold), 1).otherwise(0))
        df = df.withColumn("relative_volume", F.col("volume_ratio"))
    
    return df


def calculate_ha_color_change(
    df: Union[PolarsFrame, SparkFrame],
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """Calculate Heikin-Ashi color and color changes."""
    logger.info("Calculating Heikin-Ashi color changes")
    
    if engine == "polars":
        df = df.with_columns([
            pl.when(pl.col("ha_close") > pl.col("ha_open"))
              .then(pl.lit("green"))
              .when(pl.col("ha_close") < pl.col("ha_open"))
              .then(pl.lit("red"))
              .otherwise(pl.lit("neutral"))
              .alias("ha_color")
        ])
        
        df = df.with_columns([
            pl.col("ha_color").shift(1).over(partition_cols, order_by="timestamp").alias("prev_ha_color")
        ])
        
        df = df.with_columns([
            pl.when((pl.col("prev_ha_color") == "green") & (pl.col("ha_color") == "red"))
              .then(pl.lit("green_to_red"))
              .when((pl.col("prev_ha_color") == "red") & (pl.col("ha_color") == "green"))
              .then(pl.lit("red_to_green"))
              .otherwise(pl.lit(None))
              .alias("ha_color_change")
        ])
        
        df = df.drop("prev_ha_color")
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp")
        
        df = df.withColumn("ha_color",
                          F.when(F.col("ha_close") > F.col("ha_open"), "green")
                           .when(F.col("ha_close") < F.col("ha_open"), "red")
                           .otherwise("neutral"))
                           
        df = df.withColumn("prev_ha_color", F.lag("ha_color", 1).over(window_spec))
        
        df = df.withColumn("ha_color_change",
                          F.when((F.col("prev_ha_color") == "green") & (F.col("ha_color") == "red"), "green_to_red")
                           .when((F.col("prev_ha_color") == "red") & (F.col("ha_color") == "green"), "red_to_green")
                           .otherwise(None))
                           
        df = df.drop("prev_ha_color")
        
    return df


# ==============================================================================
# DIVERGENCE DETECTION (Both versions)
# ==============================================================================

def detect_price_volume_divergence_sma(
    df: Union[PolarsFrame, SparkFrame],
    lookback: int = 5,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """
    Detect divergence using volume SMA changes.
    Returns: 1 (bullish), -1 (bearish), 0 (none)
    """
    logger.info(f"Detecting price-volume divergence with SMA (lookback={lookback})")
    
    if engine == "polars":
        df = df.with_columns([
            (pl.col("close") - pl.col("close").shift(lookback).over(partition_cols, order_by="timestamp")).alias("price_change"),
            (pl.col("volume_sma") - pl.col("volume_sma").shift(lookback).over(partition_cols, order_by="timestamp")).alias("volume_change")
        ])
        
        df = df.with_columns([
            pl.when((pl.col("price_change") < 0) & (pl.col("volume_change") > 0))
              .then(pl.lit(1))
              .when((pl.col("price_change") > 0) & (pl.col("volume_change") < 0))
              .then(pl.lit(-1))
              .otherwise(pl.lit(0))
              .alias("price_volume_divergence_sma")
        ])
        
        df = df.drop(["price_change", "volume_change"])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp")
        
        df = df.withColumn("price_change", F.col("close") - F.lag("close", lookback).over(window_spec))
        df = df.withColumn("volume_change", F.col("volume_sma") - F.lag("volume_sma", lookback).over(window_spec))
        
        df = df.withColumn("price_volume_divergence_sma",
                          F.when((F.col("price_change") < 0) & (F.col("volume_change") > 0), 1)
                           .when((F.col("price_change") > 0) & (F.col("volume_change") < 0), -1)
                           .otherwise(0))
                           
        df = df.drop("price_change", "volume_change")
        
    return df


def detect_price_volume_divergence_direct(
    df: Union[PolarsFrame, SparkFrame],
    price_col: str = "close",
    volume_col: str = "volume",
    lookback: int = 5,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """
    Detect divergence using direct volume comparisons.
    Returns: 'bullish', 'bearish', or None
    """
    logger.info(f"Detecting price-volume divergence direct (lookback={lookback})")
    
    if engine == "polars":
        df = df.with_columns([
            (pl.col(price_col) > pl.col(price_col).shift(lookback).over(partition_cols, order_by="timestamp"))
              .alias("_price_rising"),
            (pl.col(price_col) < pl.col(price_col).shift(lookback).over(partition_cols, order_by="timestamp"))
              .alias("_price_falling"),
            (pl.col(volume_col) < pl.col(volume_col).shift(lookback).over(partition_cols, order_by="timestamp"))
              .alias("_volume_declining")
        ])
        
        df = df.with_columns([
            pl.when(pl.col("_price_rising") & pl.col("_volume_declining"))
              .then(pl.lit("bearish"))
              .when(pl.col("_price_falling") & pl.col("_volume_declining"))
              .then(pl.lit("bullish"))
              .otherwise(None)
              .alias("price_volume_divergence_direct")
        ])
        
        df = df.drop(["_price_rising", "_price_falling", "_volume_declining"])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp")
        
        df = df.withColumn("_price_rising",
                          F.col(price_col) > F.lag(price_col, lookback).over(window_spec))
        df = df.withColumn("_price_falling",
                          F.col(price_col) < F.lag(price_col, lookback).over(window_spec))
        df = df.withColumn("_volume_declining",
                          F.col(volume_col) < F.lag(volume_col, lookback).over(window_spec))
        
        df = df.withColumn("price_volume_divergence_direct",
                          F.when(F.col("_price_rising") & F.col("_volume_declining"), "bearish")
                           .when(F.col("_price_falling") & F.col("_volume_declining"), "bullish")
                           .otherwise(None))
        
        df = df.drop("_price_rising", "_price_falling", "_volume_declining")
    
    return df


# ==============================================================================
# SIGNAL GENERATION - CROSSOVER STRATEGY (Binary 0/1)
# ==============================================================================

def generate_crossover_signals(
    df: Union[PolarsFrame, SparkFrame],
    rsi_fast_col: str = "rsi_close_fast",
    rsi_slow_col: str = "rsi_close_slow",
    trend_col: str = "rsi_trend",
    rsi_buy_level: float = 30.0,
    rsi_sell_level: float = 70.0,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    output_prefix: str = "",
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """
    Generate signals based on RSI crossovers.
    Returns: Binary signals (1 = signal, 0 = no signal)
    """
    logger.info(f"Generating crossover signals (prefix='{output_prefix}')")
    
    buy_col = f"{output_prefix}buy_signal_crossover"
    sell_col = f"{output_prefix}sell_signal_crossover"
    
    if engine == "polars":
        df = df.with_columns([
            ((pl.col(rsi_fast_col) > pl.col(rsi_slow_col)) & 
             (pl.col(rsi_fast_col).shift(1).over(partition_cols, order_by="timestamp") <= 
              pl.col(rsi_slow_col).shift(1).over(partition_cols, order_by="timestamp")))
            .alias("_crossover_up"),
            
            ((pl.col(rsi_fast_col) < pl.col(rsi_slow_col)) & 
             (pl.col(rsi_fast_col).shift(1).over(partition_cols, order_by="timestamp") >= 
              pl.col(rsi_slow_col).shift(1).over(partition_cols, order_by="timestamp")))
            .alias("_crossover_down"),
            
            ((pl.col(rsi_fast_col) > rsi_buy_level) & 
             (pl.col(rsi_fast_col).shift(1).over(partition_cols, order_by="timestamp") <= rsi_buy_level))
            .alias("_cross_buy_level"),
            
            ((pl.col(rsi_fast_col) < rsi_sell_level) & 
             (pl.col(rsi_fast_col).shift(1).over(partition_cols, order_by="timestamp") >= rsi_sell_level))
            .alias("_cross_sell_level")
        ])
        
        df = df.with_columns([
            pl.when(
                (pl.col(trend_col).is_in(["Bull", "Neutral"])) &
                (pl.col("_crossover_up") | pl.col("_cross_buy_level"))
            ).then(pl.lit(1)).otherwise(pl.lit(0)).alias(buy_col),
            
            pl.when(
                (pl.col(trend_col).is_in(["Bear", "Neutral"])) &
                (pl.col("_crossover_down") | pl.col("_cross_sell_level"))
            ).then(pl.lit(1)).otherwise(pl.lit(0)).alias(sell_col)
        ])
        
        df = df.drop(["_crossover_up", "_crossover_down", "_cross_buy_level", "_cross_sell_level"])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp")
        
        df = df.withColumn("_prev_rsi_fast", F.lag(rsi_fast_col, 1).over(window_spec))
        df = df.withColumn("_prev_rsi_slow", F.lag(rsi_slow_col, 1).over(window_spec))
        
        df = df.withColumn("_crossover_up", 
                          (F.col(rsi_fast_col) > F.col(rsi_slow_col)) & 
                          (F.col("_prev_rsi_fast") <= F.col("_prev_rsi_slow")))
                          
        df = df.withColumn("_crossover_down", 
                          (F.col(rsi_fast_col) < F.col(rsi_slow_col)) & 
                          (F.col("_prev_rsi_fast") >= F.col("_prev_rsi_slow")))
                          
        df = df.withColumn("_cross_buy_level", 
                          (F.col(rsi_fast_col) > rsi_buy_level) & 
                          (F.col("_prev_rsi_fast") <= rsi_buy_level))
                          
        df = df.withColumn("_cross_sell_level", 
                          (F.col(rsi_fast_col) < rsi_sell_level) & 
                          (F.col("_prev_rsi_fast") >= rsi_sell_level))
                          
        df = df.withColumn(buy_col,
                          F.when(
                              F.col(trend_col).isin(["Bull", "Neutral"]) &
                              (F.col("_crossover_up") | F.col("_cross_buy_level")), 1
                          ).otherwise(0))
                          
        df = df.withColumn(sell_col,
                          F.when(
                              F.col(trend_col).isin(["Bear", "Neutral"]) &
                              (F.col("_crossover_down") | F.col("_cross_sell_level")), 1
                          ).otherwise(0))
                          
        df = df.drop("_prev_rsi_fast", "_prev_rsi_slow", "_crossover_up", 
                    "_crossover_down", "_cross_buy_level", "_cross_sell_level")
        
    return df


# ==============================================================================
# SIGNAL GENERATION - REVERSAL STRATEGY (Price or None)
# ==============================================================================

def generate_reversal_signals(
    df: Union[PolarsFrame, SparkFrame],
    rsi_fast_col: str = "rsi_close_fast",
    rsi_slow_col: str = "rsi_close_slow",
    trend_col: str = "ha_trend",
    rsi_buy_level: float = 30.0,
    rsi_sell_level: float = 70.0,
    lookback_bars: int = 3,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    output_prefix: str = "",
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """
    Generate signals based on trend reversals.
    Returns: Price at signal or None
    """
    logger.info(f"Generating reversal signals (prefix='{output_prefix}')")
    
    buy_col = f"{output_prefix}buy_signal_reversal"
    sell_col = f"{output_prefix}sell_signal_reversal"
    
    if engine == "polars":
        df = df.with_columns([
            ((pl.col(trend_col).shift(1).over(partition_cols, order_by="timestamp") == "Bear") &
             (pl.col(trend_col) == "Bull")).alias("_trend_reversal_buy"),
            
            ((pl.col(trend_col).shift(1).over(partition_cols, order_by="timestamp") == "Bull") &
             (pl.col(trend_col) == "Bear")).alias("_trend_reversal_sell")
        ])
        
        rsi_buy_conditions = [pl.col(rsi_slow_col) <= rsi_buy_level]
        rsi_sell_conditions = [pl.col(rsi_slow_col) >= rsi_sell_level]
        
        for i in range(1, lookback_bars + 1):
            rsi_buy_conditions.append(
                pl.col(rsi_slow_col).shift(i).over(partition_cols, order_by="timestamp") <= rsi_buy_level
            )
            rsi_sell_conditions.append(
                pl.col(rsi_slow_col).shift(i).over(partition_cols, order_by="timestamp") >= rsi_sell_level
            )
        
        rsi_buy_confirmed = pl.any_horizontal(rsi_buy_conditions)
        rsi_sell_confirmed = pl.any_horizontal(rsi_sell_conditions)
        
        df = df.with_columns([
            pl.when(pl.col("_trend_reversal_buy") & rsi_buy_confirmed)
              .then(pl.col("close"))
              .otherwise(None)
              .alias(buy_col),
            
            pl.when(pl.col("_trend_reversal_sell") & rsi_sell_confirmed)
              .then(pl.col("close"))
              .otherwise(None)
              .alias(sell_col)
        ])
        
        df = df.drop(["_trend_reversal_buy", "_trend_reversal_sell"])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp")
        
        df = df.withColumn("_trend_reversal_buy",
                          (F.lag(trend_col, 1).over(window_spec) == "Bear") &
                          (F.col(trend_col) == "Bull"))
        df = df.withColumn("_trend_reversal_sell",
                          (F.lag(trend_col, 1).over(window_spec) == "Bull") &
                          (F.col(trend_col) == "Bear"))
        
        rsi_buy_condition = F.col(rsi_slow_col) <= rsi_buy_level
        rsi_sell_condition = F.col(rsi_slow_col) >= rsi_sell_level
        
        for i in range(1, lookback_bars + 1):
            rsi_buy_condition = rsi_buy_condition | (F.lag(rsi_slow_col, i).over(window_spec) <= rsi_buy_level)
            rsi_sell_condition = rsi_sell_condition | (F.lag(rsi_slow_col, i).over(window_spec) >= rsi_sell_level)
        
        df = df.withColumn(buy_col,
                          F.when(F.col("_trend_reversal_buy") & rsi_buy_condition, F.col("close"))
                           .otherwise(None))
        df = df.withColumn(sell_col,
                          F.when(F.col("_trend_reversal_sell") & rsi_sell_condition, F.col("close"))
                           .otherwise(None))
        
        df = df.drop("_trend_reversal_buy", "_trend_reversal_sell")
    
    return df


# ==============================================================================
# VOLUME CONFIRMATION (Both versions)
# ==============================================================================

def add_volume_confirmation_binary(
    df: Union[PolarsFrame, SparkFrame],
    buy_signal_col: str = "buy_signal_crossover",
    sell_signal_col: str = "sell_signal_crossover",
    volume_spike_col: str = "volume_spike",
    output_prefix: str = "",
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """Add volume confirmation for binary (0/1) signals."""
    logger.info(f"Adding volume confirmation for binary signals (prefix='{output_prefix}')")
    
    buy_conf_col = f"{output_prefix}buy_confirmed_volume"
    sell_conf_col = f"{output_prefix}sell_confirmed_volume"
    
    if engine == "polars":
        df = df.with_columns([
            pl.when((pl.col(buy_signal_col) == 1) & (pl.col(volume_spike_col) == 1))
              .then(pl.lit(1)).otherwise(pl.lit(0)).alias(buy_conf_col),
              
            pl.when((pl.col(sell_signal_col) == 1) & (pl.col(volume_spike_col) == 1))
              .then(pl.lit(1)).otherwise(pl.lit(0)).alias(sell_conf_col)
        ])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        
        df = df.withColumn(buy_conf_col,
                          F.when((F.col(buy_signal_col) == 1) & (F.col(volume_spike_col) == 1), 1).otherwise(0))
                          
        df = df.withColumn(sell_conf_col,
                          F.when((F.col(sell_signal_col) == 1) & (F.col(volume_spike_col) == 1), 1).otherwise(0))
                          
    return df


def add_volume_confirmation_price(
    df: Union[PolarsFrame, SparkFrame],
    buy_signal_col: str = "buy_signal_reversal",
    sell_signal_col: str = "sell_signal_reversal",
    volume_spike_col: str = "volume_spike",
    output_prefix: str = "",
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """Add volume confirmation for price-based signals."""
    logger.info(f"Adding volume confirmation for price signals (prefix='{output_prefix}')")
    
    buy_conf_col = f"{output_prefix}buy_confirmed_volume"
    sell_conf_col = f"{output_prefix}sell_confirmed_volume"
    
    if engine == "polars":
        df = df.with_columns([
            (pl.col(buy_signal_col).is_not_null() & pl.col(volume_spike_col))
              .alias(buy_conf_col),
            
            (pl.col(sell_signal_col).is_not_null() & pl.col(volume_spike_col))
              .alias(sell_conf_col)
        ])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        
        df = df.withColumn(buy_conf_col,
                          F.col(buy_signal_col).isNotNull() & F.col(volume_spike_col))
        df = df.withColumn(sell_conf_col,
                          F.col(sell_signal_col).isNotNull() & F.col(volume_spike_col))
    
    return df


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_dynamic_lookback(timeframe: str, lookback_config: dict) -> int:
    """Get lookback period based on timeframe."""
    tf = timeframe.lower()
    
    category = "intraday"
    if tf in ["1d", "3d", "1w"]:
        category = "swing"
    elif tf in ["1m", "3m", "1y"]:
        category = "position"
        
    return lookback_config.get(category, lookback_config.get('default', 14))


def add_dynamic_lookback_column(
    df: Union[PolarsFrame, SparkFrame],
    lookback_config: dict,
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """Add dynamic lookback column based on timeframe."""
    logger.info("Adding dynamic lookback column")
    
    mapping = {
        "intraday": lookback_config.get("intraday", 14),
        "swing": lookback_config.get("swing", 14),
        "position": lookback_config.get("position", 14),
        "default": lookback_config.get("default", 14)
    }
    
    if engine == "polars":
        df = df.with_columns([
            pl.when(pl.col("canonical_timeframe").is_in(["1d", "3d", "1w"]))
              .then(pl.lit(mapping["swing"]))
              .when(pl.col("canonical_timeframe").is_in(["1m", "3m", "1y"]))
              .then(pl.lit(mapping["position"]))
              .otherwise(pl.lit(mapping["intraday"]))
              .alias("dynamic_lookback")
        ])
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        df = df.withColumn("dynamic_lookback",
                          F.when(F.col("canonical_timeframe").isin(["1d", "3d", "1w"]), mapping["swing"])
                           .when(F.col("canonical_timeframe").isin(["1m", "3m", "1y"]), mapping["position"])
                           .otherwise(mapping["intraday"]))
                           
    return df


def generate_signals_with_dynamic_lookback(
    df: Union[PolarsFrame, SparkFrame],
    config: dict,
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """
    Generate signals using dynamic lookback periods.
    
    Args:
        df: Input DataFrame
        config: Configuration dict with lookback periods
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with dynamic_lookback column
    """
    return add_dynamic_lookback_column(df, config, engine)