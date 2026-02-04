"""
Signal Processor Module - MERGED VERSION
=========================================
Combines both crossover and reversal strategies with clear naming.

Strategies:
1. Crossover Strategy: RSI fast/slow crossovers (binary signals)
2. Reversal Strategy: Trend reversals with RSI confirmation (price signals)
"""

from typing import Union, List, Optional, Any
from loguru import logger
import polars as pl

# Type hints
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
SparkFrame = Any


# ==============================================================================
# DIVERGENCE DETECTION (Two versions)
# ==============================================================================

def detect_price_volume_divergence_sma(
    df: Union[PolarsFrame, SparkFrame],
    lookback: int = 5,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """
    Detect divergence using volume SMA changes.
    
    Returns:
        1 = Bullish (price down, volume up)
        -1 = Bearish (price up, volume down)
        0 = None
    
    Requires: volume_sma column
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
    
    Returns:
        'bullish' = Price falling + Volume declining
        'bearish' = Price rising + Volume declining
        None = No divergence
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
    
    Strategy:
    - Buy: RSI fast crosses above RSI slow OR crosses above buy level
    - Sell: RSI fast crosses below RSI slow OR crosses below sell level
    
    Returns: Binary signals (1 = signal, 0 = no signal)
    
    Output columns:
    - {prefix}buy_signal_crossover
    - {prefix}sell_signal_crossover
    """
    logger.info(f"Generating crossover signals (prefix='{output_prefix}')")
    
    buy_col = f"{output_prefix}buy_signal_crossover"
    sell_col = f"{output_prefix}sell_signal_crossover"
    
    if engine == "polars":
        # Crossovers
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
        
        # Signals
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
    
    Strategy:
    - Buy: Trend changes Bear→Bull + RSI was oversold in last N bars
    - Sell: Trend changes Bull→Bear + RSI was overbought in last N bars
    
    Returns: Price at signal or None
    
    Output columns:
    - {prefix}buy_signal_reversal (price or None)
    - {prefix}sell_signal_reversal (price or None)
    """
    logger.info(f"Generating reversal signals (prefix='{output_prefix}')")
    
    buy_col = f"{output_prefix}buy_signal_reversal"
    sell_col = f"{output_prefix}sell_signal_reversal"
    
    if engine == "polars":
        # Trend reversal detection
        df = df.with_columns([
            ((pl.col(trend_col).shift(1).over(partition_cols, order_by="timestamp") == "Bear") &
             (pl.col(trend_col) == "Bull")).alias("_trend_reversal_buy"),
            
            ((pl.col(trend_col).shift(1).over(partition_cols, order_by="timestamp") == "Bull") &
             (pl.col(trend_col) == "Bear")).alias("_trend_reversal_sell")
        ])
        
        # RSI confirmation (check current + last N bars)
        rsi_buy_conditions = [pl.col(rsi_slow_col) <= rsi_buy_level]
        rsi_sell_conditions = [pl.col(rsi_slow_col) >= rsi_sell_level]
        
        for i in range(1, lookback_bars + 1):
            rsi_buy_conditions.append(
                pl.col(rsi_slow_col).shift(i).over(partition_cols, order_by="timestamp") <= rsi_buy_level
            )
            rsi_sell_conditions.append(
                pl.col(rsi_slow_col).shift(i).over(partition_cols, order_by="timestamp") >= rsi_sell_level
            )
        
        # Combine all RSI conditions with OR
        rsi_buy_confirmed = pl.any_horizontal(rsi_buy_conditions)
        rsi_sell_confirmed = pl.any_horizontal(rsi_sell_conditions)
        
        # Generate signals
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
# VOLUME CONFIRMATION (Two versions for different signal types)
# ==============================================================================

def add_volume_confirmation_binary(
    df: Union[PolarsFrame, SparkFrame],
    buy_signal_col: str = "buy_signal_crossover",
    sell_signal_col: str = "sell_signal_crossover",
    volume_spike_col: str = "volume_spike",
    output_prefix: str = "",
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """
    Add volume confirmation for binary (0/1) signals.
    
    Works with crossover signals.
    
    Output: 1 if signal==1 AND volume_spike==1, else 0
    """
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
    """
    Add volume confirmation for price-based signals.
    
    Works with reversal signals (price or None).
    
    Output: True if signal is not None AND volume_spike, else False
    """
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
# USAGE EXAMPLES
# ==============================================================================

"""
# Example 1: Use crossover strategy (binary signals)
df = calculate_rsi(df, column="close", period=7, output_name="rsi_close_fast")
df = calculate_rsi(df, column="close", period=11, output_name="rsi_close_slow")
df = generate_crossover_signals(df, output_prefix="std_")
df = add_volume_confirmation_binary(df, 
    buy_signal_col="std_buy_signal_crossover",
    sell_signal_col="std_sell_signal_crossover")

# Result columns:
# - std_buy_signal_crossover (0 or 1)
# - std_sell_signal_crossover (0 or 1)
# - std_buy_confirmed_volume (0 or 1)
# - std_sell_confirmed_volume (0 or 1)


# Example 2: Use reversal strategy (price signals)
df = calculate_rsi(df, column="ha_close", period=7, output_name="rsi_ha_close_fast")
df = calculate_rsi(df, column="ha_close", period=11, output_name="rsi_ha_close_slow")
df = generate_reversal_signals(df, 
    rsi_slow_col="rsi_ha_close_slow",
    trend_col="ha_trend",
    output_prefix="ha_")
df = add_volume_confirmation_price(df,
    buy_signal_col="ha_buy_signal_reversal",
    sell_signal_col="ha_sell_signal_reversal")

# Result columns:
# - ha_buy_signal_reversal (price or None)
# - ha_sell_signal_reversal (price or None)
# - ha_buy_confirmed_volume (True or False)
# - ha_sell_confirmed_volume (True or False)


# Example 3: Use both strategies simultaneously
df = generate_crossover_signals(df, output_prefix="cross_")
df = generate_reversal_signals(df, output_prefix="rev_")
df = add_volume_confirmation_binary(df, 
    buy_signal_col="cross_buy_signal_crossover",
    sell_signal_col="cross_sell_signal_crossover",
    output_prefix="cross_")
df = add_volume_confirmation_price(df,
    buy_signal_col="rev_buy_signal_reversal",
    sell_signal_col="rev_sell_signal_reversal",
    output_prefix="rev_")

# Result: Both strategies available for comparison
"""