"""
Signal Processor Module - POLARS NATIVE RSI (No pandas_ta)
===========================================================
Pure Polars implementation for maximum performance and no pandas conversions.
All indicators remain in Polars for performance.
"""

from typing import Union, List, Optional, Any, Dict
from loguru import logger
import polars as pl

# Type hints
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
SparkFrame = Any


def calculate_rsi_polars(
    prices: pl.Series,
    period: int = 14
) -> pl.Series:
    """
    Pure Polars RSI calculation (no pandas conversion).
    
    RSI = 100 - (100 / (1 + RS))
    where RS = Average Gain / Average Loss
    
    Args:
        prices: Series of prices
        period: Lookback period for RSI
        
    Returns:
        Series with RSI values
    """
    if len(prices) < period + 1:
        return pl.Series([None] * len(prices), dtype=pl.Float64)
    
    # Calculate price changes
    deltas = prices.diff()
    
    # Separate gains and losses
    gains = deltas.clip(lower_bound=0)
    losses = (-deltas).clip(lower_bound=0)
    
    # Calculate moving averages
    avg_gain = gains.rolling_mean(window_size=period)
    avg_loss = losses.rolling_mean(window_size=period)
    
    # Calculate RS and RSI
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def calculate_rsi(
    df: Union[PolarsFrame, SparkFrame],
    column: str = "close",
    period: int = 14,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    output_name: Optional[str] = None,
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """
    Calculate RSI using pure Polars (no pandas conversion).
    
    Args:
        df: Input DataFrame
        column: Column to calculate RSI on
        period: Lookback period for RSI
        partition_cols: Columns to partition by
        output_name: Name for output column (default: f"rsi_{column}_{period}")
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with RSI column added
    """
    if output_name is None:
        output_name = f"rsi_{column}_{period}"
    
    logger.info(f"Calculating RSI for {column} (period={period}) using PURE POLARS (no pandas)")
    
    if engine == "polars":
        # Check if column exists
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        
        # If DataFrame is empty, return as-is with new column as null
        if df.is_empty():
            return df.with_columns(pl.lit(None).cast(pl.Float64).alias(output_name))
        
        # Sort by partition columns and timestamp for proper windowing
        df = df.sort(partition_cols + ["timestamp"])
        
        # Step-by-step RSI calculation (Polars 1.x compatible)
        # Step 1: Calculate delta
        df = df.with_columns(
            pl.col(column).diff().over(partition_cols).alias("_delta")
        )
        
        # Step 2: Calculate gains and losses
        df = df.with_columns([
            pl.when(pl.col("_delta") > 0).then(pl.col("_delta")).otherwise(0.0).alias("_gain"),
            pl.when(pl.col("_delta") < 0).then(-pl.col("_delta")).otherwise(0.0).alias("_loss")
        ])
        
        # Step 3: Calculate rolling averages
        df = df.with_columns([
            pl.col("_gain").rolling_mean(window_size=period).over(partition_cols).alias("_avg_gain"),
            pl.col("_loss").rolling_mean(window_size=period).over(partition_cols).alias("_avg_loss")
        ])
        
        # Step 4: Calculate RSI
        df = df.with_columns(
            (100 - (100 / (1 + pl.col("_avg_gain") / pl.col("_avg_loss")))).alias(output_name)
        )
        
        # Step 5: Drop intermediate columns
        df = df.drop(["_delta", "_gain", "_loss", "_avg_gain", "_avg_loss"])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
        # Calculate deltas
        deltas = F.col(column) - F.lag(column, 1).over(window_spec)
        
        # Gains and losses
        gains = F.when(deltas > 0, deltas).otherwise(0)
        losses = F.when(deltas < 0, -deltas).otherwise(0)
        
        # Average gain/loss using window function
        avg_gain = F.avg(gains).over(window_spec)
        avg_loss = F.avg(losses).over(window_spec)
        
        # RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        df = df.withColumn(output_name, rsi)
    
    else:
        raise ValueError(f"Unknown engine: {engine}")
    
    return df


def calculate_rsi_batch(
    df: Union[PolarsFrame, SparkFrame],
    fields: List[Dict[str, Any]],
    fast_period: int = 7,
    slow_period: int = 11,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
) -> Union[PolarsFrame, SparkFrame]:
    """
    Calculate RSI for multiple fields using pure Polars (SUPER FAST - no pandas conversion).
    
    Calculates all RSI fields in a single Polars pass instead of iterating.
    
    Args:
        df: Input DataFrame
        fields: List of field configs, each with:
            - 'column': Column name to calculate RSI on
            - 'prefix': Prefix for output columns (e.g., 'close' → 'rsi_close_fast', 'rsi_close_slow')
        fast_period: Fast RSI period (default: 7)
        slow_period: Slow RSI period (default: 11)
        partition_cols: Columns to partition by
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with all RSI columns added
        
    Performance Notes:
        - Pure Polars implementation: No pandas conversion!
        - All calculations done in native Polars
        - Expected speedup: 10-50x vs pandas_ta
        
    Example:
        fields = [
            {'column': 'close', 'prefix': 'close'},
            {'column': 'ha_close', 'prefix': 'ha_close'},
            {'column': 'volume', 'prefix': 'volume'}
        ]
        df = calculate_rsi_batch(df, fields, fast_period=7, slow_period=11)
        # Creates: rsi_close_fast, rsi_close_slow, rsi_ha_close_fast, rsi_ha_close_slow, etc.
    """
    logger.info(f"PURE POLARS: Batch calculating RSI for {len(fields)} fields (NO pandas conversion!)")
    
    if not fields:
        logger.warning("No fields provided for batch RSI calculation")
        return df
    
    if engine == "polars":
        # Validate all columns exist
        for field_config in fields:
            column = field_config['column']
            if column not in df.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame")
        
        if df.is_empty():
            # Add null columns for all RSI variants
            for field_config in fields:
                prefix = field_config['prefix']
                df = df.with_columns([
                    pl.lit(None).cast(pl.Float64).alias(f"rsi_{prefix}_fast"),
                    pl.lit(None).cast(pl.Float64).alias(f"rsi_{prefix}_slow")
                ])
            return df
        
        # Sort by partition columns and timestamp for proper windowing
        df = df.sort(partition_cols + ["timestamp"])
        
        # Step 1: Calculate deltas for all fields
        delta_cols = []
        for field_config in fields:
            column = field_config['column']
            prefix = field_config['prefix']
            delta_cols.append(
                pl.col(column).diff().over(partition_cols).alias(f"_delta_{prefix}")
            )
        df = df.with_columns(delta_cols)
        
        # Step 2: Calculate gains and losses
        gain_loss_cols = []
        for field_config in fields:
            prefix = field_config['prefix']
            gain_loss_cols.extend([
                pl.when(pl.col(f"_delta_{prefix}") > 0)
                  .then(pl.col(f"_delta_{prefix}"))
                  .otherwise(0.0)
                  .alias(f"_gain_{prefix}"),
                pl.when(pl.col(f"_delta_{prefix}") < 0)
                  .then(-pl.col(f"_delta_{prefix}"))
                  .otherwise(0.0)
                  .alias(f"_loss_{prefix}")
            ])
        df = df.with_columns(gain_loss_cols)
        
        # Step 3: Calculate rolling averages
        avg_cols = []
        for field_config in fields:
            prefix = field_config['prefix']
            avg_cols.extend([
                pl.col(f"_gain_{prefix}").rolling_mean(window_size=fast_period).over(partition_cols).alias(f"_avg_gain_{prefix}_fast"),
                pl.col(f"_loss_{prefix}").rolling_mean(window_size=fast_period).over(partition_cols).alias(f"_avg_loss_{prefix}_fast"),
                pl.col(f"_gain_{prefix}").rolling_mean(window_size=slow_period).over(partition_cols).alias(f"_avg_gain_{prefix}_slow"),
                pl.col(f"_loss_{prefix}").rolling_mean(window_size=slow_period).over(partition_cols).alias(f"_avg_loss_{prefix}_slow"),
            ])
        df = df.with_columns(avg_cols)
        
        # Step 4: Calculate RSI
        rsi_cols = []
        for field_config in fields:
            prefix = field_config['prefix']
            rsi_cols.extend([
                (100 - (100 / (1 + pl.col(f"_avg_gain_{prefix}_fast") / pl.col(f"_avg_loss_{prefix}_fast")))).alias(f"rsi_{prefix}_fast"),
                (100 - (100 / (1 + pl.col(f"_avg_gain_{prefix}_slow") / pl.col(f"_avg_loss_{prefix}_slow")))).alias(f"rsi_{prefix}_slow"),
            ])
        df = df.with_columns(rsi_cols)
        
        # Step 5: Drop intermediate columns
        drop_cols = []
        for field_config in fields:
            prefix = field_config['prefix']
            drop_cols.extend([
                f"_delta_{prefix}",
                f"_gain_{prefix}",
                f"_loss_{prefix}",
                f"_avg_gain_{prefix}_fast",
                f"_avg_loss_{prefix}_fast",
                f"_avg_gain_{prefix}_slow",
                f"_avg_loss_{prefix}_slow",
            ])
        df = df.drop(drop_cols)
        
        logger.info(f"Batch RSI calculation complete for {len(fields)} fields")
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
        for field_config in fields:
            column = field_config['column']
            prefix = field_config['prefix']
            
            # Calculate deltas
            deltas_fast = F.col(column) - F.lag(column, 1).over(window_spec)
            gains_fast = F.when(deltas_fast > 0, deltas_fast).otherwise(0)
            losses_fast = F.when(deltas_fast < 0, -deltas_fast).otherwise(0)
            
            avg_gain_fast = F.avg(gains_fast).over(window_spec)
            avg_loss_fast = F.avg(losses_fast).over(window_spec)
            rs_fast = avg_gain_fast / avg_loss_fast
            rsi_fast = 100 - (100 / (1 + rs_fast))
            
            df = df.withColumn(f"rsi_{prefix}_fast", rsi_fast)
            
            # Slow RSI
            deltas_slow = F.col(column) - F.lag(column, 1).over(window_spec)
            gains_slow = F.when(deltas_slow > 0, deltas_slow).otherwise(0)
            losses_slow = F.when(deltas_slow < 0, -deltas_slow).otherwise(0)
            
            avg_gain_slow = F.avg(gains_slow).over(window_spec)
            avg_loss_slow = F.avg(losses_slow).over(window_spec)
            rs_slow = avg_gain_slow / avg_loss_slow
            rsi_slow = 100 - (100 / (1 + rs_slow))
            
            df = df.withColumn(f"rsi_{prefix}_slow", rsi_slow)
    
    else:
        raise ValueError(f"Unknown engine: {engine}")
    
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
              .cast(pl.Int8)
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