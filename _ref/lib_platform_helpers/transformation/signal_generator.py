"""
New Signal Generation Functions - FIXED
========================================
RSI crossovers, Heikin-Ashi patterns, and volume analysis with timeframe profiles.
"""

from typing import Union, List, Optional, Dict, Any, Callable
import polars as pl
import re
from loguru import logger

# Type hints
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
SparkFrame = Any


def determine_profile_settings_from_timeframe(
    timeframe: str,
    profiles_config: Optional[Dict[str, Any]] = None,
    default_settings: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Determine settings based on timeframe profile.
    
    Args:
        timeframe: Timeframe string (e.g., "5m", "1h", "1d")
        profiles_config: Configuration dict with timeframe profiles
        default_settings: Fallback settings if no pattern matches
        
    Returns:
        Dict with settings (lookback, levels, etc.)
        
    Example:
        profiles = {
            "minute": {
                "patterns": ["m", "min"],
                "max_value": 59,
                "buy_level": 25.0,
                "sell_level": 75.0,
                "lookback_obos": 3
            },
            "default": {"buy_level": 30.0, "sell_level": 70.0, "lookback_obos": 4}
        }
        settings = determine_profile_settings_from_timeframe("5m", profiles)
    """
    if default_settings is None:
        default_settings = {}
        
    if profiles_config is None or not profiles_config:
        return default_settings
    
    timeframe_lower = timeframe.lower().strip()
    
    # Extract numeric value and unit
    match = re.match(r'(\d+)\s*([a-z]+)', timeframe_lower)
    if not match:
        return profiles_config.get("default", default_settings)
    
    value, unit = match.groups()
    value = int(value)
    
    # Check each profile
    for profile_name, profile in profiles_config.items():
        if profile_name == "default":
            continue
            
        if isinstance(profile, dict) and "patterns" in profile:
            patterns = profile["patterns"]
            max_value = profile.get("max_value")
            
            # Check if unit matches any pattern
            if any(pattern in unit for pattern in patterns):
                if max_value is not None:
                    if value <= max_value:
                        # Ensure numeric types
                        return _cast_profile_types(profile)
                else:
                    # Ensure numeric types
                    return _cast_profile_types(profile)
    
    # Ensure numeric types for default
    default = profiles_config.get("default", default_settings)
    return _cast_profile_types(default) if default else default_settings


def _cast_profile_types(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Cast profile settings to correct types."""
    typed = {}
    for k, v in profile.items():
        if v is None or v == "":
            typed[k] = v
        elif k in ['buy_level', 'sell_level', 'spike_multiplier']:
            typed[k] = float(v)
        elif k in ['lookback_obos', 'rsi_fast_period', 'rsi_slow_period', 'sma_period', 'max_value']:
            typed[k] = int(v)
        else:
            typed[k] = v
    return typed


def _apply_with_timeframe_profiles(
    df: PolarsFrame,
    apply_func: Callable,
    timeframe_profiles: Optional[Dict[str, Any]],
    default_settings: Dict[str, Any],
    partition_cols: List[str]
) -> PolarsFrame:
    """
    Helper function to apply logic with timeframe-specific settings.
    
    Args:
        df: Input DataFrame
        apply_func: Function to apply to each chunk (receives chunk and settings dict)
        timeframe_profiles: Optional timeframe profile configuration
        default_settings: Default settings to use if no profiles
        partition_cols: Columns to partition by
        
    Returns:
        Processed DataFrame
    """
    df = df.sort(partition_cols + ["timestamp"])
    
    if timeframe_profiles is not None and "canonical_timeframe" in df.columns:
        timeframes = df["canonical_timeframe"].unique().to_list()
        chunks = []
        
        for tf in timeframes:
            # Get settings for this timeframe
            settings = determine_profile_settings_from_timeframe(
                tf, timeframe_profiles, default_settings
            )
            
            # Filter and process
            tf_chunk = df.filter(pl.col("canonical_timeframe") == tf)
            processed_chunk = apply_func(tf_chunk, settings)
            chunks.append(processed_chunk)
        
        # Recombine
        if chunks:
            df = pl.concat(chunks).sort(partition_cols + ["timestamp"])
    else:
        # Standard processing
        df = apply_func(df, default_settings)
    
    return df


def generate_rsi_crossover_signals(
    df: PolarsFrame,
    rsi_fast_col: str = "rsi_close_fast",
    rsi_slow_col: str = "rsi_close_slow",
    rsi_buy_level: float = 30.0,
    rsi_sell_level: float = 70.0,
    lookback_obos: int = 4,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    timeframe_profiles: Optional[Dict[str, Any]] = None,
    engine: str = "polars"
) -> PolarsFrame:
    """
    Generate RSI crossover buy/sell signals with OB/OS filtering.
    
    Signals generated:
    - rsi_crossover_buy: 1 if fast crosses above slow, 0 otherwise
    - rsi_crossover_sell: 1 if fast crosses below slow, 0 otherwise
    - rsi_crossover_buy_obos: Buy signal filtered by oversold condition
    - rsi_crossover_sell_obos: Sell signal filtered by overbought condition
    
    Args:
        df: Input DataFrame (must have rsi_fast_col and rsi_slow_col columns)
        rsi_fast_col: Fast RSI column name
        rsi_slow_col: Slow RSI column name
        rsi_buy_level: Oversold threshold (default: 30.0)
        rsi_sell_level: Overbought threshold (default: 70.0)
        lookback_obos: Periods to check for OB/OS condition (default: 4)
        partition_cols: Columns to partition by
        timeframe_profiles: Optional dict with timeframe-specific settings
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with RSI crossover signal columns added
        
    Example:
        df = generate_rsi_crossover_signals(
            df,
            timeframe_profiles={
                "minute": {
                    "patterns": ["m"],
                    "max_value": 59,
                    "buy_level": 25.0,
                    "sell_level": 75.0,
                    "lookback_obos": 3
                },
                "default": {"buy_level": 30.0, "sell_level": 70.0, "lookback_obos": 4}
            }
        )
    """
    logger.info("Generating RSI crossover signals")
    
    # Ensure numeric types for parameters
    rsi_buy_level = float(rsi_buy_level)
    rsi_sell_level = float(rsi_sell_level)
    lookback_obos = int(lookback_obos)
    
    # Validate required columns
    required_cols = [rsi_fast_col, rsi_slow_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    if engine == "polars":
        def apply_rsi_logic(chunk: PolarsFrame, settings: Dict[str, Any]) -> PolarsFrame:
            # Ensure numeric types
            buy_lvl = float(settings.get("buy_level", rsi_buy_level))
            sell_lvl = float(settings.get("sell_level", rsi_sell_level))
            lb = int(settings.get("lookback_obos", lookback_obos))
            
            # Get previous RSI values
            chunk = chunk.with_columns([
                pl.col(rsi_fast_col).shift(1).over(partition_cols, order_by="timestamp").alias("_prev_rsi_fast"),
                pl.col(rsi_slow_col).shift(1).over(partition_cols, order_by="timestamp").alias("_prev_rsi_slow")
            ])
            
            # Base crossover signals
            chunk = chunk.with_columns([
                ((pl.col(rsi_fast_col) > pl.col(rsi_slow_col)) & 
                 (pl.col("_prev_rsi_fast") <= pl.col("_prev_rsi_slow")))
                .cast(pl.Int8).alias("rsi_crossover_buy"),
                
                ((pl.col(rsi_fast_col) < pl.col(rsi_slow_col)) & 
                 (pl.col("_prev_rsi_fast") >= pl.col("_prev_rsi_slow")))
                .cast(pl.Int8).alias("rsi_crossover_sell")
            ])
            
            # OPTIMIZED: Check OB/OS conditions using rolling_min/rolling_max
            # Instead of: for i in range(lb): shift(i).over(...) [creates N shift operations]
            # Use: rolling_min <= threshold in single operation
            
            chunk = chunk.with_columns([
                pl.col(rsi_slow_col)
                  .rolling_min(window_size=lb)
                  .over(partition_cols, order_by="timestamp")
                  .le(pl.lit(buy_lvl))
                  .alias("_rsi_in_oversold"),
                  
                pl.col(rsi_slow_col)
                  .rolling_max(window_size=lb)
                  .over(partition_cols, order_by="timestamp")
                  .ge(pl.lit(sell_lvl))
                  .alias("_rsi_in_overbought")
            ])
            
            # OB/OS filtered signals
            chunk = chunk.with_columns([
                (pl.col("rsi_crossover_buy") & pl.col("_rsi_in_oversold")).cast(pl.Int8).alias("rsi_crossover_buy_obos"),
                (pl.col("rsi_crossover_sell") & pl.col("_rsi_in_overbought")).cast(pl.Int8).alias("rsi_crossover_sell_obos")
            ])
            
            return chunk.drop(["_prev_rsi_fast", "_prev_rsi_slow", "_rsi_in_oversold", "_rsi_in_overbought"])
        
        # Apply with timeframe profiles
        default_settings = {
            "buy_level": rsi_buy_level,
            "sell_level": rsi_sell_level,
            "lookback_obos": lookback_obos
        }
        
        df = _apply_with_timeframe_profiles(
            df, apply_rsi_logic, timeframe_profiles, default_settings, partition_cols
        )
        
    elif engine == "pyspark":
        raise NotImplementedError("PySpark engine not yet implemented for RSI crossover signals")
    else:
        raise ValueError(f"Unknown engine: {engine}")
    
    return df


def generate_ha_signals(
    df: PolarsFrame,
    rsi_slow_col: str = "rsi_close_slow",
    rsi_buy_level: float = 30.0,
    rsi_sell_level: float = 70.0,
    lookback_obos: int = 4,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    timeframe_profiles: Optional[Dict[str, Any]] = None,
    engine: str = "polars"
) -> PolarsFrame:
    """
    Generate Heikin-Ashi pattern signals with OB/OS filtering.
    
    Signals generated:
    - ha_trend: "Bull", "Bear", or "Neutral"
    - ha_buy_signal: 1 if ha_close > ha_open AND ha_close > prior ha_close
    - ha_sell_signal: 1 if ha_close < ha_open AND ha_close < prior ha_close
    - ha_buy_signal_obos: Buy signal filtered by oversold condition
    - ha_sell_signal_obos: Sell signal filtered by overbought condition
    
    Args:
        df: Input DataFrame (must have ha_open, ha_close columns)
        rsi_slow_col: Slow RSI column for OB/OS filtering
        rsi_buy_level: Oversold threshold (default: 30.0)
        rsi_sell_level: Overbought threshold (default: 70.0)
        lookback_obos: Periods to check for OB/OS condition (default: 4)
        partition_cols: Columns to partition by
        timeframe_profiles: Optional dict with timeframe-specific settings
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with HA signal columns added
    """
    logger.info("Generating Heikin-Ashi signals")
    
    # Ensure numeric types for parameters
    rsi_buy_level = float(rsi_buy_level)
    rsi_sell_level = float(rsi_sell_level)
    lookback_obos = int(lookback_obos)
    
    # Validate required columns
    required_cols = ["ha_open", "ha_close", rsi_slow_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    if engine == "polars":
        def apply_ha_logic(chunk: PolarsFrame, settings: Dict[str, Any]) -> PolarsFrame:
            # Ensure numeric types
            buy_lvl = float(settings.get("buy_level", rsi_buy_level))
            sell_lvl = float(settings.get("sell_level", rsi_sell_level))
            lb = int(settings.get("lookback_obos", lookback_obos))
            
            # Get previous ha_close
            chunk = chunk.with_columns([
                pl.col("ha_close").shift(1).over(partition_cols, order_by="timestamp").alias("_prev_ha_close")
            ])
            
            # HA Trend
            chunk = chunk.with_columns([
                pl.when(pl.col("ha_close") > pl.col("ha_open"))
                  .then(pl.lit("Bull"))
                  .when(pl.col("ha_close") < pl.col("ha_open"))
                  .then(pl.lit("Bear"))
                  .otherwise(pl.lit("Neutral"))
                  .alias("ha_trend")
            ])
            
            # Base HA signals
            chunk = chunk.with_columns([
                ((pl.col("ha_close") > pl.col("ha_open")) & 
                 (pl.col("ha_close") > pl.col("_prev_ha_close")))
                .cast(pl.Int8).alias("ha_buy_signal"),
                
                ((pl.col("ha_close") < pl.col("ha_open")) & 
                 (pl.col("ha_close") < pl.col("_prev_ha_close")))
                .cast(pl.Int8).alias("ha_sell_signal")
            ])
            
            # Check OB/OS conditions
            obos_buy_conditions = []
            obos_sell_conditions = []
            
            for i in range(lb):
                obos_buy_conditions.append(
                    pl.col(rsi_slow_col).shift(i).over(partition_cols, order_by="timestamp") <= pl.lit(buy_lvl)
                )
                obos_sell_conditions.append(
                    pl.col(rsi_slow_col).shift(i).over(partition_cols, order_by="timestamp") >= pl.lit(sell_lvl)
                )
            
            # Combine conditions
            obos_buy_check = pl.any_horizontal(obos_buy_conditions) if obos_buy_conditions else pl.lit(False)
            obos_sell_check = pl.any_horizontal(obos_sell_conditions) if obos_sell_conditions else pl.lit(False)
            
            # OB/OS filtered signals
            chunk = chunk.with_columns([
                (pl.col("ha_buy_signal") & obos_buy_check).cast(pl.Int8).alias("ha_buy_signal_obos"),
                (pl.col("ha_sell_signal") & obos_sell_check).cast(pl.Int8).alias("ha_sell_signal_obos")
            ])
            
            return chunk.drop(["_prev_ha_close"])
        
        # Apply with timeframe profiles
        default_settings = {
            "buy_level": rsi_buy_level,
            "sell_level": rsi_sell_level,
            "lookback_obos": lookback_obos
        }
        
        df = _apply_with_timeframe_profiles(
            df, apply_ha_logic, timeframe_profiles, default_settings, partition_cols
        )
        
    elif engine == "pyspark":
        raise NotImplementedError("PySpark engine not yet implemented for HA signals")
    else:
        raise ValueError(f"Unknown engine: {engine}")
    
    return df


def generate_volume_signals(
    df: PolarsFrame,
    rsi_slow_col: str = "rsi_close_slow",
    rsi_buy_level: float = 30.0,
    rsi_sell_level: float = 70.0,
    volume_sma_period: int = 20,
    volume_spike_multiplier: float = 1.5,
    lookback_obos: int = 4,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    timeframe_profiles: Optional[Dict[str, Any]] = None,
    engine: str = "polars"
) -> PolarsFrame:
    """
    Generate volume-based signals with OB/OS filtering.
    
    Columns created:
    - avg_volume_{period}: Rolling average of volume
    - vol_buy_signal: 1 if volume spike AND close > open
    - vol_sell_signal: 1 if volume spike AND close < open
    - vol_buy_signal_obos: Buy signal filtered by oversold condition
    - vol_sell_signal_obos: Sell signal filtered by overbought condition
    
    Args:
        df: Input DataFrame (must have volume, open, close columns)
        rsi_slow_col: Slow RSI column for OB/OS filtering
        rsi_buy_level: Oversold threshold (default: 30.0)
        rsi_sell_level: Overbought threshold (default: 70.0)
        volume_sma_period: Period for volume SMA (default: 20)
        volume_spike_multiplier: Threshold multiplier (default: 1.5)
        lookback_obos: Periods to check for OB/OS condition (default: 4)
        partition_cols: Columns to partition by
        timeframe_profiles: Optional dict with timeframe-specific settings
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with volume signal columns added
    """
    logger.info("Generating volume signals")
    
    # Ensure numeric types for parameters
    rsi_buy_level = float(rsi_buy_level)
    rsi_sell_level = float(rsi_sell_level)
    volume_sma_period = int(volume_sma_period)
    volume_spike_multiplier = float(volume_spike_multiplier)
    lookback_obos = int(lookback_obos)
    
    # Validate required columns
    required_cols = ["volume", "open", "close", rsi_slow_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    if engine == "polars":
        def apply_vol_logic(chunk: PolarsFrame, settings: Dict[str, Any]) -> PolarsFrame:
            # Ensure numeric types
            buy_lvl = float(settings.get("buy_level", rsi_buy_level))
            sell_lvl = float(settings.get("sell_level", rsi_sell_level))
            lb = int(settings.get("lookback_obos", lookback_obos))
            
            # Calculate average volume with dynamic column name
            avg_vol_col = f"avg_volume_{volume_sma_period}"
            chunk = chunk.with_columns([
                pl.col("volume")
                  .rolling_mean(window_size=volume_sma_period)
                  .over(partition_cols, order_by="timestamp")
                  .alias(avg_vol_col)
            ])
            
            # Base volume signals
            chunk = chunk.with_columns([
                ((pl.col("volume") > (pl.col(avg_vol_col) * volume_spike_multiplier)) & 
                 (pl.col("close") > pl.col("open")))
                .cast(pl.Int8).alias("vol_buy_signal"),
                
                ((pl.col("volume") > (pl.col(avg_vol_col) * volume_spike_multiplier)) & 
                 (pl.col("close") < pl.col("open")))
                .cast(pl.Int8).alias("vol_sell_signal")
            ])
            
            # Check OB/OS conditions
            obos_buy_conditions = []
            obos_sell_conditions = []
            
            for i in range(lb):
                obos_buy_conditions.append(
                    pl.col(rsi_slow_col).shift(i).over(partition_cols, order_by="timestamp") <= pl.lit(buy_lvl)
                )
                obos_sell_conditions.append(
                    pl.col(rsi_slow_col).shift(i).over(partition_cols, order_by="timestamp") >= pl.lit(sell_lvl)
                )
            
            # Combine conditions
            obos_buy_check = pl.any_horizontal(obos_buy_conditions) if obos_buy_conditions else pl.lit(False)
            obos_sell_check = pl.any_horizontal(obos_sell_conditions) if obos_sell_conditions else pl.lit(False)
            
            # OB/OS filtered signals
            chunk = chunk.with_columns([
                (pl.col("vol_buy_signal") & obos_buy_check).cast(pl.Int8).alias("vol_buy_signal_obos"),
                (pl.col("vol_sell_signal") & obos_sell_check).cast(pl.Int8).alias("vol_sell_signal_obos")
            ])
            
            return chunk
        
        # Apply with timeframe profiles
        default_settings = {
            "buy_level": rsi_buy_level,
            "sell_level": rsi_sell_level,
            "lookback_obos": lookback_obos
        }
        
        df = _apply_with_timeframe_profiles(
            df, apply_vol_logic, timeframe_profiles, default_settings, partition_cols
        )
        
    elif engine == "pyspark":
        raise NotImplementedError("PySpark engine not yet implemented for volume signals")
    else:
        raise ValueError(f"Unknown engine: {engine}")
    
    return df