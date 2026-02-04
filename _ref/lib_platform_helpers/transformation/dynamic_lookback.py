"""
Dynamic Lookback Determination
"""

from typing import Dict, Any
import re


def get_dynamic_lookback(timeframe: str, config: Dict[str, Any]) -> int:
    """
    Determine appropriate lookback period based on timeframe.
    
    Maps timeframes to lookback values:
    - Intraday (5m, 15m, 30m): lookback = 2
    - Swing (1h, 4h, 8h): lookback = 4
    - Position (1d, 1w): lookback = 6
    
    Args:
        timeframe: Canonical timeframe string (e.g., "5 mins", "1 hour", "1 day")
        config: Lookback configuration dict from YAML
        
    Returns:
        Appropriate lookback value
        
    Examples:
        >>> get_dynamic_lookback("5 mins", config)
        2
        >>> get_dynamic_lookback("4hs", config)
        4
        >>> get_dynamic_lookback("1 day", config)
        6
    """
    if not config.get('enabled', False):
        return config.get('default_lookback', 3)
    
    timeframe_lower = timeframe.lower()
    
    # Extract numeric value from timeframe
    match = re.search(r'(\d+)', timeframe)
    value = int(match.group(1)) if match else None
    
    # Check intraday
    intraday_config = config.get('intraday', {})
    if any(pattern in timeframe_lower for pattern in intraday_config.get('patterns', [])):
        max_value = intraday_config.get('max_value')
        if max_value is None or (value and value <= max_value):
            return intraday_config.get('lookback', 2)
    
    # Check swing
    swing_config = config.get('swing', {})
    if any(pattern in timeframe_lower for pattern in swing_config.get('patterns', [])):
        max_value = swing_config.get('max_value')
        if max_value is None or (value and value <= max_value):
            return swing_config.get('lookback', 4)
    
    # Check position
    position_config = config.get('position', {})
    if any(pattern in timeframe_lower for pattern in position_config.get('patterns', [])):
        return position_config.get('lookback', 6)
    
    # Default fallback
    return config.get('default_lookback', 3)


def add_dynamic_lookback_column(
    df,  # Union[pl.DataFrame, SparkFrame]
    lookback_config: Dict[str, Any],
    timeframe_col: str = "canonical_timeframe",
    engine: str = "polars"
):
    """
    Add a dynamic lookback column based on timeframe.
    
    Args:
        df: Input DataFrame
        lookback_config: Lookback configuration from YAML
        timeframe_col: Column containing timeframe values
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with 'dynamic_lookback' column added
    """
    if engine == "polars":
        import polars as pl
        
        # Create mapping for all unique timeframes
        unique_timeframes = df.select(timeframe_col).unique().to_series().to_list()
        lookback_map = {
            tf: get_dynamic_lookback(tf, lookback_config)
            for tf in unique_timeframes
        }
        
        # Apply mapping
        df = df.with_columns([
            pl.col(timeframe_col).replace(lookback_map).alias("dynamic_lookback")
        ])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        
        # Create mapping
        unique_timeframes = df.select(timeframe_col).distinct().rdd.flatMap(lambda x: x).collect()
        lookback_map = {
            tf: get_dynamic_lookback(tf, lookback_config)
            for tf in unique_timeframes
        }
        
        # Create mapping expression
        mapping_expr = F.create_map([F.lit(x) for pair in lookback_map.items() for x in pair])
        df = df.withColumn("dynamic_lookback", mapping_expr[F.col(timeframe_col)])
    
    return df
