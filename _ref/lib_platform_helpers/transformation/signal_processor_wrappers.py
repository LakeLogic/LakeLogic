"""
Signal Processor Wrappers - FIXED
==================================
High-level wrapper functions for signal processing with proper imports and error handling.
"""

from typing import Union, List, Dict, Any
from loguru import logger
import polars as pl

# Fixed imports from complete merge
from .signal_processor import (
    calculate_rsi,
    calculate_rsi_batch,
    calculate_volume_indicators,
    calculate_ha_color_change,
    generate_crossover_signals,  # Renamed from generate_buy_sell_signals
    PolarsFrame,
    SparkFrame
)

# Imports from new signals (fixed version)
from .signal_generator import (
    generate_rsi_crossover_signals,
    generate_ha_signals,
    generate_volume_signals,
    determine_profile_settings_from_timeframe
)


def calculate_multi_field_rsi(
    df: PolarsFrame,
    fields: List[Dict[str, str]],
    fast_period: int = 7,
    slow_period: int = 11,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
) -> PolarsFrame:
    """
    Calculate RSI for multiple fields.
    
    Args:
        df: Input DataFrame
        fields: List of field configurations, each containing:
            - column: Column name to calculate RSI on
            - prefix: Prefix for output columns
        fast_period: Fast RSI period (default: 7)
        slow_period: Slow RSI period (default: 11)
        partition_cols: Columns to partition by
        engine: 'polars' or 'pyspark'
    
    Returns:
        DataFrame with RSI columns for all specified fields
        
    Example:
        fields = [
            {'column': 'close', 'prefix': 'close'},
            {'column': 'ha_close', 'prefix': 'ha_close'},
            {'column': 'volume', 'prefix': 'volume'}
        ]
        df = calculate_multi_field_rsi(df, fields)
        # Creates: rsi_close_fast, rsi_close_slow, rsi_ha_close_fast, etc.
    """
    logger.info(f"Calculating RSI for {len(fields)} fields (fast={fast_period}, slow={slow_period})")
    
    if not fields:
        logger.warning("No fields provided for RSI calculation")
        return df
    
    for idx, field_config in enumerate(fields):
        try:
            column = field_config['column']
            prefix = field_config['prefix']
            
            # Validate column exists
            if column not in df.columns:
                logger.error(f"Column '{column}' not found in DataFrame")
                raise ValueError(f"Column '{column}' does not exist")
            
            logger.info(f"Processing field {idx+1}/{len(fields)}: {column} → rsi_{prefix}_fast/slow")
            
            # Fast RSI
            df = calculate_rsi(
                df,
                column=column,
                period=fast_period,
                partition_cols=partition_cols,
                output_name=f"rsi_{prefix}_fast",
                engine=engine
            )
            
            # Slow RSI
            df = calculate_rsi(
                df,
                column=column,
                period=slow_period,
                partition_cols=partition_cols,
                output_name=f"rsi_{prefix}_slow",
                engine=engine
            )
            
        except KeyError as e:
            logger.error(f"Missing key in field config {field_config}: {e}")
            raise ValueError(f"Field config must have 'column' and 'prefix': {field_config}")
        except Exception as e:
            logger.error(f"Failed to calculate RSI for field {field_config}: {e}")
            raise
    
    logger.info("Multi-field RSI calculation complete")
    return df


def calculate_ha_trend(
    df: PolarsFrame,
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
) -> PolarsFrame:
    """
    Calculate Heikin-Ashi trend from existing HA columns.
    
    Trend definitions:
    - Bull: ha_close > ha_open AND ha_close > prev_ha_close
    - Bear: ha_close < ha_open AND ha_close < prev_ha_close  
    - Neutral: Otherwise
    
    Args:
        df: Input DataFrame (must have ha_open, ha_close columns)
        partition_cols: Columns to partition by
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with ha_trend column added
    """
    logger.info("Calculating Heikin-Ashi trend")
    
    # Validate required columns
    required_cols = ["ha_open", "ha_close"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required HA columns: {missing}")
    
    if engine == "polars":
        df = df.with_columns([
            pl.when(
                (pl.col("ha_close") > pl.col("ha_open")) &
                (pl.col("ha_close") > pl.col("ha_close").shift(1).over(partition_cols, order_by="timestamp"))
            ).then(pl.lit("Bull"))
            .when(
                (pl.col("ha_close") < pl.col("ha_open")) &
                (pl.col("ha_close") < pl.col("ha_close").shift(1).over(partition_cols, order_by="timestamp"))
            ).then(pl.lit("Bear"))
            .otherwise(pl.lit("Neutral"))
            .alias("ha_trend")
        ])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy(partition_cols).orderBy("timestamp")
        
        df = df.withColumn("ha_trend",
                          F.when(
                              (F.col("ha_close") > F.col("ha_open")) &
                              (F.col("ha_close") > F.lag("ha_close", 1).over(window_spec)),
                              "Bull"
                          ).when(
                              (F.col("ha_close") < F.col("ha_open")) &
                              (F.col("ha_close") < F.lag("ha_close", 1).over(window_spec)),
                              "Bear"
                          ).otherwise("Neutral"))
    else:
        raise ValueError(f"Unknown engine: {engine}")
    
    return df


def calculate_rsi_trend(
    df: PolarsFrame,
    rsi_fast_col: str = "rsi_close_fast",
    rsi_slow_col: str = "rsi_close_slow",
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
) -> PolarsFrame:
    """
    Calculate RSI-based trend.
    
    Trend definitions:
    - Bull: rsi_fast > rsi_slow
    - Bear: rsi_fast < rsi_slow
    - Neutral: rsi_fast == rsi_slow
    
    Args:
        df: Input DataFrame (must have RSI columns)
        rsi_fast_col: Fast RSI column name
        rsi_slow_col: Slow RSI column name
        partition_cols: Columns to partition by
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with rsi_trend column added
    """
    logger.info(f"Calculating RSI trend from {rsi_fast_col} and {rsi_slow_col}")
    
    # Validate required columns
    required_cols = [rsi_fast_col, rsi_slow_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required RSI columns: {missing}")
    
    if engine == "polars":
        df = df.with_columns([
            pl.when(pl.col(rsi_fast_col) > pl.col(rsi_slow_col))
              .then(pl.lit("Bull"))
              .when(pl.col(rsi_fast_col) < pl.col(rsi_slow_col))
              .then(pl.lit("Bear"))
              .otherwise(pl.lit("Neutral"))
              .alias("rsi_trend")
        ])
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        
        df = df.withColumn("rsi_trend",
                          F.when(F.col(rsi_fast_col) > F.col(rsi_slow_col), "Bull")
                           .when(F.col(rsi_fast_col) < F.col(rsi_slow_col), "Bear")
                           .otherwise("Neutral"))
    else:
        raise ValueError(f"Unknown engine: {engine}")
    
    return df


def process_all_signals(
    df: PolarsFrame,
    config: Dict[str, Any],
    engine: str = "polars"
) -> PolarsFrame:
    """
    Comprehensive signal processing function (OPTIMIZED).
    
    Processing steps:
    1. Calculate RSI for multiple fields in BATCH (fast!)
    2. Calculate trends (RSI-based and HA-based)
    3. Generate RSI crossover signals with OB/OS filtering
    4. Generate Heikin-Ashi signals with OB/OS filtering
    5. Generate volume-based signals with OB/OS filtering
    
    OPTIMIZATIONS:
    - Uses batch RSI calculation (single pandas conversion instead of N conversions)
    - Reduces redundant timeframe splits
    - Uses rolling_min/rolling_max for OB/OS checks instead of loop
    
    Args:
        df: Input DataFrame with OHLCV and HA columns
        config: Configuration dict with indicators and signals settings
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with all indicators and signals
        
    Config structure:
        {
            'partition_cols': ['canonical_symbol', 'canonical_timeframe'],
            'indicators': {
                'rsi': {
                    'enabled': True,
                    'fast_period': 7,
                    'slow_period': 11,
                    'fields': [
                        {'column': 'close', 'prefix': 'close'},
                        {'column': 'ha_close', 'prefix': 'ha_close'},
                        {'column': 'volume', 'prefix': 'volume'}
                    ]
                }
            },
            'signals': {
                'timeframe_profiles': {
                    'enabled': True,
                    'minute': {...},
                    'hour': {...},
                    'default': {...}
                }
            },
            'new_signals': {
                'rsi_crossover': {'enabled': True, 'buy_level': 30.0, ...},
                'ha_signals': {'enabled': True, 'buy_level': 30.0, ...},
                'volume_signals': {'enabled': True, 'buy_level': 30.0, ...}
            }
        }
    """
    logger.info("=== Starting OPTIMIZED comprehensive signal processing ===")
    
    partition_cols = config.get('partition_cols', ['canonical_symbol', 'canonical_timeframe'])
    timeframe_profiles = config.get('signals', {}).get('timeframe_profiles', {})
    
    try:
        # Step 1: Calculate RSI for multiple fields using BATCH optimization
        if config.get('indicators', {}).get('rsi', {}).get('enabled', True):
            logger.info("Step 1: OPTIMIZED batch RSI calculation")
            rsi_config = config['indicators']['rsi']
            
            # Check if dynamic periods per timeframe
            if (timeframe_profiles and 
                timeframe_profiles.get('enabled', False) and 
                "canonical_timeframe" in df.columns):
                
                logger.info("Using dynamic RSI periods per timeframe with batch calculation")
                timeframes = df["canonical_timeframe"].unique().to_list()
                logger.info(f"Processing {len(timeframes)} timeframes: {timeframes}")
                
                df_parts = []
                for tf in timeframes:
                    tf_data = df.filter(pl.col("canonical_timeframe") == tf)
                    
                    # Get timeframe-specific settings
                    settings = determine_profile_settings_from_timeframe(
                        tf, timeframe_profiles,
                        default_settings={
                            "rsi_fast_period": rsi_config.get('fast_period', 7),
                            "rsi_slow_period": rsi_config.get('slow_period', 11)
                        }
                    )
                    
                    fast_period = settings.get("rsi_fast_period", 7)
                    slow_period = settings.get("rsi_slow_period", 11)
                    logger.info(f"Timeframe {tf}: RSI {fast_period}/{slow_period} (BATCH)")
                    
                    # Use OPTIMIZED batch RSI calculation
                    tf_data = calculate_rsi_batch(
                        tf_data,
                        fields=rsi_config.get('fields', []),
                        fast_period=fast_period,
                        slow_period=slow_period,
                        partition_cols=partition_cols,
                        engine=engine
                    )
                    df_parts.append(tf_data)
                
                # Recombine
                if df_parts:
                    df = pl.concat(df_parts).sort(partition_cols + ["timestamp"])
                else:
                    raise ValueError("No data after timeframe processing")
                    
            else:
                # Static periods for all data - use OPTIMIZED batch RSI
                logger.info("Using static RSI periods with OPTIMIZED batch calculation")
                df = calculate_rsi_batch(
                    df,
                    fields=rsi_config.get('fields', []),
                    fast_period=rsi_config.get('fast_period', 7),
                    slow_period=rsi_config.get('slow_period', 11),
                    partition_cols=partition_cols,
                    engine=engine
                )
        else:
            logger.warning("RSI calculation disabled in config")
        
        # Step 2: Calculate trends
        logger.info("Step 2: Calculating trends")
        
        # Get RSI column names from config
        rsi_fields = config.get('indicators', {}).get('rsi', {}).get('fields', [])
        close_field = next((f for f in rsi_fields if f['column'] == 'close'), None)
        
        if close_field:
            rsi_fast_col = f"rsi_{close_field['prefix']}_fast"
            rsi_slow_col = f"rsi_{close_field['prefix']}_slow"
        else:
            # Default fallback
            rsi_fast_col = 'rsi_close_fast'
            rsi_slow_col = 'rsi_close_slow'
        
        df = calculate_rsi_trend(
            df, 
            rsi_fast_col=rsi_fast_col,
            rsi_slow_col=rsi_slow_col,
            partition_cols=partition_cols, 
            engine=engine
        )
        df = calculate_ha_trend(df, partition_cols=partition_cols, engine=engine)
        
        # Step 3: Generate RSI crossover signals
        if config.get('new_signals', {}).get('rsi_crossover', {}).get('enabled', True):
            logger.info("Step 3: Generating RSI crossover signals")
            rsi_crossover_config = config.get('new_signals', {}).get('rsi_crossover', {})
            
            df = generate_rsi_crossover_signals(
                df,
                rsi_fast_col=rsi_fast_col,
                rsi_slow_col=rsi_slow_col,
                rsi_buy_level=rsi_crossover_config.get('buy_level', 30.0),
                rsi_sell_level=rsi_crossover_config.get('sell_level', 70.0),
                lookback_obos=rsi_crossover_config.get('lookback_obos', 4),
                partition_cols=partition_cols,
                timeframe_profiles=timeframe_profiles if timeframe_profiles.get('enabled') else None,
                engine=engine
            )
        
        # Step 4: Generate Heikin-Ashi signals
        if config.get('new_signals', {}).get('ha_signals', {}).get('enabled', True):
            logger.info("Step 4: Generating Heikin-Ashi signals")
            ha_signals_config = config.get('new_signals', {}).get('ha_signals', {})
            
            df = generate_ha_signals(
                df,
                rsi_slow_col=rsi_slow_col,
                rsi_buy_level=ha_signals_config.get('buy_level', 30.0),
                rsi_sell_level=ha_signals_config.get('sell_level', 70.0),
                lookback_obos=ha_signals_config.get('lookback_obos', 4),
                partition_cols=partition_cols,
                timeframe_profiles=timeframe_profiles if timeframe_profiles.get('enabled') else None,
                engine=engine
            )
        
        # Step 5: Generate volume-based signals
        if config.get('new_signals', {}).get('volume_signals', {}).get('enabled', True):
            logger.info("Step 5: Generating volume-based signals")
            vol_signals_config = config.get('new_signals', {}).get('volume_signals', {})
            
            df = generate_volume_signals(
                df,
                rsi_slow_col=rsi_slow_col,
                rsi_buy_level=vol_signals_config.get('buy_level', 30.0),
                rsi_sell_level=vol_signals_config.get('sell_level', 70.0),
                volume_sma_period=vol_signals_config.get('sma_period', 20),
                volume_spike_multiplier=vol_signals_config.get('spike_multiplier', 1.5),
                lookback_obos=vol_signals_config.get('lookback_obos', 4),
                partition_cols=partition_cols,
                timeframe_profiles=timeframe_profiles if timeframe_profiles.get('enabled') else None,
                engine=engine
            )
        
        logger.info("=== Signal processing complete ===")
        
    except Exception as e:
        logger.error(f"Error in signal processing: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    
    return df