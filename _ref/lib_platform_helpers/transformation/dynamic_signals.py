"""
Enhanced Signal Generation with Dynamic Lookback

Note: This implements dynamic lookback by generating signals for each timeframe group separately
with its appropriate lookback value, then combining results.
"""

from typing import Union, List, Dict, Any
from loguru import logger
import polars as pl


def generate_signals_with_dynamic_lookback(
    df,  # Union[pl.DataFrame, SparkFrame]
    config: Dict[str, Any],
    partition_cols: List[str] = ["canonical_symbol", "canonical_timeframe"],
    engine: str = "polars"
):
    """
    Generate buy/sell signals with dynamic lookback based on timeframe.
    
    Strategy:
    1. Add dynamic_lookback column based on timeframe
    2. Group by timeframe (which has same lookback)
    3. Generate signals for each group with its lookback
    4. Combine all results
    
    Args:
        df: Input DataFrame with RSI and trend columns
        config: Signal configuration from YAML
        partition_cols: Columns to partition by
        engine: 'polars' or 'pyspark'
        
    Returns:
        DataFrame with signals generated using appropriate lookback per timeframe
    """
    from .dynamic_lookback import get_dynamic_lookback, add_dynamic_lookback_column
    
    logger.info("Generating signals with dynamic lookback")
    
    lookback_config = config.get('lookback_by_timeframe', {})
    
    # Add dynamic lookback column
    df = add_dynamic_lookback_column(
        df,
        lookback_config=lookback_config,
        engine=engine
    )
    
    if engine == "polars":
        # Get unique lookback values and their corresponding timeframes
        lookback_groups = df.select(['canonical_timeframe', 'dynamic_lookback']).unique()
        
        result_dfs = []
        
        for row in lookback_groups.iter_rows(named=True):
            timeframe = row['canonical_timeframe']
            lookback = row['dynamic_lookback']
            
            logger.info(f"Processing {timeframe} with lookback={lookback}")
            
            # Filter for this timeframe
            df_subset = df.filter(pl.col('canonical_timeframe') == timeframe)
            
            # Generate standard signals
            from .signal_processor import generate_buy_sell_signals
            
            standard_config = config.get('standard', {})
            df_subset = generate_buy_sell_signals(
                df_subset,
                rsi_fast_col=standard_config.get('rsi_fast_col', 'rsi_close_fast'),
                rsi_slow_col=standard_config.get('rsi_slow_col', 'rsi_close_slow'),
                trend_col=standard_config.get('trend_col', 'rsi_trend'),
                rsi_buy_level=standard_config.get('buy_level', 30.0),
                rsi_sell_level=standard_config.get('sell_level', 70.0),
                lookback_bars=lookback,  # Use dynamic lookback
                partition_cols=['canonical_symbol'],  # Only partition by symbol
                output_prefix="",
                engine=engine
            )
            
            # Generate HA signals
            ha_config = config.get('heikin_ashi', {})
            df_subset = generate_buy_sell_signals(
                df_subset,
                rsi_fast_col=ha_config.get('rsi_fast_col', 'rsi_ha_close_fast'),
                rsi_slow_col=ha_config.get('rsi_slow_col', 'rsi_ha_close_slow'),
                trend_col=ha_config.get('trend_col', 'ha_trend'),
                rsi_buy_level=ha_config.get('buy_level', 30.0),
                rsi_sell_level=ha_config.get('sell_level', 70.0),
                lookback_bars=lookback,  # Use dynamic lookback
                partition_cols=['canonical_symbol'],
                output_prefix="ha_",
                engine=engine
            )
            
            result_dfs.append(df_subset)
        
        # Combine all results
        df = pl.concat(result_dfs)
        
    elif engine == "pyspark":
        # Similar approach for PySpark
        from pyspark.sql import functions as F
        from pyspark.sql import Window
        
        # Collect unique timeframe-lookback pairs
        lookback_groups = df.select('canonical_timeframe', 'dynamic_lookback').distinct().collect()
        
        result_dfs = []
        
        for row in lookback_groups:
            timeframe = row['canonical_timeframe']
            lookback = row['dynamic_lookback']
            
            logger.info(f"Processing {timeframe} with lookback={lookback}")
            
            df_subset = df.filter(F.col('canonical_timeframe') == timeframe)
            
            # Generate signals (implementation similar to Polars)
            from .signal_processor import generate_buy_sell_signals
            
            standard_config = config.get('standard', {})
            df_subset = generate_buy_sell_signals(
                df_subset,
                lookback_bars=lookback,
                partition_cols=['canonical_symbol'],
                **standard_config,
                output_prefix="",
                engine=engine
            )
            
            ha_config = config.get('heikin_ashi', {})
            df_subset = generate_buy_sell_signals(
                df_subset,
                lookback_bars=lookback,
                partition_cols=['canonical_symbol'],
                **ha_config,
                output_prefix="ha_",
                engine=engine
            )
            
            result_dfs.append(df_subset)
        
        # Union all results
        from functools import reduce
        df = reduce(lambda a, b: a.union(b), result_dfs)
    
    logger.info("Dynamic lookback signal generation complete")
    return df


# Usage in process_all_signals:
# if config.get('signals', {}).get('lookback_by_timeframe', {}).get('enabled', False):
#     df = generate_signals_with_dynamic_lookback(df, config['signals'], engine=engine)
# else:
#     df = generate_dual_signals(df, standard_config, ha_config, engine=engine)
