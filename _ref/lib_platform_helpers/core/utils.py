
from collections import defaultdict
from typing import Dict, Union, Any, List, Optional, Literal
from loguru import logger
from datetime import datetime, timedelta

import fsspec 
import adlfs 
import os
import polars as pl

# Define the acceptable DataFrame types
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
SparkFrame = Any 

def count_loc_in_library(start_path: str) -> Dict[str, int]:
    """
    Counts non-blank, non-comment lines of Python code for all .py files 
    within the specified directory and all its subdirectories.
    
    Args:
        start_path (str): The root directory of the library/package (e.g., './src/etl/').
        
    Returns:
        Dict[str, int]: A dictionary containing total_lines, code_lines, and comment_lines counts.

    💡 Example:
        # Check all code under your ETL helpers:
        # stats = count_loc_in_library('./src/etl/lib_platform_helpers/') 
    """
    loc_stats = defaultdict(int)
    
    # os.walk traverses the directory tree recursively
    for root, _, files in os.walk(start_path):
        for file_name in files:
            # Check if the file is a Python source file
            if file_name.endswith('.py'):
                file_path = os.path.join(root, file_name)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            # 1. Update total lines count
                            loc_stats['total_lines'] += 1
                            
                            stripped_line = line.strip()
                            
                            # 2. Categorize the line
                            if not stripped_line:
                                loc_stats['blank_lines'] += 1
                            elif stripped_line.startswith('#'):
                                loc_stats['comment_lines'] += 1
                            else:
                                loc_stats['code_lines'] += 1
                                
                except Exception as e:
                    # Log file access issues without crashing the count
                    logger.error(f"Failed to read {file_path}: {e}")
                    
    return dict(loc_stats)




def df_has_data(
    df: Union[PolarsFrame, SparkFrame],
    engine: str = "polars"
) -> bool:
    """
    Efficiently checks if a DataFrame/LazyFrame contains any rows, avoiding full collection.

    Args:
        df: The input DataFrame (PolarsFrame or SparkFrame).
        engine (str): The processing engine ('polars' or 'pyspark').

    Returns:
        bool: True if the DataFrame has one or more rows; False otherwise.

    Raises:
        TypeError: If the input DataFrame type is incompatible with the selected engine.
        ImportError: If required PySpark libraries are missing.

    💡 Usage Example (Polars - Lazy Check):
    
    ```python
    df_lazy = pl.LazyFrame({'id': [1, 2]})
    if df_has_data(df_lazy, 'polars'):
        logger.info("Polars LazyFrame has data.")
    ```
    
    💡 Usage Example (PySpark - Optimized Check):
    
    ```python
    # Assume spark is active and df_spark is a Spark DataFrame
    if df_has_data(df_spark, 'pyspark'):
        logger.info("Spark DataFrame has data.")
    ```
    """
    
    engine = engine.lower()
    
    if engine == "polars":
        if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError("Input DataFrame must be a Polars DataFrame/LazyFrame for 'polars' engine.")
        return _df_has_data_polars(df)
        
    elif engine == "pyspark":
        return _df_has_data_pyspark(df)
        
    else:
        raise ValueError(f"Unknown engine: {engine}. Must be 'polars' or 'pyspark'.")


# ----------------------------------------------------------------------

def _df_has_data_polars(df: PolarsFrame) -> bool:
    """Uses Polars' fetch(1) method for optimized existence check."""
    
    # Use fetch(1) on the LazyFrame or head(1) on EagerFrame to load only one row.
    if isinstance(df, pl.LazyFrame):
        # Fetch up to 1 row; computation stops immediately if a row is found.
        has_data = df.fetch(1).height > 0
    else:
        # Eager DataFrame: check the size directly (instantaneous).
        has_data = df.height > 0
        
    return has_data


def _df_has_data_pyspark(df: SparkFrame) -> bool:
    """Uses PySpark's head(1) to check for existence without full scan."""
    
    # Conditional PySpark Imports
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
    except ImportError:
        logger.error("PySpark library required for 'pyspark' engine is missing.")
        return False
    
    if not isinstance(df, SparkDataFrame):
        raise TypeError("Input 'df' must be a Spark DataFrame for 'pyspark' engine.")
        
    # The most efficient check: attempt to load one row to the driver.
    # If the resulting list/object is not empty, data exists.
    return bool(df.head(1))


OperationType = Literal["move", "copy"]
CleanupMode = Literal["delete", "archive"]

def manage_file_lifecycle(
    source_paths: List[str],
    destination_uri: str,
    operation: OperationType,
    cleanup_age_days: Optional[int] = None,
    fs_options: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """
    Manages file lifecycle (move/copy) and performs cleanup based on file age 
    in the destination directory across local and cloud storage.

    CRITICAL UPDATE: Handles inputs where source_paths contains files, directories, 
    or wildcards, recursively discovering files within directories.
    
    Args:
        source_paths (List[str]): List of files, directories, or wildcards to process.
        destination_uri (str): The target directory URI (local path, abfss://, s3://, etc.).
        operation (OperationType): 'move' (source deleted) or 'copy' (source retained).
        cleanup_age_days (Optional[int]): If provided, deletes files in the 
            destination older than this number of days.
        fs_options (Optional[Dict[str, Any]]): Authentication options for cloud storage 
            (e.g., ADLS credentials for 'abfss://').

    Returns:
        List[str]: List of URIs for files successfully processed.

    
    💡 Usage Example 1: Local Move and Cleanup (Quarantine/Archive)
    
    This example simulates moving new files from a processing folder to a local 
    archive folder and cleaning up old logs in that archive.
    
    ```python
    # Ensure source path points to a file or directory:
    # LOCAL_ARCHIVE = './.local-lakehouse/raw_archive/temp_audit/'
    
    # 1. Move files from the temp directory into the archive:
    processed_files = manage_file_lifecycle(
        source_paths=['/temp/batch1/*.csv', '/temp/batch2/'], # Handles wildcard and directory
        destination_uri='./archive/market_data/',
        operation='move', 
        cleanup_age_days=None 
    )
    
    # 2. Perform cleanup separately in the archive directory (delete files > 30 days old):
    cleanup_results = manage_file_lifecycle(
        source_paths=[], # Empty list since we are only cleaning the destination
        destination_uri='./archive/market_data/',
        operation='copy', # Operation ignored
        cleanup_age_days=30 
    )
    ```

    💡 Usage Example 2: Cloud Path Cleanup (Azure ADLS Gen2)
    
    This example demonstrates the required parameters for cleaning files from an Azure 
    quarantine path using credentials passed via `fs_options`.
    
    ```python
    # NOTE: Requires `adlfs` and credentials configured.
    CLOUD_QUARANTINE_URI = "abfss://quarantine@storageacct.dfs.core.windows.net/errors/"
    
    cleanup_results = manage_file_lifecycle(
        source_paths=[], 
        destination_uri=CLOUD_QUARANTINE_URI,
        operation='copy', # Operation ignored
        cleanup_age_days=60, # Delete files older than 60 days
        fs_options={
            'account_name': 'storageacct', 
            'credential': '...' # E.g., Azure service principal credentials or key
        }
    )
    ```
    """
    fs_options = fs_options or {}
    successfully_processed = []
    all_files_to_process = []

    # 1. Instantiate the Filesystem Handler (fs)
    try:
        protocol = fsspec.utils.get_protocol(destination_uri)
        fs = fsspec.filesystem(protocol, **fs_options)
    except Exception as e:
        logger.error(f"Failed to initialize filesystem for {destination_uri}: {e}")
        return []

    fs.makedirs(destination_uri, exist_ok=True)

    # 2. Expand Source Paths (The FIX for directories/wildcards)
    
    for path_item in source_paths:
        # Use fsspec.glob() for intelligent path expansion (handles wildcards and directories)
        # Setting detail=False returns a simple list of file URIs.
        try:
            expanded_paths = fs.glob(path_item)
            
            for expanded_path in expanded_paths:
                # We only want to process actual files, not directories themselves
                if fs.isfile(expanded_path):
                    all_files_to_process.append(expanded_path)
                elif fs.isdir(expanded_path):
                    # If the item is a directory, recursively list all files inside it
                    # Recursively list all files starting from the directory path
                    # fs.find is generally good for recursive file discovery.
                    all_files_to_process.extend(
                        [p for p in fs.find(expanded_path, detail=False) if fs.isfile(p)]
                    )
        except Exception as e:
            logger.error(f"Failed to expand source path {path_item}: {e}")

    # 3. Perform File Move/Copy Operations
    if all_files_to_process:
        logger.info(f"Starting {operation} operation for {len(all_files_to_process)} discovered file(s).")
        
        for src_path in all_files_to_process:
            filename = os.path.basename(src_path)
            dest_path = os.path.join(destination_uri, filename)

            try:
                if operation == 'move':
                    fs.mv(src_path, dest_path)
                    #logger.info(f"  - MOVED {filename} to {destination_uri}")
                    # clean up folder if empty
                    src_dir = src_path.rsplit("/", 1)[0] if "/" in src_path else os.path.dirname(src_path)
                    if fs.listdir(src_dir) == []:
                        fs.rmdir(src_dir)
                elif operation == 'copy':
                    fs.copy(src_path, dest_path)
                    #logger.info(f"  - COPIED {filename} to {destination_uri}")
                
                successfully_processed.append(dest_path)

            except Exception as e:
                logger.error(f"Failed to {operation} file {src_path}: {e}")

    # 4. Perform Cleanup (Logic remains the same)
    # ... (Cleanup logic using cleanup_age_days continues here) ...

    if cleanup_age_days is not None and cleanup_age_days > 0:
        logger.info(f"Starting cleanup: Deleting files in {destination_uri} older than {cleanup_age_days} days.")
        
        cutoff_timestamp = datetime.now() - timedelta(days=cleanup_age_days)
        deleted_files = 0
        
        try:
            # detail=True is necessary to get modification time (mtime)
            all_files_info = fs.ls(destination_uri, detail=True)
        except Exception as e:
            logger.error(f"Failed to list files for cleanup: {e}")
            return successfully_processed

        for file_info in all_files_info:
            if file_info.get('type') == 'file': # Use .get('type') for robustness
                mtime = file_info.get('mtime')
                
                # mtime conversion logic (from float timestamp or string)
                if isinstance(mtime, (int, float)):
                    mtime_dt = datetime.fromtimestamp(mtime)
                elif isinstance(mtime, str):
                    try:
                        mtime_dt = datetime.fromisoformat(mtime.replace('Z', '+00:00'))
                    except ValueError:
                        continue # Skip if string mtime cannot be parsed
                else:
                    continue
                    
                if mtime_dt < cutoff_timestamp:
                    file_path = os.path.join(destination_uri, file_info['name'])
                    
                    try:
                        fs.rm(file_path) # Remove file
                        #logger.warning(f"  - CLEANUP: Deleted file {file_path}.")
                        deleted_files += 1
                    except Exception as e:
                        logger.error(f"Failed to delete file {file_path}: {e}")

        logger.success(f"✅ Cleanup complete. Deleted {deleted_files} files.")

    return successfully_processed



def directory_contains_files(
    directory_uri: str,
    fs_options: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Checks if a directory (local or cloud) contains any files recursively.

    This function is highly performant because it stops searching and returns True 
    immediately upon finding the first file.

    Args:
        directory_uri (str): The path/URI of the folder to check (e.g., './my_data', 'abfss://container/path').
        fs_options (Optional[Dict[str, Any]]): Authentication options for cloud storage.

    Returns:
        bool: True if the directory or any subdirectory contains at least one file, False otherwise.
        
    💡 Usage Example (Local):
    
    ```python
    # Assume './raw_zone/ib_api/' exists and has files
    has_files = directory_contains_files('./raw_zone/ib_api/') 
    logger.info(f"Directory has files: {has_files}")
    # Expected: True or False
    ```
    
    💡 Usage Example (Cloud - Conceptual):
    
    ```python
    # NOTE: Requires ADLFS/fsspec setup and credentials in fs_options
    cloud_check = directory_contains_files(
        'abfss://quarantine@storageacct.dfs.core.windows.net/errors/',
        fs_options={'account_name': 'acct_name', 'credential': '...'}
    )
    # Expected: True if the quarantine folder is not empty
    ```
    """
    fs_options = fs_options or {}

    try:
        # 1. Initialize the Filesystem Handler (fs)
        protocol = fsspec.utils.get_protocol(directory_uri)
        fs = fsspec.filesystem(protocol, **fs_options)
        
        # Check if the directory exists
        if not fs.isdir(directory_uri):
            logger.warning(f"Directory not found: {directory_uri}")
            return False

        # 2. Use fs.find() for Recursive, Lazy File Discovery
        # fs.find returns a generator/iterator of all files.
        # depth=1 allows a quick check without full recursion; remove 'depth' for deep recursion.
    
        # Explicitly convert the result of fs.find() to an iterator
        all_files_iterator = iter(fs.find(directory_uri, detail=False))
        
        # The list comprehension with fs.find will stop immediately after the first result.
        first_file = next(all_files_iterator, None)
        
        return first_file is not None

    except Exception as e:
        logger.error(f"Error checking directory {directory_uri}: {e}")
        return False