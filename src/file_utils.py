"""
Module: file_utils.py

Utility functions for file discovery and sync-state saving.
Supports recursive file listing and Parquet-based tracking for data pipelines.

Author: Ashutosh Singh
"""

import os
import logging
from pathlib import Path
from typing import Union, List
import pandas as pd

logger = logging.getLogger(__name__)


def get_file_list(
    path: Union[str, Path],
    ext: Union[str, List[str]] = '*',
    return_df: bool = False
) -> Union[List[str], pd.DataFrame]:
    """
    Recursively collects file paths from a given directory.

    Args:
        path (Union[str, Path]): Root directory to scan.
        ext (Union[str, List[str]]): Extension(s) to filter by (e.g., '.csv', ['.json', '.parquet']). Use '*' for all files.
        return_df (bool): If True, returns a DataFrame with file metadata.

    Returns:
        Union[List[str], pd.DataFrame]: File paths or metadata DataFrame.

    Raises:
        FileNotFoundError: If the input path does not exist or is not a directory.
    """
    root = Path(path)
    if not root.is_dir():
        raise FileNotFoundError(f"❌ Directory does not exist: {root.resolve()}")

    all_files = [str(f) for f in root.rglob("*") if f.is_file()]
    
    if ext != '*':
        if isinstance(ext, str):
            ext = [ext]
        all_files = [f for f in all_files if Path(f).suffix in ext]

    logger.info(f"🔍 Found {len(all_files)} file(s) under {root.resolve()}")

    if not return_df:
        return all_files

    df = pd.DataFrame({
        'Path': all_files,
        'FileName': [os.path.basename(f) for f in all_files],
        'FileSize (in bytes)': [os.path.getsize(f) for f in all_files]
    })

    return df


def save_sync_state(
    df: pd.DataFrame,
    output_dir: Union[str, Path],
    file_name: str = "sync_state.parquet"
) -> None:
    """
    Saves the current S3 sync state to a Parquet file.

    Args:
        df (pd.DataFrame): DataFrame containing object metadata (must include 'Key', 'LastModified', 'Size').
        output_dir (Union[str, Path]): Path where the Parquet file should be saved.
        file_name (str): Output filename. Defaults to 'sync_state.parquet'.

    Raises:
        ValueError: If required columns are missing.
    """
    required_columns = {'Key', 'LastModified', 'Size'}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"❌ Missing required columns in DataFrame: {missing}")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    parquet_file = output_path / file_name
    df_to_save = df[['Key', 'LastModified', 'Size']].drop_duplicates()

    df_to_save.to_parquet(parquet_file, index=False)
    logger.info(f"📦 Sync state saved to: {parquet_file.resolve()}")
