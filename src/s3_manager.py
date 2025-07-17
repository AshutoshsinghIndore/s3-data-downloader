"""
Module: s3_manager.py

Handles AWS S3 operations:
- Establishing secure connection
- Validating bucket/prefix mappings
- Listing & filtering objects
- Multithreaded downloads with sync tracking

Author: Ashutosh Singh
"""

import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple, Optional, Dict

import boto3
import pandas as pd
from botocore.client import BaseClient
from botocore.exceptions import ClientError, NoCredentialsError
from tqdm import tqdm

from file_utils import get_file_list

logger = logging.getLogger(__name__)


def establish_connection_s3() -> BaseClient:
    """
    Establishes a boto3 S3 client using environment variables.

    Returns:
        BaseClient: Authenticated boto3 S3 client.

    Raises:
        RuntimeError: If credentials are missing or invalid.
    """
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    if not aws_access_key or not aws_secret_key:
        raise RuntimeError("❌ AWS credentials not found in environment variables.")

    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region,
        )
        logger.info("✅ S3 client created successfully.")
        return client

    except NoCredentialsError as e:
        raise RuntimeError("❌ AWS credentials are invalid or not configured.") from e
    except ClientError as ce:
        raise RuntimeError(f"❌ Failed to connect to AWS S3: {ce}") from ce


def preprocess_bucket_prefix(
    s3_client: BaseClient, bucket_prefix_map: Dict[str, List[str]]
) -> List[Tuple[str, str]]:
    """
    Validates S3 bucket and prefix combinations.

    Args:
        s3_client (BaseClient): Boto3 S3 client.
        bucket_prefix_map (Dict[str, List[str]]): Mapping of bucket names to prefix lists.

    Returns:
        List[Tuple[str, str]]: Valid (bucket, prefix) pairs.
    """
    valid_pairs = []

    for bucket, prefixes in bucket_prefix_map.items():
        try:
            s3_client.head_bucket(Bucket=bucket)
        except ClientError as e:
            logger.error(f"❌ Bucket '{bucket}' not accessible: {e.response['Error']['Message']}")
            continue

        prefixes = prefixes or ['*']
        for prefix in prefixes:
            try:
                response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
                if 'Contents' in response:
                    valid_pairs.append((bucket, prefix))
                else:
                    logger.warning(f"⚠️ No objects found in: {bucket}/{prefix}")
            except ClientError as e:
                logger.error(f"❌ Failed to validate prefix '{prefix}' in bucket '{bucket}': {e.response['Error']['Message']}")

    return valid_pairs


def list_s3_objects(client: BaseClient, bucket_prefixes: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    Lists objects from specified S3 buckets and prefixes.

    Args:
        client (BaseClient): Boto3 S3 client.
        bucket_prefixes (List[Tuple[str, str]]): List of (bucket, prefix) pairs.

    Returns:
        pd.DataFrame: Object metadata including Key, Size, and LastModified.
    """
    all_objects = []

    for bucket, prefix in bucket_prefixes:
        logger.info(f"📦 Scanning bucket: {bucket} | Prefix: {prefix or '[root]'}")

        paginator = client.get_paginator('list_objects_v2')
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    all_objects.append({
                        'Key': obj['Key'],
                        'LastModified': obj['LastModified'],
                        'Size': obj['Size'],
                        'FileName': os.path.basename(obj['Key']),
                        'ext': os.path.splitext(obj['Key'])[-1],
                        'ETag': obj['ETag'],
                        'BucketName': bucket,
                        'PrefixName': prefix
                    })
        except client.exceptions.NoSuchBucket:
            logger.error(f"❌ Bucket not found: {bucket}")
        except Exception as e:
            logger.exception(f"❌ Error listing {bucket}/{prefix}: {e}")

    return pd.DataFrame(all_objects)


def filter_obj_list(
    obj_list: pd.DataFrame,
    filter_criteria: Dict[str, List[str]],
    sync_mode: str,
    loc_downloads: str
) -> pd.DataFrame:
    """
    Filters S3 object list based on file extensions, names, and sync state.

    Args:
        obj_list (pd.DataFrame): Raw S3 object metadata.
        filter_criteria (Dict[str, List[str]]): Include/exclude filters.
        sync_mode (str): "incremental" or "full_refresh".
        loc_downloads (str): Local download path to track sync state.

    Returns:
        pd.DataFrame: Filtered objects with download metadata.
    """
    include_exts = filter_criteria.get("include_extensions")
    exclude_files = filter_criteria.get("exclude_files")

    if include_exts:
        obj_list = obj_list[obj_list["ext"].isin(include_exts)]

    if exclude_files:
        obj_list = obj_list[~obj_list["FileName"].isin(exclude_files)]

    obj_list.reset_index(drop=True, inplace=True)

    if sync_mode == "incremental":
        sync_path = Path(loc_downloads) / "sync_state.parquet"
        if sync_path.exists():
            last_sync = pd.read_parquet(sync_path)
            latest_ts = last_sync["LastModified"].max()
            obj_list = obj_list[obj_list["LastModified"] > latest_ts]
        else:
            logger.warning("⚠️ No existing sync state. Performing full download.")

    elif sync_mode != "full_refresh":
        raise ValueError("❌ Invalid sync_mode. Choose from ['full_refresh', 'incremental']")

    obj_list["Status"] = "Pending"
    obj_list["Dest_Path"] = obj_list["Key"].apply(lambda x: os.path.join(os.path.abspath(loc_downloads), x))
    return obj_list


def download_file_s3(
    client: BaseClient,
    bucket: str,
    key: str,
    size: int,
    dest_path: str
) -> None:
    """
    Downloads a single file from S3 with a progress bar.

    Args:
        client (BaseClient): S3 client.
        bucket (str): Bucket name.
        key (str): Object key.
        size (int): Object size (bytes).
        dest_path (str): Local destination path.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with tqdm(
        total=size,
        unit="B",
        unit_scale=True,
        ncols=100,
        desc=f"⬇️ {key[-40:]}...",
        leave=False
    ) as pbar:
        client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=dest_path,
            Callback=lambda b: pbar.update(b)
        )


def execute_download(client: BaseClient, obj_list: pd.DataFrame, threads: int = 12) -> None:
    """
    Downloads multiple S3 files concurrently using ThreadPoolExecutor.

    Args:
        client (BaseClient): S3 client.
        obj_list (pd.DataFrame): Filtered object list with Dest_Path.
        threads (int): Max concurrent download threads.
    """
    targets = obj_list[['BucketName', 'Key', 'Size', 'Dest_Path']].values.tolist()

    with tqdm(total=len(targets), desc="📥 Downloading Files", ncols=100) as pbar:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [
                executor.submit(download_file_s3, client, b, k, s, d)
                for b, k, s, d in targets
            ]
            for future in as_completed(futures):
                pbar.update(1)
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"❌ Download failed: {e}")
