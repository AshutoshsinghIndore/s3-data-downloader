import os
import hashlib
import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Tuple, Optional

import boto3
import pandas as pd
import numpy as np
from boto3.s3.transfer import TransferConfig
import logging
from typing import List, Tuple
from botocore.client import BaseClient
logger = logging.getLogger(__name__)
from file_utils import file_utils

from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

class S3_Downloader:
    """
    A utility class to manage downloading video logs from AWS S3 buckets.
    """
                
    def establish_connection_s3():
        """
        Establish an S3 client using AWS credentials from environment variables.
        
        Returns:
            boto3.client: S3 client object if successful.
            
        Raises:
            NoCredentialsError: If AWS credentials are missing.
            ClientError: If AWS client initialization fails.
        """
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION", "us-east-1")
    
        if not aws_access_key or not aws_secret_key:
            raise RuntimeError("AWS credentials not found in environment variables.")
    
        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
            print("✅ S3 client created successfully...!!")
            return s3
    
        except NoCredentialsError:
            print("Please configure AWS credentials properly.")
        except ClientError as ce:
            print(f"Client error occurred: {ce}")


    def preprocess_bucket_prefix(s3_client: str, bucket_prefix_map: dict[str, list[str]]) -> list[tuple[str, str]]:
        """
        Converts a dictionary of {bucket: [prefixes]} into a list of (bucket, prefix) pairs.
        Checks if each bucket and prefix exists in S3.
    
        Args:
            bucket_prefix_map (Dict[str, List[str]]): A dictionary mapping bucket names to lists of prefixes.
    
        Returns:
            List[Tuple[str, str]]: A validated list of (bucket, prefix) tuples. 
                                   If prefix list is empty, uses wildcard '*'.
    
        Raises:
            ClientError: If the bucket does not exist or access is denied.
        """
        valid_bucket_prefixes = []
    
        for bucket, prefixes in bucket_prefix_map.items():
            try:
                # Check if bucket exists
                s3_client.head_bucket(Bucket=bucket)
            except ClientError as e:
                logger.error(f"❌ Bucket '{bucket}' is not accessible: {e.response['Error']['Message']}")
                continue
    
            if not prefixes:
                valid_bucket_prefixes.append((bucket, '*'))
                continue
    
            for prefix in prefixes:
                try:
                    # Check if prefix has at least one object
                    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
                    if 'Contents' in response:
                        valid_bucket_prefixes.append((bucket, prefix))
                    else:
                        logger.warning(f"⚠️ No objects found for prefix '{prefix}' in bucket '{bucket}'. Skipping.")
                except ClientError as e:
                    logger.error(f"❌ Failed to validate prefix '{prefix}' in bucket '{bucket}': {e.response['Error']['Message']}")
    
        return valid_bucket_prefixes

    def list_s3_objects(client: BaseClient, bucket_prefixes: List[Tuple[str, str]]) -> pd.DataFrame:
        """
        List S3 objects for the given list of (bucket, prefix) pairs.
    
        Args:
            client (BaseClient): Boto3 S3 client instance.
            bucket_prefixes (List[Tuple[str, str]]): List of tuples with (bucket_name, prefix).
    
        Returns:
            pd.DataFrame: DataFrame with object metadata:
                          [Key, LastModified, Size, FileName, ext, ETag, BucketName, PrefixName]
        """
        all_objects = []
    
        for bucket_name, prefix in bucket_prefixes:
            logger.info(f"📦 Scanning bucket: {bucket_name} | Prefix: {prefix or '[root]'}")
    
            try:
                paginator = client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
                for page in page_iterator:
                    contents = page.get('Contents', [])
                    if not contents:
                        logger.warning(f"⚠️ No objects found for: {bucket_name}/{prefix}")
                        continue
    
                    for obj in contents:
                        all_objects.append({
                            'Key': obj['Key'],
                            'LastModified': obj['LastModified'],
                            'Size': obj['Size'],
                            'FileName': os.path.basename(obj['Key']),
                            'ext': os.path.splitext(obj['Key'])[-1],
                            'ETag': obj['ETag'],
                            'BucketName': bucket_name,
                            'PrefixName': prefix
                        })
    
            except client.exceptions.NoSuchBucket:
                logger.error(f"❌ Bucket not found: {bucket_name}")
            except Exception as e:
                logger.exception(f"❌ Error while listing objects in {bucket_name}/{prefix}: {e}")
    
        return pd.DataFrame(all_objects)


    def filter_obj_list(
        obj_list: pd.DataFrame,
        filter_criteria: dict[str, list],
        sync_mode: str,
        loc_downloads: str
        ) -> pd.DataFrame:
        """
        Filters the object list based on extension and exclusion rules,
        and applies sync mode logic to retain only relevant files.
    
        Args:
            obj_list (pd.DataFrame): Raw S3 object metadata.
            filter_criteria (dict): Includes 'include_extensions' and 'exclude_files' lists.
            sync_mode (str): "full_refresh" or "incremental".
            loc_downloads (str): Local path to check sync state and local files.
    
        Returns:
            pd.DataFrame: Filtered and updated object list with status.
        """
    
        # --- Extension Filter ---
        include_exts = filter_criteria.get("include_extensions")
        if include_exts is not None:
            obj_list = obj_list[obj_list["ext"].isin(include_exts)].reset_index(drop=True)
    
        # --- Exclude Files Filter ---
        exclude_files = filter_criteria.get("exclude_files")
        if exclude_files is not None:
            obj_list = obj_list[~obj_list["FileName"].isin(exclude_files)].reset_index(drop=True)
    
        # --- Sync Logic ---
        if sync_mode == "incremental":
            sync_state_path = os.path.join(loc_downloads, "sync_state.parquet")
            download_files = os.listdir(loc_downloads)
    
            if "sync_state.parquet" in download_files:
                last_sync_df = pd.read_parquet(sync_state_path)
                last_ts = last_sync_df["LastModified"].max()
                obj_list = obj_list[obj_list["LastModified"] > last_ts].reset_index(drop=True)
    
            elif not download_files:
                # First sync - allow all files
                pass
            else:
                # Parquet is missing but other files exist
                file_list_df = file_utils.get_file_list(loc_downloads, ext="*", return_df=True)
                file_list_df["modified_path"] = file_list_df["Path"].apply(
                    lambda p: p.replace(loc_downloads, "").lstrip(r"/\\")
                )
    
                obj_list = pd.merge(
                    obj_list,
                    file_list_df[["modified_path", "FileSize (in bytes)"]],
                    left_on="Key",
                    right_on="modified_path",
                    how="left"
                )
    
                obj_list = obj_list[obj_list["FileSize (in bytes)"].isnull()].reset_index(drop=True)
                obj_list = obj_list.drop(columns = ['modified_path', 
                                                    'FileSize (in bytes)'])
    
        elif sync_mode != "full_refresh":
            raise ValueError('❌ Invalid sync_mode passed. Choose from ["full_refresh", "incremental"]')
    
        # Add a tracking status
        obj_list["Status"] = "Pending"
    
        return obj_list


    def execute_download(client, object_df: pd.DataFrame, loc_save_files: str) -> pd.DataFrame:
        """
        Downloads all pending files from S3 to the specified local path.
    
        Args:
            client: Boto3 S3 client object.
            object_df (pd.DataFrame): DataFrame containing S3 object metadata with 'Pending' status.
            loc_save_files (str): Local directory path to save downloaded files.
    
        Returns:
            pd.DataFrame: Updated object_df with download statuses.
        """
    
        def download_file_s3(client, bucket_name, s3_key, dest_path) -> str:
            """
            Downloads a single file from S3 to local.
    
            Args:
                client: Boto3 client.
                bucket_name (str): S3 bucket name.
                s3_key (str): S3 object key.
                dest_path (str): Local destination file path.
    
            Returns:
                str: 'Success' or 'Failed'
            """
            try:
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                client.download_file(Bucket=bucket_name, Key=s3_key, Filename=dest_path)
                return 'Success'
            except ClientError as e:
                print(f"❌ Error downloading {s3_key}: {e}")
                return 'Failed'
    
        # Filter out files marked as 'Pending'
        pending_df = object_df[object_df['Status'] == 'Pending'].copy()
        pending_df.reset_index(drop=True, inplace=True)
    
        print(f"🚀 Starting download of {len(pending_df)} files...")
    
        # Prepare for parallel download
        with ThreadPoolExecutor(max_workers=12) as executor:
            future_to_idx = {
                executor.submit(
                    download_file_s3,
                    client,
                    row['BucketName'],
                    row['Key'],
                    os.path.join(loc_save_files, row['Key'])
                ): idx
                for idx, row in pending_df.iterrows()
            }
    
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    result = future.result()
                    pending_df.at[idx, 'Status'] = result
                except Exception as exc:
                    print(f"Unhandled error: {exc}")
                    pending_df.at[idx, 'Status'] = 'Failed'
    
        # Update original DataFrame with results
        object_df.update(pending_df[['Key', 'Status']])
    
        return object_df
