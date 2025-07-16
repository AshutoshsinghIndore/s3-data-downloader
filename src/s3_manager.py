import os
import hashlib
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple, Optional

import boto3
import pandas as pd
import numpy as np
from boto3.s3.transfer import TransferConfig
import logging
from typing import List, Tuple
from botocore.client import BaseClient
from tqdm import tqdm

logger = logging.getLogger(__name__)
from file_utils import file_utils
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError

class S3_Downloader:
    """
    A utility class to manage downloading video logs from AWS S3 buckets.
    """

    def establish_connection_s3():
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
        valid_bucket_prefixes = []

        for bucket, prefixes in bucket_prefix_map.items():
            try:
                s3_client.head_bucket(Bucket=bucket)
            except ClientError as e:
                logger.error(f"❌ Bucket '{bucket}' is not accessible: {e.response['Error']['Message']}")
                continue

            if not prefixes:
                valid_bucket_prefixes.append((bucket, '*'))
                continue

            for prefix in prefixes:
                try:
                    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
                    if 'Contents' in response:
                        valid_bucket_prefixes.append((bucket, prefix))
                    else:
                        logger.warning(f"⚠️ No objects found for prefix '{prefix}' in bucket '{bucket}'. Skipping.")
                except ClientError as e:
                    logger.error(f"❌ Failed to validate prefix '{prefix}' in bucket '{bucket}': {e.response['Error']['Message']}")

        return valid_bucket_prefixes

    def list_s3_objects(client: BaseClient, bucket_prefixes: List[Tuple[str, str]]) -> pd.DataFrame:
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

        include_exts = filter_criteria.get("include_extensions")
        if include_exts is not None:
            obj_list = obj_list[obj_list["ext"].isin(include_exts)].reset_index(drop=True)

        exclude_files = filter_criteria.get("exclude_files")
        if exclude_files is not None:
            obj_list = obj_list[~obj_list["FileName"].isin(exclude_files)].reset_index(drop=True)

        if sync_mode == "incremental":
            sync_state_path = os.path.join(loc_downloads, "sync_state.parquet")
            download_files = os.listdir(loc_downloads)

            if "sync_state.parquet" in download_files:
                last_sync_df = pd.read_parquet(sync_state_path)
                last_ts = last_sync_df["LastModified"].max()
                obj_list = obj_list[obj_list["LastModified"] > last_ts].reset_index(drop=True)

            elif not download_files:
                pass
            else:
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
                obj_list = obj_list.drop(columns=["modified_path", "FileSize (in bytes)"])

        elif sync_mode != "full_refresh":
            raise ValueError('❌ Invalid sync_mode passed. Choose from ["full_refresh", "incremental"]')

        obj_list["Status"] = "Pending"

        # Adding Destination Path
        obj_list['Dest_Path'] = obj_list['Key'].apply(lambda x: os.path.join(os.path.abspath(loc_downloads), x))
        return obj_list
    @staticmethod
    def download_file_s3(client, bucket_name, s3_Obj_Key, object_size, Dest_file_path):
        os.makedirs(os.path.dirname(Dest_file_path), exist_ok=True)
        with tqdm(total=object_size, unit="B", unit_scale=True, ncols=100, desc=f"Downloading: {s3_Obj_Key[:-35]}...: ", leave=False) as inner_pbar:
            client.download_file(
                Bucket=bucket_name,
                Key=s3_Obj_Key,
                Filename=Dest_file_path,
                Callback=lambda bytes_transferred: inner_pbar.update(bytes_transferred)
            )

    def execute_download(client, obj_list: pd.DataFrame, threads=12):
        BucketName_ObjKeys_DestPath = obj_list[['BucketName', 'Key', 'Size', 'Dest_Path']].values.tolist()
    
        with tqdm(total=len(BucketName_ObjKeys_DestPath), desc='Downloading Files...', ncols=100) as pbar:
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = [
                    executor.submit(
                        S3_Downloader.download_file_s3,  # If it's an instance method
                        client,
                        bucket_name,
                        obj_key,
                        object_size,
                        dest_path
                    )
                    for bucket_name, obj_key, object_size, dest_path in BucketName_ObjKeys_DestPath
                ]
    
                for future in as_completed(futures):
                    pbar.update(1)
                    future.result()  # yield only if download_file_s3 returns something