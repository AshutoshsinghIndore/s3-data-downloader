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


    def preprocess_bucket_prefix(bucket_prefix_map: dict) -> List[Tuple[str, str]]:
        """
        Converts a dictionary of {bucket: [prefixes]} into a list of (bucket, prefix) pairs.
        """
        bucket_prefix = []
        for bucket, prefixes in bucket_prefix_map.items():
            if not prefixes:
                bucket_prefix.append((bucket, '*'))
            else:
                for prefix in prefixes:
                    bucket_prefix.append((bucket, prefix))
        return bucket_prefix

    def list_s3_objects(self, client, bucket_prefix: List[Tuple[str, str]]) -> pd.DataFrame:
        """
        Lists all objects for the given (bucket, prefix) pairs.

        Returns:
            pd.DataFrame: Object metadata including Key, Size, LastModified, etc.
        """
        object_df = pd.DataFrame()

        for bucket_name, prefix in bucket_prefix:
            paginator = client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                contents = page.get('Contents', [])
                if not contents:
                    continue

                df = pd.DataFrame({
                    'Key': [obj['Key'] for obj in contents],
                    'LastModified': [obj['LastModified'] for obj in contents],
                    'Size': [obj['Size'] for obj in contents],
                    'FileName': [os.path.basename(obj['Key']) for obj in contents],
                    'ext': [os.path.splitext(obj['Key'])[-1] for obj in contents],
                    'ETag': [obj['ETag'] for obj in contents],
                })
                df['BucketName'] = bucket_name
                df['PrefixName'] = prefix

                object_df = pd.concat([object_df, df], ignore_index=True)

        return object_df

    def check_md5(self, file_path: str, original_md5: str) -> bool:
        """
        Validates MD5 checksum of a file against the original hash.
        """
        with open(file_path, 'rb') as f:
            file_data = f.read()
            computed_md5 = hashlib.md5(file_data).hexdigest()
        return computed_md5 == original_md5

    def _download_file(self, client, bucket_name: str, key: str, dest_path: str):
        """
        Internal helper to download a single file from S3.
        """
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        client.download_file(bucket_name, key, dest_path)

    def execute_download(self, client, object_df: pd.DataFrame, local_path: str):
        """
        Download all 'Pending' files from object_df to the local path using threading.
        """
