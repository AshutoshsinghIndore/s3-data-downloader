"""
S3 Downloader Pipeline
-----------------------
This script reads configuration from a YAML file and .env file,
establishes an S3 connection, fetches object metadata, filters
based on config, and downloads the relevant S3 objects locally.

Author: Your Name
"""

import os
import sys
import boto3
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, ClientError

# Add source directory to system path for module imports
sys.path.append('src')

from s3_manager import S3_Downloader
from config_loader import read_yaml_file


def main():
    """Main execution flow for downloading S3 files based on configuration."""
    
    # --- Step 1: Load environment variables ---
    load_dotenv('config/config.env')

    # --- Step 2: Load YAML Configuration ---
    config = read_yaml_file('config/default_config.yaml')
    print("✅ Configuration loaded successfully.")

    # --- Step 3: Establish AWS S3 Connection ---
    client_obj = S3_Downloader.establish_connection_s3()
    print("🔗 AWS S3 connection established.")

    # --- Step 4: Prepare (bucket, prefix) pairs ---
    bucket_prefix_pair = S3_Downloader.preprocess_bucket_prefix(
        s3_client=client_obj,
        bucket_prefix_map=config['s3']
    )
    print("📦 Bucket-Prefix pairs to process:")
    for pair in bucket_prefix_pair:
        print(f"\t- {pair}")

    # --- Step 5: Ensure download directory exists ---
    download_path = config['sync']['loc_download']
    os.makedirs(download_path, exist_ok=True)
    print(f"📁 Download directory ready: {download_path}")

    # --- Step 6: List objects from S3 ---
    s3_obj_list = S3_Downloader.list_s3_objects(
        client=client_obj,
        bucket_prefixes=bucket_prefix_pair
    )
    print(f"📄 {len(s3_obj_list)} objects listed from S3.")

    # --- Step 7: Filter object list based on config ---
    s3_obj_list = S3_Downloader.filter_obj_list(
        obj_list=s3_obj_list,
        filter_criteria=config['filters'],
        sync_mode=config['sync']['mode'],
        loc_downloads=download_path
    )
    print(f"🔍 {len(s3_obj_list)} objects after applying filters.")

    # --- Step 8: Execute download ---
    S3_Downloader.execute_download(
        client=client_obj,
        object_df=s3_obj_list,
        loc_save_files=download_path
    )
    print("✅ Download process completed.")

    # --- Step 9: Saving ParquetFile
    file_utils.save_sync_state(df = s3_obj_list, output_dir = download_path)


if __name__ == '__main__':
    main()