"""
Script: s3_downloader_pipeline.py

Main pipeline to download S3 files based on a YAML configuration and environment variables.

Steps:
1. Load .env for AWS credentials
2. Load YAML config (buckets, prefixes, filters)
3. Establish S3 connection
4. List and filter S3 objects
5. Download filtered objects
6. Save sync state for incremental processing

Author: Ashutosh Singh
"""

import os
import sys
import logging

from dotenv import load_dotenv
import pandas as pd
from botocore.exceptions import ClientError

# Adjust system path for local imports
sys.path.append("src")

from s3_manager import (
    establish_connection_s3,
    preprocess_bucket_prefix,
    list_s3_objects,
    filter_obj_list,
    execute_download
)
from config_loader import read_yaml_file
from file_utils import save_sync_state

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def main():
    """Main entrypoint for the S3 download pipeline."""
    try:
        # Step 1: Load environment variables
        load_dotenv("config/config.env")

        # Step 2: Load configuration from YAML
        config = read_yaml_file("config/default_config.yaml")
        logger.info("✅ Configuration loaded successfully.")

        # Step 3: Establish AWS S3 connection
        s3_client = establish_connection_s3()
        logger.info("🔗 S3 connection established.")

        # Step 4: Validate bucket/prefix pairs
        bucket_prefixes = preprocess_bucket_prefix(
            s3_client=s3_client,
            bucket_prefix_map=config["s3"]
        )
        if not bucket_prefixes:
            logger.warning("⚠️ No valid (bucket, prefix) pairs found.")
            return

        logger.info(f"📦 Valid bucket/prefix pairs: {len(bucket_prefixes)}")

        # Step 5: Ensure download directory exists
        download_path = config["sync"]["loc_download"]
        os.makedirs(download_path, exist_ok=True)
        logger.info(f"📁 Download directory: {os.path.abspath(download_path)}")

        # Step 6: List objects from S3
        s3_objects = list_s3_objects(s3_client, bucket_prefixes)
        logger.info(f"📄 Total objects listed: {len(s3_objects)}")

        if s3_objects.empty:
            logger.warning("⚠️ No objects found to process.")
            return

        # Step 7: Apply filters (extensions, filenames, incremental sync)
        filtered_objects = filter_obj_list(
            obj_list=s3_objects,
            filter_criteria=config["filters"],
            sync_mode=config["sync"]["mode"],
            loc_downloads=download_path
        )
        logger.info(f"🔍 Objects after filtering: {len(filtered_objects)}")

        if filtered_objects.empty:
            logger.info("✅ No new files to download.")
            return

        # Step 8: Download files
        execute_download(
            client=s3_client,
            obj_list=filtered_objects,
            threads=config.get("sync", {}).get("threads", 12)
        )
        logger.info("✅ Download completed successfully.")

        # Step 9: Save sync state
        save_sync_state(df=filtered_objects, output_dir=download_path)

    except ClientError as ce:
        logger.error(f"❌ AWS Client Error: {ce}")
    except Exception as e:
        logger.exception(f"❌ Pipeline execution failed: {e}")


if __name__ == "__main__":
    main()
