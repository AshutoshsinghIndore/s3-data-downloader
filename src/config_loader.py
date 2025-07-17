"""
Module: config_loader.py

Utility to load configuration files in YAML format.
Used for managing ETL, pipeline, and environment configurations.

Author: Ashutosh Singh
"""

import yaml
import logging
from pathlib import Path
from typing import Union, Any

# Set up module-level logger
logger = logging.getLogger(__name__)


def read_yaml_file(filepath: Union[str, Path]) -> Any:
    """
    Reads a YAML configuration file and returns its content.

    Args:
        filepath (Union[str, Path]): Path to the YAML file.

    Returns:
        Any: Parsed YAML content (typically a dictionary).

    Raises:
        FileNotFoundError: If the specified file does not exist.
        yaml.YAMLError: If the YAML file has syntax/format issues.
        Exception: For any other unexpected errors.

    Example:
        >>> config = read_yaml_file("config/default_config.yaml")
        >>> print(config["s3"]["bucket_name"])
    """
    path = Path(filepath)

    if not path.is_file():
        raise FileNotFoundError(f"❌ YAML file not found: {path.resolve()}")

    try:
        with path.open("r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
            logger.info(f"✅ YAML config loaded: {path.name}")
            return config
    except yaml.YAMLError as e:
        logger.error(f"❌ YAML parsing failed for {path.name}: {e}")
        raise
    except Exception as e:
        logger.exception(f"❌ Unexpected error reading {path.name}")
        raise
