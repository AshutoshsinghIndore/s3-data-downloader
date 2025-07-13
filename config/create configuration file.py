import os
import logging
from pathlib import Path

# Default YAML content
YAML_CONTENT = """
aws:
  region: us-east-1
  access_key_id: YOUR_ACCESS_KEY_ID
  secret_access_key: YOUR_SECRET_ACCESS_KEY

s3:
  bucket_name: example-bucket
  prefix: sample-data/
  file_types:
    - ".csv"
    - ".json"
  recursive: true
  sync_mode: true

download:
  destination_folder: ./downloads
  overwrite_existing: false

logging:
  level: INFO
  log_to_file: true
  log_file_path: logs/download.log
"""

def setup_logger(name: str = "config_writer", level: str = "INFO") -> logging.Logger:
    """
    Sets up a logger for the module.

    Args:
        name (str): Logger name.
        level (str): Logging level.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
        logger.addHandler(console_handler)

    return logger


def create_yaml_file(path: Union[str, Path] = 'config/default_config.yaml', content: str = YAML_CONTENT) -> None:
    """
    Creates a YAML configuration file at the specified path.

    Args:
        path (str or Path): Destination path for the YAML file.
        content (str): YAML string to write.
    """
    logger = setup_logger()

    try:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists():
            logger.warning(f"File already exists at: {path}")
        else:
            logger.info(f"Creating YAML config at: {path}")

        with path.open('w', encoding='utf-8') as f:
            f.write(content)

        logger.info("YAML config file created successfully.")

    except Exception as e:
        logger.error(f"Failed to create YAML file: {e}")
        raise


if __name__ == '__main__':
    create_yaml_file()
