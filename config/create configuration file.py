import logging
from pathlib import Path
from typing import Union

# Default YAML content
YAML_CONTENT = """\
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
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        handler = logging.StreamHandler()
        formatter = logging.Formatter('[%(levelname)s] %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def create_yaml_file(
    path: Union[str, Path] = "default_config.yaml",
    content: str = YAML_CONTENT
) -> None:
    logger = setup_logger()
    output_path = Path.cwd() / Path(path)

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.exists():
            logger.warning(f"File already exists: {output_path}")
        else:
            logger.info(f"Creating YAML file: {output_path}")

        with output_path.open(mode='w', encoding='utf-8') as file:
            file.write(content)

        logger.info("YAML configuration file created successfully.")

    except Exception as e:
        logger.error(f"Error writing YAML file: {e}")
        raise


# Always execute on import or run
create_yaml_file()
