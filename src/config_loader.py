import yaml
from pathlib import Path
from typing import Union, Any


def read_yaml_file(filepath: Union[str, Path]) -> Any:
    """
    Reads a YAML configuration file and returns its content.

    Args:
        filepath (Union[str, Path]): Path to the YAML file.

    Returns:
        Any: Parsed YAML content as a Python dictionary, list, or scalar.

    Raises:
        FileNotFoundError: If the file does not exist.
        yaml.YAMLError: If there is a parsing error.
        Exception: For any other unexpected error.
    """
    path = Path(filepath)

    if not path.exists():
        raise FileNotFoundError(f"YAML file not found: {path}")

    try:
        with path.open('r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Failed to parse YAML file: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error reading YAML file: {e}")
