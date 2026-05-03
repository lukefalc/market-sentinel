"""YAML configuration loading helpers.

This module loads the small YAML files that control the project. It does not
download market data or run analysis; it only reads configuration files and
returns their contents as Python dictionaries.
"""

from pathlib import Path
from typing import Any

import yaml

CONFIG_FILES = {
    "settings": "settings.yaml",
    "markets": "markets.yaml",
    "moving_averages": "moving_averages.yaml",
    "alert_rules": "alert_rules.yaml",
    "watchlist": "watchlist.yaml",
}


def default_config_dir() -> Path:
    """Return the default project configuration directory."""
    project_root = Path(__file__).resolve().parents[3]
    return project_root / "config"


def load_yaml_config(file_path: Path) -> dict[str, Any]:
    """Load one YAML config file and return it as a dictionary.

    Args:
        file_path: Path to the YAML file.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the YAML is invalid or does not contain a dictionary.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(
            "Config file not found: "
            f"{file_path}. Create this file or check that the path is correct."
        )

    try:
        with file_path.open("r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
    except yaml.YAMLError as error:
        raise ValueError(
            "Invalid YAML in config file: "
            f"{file_path}. Check indentation, colons, and list formatting."
        ) from error

    if config is None:
        return {}

    if not isinstance(config, dict):
        raise ValueError(
            "Config file must contain a YAML dictionary at the top level: "
            f"{file_path}."
        )

    return config


def load_named_config(name: str, config_dir: Path | None = None) -> dict[str, Any]:
    """Load one known project config by name.

    Args:
        name: One of settings, markets, moving_averages, alert_rules, watchlist.
        config_dir: Optional folder containing the YAML config files.
    """
    if name not in CONFIG_FILES:
        known_names = ", ".join(sorted(CONFIG_FILES))
        raise ValueError(f"Unknown config name: {name}. Expected one of: {known_names}.")

    base_dir = Path(config_dir) if config_dir is not None else default_config_dir()
    return load_yaml_config(base_dir / CONFIG_FILES[name])


def load_all_configs(config_dir: Path | None = None) -> dict[str, dict[str, Any]]:
    """Load all project YAML configs into one dictionary."""
    base_dir = Path(config_dir) if config_dir is not None else default_config_dir()

    return {
        config_name: load_yaml_config(base_dir / file_name)
        for config_name, file_name in CONFIG_FILES.items()
    }
