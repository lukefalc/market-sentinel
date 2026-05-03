"""Tests for YAML configuration loading."""

from pathlib import Path

import pytest

from market_sentinel.config.loader import CONFIG_FILES, load_all_configs


def write_config_files(config_dir: Path, content: str = "enabled: true\n") -> None:
    """Write all required config files for loader tests."""
    config_dir.mkdir(parents=True, exist_ok=True)

    for file_name in CONFIG_FILES.values():
        (config_dir / file_name).write_text(content, encoding="utf-8")


def test_load_all_configs_successfully(tmp_path: Path) -> None:
    """All required YAML files should load into dictionaries."""
    write_config_files(tmp_path)

    configs = load_all_configs(tmp_path)

    assert set(configs) == {
        "settings",
        "markets",
        "moving_averages",
        "alert_rules",
        "watchlist",
    }
    assert configs["settings"] == {"enabled": True}
    assert configs["markets"] == {"enabled": True}
    assert configs["moving_averages"] == {"enabled": True}
    assert configs["alert_rules"] == {"enabled": True}
    assert configs["watchlist"] == {"enabled": True}


def test_load_all_configs_raises_clear_error_for_missing_file(
    tmp_path: Path,
) -> None:
    """Missing files should produce a friendly error message."""
    write_config_files(tmp_path)
    (tmp_path / "watchlist.yaml").unlink()

    with pytest.raises(FileNotFoundError, match="Config file not found"):
        load_all_configs(tmp_path)


def test_load_all_configs_raises_clear_error_for_invalid_yaml(
    tmp_path: Path,
) -> None:
    """Invalid YAML should produce a friendly error message."""
    write_config_files(tmp_path)
    (tmp_path / "settings.yaml").write_text(
        "settings: [unclosed\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid YAML in config file"):
        load_all_configs(tmp_path)
