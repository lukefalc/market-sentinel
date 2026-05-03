"""DuckDB connection helpers.

This module opens the local DuckDB database used by market-sentinel. It reads
the database path from ``config/settings.yaml`` and creates the parent data
folder automatically when needed.
"""

from pathlib import Path

import duckdb

from market_sentinel.config.loader import default_config_dir, load_named_config


def get_database_path(config_dir: Path | None = None) -> Path:
    """Return the configured DuckDB database path.

    Args:
        config_dir: Optional folder containing ``settings.yaml``. Tests can pass
            a temporary folder here.

    Raises:
        ValueError: If ``database_path`` is missing or empty.
    """
    base_config_dir = (
        Path(config_dir) if config_dir is not None else default_config_dir()
    )
    settings = load_named_config("settings", base_config_dir)
    database_path = settings.get("database_path")

    if not database_path:
        raise ValueError(
            "The setting 'database_path' is missing from config/settings.yaml. "
            "Add a value such as 'data/market_sentinel.duckdb'."
        )

    path = Path(str(database_path)).expanduser()

    if not path.is_absolute():
        path = base_config_dir.parent / path

    return path


def open_duckdb_connection(config_dir: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection using the configured database path.

    The database file is created by DuckDB if it does not already exist.
    """
    database_path = get_database_path(config_dir)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        return duckdb.connect(str(database_path))
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not open the DuckDB database at "
            f"{database_path}. Check that the folder exists and is writable."
        ) from error
