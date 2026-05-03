"""Tests for DuckDB database setup."""

from pathlib import Path

import pytest

from market_sentinel.database.connection import (
    get_database_path,
    open_duckdb_connection,
)
from market_sentinel.database.schema import initialise_database_schema

EXPECTED_TABLES = {
    "securities",
    "daily_prices",
    "dividends",
    "fundamentals",
    "moving_average_signals",
    "dividend_metrics",
    "alerts",
    "report_runs",
}


def write_settings(config_dir: Path, database_path: Path) -> None:
    """Create a minimal settings file pointing at a test database."""
    config_dir.mkdir()
    config_dir.joinpath("settings.yaml").write_text(
        f"database_path: {database_path}\n",
        encoding="utf-8",
    )


def test_open_duckdb_connection_creates_parent_folder(tmp_path: Path) -> None:
    """Opening a connection should create the parent data folder automatically."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)

    connection = open_duckdb_connection(config_dir)
    try:
        connection.execute("SELECT 1")
    finally:
        connection.close()

    assert database_path.parent.exists()
    assert database_path.exists()
    assert get_database_path(config_dir) == database_path


def test_initialise_database_schema_creates_expected_tables(tmp_path: Path) -> None:
    """Schema initialisation should create all required tables."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)

    connection = open_duckdb_connection(config_dir)
    try:
        initialise_database_schema(connection)
        tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
    finally:
        connection.close()

    assert EXPECTED_TABLES.issubset(tables)


def test_initialise_database_schema_can_run_more_than_once(tmp_path: Path) -> None:
    """Schema setup should be safe to run repeatedly."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)

    connection = open_duckdb_connection(config_dir)
    try:
        initialise_database_schema(connection)
        initialise_database_schema(connection)

        table_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchone()[0]
    finally:
        connection.close()

    assert table_count >= len(EXPECTED_TABLES)


def test_get_database_path_raises_clear_error_when_setting_is_missing(
    tmp_path: Path,
) -> None:
    """A missing database_path setting should explain how to fix the config."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_dir.joinpath("settings.yaml").write_text(
        "environment: test\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="database_path"):
        get_database_path(config_dir)
