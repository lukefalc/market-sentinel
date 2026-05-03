"""Tests for loading stock universe CSV files."""

from pathlib import Path

import pytest

from market_sentinel.data.universe_loader import load_universe_csv
from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema


def write_settings(config_dir: Path, database_path: Path) -> None:
    """Create a minimal settings file pointing at a test database."""
    config_dir.mkdir(parents=True, exist_ok=True)
    config_dir.joinpath("settings.yaml").write_text(
        f"database_path: {database_path}\n",
        encoding="utf-8",
    )


def write_csv(csv_path: Path, content: str) -> None:
    """Write a test stock universe CSV file."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(content, encoding="utf-8")


def open_test_database(tmp_path: Path):
    """Open a temporary DuckDB database with the project schema."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    return connection


def test_load_universe_csv_inserts_and_updates_securities(tmp_path: Path) -> None:
    """Loading a universe CSV should insert rows and update existing tickers."""
    csv_path = tmp_path / "universes" / "sample.csv"
    write_csv(
        csv_path,
        "\n".join(
            [
                "ticker,name,market,region,currency,sector",
                "AAA,Example A,S&P 500,United States,USD,Technology",
                "BBB,Example B,FTSE 350,United Kingdom,GBP,Energy",
            ]
        ),
    )
    connection = open_test_database(tmp_path)

    try:
        row_count = load_universe_csv(connection, csv_path)

        assert row_count == 2
        assert connection.execute("SELECT COUNT(*) FROM securities").fetchone()[0] == 2

        write_csv(
            csv_path,
            "\n".join(
                [
                    "ticker,name,market,region,currency,sector",
                    "AAA,Example A Updated,S&P 500,United States,USD,Healthcare",
                ]
            ),
        )

        row_count = load_universe_csv(connection, csv_path)
        saved_row = connection.execute(
            """
            SELECT name, sector
            FROM securities
            WHERE ticker = 'AAA'
            """
        ).fetchone()

        assert row_count == 1
        assert saved_row == ("Example A Updated", "Healthcare")
        assert connection.execute("SELECT COUNT(*) FROM securities").fetchone()[0] == 2
    finally:
        connection.close()


def test_load_universe_csv_raises_clear_error_for_missing_file(
    tmp_path: Path,
) -> None:
    """Missing CSV files should produce a friendly error message."""
    connection = open_test_database(tmp_path)

    try:
        with pytest.raises(
            FileNotFoundError,
            match="Stock universe CSV file not found",
        ):
            load_universe_csv(connection, tmp_path / "missing.csv")
    finally:
        connection.close()


def test_load_universe_csv_raises_clear_error_for_missing_columns(
    tmp_path: Path,
) -> None:
    """CSV files missing required columns should produce a friendly error."""
    csv_path = tmp_path / "universes" / "bad.csv"
    write_csv(
        csv_path,
        "\n".join(
            [
                "ticker,name,market,currency,sector",
                "AAA,Example A,S&P 500,USD,Technology",
            ]
        ),
    )
    connection = open_test_database(tmp_path)

    try:
        with pytest.raises(ValueError, match="missing required columns"):
            load_universe_csv(connection, csv_path)
    finally:
        connection.close()
