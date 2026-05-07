"""Tests for loading stock universe CSV files."""

from pathlib import Path

import pytest

from market_sentinel.data.universe_loader import (
    default_universe_files,
    load_universe_csv,
    load_universe_files,
)
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


def test_load_universe_csv_raises_clear_error_for_missing_required_values(
    tmp_path: Path,
) -> None:
    """Rows missing required values should produce a friendly error."""
    csv_path = tmp_path / "universes" / "bad_values.csv"
    write_csv(
        csv_path,
        "\n".join(
            [
                "ticker,name,market,region,currency,sector",
                ",Example A,S&P 500,United States,USD,Technology",
            ]
        ),
    )
    connection = open_test_database(tmp_path)

    try:
        with pytest.raises(ValueError, match="missing required values"):
            load_universe_csv(connection, csv_path)
    finally:
        connection.close()


def test_load_universe_csv_raises_clear_error_for_malformed_row(
    tmp_path: Path,
) -> None:
    """Rows with too many values should produce a friendly error."""
    csv_path = tmp_path / "universes" / "malformed.csv"
    write_csv(
        csv_path,
        "\n".join(
            [
                "ticker,name,market,region,currency,sector",
                "AAA,Example A,S&P 500,United States,USD,Technology,Extra",
            ]
        ),
    )
    connection = open_test_database(tmp_path)

    try:
        with pytest.raises(ValueError, match="too many values"):
            load_universe_csv(connection, csv_path)
    finally:
        connection.close()


def test_default_universe_files_prefers_ftse350_over_ftse100(
    tmp_path: Path,
) -> None:
    """The default load should avoid loading both UK universe files."""
    universe_dir = tmp_path / "universes"
    write_csv(
        universe_dir / "sp_500.csv",
        "ticker,name,market,region,currency,sector\n",
    )
    write_csv(
        universe_dir / "ftse_100.csv",
        "ticker,name,market,region,currency,sector\n",
    )
    write_csv(
        universe_dir / "ftse_350.csv",
        "ticker,name,market,region,currency,sector\n",
    )

    files = default_universe_files(universe_dir)

    assert [path.name for path in files] == ["sp_500.csv", "ftse_350.csv"]


def test_load_universe_files_avoids_duplicate_ftse100_and_ftse350(
    tmp_path: Path,
) -> None:
    """Loading defaults should store UK overlap once as FTSE 350."""
    universe_dir = tmp_path / "universes"
    write_csv(
        universe_dir / "sp_500.csv",
        "\n".join(
            [
                "ticker,name,market,region,currency,sector",
                "AAPL,Apple,S&P 500,US,USD,Technology",
            ]
        ),
    )
    write_csv(
        universe_dir / "ftse_100.csv",
        "\n".join(
            [
                "ticker,name,market,region,currency,sector",
                "HSBA.L,HSBC Holdings,FTSE 100,UK,GBP,Banks",
            ]
        ),
    )
    write_csv(
        universe_dir / "ftse_350.csv",
        "\n".join(
            [
                "ticker,name,market,region,currency,sector",
                "HSBA.L,HSBC Holdings,FTSE 350,UK,GBP,Banks",
            ]
        ),
    )
    connection = open_test_database(tmp_path)

    try:
        loaded_counts = load_universe_files(
            connection,
            default_universe_files(universe_dir),
        )
        saved_rows = connection.execute(
            """
            SELECT ticker, market
            FROM securities
            ORDER BY ticker
            """
        ).fetchall()
    finally:
        connection.close()

    assert set(loaded_counts) == {"sp_500.csv", "ftse_350.csv"}
    assert saved_rows == [("AAPL", "S&P 500"), ("HSBA.L", "FTSE 350")]
