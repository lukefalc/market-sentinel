"""Tests for simple moving average calculations."""

from pathlib import Path

from market_sentinel.analytics.moving_averages import (
    calculate_and_store_moving_averages,
    calculate_simple_moving_average,
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


def write_moving_average_config(config_dir: Path, periods) -> None:
    """Create a moving average config file for tests."""
    config_dir.mkdir(parents=True, exist_ok=True)
    period_lines = "\n".join(f"    - {period}" for period in periods)
    config_dir.joinpath("moving_averages.yaml").write_text(
        f"moving_averages:\n  periods:\n{period_lines}\n",
        encoding="utf-8",
    )


def open_test_database(tmp_path: Path):
    """Open a temporary DuckDB database with the project schema."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)
    write_moving_average_config(config_dir, [7, 30])
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    return connection, config_dir


def insert_security(connection, security_id: int, ticker: str) -> None:
    """Insert a test security."""
    connection.execute(
        """
        INSERT INTO securities (
            security_id,
            ticker,
            name,
            market,
            region,
            currency,
            sector
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            security_id,
            ticker,
            ticker,
            "Test Market",
            "Test Region",
            "USD",
            "Technology",
        ],
    )


def insert_daily_prices(connection, security_id: int, count: int) -> None:
    """Insert fake daily prices with steadily increasing close prices."""
    for day_number in range(1, count + 1):
        connection.execute(
            """
            INSERT INTO daily_prices (
                price_id,
                security_id,
                price_date,
                close_price
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                security_id * 1000 + day_number,
                security_id,
                f"2026-01-{day_number:02d}",
                float(day_number),
            ],
        )


def test_calculate_simple_moving_average() -> None:
    """A simple moving average should be the mean of supplied prices."""
    assert calculate_simple_moving_average([1.0, 2.0, 3.0]) == 2.0


def test_calculate_and_store_moving_averages(tmp_path: Path) -> None:
    """Latest moving averages should be stored for periods with enough data."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, 30)

        summary = calculate_and_store_moving_averages(connection, config_dir)
        saved_rows = connection.execute(
            """
            SELECT
                moving_average_period_days,
                moving_average_value,
                signal_type
            FROM moving_average_signals
            WHERE security_id = 1
            ORDER BY moving_average_period_days
            """
        ).fetchall()

        assert summary["tickers_checked"] == 1
        assert summary["signals_written"] == 2
        assert summary["skipped_tickers"] == {}
        assert saved_rows == [(7, 27.0, "SMA"), (30, 15.5, "SMA")]
    finally:
        connection.close()


def test_calculate_and_store_moving_averages_skips_short_history(
    tmp_path: Path,
) -> None:
    """Tickers with too little history should be skipped gracefully."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, 3)

        summary = calculate_and_store_moving_averages(connection, config_dir)
        saved_count = connection.execute(
            "SELECT COUNT(*) FROM moving_average_signals"
        ).fetchone()[0]

        assert summary["tickers_checked"] == 1
        assert summary["signals_written"] == 0
        assert "AAA" in summary["skipped_tickers"]
        assert saved_count == 0
    finally:
        connection.close()


def test_calculate_and_store_moving_averages_updates_existing_values(
    tmp_path: Path,
) -> None:
    """Running the calculator twice should update existing latest SMA rows."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, 30)

        calculate_and_store_moving_averages(connection, config_dir)
        calculate_and_store_moving_averages(connection, config_dir)

        saved_count = connection.execute(
            "SELECT COUNT(*) FROM moving_average_signals"
        ).fetchone()[0]

        assert saved_count == 2
    finally:
        connection.close()
