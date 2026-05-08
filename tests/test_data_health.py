"""Tests for data health checks."""

from datetime import date, timedelta
from pathlib import Path

from market_sentinel.analytics.data_health import (
    DATA_HEALTH_ACTION_NEEDED,
    DATA_HEALTH_OK,
    DATA_HEALTH_WARNING,
    check_data_health,
    format_data_health_line,
)
from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema


def write_settings(config_dir: Path, database_path: Path) -> None:
    """Create settings for a temporary test database."""
    config_dir.mkdir(parents=True, exist_ok=True)
    config_dir.joinpath("settings.yaml").write_text(
        f"database_path: {database_path}\n"
        "data_health_stale_price_days: 5\n",
        encoding="utf-8",
    )


def open_test_database(tmp_path: Path):
    """Open a temporary DuckDB database with the project schema."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    return connection


def insert_security(
    connection,
    security_id: int,
    ticker: str,
    market: str = "S&P 500",
) -> None:
    """Insert one fake security."""
    connection.execute(
        """
        INSERT INTO securities (security_id, ticker, name, market)
        VALUES (?, ?, ?, ?)
        """,
        [security_id, ticker, f"Example {ticker}", market],
    )


def insert_price_history(
    connection,
    security_id: int,
    start_price_id: int,
    end_date: date,
    rows: int = 180,
) -> None:
    """Insert daily close rows ending on end_date."""
    start_date = end_date - timedelta(days=rows - 1)
    for offset in range(rows):
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
                start_price_id + offset,
                security_id,
                start_date + timedelta(days=offset),
                100.0 + offset,
            ],
        )


def insert_sma(connection, signal_id: int, security_id: int, signal_date: date) -> None:
    """Insert a minimal SMA row for one security."""
    connection.execute(
        """
        INSERT INTO moving_average_signals (
            signal_id,
            security_id,
            signal_date,
            moving_average_period_days,
            moving_average_value,
            signal_type
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [signal_id, security_id, signal_date, 50, 101.0, "SMA"],
    )


def test_data_health_ok_when_prices_and_moving_averages_are_complete(
    tmp_path: Path,
) -> None:
    """Complete price and SMA coverage should be OK."""
    connection = open_test_database(tmp_path)
    try:
        insert_security(connection, 1, "AAA", "S&P 500")
        insert_price_history(connection, 1, 1, date(2026, 5, 8), rows=180)
        insert_sma(connection, 1, 1, date(2026, 5, 8))

        summary = check_data_health(connection)
    finally:
        connection.close()

    assert summary["status"] == DATA_HEALTH_OK
    assert summary["securities_checked"] == 1
    assert summary["securities_by_market"] == {"S&P 500": 1}
    assert format_data_health_line(summary) == "Data health: OK - 1 securities checked"


def test_data_health_warning_for_stale_or_short_history(tmp_path: Path) -> None:
    """Stale data or short history should warn without blocking reports."""
    connection = open_test_database(tmp_path)
    try:
        insert_security(connection, 1, "AAA", "S&P 500")
        insert_security(connection, 2, "BBB.L", "FTSE 350")
        insert_price_history(connection, 1, 1, date(2026, 5, 8), rows=180)
        insert_price_history(connection, 2, 1000, date(2026, 4, 25), rows=180)
        insert_sma(connection, 1, 1, date(2026, 5, 8))
        insert_sma(connection, 2, 2, date(2026, 4, 25))

        summary = check_data_health(connection, stale_price_days=5)
    finally:
        connection.close()

    assert summary["status"] == DATA_HEALTH_WARNING
    assert [row["ticker"] for row in summary["stale_price_tickers"]] == ["BBB.L"]
    assert "1 stale ticker" in format_data_health_line(summary)


def test_data_health_action_needed_for_missing_price_history(tmp_path: Path) -> None:
    """Securities with no prices should be visible as action needed."""
    connection = open_test_database(tmp_path)
    try:
        insert_security(connection, 1, "AAA", "S&P 500")

        summary = check_data_health(connection)
    finally:
        connection.close()

    assert summary["status"] == DATA_HEALTH_ACTION_NEEDED
    assert [row["ticker"] for row in summary["no_price_data"]] == ["AAA"]
    assert "1 missing price history" in format_data_health_line(summary)
