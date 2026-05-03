"""Tests for moving average crossover detection."""

from pathlib import Path

from market_sentinel.analytics.crossovers import (
    detect_and_store_crossovers,
    detect_crossover,
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


def write_crossover_config(config_dir: Path) -> None:
    """Create a crossover config file for tests."""
    config_dir.mkdir(parents=True, exist_ok=True)
    config_dir.joinpath("moving_averages.yaml").write_text(
        "\n".join(
            [
                "moving_averages:",
                "  periods:",
                "    - 7",
                "    - 30",
                "  crossover_pairs:",
                "    - short_period_days: 7",
                "      long_period_days: 30",
            ]
        ),
        encoding="utf-8",
    )


def open_test_database(tmp_path: Path):
    """Open a temporary DuckDB database with the project schema."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)
    write_crossover_config(config_dir)
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
            market
        )
        VALUES (?, ?, ?, ?)
        """,
        [security_id, ticker, ticker, "Test Market"],
    )


def insert_sma(
    connection,
    signal_id: int,
    security_id: int,
    signal_date: str,
    period: int,
    value: float,
) -> None:
    """Insert a fake SMA value."""
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
        [signal_id, security_id, signal_date, period, value, "SMA"],
    )


def test_detect_crossover_rules() -> None:
    """Bullish and bearish crossover rules should match the definition."""
    assert detect_crossover(10.0, 10.0, 11.0, 10.0) == "BULLISH_CROSSOVER"
    assert detect_crossover(11.0, 10.0, 9.0, 10.0) == "BEARISH_CROSSOVER"
    assert detect_crossover(8.0, 10.0, 9.0, 10.0) is None


def test_detect_and_store_bullish_crossover(tmp_path: Path) -> None:
    """A bullish crossover should be stored in moving_average_signals."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_sma(connection, 1, 1, "2026-01-01", 7, 9.0)
        insert_sma(connection, 2, 1, "2026-01-01", 30, 10.0)
        insert_sma(connection, 3, 1, "2026-01-02", 7, 11.0)
        insert_sma(connection, 4, 1, "2026-01-02", 30, 10.0)

        summary = detect_and_store_crossovers(connection, config_dir)
        saved_row = connection.execute(
            """
            SELECT
                moving_average_period_days,
                comparison_period_days,
                signal_type,
                crossover_direction
            FROM moving_average_signals
            WHERE signal_type = 'BULLISH_CROSSOVER'
            """
        ).fetchone()

        assert summary["crossovers_written"] == 1
        assert saved_row == (7, 30, "BULLISH_CROSSOVER", "BULLISH_CROSSOVER")
    finally:
        connection.close()


def test_detect_and_store_bearish_crossover(tmp_path: Path) -> None:
    """A bearish crossover should be stored in moving_average_signals."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_sma(connection, 1, 1, "2026-01-01", 7, 11.0)
        insert_sma(connection, 2, 1, "2026-01-01", 30, 10.0)
        insert_sma(connection, 3, 1, "2026-01-02", 7, 9.0)
        insert_sma(connection, 4, 1, "2026-01-02", 30, 10.0)

        summary = detect_and_store_crossovers(connection, config_dir)
        saved_type = connection.execute(
            """
            SELECT signal_type
            FROM moving_average_signals
            WHERE signal_type = 'BEARISH_CROSSOVER'
            """
        ).fetchone()[0]

        assert summary["crossovers_written"] == 1
        assert saved_type == "BEARISH_CROSSOVER"
    finally:
        connection.close()


def test_detect_crossovers_skips_missing_values(tmp_path: Path) -> None:
    """Missing matching SMA values should be skipped gracefully."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_sma(connection, 1, 1, "2026-01-01", 7, 9.0)
        insert_sma(connection, 2, 1, "2026-01-02", 7, 11.0)

        summary = detect_and_store_crossovers(connection, config_dir)
        saved_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM moving_average_signals
            WHERE signal_type LIKE '%CROSSOVER'
            """
        ).fetchone()[0]

        assert summary["crossovers_written"] == 0
        assert "AAA:7/30" in summary["skipped"]
        assert saved_count == 0
    finally:
        connection.close()
