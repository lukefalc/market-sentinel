"""Tests for moving average crossover detection."""

from pathlib import Path

from market_sentinel.analytics.crossovers import (
    describe_crossover,
    detect_and_store_crossovers,
    detect_crossover,
    format_days_since_crossover,
    is_recent_crossover,
)
from market_sentinel.analytics.moving_averages import (
    calculate_and_store_moving_averages,
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


def insert_daily_prices(connection, security_id: int, prices) -> None:
    """Insert fake daily close prices."""
    for day_number, close_price in enumerate(prices, start=1):
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
                float(close_price),
            ],
        )


def test_detect_crossover_rules() -> None:
    """Bullish and bearish crossover rules should match the definition."""
    assert detect_crossover(10.0, 10.0, 11.0, 10.0) == "BULLISH_CROSSOVER"
    assert detect_crossover(11.0, 10.0, 9.0, 10.0) == "BEARISH_CROSSOVER"
    assert detect_crossover(8.0, 10.0, 9.0, 10.0) is None


def test_describe_crossover_uses_beginner_friendly_words() -> None:
    """Crossover descriptions should avoid terse technical labels."""
    assert (
        describe_crossover(7, 30, "BULLISH_CROSSOVER")
        == "7-day trend line crossed above 30-day trend line"
    )
    assert (
        describe_crossover(50, 200, "BEARISH_CROSSOVER")
        == "50-day trend line crossed below 200-day trend line"
    )


def test_format_days_since_crossover() -> None:
    """Crossover ages should read naturally in reports."""
    assert format_days_since_crossover("2026-05-04", "2026-05-04") == "Today"
    assert format_days_since_crossover("2026-05-03", "2026-05-04") == "1 day ago"
    assert format_days_since_crossover("2026-04-24", "2026-05-04") == "10 days ago"


def test_is_recent_crossover_uses_configurable_day_window() -> None:
    """Recent crossover checks should include only the configured window."""
    assert is_recent_crossover("2026-04-27", "2026-05-04", 7) is True
    assert is_recent_crossover("2026-04-26", "2026-05-04", 7) is False


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


def test_detect_crossovers_after_historical_moving_average_calculation(
    tmp_path: Path,
) -> None:
    """Crossover detection should work from dated historical SMA rows."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, [10.0] * 30 + [100.0])

        moving_average_summary = calculate_and_store_moving_averages(
            connection,
            config_dir,
        )
        crossover_summary = detect_and_store_crossovers(connection, config_dir)
        saved_row = connection.execute(
            """
            SELECT signal_date, signal_type, crossover_direction
            FROM moving_average_signals
            WHERE signal_type = 'BULLISH_CROSSOVER'
            """
        ).fetchone()

        assert moving_average_summary["signals_written"] == 27
        assert crossover_summary["crossovers_written"] == 1
        assert _normalise_row(saved_row) == (
            "2026-01-31",
            "BULLISH_CROSSOVER",
            "BULLISH_CROSSOVER",
        )
    finally:
        connection.close()


def test_detect_crossovers_scans_recent_history_and_uses_actual_dates(
    tmp_path: Path,
) -> None:
    """Detection should store actual crossover dates from the recent SMA window."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_security(connection, 2, "BBB")
        insert_security(connection, 3, "CCC")

        # AAA crosses 5 days before the latest SMA date.
        insert_sma(connection, 1, 1, "2026-04-28", 7, 9.0)
        insert_sma(connection, 2, 1, "2026-04-28", 30, 10.0)
        insert_sma(connection, 3, 1, "2026-04-29", 7, 11.0)
        insert_sma(connection, 4, 1, "2026-04-29", 30, 10.0)
        insert_sma(connection, 5, 1, "2026-05-04", 7, 12.0)
        insert_sma(connection, 6, 1, "2026-05-04", 30, 10.0)

        # BBB crosses 2 days before the latest SMA date.
        insert_sma(connection, 7, 2, "2026-05-01", 7, 11.0)
        insert_sma(connection, 8, 2, "2026-05-01", 30, 10.0)
        insert_sma(connection, 9, 2, "2026-05-02", 7, 9.0)
        insert_sma(connection, 10, 2, "2026-05-02", 30, 10.0)
        insert_sma(connection, 11, 2, "2026-05-04", 7, 8.0)
        insert_sma(connection, 12, 2, "2026-05-04", 30, 10.0)

        # CCC crossed 10 days before the latest SMA date and should be skipped.
        insert_sma(connection, 13, 3, "2026-04-23", 7, 9.0)
        insert_sma(connection, 14, 3, "2026-04-23", 30, 10.0)
        insert_sma(connection, 15, 3, "2026-04-24", 7, 11.0)
        insert_sma(connection, 16, 3, "2026-04-24", 30, 10.0)
        insert_sma(connection, 17, 3, "2026-05-04", 7, 12.0)
        insert_sma(connection, 18, 3, "2026-05-04", 30, 10.0)

        summary = detect_and_store_crossovers(connection, config_dir)
        saved_rows = connection.execute(
            """
            SELECT
                securities.ticker,
                signals.signal_date,
                signals.signal_type
            FROM moving_average_signals AS signals
            INNER JOIN securities
                ON signals.security_id = securities.security_id
            WHERE signals.signal_type IN ('BULLISH_CROSSOVER', 'BEARISH_CROSSOVER')
            ORDER BY securities.ticker
            """
        ).fetchall()

        assert summary["crossovers_written"] == 2
        assert [_normalise_row(row) for row in saved_rows] == [
            ("AAA", "2026-04-29", "BULLISH_CROSSOVER"),
            ("BBB", "2026-05-02", "BEARISH_CROSSOVER"),
        ]
    finally:
        connection.close()


def _normalise_row(row):
    """Convert date-like values in one fetched row to ISO strings."""
    return tuple(
        value.isoformat() if hasattr(value, "isoformat") else value
        for value in row
    )
