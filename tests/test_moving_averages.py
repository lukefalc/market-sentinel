"""Tests for simple moving average calculations."""

from pathlib import Path

from scripts import backfill_moving_averages
from market_sentinel.analytics.moving_averages import (
    calculate_and_store_incremental_moving_averages,
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


def insert_daily_price(
    connection,
    security_id: int,
    price_id: int,
    price_date: str,
    close_price: float,
) -> None:
    """Insert one fake daily price."""
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
        [price_id, security_id, price_date, close_price],
    )


def test_calculate_simple_moving_average() -> None:
    """A simple moving average should be the mean of supplied prices."""
    assert calculate_simple_moving_average([1.0, 2.0, 3.0]) == 2.0


def test_calculate_and_store_moving_averages(tmp_path: Path) -> None:
    """Dated moving averages should be stored for periods with enough data."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, 30)

        summary = calculate_and_store_moving_averages(connection, config_dir)
        saved_rows = connection.execute(
            """
            SELECT
                signal_date,
                moving_average_period_days,
                moving_average_value,
                signal_type
            FROM moving_average_signals
            WHERE security_id = 1
            ORDER BY signal_date, moving_average_period_days
            """
        ).fetchall()

        assert summary["tickers_checked"] == 1
        assert summary["signals_written"] == 25
        assert summary["skipped_tickers"] == {}
        assert _normalise_rows(saved_rows)[0] == ("2026-01-07", 7, 4.0, "SMA")
        assert _normalise_rows(saved_rows[-2:]) == [
            ("2026-01-30", 7, 27.0, "SMA"),
            ("2026-01-30", 30, 15.5, "SMA"),
        ]
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
    """Running the calculator twice should not duplicate dated SMA rows."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, 30)

        calculate_and_store_moving_averages(connection, config_dir)
        calculate_and_store_moving_averages(connection, config_dir)

        saved_count = connection.execute(
            "SELECT COUNT(*) FROM moving_average_signals"
        ).fetchone()[0]

        assert saved_count == 25
    finally:
        connection.close()


def test_calculate_and_store_moving_averages_has_two_latest_dates(
    tmp_path: Path,
) -> None:
    """Historical storage should provide enough dates for crossover checks."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, 31)

        calculate_and_store_moving_averages(connection, config_dir)
        dates_with_both_periods = connection.execute(
            """
            SELECT signal_date
            FROM moving_average_signals
            WHERE security_id = 1
              AND signal_type = 'SMA'
              AND moving_average_period_days IN (7, 30)
            GROUP BY signal_date
            HAVING COUNT(DISTINCT moving_average_period_days) = 2
            ORDER BY signal_date DESC
            LIMIT 2
            """
        ).fetchall()

        assert _normalise_rows(dates_with_both_periods) == [
            ("2026-01-31",),
            ("2026-01-30",),
        ]
    finally:
        connection.close()


def test_calculate_and_store_moving_averages_limits_stored_history(
    tmp_path: Path,
) -> None:
    """Only recent SMA dates should be stored while using earlier prices."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, 31)

        summary = calculate_and_store_moving_averages(
            connection,
            config_dir,
            history_days=2,
        )
        saved_rows = connection.execute(
            """
            SELECT signal_date, moving_average_period_days, moving_average_value
            FROM moving_average_signals
            WHERE security_id = 1
            ORDER BY signal_date, moving_average_period_days
            """
        ).fetchall()

        assert summary["signals_written"] == 4
        assert _normalise_rows(saved_rows) == [
            ("2026-01-30", 7, 27.0),
            ("2026-01-30", 30, 15.5),
            ("2026-01-31", 7, 28.0),
            ("2026-01-31", 30, 16.5),
        ]
    finally:
        connection.close()


def test_calculate_and_store_moving_averages_prints_progress(
    tmp_path: Path,
    capsys,
) -> None:
    """Moving average calculation should show visible ticker progress."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_security(connection, 2, "BBB")
        insert_daily_prices(connection, 1, 30)
        insert_daily_prices(connection, 2, 3)

        summary = calculate_and_store_moving_averages(connection, config_dir)
    finally:
        connection.close()

    captured = capsys.readouterr()

    assert summary["tickers_checked"] == 2
    assert "Total tickers for moving averages: 2" in captured.out
    assert "Processing ticker 1 of 2: AAA" in captured.out
    assert "Rows written for AAA: 25" in captured.out
    assert "Processing ticker 2 of 2: BBB" in captured.out
    assert "Rows written for BBB: 0" in captured.out
    assert "Moving average calculation summary" in captured.out


def test_calculate_and_store_incremental_moving_averages_only_recent_rows(
    tmp_path: Path,
) -> None:
    """Incremental SMA mode should store only recent dates."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, 31)

        summary = calculate_and_store_incremental_moving_averages(
            connection,
            config_dir,
            recent_days=2,
        )
        saved_rows = connection.execute(
            """
            SELECT signal_date, moving_average_period_days, moving_average_value
            FROM moving_average_signals
            WHERE security_id = 1
            ORDER BY signal_date, moving_average_period_days
            """
        ).fetchall()
    finally:
        connection.close()

    assert summary["mode"] == "incremental"
    assert summary["limited_backfill_tickers"] == 1
    assert summary["incremental_tickers"] == 0
    assert summary["signals_written"] == 4
    assert _normalise_rows(saved_rows) == [
        ("2026-01-30", 7, 27.0),
        ("2026-01-30", 30, 15.5),
        ("2026-01-31", 7, 28.0),
        ("2026-01-31", 30, 16.5),
    ]


def test_calculate_and_store_incremental_moving_averages_upserts_rows(
    tmp_path: Path,
) -> None:
    """Incremental SMA mode should not duplicate existing recent rows."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, 31)

        calculate_and_store_incremental_moving_averages(
            connection,
            config_dir,
            recent_days=2,
        )
        calculate_and_store_incremental_moving_averages(
            connection,
            config_dir,
            recent_days=2,
        )
        saved_count = connection.execute(
            "SELECT COUNT(*) FROM moving_average_signals"
        ).fetchone()[0]
    finally:
        connection.close()

    assert saved_count == 4


def test_incremental_moving_averages_calculates_recent_and_missing_rows(
    tmp_path: Path,
) -> None:
    """Incremental mode should update recent rows and add newly missing dates."""
    connection, config_dir = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_daily_prices(connection, 1, 31)
        calculate_and_store_moving_averages(connection, config_dir)
        insert_daily_price(connection, 1, 2000, "2026-02-01", 32.0)

        summary = calculate_and_store_incremental_moving_averages(
            connection,
            config_dir,
            recent_days=2,
            price_history_buffer_days=5,
        )
        saved_count = connection.execute(
            "SELECT COUNT(*) FROM moving_average_signals"
        ).fetchone()[0]
        latest_dates = connection.execute(
            """
            SELECT signal_date
            FROM moving_average_signals
            WHERE security_id = 1
              AND signal_type = 'SMA'
              AND moving_average_period_days IN (7, 30)
            GROUP BY signal_date
            HAVING COUNT(DISTINCT moving_average_period_days) = 2
            ORDER BY signal_date DESC
            LIMIT 2
            """
        ).fetchall()
    finally:
        connection.close()

    assert summary["incremental_tickers"] == 1
    assert summary["limited_backfill_tickers"] == 0
    assert summary["signals_written"] == 4
    assert saved_count == 29
    assert _normalise_rows(latest_dates) == [
        ("2026-02-01",),
        ("2026-01-31",),
    ]


def test_full_moving_average_backfill_script_exists() -> None:
    """A separate full/backfill moving-average script should remain available."""
    assert hasattr(backfill_moving_averages, "main")
    assert hasattr(backfill_moving_averages, "load_moving_average_history_days")


def _normalise_rows(rows):
    """Convert date-like values in fetched rows to ISO strings."""
    return [
        tuple(
            value.isoformat() if hasattr(value, "isoformat") else value
            for value in row
        )
        for row in rows
    ]
