"""Tests for dividend trap risk flags."""

from pathlib import Path

from market_sentinel.analytics.risk_flags import (
    calculate_and_store_risk_flags,
    evaluate_dividend_risk,
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


def open_test_database(tmp_path: Path):
    """Open a temporary DuckDB database with the project schema."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    return connection


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


def insert_price(connection, security_id: int, close_price: float) -> None:
    """Insert a latest close price."""
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
        [security_id, security_id, "2026-05-04", close_price],
    )


def insert_sma_200(connection, security_id: int, value: float) -> None:
    """Insert a latest 200-day SMA."""
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
        [security_id, security_id, "2026-05-04", 200, value, "SMA"],
    )


def insert_dividend_metric(
    connection,
    security_id: int,
    trailing_annual_dividend,
    dividend_yield,
) -> None:
    """Insert a dividend metric row."""
    connection.execute(
        """
        INSERT INTO dividend_metrics (
            metric_id,
            security_id,
            metric_date,
            trailing_annual_dividend,
            dividend_yield
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            security_id,
            security_id,
            "2026-05-04",
            trailing_annual_dividend,
            dividend_yield,
        ],
    )


def test_evaluate_dividend_risk_rules() -> None:
    """Risk rule priority should match the initial simple rules."""
    assert evaluate_dividend_risk(0.071, 1.0, 100.0, 90.0)[0] is not None
    assert evaluate_dividend_risk(0.061, 1.0, 80.0, 90.0)[0] is not None
    assert evaluate_dividend_risk(0.061, 1.0, 100.0, None)[0] is not None
    assert evaluate_dividend_risk(0.061, 0.0, 100.0, 90.0)[0] is not None
    assert evaluate_dividend_risk(0.05, 1.0, 100.0, 90.0) == (None, None)


def test_calculate_risk_flag_for_high_yield(tmp_path: Path) -> None:
    """Dividend yield above 7% should create a risk flag."""
    connection = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_price(connection, 1, 100.0)
        insert_sma_200(connection, 1, 90.0)
        insert_dividend_metric(connection, 1, 8.0, 0.08)

        summary = calculate_and_store_risk_flags(connection)
        saved = connection.execute(
            """
            SELECT dividend_risk_flag, dividend_risk_reason
            FROM dividend_metrics
            WHERE security_id = 1
            """
        ).fetchone()

        assert summary["risk_flags_written"] == 1
        assert saved[0] == "DIVIDEND_TRAP_RISK"
        assert "above 7%" in saved[1]
    finally:
        connection.close()


def test_calculate_risk_flag_for_price_below_200_day_sma(
    tmp_path: Path,
) -> None:
    """Yield above 6% and price below SMA should create a risk flag."""
    connection = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_price(connection, 1, 80.0)
        insert_sma_200(connection, 1, 90.0)
        insert_dividend_metric(connection, 1, 5.0, 0.065)

        calculate_and_store_risk_flags(connection)
        reason = connection.execute(
            """
            SELECT dividend_risk_reason
            FROM dividend_metrics
            WHERE security_id = 1
            """
        ).fetchone()[0]

        assert "below the 200-day SMA" in reason
    finally:
        connection.close()


def test_calculate_risk_flag_for_missing_200_day_sma(tmp_path: Path) -> None:
    """Yield above 6% and missing SMA should create a risk flag."""
    connection = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_price(connection, 1, 100.0)
        insert_dividend_metric(connection, 1, 5.0, 0.065)

        calculate_and_store_risk_flags(connection)
        reason = connection.execute(
            """
            SELECT dividend_risk_reason
            FROM dividend_metrics
            WHERE security_id = 1
            """
        ).fetchone()[0]

        assert "200-day SMA is missing" in reason
    finally:
        connection.close()


def test_calculate_risk_flag_for_zero_trailing_dividends(tmp_path: Path) -> None:
    """Yield above 6% and zero dividends should create a risk flag."""
    connection = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_price(connection, 1, 100.0)
        insert_sma_200(connection, 1, 90.0)
        insert_dividend_metric(connection, 1, 0.0, 0.065)

        calculate_and_store_risk_flags(connection)
        reason = connection.execute(
            """
            SELECT dividend_risk_reason
            FROM dividend_metrics
            WHERE security_id = 1
            """
        ).fetchone()[0]

        assert "zero or missing" in reason
    finally:
        connection.close()


def test_calculate_risk_flags_clears_safe_metric(tmp_path: Path) -> None:
    """A safe metric should have no risk flag or reason."""
    connection = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_price(connection, 1, 100.0)
        insert_sma_200(connection, 1, 90.0)
        insert_dividend_metric(connection, 1, 4.0, 0.04)

        summary = calculate_and_store_risk_flags(connection)
        saved = connection.execute(
            """
            SELECT dividend_risk_flag, dividend_risk_reason
            FROM dividend_metrics
            WHERE security_id = 1
            """
        ).fetchone()

        assert summary["cleared_flags"] == 1
        assert saved == (None, None)
    finally:
        connection.close()
