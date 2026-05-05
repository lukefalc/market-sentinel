"""Tests for PDF trade candidate card calculations."""

from datetime import date, timedelta
from pathlib import Path

from market_sentinel.analytics.trade_candidates import build_trade_candidate
from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema


def write_settings(config_dir: Path, database_path: Path) -> None:
    """Create settings for trade candidate tests."""
    config_dir.mkdir(parents=True, exist_ok=True)
    config_dir.joinpath("settings.yaml").write_text(
        "\n".join(
            [
                f"database_path: {database_path}",
                "candidate_stop_short_window_days: 20",
                "candidate_trailing_stop_percent: 20",
                "candidate_include_50_sma_stop: true",
                "candidate_include_20_day_extreme_stop: true",
                "candidate_include_trailing_reference: true",
                "candidate_grade_stop_distance_warning_percent: 12",
                "candidate_recent_strong_days: 2",
            ]
        ),
        encoding="utf-8",
    )


def open_test_database(tmp_path: Path):
    """Open a temporary DuckDB database with candidate settings."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    return connection, config_dir


def insert_candidate_security(
    connection,
    security_id: int = 1,
    ticker: str = "AAA",
    close_start: float = 100.0,
) -> None:
    """Insert one security with 20 close prices."""
    connection.execute(
        """
        INSERT INTO securities (
            security_id,
            ticker,
            name,
            market,
            currency
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        [security_id, ticker, "Example A", "S&P 500", "USD"],
    )
    start_date = date(2026, 4, 15)

    for index in range(20):
        close_price = close_start + index
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
                security_id * 100 + index,
                security_id,
                start_date + timedelta(days=index),
                close_price,
            ],
        )


def insert_latest_sma(
    connection,
    security_id: int = 1,
    period: int = 50,
    value: float = 110.0,
) -> None:
    """Insert a latest SMA value."""
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
        [5000 + security_id + period, security_id, "2026-05-04", period, value, "SMA"],
    )


def insert_50_day_sma(connection, security_id: int = 1, value: float = 110.0) -> None:
    """Insert a latest 50-day SMA value."""
    insert_latest_sma(connection, security_id, 50, value)


def insert_dividend_risk_flag(connection, security_id: int = 1) -> None:
    """Insert a latest dividend risk flag."""
    connection.execute(
        """
        INSERT INTO dividend_metrics (
            metric_id,
            security_id,
            metric_date,
            dividend_risk_flag
        )
        VALUES (?, ?, ?, ?)
        """,
        [9000 + security_id, security_id, "2026-05-04", "DIVIDEND_TRAP_RISK"],
    )


def test_bullish_candidate_review_levels(tmp_path: Path) -> None:
    """Bullish candidates should use 50-day SMA, 20-day low, and -20% levels."""
    connection, config_dir = open_test_database(tmp_path)
    try:
        insert_candidate_security(connection)
        insert_50_day_sma(connection, value=110.0)
        candidate = build_trade_candidate(
            connection,
            "AAA",
            bullish_signal(),
            config_dir,
        )
    finally:
        connection.close()

    assert candidate["latest_close_price"] == 119.0
    assert candidate["review_levels"]["50-day SMA"] == 110.0
    assert candidate["review_levels"]["20-day low"] == 100.0
    assert candidate["review_levels"]["20% trailing reference"] == 95.2
    assert "Close price is above the 50-day trend line." in candidate["risk_notes"]
    assert "No dividend risk flag." in candidate["risk_notes"]


def test_strong_buy_setup_grading(tmp_path: Path) -> None:
    """Bullish candidates with aligned positive rules should grade strong buy."""
    connection, config_dir = open_test_database(tmp_path)
    try:
        insert_candidate_security(connection, close_start=140.0)
        insert_latest_sma(connection, period=7, value=156.0)
        insert_latest_sma(connection, period=30, value=153.0)
        insert_latest_sma(connection, period=50, value=150.0)
        candidate = build_trade_candidate(
            connection,
            "AAA",
            bullish_signal(),
            config_dir,
        )
    finally:
        connection.close()

    assert candidate["action_grade"] == "Strong Buy Setup"
    assert candidate["score"] == 8
    assert "Recent bullish crossover within 2 days." in candidate["grade_reasons"]


def test_buy_setup_grading(tmp_path: Path) -> None:
    """Moderately positive bullish candidates should grade buy setup."""
    connection, config_dir = open_test_database(tmp_path)
    try:
        insert_candidate_security(connection, close_start=140.0)
        insert_50_day_sma(connection, value=150.0)
        candidate = build_trade_candidate(
            connection,
            "AAA",
            {
                **bullish_signal(),
                "crossover_date": date(2026, 5, 1),
                "days_since_crossover": "3 days ago",
            },
            config_dir,
        )
    finally:
        connection.close()

    assert candidate["action_grade"] == "Buy Setup"
    assert candidate["score"] == 5


def test_track_only_grading(tmp_path: Path) -> None:
    """Weak or stale candidates should grade track only."""
    connection, config_dir = open_test_database(tmp_path)
    try:
        insert_candidate_security(connection, close_start=140.0)
        insert_50_day_sma(connection, value=170.0)
        candidate = build_trade_candidate(
            connection,
            "AAA",
            {
                **bullish_signal(),
                "crossover_date": date(2026, 4, 20),
                "days_since_crossover": "14 days ago",
            },
            config_dir,
        )
    finally:
        connection.close()

    assert candidate["action_grade"] == "Track Only"


def test_sell_setup_grading(tmp_path: Path) -> None:
    """Moderately bearish candidates should grade sell setup."""
    connection, config_dir = open_test_database(tmp_path)
    try:
        insert_candidate_security(connection, close_start=140.0)
        insert_50_day_sma(connection, value=170.0)
        candidate = build_trade_candidate(
            connection,
            "AAA",
            {
                **bearish_signal(),
                "crossover_date": date(2026, 5, 1),
                "days_since_crossover": "3 days ago",
            },
            config_dir,
        )
    finally:
        connection.close()

    assert candidate["action_grade"] == "Sell Setup"
    assert candidate["score"] == 4


def test_strong_sell_setup_grading(tmp_path: Path) -> None:
    """Bearish candidates with aligned negative trend rules should grade strong."""
    connection, config_dir = open_test_database(tmp_path)
    try:
        insert_candidate_security(connection, close_start=140.0)
        insert_latest_sma(connection, period=7, value=150.0)
        insert_latest_sma(connection, period=30, value=155.0)
        insert_latest_sma(connection, period=50, value=170.0)
        candidate = build_trade_candidate(
            connection,
            "AAA",
            bearish_signal(),
            config_dir,
        )
    finally:
        connection.close()

    assert candidate["action_grade"] == "Strong Sell Setup"
    assert candidate["score"] == 7


def test_dividend_risk_flag_reduces_bullish_score(tmp_path: Path) -> None:
    """Dividend risk flags should reduce bullish setup scores."""
    connection, config_dir = open_test_database(tmp_path)
    try:
        insert_candidate_security(connection, close_start=140.0)
        insert_latest_sma(connection, period=7, value=156.0)
        insert_latest_sma(connection, period=30, value=153.0)
        insert_latest_sma(connection, period=50, value=150.0)
        insert_dividend_risk_flag(connection)
        candidate = build_trade_candidate(
            connection,
            "AAA",
            bullish_signal(),
            config_dir,
        )
    finally:
        connection.close()

    assert candidate["action_grade"] == "Buy Setup"
    assert candidate["score"] == 5
    assert "Dividend risk flag present." in candidate["grade_cautions"]


def test_bearish_candidate_review_levels(tmp_path: Path) -> None:
    """Bearish candidates should use 50-day SMA, 20-day high, and +20% levels."""
    connection, config_dir = open_test_database(tmp_path)
    try:
        insert_candidate_security(connection)
        insert_50_day_sma(connection, value=125.0)
        candidate = build_trade_candidate(
            connection,
            "AAA",
            bearish_signal(),
            config_dir,
        )
    finally:
        connection.close()

    assert candidate["latest_close_price"] == 119.0
    assert candidate["review_levels"]["50-day SMA"] == 125.0
    assert candidate["review_levels"]["20-day high"] == 119.0
    assert candidate["review_levels"]["20% trailing reference"] == 142.8
    assert "Close price is below the 50-day trend line." in candidate["risk_notes"]


def test_candidate_review_levels_handle_missing_sma(tmp_path: Path) -> None:
    """Missing SMA values should produce Not available-ready candidate data."""
    connection, config_dir = open_test_database(tmp_path)
    try:
        insert_candidate_security(connection)
        candidate = build_trade_candidate(
            connection,
            "AAA",
            bullish_signal(),
            config_dir,
        )
    finally:
        connection.close()

    assert candidate["review_levels"]["50-day SMA"] is None
    assert (
        "50-day trend line comparison is not available."
        in candidate["risk_notes"]
    )


def bullish_signal():
    """Return a sample bullish signal."""
    return {
        "direction": "Bullish",
        "trend_description": "7-day trend line crossed above 30-day trend line",
        "crossover_date": date(2026, 5, 4),
        "days_since_crossover": "Today",
    }


def bearish_signal():
    """Return a sample bearish signal."""
    return {
        "direction": "Bearish",
        "trend_description": "7-day trend line crossed below 30-day trend line",
        "crossover_date": date(2026, 5, 4),
        "days_since_crossover": "Today",
    }
