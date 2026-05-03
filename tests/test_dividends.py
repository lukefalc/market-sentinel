"""Tests for dividend analysis."""

from pathlib import Path

from market_sentinel.analytics.dividends import calculate_and_store_dividends
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
            market,
            currency
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        [security_id, ticker, ticker, "Test Market", "USD"],
    )


def insert_latest_price(connection, security_id: int, close_price: float) -> None:
    """Insert a latest fake close price."""
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
        [security_id, security_id, "2026-05-03", close_price],
    )


def fake_dividend_downloader(ticker: str):
    """Return fake dividend history without using the internet."""
    if ticker == "AAA":
        return [
            {"ex_dividend_date": "2025-06-01", "dividend_amount": 1.0},
            {"ex_dividend_date": "2025-12-01", "dividend_amount": 1.5},
            {"ex_dividend_date": "2024-01-01", "dividend_amount": 9.0},
        ]

    return []


def test_calculate_and_store_dividends_writes_events_and_metrics(
    tmp_path: Path,
) -> None:
    """Dividend events and metrics should be stored from fake data."""
    connection = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_latest_price(connection, 1, 50.0)

        summary = calculate_and_store_dividends(
            connection,
            downloader=fake_dividend_downloader,
        )
        event_count = connection.execute("SELECT COUNT(*) FROM dividends").fetchone()[0]
        metric = connection.execute(
            """
            SELECT
                trailing_annual_dividend,
                dividend_yield,
                annual_dividend_cash_per_10000
            FROM dividend_metrics
            WHERE security_id = 1
            """
        ).fetchone()

        assert summary["tickers_checked"] == 1
        assert summary["dividend_rows_written"] == 3
        assert summary["metrics_written"] == 1
        assert event_count == 3
        assert metric == (2.5, 0.05, 500.0)
    finally:
        connection.close()


def test_calculate_and_store_dividends_avoids_duplicate_events(
    tmp_path: Path,
) -> None:
    """Running dividend analysis twice should not duplicate dividend events."""
    connection = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")
        insert_latest_price(connection, 1, 50.0)

        calculate_and_store_dividends(connection, downloader=fake_dividend_downloader)
        calculate_and_store_dividends(connection, downloader=fake_dividend_downloader)

        event_count = connection.execute("SELECT COUNT(*) FROM dividends").fetchone()[0]
        metric_count = connection.execute(
            "SELECT COUNT(*) FROM dividend_metrics"
        ).fetchone()[0]

        assert event_count == 3
        assert metric_count == 1
    finally:
        connection.close()


def test_calculate_and_store_dividends_handles_no_history(tmp_path: Path) -> None:
    """Tickers with no dividend history should still get a zero metric."""
    connection = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "BBB")
        insert_latest_price(connection, 1, 25.0)

        summary = calculate_and_store_dividends(
            connection,
            downloader=fake_dividend_downloader,
        )
        metric = connection.execute(
            """
            SELECT trailing_annual_dividend, dividend_yield
            FROM dividend_metrics
            WHERE security_id = 1
            """
        ).fetchone()

        assert summary["no_dividend_history"] == ["BBB"]
        assert summary["metrics_written"] == 1
        assert metric == (0.0, 0.0)
    finally:
        connection.close()


def test_calculate_and_store_dividends_continues_after_failure(
    tmp_path: Path,
) -> None:
    """A failed ticker should be reported while later tickers still run."""
    connection = open_test_database(tmp_path)

    def mixed_downloader(ticker: str):
        if ticker == "AAA":
            raise RuntimeError("temporary provider failure")
        return fake_dividend_downloader(ticker)

    try:
        insert_security(connection, 1, "AAA")
        insert_security(connection, 2, "BBB")
        insert_latest_price(connection, 1, 50.0)
        insert_latest_price(connection, 2, 25.0)

        summary = calculate_and_store_dividends(
            connection,
            downloader=mixed_downloader,
        )

        assert "AAA" in summary["failed_tickers"]
        assert summary["metrics_written"] == 1
    finally:
        connection.close()
