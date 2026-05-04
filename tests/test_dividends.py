"""Tests for dividend analysis."""

import csv
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
            pause_seconds=0,
            failed_log_path=tmp_path / "failed_dividend_updates.csv",
        )

        assert "AAA" in summary["failed_tickers"]
        assert summary["failed_tickers"]["AAA"]["reason"] == "network_error"
        assert summary["metrics_written"] == 1
    finally:
        connection.close()


def test_calculate_and_store_dividends_processes_batches(
    tmp_path: Path,
    capsys,
) -> None:
    """Dividend downloads should show progress by batch."""
    connection = open_test_database(tmp_path)
    sleep_calls = []

    def fake_sleep(seconds):
        sleep_calls.append(seconds)

    try:
        for security_id, ticker in enumerate(["AAA", "BBB", "CCC"], start=1):
            insert_security(connection, security_id, ticker)
            insert_latest_price(connection, security_id, 50.0)

        summary = calculate_and_store_dividends(
            connection,
            downloader=fake_dividend_downloader,
            batch_size=2,
            pause_seconds=0.25,
            retry_batch_size=1,
            sleep_function=fake_sleep,
            failed_log_path=tmp_path / "failed_dividend_updates.csv",
        )
    finally:
        connection.close()

    captured = capsys.readouterr()

    assert summary["tickers_checked"] == 3
    assert summary["metrics_written"] == 3
    assert sleep_calls == [0.25]
    assert "Total tickers to process for dividends: 3" in captured.out
    assert "Starting dividend batch 1 of 2" in captured.out
    assert "Tickers in this batch: AAA, BBB" in captured.out
    assert "Dividend rows written in this batch: 3" in captured.out
    assert "Dividend metric rows written in this batch: 2" in captured.out
    assert "Failed dividend tickers in this batch: none" in captured.out


def test_calculate_and_store_dividends_retries_failed_tickers(
    tmp_path: Path,
) -> None:
    """Failed dividend tickers should be retried once in smaller groups."""
    connection = open_test_database(tmp_path)
    calls = []
    sleep_calls = []

    def fake_sleep(seconds):
        sleep_calls.append(seconds)

    def flaky_downloader(ticker: str):
        calls.append(ticker)

        if calls.count(ticker) == 1:
            raise RuntimeError("Temporary DNS failure")

        return fake_dividend_downloader(ticker)

    try:
        insert_security(connection, 1, "AAA")
        insert_security(connection, 2, "BBB")
        insert_latest_price(connection, 1, 50.0)
        insert_latest_price(connection, 2, 25.0)

        summary = calculate_and_store_dividends(
            connection,
            downloader=flaky_downloader,
            batch_size=2,
            pause_seconds=0.5,
            retry_batch_size=1,
            sleep_function=fake_sleep,
            failed_log_path=tmp_path / "failed_dividend_updates.csv",
        )
    finally:
        connection.close()

    assert calls == ["AAA", "BBB", "AAA", "BBB"]
    assert sleep_calls == [0.5]
    assert summary["failed_tickers"] == {}
    assert summary["metrics_written"] == 2


def test_calculate_and_store_dividends_writes_failed_ticker_log(
    tmp_path: Path,
) -> None:
    """Final failed dividend tickers should be written to a CSV file."""
    connection = open_test_database(tmp_path)
    failed_log_path = tmp_path / "outputs" / "failed_dividend_updates.csv"

    def failing_downloader(ticker: str):
        raise RuntimeError("getaddrinfo failed")

    try:
        insert_security(connection, 1, "AAA")
        insert_latest_price(connection, 1, 50.0)

        summary = calculate_and_store_dividends(
            connection,
            downloader=failing_downloader,
            batch_size=1,
            pause_seconds=0,
            retry_batch_size=1,
            failed_log_path=failed_log_path,
        )
    finally:
        connection.close()

    with failed_log_path.open("r", encoding="utf-8", newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert "AAA" in summary["failed_tickers"]
    assert summary["failed_tickers"]["AAA"]["reason"] == "network_error"
    assert rows[0]["ticker"] == "AAA"
    assert rows[0]["reason"] == "network_error"
    assert "getaddrinfo failed" in rows[0]["details"]
    assert rows[0]["date"]
