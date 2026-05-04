"""Tests for daily price loading."""

from pathlib import Path
import sys
from types import SimpleNamespace

from market_sentinel.data.price_loader import (
    backfill_daily_prices,
    update_daily_prices,
    update_recent_daily_prices,
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


def fake_downloader(ticker: str, start_date, end_date):
    """Return fake daily prices without using the internet."""
    return [
        {
            "price_date": "2026-01-02",
            "open_price": 10.0,
            "high_price": 12.0,
            "low_price": 9.5,
            "close_price": 11.0,
            "adjusted_close_price": 10.8,
            "volume": 1000,
        },
        {
            "price_date": "2026-01-03",
            "open_price": 11.0,
            "high_price": 13.0,
            "low_price": 10.5,
            "close_price": 12.0,
            "adjusted_close_price": 11.8,
            "volume": 1200,
        },
    ]


def test_update_daily_prices_loads_fake_prices(tmp_path: Path) -> None:
    """Daily prices should be written for active securities."""
    connection = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")

        summary = update_daily_prices(connection, downloader=fake_downloader)
        saved_count = connection.execute(
            "SELECT COUNT(*) FROM daily_prices"
        ).fetchone()[0]

        assert summary["tickers_checked"] == 1
        assert summary["price_rows_written"] == 2
        assert summary["failed_tickers"] == {}
        assert saved_count == 2
    finally:
        connection.close()


def test_update_daily_prices_does_not_insert_duplicate_rows(
    tmp_path: Path,
) -> None:
    """Running the loader twice should update existing dates, not duplicate them."""
    connection = open_test_database(tmp_path)

    try:
        insert_security(connection, 1, "AAA")

        update_daily_prices(connection, downloader=fake_downloader)
        update_daily_prices(connection, downloader=fake_downloader)

        saved_count = connection.execute(
            "SELECT COUNT(*) FROM daily_prices"
        ).fetchone()[0]

        assert saved_count == 2
    finally:
        connection.close()


def test_update_daily_prices_continues_after_failed_ticker(
    tmp_path: Path,
) -> None:
    """A failed ticker should be reported while later tickers still load."""
    connection = open_test_database(tmp_path)

    def mixed_downloader(ticker: str, start_date, end_date):
        if ticker == "AAA":
            raise RuntimeError("temporary provider failure")
        return fake_downloader(ticker, start_date, end_date)

    try:
        insert_security(connection, 1, "AAA")
        insert_security(connection, 2, "BBB")

        summary = update_daily_prices(connection, downloader=mixed_downloader)
        saved_count = connection.execute(
            "SELECT COUNT(*) FROM daily_prices"
        ).fetchone()[0]

        assert summary["tickers_checked"] == 2
        assert summary["price_rows_written"] == 2
        assert "AAA" in summary["failed_tickers"]
        assert saved_count == 2
    finally:
        connection.close()


def test_update_daily_prices_processes_tickers_in_batches(
    tmp_path: Path,
    capsys,
) -> None:
    """Large universes should be processed in visible batches."""
    connection = open_test_database(tmp_path)
    sleep_calls = []

    def fake_sleep(seconds):
        sleep_calls.append(seconds)

    try:
        for security_id, ticker in enumerate(["AAA", "BBB", "CCC", "DDD", "EEE"], 1):
            insert_security(connection, security_id, ticker)

        summary = update_daily_prices(
            connection,
            downloader=fake_downloader,
            batch_size=2,
            pause_seconds=0.25,
            sleep_function=fake_sleep,
        )
    finally:
        connection.close()

    captured = capsys.readouterr()

    assert summary["tickers_checked"] == 5
    assert summary["price_rows_written"] == 10
    assert summary["failed_tickers"] == {}
    assert sleep_calls == [0.25, 0.25]
    assert "Total tickers to update: 5" in captured.out
    assert "Starting batch 1 of 3" in captured.out
    assert "Tickers in this batch: AAA, BBB" in captured.out
    assert "Rows written in this batch: 4" in captured.out
    assert "Failed tickers in this batch: none" in captured.out
    assert "Total price rows written: 10" in captured.out


def test_update_daily_prices_reports_failed_tickers_per_batch(
    tmp_path: Path,
    capsys,
) -> None:
    """Batch progress should include failed tickers and keep going."""
    connection = open_test_database(tmp_path)

    def mixed_downloader(ticker: str, start_date, end_date):
        if ticker == "BBB":
            raise RuntimeError("temporary provider failure")
        return fake_downloader(ticker, start_date, end_date)

    try:
        insert_security(connection, 1, "AAA")
        insert_security(connection, 2, "BBB")
        insert_security(connection, 3, "CCC")

        summary = update_daily_prices(
            connection,
            downloader=mixed_downloader,
            batch_size=2,
            pause_seconds=0,
        )
    finally:
        connection.close()

    captured = capsys.readouterr()

    assert summary["tickers_checked"] == 3
    assert summary["price_rows_written"] == 4
    assert "BBB" in summary["failed_tickers"]
    assert "Failed tickers in this batch:" in captured.out
    assert "- BBB:" in captured.out
    assert "Starting batch 2 of 2" in captured.out


def test_update_daily_prices_uses_yfinance_batch_download(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Production downloads should call yfinance once per ticker batch."""
    connection = open_test_database(tmp_path)
    download_calls = []

    def fake_yfinance_download(tickers, **options):
        download_calls.append({"tickers": tickers, "options": options})
        return {
            ticker: fake_downloader(ticker, options.get("start"), options.get("end"))
            for ticker in tickers
        }

    monkeypatch.setitem(
        sys.modules,
        "yfinance",
        SimpleNamespace(download=fake_yfinance_download),
    )

    try:
        insert_security(connection, 1, "AAA")
        insert_security(connection, 2, "BBB")
        insert_security(connection, 3, "CCC")

        summary = update_daily_prices(
            connection,
            batch_size=2,
            pause_seconds=0,
            lookback_days=10,
        )
    finally:
        connection.close()

    assert summary["tickers_checked"] == 3
    assert summary["price_rows_written"] == 6
    assert summary["failed_tickers"] == {}
    assert download_calls[0]["tickers"] == ["AAA", "BBB"]
    assert download_calls[1]["tickers"] == ["CCC"]
    assert download_calls[0]["options"]["start"]
    assert "period" not in download_calls[0]["options"]


def test_update_daily_prices_continues_after_failed_batch(
    tmp_path: Path,
) -> None:
    """A failed batch should be reported while later batches still run."""
    connection = open_test_database(tmp_path)

    def fake_batch_downloader(tickers, start_date, end_date, period):
        if "AAA" in tickers:
            raise RuntimeError("temporary batch failure")

        return {
            ticker: fake_downloader(ticker, start_date, end_date)
            for ticker in tickers
        }

    try:
        insert_security(connection, 1, "AAA")
        insert_security(connection, 2, "BBB")
        insert_security(connection, 3, "CCC")

        summary = update_daily_prices(
            connection,
            batch_downloader=fake_batch_downloader,
            batch_size=2,
            pause_seconds=0,
        )
    finally:
        connection.close()

    assert summary["tickers_checked"] == 3
    assert summary["price_rows_written"] == 2
    assert "AAA" in summary["failed_tickers"]
    assert "BBB" in summary["failed_tickers"]
    assert "CCC" not in summary["failed_tickers"]


def test_update_recent_daily_prices_uses_lookback_start_date(
    tmp_path: Path,
) -> None:
    """Daily update mode should use a recent start date instead of a period."""
    connection = open_test_database(tmp_path)
    batch_calls = []

    def fake_batch_downloader(tickers, start_date, end_date, period):
        batch_calls.append(
            {"start_date": start_date, "end_date": end_date, "period": period}
        )
        return {
            ticker: fake_downloader(ticker, start_date, end_date)
            for ticker in tickers
        }

    try:
        insert_security(connection, 1, "AAA")
        summary = update_recent_daily_prices(
            connection,
            batch_size=50,
            lookback_days=10,
            pause_seconds=0,
            batch_downloader=fake_batch_downloader,
        )
    finally:
        connection.close()

    assert summary["tickers_checked"] == 1
    assert batch_calls[0]["start_date"]
    assert batch_calls[0]["period"] is None


def test_update_daily_prices_daily_mode_passes_recent_start_date(
    tmp_path: Path,
) -> None:
    """Daily mode should pass a start date and no yfinance period."""
    connection = open_test_database(tmp_path)
    batch_calls = []

    def fake_batch_downloader(tickers, start_date, end_date, period):
        batch_calls.append(
            {"start_date": start_date, "end_date": end_date, "period": period}
        )
        return {
            ticker: fake_downloader(ticker, start_date, end_date)
            for ticker in tickers
        }

    try:
        insert_security(connection, 1, "AAA")
        update_daily_prices(
            connection,
            batch_downloader=fake_batch_downloader,
            batch_size=50,
            pause_seconds=0,
            mode="daily",
            lookback_days=10,
        )
    finally:
        connection.close()

    assert batch_calls[0]["start_date"]
    assert batch_calls[0]["period"] is None


def test_backfill_daily_prices_uses_backfill_period(
    tmp_path: Path,
) -> None:
    """Backfill mode should use the configured yfinance period."""
    connection = open_test_database(tmp_path)
    batch_calls = []

    def fake_batch_downloader(tickers, start_date, end_date, period):
        batch_calls.append(
            {"start_date": start_date, "end_date": end_date, "period": period}
        )
        return {
            ticker: fake_downloader(ticker, start_date, end_date)
            for ticker in tickers
        }

    try:
        insert_security(connection, 1, "AAA")
        summary = backfill_daily_prices(
            connection,
            batch_downloader=fake_batch_downloader,
            batch_size=50,
            pause_seconds=0,
            backfill_period="5y",
        )
    finally:
        connection.close()

    assert summary["tickers_checked"] == 1
    assert batch_calls[0]["start_date"] is None
    assert batch_calls[0]["period"] == "5y"
