"""Tests for daily price loading."""

from pathlib import Path

from market_sentinel.data.price_loader import update_daily_prices
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
