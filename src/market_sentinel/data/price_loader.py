"""Daily price downloading and storage helpers.

This module downloads daily historical prices for securities already stored in
the database. It uses yfinance for real downloads, but tests can pass fake
download functions so the test suite never needs live internet access.
"""

from datetime import date, datetime
import time
from typing import Any, Callable, Dict, List, Optional

import duckdb

Downloader = Callable[[str, Optional[str], Optional[str]], Any]
SleepFunction = Callable[[float], None]
DEFAULT_BATCH_SIZE = 50
DEFAULT_BATCH_PAUSE_SECONDS = 1.0


def get_active_securities(
    connection: duckdb.DuckDBPyConnection,
) -> List[Dict[str, Any]]:
    """Read tickers from the securities table.

    The project does not have an ``active`` flag yet, so every security with a
    non-empty ticker is treated as active.
    """
    try:
        rows = connection.execute(
            """
            SELECT security_id, ticker
            FROM securities
            WHERE ticker IS NOT NULL
              AND TRIM(ticker) <> ''
            ORDER BY ticker
            """
        ).fetchall()
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read tickers from DuckDB. Check that the database is "
            "open and the securities table has been created."
        ) from error

    return [{"security_id": row[0], "ticker": row[1]} for row in rows]


def download_daily_prices(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    downloader: Optional[Downloader] = None,
) -> List[Dict[str, Any]]:
    """Download daily prices for one ticker and return normalised rows."""
    download_function = downloader or _download_from_yfinance

    try:
        raw_prices = download_function(ticker, start_date, end_date)
    except Exception as error:
        raise RuntimeError(
            f"Could not download daily prices for {ticker}. "
            "Check the ticker symbol and your internet connection. "
            f"Details: {error}"
        ) from error

    return normalise_price_data(raw_prices)


def normalise_price_data(raw_prices: Any) -> List[Dict[str, Any]]:
    """Convert downloaded price data into dictionaries for database storage."""
    if raw_prices is None:
        return []

    if isinstance(raw_prices, list):
        return raw_prices

    if getattr(raw_prices, "empty", False):
        return []

    price_rows = []
    for index_value, row in raw_prices.iterrows():
        close_price = _get_row_value(row, "Close")

        if close_price is None:
            continue

        price_rows.append(
            {
                "price_date": _normalise_date(index_value),
                "open_price": _get_row_value(row, "Open"),
                "high_price": _get_row_value(row, "High"),
                "low_price": _get_row_value(row, "Low"),
                "close_price": close_price,
                "adjusted_close_price": _get_row_value(row, "Adj Close"),
                "volume": _get_row_value(row, "Volume"),
            }
        )

    return price_rows


def upsert_daily_prices(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    price_rows: List[Dict[str, Any]],
) -> int:
    """Insert or update daily price rows without creating duplicates."""
    written_count = 0

    try:
        for row in price_rows:
            existing_id = connection.execute(
                """
                SELECT price_id
                FROM daily_prices
                WHERE security_id = ?
                  AND price_date = ?
                """,
                [security_id, row["price_date"]],
            ).fetchone()

            values = [
                row.get("open_price"),
                row.get("high_price"),
                row.get("low_price"),
                row["close_price"],
                row.get("adjusted_close_price"),
                row.get("volume"),
            ]

            if existing_id:
                connection.execute(
                    """
                    UPDATE daily_prices
                    SET open_price = ?,
                        high_price = ?,
                        low_price = ?,
                        close_price = ?,
                        adjusted_close_price = ?,
                        volume = ?
                    WHERE price_id = ?
                    """,
                    values + [existing_id[0]],
                )
            else:
                next_id = connection.execute(
                    "SELECT COALESCE(MAX(price_id), 0) + 1 FROM daily_prices"
                ).fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO daily_prices (
                        price_id,
                        security_id,
                        price_date,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        adjusted_close_price,
                        volume
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [next_id, security_id, row["price_date"]] + values,
                )

            written_count += 1
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not save daily prices to DuckDB. Check that the database is "
            "open and the daily_prices table has been created."
        ) from error

    return written_count


def update_daily_prices(
    connection: duckdb.DuckDBPyConnection,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    downloader: Optional[Downloader] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    pause_seconds: float = DEFAULT_BATCH_PAUSE_SECONDS,
    sleep_function: SleepFunction = time.sleep,
) -> Dict[str, Any]:
    """Download and store daily prices for every active ticker."""
    if batch_size <= 0:
        raise ValueError("Market data batch size must be greater than zero.")

    if pause_seconds < 0:
        raise ValueError("Market data batch pause must be zero or greater.")

    securities = get_active_securities(connection)
    summary = {
        "tickers_checked": len(securities),
        "price_rows_written": 0,
        "failed_tickers": {},
    }

    total_tickers = len(securities)
    batches = _chunk_securities(securities, batch_size)
    total_batches = len(batches)

    print(f"Total tickers to update: {total_tickers}")

    for batch_number, batch in enumerate(batches, start=1):
        batch_tickers = [security["ticker"] for security in batch]
        batch_rows_written = 0
        batch_failed_tickers = {}

        print(f"Starting batch {batch_number} of {total_batches}")
        print(f"Tickers in this batch: {', '.join(batch_tickers)}")

        for security in batch:
            ticker = security["ticker"]

            try:
                price_rows = download_daily_prices(
                    ticker,
                    start_date=start_date,
                    end_date=end_date,
                    downloader=downloader,
                )
                rows_written = upsert_daily_prices(
                    connection,
                    security["security_id"],
                    price_rows,
                )
                batch_rows_written += rows_written
                summary["price_rows_written"] += rows_written
            except RuntimeError as error:
                batch_failed_tickers[ticker] = str(error)
                summary["failed_tickers"][ticker] = str(error)

        print(f"Rows written in this batch: {batch_rows_written}")

        if batch_failed_tickers:
            print("Failed tickers in this batch:")
            for ticker, message in batch_failed_tickers.items():
                print(f"- {ticker}: {message}")
        else:
            print("Failed tickers in this batch: none")

        if batch_number < total_batches and pause_seconds > 0:
            print(f"Pausing {pause_seconds:g} seconds before the next batch")
            sleep_function(pause_seconds)

    print("Market data update summary")
    print(f"Total tickers checked: {summary['tickers_checked']}")
    print(f"Total price rows written: {summary['price_rows_written']}")

    if summary["failed_tickers"]:
        print("Failed tickers:")
        for ticker, message in summary["failed_tickers"].items():
            print(f"- {ticker}: {message}")
    else:
        print("Failed tickers: none")

    return summary


def _chunk_securities(
    securities: List[Dict[str, Any]],
    batch_size: int,
) -> List[List[Dict[str, Any]]]:
    """Split securities into batches."""
    return [
        securities[start_index : start_index + batch_size]
        for start_index in range(0, len(securities), batch_size)
    ]


def _download_from_yfinance(
    ticker: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> Any:
    """Download prices with yfinance."""
    try:
        import yfinance as yf
    except ImportError as error:
        raise RuntimeError(
            "yfinance is not installed. Install the project dependencies with "
            'pip install -e ".[dev]" and try again.'
        ) from error

    options = {
        "auto_adjust": False,
        "progress": False,
        "interval": "1d",
        "threads": False,
    }

    if start_date:
        options["start"] = start_date
    if end_date:
        options["end"] = end_date
    if not start_date and not end_date:
        options["period"] = "max"

    return yf.download(ticker, **options)


def _get_row_value(row: Any, column_name: str) -> Any:
    """Read a value from a yfinance row, including simple MultiIndex rows."""
    value = None

    if column_name in row:
        value = row[column_name]
    else:
        for key in row.index:
            if isinstance(key, tuple) and column_name in key:
                value = row[key]
                break

    return _clean_value(value)


def _clean_value(value: Any) -> Any:
    """Convert missing numeric values to None."""
    if value is None:
        return None

    if hasattr(value, "iloc") and len(value) == 1:
        value = value.iloc[0]

    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except ValueError:
            return value

    try:
        if value != value:
            return None
    except TypeError:
        return value

    return value


def _normalise_date(value: Any) -> str:
    """Return a date string that DuckDB can store in a DATE column."""
    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if hasattr(value, "date"):
        return value.date().isoformat()

    return str(value)
