"""Daily price downloading and storage helpers.

This module downloads daily historical prices for securities already stored in
the database. It uses yfinance for real downloads, but tests can pass fake
download functions so the test suite never needs live internet access.
"""

from datetime import date, datetime, timedelta
import time
from typing import Any, Callable, Dict, List, Optional

import duckdb

Downloader = Callable[[str, Optional[str], Optional[str]], Any]
BatchDownloader = Callable[
    [List[str], Optional[str], Optional[str], Optional[str]],
    Any,
]
SleepFunction = Callable[[float], None]
DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE = 50
DEFAULT_PRICE_DAILY_LOOKBACK_DAYS = 10
DEFAULT_PRICE_DOWNLOAD_LOOKBACK_DAYS = DEFAULT_PRICE_DAILY_LOOKBACK_DAYS
DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS = 1.0
DEFAULT_PRICE_BACKFILL_PERIOD = "5y"
DEFAULT_BATCH_SIZE = DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE
DEFAULT_BATCH_PAUSE_SECONDS = DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS


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


def download_daily_prices_for_batch(
    tickers: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: Optional[str] = None,
    batch_downloader: Optional[BatchDownloader] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Download daily prices for a batch of tickers."""
    download_function = batch_downloader or _download_batch_from_yfinance

    try:
        raw_prices = download_function(tickers, start_date, end_date, period)
    except Exception as error:
        raise RuntimeError(
            "Could not download daily prices for this batch. Check your "
            "internet connection and the ticker symbols in the batch. "
            f"Details: {error}"
        ) from error

    return normalise_batch_price_data(raw_prices, tickers)


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


def normalise_batch_price_data(
    raw_prices: Any,
    tickers: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    """Convert batch price data into rows grouped by ticker."""
    if isinstance(raw_prices, dict):
        return {
            ticker: normalise_price_data(raw_prices.get(ticker))
            for ticker in tickers
        }

    if raw_prices is None or getattr(raw_prices, "empty", False):
        return {ticker: [] for ticker in tickers}

    if len(tickers) == 1:
        return {tickers[0]: normalise_price_data(raw_prices)}

    if hasattr(raw_prices, "columns") and hasattr(raw_prices.columns, "nlevels"):
        if raw_prices.columns.nlevels > 1:
            return _normalise_multi_ticker_frame(raw_prices, tickers)

    return {ticker: [] for ticker in tickers}


def _normalise_multi_ticker_frame(
    raw_prices: Any,
    tickers: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    """Normalise a yfinance multi-ticker DataFrame."""
    price_rows_by_ticker = {}

    for ticker in tickers:
        ticker_frame = _select_ticker_frame(raw_prices, ticker)
        price_rows_by_ticker[ticker] = normalise_price_data(ticker_frame)

    return price_rows_by_ticker


def _select_ticker_frame(raw_prices: Any, ticker: str) -> Any:
    """Select one ticker from a yfinance multi-ticker DataFrame."""
    columns = raw_prices.columns

    for level_number in range(columns.nlevels):
        if ticker in columns.get_level_values(level_number):
            return raw_prices.xs(
                ticker,
                axis=1,
                level=level_number,
                drop_level=True,
            )

    return None


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
    batch_downloader: Optional[BatchDownloader] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    pause_seconds: float = DEFAULT_BATCH_PAUSE_SECONDS,
    lookback_days: int = DEFAULT_PRICE_DAILY_LOOKBACK_DAYS,
    mode: str = "daily",
    backfill_period: str = DEFAULT_PRICE_BACKFILL_PERIOD,
    sleep_function: SleepFunction = time.sleep,
) -> Dict[str, Any]:
    """Download and store daily prices for every active ticker."""
    if batch_size <= 0:
        raise ValueError("Market data batch size must be greater than zero.")

    if pause_seconds < 0:
        raise ValueError("Market data batch pause must be zero or greater.")

    effective_start_date = _resolve_start_date(start_date, lookback_days, mode)
    download_period = _resolve_download_period(mode, backfill_period)
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

        if downloader is not None:
            batch_rows_written = _update_batch_with_single_ticker_downloader(
                connection,
                batch,
                effective_start_date,
                end_date,
                downloader,
                batch_failed_tickers,
            )
        else:
            batch_rows_written = _update_batch_with_batch_downloader(
                connection,
                batch,
                effective_start_date,
                end_date,
                download_period,
                batch_downloader,
                batch_failed_tickers,
            )

        summary["price_rows_written"] += batch_rows_written
        summary["failed_tickers"].update(batch_failed_tickers)

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


def update_recent_daily_prices(
    connection: duckdb.DuckDBPyConnection,
    batch_size: int = DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
    lookback_days: int = DEFAULT_PRICE_DAILY_LOOKBACK_DAYS,
    pause_seconds: float = DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
    batch_downloader: Optional[BatchDownloader] = None,
) -> Dict[str, Any]:
    """Run the normal daily price update mode."""
    return update_daily_prices(
        connection,
        batch_size=batch_size,
        lookback_days=lookback_days,
        pause_seconds=pause_seconds,
        batch_downloader=batch_downloader,
        mode="daily",
    )


def backfill_daily_prices(
    connection: duckdb.DuckDBPyConnection,
    batch_size: int = DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
    backfill_period: str = DEFAULT_PRICE_BACKFILL_PERIOD,
    pause_seconds: float = DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
    batch_downloader: Optional[BatchDownloader] = None,
) -> Dict[str, Any]:
    """Run the larger historical price backfill mode."""
    return update_daily_prices(
        connection,
        batch_size=batch_size,
        pause_seconds=pause_seconds,
        mode="backfill",
        backfill_period=backfill_period,
        batch_downloader=batch_downloader,
    )


def _update_batch_with_single_ticker_downloader(
    connection: duckdb.DuckDBPyConnection,
    batch: List[Dict[str, Any]],
    start_date: Optional[str],
    end_date: Optional[str],
    downloader: Downloader,
    batch_failed_tickers: Dict[str, str],
) -> int:
    """Update one batch by downloading one ticker at a time."""
    batch_rows_written = 0

    for security in batch:
        ticker = security["ticker"]

        try:
            price_rows = download_daily_prices(
                ticker,
                start_date=start_date,
                end_date=end_date,
                downloader=downloader,
            )
            batch_rows_written += upsert_daily_prices(
                connection,
                security["security_id"],
                price_rows,
            )
        except RuntimeError as error:
            batch_failed_tickers[ticker] = str(error)

    return batch_rows_written


def _update_batch_with_batch_downloader(
    connection: duckdb.DuckDBPyConnection,
    batch: List[Dict[str, Any]],
    start_date: Optional[str],
    end_date: Optional[str],
    period: Optional[str],
    batch_downloader: Optional[BatchDownloader],
    batch_failed_tickers: Dict[str, str],
) -> int:
    """Update one batch using one yfinance request for all tickers."""
    batch_tickers = [security["ticker"] for security in batch]

    try:
        price_rows_by_ticker = download_daily_prices_for_batch(
            batch_tickers,
            start_date=start_date,
            end_date=end_date,
            period=period,
            batch_downloader=batch_downloader,
        )
    except RuntimeError as error:
        for ticker in batch_tickers:
            batch_failed_tickers[ticker] = str(error)
        return 0

    batch_rows_written = 0

    for security in batch:
        ticker = security["ticker"]
        price_rows = price_rows_by_ticker.get(ticker, [])

        if not price_rows:
            batch_failed_tickers[ticker] = (
                "No recent price rows were returned for this ticker."
            )
            continue

        try:
            batch_rows_written += upsert_daily_prices(
                connection,
                security["security_id"],
                price_rows,
            )
        except RuntimeError as error:
            batch_failed_tickers[ticker] = str(error)

    return batch_rows_written


def _resolve_start_date(
    start_date: Optional[str],
    lookback_days: int,
    mode: str,
) -> Optional[str]:
    """Return the start date for a normal daily update."""
    if start_date:
        return start_date

    if mode == "backfill":
        return None

    if mode != "daily":
        raise ValueError("Price download mode must be either daily or backfill.")

    if lookback_days <= 0:
        return None

    return (date.today() - timedelta(days=lookback_days)).isoformat()


def _resolve_download_period(mode: str, backfill_period: str) -> Optional[str]:
    """Return the yfinance period for the selected download mode."""
    if mode == "daily":
        return None

    if mode == "backfill":
        return backfill_period

    raise ValueError("Price download mode must be either daily or backfill.")


def _chunk_securities(
    securities: List[Dict[str, Any]],
    batch_size: int,
) -> List[List[Dict[str, Any]]]:
    """Split securities into batches."""
    return [
        securities[start_index : start_index + batch_size]
        for start_index in range(0, len(securities), batch_size)
    ]


def _download_batch_from_yfinance(
    tickers: List[str],
    start_date: Optional[str],
    end_date: Optional[str],
    period: Optional[str],
) -> Any:
    """Download prices for a batch of tickers with yfinance."""
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
        "threads": True,
        "group_by": "column",
    }

    if start_date:
        options["start"] = start_date
    if end_date:
        options["end"] = end_date
    if period:
        options["period"] = period
    elif not start_date and not end_date:
        options["period"] = "max"

    return yf.download(tickers, **options)


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
