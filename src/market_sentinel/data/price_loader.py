"""Daily price downloading and storage helpers.

This module downloads daily historical prices for securities already stored in
the database. It uses yfinance for real downloads, but tests can pass fake
download functions so the test suite never needs live internet access.
"""

import csv
from datetime import date, datetime, timedelta
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Optional

import duckdb

Downloader = Callable[[str, Optional[str], Optional[str]], Any]
BatchDownloader = Callable[
    [List[str], Optional[str], Optional[str], Optional[str]],
    Any,
]
SleepFunction = Callable[[float], None]
DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE = 20
DEFAULT_PRICE_DAILY_LOOKBACK_DAYS = 10
DEFAULT_PRICE_UPDATE_OVERLAP_DAYS = 5
DEFAULT_SKIP_PRICE_UPDATE_IF_LATEST_DATE_IS_TODAY = True
DEFAULT_PRICE_UPDATE_STALE_AFTER_DAYS = 3
DEFAULT_PRICE_DOWNLOAD_LOOKBACK_DAYS = DEFAULT_PRICE_DAILY_LOOKBACK_DAYS
DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS = 3.0
DEFAULT_PRICE_BACKFILL_PERIOD = "5y"
DEFAULT_HISTORICAL_BACKFILL_YEARS = 2
DEFAULT_PRICE_RETRY_BATCH_SIZE = 5
DEFAULT_FAILED_PRICE_UPDATES_PATH = Path("outputs") / "failed_price_updates.csv"
DEFAULT_BATCH_SIZE = DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE
DEFAULT_BATCH_PAUSE_SECONDS = DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS


def get_active_securities(
    connection: duckdb.DuckDBPyConnection,
    market: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Read tickers from the securities table.

    The project does not have an ``active`` flag yet, so every security with a
    non-empty ticker is treated as active.
    """
    parameters: List[Any] = []
    market_filter = ""
    if market:
        market_filter = "AND market = ?"
        parameters.append(market)

    try:
        rows = connection.execute(
            f"""
            SELECT security_id, ticker
            FROM securities
            WHERE ticker IS NOT NULL
              AND TRIM(ticker) <> ''
              {market_filter}
            ORDER BY ticker
            """,
            parameters,
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
    failed_log_path: Path = DEFAULT_FAILED_PRICE_UPDATES_PATH,
    market: Optional[str] = None,
) -> Dict[str, Any]:
    """Download and store daily prices for every active ticker."""
    if batch_size <= 0:
        raise ValueError("Market data batch size must be greater than zero.")

    if pause_seconds < 0:
        raise ValueError("Market data batch pause must be zero or greater.")

    effective_start_date = _resolve_start_date(start_date, lookback_days, mode)
    download_period = _resolve_download_period(mode, backfill_period)
    securities = get_active_securities(connection, market=market)
    summary = {
        "tickers_checked": len(securities),
        "price_rows_written": 0,
        "failed_tickers": {},
    }
    if market:
        summary["market"] = market

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
                sleep_function,
                pause_seconds,
            )

        summary["price_rows_written"] += batch_rows_written
        summary["failed_tickers"].update(batch_failed_tickers)

        print(f"Rows written in this batch: {batch_rows_written}")

        if batch_failed_tickers:
            print("Failed tickers in this batch:")
            for ticker, failure in batch_failed_tickers.items():
                print(
                    f"- {ticker}: {failure['reason']} - {failure['details']}"
                )
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
        for ticker, failure in summary["failed_tickers"].items():
            print(
                f"- {ticker}: {failure['reason']} - {failure['details']}"
            )
    else:
        print("Failed tickers: none")

    if summary["failed_tickers"]:
        write_failed_price_updates(summary["failed_tickers"], failed_log_path)

    return summary


def update_recent_daily_prices(
    connection: duckdb.DuckDBPyConnection,
    batch_size: int = DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
    lookback_days: int = DEFAULT_PRICE_DAILY_LOOKBACK_DAYS,
    pause_seconds: float = DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
    batch_downloader: Optional[BatchDownloader] = None,
    failed_log_path: Path = DEFAULT_FAILED_PRICE_UPDATES_PATH,
) -> Dict[str, Any]:
    """Run the normal daily price update mode."""
    return update_daily_prices(
        connection,
        batch_size=batch_size,
        lookback_days=lookback_days,
        pause_seconds=pause_seconds,
        batch_downloader=batch_downloader,
        failed_log_path=failed_log_path,
        mode="daily",
    )


def update_incremental_daily_prices(
    connection: duckdb.DuckDBPyConnection,
    batch_size: int = DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
    overlap_days: int = DEFAULT_PRICE_UPDATE_OVERLAP_DAYS,
    backfill_period: str = DEFAULT_PRICE_BACKFILL_PERIOD,
    pause_seconds: float = DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
    downloader: Optional[Downloader] = None,
    batch_downloader: Optional[BatchDownloader] = None,
    skip_if_latest_date_is_today: bool = (
        DEFAULT_SKIP_PRICE_UPDATE_IF_LATEST_DATE_IS_TODAY
    ),
    stale_after_days: int = DEFAULT_PRICE_UPDATE_STALE_AFTER_DAYS,
    today: Optional[date] = None,
    failed_log_path: Path = DEFAULT_FAILED_PRICE_UPDATES_PATH,
    sleep_function: SleepFunction = time.sleep,
) -> Dict[str, Any]:
    """Update stale tickers from latest stored price date with an overlap."""
    if batch_size <= 0:
        raise ValueError("Market data batch size must be greater than zero.")
    if overlap_days < 0:
        raise ValueError("price_update_overlap_days must be zero or greater.")
    if pause_seconds < 0:
        raise ValueError("Market data batch pause must be zero or greater.")
    if stale_after_days < 0:
        raise ValueError("price_update_stale_after_days must be zero or greater.")

    today = today or date.today()
    securities = get_active_securities(connection)
    summary = {
        "tickers_checked": len(securities),
        "price_rows_written": 0,
        "current_tickers": 0,
        "incremental_tickers": 0,
        "full_tickers": 0,
        "failed_tickers": {},
    }
    expected_market_date = _latest_expected_market_date(today)
    update_groups: Dict[tuple, List[Dict[str, Any]]] = {}

    print(f"Total tickers checked for incremental market data: {len(securities)}")

    for security in securities:
        latest_price_date = _latest_price_date(connection, security["security_id"])
        classification = _classify_price_update(
            latest_price_date=latest_price_date,
            today=today,
            expected_market_date=expected_market_date,
            skip_if_latest_date_is_today=skip_if_latest_date_is_today,
            stale_after_days=stale_after_days,
            overlap_days=overlap_days,
            backfill_period=backfill_period,
        )

        if classification["mode"] == "current":
            summary["current_tickers"] += 1
            continue

        if classification["mode"] == "full":
            summary["full_tickers"] += 1
        else:
            summary["incremental_tickers"] += 1

        group_key = (
            classification["mode"],
            classification["start_date"],
            classification["period"],
        )
        update_groups.setdefault(group_key, []).append(security)

    print(f"Tickers skipped as current: {summary['current_tickers']}")
    print(f"Tickers updated incrementally: {summary['incremental_tickers']}")
    print(f"Tickers needing full download: {summary['full_tickers']}")

    update_batches = [
        (group_key, batch)
        for group_key, group_securities in update_groups.items()
        for batch in _chunk_securities(group_securities, batch_size)
    ]
    total_batches = len(update_batches)

    for batch_number, (group_key, batch) in enumerate(update_batches, 1):
        mode, start_date, period = group_key
        batch_failed_tickers: Dict[str, Dict[str, str]] = {}
        print(
            f"Starting {mode} price batch {batch_number} of {total_batches} "
            f"({len(batch)} tickers)"
        )

        if downloader is not None:
            batch_rows_written = _update_batch_with_single_ticker_downloader(
                connection,
                batch,
                start_date,
                None,
                downloader,
                batch_failed_tickers,
            )
        else:
            batch_rows_written = _update_batch_with_batch_downloader(
                connection,
                batch,
                start_date,
                None,
                period,
                batch_downloader,
                batch_failed_tickers,
                sleep_function,
                pause_seconds,
            )

        summary["price_rows_written"] += batch_rows_written
        summary["failed_tickers"].update(batch_failed_tickers)
        print(f"Rows written in this batch: {batch_rows_written}")

        if batch_number < total_batches and pause_seconds > 0:
            print(f"Pausing {pause_seconds:g} seconds before the next batch")
            sleep_function(pause_seconds)

    if summary["failed_tickers"]:
        write_failed_price_updates(summary["failed_tickers"], failed_log_path)

    print("Incremental market data update summary")
    print(f"Total tickers checked: {summary['tickers_checked']}")
    print(f"Tickers skipped as current: {summary['current_tickers']}")
    print(f"Incremental tickers: {summary['incremental_tickers']}")
    print(f"Full-history tickers: {summary['full_tickers']}")
    print(f"Failed tickers: {len(summary['failed_tickers'])}")
    print(f"Total price rows written: {summary['price_rows_written']}")
    return summary


def backfill_daily_prices(
    connection: duckdb.DuckDBPyConnection,
    batch_size: int = DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
    backfill_period: str = DEFAULT_PRICE_BACKFILL_PERIOD,
    required_history_days: int = DEFAULT_HISTORICAL_BACKFILL_YEARS * 365,
    pause_seconds: float = DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
    batch_downloader: Optional[BatchDownloader] = None,
    failed_log_path: Path = DEFAULT_FAILED_PRICE_UPDATES_PATH,
    market: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the larger historical price backfill mode."""
    securities = get_active_securities(connection, market=market)
    sufficient_history_count = sum(
        1
        for security in securities
        if _has_sufficient_price_history(
            connection,
            security["security_id"],
            required_history_days,
        )
    )

    print(
        "Historical backfill target: "
        f"{len(securities)} ticker(s)"
        + (f" in {market}" if market else "")
    )
    print(
        "Tickers already holding sufficient history: "
        f"{sufficient_history_count}"
    )

    summary = update_daily_prices(
        connection,
        batch_size=batch_size,
        pause_seconds=pause_seconds,
        mode="backfill",
        backfill_period=backfill_period,
        batch_downloader=batch_downloader,
        failed_log_path=failed_log_path,
        market=market,
    )
    summary["tickers_with_sufficient_history"] = sufficient_history_count
    summary["tickers_backfilled"] = (
        summary["tickers_checked"] - len(summary["failed_tickers"])
    )

    print("Historical backfill summary")
    print(f"Tickers checked: {summary['tickers_checked']}")
    print(
        "Tickers with sufficient history before backfill: "
        f"{summary['tickers_with_sufficient_history']}"
    )
    print(f"Tickers backfilled: {summary['tickers_backfilled']}")
    print(f"Tickers failed: {len(summary['failed_tickers'])}")
    return summary


def _has_sufficient_price_history(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    required_history_days: int,
) -> bool:
    """Return whether a ticker already has enough stored price history."""
    if required_history_days <= 0:
        return True

    row = connection.execute(
        """
        SELECT MIN(price_date), MAX(price_date)
        FROM daily_prices
        WHERE security_id = ?
        """,
        [security_id],
    ).fetchone()

    if row is None or row[0] is None or row[1] is None:
        return False

    first_date = _to_date(row[0])
    latest_date = _to_date(row[1])
    if first_date is None or latest_date is None:
        return False

    return (latest_date - first_date).days >= required_history_days


def _update_batch_with_single_ticker_downloader(
    connection: duckdb.DuckDBPyConnection,
    batch: List[Dict[str, Any]],
    start_date: Optional[str],
    end_date: Optional[str],
    downloader: Downloader,
    batch_failed_tickers: Dict[str, Dict[str, str]],
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
            batch_failed_tickers[ticker] = _failure_details(error)

    return batch_rows_written


def _update_batch_with_batch_downloader(
    connection: duckdb.DuckDBPyConnection,
    batch: List[Dict[str, Any]],
    start_date: Optional[str],
    end_date: Optional[str],
    period: Optional[str],
    batch_downloader: Optional[BatchDownloader],
    batch_failed_tickers: Dict[str, Dict[str, str]],
    sleep_function: SleepFunction,
    pause_seconds: float,
) -> int:
    """Update one batch using one yfinance request for all tickers."""
    batch_tickers = [security["ticker"] for security in batch]
    securities_by_ticker = {security["ticker"]: security for security in batch}

    batch_rows_written = _download_and_store_batch(
        connection,
        securities_by_ticker,
        batch_tickers,
        start_date,
        end_date,
        period,
        batch_downloader,
        batch_failed_tickers,
    )

    if not batch_failed_tickers:
        return batch_rows_written

    retry_tickers = list(batch_failed_tickers)
    print(
        "Retrying failed tickers once in smaller groups: "
        + ", ".join(retry_tickers)
    )

    if pause_seconds > 0:
        sleep_function(pause_seconds)

    for retry_group in _chunk_tickers(retry_tickers, DEFAULT_PRICE_RETRY_BATCH_SIZE):
        retry_failures: Dict[str, Dict[str, str]] = {}
        retry_rows_written = _download_and_store_batch(
            connection,
            securities_by_ticker,
            retry_group,
            start_date,
            end_date,
            period,
            batch_downloader,
            retry_failures,
        )
        batch_rows_written += retry_rows_written

        for ticker in retry_group:
            if ticker not in retry_failures:
                batch_failed_tickers.pop(ticker, None)
            else:
                batch_failed_tickers[ticker] = retry_failures[ticker]

    return batch_rows_written


def _download_and_store_batch(
    connection: duckdb.DuckDBPyConnection,
    securities_by_ticker: Dict[str, Dict[str, Any]],
    tickers: List[str],
    start_date: Optional[str],
    end_date: Optional[str],
    period: Optional[str],
    batch_downloader: Optional[BatchDownloader],
    batch_failed_tickers: Dict[str, Dict[str, str]],
) -> int:
    """Download and store one group of tickers."""
    try:
        price_rows_by_ticker = download_daily_prices_for_batch(
            tickers,
            start_date=start_date,
            end_date=end_date,
            period=period,
            batch_downloader=batch_downloader,
        )
    except RuntimeError as error:
        failure = _failure_details(error)
        for ticker in tickers:
            batch_failed_tickers[ticker] = failure
        return 0

    batch_rows_written = 0

    for ticker in tickers:
        security = securities_by_ticker[ticker]
        price_rows = price_rows_by_ticker.get(ticker, [])

        if not price_rows:
            batch_failed_tickers[ticker] = {
                "reason": "no_price_rows",
                "details": "No recent price rows were returned for this ticker.",
            }
            continue

        try:
            batch_rows_written += upsert_daily_prices(
                connection,
                security["security_id"],
                price_rows,
            )
        except RuntimeError as error:
            batch_failed_tickers[ticker] = _failure_details(error)

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


def _latest_price_date(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
) -> Optional[date]:
    """Return the latest stored daily price date for one security."""
    value = connection.execute(
        """
        SELECT MAX(price_date)
        FROM daily_prices
        WHERE security_id = ?
        """,
        [security_id],
    ).fetchone()[0]

    return _to_date(value)


def _to_date(value: Any) -> Optional[date]:
    """Convert DuckDB date-like values to a date."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    return date.fromisoformat(str(value)[:10])


def _classify_price_update(
    latest_price_date: Optional[date],
    today: date,
    expected_market_date: date,
    skip_if_latest_date_is_today: bool,
    stale_after_days: int,
    overlap_days: int,
    backfill_period: str,
) -> Dict[str, Any]:
    """Classify one ticker as current, incremental, or full download."""
    if latest_price_date is None:
        return {"mode": "full", "start_date": None, "period": backfill_period}

    if _price_date_is_current_enough(
        latest_price_date=latest_price_date,
        today=today,
        expected_market_date=expected_market_date,
        skip_if_latest_date_is_today=skip_if_latest_date_is_today,
        stale_after_days=stale_after_days,
    ):
        return {"mode": "current", "start_date": None, "period": None}

    start_date = (latest_price_date - timedelta(days=overlap_days)).isoformat()
    return {"mode": "incremental", "start_date": start_date, "period": None}


def _price_date_is_current_enough(
    latest_price_date: date,
    today: date,
    expected_market_date: date,
    skip_if_latest_date_is_today: bool,
    stale_after_days: int,
) -> bool:
    """Return whether a ticker can skip a daily price download."""
    if skip_if_latest_date_is_today and latest_price_date >= today:
        return True

    if latest_price_date >= expected_market_date:
        return True

    stale_cutoff_date = today - timedelta(days=stale_after_days)
    return latest_price_date >= stale_cutoff_date


def _latest_expected_market_date(today: date) -> date:
    """Return a simple expected market date without holiday calendar support."""
    expected_date = today
    while expected_date.weekday() >= 5:
        expected_date -= timedelta(days=1)
    return expected_date


def _chunk_securities(
    securities: List[Dict[str, Any]],
    batch_size: int,
) -> List[List[Dict[str, Any]]]:
    """Split securities into batches."""
    return [
        securities[start_index : start_index + batch_size]
        for start_index in range(0, len(securities), batch_size)
    ]


def _chunk_tickers(tickers: List[str], batch_size: int) -> List[List[str]]:
    """Split tickers into batches."""
    return [
        tickers[start_index : start_index + batch_size]
        for start_index in range(0, len(tickers), batch_size)
    ]


def _failure_details(error: Exception) -> Dict[str, str]:
    """Return a clearer failure reason and details."""
    details = str(error)
    lower_details = details.lower()

    if any(
        text in lower_details
        for text in [
            "dns",
            "certificate",
            "cert",
            "getaddrinfo",
            "name resolution",
            "temporary failure",
            "connection",
            "timeout",
            "timed out",
            "network",
            "ssl",
        ]
    ):
        reason = "network_error"
    elif "no recent price rows" in lower_details or "no price" in lower_details:
        reason = "no_price_rows"
    elif "parse" in lower_details or "parsing" in lower_details:
        reason = "parsing_error"
    else:
        reason = "unknown_error"

    return {"reason": reason, "details": details}


def write_failed_price_updates(
    failed_tickers: Dict[str, Dict[str, str]],
    failed_log_path: Path = DEFAULT_FAILED_PRICE_UPDATES_PATH,
) -> None:
    """Write failed price updates to a CSV file."""
    failed_log_path = Path(failed_log_path)
    failed_log_path.parent.mkdir(parents=True, exist_ok=True)

    with failed_log_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["ticker", "reason", "details", "date"],
        )
        writer.writeheader()

        for ticker, failure in sorted(failed_tickers.items()):
            writer.writerow(
                {
                    "ticker": ticker,
                    "reason": failure["reason"],
                    "details": failure["details"],
                    "date": date.today().isoformat(),
                }
            )


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
        "threads": False,
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
