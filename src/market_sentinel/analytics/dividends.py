"""Dividend analysis calculations.

This module fetches dividend history for active securities, stores dividend
events, and calculates simple dividend metrics using the latest local close
price. It does not implement dividend trap flags.
"""

import csv
from datetime import date, datetime, timedelta
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Optional

import duckdb

from market_sentinel.data.price_loader import get_active_securities

DividendDownloader = Callable[[str], Any]
SleepFunction = Callable[[float], None]
DEFAULT_DIVIDEND_DOWNLOAD_BATCH_SIZE = 20
DEFAULT_DIVIDEND_DOWNLOAD_PAUSE_SECONDS = 3.0
DEFAULT_DIVIDEND_RETRY_BATCH_SIZE = 5
DEFAULT_FAILED_DIVIDEND_UPDATES_PATH = (
    Path("outputs") / "failed_dividend_updates.csv"
)


def download_dividends(
    ticker: str,
    downloader: Optional[DividendDownloader] = None,
) -> List[Dict[str, Any]]:
    """Download dividend history for one ticker and normalise it."""
    download_function = downloader or _download_from_yfinance

    try:
        raw_dividends = download_function(ticker)
    except Exception as error:
        raise RuntimeError(
            f"Could not download dividends for {ticker}. Check the ticker "
            f"symbol and your internet connection. Details: {error}"
        ) from error

    return normalise_dividend_data(raw_dividends)


def normalise_dividend_data(raw_dividends: Any) -> List[Dict[str, Any]]:
    """Convert downloaded dividend data into dictionaries."""
    if raw_dividends is None:
        return []

    if isinstance(raw_dividends, list):
        return [
            {
                "ex_dividend_date": _normalise_date(row["ex_dividend_date"]),
                "dividend_amount": float(row["dividend_amount"]),
            }
            for row in raw_dividends
        ]

    if getattr(raw_dividends, "empty", False):
        return []

    rows = []
    for index_value, amount in raw_dividends.items():
        if amount is None:
            continue
        rows.append(
            {
                "ex_dividend_date": _normalise_date(index_value),
                "dividend_amount": float(amount),
            }
        )

    return rows


def upsert_dividend_events(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    currency: Optional[str],
    dividend_rows: List[Dict[str, Any]],
) -> int:
    """Insert or update dividend events without creating duplicates."""
    written_count = 0

    try:
        for row in dividend_rows:
            existing_id = connection.execute(
                """
                SELECT dividend_id
                FROM dividends
                WHERE security_id = ?
                  AND ex_dividend_date = ?
                """,
                [security_id, row["ex_dividend_date"]],
            ).fetchone()

            if existing_id:
                connection.execute(
                    """
                    UPDATE dividends
                    SET dividend_amount = ?,
                        currency = ?
                    WHERE dividend_id = ?
                    """,
                    [row["dividend_amount"], currency, existing_id[0]],
                )
            else:
                next_id = connection.execute(
                    "SELECT COALESCE(MAX(dividend_id), 0) + 1 FROM dividends"
                ).fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO dividends (
                        dividend_id,
                        security_id,
                        ex_dividend_date,
                        dividend_amount,
                        currency
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        next_id,
                        security_id,
                        row["ex_dividend_date"],
                        row["dividend_amount"],
                        currency,
                    ],
                )

            written_count += 1
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not save dividend events to DuckDB. Check that the database "
            f"is open and the dividends table exists. Details: {error}"
        ) from error

    return written_count


def get_latest_close_price(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
) -> Optional[Dict[str, Any]]:
    """Return the latest close price for a security."""
    try:
        row = connection.execute(
            """
            SELECT price_date, close_price
            FROM daily_prices
            WHERE security_id = ?
            ORDER BY price_date DESC
            LIMIT 1
            """,
            [security_id],
        ).fetchone()
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read latest prices from DuckDB. Check that the database "
            f"is open and the daily_prices table exists. Details: {error}"
        ) from error

    if row is None:
        return None

    return {"price_date": row[0], "close_price": row[1]}


def calculate_trailing_annual_dividend(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    metric_date: Any,
) -> float:
    """Calculate dividends paid in the trailing 12 months."""
    end_date = _as_date(metric_date)
    start_date = end_date - timedelta(days=365)

    try:
        total = connection.execute(
            """
            SELECT COALESCE(SUM(dividend_amount), 0)
            FROM dividends
            WHERE security_id = ?
              AND ex_dividend_date > ?
              AND ex_dividend_date <= ?
            """,
            [security_id, start_date.isoformat(), end_date.isoformat()],
        ).fetchone()[0]
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not calculate trailing dividends from DuckDB. Check that "
            f"the dividends table exists. Details: {error}"
        ) from error

    return float(total or 0)


def upsert_dividend_metric(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    metric_date: Any,
    trailing_annual_dividend: float,
    dividend_yield: float,
    annual_cash_per_10000: float,
) -> None:
    """Insert or update one dividend metric row."""
    try:
        existing_id = connection.execute(
            """
            SELECT metric_id
            FROM dividend_metrics
            WHERE security_id = ?
              AND metric_date = ?
            """,
            [security_id, metric_date],
        ).fetchone()

        if existing_id:
            connection.execute(
                """
                UPDATE dividend_metrics
                SET trailing_annual_dividend = ?,
                    dividend_yield = ?,
                    annual_dividend_cash_per_10000 = ?
                WHERE metric_id = ?
                """,
                [
                    trailing_annual_dividend,
                    dividend_yield,
                    annual_cash_per_10000,
                    existing_id[0],
                ],
            )
            return

        next_id = connection.execute(
            "SELECT COALESCE(MAX(metric_id), 0) + 1 FROM dividend_metrics"
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO dividend_metrics (
                metric_id,
                security_id,
                metric_date,
                trailing_annual_dividend,
                dividend_yield,
                annual_dividend_cash_per_10000
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                next_id,
                security_id,
                metric_date,
                trailing_annual_dividend,
                dividend_yield,
                annual_cash_per_10000,
            ],
        )
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not save dividend metrics to DuckDB. Check that the "
            f"dividend_metrics table exists. Details: {error}"
        ) from error


def calculate_and_store_dividends(
    connection: duckdb.DuckDBPyConnection,
    downloader: Optional[DividendDownloader] = None,
    batch_size: int = DEFAULT_DIVIDEND_DOWNLOAD_BATCH_SIZE,
    pause_seconds: float = DEFAULT_DIVIDEND_DOWNLOAD_PAUSE_SECONDS,
    retry_batch_size: int = DEFAULT_DIVIDEND_RETRY_BATCH_SIZE,
    sleep_function: SleepFunction = time.sleep,
    failed_log_path: Path = DEFAULT_FAILED_DIVIDEND_UPDATES_PATH,
) -> Dict[str, Any]:
    """Fetch dividends and calculate dividend metrics for active securities."""
    if batch_size <= 0:
        raise ValueError("Dividend download batch size must be greater than zero.")

    if retry_batch_size <= 0:
        raise ValueError("Dividend retry batch size must be greater than zero.")

    if pause_seconds < 0:
        raise ValueError("Dividend download pause must be zero or greater.")

    securities = _get_active_securities_with_currency(connection)
    summary = {
        "tickers_checked": len(securities),
        "dividend_rows_written": 0,
        "metrics_written": 0,
        "no_dividend_history": [],
        "skipped_tickers": {},
        "failed_tickers": {},
    }

    batches = _chunk_securities(securities, batch_size)
    total_batches = len(batches)

    print(f"Total tickers to process for dividends: {len(securities)}")

    for batch_number, batch in enumerate(batches, start=1):
        batch_tickers = [security["ticker"] for security in batch]
        batch_failed_tickers: Dict[str, Dict[str, str]] = {}
        batch_summary = _empty_batch_summary()

        print(f"Starting dividend batch {batch_number} of {total_batches}")
        print(f"Tickers in this batch: {', '.join(batch_tickers)}")

        _process_dividend_batch(
            connection,
            batch,
            summary,
            batch_summary,
            batch_failed_tickers,
            downloader,
        )

        if batch_failed_tickers:
            print(
                "Retrying failed dividend tickers once in smaller groups: "
                + ", ".join(batch_failed_tickers)
            )
            if pause_seconds > 0:
                sleep_function(pause_seconds)

            retry_tickers = list(batch_failed_tickers)
            securities_by_ticker = {
                security["ticker"]: security for security in securities
            }

            for retry_group in _chunk_tickers(retry_tickers, retry_batch_size):
                retry_failures: Dict[str, Dict[str, str]] = {}
                retry_batch = [
                    securities_by_ticker[ticker] for ticker in retry_group
                ]
                _process_dividend_batch(
                    connection,
                    retry_batch,
                    summary,
                    batch_summary,
                    retry_failures,
                    downloader,
                )

                for ticker in retry_group:
                    if ticker not in retry_failures:
                        batch_failed_tickers.pop(ticker, None)
                    else:
                        batch_failed_tickers[ticker] = retry_failures[ticker]

        summary["failed_tickers"].update(batch_failed_tickers)

        print(
            "Dividend rows written in this batch: "
            f"{batch_summary['dividend_rows_written']}"
        )
        print(
            "Dividend metric rows written in this batch: "
            f"{batch_summary['metrics_written']}"
        )

        if batch_failed_tickers:
            print("Failed dividend tickers in this batch:")
            for ticker, failure in batch_failed_tickers.items():
                print(f"- {ticker}: {failure['reason']} - {failure['details']}")
        else:
            print("Failed dividend tickers in this batch: none")

        if batch_number < total_batches and pause_seconds > 0:
            print(f"Pausing {pause_seconds:g} seconds before the next batch")
            sleep_function(pause_seconds)

    print("Dividend update summary")
    print(f"Total tickers checked: {summary['tickers_checked']}")
    print(f"Total dividend rows written: {summary['dividend_rows_written']}")
    print(f"Total dividend metric rows written: {summary['metrics_written']}")

    if summary["failed_tickers"]:
        print("Failed dividend tickers:")
        for ticker, failure in summary["failed_tickers"].items():
            print(f"- {ticker}: {failure['reason']} - {failure['details']}")
        write_failed_dividend_updates(summary["failed_tickers"], failed_log_path)
    else:
        print("Failed dividend tickers: none")

    return summary


def _process_dividend_batch(
    connection: duckdb.DuckDBPyConnection,
    batch: List[Dict[str, Any]],
    summary: Dict[str, Any],
    batch_summary: Dict[str, int],
    failed_tickers: Dict[str, Dict[str, str]],
    downloader: Optional[DividendDownloader],
) -> None:
    """Process one group of dividend tickers."""
    for security in batch:
        ticker = security["ticker"]

        try:
            ticker_summary = _process_one_dividend_ticker(
                connection,
                security,
                downloader,
            )
        except RuntimeError as error:
            failed_tickers[ticker] = _failure_details(error)
            continue

        summary["dividend_rows_written"] += ticker_summary[
            "dividend_rows_written"
        ]
        batch_summary["dividend_rows_written"] += ticker_summary[
            "dividend_rows_written"
        ]

        if ticker_summary["no_dividend_history"]:
            summary["no_dividend_history"].append(ticker)

        if ticker_summary["skipped_reason"]:
            summary["skipped_tickers"][ticker] = ticker_summary["skipped_reason"]

        summary["metrics_written"] += ticker_summary["metrics_written"]
        batch_summary["metrics_written"] += ticker_summary["metrics_written"]


def _process_one_dividend_ticker(
    connection: duckdb.DuckDBPyConnection,
    security: Dict[str, Any],
    downloader: Optional[DividendDownloader],
) -> Dict[str, Any]:
    """Download dividends and calculate metrics for one ticker."""
    ticker = security["ticker"]
    dividend_rows = download_dividends(ticker, downloader=downloader)
    dividend_rows_written = upsert_dividend_events(
        connection,
        security["security_id"],
        security.get("currency"),
        dividend_rows,
    )

    latest_price = get_latest_close_price(connection, security["security_id"])
    if latest_price is None:
        return {
            "dividend_rows_written": dividend_rows_written,
            "metrics_written": 0,
            "no_dividend_history": not dividend_rows,
            "skipped_reason": "No latest close price is available for this ticker.",
        }

    close_price = latest_price["close_price"]
    metric_date = latest_price["price_date"]
    trailing_dividend = calculate_trailing_annual_dividend(
        connection,
        security["security_id"],
        metric_date,
    )
    dividend_yield = trailing_dividend / close_price if close_price else 0
    annual_cash = dividend_yield * 10000

    upsert_dividend_metric(
        connection,
        security["security_id"],
        metric_date,
        trailing_dividend,
        dividend_yield,
        annual_cash,
    )

    return {
        "dividend_rows_written": dividend_rows_written,
        "metrics_written": 1,
        "no_dividend_history": not dividend_rows,
        "skipped_reason": None,
    }


def _empty_batch_summary() -> Dict[str, int]:
    """Return counters for one dividend batch."""
    return {"dividend_rows_written": 0, "metrics_written": 0}


def _get_active_securities_with_currency(
    connection: duckdb.DuckDBPyConnection,
) -> List[Dict[str, Any]]:
    """Read active tickers and currency from the securities table."""
    securities = get_active_securities(connection)

    try:
        currencies = {
            row[0]: row[1]
            for row in connection.execute(
                """
                SELECT security_id, currency
                FROM securities
                """
            ).fetchall()
        }
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read security currencies from DuckDB. Check that the "
            f"securities table has been created. Details: {error}"
        ) from error

    for security in securities:
        security["currency"] = currencies.get(security["security_id"])

    return securities


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
    """Split tickers into retry batches."""
    return [
        tickers[start_index : start_index + batch_size]
        for start_index in range(0, len(tickers), batch_size)
    ]


def _failure_details(error: Exception) -> Dict[str, str]:
    """Return a beginner-friendly failure reason and details."""
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
    elif "no dividend" in lower_details or "no history" in lower_details:
        reason = "no_dividend_rows"
    elif "parse" in lower_details or "parsing" in lower_details:
        reason = "parsing_error"
    else:
        reason = "unknown_error"

    return {"reason": reason, "details": details}


def write_failed_dividend_updates(
    failed_tickers: Dict[str, Dict[str, str]],
    failed_log_path: Path = DEFAULT_FAILED_DIVIDEND_UPDATES_PATH,
) -> None:
    """Write failed dividend updates to a CSV file."""
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


def _download_from_yfinance(ticker: str) -> Any:
    """Download dividend history with yfinance."""
    try:
        import yfinance as yf
    except ImportError as error:
        raise RuntimeError(
            "yfinance is not installed. Install the project dependencies with "
            'pip install -e ".[dev]" and try again.'
        ) from error

    return yf.Ticker(ticker).dividends


def _normalise_date(value: Any) -> str:
    """Return a date string that DuckDB can store in a DATE column."""
    return _as_date(value).isoformat()


def _as_date(value: Any) -> date:
    """Convert common date-like values to a Python date."""
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if hasattr(value, "date"):
        return value.date()

    return datetime.strptime(str(value), "%Y-%m-%d").date()
