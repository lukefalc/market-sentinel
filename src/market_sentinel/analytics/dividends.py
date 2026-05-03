"""Dividend analysis calculations.

This module fetches dividend history for active securities, stores dividend
events, and calculates simple dividend metrics using the latest local close
price. It does not implement dividend trap flags.
"""

from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import duckdb

from market_sentinel.data.price_loader import get_active_securities

DividendDownloader = Callable[[str], Any]


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
) -> Dict[str, Any]:
    """Fetch dividends and calculate dividend metrics for active securities."""
    securities = _get_active_securities_with_currency(connection)
    summary = {
        "tickers_checked": len(securities),
        "dividend_rows_written": 0,
        "metrics_written": 0,
        "no_dividend_history": [],
        "skipped_tickers": {},
        "failed_tickers": {},
    }

    for security in securities:
        ticker = security["ticker"]

        try:
            dividend_rows = download_dividends(ticker, downloader=downloader)
            summary["dividend_rows_written"] += upsert_dividend_events(
                connection,
                security["security_id"],
                security.get("currency"),
                dividend_rows,
            )

            if not dividend_rows:
                summary["no_dividend_history"].append(ticker)

            latest_price = get_latest_close_price(connection, security["security_id"])
            if latest_price is None:
                summary["skipped_tickers"][ticker] = (
                    "No latest close price is available for this ticker."
                )
                continue

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
            summary["metrics_written"] += 1
        except RuntimeError as error:
            summary["failed_tickers"][ticker] = str(error)

    return summary


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
