"""Data health checks for market-sentinel reports and daily runs."""

from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import duckdb

from market_sentinel.config.loader import load_named_config

DEFAULT_DATA_HEALTH_STALE_PRICE_DAYS = 5
DEFAULT_MIN_PRICE_ROWS = 180
DEFAULT_FAILED_PRICE_UPDATES_PATH = Path("outputs") / "failed_price_updates.csv"
DATA_HEALTH_OK = "OK"
DATA_HEALTH_WARNING = "Warning"
DATA_HEALTH_ACTION_NEEDED = "Action needed"


def check_data_health(
    connection: duckdb.DuckDBPyConnection,
    config_dir: Optional[Path] = None,
    stale_price_days: Optional[int] = None,
    min_price_rows: int = DEFAULT_MIN_PRICE_ROWS,
    failed_tickers: Optional[Iterable[str]] = None,
    failed_log_path: Path = DEFAULT_FAILED_PRICE_UPDATES_PATH,
) -> Dict[str, Any]:
    """Return a compact health summary for the market data in DuckDB."""
    settings = _load_settings(config_dir)
    stale_days = _positive_int(
        stale_price_days
        if stale_price_days is not None
        else settings.get("data_health_stale_price_days"),
        DEFAULT_DATA_HEALTH_STALE_PRICE_DAYS,
    )

    try:
        securities_by_market = _fetch_securities_by_market(connection)
        price_rows = _fetch_security_price_health(connection)
        moving_average_security_ids = _fetch_moving_average_security_ids(connection)
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read database tables for the data health check. "
            "Reports can only continue after the database can be read."
        ) from error

    reference_date = _latest_price_date(price_rows)
    no_price_data: List[Dict[str, Any]] = []
    stale_price_tickers: List[Dict[str, Any]] = []
    insufficient_price_history: List[Dict[str, Any]] = []
    missing_moving_average_data: List[Dict[str, Any]] = []

    for row in price_rows:
        latest_price_date = _coerce_date(row["latest_price_date"])
        price_count = int(row["price_rows"] or 0)
        ticker_summary = {
            "ticker": row["ticker"],
            "market": row["market"],
            "latest_price_date": latest_price_date,
            "price_rows": price_count,
        }

        if price_count == 0:
            no_price_data.append(ticker_summary)
        elif reference_date and latest_price_date:
            days_stale = (reference_date - latest_price_date).days
            if days_stale > stale_days:
                stale_copy = dict(ticker_summary)
                stale_copy["days_stale"] = days_stale
                stale_price_tickers.append(stale_copy)

        if price_count < min_price_rows:
            insufficient_price_history.append(ticker_summary)

        if row["security_id"] not in moving_average_security_ids:
            missing_moving_average_data.append(ticker_summary)

    failed_source = (
        failed_tickers
        if failed_tickers is not None
        else _read_latest_failed_price_updates(failed_log_path)
    )
    failed = sorted({str(ticker) for ticker in failed_source or [] if ticker})
    status = _health_status(
        no_price_data=no_price_data,
        stale_price_tickers=stale_price_tickers,
        insufficient_price_history=insufficient_price_history,
        missing_moving_average_data=missing_moving_average_data,
        failed_tickers=failed,
    )

    summary = {
        "status": status,
        "securities_checked": len(price_rows),
        "securities_by_market": securities_by_market,
        "reference_price_date": reference_date,
        "stale_price_days": stale_days,
        "min_price_rows": min_price_rows,
        "no_price_data": no_price_data,
        "stale_price_tickers": stale_price_tickers,
        "insufficient_price_history": insufficient_price_history,
        "missing_moving_average_data": missing_moving_average_data,
        "failed_tickers": failed,
    }
    summary["summary_line"] = format_data_health_line(summary)
    return summary


def format_data_health_line(summary: Dict[str, Any]) -> str:
    """Return the short health line used in PDF and console output."""
    status = summary.get("status", DATA_HEALTH_WARNING)
    checked = int(summary.get("securities_checked") or 0)

    if status == DATA_HEALTH_OK:
        return f"Data health: OK - {checked} securities checked"

    issue_parts = []
    stale_count = len(summary.get("stale_price_tickers") or [])
    missing_price_count = len(summary.get("no_price_data") or [])
    short_history_count = len(summary.get("insufficient_price_history") or [])
    missing_ma_count = len(summary.get("missing_moving_average_data") or [])
    failed_count = len(summary.get("failed_tickers") or [])

    if stale_count:
        issue_parts.append(_plural_count(stale_count, "stale ticker"))
    if missing_price_count:
        issue_parts.append(_plural_count(missing_price_count, "missing price history"))
    if short_history_count:
        issue_parts.append(_plural_count(short_history_count, "short price history"))
    if missing_ma_count:
        issue_parts.append(_plural_count(missing_ma_count, "missing moving average"))
    if failed_count:
        issue_parts.append(_plural_count(failed_count, "failed update"))

    if not issue_parts:
        issue_parts.append(f"{checked} securities checked")

    return f"Data health: {status} - {', '.join(issue_parts)}"


def print_data_health_summary(summary: Dict[str, Any]) -> None:
    """Print a beginner-friendly health summary."""
    print(format_data_health_line(summary))
    market_counts = summary.get("securities_by_market") or {}
    if market_counts:
        market_text = " | ".join(
            f"{market}: {count}" for market, count in sorted(market_counts.items())
        )
        print(f"Securities by market: {market_text}")

    print(
        "Issues: "
        f"{len(summary.get('no_price_data') or [])} missing price histories, "
        f"{len(summary.get('stale_price_tickers') or [])} stale tickers, "
        f"{len(summary.get('insufficient_price_history') or [])} short histories, "
        f"{len(summary.get('missing_moving_average_data') or [])} missing moving averages"
    )

    failed = summary.get("failed_tickers") or []
    if failed:
        print(f"Failed tickers from latest update: {', '.join(failed)}")


def _load_settings(config_dir: Optional[Path]) -> Dict[str, Any]:
    try:
        return load_named_config("settings", config_dir)
    except FileNotFoundError:
        return {}


def _fetch_securities_by_market(
    connection: duckdb.DuckDBPyConnection,
) -> Dict[str, int]:
    rows = connection.execute(
        """
        SELECT COALESCE(market, 'Market unknown') AS market, COUNT(*) AS count
        FROM securities
        GROUP BY COALESCE(market, 'Market unknown')
        ORDER BY market
        """
    ).fetchall()
    return {str(market): int(count) for market, count in rows}


def _fetch_security_price_health(
    connection: duckdb.DuckDBPyConnection,
) -> List[Dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            securities.security_id,
            securities.ticker,
            COALESCE(securities.market, 'Market unknown') AS market,
            COUNT(daily_prices.price_date) AS price_rows,
            MAX(daily_prices.price_date) AS latest_price_date
        FROM securities
        LEFT JOIN daily_prices
            ON securities.security_id = daily_prices.security_id
        GROUP BY
            securities.security_id,
            securities.ticker,
            COALESCE(securities.market, 'Market unknown')
        ORDER BY securities.ticker
        """
    ).fetchall()
    return [
        {
            "security_id": security_id,
            "ticker": ticker,
            "market": market,
            "price_rows": price_rows,
            "latest_price_date": latest_price_date,
        }
        for security_id, ticker, market, price_rows, latest_price_date in rows
    ]


def _fetch_moving_average_security_ids(
    connection: duckdb.DuckDBPyConnection,
) -> set:
    rows = connection.execute(
        """
        SELECT DISTINCT security_id
        FROM moving_average_signals
        WHERE signal_type = 'SMA'
        """
    ).fetchall()
    return {row[0] for row in rows}


def _read_latest_failed_price_updates(failed_log_path: Path) -> List[str]:
    """Return tickers from today's failed price update log when it exists."""
    failed_log_path = Path(failed_log_path)
    if not failed_log_path.exists():
        return []

    try:
        with failed_log_path.open("r", encoding="utf-8", newline="") as csv_file:
            rows = list(csv.DictReader(csv_file))
    except OSError:
        return []

    today = date.today().isoformat()
    return [
        str(row.get("ticker", "")).strip()
        for row in rows
        if str(row.get("date", "")).strip() == today
        and str(row.get("ticker", "")).strip()
    ]


def _latest_price_date(price_rows: List[Dict[str, Any]]) -> Optional[date]:
    latest_dates = [
        coerced
        for coerced in (
            _coerce_date(row.get("latest_price_date")) for row in price_rows
        )
        if coerced is not None
    ]
    if not latest_dates:
        return None
    return max(latest_dates)


def _coerce_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    return None


def _health_status(
    no_price_data: List[Dict[str, Any]],
    stale_price_tickers: List[Dict[str, Any]],
    insufficient_price_history: List[Dict[str, Any]],
    missing_moving_average_data: List[Dict[str, Any]],
    failed_tickers: List[str],
) -> str:
    if no_price_data or failed_tickers:
        return DATA_HEALTH_ACTION_NEEDED
    if stale_price_tickers or insufficient_price_history or missing_moving_average_data:
        return DATA_HEALTH_WARNING
    return DATA_HEALTH_OK


def _positive_int(value: Any, default_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default_value
    return parsed if parsed > 0 else default_value


def _plural_count(count: int, singular: str) -> str:
    if count == 1:
        return f"{count} {singular}"
    if singular.endswith("y"):
        return f"{count} {singular[:-1]}ies"
    return f"{count} {singular}s"
