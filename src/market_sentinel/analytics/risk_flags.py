"""Dividend risk flag calculations.

This module applies simple dividend trap risk rules to the latest dividend
metrics. It does not generate reports or PDF files.
"""

from typing import Any, Dict, List, Optional, Tuple

import duckdb

HIGH_YIELD_THRESHOLD = 0.07
CAUTION_YIELD_THRESHOLD = 0.06
LONG_SMA_PERIOD = 200


def evaluate_dividend_risk(
    dividend_yield: Optional[float],
    trailing_annual_dividend: Optional[float],
    latest_close_price: Optional[float],
    sma_200: Optional[float],
) -> Tuple[Optional[str], Optional[str]]:
    """Return a risk flag and reason for one security."""
    if dividend_yield is None:
        return None, None

    if dividend_yield > HIGH_YIELD_THRESHOLD:
        return "DIVIDEND_TRAP_RISK", "Dividend yield is above 7%."

    if dividend_yield > CAUTION_YIELD_THRESHOLD:
        if not trailing_annual_dividend:
            return (
                "DIVIDEND_TRAP_RISK",
                "Dividend yield is above 6%, but trailing 12-month dividends "
                "are zero or missing.",
            )

        if sma_200 is None:
            return (
                "DIVIDEND_TRAP_RISK",
                "Dividend yield is above 6%, but the 200-day SMA is missing.",
            )

        if latest_close_price is not None and latest_close_price < sma_200:
            return (
                "DIVIDEND_TRAP_RISK",
                "Dividend yield is above 6% and price is below the 200-day SMA.",
            )

    return None, None


def calculate_and_store_risk_flags(
    connection: duckdb.DuckDBPyConnection,
) -> Dict[str, Any]:
    """Calculate dividend risk flags for latest dividend metrics."""
    latest_metrics = _get_latest_dividend_metrics(connection)
    summary = {
        "metrics_checked": len(latest_metrics),
        "risk_flags_written": 0,
        "cleared_flags": 0,
        "skipped": {},
    }

    for metric in latest_metrics:
        ticker = metric["ticker"]
        latest_price = _get_latest_close_price(connection, metric["security_id"])
        sma_200 = _get_latest_sma_value(
            connection,
            metric["security_id"],
            LONG_SMA_PERIOD,
        )

        if latest_price is None:
            summary["skipped"][ticker] = (
                "No latest close price is available for this ticker."
            )
            continue

        risk_flag, risk_reason = evaluate_dividend_risk(
            metric["dividend_yield"],
            metric["trailing_annual_dividend"],
            latest_price,
            sma_200,
        )
        _update_metric_risk_flag(
            connection,
            metric["metric_id"],
            risk_flag,
            risk_reason,
        )

        if risk_flag:
            summary["risk_flags_written"] += 1
        else:
            summary["cleared_flags"] += 1

    return summary


def _get_latest_dividend_metrics(
    connection: duckdb.DuckDBPyConnection,
) -> List[Dict[str, Any]]:
    """Read the latest dividend metric row for each security."""
    try:
        rows = connection.execute(
            """
            SELECT
                metrics.metric_id,
                metrics.security_id,
                securities.ticker,
                metrics.metric_date,
                metrics.trailing_annual_dividend,
                metrics.dividend_yield
            FROM dividend_metrics AS metrics
            INNER JOIN securities
                ON metrics.security_id = securities.security_id
            INNER JOIN (
                SELECT security_id, MAX(metric_date) AS latest_metric_date
                FROM dividend_metrics
                GROUP BY security_id
            ) AS latest_metrics
                ON metrics.security_id = latest_metrics.security_id
               AND metrics.metric_date = latest_metrics.latest_metric_date
            ORDER BY securities.ticker
            """
        ).fetchall()
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read dividend metrics from DuckDB. Check that the "
            f"dividend_metrics table exists. Details: {error}"
        ) from error

    return [
        {
            "metric_id": row[0],
            "security_id": row[1],
            "ticker": row[2],
            "metric_date": row[3],
            "trailing_annual_dividend": row[4],
            "dividend_yield": row[5],
        }
        for row in rows
    ]


def _get_latest_close_price(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
) -> Optional[float]:
    """Read the latest close price for one security."""
    try:
        row = connection.execute(
            """
            SELECT close_price
            FROM daily_prices
            WHERE security_id = ?
            ORDER BY price_date DESC
            LIMIT 1
            """,
            [security_id],
        ).fetchone()
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read latest prices from DuckDB. Check that the "
            f"daily_prices table exists. Details: {error}"
        ) from error

    if row is None:
        return None

    return row[0]


def _get_latest_sma_value(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    period_days: int,
) -> Optional[float]:
    """Read the latest SMA value for one security and period."""
    try:
        row = connection.execute(
            """
            SELECT moving_average_value
            FROM moving_average_signals
            WHERE security_id = ?
              AND moving_average_period_days = ?
              AND signal_type = 'SMA'
            ORDER BY signal_date DESC
            LIMIT 1
            """,
            [security_id, period_days],
        ).fetchone()
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read moving averages from DuckDB. Check that the "
            f"moving_average_signals table exists. Details: {error}"
        ) from error

    if row is None:
        return None

    return row[0]


def _update_metric_risk_flag(
    connection: duckdb.DuckDBPyConnection,
    metric_id: int,
    risk_flag: Optional[str],
    risk_reason: Optional[str],
) -> None:
    """Store the risk flag and reason on a dividend metric row."""
    try:
        connection.execute(
            """
            UPDATE dividend_metrics
            SET dividend_risk_flag = ?,
                dividend_risk_reason = ?
            WHERE metric_id = ?
            """,
            [risk_flag, risk_reason, metric_id],
        )
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not save dividend risk flags to DuckDB. Check that the "
            "dividend_metrics table has dividend_risk_flag and "
            f"dividend_risk_reason columns. Details: {error}"
        ) from error
