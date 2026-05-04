"""Moving average crossover detection.

This module compares short and long simple moving averages for each active
ticker. It stores detected bullish and bearish crossover events in the
``moving_average_signals`` table without generating reports.
"""

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duckdb

from market_sentinel.config.loader import load_named_config
from market_sentinel.data.price_loader import get_active_securities

DEFAULT_CROSSOVER_PAIRS = [(50, 200)]
DEFAULT_CROSSOVER_RECENT_DAYS = 7


def load_crossover_pairs(config_dir: Optional[Path] = None) -> List[Tuple[int, int]]:
    """Read crossover pairs from ``config/moving_averages.yaml``."""
    config = load_named_config("moving_averages", config_dir)
    moving_average_config = config.get("moving_averages", {})
    configured_pairs = moving_average_config.get("crossover_pairs")

    if not configured_pairs:
        return DEFAULT_CROSSOVER_PAIRS

    pairs = []
    try:
        for pair in configured_pairs:
            short_period = int(pair["short_period_days"])
            long_period = int(pair["long_period_days"])

            if short_period <= 0 or long_period <= 0:
                raise ValueError
            if short_period >= long_period:
                raise ValueError

            pairs.append((short_period, long_period))
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(
            "Crossover pairs in config/moving_averages.yaml must use positive "
            "whole numbers with short_period_days less than long_period_days."
        ) from error

    return pairs


def load_crossover_recent_days(config_dir: Optional[Path] = None) -> int:
    """Read the recent crossover scan window from ``config/settings.yaml``."""
    try:
        settings = load_named_config("settings", config_dir)
    except FileNotFoundError:
        return DEFAULT_CROSSOVER_RECENT_DAYS

    raw_recent_days = settings.get(
        "crossover_recent_days",
        DEFAULT_CROSSOVER_RECENT_DAYS,
    )

    try:
        recent_days = int(raw_recent_days)
    except (TypeError, ValueError):
        return DEFAULT_CROSSOVER_RECENT_DAYS

    if recent_days < 1:
        return DEFAULT_CROSSOVER_RECENT_DAYS

    return recent_days


def detect_crossover(
    previous_short: float,
    previous_long: float,
    latest_short: float,
    latest_long: float,
) -> Optional[str]:
    """Return the crossover direction, or None if no crossover happened."""
    if previous_short <= previous_long and latest_short > latest_long:
        return "BULLISH_CROSSOVER"

    if previous_short >= previous_long and latest_short < latest_long:
        return "BEARISH_CROSSOVER"

    return None


def describe_crossover(
    short_period: int,
    long_period: int,
    crossover_direction: str,
) -> str:
    """Return a beginner-friendly crossover description."""
    if crossover_direction == "BULLISH_CROSSOVER":
        action = "crossed above"
    elif crossover_direction == "BEARISH_CROSSOVER":
        action = "crossed below"
    else:
        action = "crossed"

    return (
        f"{short_period}-day trend line {action} "
        f"{long_period}-day trend line"
    )


def days_since_crossover(crossover_date: Any, report_date: Any) -> int:
    """Return whole days between a crossover date and the report date."""
    crossover_day = _to_date(crossover_date)
    report_day = _to_date(report_date)
    return max((report_day - crossover_day).days, 0)


def format_days_since_crossover(crossover_date: Any, report_date: Any) -> str:
    """Return a friendly age label for a crossover signal."""
    days_since = days_since_crossover(crossover_date, report_date)

    if days_since == 0:
        return "Today"

    if days_since == 1:
        return "1 day ago"

    return f"{days_since} days ago"


def is_recent_crossover(
    crossover_date: Any,
    report_date: Any,
    recent_days: int = DEFAULT_CROSSOVER_RECENT_DAYS,
) -> bool:
    """Return True when a crossover happened within the recent report window."""
    return days_since_crossover(crossover_date, report_date) <= recent_days


def get_latest_sma_pair_values(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    short_period: int,
    long_period: int,
) -> List[Dict[str, Any]]:
    """Read the previous and latest dates where both SMA values are available."""
    try:
        rows = connection.execute(
            """
            SELECT
                short_sma.signal_date,
                short_sma.moving_average_value,
                long_sma.moving_average_value
            FROM moving_average_signals AS short_sma
            INNER JOIN moving_average_signals AS long_sma
                ON short_sma.security_id = long_sma.security_id
               AND short_sma.signal_date = long_sma.signal_date
            WHERE short_sma.security_id = ?
              AND short_sma.signal_type = 'SMA'
              AND long_sma.signal_type = 'SMA'
              AND short_sma.moving_average_period_days = ?
              AND long_sma.moving_average_period_days = ?
              AND short_sma.moving_average_value IS NOT NULL
              AND long_sma.moving_average_value IS NOT NULL
            ORDER BY short_sma.signal_date DESC
            LIMIT 2
            """,
            [security_id, short_period, long_period],
        ).fetchall()
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read moving average values from DuckDB. Check that the "
            "database is open and the moving_average_signals table exists. "
            f"Details: {error}"
        ) from error

    return [
        {
            "signal_date": row[0],
            "short_value": row[1],
            "long_value": row[2],
        }
        for row in reversed(rows)
    ]


def get_sma_pair_history(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    short_period: int,
    long_period: int,
) -> List[Dict[str, Any]]:
    """Read all dates where both short and long SMA values are available."""
    try:
        rows = connection.execute(
            """
            SELECT
                short_sma.signal_date,
                short_sma.moving_average_value,
                long_sma.moving_average_value
            FROM moving_average_signals AS short_sma
            INNER JOIN moving_average_signals AS long_sma
                ON short_sma.security_id = long_sma.security_id
               AND short_sma.signal_date = long_sma.signal_date
            WHERE short_sma.security_id = ?
              AND short_sma.signal_type = 'SMA'
              AND long_sma.signal_type = 'SMA'
              AND short_sma.moving_average_period_days = ?
              AND long_sma.moving_average_period_days = ?
              AND short_sma.moving_average_value IS NOT NULL
              AND long_sma.moving_average_value IS NOT NULL
            ORDER BY short_sma.signal_date
            """,
            [security_id, short_period, long_period],
        ).fetchall()
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read moving average history from DuckDB. Check that the "
            "database is open and the moving_average_signals table exists. "
            f"Details: {error}"
        ) from error

    return [
        {
            "signal_date": row[0],
            "short_value": row[1],
            "long_value": row[2],
        }
        for row in rows
    ]


def upsert_crossover_signal(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    signal_date: Any,
    short_period: int,
    long_period: int,
    short_value: float,
    long_value: float,
    crossover_type: str,
) -> None:
    """Insert or update one crossover signal."""
    try:
        columns = _get_table_columns(connection, "moving_average_signals")
        existing_id = connection.execute(
            """
            SELECT signal_id
            FROM moving_average_signals
            WHERE security_id = ?
              AND signal_date = ?
              AND moving_average_period_days = ?
              AND comparison_period_days = ?
              AND signal_type = ?
            """,
            [security_id, signal_date, short_period, long_period, crossover_type],
        ).fetchone()

        if existing_id:
            connection.execute(
                _update_crossover_query(columns),
                _update_crossover_values(
                    short_period,
                    long_period,
                    short_value,
                    long_value,
                    crossover_type,
                    existing_id[0],
                    columns,
                ),
            )
            return

        next_id = connection.execute(
            "SELECT COALESCE(MAX(signal_id), 0) + 1 FROM moving_average_signals"
        ).fetchone()[0]
        insert_sql = (
            "INSERT INTO moving_average_signals ("
            + _insert_crossover_columns(columns)
            + ") VALUES ("
            + _insert_crossover_placeholders(columns)
            + ")"
        )
        connection.execute(
            insert_sql,
            _insert_crossover_values(
                next_id,
                security_id,
                signal_date,
                short_period,
                long_period,
                short_value,
                long_value,
                crossover_type,
                columns,
            )
        )
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not save crossover signals to DuckDB. Check that the "
            "moving_average_signals table has the latest schema. "
            f"Details: {error}"
        ) from error


def detect_and_store_crossovers(
    connection: duckdb.DuckDBPyConnection,
    config_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Detect and store moving average crossovers for active securities."""
    pairs = load_crossover_pairs(config_dir)
    recent_days = load_crossover_recent_days(config_dir)
    securities = get_active_securities(connection)
    latest_sma_date = get_latest_sma_date(connection)
    summary = {
        "tickers_checked": len(securities),
        "crossovers_written": 0,
        "skipped": {},
    }

    if latest_sma_date is None:
        return summary

    cutoff_date = latest_sma_date - timedelta(days=recent_days)

    for security in securities:
        ticker = security["ticker"]

        for short_period, long_period in pairs:
            pair_key = f"{ticker}:{short_period}/{long_period}"
            values = get_sma_pair_history(
                connection,
                security["security_id"],
                short_period,
                long_period,
            )

            if len(values) < 2:
                summary["skipped"][pair_key] = (
                    "Not enough matching SMA values to compare previous and "
                    "latest signal dates."
                )
                continue

            for previous, current in zip(values, values[1:]):
                current_date = _to_date(current["signal_date"])

                if current_date < cutoff_date:
                    continue

                crossover_type = detect_crossover(
                    previous["short_value"],
                    previous["long_value"],
                    current["short_value"],
                    current["long_value"],
                )

                if crossover_type is None:
                    continue

                upsert_crossover_signal(
                    connection,
                    security["security_id"],
                    current["signal_date"],
                    short_period,
                    long_period,
                    current["short_value"],
                    current["long_value"],
                    crossover_type,
                )
                summary["crossovers_written"] += 1

    return summary


def get_latest_sma_date(connection: duckdb.DuckDBPyConnection) -> Optional[date]:
    """Return the latest date with stored SMA values."""
    try:
        latest_value = connection.execute(
            """
            SELECT MAX(signal_date)
            FROM moving_average_signals
            WHERE signal_type = 'SMA'
            """
        ).fetchone()[0]
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not find the latest SMA date in DuckDB. Check that the "
            "database is open and the moving_average_signals table exists. "
            f"Details: {error}"
        ) from error

    if latest_value is None:
        return None

    return _to_date(latest_value)


def _to_date(value: Any) -> date:
    """Convert date-like values to ``date`` objects."""
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    return date.fromisoformat(str(value)[:10])


def _get_table_columns(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> set:
    """Return column names for a DuckDB table."""
    return {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchall()
    }


def _insert_crossover_columns(columns: set) -> str:
    """Return INSERT columns for the current signal table shape."""
    insert_columns = [
        "signal_id",
        "security_id",
        "signal_date",
        "moving_average_period_days",
        "moving_average_value",
        "comparison_period_days",
        "comparison_moving_average_value",
        "signal_type",
        "crossover_direction",
    ]

    if "short_window_days" in columns:
        insert_columns.append("short_window_days")
    if "long_window_days" in columns:
        insert_columns.append("long_window_days")
    if "short_average" in columns:
        insert_columns.append("short_average")
    if "long_average" in columns:
        insert_columns.append("long_average")

    return ", ".join(insert_columns)


def _insert_crossover_placeholders(columns: set) -> str:
    """Return INSERT placeholders for crossover values."""
    values = _insert_crossover_values(0, 0, 0, 0, 0, 0.0, 0.0, "", columns)
    return ", ".join(["?"] * len(values))


def _insert_crossover_values(
    signal_id: int,
    security_id: int,
    signal_date: Any,
    short_period: int,
    long_period: int,
    short_value: float,
    long_value: float,
    crossover_type: str,
    columns: set,
) -> List[Any]:
    """Return INSERT values for crossover rows."""
    values = [
        signal_id,
        security_id,
        signal_date,
        short_period,
        short_value,
        long_period,
        long_value,
        crossover_type,
        crossover_type,
    ]

    if "short_window_days" in columns:
        values.append(short_period)
    if "long_window_days" in columns:
        values.append(long_period)
    if "short_average" in columns:
        values.append(short_value)
    if "long_average" in columns:
        values.append(long_value)

    return values


def _update_crossover_query(columns: set) -> str:
    """Build an update query for crossover rows."""
    update_columns = [
        "moving_average_value = ?",
        "comparison_moving_average_value = ?",
        "crossover_direction = ?",
    ]

    if "short_window_days" in columns:
        update_columns.append("short_window_days = ?")
    if "long_window_days" in columns:
        update_columns.append("long_window_days = ?")
    if "short_average" in columns:
        update_columns.append("short_average = ?")
    if "long_average" in columns:
        update_columns.append("long_average = ?")

    return (
        "UPDATE moving_average_signals SET "
        + ", ".join(update_columns)
        + " WHERE signal_id = ?"
    )


def _update_crossover_values(
    short_period: int,
    long_period: int,
    short_value: float,
    long_value: float,
    crossover_type: str,
    signal_id: int,
    columns: set,
) -> List[Any]:
    """Return update values for crossover rows."""
    values = [short_value, long_value, crossover_type]

    if "short_window_days" in columns:
        values.append(short_period)
    if "long_window_days" in columns:
        values.append(long_period)
    if "short_average" in columns:
        values.append(short_value)
    if "long_average" in columns:
        values.append(long_value)

    values.append(signal_id)
    return values
