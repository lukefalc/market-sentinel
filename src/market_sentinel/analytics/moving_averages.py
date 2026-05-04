"""Simple moving average calculations.

This module reads daily closing prices from DuckDB, calculates dated simple
moving averages for configured periods, and stores those values in the
``moving_average_signals`` table. Crossover detection uses these dated rows.
"""

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import duckdb

from market_sentinel.config.loader import load_named_config
from market_sentinel.data.price_loader import get_active_securities

DEFAULT_PERIODS = [7, 30, 50, 100, 200]
DEFAULT_MOVING_AVERAGE_HISTORY_DAYS = 260


def load_moving_average_periods(config_dir: Optional[Path] = None) -> List[int]:
    """Read moving average periods from ``config/moving_averages.yaml``."""
    config = load_named_config("moving_averages", config_dir)
    moving_average_config = config.get("moving_averages", {})
    periods = moving_average_config.get("periods")

    if periods is None:
        short_window = moving_average_config.get("short_window_days")
        long_window = moving_average_config.get("long_window_days")
        periods = [period for period in [short_window, long_window] if period]

    if not periods:
        periods = DEFAULT_PERIODS

    try:
        clean_periods = sorted({int(period) for period in periods})
    except (TypeError, ValueError) as error:
        raise ValueError(
            "Moving average periods must be whole numbers in "
            "config/moving_averages.yaml."
        ) from error

    if any(period <= 0 for period in clean_periods):
        raise ValueError(
            "Moving average periods must be greater than zero in "
            "config/moving_averages.yaml."
        )

    return clean_periods


def calculate_simple_moving_average(prices: Iterable[float]) -> float:
    """Calculate the arithmetic mean of the supplied prices."""
    price_list = list(prices)

    if not price_list:
        raise ValueError("Cannot calculate a moving average without prices.")

    return sum(price_list) / len(price_list)


def get_closing_prices(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
) -> List[Dict[str, Any]]:
    """Read closing prices for one security, oldest first."""
    try:
        rows = connection.execute(
            """
            SELECT price_date, close_price
            FROM daily_prices
            WHERE security_id = ?
            ORDER BY price_date
            """,
            [security_id],
        ).fetchall()
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read daily prices from DuckDB. Check that the database "
            "is open and the daily_prices table has been created."
        ) from error

    return [{"price_date": row[0], "close_price": row[1]} for row in rows]


def calculate_latest_moving_averages(
    price_rows: List[Dict[str, Any]],
    periods: Iterable[int],
) -> Dict[int, float]:
    """Calculate latest simple moving averages for periods with enough history."""
    averages = {}

    for period in periods:
        if len(price_rows) < period:
            continue

        latest_prices = [row["close_price"] for row in price_rows[-period:]]
        averages[period] = calculate_simple_moving_average(latest_prices)

    return averages


def calculate_historical_moving_averages(
    price_rows: List[Dict[str, Any]],
    periods: Iterable[int],
    history_days: int = DEFAULT_MOVING_AVERAGE_HISTORY_DAYS,
) -> List[Dict[str, Any]]:
    """Calculate dated simple moving averages for all available windows."""
    signals = []
    first_stored_index = _first_stored_price_index(price_rows, history_days)

    for period in periods:
        if len(price_rows) < period:
            continue

        rolling_sum = sum(row["close_price"] for row in price_rows[:period])

        for end_index in range(period - 1, len(price_rows)):
            if end_index > period - 1:
                rolling_sum += price_rows[end_index]["close_price"]
                rolling_sum -= price_rows[end_index - period]["close_price"]

            if end_index < first_stored_index:
                continue

            average = rolling_sum / period
            signals.append(
                {
                    "signal_date": price_rows[end_index]["price_date"],
                    "period": period,
                    "average": average,
                }
            )

    return signals


def upsert_moving_average_signal(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    signal_date: Any,
    period: int,
    average: float,
) -> None:
    """Insert or update one latest moving average value."""
    try:
        columns = _get_table_columns(connection, "moving_average_signals")
        existing_id = connection.execute(
            _existing_signal_query(columns),
            _existing_signal_values(security_id, signal_date, period, columns),
        ).fetchone()

        if existing_id:
            connection.execute(
                _update_signal_query(columns),
                _update_signal_values(period, average, existing_id[0], columns),
            )
        else:
            next_id = connection.execute(
                """
                SELECT COALESCE(MAX(signal_id), 0) + 1
                FROM moving_average_signals
                """
            ).fetchone()[0]
            insert_sql = (
                "INSERT INTO moving_average_signals ("
                + _insert_signal_columns(columns)
                + ") VALUES ("
                + _insert_signal_placeholders(columns)
                + ")"
            )
            connection.execute(
                insert_sql,
                _insert_signal_values(
                    next_id,
                    security_id,
                    signal_date,
                    period,
                    average,
                    columns,
                ),
            )
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not save moving averages to DuckDB. Check that the database "
            "is open and the moving_average_signals table has been created. "
            f"Details: {error}"
        ) from error


def calculate_and_store_moving_averages(
    connection: duckdb.DuckDBPyConnection,
    config_dir: Optional[Path] = None,
    history_days: int = DEFAULT_MOVING_AVERAGE_HISTORY_DAYS,
) -> Dict[str, Any]:
    """Calculate and store dated simple moving averages for active securities."""
    if history_days <= 0:
        raise ValueError("moving_average_history_days must be greater than zero.")

    periods = load_moving_average_periods(config_dir)
    securities = get_active_securities(connection)
    summary = {
        "tickers_checked": len(securities),
        "signals_written": 0,
        "skipped_tickers": {},
    }

    print(f"Total tickers for moving averages: {len(securities)}")

    for ticker_number, security in enumerate(securities, start=1):
        ticker = security["ticker"]
        print(
            f"Processing ticker {ticker_number} of {len(securities)}: {ticker}"
        )

        price_rows = get_closing_prices(connection, security["security_id"])
        moving_average_rows = calculate_historical_moving_averages(
            price_rows,
            periods,
            history_days=history_days,
        )

        if not moving_average_rows:
            summary["skipped_tickers"][ticker] = (
                "Not enough daily price history for the configured moving "
                "average periods."
            )
            print(f"Rows written for {ticker}: 0")
            continue

        rows_written_for_ticker = 0
        for row in moving_average_rows:
            upsert_moving_average_signal(
                connection,
                security["security_id"],
                row["signal_date"],
                row["period"],
                row["average"],
            )
            summary["signals_written"] += 1
            rows_written_for_ticker += 1

        print(f"Rows written for {ticker}: {rows_written_for_ticker}")

    print("Moving average calculation summary")
    print(f"Total tickers checked: {summary['tickers_checked']}")
    print(f"Total moving average rows written: {summary['signals_written']}")
    skipped_count = len(summary["skipped_tickers"])
    print(f"Tickers skipped for insufficient history: {skipped_count}")

    return summary


def _first_stored_price_index(
    price_rows: List[Dict[str, Any]],
    history_days: int,
) -> int:
    """Return the first price index whose SMA rows should be stored."""
    if not price_rows:
        return 0

    return max(0, len(price_rows) - history_days)


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


def _existing_signal_query(columns: set) -> str:
    """Build the lookup query for the current moving average table shape."""
    if "short_window_days" in columns and "long_window_days" in columns:
        return """
            SELECT signal_id
            FROM moving_average_signals
            WHERE security_id = ?
              AND signal_date = ?
              AND signal_type = ?
              AND (
                  moving_average_period_days = ?
                  OR (
                      short_window_days = ?
                      AND long_window_days = ?
                  )
              )
            """

    return """
        SELECT signal_id
        FROM moving_average_signals
        WHERE security_id = ?
          AND signal_date = ?
          AND signal_type = ?
          AND moving_average_period_days = ?
        """


def _existing_signal_values(
    security_id: int,
    signal_date: Any,
    period: int,
    columns: set,
) -> List[Any]:
    """Return lookup values for the current moving average table shape."""
    values = [security_id, signal_date, "SMA", period]

    if "short_window_days" in columns and "long_window_days" in columns:
        values.extend([period, period])

    return values


def _insert_signal_columns(columns: set) -> str:
    """Return INSERT columns for the current moving average table shape."""
    insert_columns = [
        "signal_id",
        "security_id",
        "signal_date",
        "moving_average_period_days",
        "moving_average_value",
        "signal_type",
    ]

    if "short_window_days" in columns:
        insert_columns.append("short_window_days")
    if "long_window_days" in columns:
        insert_columns.append("long_window_days")
    if "short_average" in columns:
        insert_columns.append("short_average")
    if "long_average" in columns:
        insert_columns.append("long_average")

    return ",\n                    ".join(insert_columns)


def _update_signal_query(columns: set) -> str:
    """Build the update query for the current moving average table shape."""
    update_columns = [
        "moving_average_period_days = ?",
        "moving_average_value = ?",
        "signal_type = ?",
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


def _update_signal_values(
    period: int,
    average: float,
    signal_id: int,
    columns: set,
) -> List[Any]:
    """Return update values for the current moving average table shape."""
    values = [period, average, "SMA"]

    if "short_window_days" in columns:
        values.append(period)
    if "long_window_days" in columns:
        values.append(period)
    if "short_average" in columns:
        values.append(average)
    if "long_average" in columns:
        values.append(average)

    values.append(signal_id)
    return values


def _insert_signal_placeholders(columns: set) -> str:
    """Return INSERT placeholders for the current moving average table shape."""
    values = _insert_signal_values(
        signal_id=0,
        security_id=0,
        signal_date=0,
        period=0,
        average=0.0,
        columns=columns,
    )
    return ", ".join(["?"] * len(values))


def _insert_signal_values(
    signal_id: int,
    security_id: int,
    signal_date: Any,
    period: int,
    average: float,
    columns: set,
) -> List[Any]:
    """Return INSERT values for the current moving average table shape."""
    values = [signal_id, security_id, signal_date, period, average, "SMA"]

    if "short_window_days" in columns:
        values.append(period)
    if "long_window_days" in columns:
        values.append(period)
    if "short_average" in columns:
        values.append(average)
    if "long_average" in columns:
        values.append(average)

    return values
