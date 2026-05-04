"""Backfill historical market prices from yfinance.

Run this script from the project root with:

    PYTHONPATH=src python3 scripts/backfill_market_data.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.config.loader import load_named_config  # noqa: E402
from market_sentinel.data.price_loader import (  # noqa: E402
    DEFAULT_PRICE_BACKFILL_PERIOD,
    DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
    DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
    backfill_daily_prices,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402


def main() -> None:
    """Download and store historical prices for securities in DuckDB."""
    connection = None

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
        batch_size, backfill_period, pause_seconds = load_backfill_settings()
        summary = backfill_daily_prices(
            connection,
            batch_size=batch_size,
            backfill_period=backfill_period,
            pause_seconds=pause_seconds,
        )
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Market data backfill failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"Checked {summary['tickers_checked']} tickers")
    print(f"Wrote {summary['price_rows_written']} daily price rows")

    if summary["failed_tickers"]:
        print("Some tickers could not be backfilled:")
        for ticker, message in summary["failed_tickers"].items():
            print(f"- {ticker}: {message}")


def load_backfill_settings():
    """Read market data backfill settings from config/settings.yaml."""
    settings = load_named_config("settings")

    try:
        batch_size = int(
            settings.get(
                "price_download_batch_size",
                DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
            )
        )
        backfill_period = str(
            settings.get("price_backfill_period", DEFAULT_PRICE_BACKFILL_PERIOD)
        )
        pause_seconds = float(
            settings.get(
                "price_download_pause_seconds",
                DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
            )
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            "Market data backfill settings must use a number for "
            "price_download_batch_size, text like 5y for price_backfill_period, "
            "and a number for price_download_pause_seconds."
        ) from error

    return batch_size, backfill_period, pause_seconds


if __name__ == "__main__":
    main()
