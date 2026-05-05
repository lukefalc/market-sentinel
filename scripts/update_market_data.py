"""Update daily market prices from yfinance.

Run this script from the project root with:

    python3 scripts/update_market_data.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.config.loader import load_named_config  # noqa: E402
from market_sentinel.data.price_loader import (  # noqa: E402
    DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
    DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
    DEFAULT_PRICE_DAILY_LOOKBACK_DAYS,
    DEFAULT_PRICE_UPDATE_OVERLAP_DAYS,
    update_incremental_daily_prices,
    update_recent_daily_prices,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402


def main() -> None:
    """Download and store daily prices for securities in DuckDB."""
    connection = None

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
        batch_size, lookback_days, pause_seconds, overlap_days = load_market_data_settings()
        summary = update_incremental_daily_prices(
            connection,
            batch_size=batch_size,
            overlap_days=overlap_days,
            pause_seconds=pause_seconds,
        )
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Market data update failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"Checked {summary['tickers_checked']} tickers")
    print(f"Wrote {summary['price_rows_written']} daily price rows")
    print(f"Incremental tickers: {summary.get('incremental_tickers', 0)}")
    print(f"Full-history tickers: {summary.get('full_tickers', 0)}")

    if summary["failed_tickers"]:
        print("Some tickers could not be updated:")
        for ticker, failure in summary["failed_tickers"].items():
            print(f"- {ticker}: {failure['reason']} - {failure['details']}")


def load_market_data_settings():
    """Read market data batching settings from config/settings.yaml."""
    settings = load_named_config("settings")

    try:
        batch_size = int(
            settings.get(
                "price_download_batch_size",
                DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
            )
        )
        lookback_days = int(
            settings.get(
                "price_daily_lookback_days",
                DEFAULT_PRICE_DAILY_LOOKBACK_DAYS,
            )
        )
        pause_seconds = float(
            settings.get(
                "price_download_pause_seconds",
                DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
            )
        )
        overlap_days = int(
            settings.get(
                "price_update_overlap_days",
                DEFAULT_PRICE_UPDATE_OVERLAP_DAYS,
            )
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            "Market data settings must use numbers for "
            "price_download_batch_size, price_daily_lookback_days, and "
            "price_download_pause_seconds, and price_update_overlap_days."
        ) from error

    return batch_size, lookback_days, pause_seconds, overlap_days


if __name__ == "__main__":
    main()
