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
    DEFAULT_BATCH_PAUSE_SECONDS,
    DEFAULT_BATCH_SIZE,
    update_daily_prices,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402


def main() -> None:
    """Download and store daily prices for securities in DuckDB."""
    connection = None

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
        batch_size, pause_seconds = load_market_data_settings()
        summary = update_daily_prices(
            connection,
            batch_size=batch_size,
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

    if summary["failed_tickers"]:
        print("Some tickers could not be updated:")
        for ticker, message in summary["failed_tickers"].items():
            print(f"- {ticker}: {message}")


def load_market_data_settings():
    """Read market data batching settings from config/settings.yaml."""
    settings = load_named_config("settings")
    market_data_settings = settings.get("market_data", {})

    try:
        batch_size = int(
            market_data_settings.get("batch_size", DEFAULT_BATCH_SIZE)
        )
        pause_seconds = float(
            market_data_settings.get(
                "pause_seconds_between_batches",
                DEFAULT_BATCH_PAUSE_SECONDS,
            )
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            "Market data settings must use numbers for batch_size and "
            "pause_seconds_between_batches."
        ) from error

    return batch_size, pause_seconds


if __name__ == "__main__":
    main()
