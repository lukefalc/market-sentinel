"""Backfill historical market prices from yfinance.

Run this script from the project root with:

    PYTHONPATH=src python3 scripts/backfill_market_data.py

To backfill one market only:

    PYTHONPATH=src python3 scripts/backfill_market_data.py --market "FTSE 350"
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.config.loader import load_named_config  # noqa: E402
from market_sentinel.data.price_loader import (  # noqa: E402
    DEFAULT_HISTORICAL_BACKFILL_YEARS,
    DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
    DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
    backfill_daily_prices,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402


def main(argv: Optional[list] = None) -> None:
    """Download and store historical prices for securities in DuckDB."""
    args = parse_args(argv)
    connection = None

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
        (
            batch_size,
            backfill_period,
            pause_seconds,
            historical_backfill_years,
        ) = load_backfill_settings()
        summary = backfill_daily_prices(
            connection,
            batch_size=batch_size,
            backfill_period=backfill_period,
            required_history_days=historical_backfill_years * 365,
            pause_seconds=pause_seconds,
            market=args.market,
        )
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Market data backfill failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"Checked {summary['tickers_checked']} tickers")
    if summary.get("market"):
        print(f"Market filter: {summary['market']}")
    print(f"Wrote {summary['price_rows_written']} daily price rows")
    print(
        "Tickers with sufficient history before backfill: "
        f"{summary.get('tickers_with_sufficient_history', 0)}"
    )
    print(f"Tickers backfilled: {summary.get('tickers_backfilled', 0)}")
    print(f"Tickers failed: {len(summary['failed_tickers'])}")

    if summary["failed_tickers"]:
        print("Some tickers could not be backfilled:")
        for ticker, failure in summary["failed_tickers"].items():
            print(f"- {ticker}: {failure['reason']} - {failure['details']}")


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
        historical_backfill_years = int(
            settings.get(
                "historical_backfill_years",
                DEFAULT_HISTORICAL_BACKFILL_YEARS,
            )
        )
        if historical_backfill_years < DEFAULT_HISTORICAL_BACKFILL_YEARS:
            historical_backfill_years = DEFAULT_HISTORICAL_BACKFILL_YEARS
        backfill_period = f"{historical_backfill_years}y"
        pause_seconds = float(
            settings.get(
                "price_download_pause_seconds",
                DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
            )
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            "Market data backfill settings must use a number for "
            "price_download_batch_size, historical_backfill_years, "
            "and price_download_pause_seconds."
        ) from error

    return batch_size, backfill_period, pause_seconds, historical_backfill_years


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    """Parse market data backfill command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--market",
        help='only backfill securities in this market, for example "FTSE 350"',
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    main()
