"""Calculate dividend metrics.

Run this script from the project root with:

    python3 scripts/calculate_dividends.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.analytics.dividends import (  # noqa: E402
    DEFAULT_DIVIDEND_DOWNLOAD_BATCH_SIZE,
    DEFAULT_DIVIDEND_DOWNLOAD_PAUSE_SECONDS,
    DEFAULT_DIVIDEND_RETRY_BATCH_SIZE,
    calculate_and_store_dividends,
)
from market_sentinel.config.loader import load_named_config  # noqa: E402
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402
from market_sentinel.utils.timing import timed_step  # noqa: E402


def main() -> None:
    """Fetch dividends and calculate dividend metrics."""
    connection = None

    try:
        with timed_step("Calculate dividends"):
            connection = open_duckdb_connection()
            initialise_database_schema(connection)
            batch_size, pause_seconds, retry_batch_size = load_dividend_settings()
            summary = calculate_and_store_dividends(
                connection,
                batch_size=batch_size,
                pause_seconds=pause_seconds,
                retry_batch_size=retry_batch_size,
            )
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Dividend calculation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"Checked {summary['tickers_checked']} tickers")
    print(f"Wrote {summary['dividend_rows_written']} dividend rows")
    print(f"Wrote {summary['metrics_written']} dividend metric rows")

    if summary["no_dividend_history"]:
        print("No dividend history found for:")
        for ticker in summary["no_dividend_history"]:
            print(f"- {ticker}")

    if summary["failed_tickers"]:
        print("Some tickers could not be processed:")
        for ticker, failure in summary["failed_tickers"].items():
            print(f"- {ticker}: {failure['reason']} - {failure['details']}")


def load_dividend_settings():
    """Read dividend download settings from config/settings.yaml."""
    settings = load_named_config("settings")

    try:
        batch_size = int(
            settings.get(
                "dividend_download_batch_size",
                DEFAULT_DIVIDEND_DOWNLOAD_BATCH_SIZE,
            )
        )
        pause_seconds = float(
            settings.get(
                "dividend_download_pause_seconds",
                DEFAULT_DIVIDEND_DOWNLOAD_PAUSE_SECONDS,
            )
        )
        retry_batch_size = int(
            settings.get(
                "dividend_retry_batch_size",
                DEFAULT_DIVIDEND_RETRY_BATCH_SIZE,
            )
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            "Dividend settings must use numbers for "
            "dividend_download_batch_size, dividend_download_pause_seconds, "
            "and dividend_retry_batch_size."
        ) from error

    return batch_size, pause_seconds, retry_batch_size


if __name__ == "__main__":
    main()
