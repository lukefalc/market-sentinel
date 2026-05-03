"""Calculate latest simple moving averages.

Run this script from the project root with:

    python3 scripts/calculate_moving_averages.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.analytics.moving_averages import (  # noqa: E402
    calculate_and_store_moving_averages,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402


def main() -> None:
    """Calculate and store latest simple moving averages."""
    connection = None

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
        summary = calculate_and_store_moving_averages(connection)
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Moving average calculation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"Checked {summary['tickers_checked']} tickers")
    print(f"Wrote {summary['signals_written']} moving average values")

    if summary["skipped_tickers"]:
        print("Some tickers did not have enough price history:")
        for ticker, message in summary["skipped_tickers"].items():
            print(f"- {ticker}: {message}")


if __name__ == "__main__":
    main()
