"""Calculate dated simple moving averages.

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
    DEFAULT_MOVING_AVERAGE_HISTORY_DAYS,
    calculate_and_store_moving_averages,
)
from market_sentinel.config.loader import load_named_config  # noqa: E402
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402


def main() -> None:
    """Calculate and store dated simple moving averages."""
    connection = None

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
        history_days = load_moving_average_history_days()
        summary = calculate_and_store_moving_averages(
            connection,
            history_days=history_days,
        )
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Moving average calculation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"Checked {summary['tickers_checked']} tickers")
    print(f"Wrote {summary['signals_written']} moving average values")

    if summary["skipped_tickers"]:
        skipped_count = len(summary["skipped_tickers"])
        print(
            "Some tickers did not have enough price history: "
            f"{skipped_count} skipped."
        )


def load_moving_average_history_days() -> int:
    """Read moving average storage history from config/settings.yaml."""
    settings = load_named_config("settings")

    try:
        history_days = int(
            settings.get(
                "moving_average_history_days",
                DEFAULT_MOVING_AVERAGE_HISTORY_DAYS,
            )
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            "moving_average_history_days must be a whole number in "
            "config/settings.yaml."
        ) from error

    return history_days


if __name__ == "__main__":
    main()
