"""Detect moving average crossovers.

Run this script from the project root with:

    python3 scripts/detect_crossovers.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.analytics.crossovers import (  # noqa: E402
    detect_and_store_crossovers,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402


def main() -> None:
    """Detect and store moving average crossover signals."""
    connection = None

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
        summary = detect_and_store_crossovers(connection)
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Crossover detection failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"Checked {summary['tickers_checked']} tickers")
    print(f"Wrote {summary['crossovers_written']} crossover signals")

    if summary["skipped"]:
        skipped_count = len(summary["skipped"])
        print(
            "Some ticker pairs did not have enough moving average history: "
            f"{skipped_count} skipped."
        )


if __name__ == "__main__":
    main()
