"""Calculate dividend risk flags.

Run this script from the project root with:

    python3 scripts/calculate_risk_flags.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.analytics.risk_flags import (  # noqa: E402
    calculate_and_store_risk_flags,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402
from market_sentinel.utils.timing import timed_step  # noqa: E402


def main() -> None:
    """Calculate and store dividend risk flags."""
    connection = None

    try:
        with timed_step("Calculate risk flags"):
            connection = open_duckdb_connection()
            initialise_database_schema(connection)
            summary = calculate_and_store_risk_flags(connection)
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Dividend risk flag calculation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"Checked {summary['metrics_checked']} dividend metric rows")
    print(f"Wrote {summary['risk_flags_written']} risk flags")
    print(f"Cleared {summary['cleared_flags']} risk flags")

    if summary["skipped"]:
        print("Some tickers could not be checked:")
        for ticker, message in summary["skipped"].items():
            print(f"- {ticker}: {message}")


if __name__ == "__main__":
    main()
