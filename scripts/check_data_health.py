"""Check whether Market Sentinel data is ready for today's reports.

Run this script from the project root with:

    PYTHONPATH=src python3 scripts/check_data_health.py
"""

import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.analytics.data_health import (  # noqa: E402
    check_data_health,
    print_data_health_summary,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402
from market_sentinel.utils.timing import timed_step  # noqa: E402


def main() -> None:
    """Print a compact data health summary."""
    load_dotenv()
    connection = None

    try:
        with timed_step("Check data health"):
            connection = open_duckdb_connection()
            initialise_database_schema(connection)
            summary = check_data_health(connection)
            print_data_health_summary(summary)
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Data health check failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
