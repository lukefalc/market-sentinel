"""Generate market-sentinel chart images.

Run this script from the project root with:

    python3 scripts/generate_charts.py
    python3 scripts/generate_charts.py --force
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402
from market_sentinel.reports.charts import generate_charts  # noqa: E402
from market_sentinel.utils.timing import timed_step  # noqa: E402


def main(argv: Optional[list] = None) -> None:
    """Generate chart images from the local DuckDB database."""
    args = parse_args(argv)
    connection = None

    try:
        with timed_step("Generate charts"):
            connection = open_duckdb_connection()
            initialise_database_schema(connection)
            summary = generate_charts(connection, force=args.force)
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Chart generation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"Charts created: {summary['charts_created']}")

    if summary["skipped"]:
        print("Some tickers were skipped:")
        for ticker, reason in summary["skipped"].items():
            print(f"- {ticker}: {reason}")


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate chart PNG files even when cached images look current.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    main()
