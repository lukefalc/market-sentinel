"""Generate the market-sentinel Excel report.

Run this script from the project root with:

    python3 scripts/generate_excel_report.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402
from market_sentinel.reports.excel_report import generate_excel_report  # noqa: E402


def main() -> None:
    """Generate an Excel workbook from the local DuckDB database."""
    connection = None

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
        output_path = generate_excel_report(connection)
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Excel report generation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"Excel report saved: {output_path}")


if __name__ == "__main__":
    main()
