"""Initialise the local DuckDB database for market-sentinel.

Run this script from the project root with:

    python3 scripts/init_database.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.database.connection import (  # noqa: E402
    get_database_path,
    open_duckdb_connection,
)
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402


def main() -> None:
    """Create the local DuckDB database and required tables."""
    connection = None

    try:
        database_path = get_database_path()
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
    except (FileNotFoundError, RuntimeError, ValueError) as error:
        print(f"Database setup failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print(f"DuckDB database is ready: {database_path}")


if __name__ == "__main__":
    main()
