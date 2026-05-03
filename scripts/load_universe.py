"""Load local stock universe CSV files into DuckDB.

Run this script from the project root with:

    python3 scripts/load_universe.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.data.universe_loader import (  # noqa: E402
    default_universe_files,
    load_universe_files,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402


def main() -> None:
    """Load the configured local universe CSV files into DuckDB."""
    connection = None

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
        loaded_counts = load_universe_files(connection, default_universe_files())
    except (FileNotFoundError, RuntimeError, ValueError) as error:
        print(f"Universe loading failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    for file_name, row_count in loaded_counts.items():
        print(f"Loaded {row_count} securities from {file_name}")


if __name__ == "__main__":
    main()
