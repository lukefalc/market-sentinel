"""Database package.

This package contains DuckDB setup, connection, and repository helpers for
storing market data, analysis results, report metadata, and alert history.
"""

from market_sentinel.database.connection import (
    get_database_path,
    open_duckdb_connection,
)
from market_sentinel.database.schema import initialise_database_schema

__all__ = [
    "get_database_path",
    "initialise_database_schema",
    "open_duckdb_connection",
]
