"""DuckDB schema definitions.

This module creates the local database tables used by market-sentinel. Every
statement uses ``CREATE TABLE IF NOT EXISTS`` so setup can be run safely more
than once.
"""

import duckdb

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS securities (
        security_id INTEGER PRIMARY KEY,
        ticker TEXT NOT NULL UNIQUE,
        name TEXT,
        market TEXT NOT NULL,
        region TEXT,
        currency TEXT,
        sector TEXT,
        industry TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_prices (
        price_id INTEGER PRIMARY KEY,
        security_id INTEGER NOT NULL,
        price_date DATE NOT NULL,
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        close_price DOUBLE NOT NULL,
        adjusted_close_price DOUBLE,
        volume BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (security_id, price_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dividends (
        dividend_id INTEGER PRIMARY KEY,
        security_id INTEGER NOT NULL,
        ex_dividend_date DATE NOT NULL,
        payment_date DATE,
        dividend_amount DOUBLE NOT NULL,
        currency TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (security_id, ex_dividend_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fundamentals (
        fundamental_id INTEGER PRIMARY KEY,
        security_id INTEGER NOT NULL,
        as_of_date DATE NOT NULL,
        market_cap DOUBLE,
        pe_ratio DOUBLE,
        dividend_yield DOUBLE,
        earnings_per_share DOUBLE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (security_id, as_of_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS moving_average_signals (
        signal_id INTEGER PRIMARY KEY,
        security_id INTEGER NOT NULL,
        signal_date DATE NOT NULL,
        short_window_days INTEGER NOT NULL,
        long_window_days INTEGER NOT NULL,
        short_average DOUBLE NOT NULL,
        long_average DOUBLE NOT NULL,
        signal_type TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (security_id, signal_date, short_window_days, long_window_days)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dividend_metrics (
        metric_id INTEGER PRIMARY KEY,
        security_id INTEGER NOT NULL,
        metric_date DATE NOT NULL,
        trailing_annual_dividend DOUBLE,
        dividend_yield DOUBLE,
        total_return DOUBLE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (security_id, metric_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
        alert_id INTEGER PRIMARY KEY,
        security_id INTEGER,
        alert_date DATE NOT NULL,
        alert_type TEXT NOT NULL,
        message TEXT NOT NULL,
        is_resolved BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS report_runs (
        report_run_id INTEGER PRIMARY KEY,
        report_type TEXT NOT NULL,
        output_path TEXT NOT NULL,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        status TEXT NOT NULL
    )
    """,
]


def initialise_database_schema(connection: duckdb.DuckDBPyConnection) -> None:
    """Create all market-sentinel database tables if they do not exist."""
    try:
        for statement in SCHEMA_STATEMENTS:
            connection.execute(statement)
        _ensure_securities_region_column(connection)
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not initialise the DuckDB schema. Check the table definitions "
            "and make sure the database file is writable."
        ) from error


def initialize_database_schema(connection: duckdb.DuckDBPyConnection) -> None:
    """US spelling alias for ``initialise_database_schema``."""
    initialise_database_schema(connection)


def _ensure_securities_region_column(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Add the securities.region column for databases created before it existed."""
    columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name = 'securities'
            """
        ).fetchall()
    }

    if "region" not in columns:
        connection.execute("ALTER TABLE securities ADD COLUMN region TEXT")
