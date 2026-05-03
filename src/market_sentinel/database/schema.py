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
        moving_average_period_days INTEGER NOT NULL,
        moving_average_value DOUBLE NOT NULL,
        comparison_period_days INTEGER,
        comparison_moving_average_value DOUBLE,
        signal_type TEXT NOT NULL,
        crossover_direction TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (
            security_id,
            signal_date,
            moving_average_period_days,
            comparison_period_days,
            signal_type
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dividend_metrics (
        metric_id INTEGER PRIMARY KEY,
        security_id INTEGER NOT NULL,
        metric_date DATE NOT NULL,
        trailing_annual_dividend DOUBLE,
        dividend_yield DOUBLE,
        annual_dividend_cash_per_10000 DOUBLE,
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
        _ensure_moving_average_signal_columns(connection)
        _ensure_dividend_metric_columns(connection)
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


def _ensure_moving_average_signal_columns(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Add SMA columns for databases created before the schema was simplified."""
    columns = _get_table_columns(connection, "moving_average_signals")

    if "moving_average_period_days" not in columns:
        connection.execute(
            """
            ALTER TABLE moving_average_signals
            ADD COLUMN moving_average_period_days INTEGER
            """
        )

    if "moving_average_value" not in columns:
        connection.execute(
            """
            ALTER TABLE moving_average_signals
            ADD COLUMN moving_average_value DOUBLE
            """
        )

    if "comparison_period_days" not in columns:
        connection.execute(
            """
            ALTER TABLE moving_average_signals
            ADD COLUMN comparison_period_days INTEGER
            """
        )

    if "comparison_moving_average_value" not in columns:
        connection.execute(
            """
            ALTER TABLE moving_average_signals
            ADD COLUMN comparison_moving_average_value DOUBLE
            """
        )

    if "crossover_direction" not in columns:
        connection.execute(
            """
            ALTER TABLE moving_average_signals
            ADD COLUMN crossover_direction TEXT
            """
        )


def _get_table_columns(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> set:
    """Return column names for a DuckDB table."""
    return {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchall()
    }


def _ensure_dividend_metric_columns(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Add dividend metric columns for databases created before they existed."""
    columns = _get_table_columns(connection, "dividend_metrics")

    if "annual_dividend_cash_per_10000" not in columns:
        connection.execute(
            """
            ALTER TABLE dividend_metrics
            ADD COLUMN annual_dividend_cash_per_10000 DOUBLE
            """
        )
