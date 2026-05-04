"""Tests for PDF report generation."""

from datetime import date
from pathlib import Path

from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema
from market_sentinel.reports.pdf_report import generate_pdf_report


def write_settings(config_dir: Path, database_path: Path) -> None:
    """Create a minimal settings file pointing at a test database."""
    config_dir.mkdir(parents=True, exist_ok=True)
    config_dir.joinpath("settings.yaml").write_text(
        f"database_path: {database_path}\n",
        encoding="utf-8",
    )


def open_test_database(tmp_path: Path):
    """Open a temporary DuckDB database with the project schema."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    return connection


def insert_pdf_report_data(connection) -> None:
    """Insert fake rows for PDF report tests."""
    connection.execute(
        """
        INSERT INTO securities (
            security_id,
            ticker,
            name,
            market
        )
        VALUES (?, ?, ?, ?)
        """,
        [1, "AAA", "Example A", "S&P 500"],
    )
    connection.execute(
        """
        INSERT INTO daily_prices (
            price_id,
            security_id,
            price_date,
            close_price
        )
        VALUES (?, ?, ?, ?)
        """,
        [1, 1, "2026-05-04", 100.0],
    )
    connection.execute(
        """
        INSERT INTO moving_average_signals (
            signal_id,
            security_id,
            signal_date,
            moving_average_period_days,
            moving_average_value,
            signal_type
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [1, 1, "2026-05-04", 200, 95.0, "SMA"],
    )
    connection.execute(
        """
        INSERT INTO moving_average_signals (
            signal_id,
            security_id,
            signal_date,
            moving_average_period_days,
            moving_average_value,
            comparison_period_days,
            comparison_moving_average_value,
            signal_type,
            crossover_direction
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            2,
            1,
            "2026-05-04",
            50,
            101.0,
            200,
            95.0,
            "BULLISH_CROSSOVER",
            "BULLISH_CROSSOVER",
        ],
    )
    connection.execute(
        """
        INSERT INTO dividend_metrics (
            metric_id,
            security_id,
            metric_date,
            trailing_annual_dividend,
            dividend_yield,
            annual_dividend_cash_per_10000,
            dividend_risk_flag,
            dividend_risk_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            1,
            1,
            "2026-05-04",
            8.0,
            0.08,
            800.0,
            "DIVIDEND_TRAP_RISK",
            "Dividend yield is above 7%.",
        ],
    )


def test_generate_pdf_report_creates_pdf_file(tmp_path: Path) -> None:
    """PDF report generation should create a dated PDF file."""
    connection = open_test_database(tmp_path)
    output_dir = tmp_path / "outputs" / "pdf"

    try:
        insert_pdf_report_data(connection)
        output_path = generate_pdf_report(
            connection,
            output_dir=output_dir,
            report_date=date(2026, 5, 4),
        )
    finally:
        connection.close()

    assert output_path == output_dir / "market_sentinel_report_2026-05-04.pdf"
    assert output_path.exists()
    assert output_path.read_bytes().startswith(b"%PDF")


def test_generate_pdf_report_creates_output_folder(tmp_path: Path) -> None:
    """PDF report generation should create the output folder automatically."""
    connection = open_test_database(tmp_path)
    output_dir = tmp_path / "new" / "outputs" / "pdf"

    try:
        output_path = generate_pdf_report(
            connection,
            output_dir=output_dir,
            report_date=date(2026, 5, 4),
        )
    finally:
        connection.close()

    assert output_dir.exists()
    assert output_path.exists()
