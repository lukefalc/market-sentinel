"""Tests for Excel report generation."""

from datetime import date
from pathlib import Path

from openpyxl import load_workbook

from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema
from market_sentinel.reports.excel_report import generate_excel_report


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


def insert_report_data(connection) -> None:
    """Insert fake database rows for the Excel report."""
    connection.execute(
        """
        INSERT INTO securities (
            security_id,
            ticker,
            name,
            market,
            region,
            currency,
            sector
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [1, "AAA", "Example A", "S&P 500", "United States", "USD", "Technology"],
    )
    connection.execute(
        """
        INSERT INTO daily_prices (
            price_id,
            security_id,
            price_date,
            open_price,
            high_price,
            low_price,
            close_price,
            adjusted_close_price,
            volume
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [1, 1, "2026-05-01", 10.0, 12.0, 9.0, 11.0, 10.8, 1000],
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
        [1, 1, "2026-05-01", 7, 10.5, "SMA"],
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
            "2026-05-01",
            7,
            10.5,
            30,
            10.0,
            "BULLISH_CROSSOVER",
            "BULLISH_CROSSOVER",
        ],
    )


def test_generate_excel_report_creates_expected_workbook(tmp_path: Path) -> None:
    """Excel report generation should create the expected workbook tabs."""
    connection = open_test_database(tmp_path)
    output_dir = tmp_path / "outputs" / "excel"

    try:
        insert_report_data(connection)
        output_path = generate_excel_report(
            connection,
            output_dir=output_dir,
            report_date=date(2026, 5, 3),
        )
    finally:
        connection.close()

    workbook = load_workbook(output_path)

    assert output_path == output_dir / "market_sentinel_report_2026-05-03.xlsx"
    assert output_path.exists()
    assert workbook.sheetnames == [
        "Summary",
        "Securities",
        "Latest Prices",
        "Moving Averages",
        "Crossover Signals",
    ]
    assert workbook["Securities"]["A2"].value == "AAA"
    assert workbook["Latest Prices"]["A2"].value == "AAA"
    assert workbook["Moving Averages"]["A2"].value == "AAA"
    assert workbook["Crossover Signals"]["G2"].value == "BULLISH_CROSSOVER"


def test_generate_excel_report_creates_output_folder(tmp_path: Path) -> None:
    """Excel report generation should create the output folder automatically."""
    connection = open_test_database(tmp_path)
    output_dir = tmp_path / "new" / "outputs" / "excel"

    try:
        output_path = generate_excel_report(
            connection,
            output_dir=output_dir,
            report_date=date(2026, 5, 3),
        )
    finally:
        connection.close()

    assert output_dir.exists()
    assert output_path.exists()
