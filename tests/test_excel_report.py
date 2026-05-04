"""Tests for Excel report generation."""

from datetime import date
from pathlib import Path

from openpyxl import load_workbook

from scripts import generate_excel_report as generate_excel_report_script
from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema
from market_sentinel.reports.excel_report import (
    EXPECTED_WORKSHEET_TITLES,
    generate_excel_report,
)


def write_settings(config_dir: Path, database_path: Path) -> None:
    """Create a minimal settings file pointing at a test database."""
    config_dir.mkdir(parents=True, exist_ok=True)
    config_dir.joinpath("settings.yaml").write_text(
        f"database_path: {database_path}\n",
        encoding="utf-8",
    )


def write_report_settings(
    config_dir: Path,
    database_path: Path,
    excel_dir: str,
    max_rows_per_sheet: int = 50000,
    recent_days: int = 10,
) -> None:
    """Create settings with a custom Excel report output folder."""
    config_dir.mkdir(parents=True, exist_ok=True)
    config_dir.joinpath("settings.yaml").write_text(
        "\n".join(
            [
                f"database_path: {database_path}",
                f"excel_max_rows_per_sheet: {max_rows_per_sheet}",
                f"excel_moving_average_recent_days: {recent_days}",
                "report_outputs:",
                f"  excel_dir: {excel_dir}",
            ]
        ),
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
        [2, "BBB", "Example B", "FTSE 350", "United Kingdom", "GBP", "Energy"],
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
            4,
            2,
            "2026-04-20",
            7,
            9.0,
            30,
            10.0,
            "BEARISH_CROSSOVER",
            "BEARISH_CROSSOVER",
        ],
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
            3,
            2,
            "2026-05-01",
            7,
            9.5,
            30,
            10.0,
            "BEARISH_CROSSOVER",
            "BEARISH_CROSSOVER",
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
            "2026-05-01",
            2.5,
            0.08,
            800.0,
            "DIVIDEND_TRAP_RISK",
            "Dividend yield is above 7%.",
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
            annual_dividend_cash_per_10000
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [2, 2, "2026-05-01", 1.0, 0.03, 300.0],
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
    assert workbook.sheetnames == EXPECTED_WORKSHEET_TITLES
    summary_values = {
        workbook["Summary"][f"A{row}"].value: workbook["Summary"][f"B{row}"].value
        for row in range(1, workbook["Summary"].max_row + 1)
    }

    assert summary_values["Securities"] == 2
    assert summary_values["Securities With Dividend Metrics"] == 2
    assert summary_values["Dividend Risk Flags"] == 1
    assert workbook["Securities"]["A2"].value == "AAA"
    assert workbook["Latest Prices"]["A2"].value == "AAA"
    assert workbook["Moving Averages"]["A2"].value == "AAA"
    assert workbook["Recent Moving Averages"]["A2"].value == "AAA"
    assert workbook["Recent Crossovers"]["A2"].value == "AAA"
    assert workbook["Recent Crossovers"]["D2"].value == "Bullish"
    assert workbook["Recent Crossovers"]["A3"].value == "BBB"
    assert workbook["Recent Crossovers"].max_row == 3
    assert workbook["Crossover Signals"]["D2"].value == "Bullish"
    assert (
        workbook["Crossover Signals"]["E2"].value
        == "7-day trend line crossed above 30-day trend line"
    )
    assert workbook["Crossover Signals"]["F2"].value.date() == date(2026, 5, 1)
    assert workbook["Crossover Signals"]["G2"].value == "2 days ago"
    assert workbook["Crossover Signals"]["A3"].value == "BBB"
    assert workbook["Crossover Signals"]["D3"].value == "Bearish"
    assert (
        workbook["Crossover Signals"]["E3"].value
        == "7-day trend line crossed below 30-day trend line"
    )
    assert workbook["Crossover Signals"]["G4"].value == "13 days ago"
    assert workbook["Dividend Metrics"]["A2"].value == "AAA"
    assert workbook["High Dividend Stocks"]["A2"].value == "AAA"
    assert workbook["Dividend Risk Flags"]["E2"].value == "Dividend yield is above 7%."


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


def test_generate_excel_report_uses_configured_output_folder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Excel reports should use configured output folders and expand ~."""
    monkeypatch.setenv("HOME", str(tmp_path))
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_report_settings(
        config_dir,
        database_path,
        "~/Library/CloudStorage/OneDrive-Personal/Finance/MarketSentinel/Excel",
    )
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)

    try:
        output_path = generate_excel_report(
            connection,
            report_date=date(2026, 5, 3),
            config_dir=config_dir,
        )
    finally:
        connection.close()

    expected_dir = (
        tmp_path
        / "Library"
        / "CloudStorage"
        / "OneDrive-Personal"
        / "Finance"
        / "MarketSentinel"
        / "Excel"
    )

    assert output_path.parent == expected_dir
    assert output_path.exists()


def test_generate_excel_report_handles_missing_dividend_data(
    tmp_path: Path,
) -> None:
    """Dividend worksheets should still exist when there is no dividend data."""
    connection = open_test_database(tmp_path)
    output_dir = tmp_path / "outputs" / "excel"

    try:
        output_path = generate_excel_report(
            connection,
            output_dir=output_dir,
            report_date=date(2026, 5, 3),
        )
    finally:
        connection.close()

    workbook = load_workbook(output_path)

    assert workbook["Dividend Metrics"].max_row == 1
    assert workbook["High Dividend Stocks"].max_row == 1
    assert workbook["Dividend Risk Flags"].max_row == 1


def test_generate_excel_report_limits_large_moving_average_history(
    tmp_path: Path,
) -> None:
    """Large SMA history should be trimmed so Excel row limits are never hit."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    output_dir = tmp_path / "outputs" / "excel"
    write_report_settings(
        config_dir,
        database_path,
        str(output_dir),
        max_rows_per_sheet=5,
        recent_days=30,
    )
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)

    try:
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
            [
                1,
                "AAA",
                "Example A",
                "S&P 500",
                "United States",
                "USD",
                "Technology",
            ],
        )

        for index in range(20):
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
                [
                    index + 1,
                    1,
                    f"2026-05-{index + 1:02d}",
                    7,
                    100.0 + index,
                    "SMA",
                ],
            )

        output_path = generate_excel_report(
            connection,
            report_date=date(2026, 5, 3),
            config_dir=config_dir,
        )
    finally:
        connection.close()

    workbook = load_workbook(output_path)
    summary_text = "\n".join(
        str(workbook["Summary"][f"A{row}"].value)
        for row in range(1, workbook["Summary"].max_row + 1)
    )

    assert workbook["Moving Averages"].max_row == 2
    assert workbook["Moving Averages"]["B2"].value.date() == date(2026, 5, 20)
    assert workbook["Recent Moving Averages"].max_row == 6
    assert "Recent Moving Averages was limited to 5 rows" in summary_text


def test_generate_excel_report_script_uses_updated_report_code(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The script path should produce a workbook with all expected worksheets."""
    connection = open_test_database(tmp_path)
    output_dir = tmp_path / "outputs" / "excel"

    def fake_open_connection():
        return connection

    def fake_generate_report(connection_arg):
        return generate_excel_report(
            connection_arg,
            output_dir=output_dir,
            report_date=date(2026, 5, 3),
        )

    monkeypatch.setattr(
        generate_excel_report_script,
        "open_duckdb_connection",
        fake_open_connection,
    )
    monkeypatch.setattr(
        generate_excel_report_script,
        "generate_excel_report",
        fake_generate_report,
    )

    insert_report_data(connection)
    generate_excel_report_script.main()

    output_path = output_dir / "market_sentinel_report_2026-05-03.xlsx"
    workbook = load_workbook(output_path)

    assert workbook.sheetnames == EXPECTED_WORKSHEET_TITLES
