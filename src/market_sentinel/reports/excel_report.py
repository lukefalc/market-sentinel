"""Excel report generation for market-sentinel.

This module reads summary data from DuckDB and writes a beginner-friendly Excel
workbook using openpyxl. It does not create PDF reports.
"""

from datetime import date
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

import duckdb
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

from market_sentinel.config.loader import load_named_config

DEFAULT_OUTPUT_DIR = Path("outputs") / "excel"
EXPECTED_WORKSHEET_TITLES = [
    "Summary",
    "Securities",
    "Latest Prices",
    "Moving Averages",
    "Crossover Signals",
    "Dividend Metrics",
    "High Dividend Stocks",
    "Dividend Risk Flags",
]

HEADER_FILL = PatternFill(
    fill_type="solid",
    fgColor="D9EAF7",
)
HEADER_FONT = Font(bold=True)


def default_report_filename(report_date: Optional[date] = None) -> str:
    """Return the default Excel report filename for a date."""
    selected_date = report_date or date.today()
    return f"market_sentinel_report_{selected_date.isoformat()}.xlsx"


def generate_excel_report(
    connection: duckdb.DuckDBPyConnection,
    output_dir: Optional[Path] = None,
    report_date: Optional[date] = None,
    config_dir: Optional[Path] = None,
) -> Path:
    """Generate an Excel report from the local DuckDB database."""
    target_dir = _resolve_excel_output_dir(output_dir, config_dir)
    output_path = target_dir / default_report_filename(report_date)

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        workbook = Workbook()
        summary_sheet = workbook.active
        summary_sheet.title = EXPECTED_WORKSHEET_TITLES[0]

        _write_summary_sheet(summary_sheet, connection)

        report_sections = [
            ("Securities", _fetch_securities),
            ("Latest Prices", _fetch_latest_prices),
            ("Moving Averages", _fetch_moving_averages),
            ("Crossover Signals", _fetch_crossover_signals),
            ("Dividend Metrics", _fetch_dividend_metrics),
            ("High Dividend Stocks", _fetch_high_dividend_stocks),
            ("Dividend Risk Flags", _fetch_dividend_risk_flags),
        ]

        for sheet_title, fetch_data in report_sections:
            _write_table_sheet(workbook, sheet_title, fetch_data(connection))

        workbook.save(output_path)
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read report data from DuckDB. Check that the database "
            "is open and the required tables have been created. "
            f"Details: {error}"
        ) from error
    except OSError as error:
        raise RuntimeError(
            "Could not create or write to the Excel report folder. Check that "
            f"this path exists or can be created: {target_dir}."
        ) from error

    return output_path


def _resolve_excel_output_dir(
    output_dir: Optional[Path],
    config_dir: Optional[Path],
) -> Path:
    """Resolve the Excel output directory from arguments, settings, or fallback."""
    if output_dir is not None:
        return Path(output_dir).expanduser()

    try:
        settings = load_named_config("settings", config_dir)
    except FileNotFoundError:
        return DEFAULT_OUTPUT_DIR

    configured_dir = settings.get("report_outputs", {}).get("excel_dir")

    if configured_dir:
        return Path(str(configured_dir)).expanduser()

    return DEFAULT_OUTPUT_DIR


def _write_summary_sheet(sheet, connection: duckdb.DuckDBPyConnection) -> None:
    """Write the Summary worksheet."""
    rows = [
        ("Metric", "Value"),
        ("Securities", _count_rows(connection, "securities")),
        (
            "Securities With Dividend Metrics",
            _count_dividend_metric_securities(connection),
        ),
        ("Dividend Risk Flags", _count_dividend_risk_flags(connection)),
        ("Daily Price Rows", _count_rows(connection, "daily_prices")),
        ("Moving Average Rows", _count_rows(connection, "moving_average_signals")),
        ("Latest Price Date", _latest_price_date(connection) or "No prices yet"),
    ]
    _write_rows(sheet, rows)


def _write_table_sheet(
    workbook: Workbook,
    title: str,
    table_data: Tuple[List[str], List[Sequence[Any]]],
) -> None:
    """Create one worksheet from headers and rows."""
    sheet = workbook.create_sheet(title)
    headers, rows = table_data
    _write_rows(sheet, [headers] + rows)


def _write_rows(sheet, rows: Iterable[Sequence[Any]]) -> None:
    """Write rows and apply simple formatting."""
    for row in rows:
        sheet.append(list(row))

    if sheet.max_row >= 1:
        for cell in sheet[1]:
            cell.font = HEADER_FONT
            cell.fill = HEADER_FILL

    sheet.freeze_panes = "A2"
    _auto_size_columns(sheet)


def _auto_size_columns(sheet) -> None:
    """Set readable column widths."""
    for column_cells in sheet.columns:
        column_letter = get_column_letter(column_cells[0].column)
        max_length = 0

        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(value))

        sheet.column_dimensions[column_letter].width = min(max_length + 2, 40)


def _fetch_securities(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch securities for the report."""
    headers = ["Ticker", "Name", "Market", "Region", "Currency", "Sector"]
    rows = connection.execute(
        """
        SELECT ticker, name, market, region, currency, sector
        FROM securities
        ORDER BY ticker
        """
    ).fetchall()
    return headers, rows


def _fetch_latest_prices(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch each security's latest daily price."""
    headers = [
        "Ticker",
        "Price Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Adjusted Close",
        "Volume",
    ]
    rows = connection.execute(
        """
        SELECT
            securities.ticker,
            latest_prices.price_date,
            latest_prices.open_price,
            latest_prices.high_price,
            latest_prices.low_price,
            latest_prices.close_price,
            latest_prices.adjusted_close_price,
            latest_prices.volume
        FROM securities
        INNER JOIN daily_prices AS latest_prices
            ON securities.security_id = latest_prices.security_id
        INNER JOIN (
            SELECT security_id, MAX(price_date) AS latest_date
            FROM daily_prices
            GROUP BY security_id
        ) AS latest_dates
            ON latest_prices.security_id = latest_dates.security_id
           AND latest_prices.price_date = latest_dates.latest_date
        ORDER BY securities.ticker
        """
    ).fetchall()
    return headers, rows


def _fetch_moving_averages(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch latest SMA rows."""
    headers = ["Ticker", "Signal Date", "Period Days", "SMA Value"]
    rows = connection.execute(
        """
        SELECT
            securities.ticker,
            signals.signal_date,
            signals.moving_average_period_days,
            signals.moving_average_value
        FROM moving_average_signals AS signals
        INNER JOIN securities
            ON signals.security_id = securities.security_id
        WHERE signals.signal_type = 'SMA'
        ORDER BY securities.ticker, signals.moving_average_period_days
        """
    ).fetchall()
    return headers, rows


def _fetch_crossover_signals(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch crossover signal rows."""
    headers = [
        "Ticker",
        "Signal Date",
        "Short Period",
        "Short SMA",
        "Long Period",
        "Long SMA",
        "Direction",
    ]
    rows = connection.execute(
        """
        SELECT
            securities.ticker,
            signals.signal_date,
            signals.moving_average_period_days,
            signals.moving_average_value,
            signals.comparison_period_days,
            signals.comparison_moving_average_value,
            signals.crossover_direction
        FROM moving_average_signals AS signals
        INNER JOIN securities
            ON signals.security_id = securities.security_id
        WHERE signals.signal_type IN ('BULLISH_CROSSOVER', 'BEARISH_CROSSOVER')
        ORDER BY signals.signal_date DESC, securities.ticker
        """
    ).fetchall()
    return headers, rows


def _fetch_dividend_metrics(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch dividend metrics rows."""
    headers = [
        "Ticker",
        "Metric Date",
        "Trailing 12M Dividend",
        "Dividend Yield",
        "Annual Cash Per 10000",
        "Risk Flag",
        "Risk Reason",
    ]
    rows = connection.execute(
        """
        SELECT
            securities.ticker,
            metrics.metric_date,
            metrics.trailing_annual_dividend,
            metrics.dividend_yield,
            metrics.annual_dividend_cash_per_10000,
            metrics.dividend_risk_flag,
            metrics.dividend_risk_reason
        FROM dividend_metrics AS metrics
        INNER JOIN securities
            ON metrics.security_id = securities.security_id
        ORDER BY securities.ticker, metrics.metric_date DESC
        """
    ).fetchall()
    return headers, rows


def _fetch_high_dividend_stocks(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch dividend metrics sorted by yield descending."""
    headers = [
        "Ticker",
        "Metric Date",
        "Dividend Yield",
        "Trailing 12M Dividend",
        "Annual Cash Per 10000",
        "Risk Flag",
        "Risk Reason",
    ]
    rows = connection.execute(
        """
        SELECT
            securities.ticker,
            metrics.metric_date,
            metrics.dividend_yield,
            metrics.trailing_annual_dividend,
            metrics.annual_dividend_cash_per_10000,
            metrics.dividend_risk_flag,
            metrics.dividend_risk_reason
        FROM dividend_metrics AS metrics
        INNER JOIN securities
            ON metrics.security_id = securities.security_id
        WHERE metrics.dividend_yield IS NOT NULL
        ORDER BY metrics.dividend_yield DESC, securities.ticker
        """
    ).fetchall()
    return headers, rows


def _fetch_dividend_risk_flags(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch dividend risk flag rows."""
    headers = [
        "Ticker",
        "Metric Date",
        "Dividend Yield",
        "Risk Flag",
        "Risk Reason",
    ]
    rows = connection.execute(
        """
        SELECT
            securities.ticker,
            metrics.metric_date,
            metrics.dividend_yield,
            metrics.dividend_risk_flag,
            metrics.dividend_risk_reason
        FROM dividend_metrics AS metrics
        INNER JOIN securities
            ON metrics.security_id = securities.security_id
        WHERE metrics.dividend_risk_flag IS NOT NULL
        ORDER BY metrics.dividend_yield DESC, securities.ticker
        """
    ).fetchall()
    return headers, rows


def _count_rows(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    """Count rows in a known project table."""
    allowed_tables = {
        "securities",
        "daily_prices",
        "moving_average_signals",
        "dividend_metrics",
    }

    if table_name not in allowed_tables:
        raise ValueError(f"Unknown report table: {table_name}")

    return connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def _count_dividend_metric_securities(connection: duckdb.DuckDBPyConnection) -> int:
    """Count securities that have at least one dividend metric row."""
    return connection.execute(
        """
        SELECT COUNT(DISTINCT security_id)
        FROM dividend_metrics
        """
    ).fetchone()[0]


def _count_dividend_risk_flags(connection: duckdb.DuckDBPyConnection) -> int:
    """Count dividend metric rows with risk flags."""
    return connection.execute(
        """
        SELECT COUNT(*)
        FROM dividend_metrics
        WHERE dividend_risk_flag IS NOT NULL
        """
    ).fetchone()[0]


def _latest_price_date(connection: duckdb.DuckDBPyConnection) -> Any:
    """Return the latest daily price date."""
    return connection.execute("SELECT MAX(price_date) FROM daily_prices").fetchone()[0]
