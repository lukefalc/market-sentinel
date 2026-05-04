"""PDF summary report generation for market-sentinel."""

from datetime import date
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

import duckdb
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from market_sentinel.config.loader import load_named_config

DEFAULT_OUTPUT_DIR = Path("outputs") / "pdf"


def default_report_filename(report_date: Optional[date] = None) -> str:
    """Return the default PDF report filename for a date."""
    selected_date = report_date or date.today()
    return f"market_sentinel_report_{selected_date.isoformat()}.pdf"


def generate_pdf_report(
    connection: duckdb.DuckDBPyConnection,
    output_dir: Optional[Path] = None,
    report_date: Optional[date] = None,
    config_dir: Optional[Path] = None,
) -> Path:
    """Generate a simple daily PDF summary report."""
    selected_date = report_date or date.today()
    target_dir = _resolve_pdf_output_dir(output_dir, config_dir)
    output_path = target_dir / default_report_filename(selected_date)

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        story = _build_report_story(connection, selected_date)
        document = SimpleDocTemplate(
            str(output_path),
            pagesize=A4,
            rightMargin=36,
            leftMargin=36,
            topMargin=36,
            bottomMargin=36,
        )
        document.build(story)
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not read PDF report data from DuckDB. Check that the "
            f"required tables have been created. Details: {error}"
        ) from error
    except OSError as error:
        raise RuntimeError(
            "Could not create or write to the PDF report folder. Check that "
            f"this path exists or can be created: {target_dir}."
        ) from error

    return output_path


def _resolve_pdf_output_dir(
    output_dir: Optional[Path],
    config_dir: Optional[Path],
) -> Path:
    """Resolve the PDF output directory from arguments, settings, or fallback."""
    if output_dir is not None:
        return Path(output_dir).expanduser()

    try:
        settings = load_named_config("settings", config_dir)
    except FileNotFoundError:
        return DEFAULT_OUTPUT_DIR

    configured_dir = settings.get("report_outputs", {}).get("pdf_dir")

    if configured_dir:
        return Path(str(configured_dir)).expanduser()

    return DEFAULT_OUTPUT_DIR


def _build_report_story(
    connection: duckdb.DuckDBPyConnection,
    report_date: date,
) -> List[Any]:
    """Build reportlab flowables for the PDF."""
    styles = getSampleStyleSheet()
    story = [
        Paragraph("Market Sentinel Daily Summary", styles["Title"]),
        Paragraph(f"Report date: {report_date.isoformat()}", styles["Normal"]),
        Spacer(1, 12),
    ]

    sections = [
        ("Summary Counts", _summary_counts(connection)),
        ("Top High-Dividend Stocks", _top_high_dividend_stocks(connection)),
        ("Dividend Risk Flags", _dividend_risk_flags(connection)),
        ("Latest Moving Average Values", _latest_moving_averages(connection)),
        ("Crossover Signals", _crossover_signals(connection)),
    ]

    for title, table_data in sections:
        story.append(Paragraph(title, styles["Heading2"]))
        story.append(_make_table(*table_data))
        story.append(Spacer(1, 12))

    return story


def _make_table(headers: Sequence[Any], rows: Sequence[Sequence[Any]]) -> Table:
    """Create a compact reportlab table."""
    display_rows = [headers] + [_format_row(row) for row in rows]

    if not rows:
        display_rows.append(["No data available"] + [""] * (len(headers) - 1))

    table = Table(display_rows, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#D9EAF7")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return table


def _format_row(row: Sequence[Any]) -> List[Any]:
    """Format common values for display."""
    return [_format_value(value) for value in row]


def _format_value(value: Any) -> Any:
    """Format one value for PDF output."""
    if value is None:
        return ""

    if isinstance(value, float):
        return round(value, 4)

    return value


def _summary_counts(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch summary counts for the PDF."""
    rows = [
        ("Securities", _count_rows(connection, "securities")),
        ("Latest Prices", _count_latest_prices(connection)),
        ("Moving Average Values", _count_rows(connection, "moving_average_signals")),
        ("Dividend Metric Rows", _count_rows(connection, "dividend_metrics")),
        ("Dividend Risk Flags", _count_dividend_risk_flags(connection)),
    ]
    return ["Metric", "Value"], rows


def _top_high_dividend_stocks(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch top dividend-yield rows."""
    rows = connection.execute(
        """
        SELECT
            securities.ticker,
            metrics.metric_date,
            metrics.dividend_yield,
            metrics.trailing_annual_dividend,
            metrics.dividend_risk_flag
        FROM dividend_metrics AS metrics
        INNER JOIN securities
            ON metrics.security_id = securities.security_id
        WHERE metrics.dividend_yield IS NOT NULL
        ORDER BY metrics.dividend_yield DESC, securities.ticker
        LIMIT 10
        """
    ).fetchall()
    return ["Ticker", "Date", "Yield", "Trailing Dividend", "Risk Flag"], rows


def _dividend_risk_flags(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch dividend risk flags."""
    rows = connection.execute(
        """
        SELECT
            securities.ticker,
            metrics.metric_date,
            metrics.dividend_yield,
            metrics.dividend_risk_reason
        FROM dividend_metrics AS metrics
        INNER JOIN securities
            ON metrics.security_id = securities.security_id
        WHERE metrics.dividend_risk_flag IS NOT NULL
        ORDER BY metrics.dividend_yield DESC, securities.ticker
        LIMIT 10
        """
    ).fetchall()
    return ["Ticker", "Date", "Yield", "Reason"], rows


def _latest_moving_averages(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch latest moving average values."""
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
        ORDER BY signals.signal_date DESC,
                 securities.ticker,
                 signals.moving_average_period_days
        LIMIT 20
        """
    ).fetchall()
    return ["Ticker", "Date", "Period", "SMA"], rows


def _crossover_signals(
    connection: duckdb.DuckDBPyConnection,
) -> Tuple[List[str], List[Sequence[Any]]]:
    """Fetch crossover signals."""
    rows = connection.execute(
        """
        SELECT
            securities.ticker,
            signals.signal_date,
            signals.moving_average_period_days,
            signals.comparison_period_days,
            signals.crossover_direction
        FROM moving_average_signals AS signals
        INNER JOIN securities
            ON signals.security_id = securities.security_id
        WHERE signals.signal_type IN ('BULLISH_CROSSOVER', 'BEARISH_CROSSOVER')
        ORDER BY signals.signal_date DESC, securities.ticker
        LIMIT 20
        """
    ).fetchall()
    return ["Ticker", "Date", "Short", "Long", "Direction"], rows


def _count_rows(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    """Count rows in a known report table."""
    allowed_tables = {
        "securities",
        "moving_average_signals",
        "dividend_metrics",
    }

    if table_name not in allowed_tables:
        raise ValueError(f"Unknown report table: {table_name}")

    return connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def _count_latest_prices(connection: duckdb.DuckDBPyConnection) -> int:
    """Count securities with at least one daily price."""
    return connection.execute(
        """
        SELECT COUNT(DISTINCT security_id)
        FROM daily_prices
        """
    ).fetchone()[0]


def _count_dividend_risk_flags(connection: duckdb.DuckDBPyConnection) -> int:
    """Count dividend risk flags."""
    return connection.execute(
        """
        SELECT COUNT(*)
        FROM dividend_metrics
        WHERE dividend_risk_flag IS NOT NULL
        """
    ).fetchone()[0]
