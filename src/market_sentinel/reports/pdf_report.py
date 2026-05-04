"""PDF summary report generation for market-sentinel."""

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import duckdb
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import (
    Image,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from market_sentinel.config.loader import load_named_config
from market_sentinel.reports.charts import generate_charts

DEFAULT_OUTPUT_DIR = Path("outputs") / "pdf"
LANDSCAPE_PAGE_SIZE = landscape(A4)


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
    settings = _load_pdf_settings(config_dir)
    target_dir = _resolve_pdf_output_dir(output_dir, settings)
    output_path = target_dir / default_report_filename(selected_date)

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        chart_summary = generate_charts(connection, config_dir=config_dir)
        story = _build_report_story(
            selected_date,
            chart_summary,
        )
        document = SimpleDocTemplate(
            str(output_path),
            pagesize=LANDSCAPE_PAGE_SIZE,
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
    settings: Dict[str, Any],
) -> Path:
    """Resolve the PDF output directory from arguments, settings, or fallback."""
    if output_dir is not None:
        return Path(output_dir).expanduser()

    configured_dir = settings.get("report_outputs", {}).get("pdf_dir")

    if configured_dir:
        return Path(str(configured_dir)).expanduser()

    return DEFAULT_OUTPUT_DIR


def _load_pdf_settings(config_dir: Optional[Path]) -> Dict[str, Any]:
    """Load PDF settings, falling back to defaults if settings are unavailable."""
    try:
        return load_named_config("settings", config_dir)
    except FileNotFoundError:
        return {}


def _build_report_story(
    report_date: date,
    chart_summary: Optional[Dict[str, Any]] = None,
) -> List[Any]:
    """Build reportlab flowables for the PDF."""
    styles = getSampleStyleSheet()
    chart_summary = chart_summary or {}
    story = _index_page_flowables(chart_summary, report_date, styles)
    story.extend(_chart_flowables(chart_summary, styles, include_initial_break=True))
    return story


def _index_page_flowables(
    chart_summary: Dict[str, Any],
    report_date: date,
    styles,
) -> List[Any]:
    """Build the first-page index of selected chart stocks."""
    return [
        Paragraph("Market Sentinel Crossover Chart Report", styles["Title"]),
        Paragraph(f"Report date: {report_date.isoformat()}", styles["Heading3"]),
        Spacer(1, 8),
        Paragraph(
            "Stocks shown below are the latest crossover signals selected for "
            "chart review.",
            styles["Normal"],
        ),
        Spacer(1, 12),
        _index_tables(chart_summary.get("chart_details", [])),
    ]


def _index_tables(chart_details: Sequence[Dict[str, Any]]) -> Table:
    """Return two side-by-side compact index tables."""
    left_rows = _index_rows(chart_details[:25])
    right_rows = _index_rows(chart_details[25:50])
    index_table = Table(
        [[_compact_index_table(left_rows), _compact_index_table(right_rows)]],
        colWidths=[370, 370],
    )
    index_table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )
    return index_table


def _compact_index_table(rows: Sequence[Sequence[Any]]) -> Table:
    """Return one compact index table for up to 25 stocks."""
    headers = ["Ticker", "Name", "Direction", "Crossed", "Days"]
    table = Table([headers] + list(rows), colWidths=[48, 135, 55, 70, 62])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#D9EAF7")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
                ("FONTSIZE", (0, 0), (-1, -1), 6.6),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                ("TOPPADDING", (0, 0), (-1, -1), 2),
            ]
        )
    )
    return table


def _index_rows(chart_details: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    """Return compact index rows in the same order as the chart pages."""
    rows = []

    for chart_detail in chart_details:
        first_signal = (chart_detail.get("signals") or [{}])[0]
        crossover_date = first_signal.get("crossover_date", "")
        if hasattr(crossover_date, "isoformat"):
            crossover_date = crossover_date.isoformat()

        rows.append(
            [
                chart_detail.get("ticker", ""),
                _shorten_name(chart_detail.get("company_name", "")),
                first_signal.get("direction", ""),
                crossover_date,
                first_signal.get("days_since_crossover", ""),
            ]
        )

    return rows


def _shorten_name(name: str, max_length: int = 28) -> str:
    """Shorten a company name for the compact PDF index."""
    if len(name) <= max_length:
        return name

    return f"{name[: max_length - 3]}..."


def _chart_flowables(
    chart_summary: Dict[str, Any],
    styles,
    include_initial_break: bool = True,
) -> List[Any]:
    """Build the selected trend chart section."""
    flowables: List[Any] = []
    chart_details = chart_summary.get("chart_details", [])

    if not chart_details:
        flowables.append(Paragraph("No recent crossover charts to show.", styles["Heading2"]))
        flowables.append(Spacer(1, 12))
        return flowables

    if include_initial_break:
        flowables.append(PageBreak())

    for index, chart_detail in enumerate(chart_details):
        path = Path(chart_detail["chart_path"])
        chart_group = [
            Paragraph(_chart_title(chart_detail), styles["Heading2"]),
            Image(str(path), width=760, height=395, kind="proportional"),
            Spacer(1, 10),
            Paragraph(_selection_reason_text(chart_detail), styles["Normal"]),
            Spacer(1, 12),
        ]
        flowables.append(KeepTogether(chart_group))
        if index < len(chart_details) - 1:
            flowables.append(PageBreak())

    return flowables


def _chart_title(chart_detail: Dict[str, Any]) -> str:
    """Return a clear chart page title."""
    ticker = chart_detail.get("ticker", "")
    company_name = chart_detail.get("company_name", "")
    market = chart_detail.get("market", "")
    title = ticker

    if company_name:
        title = f"{title} - {company_name}"
    if market:
        title = f"{title} ({market})"

    return title


def _selection_reason_text(chart_detail: Dict[str, Any]) -> str:
    """Return the reason text shown below a chart."""
    signals = chart_detail.get("signals", [])

    if not signals:
        return "Selected because: this ticker was requested for chart generation."

    reasons = []
    for signal in signals:
        reasons.append(
            "Selected because: "
            f"{signal['trend_description']} on "
            f"{signal['crossover_date'].isoformat()} — "
            f"{signal['days_since_crossover']}."
        )

    return "<br/>".join(reasons)
