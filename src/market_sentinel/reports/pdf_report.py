"""PDF summary report generation for market-sentinel."""

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import duckdb
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
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
PDF_CHART_WIDTH = 770
PDF_CHART_HEIGHT = 350
DEFAULT_PDF_INCLUDE_SETUP_GRADES = ["Strong Buy Setup", "Strong Sell Setup"]
NO_STRONG_SETUPS_MESSAGE = (
    "No strong buy or strong sell setups were found for this report period."
)


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
            settings,
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
    settings: Optional[Dict[str, Any]] = None,
) -> List[Any]:
    """Build reportlab flowables for the PDF."""
    styles = getSampleStyleSheet()
    chart_summary = chart_summary or {}
    included_details = _included_chart_details(
        chart_summary.get("chart_details", []),
        settings or {},
    )
    story = _index_page_flowables(included_details, report_date, styles)
    story.extend(_chart_flowables(included_details, styles, include_initial_break=True))
    return story


def _index_page_flowables(
    chart_details: Sequence[Dict[str, Any]],
    report_date: date,
    styles,
) -> List[Any]:
    """Build the first-page index of selected chart stocks."""
    detail_count = len(chart_details)
    summary_text = (
        "Stocks shown below are strong buy and strong sell setup grades selected "
        "for chart review."
    )
    if detail_count == 0:
        summary_text = NO_STRONG_SETUPS_MESSAGE

    return [
        Paragraph("Market Sentinel Crossover Chart Report", styles["Title"]),
        Paragraph(f"Report date: {report_date.isoformat()}", styles["Heading3"]),
        Spacer(1, 8),
        Paragraph(summary_text, styles["Normal"]),
        Spacer(1, 12),
        _index_tables(chart_details),
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
    headers = [
        "Ticker",
        "Name",
        "Market",
        "Action grade",
        "Direction",
        "Crossed",
        "Days",
    ]
    table = Table(
        [headers] + list(rows),
        colWidths=[36, 78, 54, 72, 44, 50, 36],
    )
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
                _shorten_name(chart_detail.get("company_name", ""), max_length=17),
                _market_marker(
                    chart_detail.get("market"),
                    chart_detail.get("ticker", ""),
                ),
                _candidate_action_grade(chart_detail),
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
    chart_details: Sequence[Dict[str, Any]],
    styles,
    include_initial_break: bool = True,
) -> List[Any]:
    """Build the selected trend chart section."""
    flowables: List[Any] = []

    if not chart_details:
        return flowables

    if include_initial_break:
        flowables.append(PageBreak())

    for index, chart_detail in enumerate(chart_details):
        path = Path(chart_detail["chart_path"])
        chart_group = [
            Paragraph(_chart_title(chart_detail), styles["Heading2"]),
            Spacer(1, 2),
            Image(
                str(path),
                width=PDF_CHART_WIDTH,
                height=PDF_CHART_HEIGHT,
            ),
            Spacer(1, 3),
            _candidate_card_flowable(chart_detail, styles),
        ]
        flowables.append(KeepTogether(chart_group))
        if index < len(chart_details) - 1:
            flowables.append(PageBreak())

    return flowables


def _chart_title(chart_detail: Dict[str, Any]) -> str:
    """Return a clear chart page title."""
    ticker = chart_detail.get("ticker", "")
    company_name = chart_detail.get("company_name", "")
    market = _market_marker(chart_detail.get("market"), ticker)
    action_grade = _candidate_action_grade(chart_detail)
    title = ticker

    if company_name:
        title = f"{title} — {company_name}"
    title = f"{title} — {market}"
    if action_grade:
        title = f"{title} — {action_grade}"

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


def _candidate_card_flowable(chart_detail: Dict[str, Any], styles) -> Table:
    """Return a compact trade candidate card for one chart page."""
    candidate = chart_detail.get("trade_candidate") or _candidate_from_chart_detail(
        chart_detail
    )
    card_style = ParagraphStyle(
        "CandidateCardCompact",
        parent=styles["Normal"],
        fontSize=7.1,
        leading=8.0,
        spaceBefore=0,
        spaceAfter=0,
    )
    label_style = ParagraphStyle(
        "CandidateCardLabel",
        parent=card_style,
        fontName="Helvetica-Bold",
    )
    rows = [
        [
            Paragraph("Setup", label_style),
            Paragraph(
                "Candidate review | <b>Action grade: "
                f"{_not_available(candidate.get('action_grade'))}</b> | "
                f"Score: {_not_available(candidate.get('score'))} / "
                f"{_not_available(candidate.get('max_score', 10))} | "
                f"Market: {_market_marker(candidate.get('market'), candidate.get('ticker', ''))} | "
                "Rule-based setup grade only. These are not trading instructions.",
                card_style,
            ),
        ],
        [
            Paragraph("Signal", label_style),
            Paragraph(
                _signal_summary(candidate),
                card_style,
            ),
        ],
        [
            Paragraph("Why", label_style),
            Paragraph(f"Why: {_grade_reasons_text(candidate)}", card_style),
        ],
        [
            Paragraph("Caution", label_style),
            Paragraph(
                f"Caution: {_grade_cautions_text(candidate)}",
                card_style,
            ),
        ],
        [
            Paragraph("Planning", label_style),
            Paragraph(
                "Latest close: "
                + _format_price(
                    candidate.get("latest_close_price"),
                    candidate.get("currency"),
                )
                + " | Planning reference levels: "
                + _review_levels_text(candidate)
                + " | Key risk note: "
                + _key_risk_note_text(candidate),
                card_style,
            ),
        ],
    ]
    table = Table(rows, colWidths=[86, 664], splitByRow=0)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EEF4F8")),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#AAB7C4")),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D7DEE6")),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 7.1),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 1.5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5),
                ("LEFTPADDING", (0, 0), (-1, -1), 3),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    return table


def _candidate_from_chart_detail(chart_detail: Dict[str, Any]) -> Dict[str, Any]:
    """Build a minimal card model when charts were mocked in tests."""
    first_signal = (chart_detail.get("signals") or [{}])[0]
    return {
        "ticker": chart_detail.get("ticker", ""),
        "company_name": chart_detail.get("company_name", ""),
        "market": _market_marker(
            chart_detail.get("market"),
            chart_detail.get("ticker", ""),
        ),
        "currency": chart_detail.get("currency", ""),
        "signal_direction": first_signal.get("direction", ""),
        "signal_description": first_signal.get("trend_description", "Not available"),
        "crossover_date": first_signal.get("crossover_date"),
        "days_since_crossover": first_signal.get(
            "days_since_crossover",
            "Not available",
        ),
        "latest_close_price": chart_detail.get("latest_close_price"),
        "review_levels": chart_detail.get("review_levels", {}),
        "action_grade": chart_detail.get("action_grade", "Track Only"),
        "score": chart_detail.get("score", 0),
        "max_score": chart_detail.get("max_score", 10),
        "grade_reasons": chart_detail.get("grade_reasons", []),
        "grade_cautions": chart_detail.get("grade_cautions", []),
        "risk_notes": chart_detail.get("risk_notes", []),
    }


def _candidate_action_grade(chart_detail: Dict[str, Any]) -> str:
    """Return the action grade for index display."""
    candidate = chart_detail.get("trade_candidate") or {}
    return _not_available(candidate.get("action_grade", "Track Only"))


def _sorted_chart_details(
    chart_details: Sequence[Dict[str, Any]],
    include_grades: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Return chart details in the requested PDF page order."""
    filtered_details = [
        chart_detail
        for chart_detail in chart_details
        if _candidate_action_grade(chart_detail) in _include_setup_grades(include_grades)
    ]
    return sorted(filtered_details, key=_chart_detail_sort_key)


def _included_chart_details(
    chart_details: Sequence[Dict[str, Any]],
    settings: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Return chart details included in the PDF after grade filtering."""
    return _sorted_chart_details(
        chart_details,
        include_grades=_pdf_include_setup_grades(settings),
    )


def _pdf_include_setup_grades(settings: Dict[str, Any]) -> List[str]:
    """Read which setup grades should appear in the PDF."""
    return _include_setup_grades(settings.get("pdf_include_setup_grades"))


def _include_setup_grades(raw_grades: Optional[Sequence[str]]) -> List[str]:
    """Return a cleaned grade allow-list with a safe default."""
    if not raw_grades:
        return DEFAULT_PDF_INCLUDE_SETUP_GRADES

    if isinstance(raw_grades, str):
        raw_grades = [raw_grades]

    grades = [str(grade) for grade in raw_grades if str(grade).strip()]
    return grades or DEFAULT_PDF_INCLUDE_SETUP_GRADES


def _chart_detail_sort_key(chart_detail: Dict[str, Any]) -> tuple:
    """Sort chart pages by setup grade, recency, score, then ticker."""
    candidate = chart_detail.get("trade_candidate") or {}
    first_signal = (chart_detail.get("signals") or [{}])[0]
    crossover_date = first_signal.get("crossover_date")

    if hasattr(crossover_date, "toordinal"):
        crossover_ordinal = crossover_date.toordinal()
    else:
        crossover_ordinal = 0

    return (
        _grade_sort_rank(candidate.get("action_grade")),
        -crossover_ordinal,
        -(candidate.get("score") or 0),
        chart_detail.get("ticker", ""),
    )


def _grade_sort_rank(action_grade: Any) -> int:
    """Return the requested PDF sort rank for a setup grade."""
    ranks = {
        "Strong Buy Setup": 0,
        "Strong Sell Setup": 1,
        "Buy Setup": 2,
        "Track Only": 3,
        "Sell Setup": 4,
    }
    return ranks.get(str(action_grade), 3)


def _signal_summary(candidate: Dict[str, Any]) -> str:
    """Return signal fields as compact card text."""
    crossover_date = candidate.get("crossover_date")
    if hasattr(crossover_date, "isoformat"):
        crossover_date = crossover_date.isoformat()

    return (
        f"{_not_available(candidate.get('signal_direction'))}: "
        f"{_not_available(candidate.get('signal_description'))} | "
        f"Crossover date: {_not_available(crossover_date)} | "
        f"Days since crossover: {_not_available(candidate.get('days_since_crossover'))}"
    )


def _review_levels_text(candidate: Dict[str, Any]) -> str:
    """Return formatted planning reference levels."""
    review_levels = candidate.get("review_levels") or {}

    if not review_levels:
        return "Not available"

    return " | ".join(
        f"{label} {_format_price(value, candidate.get('currency'))}"
        for label, value in review_levels.items()
    )


def _grade_reasons_text(candidate: Dict[str, Any]) -> str:
    """Return the positive reasons behind the setup grade."""
    reasons = candidate.get("grade_reasons") or []

    if not reasons:
        return "Not available"

    return "; ".join(_compact_sentence(reason) for reason in reasons[:3])


def _grade_cautions_text(candidate: Dict[str, Any]) -> str:
    """Return cautions behind the setup grade."""
    cautions = candidate.get("grade_cautions") or []

    if not cautions:
        return "No major rule-based cautions."

    return "; ".join(_compact_sentence(caution) for caution in cautions[:2])


def _risk_notes_text(candidate: Dict[str, Any]) -> str:
    """Return compact risk notes text."""
    risk_notes = candidate.get("risk_notes") or []

    if not risk_notes:
        return "Not available"

    return " | ".join(str(note) for note in risk_notes)


def _key_risk_note_text(candidate: Dict[str, Any]) -> str:
    """Return one compact risk note for the one-page PDF card."""
    risk_notes = candidate.get("risk_notes") or []

    if not risk_notes:
        return "Not available"

    return _compact_sentence(risk_notes[0])


def _compact_sentence(value: Any) -> str:
    """Return short sentence-like text for compact PDF cards."""
    text = str(value).strip()
    return text[:-1] if text.endswith(".") else text


def _format_price(value: Any, currency: Optional[str] = None) -> str:
    """Format a price-like value for PDF text."""
    if value is None:
        return "Not available"

    try:
        formatted_value = f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "Not available"

    if currency:
        return f"{currency} {formatted_value}"

    return formatted_value


def _not_available(value: Any) -> str:
    """Return readable text for optional values."""
    if value is None or value == "":
        return "Not available"

    return str(value)


def _market_marker(market: Any, ticker: str = "") -> str:
    """Return a readable market marker with a simple ticker fallback."""
    if market is not None and str(market).strip():
        return str(market).strip()

    if str(ticker).upper().endswith(".L"):
        return "FTSE 350"

    if ticker:
        return "S&P 500"

    return "Market unknown"
