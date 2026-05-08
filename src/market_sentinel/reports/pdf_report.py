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
from market_sentinel.analytics.trade_candidates import (
    DEFAULT_PORTFOLIO_PRIORITY_ORDER,
    portfolio_priority_rank,
)
from market_sentinel.reports.charts import generate_charts

DEFAULT_OUTPUT_DIR = Path("outputs") / "pdf"
LANDSCAPE_PAGE_SIZE = landscape(A4)
PDF_CHART_WIDTH = 770
PDF_CHART_HEIGHT = 350
DEFAULT_PDF_INCLUDE_SETUP_GRADES = ["Strong Buy Setup", "Strong Sell Setup"]
DEFAULT_PDF_MAX_CHARTS_TOTAL = 50
DEFAULT_PDF_MAX_CHARTS_PER_MARKET = 25
PDF_MARKET_ORDER = ["S&P 500", "FTSE 350"]
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
        *(_daily_action_summary_flowables(chart_details, styles) if detail_count else []),
        *(_portfolio_market_count_flowables(chart_details, styles) if detail_count else []),
        Spacer(1, 12),
        _index_tables(chart_details),
    ]


def _daily_action_summary_flowables(
    chart_details: Sequence[Dict[str, Any]],
    styles,
) -> List[Any]:
    """Return a compact first-page Daily Action Summary."""
    summary = _daily_action_summary(chart_details)
    rows = [
        [
            "Total",
            "Strong Buy",
            "Strong Sell",
            "Held",
            "Watchlist",
            "New",
            "S&P 500",
            "FTSE 350",
            "Top score",
            "Dividend risk",
        ],
        [
            summary["total"],
            summary["strong_buy"],
            summary["strong_sell"],
            summary["held"],
            summary["watchlist"],
            summary["new"],
            summary["sp_500"],
            summary["ftse_350"],
            summary["highest_score_candidate"],
            summary["dividend_risk_flags"],
        ],
    ]
    table = Table(rows, colWidths=[42, 58, 58, 42, 58, 42, 52, 52, 118, 62])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#D9EAF7")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ]
        )
    )
    return [
        Spacer(1, 8),
        Paragraph("Daily Action Summary", styles["Heading3"]),
        table,
    ]


def _daily_action_summary(chart_details: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Return PDF Daily Action Summary counts for included candidates."""
    summary = {
        "total": len(chart_details),
        "strong_buy": 0,
        "strong_sell": 0,
        "held": 0,
        "watchlist": 0,
        "new": 0,
        "sp_500": 0,
        "ftse_350": 0,
        "highest_score_candidate": "Not available",
        "dividend_risk_flags": 0,
    }
    highest_detail = None
    highest_score = None

    for chart_detail in chart_details:
        candidate = chart_detail.get("trade_candidate") or {}
        grade = candidate.get("action_grade")
        market = _market_marker(chart_detail.get("market"), chart_detail.get("ticker", ""))
        status = _candidate_portfolio_status(chart_detail)
        score = _score_value(candidate.get("score"))

        if grade == "Strong Buy Setup":
            summary["strong_buy"] += 1
        elif grade == "Strong Sell Setup":
            summary["strong_sell"] += 1

        if status in {"Held", "Held + Watchlist"}:
            summary["held"] += 1
        elif status == "Watchlist":
            summary["watchlist"] += 1
        else:
            summary["new"] += 1

        if market == "S&P 500":
            summary["sp_500"] += 1
        elif market == "FTSE 350":
            summary["ftse_350"] += 1

        if candidate.get("dividend_risk_flag"):
            summary["dividend_risk_flags"] += 1

        if highest_score is None or score > highest_score:
            highest_score = score
            highest_detail = chart_detail

    if highest_detail is not None:
        candidate = highest_detail.get("trade_candidate") or {}
        summary["highest_score_candidate"] = (
            f"{highest_detail.get('ticker', '')} ({_score_value(candidate.get('score')):g})"
        )

    return summary


def _portfolio_market_count_flowables(
    chart_details: Sequence[Dict[str, Any]],
    styles,
) -> List[Any]:
    """Return a compact portfolio and market count summary for the PDF index."""
    market_counts: Dict[str, int] = {}
    portfolio_counts = {
        "Held": 0,
        "Watchlist": 0,
        "New": 0,
    }

    for chart_detail in chart_details:
        market = _market_marker(chart_detail.get("market"), chart_detail.get("ticker", ""))
        market_counts[market] = market_counts.get(market, 0) + 1
        portfolio_status = _candidate_portfolio_status(chart_detail)
        if portfolio_status in {"Held", "Held + Watchlist"}:
            portfolio_counts["Held"] += 1
        elif portfolio_status == "Watchlist":
            portfolio_counts["Watchlist"] += 1
        else:
            portfolio_counts["New"] += 1

    ordered_markets = [
        market for market in PDF_MARKET_ORDER if market in market_counts
    ] + sorted(market for market in market_counts if market not in PDF_MARKET_ORDER)
    summary_parts = [
        f"Held: {portfolio_counts['Held']}",
        f"Watchlist: {portfolio_counts['Watchlist']}",
        f"New: {portfolio_counts['New']}",
    ]
    summary_parts.extend(
        f"{market}: {market_counts[market]}" for market in ordered_markets
    )

    return [
        Spacer(1, 4),
        Paragraph("Included: " + " | ".join(summary_parts), styles["Normal"]),
    ]


def _index_tables(chart_details: Sequence[Dict[str, Any]]) -> Table:
    """Return two side-by-side compact index tables."""
    grouped_rows = _grouped_index_rows(chart_details)
    left_rows = grouped_rows[:25]
    right_rows = grouped_rows[25:50]
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
        "Portfolio",
    ]
    table = Table(
        [headers] + list(rows),
        colWidths=[36, 70, 50, 64, 40, 46, 28, 36],
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


def _grouped_index_rows(chart_details: Sequence[Dict[str, Any]]) -> List[List[Any]]:
    """Return index rows with compact portfolio group marker rows."""
    grouped_rows = []
    group_labels = [
        ("Held / Held + Watchlist", {"Held", "Held + Watchlist"}),
        ("Watchlist", {"Watchlist"}),
        ("New candidates", {"New"}),
    ]

    for label, statuses in group_labels:
        group_details = [
            chart_detail
            for chart_detail in chart_details
            if _candidate_portfolio_status(chart_detail) in statuses
        ]
        if not group_details:
            continue
        grouped_rows.append([label, "", "", "", "", "", "", ""])
        grouped_rows.extend(_index_rows(group_details))

    return grouped_rows


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
                _portfolio_status_text(
                    chart_detail.get("trade_candidate") or chart_detail,
                    compact=True,
                ),
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
                f"Portfolio status: {_portfolio_status_text(candidate)} | "
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
        "portfolio_status": chart_detail.get("portfolio_status", "New"),
        "holding_quantity": chart_detail.get("holding_quantity", ""),
        "watchlist_reason": chart_detail.get("watchlist_reason", ""),
    }


def _candidate_action_grade(chart_detail: Dict[str, Any]) -> str:
    """Return the action grade for index display."""
    candidate = chart_detail.get("trade_candidate") or {}
    return _not_available(candidate.get("action_grade", "Track Only"))


def _sorted_chart_details(
    chart_details: Sequence[Dict[str, Any]],
    include_grades: Optional[Sequence[str]] = None,
    max_total: int = DEFAULT_PDF_MAX_CHARTS_TOTAL,
    max_per_market: int = DEFAULT_PDF_MAX_CHARTS_PER_MARKET,
) -> List[Dict[str, Any]]:
    """Return chart details in the market-balanced PDF page order."""
    filtered_details = [
        chart_detail
        for chart_detail in chart_details
        if _candidate_action_grade(chart_detail) in _include_setup_grades(include_grades)
    ]
    return _market_balanced_chart_details(
        filtered_details,
        max_total=max_total,
        max_per_market=max_per_market,
    )


def _included_chart_details(
    chart_details: Sequence[Dict[str, Any]],
    settings: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Return chart details included in the PDF after grade filtering."""
    return _sorted_chart_details(
        chart_details,
        include_grades=_pdf_include_setup_grades(settings),
        max_total=_positive_int_setting(
            settings,
            "pdf_max_charts_total",
            DEFAULT_PDF_MAX_CHARTS_TOTAL,
        ),
        max_per_market=_positive_int_setting(
            settings,
            "pdf_max_charts_per_market",
            DEFAULT_PDF_MAX_CHARTS_PER_MARKET,
        ),
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


def _positive_int_setting(
    settings: Dict[str, Any],
    setting_name: str,
    default_value: int,
) -> int:
    """Read a positive integer setting with a safe fallback."""
    raw_value = settings.get(setting_name, default_value)

    try:
        parsed_value = int(raw_value)
    except (TypeError, ValueError):
        return default_value

    if parsed_value < 1:
        return default_value

    return parsed_value


def _market_balanced_chart_details(
    chart_details: Sequence[Dict[str, Any]],
    max_total: int,
    max_per_market: int,
) -> List[Dict[str, Any]]:
    """Select strong setup charts with a fair market split and spillover."""
    if not chart_details:
        return []

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for chart_detail in chart_details:
        market = _market_marker(chart_detail.get("market"), chart_detail.get("ticker", ""))
        grouped.setdefault(market, []).append(chart_detail)

    for market_details in grouped.values():
        market_details.sort(key=_market_candidate_sort_key)

    selected_by_market: Dict[str, List[Dict[str, Any]]] = {}
    leftovers: List[Dict[str, Any]] = []

    for market, market_details in grouped.items():
        selected_by_market[market] = market_details[:max_per_market]
        leftovers.extend(market_details[max_per_market:])

    selected_count = sum(len(details) for details in selected_by_market.values())
    remaining_slots = max(0, max_total - selected_count)

    if remaining_slots:
        leftovers.sort(key=_market_candidate_sort_key)
        for chart_detail in leftovers[:remaining_slots]:
            market = _market_marker(
                chart_detail.get("market"),
                chart_detail.get("ticker", ""),
            )
            selected_by_market.setdefault(market, []).append(chart_detail)

    selected: List[Dict[str, Any]] = []
    for market_details in selected_by_market.values():
        selected.extend(market_details)

    selected.sort(key=_portfolio_candidate_sort_key)
    return selected[:max_total]


def _market_candidate_sort_key(chart_detail: Dict[str, Any]) -> tuple:
    """Sort candidates within one market for PDF selection."""
    candidate = chart_detail.get("trade_candidate") or {}
    crossover_date = _chart_crossover_date(chart_detail)

    if hasattr(crossover_date, "toordinal"):
        crossover_ordinal = crossover_date.toordinal()
    else:
        crossover_ordinal = 0

    return (
        _portfolio_status_rank(candidate.get("portfolio_status")),
        -_score_value(candidate.get("score")),
        _strong_grade_sort_rank(candidate.get("action_grade")),
        -crossover_ordinal,
        chart_detail.get("ticker", ""),
    )


def _chart_detail_sort_key(chart_detail: Dict[str, Any]) -> tuple:
    """Return the current PDF chart sort key for compatibility."""
    market = _market_marker(chart_detail.get("market"), chart_detail.get("ticker", ""))
    return (
        *_portfolio_candidate_sort_key(chart_detail),
        _market_sort_key(market),
    )


def _portfolio_candidate_sort_key(chart_detail: Dict[str, Any]) -> tuple:
    """Sort selected PDF candidates in portfolio-aware review order."""
    candidate = chart_detail.get("trade_candidate") or {}
    crossover_date = _chart_crossover_date(chart_detail)

    if hasattr(crossover_date, "toordinal"):
        crossover_ordinal = crossover_date.toordinal()
    else:
        crossover_ordinal = 0

    return (
        _portfolio_status_rank(candidate.get("portfolio_status")),
        -_score_value(candidate.get("score")),
        _strong_grade_sort_rank(candidate.get("action_grade")),
        -crossover_ordinal,
        chart_detail.get("ticker", ""),
    )


def _strong_grade_sort_rank(action_grade: Any) -> int:
    """Sort strong buy before strong sell when scores are equal."""
    ranks = {
        "Strong Buy Setup": 0,
        "Strong Sell Setup": 1,
    }
    return ranks.get(str(action_grade), 2)


def _market_sort_key(market: str) -> tuple:
    """Return a deterministic market group order for PDF pages."""
    try:
        return (PDF_MARKET_ORDER.index(market), market)
    except ValueError:
        return (len(PDF_MARKET_ORDER), market)


def _score_value(score: Any) -> float:
    """Return a numeric score for sorting."""
    try:
        return float(score)
    except (TypeError, ValueError):
        return 0.0


def _candidate_portfolio_status(chart_detail: Dict[str, Any]) -> str:
    """Return portfolio status from a chart detail or nested candidate."""
    candidate = chart_detail.get("trade_candidate") or chart_detail
    return str(candidate.get("portfolio_status") or "New")


def _portfolio_status_rank(portfolio_status: Any) -> int:
    """Return the configured default PDF portfolio priority rank."""
    return portfolio_priority_rank(
        portfolio_status,
        DEFAULT_PORTFOLIO_PRIORITY_ORDER,
    )


def _chart_crossover_date(chart_detail: Dict[str, Any]) -> Any:
    """Return the crossover date used for PDF sorting."""
    candidate = chart_detail.get("trade_candidate") or {}
    if candidate.get("crossover_date"):
        return candidate.get("crossover_date")

    first_signal = (chart_detail.get("signals") or [{}])[0]
    return first_signal.get("crossover_date")


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


def _portfolio_status_text(
    candidate: Dict[str, Any],
    compact: bool = False,
) -> str:
    """Return compact portfolio status text for PDF index and cards."""
    status = str(candidate.get("portfolio_status") or "New").strip()
    if status == "New" and not compact:
        status = "New candidate"

    quantity = candidate.get("holding_quantity")
    if quantity not in (None, ""):
        return f"{status} ({quantity})"

    return status


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
