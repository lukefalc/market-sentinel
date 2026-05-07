"""Tests for PDF report generation."""

import base64
from datetime import date
from pathlib import Path
import re

from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema
from market_sentinel.reports import pdf_report as pdf_report_module
from market_sentinel.reports.pdf_report import (
    LANDSCAPE_PAGE_SIZE,
    PDF_CHART_HEIGHT,
    PDF_CHART_WIDTH,
    _candidate_card_flowable,
    _chart_flowables,
    _index_page_flowables,
    _index_rows,
    _included_chart_details,
    _review_levels_text,
    _selection_reason_text,
    _sorted_chart_details,
    generate_pdf_report,
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
    pdf_dir: str,
    chart_dir: str = "",
) -> None:
    """Create settings with a custom PDF report output folder."""
    config_dir.mkdir(parents=True, exist_ok=True)
    chart_output_dir = chart_dir or str(config_dir.parent / "charts")
    config_dir.joinpath("settings.yaml").write_text(
        "\n".join(
            [
                f"database_path: {database_path}",
                "chart_max_tickers: 2",
                "report_outputs:",
                f"  pdf_dir: {pdf_dir}",
                f"  chart_dir: {chart_output_dir}",
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


def test_generate_pdf_report_creates_pdf_file(tmp_path: Path, monkeypatch) -> None:
    """PDF report generation should create a dated PDF file."""
    connection = open_test_database(tmp_path)
    output_dir = tmp_path / "outputs" / "pdf"
    monkeypatch.setattr(
        pdf_report_module,
        "generate_charts",
        fake_generate_charts_without_images,
    )

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


def test_generate_pdf_report_creates_output_folder(tmp_path: Path, monkeypatch) -> None:
    """PDF report generation should create the output folder automatically."""
    connection = open_test_database(tmp_path)
    output_dir = tmp_path / "new" / "outputs" / "pdf"
    monkeypatch.setattr(
        pdf_report_module,
        "generate_charts",
        fake_generate_charts_without_images,
    )

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


def test_generate_pdf_report_uses_configured_output_folder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """PDF reports should use configured output folders and expand ~."""
    monkeypatch.setenv("HOME", str(tmp_path))
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_report_settings(
        config_dir,
        database_path,
        "~/Library/CloudStorage/OneDrive-Personal/Finance/MarketSentinel/PDF",
    )
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    monkeypatch.setattr(
        pdf_report_module,
        "generate_charts",
        fake_generate_charts_without_images,
    )

    try:
        output_path = generate_pdf_report(
            connection,
            report_date=date(2026, 5, 4),
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
        / "PDF"
    )

    assert output_path.parent == expected_dir
    assert output_path.exists()


def test_generate_pdf_report_can_include_chart_images(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """PDF generation should be able to embed selected chart images."""
    connection = open_test_database(tmp_path)
    output_dir = tmp_path / "outputs" / "pdf"
    chart_path = tmp_path / "charts" / "AAA_price_trend.png"
    chart_path.parent.mkdir(parents=True, exist_ok=True)
    chart_path.write_bytes(_tiny_png_bytes())

    def fake_generate_charts_with_image(connection_arg, config_dir=None):
        return {
            "tickers_checked": 1,
            "charts_created": 1,
            "chart_paths": [chart_path],
            "chart_details": [
                {
                    "ticker": "AAA",
                    "company_name": "Example A",
                    "market": "S&P 500",
                    "chart_path": chart_path,
                    "trade_candidate": {
                        "ticker": "AAA",
                        "company_name": "Example A",
                        "market": "S&P 500",
                        "currency": "USD",
                        "signal_direction": "Bullish",
                        "signal_description": (
                            "7-day trend line crossed above 30-day trend line"
                        ),
                        "crossover_date": date(2026, 5, 4),
                        "days_since_crossover": "Today",
                        "latest_close_price": 124.5,
                        "review_levels": {
                            "50-day SMA": 120.0,
                            "20-day low": 118.2,
                            "20% trailing reference": 99.6,
                        },
                        "action_grade": "Strong Buy Setup",
                        "score": 7,
                        "max_score": 10,
                        "grade_reasons": [
                            "Recent bullish crossover within 2 days.",
                            "Latest close is above the 50-day SMA.",
                        ],
                        "grade_cautions": [],
                        "risk_notes": [
                            "Close price is above the 50-day trend line.",
                            "Dividend risk flag present.",
                        ],
                    },
                    "signals": [
                        {
                            "direction": "Bullish",
                            "trend_description": (
                                "7-day trend line crossed above 30-day trend line"
                            ),
                            "crossover_date": date(2026, 5, 4),
                            "days_since_crossover": "Today",
                        }
                    ],
                }
            ],
            "skipped": {},
            "output_dir": chart_path.parent,
        }

    monkeypatch.setattr(
        pdf_report_module,
        "generate_charts",
        fake_generate_charts_with_image,
    )

    try:
        insert_pdf_report_data(connection)
        output_path = generate_pdf_report(
            connection,
            output_dir=output_dir,
            report_date=date(2026, 5, 4),
        )
    finally:
        connection.close()

    assert output_path.exists()
    assert output_path.read_bytes().startswith(b"%PDF")
    assert output_path.stat().st_size > 1000


def test_pdf_keeps_each_selected_stock_to_one_page(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The index plus two selected stocks should produce exactly three pages."""
    connection = open_test_database(tmp_path)
    output_dir = tmp_path / "outputs" / "pdf"
    chart_dir = tmp_path / "charts"
    first_chart = chart_dir / "AAA_price_trend.png"
    second_chart = chart_dir / "BBB_price_trend.png"
    chart_dir.mkdir(parents=True, exist_ok=True)
    first_chart.write_bytes(_tiny_png_bytes())
    second_chart.write_bytes(_tiny_png_bytes())

    def fake_generate_two_charts(connection_arg, config_dir=None):
        return {
            "tickers_checked": 2,
            "charts_created": 2,
            "chart_paths": [first_chart, second_chart],
            "chart_details": [
                sample_chart_detail("AAA", first_chart),
                sample_chart_detail(
                    "BBB",
                    second_chart,
                    action_grade="Strong Sell Setup",
                    score=9,
                ),
            ],
            "skipped": {},
            "output_dir": chart_dir,
        }

    monkeypatch.setattr(
        pdf_report_module,
        "generate_charts",
        fake_generate_two_charts,
    )

    try:
        output_path = generate_pdf_report(
            connection,
            output_dir=output_dir,
            report_date=date(2026, 5, 4),
        )
    finally:
        connection.close()

    assert _pdf_page_count(output_path) == 3


def test_pdf_uses_landscape_page_size() -> None:
    """PDF reports should use landscape orientation."""
    width, height = LANDSCAPE_PAGE_SIZE

    assert width > height


def test_chart_flowables_put_each_chart_on_its_own_page(tmp_path: Path) -> None:
    """Chart pages should be separated by page breaks."""
    first_chart = tmp_path / "AAA_price_trend.png"
    second_chart = tmp_path / "BBB_price_trend.png"
    first_chart.write_bytes(_tiny_png_bytes())
    second_chart.write_bytes(_tiny_png_bytes())
    styles = pdf_report_module.getSampleStyleSheet()

    flowables = _chart_flowables(
        [
            sample_chart_detail("AAA", first_chart),
            sample_chart_detail("BBB", second_chart, action_grade="Strong Sell Setup"),
        ],
        styles,
    )
    page_breaks = [
        flowable for flowable in flowables
        if isinstance(flowable, pdf_report_module.PageBreak)
    ]

    assert len(page_breaks) == 2


def test_chart_flowables_include_chart_and_candidate_card(tmp_path: Path) -> None:
    """One chart page should include both the chart image and candidate card."""
    chart_path = tmp_path / "AAA_price_trend.png"
    chart_path.write_bytes(_tiny_png_bytes())
    styles = pdf_report_module.getSampleStyleSheet()

    flowables = _chart_flowables(
        [sample_chart_detail("AAA", chart_path)],
        styles,
    )
    keep_together = flowables[1]

    assert any(
        isinstance(item, pdf_report_module.Image)
        for item in keep_together._content
    )
    assert any(
        isinstance(item, pdf_report_module.Table)
        for item in keep_together._content
    )
    assert "Action grade: Strong Buy Setup" in _flowable_text(keep_together)
    assert "AAA — AAA Example Holdings PLC — S&P 500" in _flowable_text(keep_together)
    assert "Market: S&P 500" in _flowable_text(keep_together)
    assert "Planning reference levels" in _flowable_text(keep_together)
    assert "These are not trading instructions." in _flowable_text(keep_together)


def test_candidate_card_text_appears_in_pdf_flowable(tmp_path: Path) -> None:
    """Candidate card text should be present in the PDF page content model."""
    styles = pdf_report_module.getSampleStyleSheet()
    chart_detail = sample_chart_detail("AAA", tmp_path / "AAA.png")

    card = _candidate_card_flowable(chart_detail, styles)
    card_text = _flowable_text(card)

    assert "Candidate review" in card_text
    assert "Action grade: Strong Buy Setup" in card_text
    assert "Market: S&P 500" in card_text
    assert "Score: 8 / 10" in card_text
    assert "Why" in card_text
    assert "Caution" in card_text
    assert "Bullish" in card_text
    assert "7-day trend line crossed above 30-day trend line" in card_text
    assert "Crossover date: 2026-05-04" in card_text
    assert "USD 124.50" in card_text
    assert "20-day low USD 118.20" in card_text
    assert "Close price is above the 50-day trend line" in card_text
    assert len(card._cellvalues) <= 5


def test_chart_flowable_uses_larger_pdf_chart_dimensions(tmp_path: Path) -> None:
    """Stock pages should reserve most of the page for the chart image."""
    chart_path = tmp_path / "AAA_price_trend.png"
    chart_path.write_bytes(_tiny_png_bytes())
    styles = pdf_report_module.getSampleStyleSheet()

    flowables = _chart_flowables([sample_chart_detail("AAA", chart_path)], styles)
    keep_together = flowables[1]
    image = next(
        item for item in keep_together._content
        if isinstance(item, pdf_report_module.Image)
    )

    assert image.drawWidth == PDF_CHART_WIDTH
    assert image.drawHeight == PDF_CHART_HEIGHT


def test_candidate_card_wraps_long_text_in_readable_paragraphs(tmp_path: Path) -> None:
    """Long candidate details should live in wrapped paragraph cells."""
    styles = pdf_report_module.getSampleStyleSheet()
    chart_detail = sample_chart_detail("LONG", tmp_path / "LONG.png")
    chart_detail["trade_candidate"]["grade_reasons"] = [
        "Recent bullish crossover within 2 days with a very long explanatory phrase",
        "Latest close is above the 50-day SMA with further context",
        "No dividend risk flag present after the latest dividend metrics refresh",
    ]
    chart_detail["trade_candidate"]["grade_cautions"] = [
        "Stop distance 13.2% is wider than the planning threshold",
    ]

    card = _candidate_card_flowable(chart_detail, styles)
    card_text = _flowable_text(card)

    assert "Recent bullish crossover within 2 days" in card_text
    assert "Stop distance 13.2%" in card_text
    assert all(
        isinstance(row[1], pdf_report_module.Paragraph)
        for row in card._cellvalues
    )


def test_candidate_card_formats_missing_values_as_not_available() -> None:
    """Missing reference values should be shown as Not available."""
    text = _review_levels_text(
        {
            "currency": "USD",
            "review_levels": {
                "50-day SMA": None,
                "20-day low": 118.2,
            },
        }
    )

    assert "50-day SMA Not available" in text
    assert "20-day low USD 118.20" in text


def test_pdf_index_page_is_generated_before_chart_pages(tmp_path: Path) -> None:
    """The PDF should start with a report index page."""
    styles = pdf_report_module.getSampleStyleSheet()
    chart_path = tmp_path / "AAA_price_trend.png"
    chart_path.write_bytes(_tiny_png_bytes())
    flowables = _index_page_flowables(
        [sample_chart_detail("AAA", chart_path)],
        date(2026, 5, 4),
        styles,
    )

    assert flowables[0].text == "Market Sentinel Crossover Chart Report"
    assert "2026-05-04" in flowables[1].text
    assert "Stocks shown below" in flowables[3].text
    assert "Action grade" in _flowable_text(flowables)
    assert "Market" in _flowable_text(flowables)


def test_pdf_index_rows_split_into_two_groups_of_25(tmp_path: Path) -> None:
    """Selected stocks should be split into two index groups of up to 25."""
    chart_details = [
        sample_chart_detail(f"T{index:02d}", tmp_path / f"T{index:02d}.png")
        for index in range(1, 51)
    ]

    left_rows = _index_rows(chart_details[:25])
    right_rows = _index_rows(chart_details[25:50])

    assert len(left_rows) == 25
    assert len(right_rows) == 25
    assert left_rows[0][0] == "T01"
    assert right_rows[0][0] == "T26"


def test_pdf_index_uses_same_order_as_chart_pages(tmp_path: Path) -> None:
    """The index should keep the same selected stock order as chart pages."""
    chart_details = [
        sample_chart_detail("NEW", tmp_path / "NEW.png"),
        sample_chart_detail("BUL", tmp_path / "BUL.png"),
        sample_chart_detail("OLD", tmp_path / "OLD.png"),
    ]

    rows = _index_rows(chart_details)

    assert [row[0] for row in rows] == ["NEW", "BUL", "OLD"]


def test_pdf_pages_sort_by_action_grade(tmp_path: Path) -> None:
    """PDF chart pages should include only strong grades in market sort order."""
    chart_details = [
        sample_chart_detail(
            "SELL",
            tmp_path / "SELL.png",
            action_grade="Sell Setup",
            score=4,
            crossover_date=date(2026, 5, 5),
        ),
        sample_chart_detail(
            "BUY",
            tmp_path / "BUY.png",
            action_grade="Buy Setup",
            score=5,
            crossover_date=date(2026, 5, 2),
        ),
        sample_chart_detail(
            "STRB",
            tmp_path / "STRB.png",
            action_grade="Strong Buy Setup",
            score=6,
            crossover_date=date(2026, 5, 1),
        ),
        sample_chart_detail(
            "TRK",
            tmp_path / "TRK.png",
            action_grade="Track Only",
            score=2,
            crossover_date=date(2026, 5, 4),
        ),
        sample_chart_detail(
            "STRS",
            tmp_path / "STRS.png",
            action_grade="Strong Sell Setup",
            score=8,
            crossover_date=date(2026, 5, 5),
        ),
    ]

    sorted_details = _sorted_chart_details(chart_details)

    assert [detail["ticker"] for detail in sorted_details] == [
        "STRS",
        "STRB",
    ]


def test_pdf_filter_includes_strong_buy_and_strong_sell(tmp_path: Path) -> None:
    """Only strong buy and strong sell setup charts should be included."""
    chart_details = [
        sample_chart_detail("STRB", tmp_path / "STRB.png", "Strong Buy Setup"),
        sample_chart_detail("STRS", tmp_path / "STRS.png", "Strong Sell Setup"),
        sample_chart_detail("BUY", tmp_path / "BUY.png", "Buy Setup"),
        sample_chart_detail("SELL", tmp_path / "SELL.png", "Sell Setup"),
        sample_chart_detail("TRK", tmp_path / "TRK.png", "Track Only"),
    ]

    included = _included_chart_details(chart_details, {})

    assert [detail["ticker"] for detail in included] == ["STRB", "STRS"]


def test_pdf_index_only_lists_included_grades(tmp_path: Path) -> None:
    """The first-page index should list only stocks included in chart pages."""
    chart_details = [
        sample_chart_detail("STRB", tmp_path / "STRB.png", "Strong Buy Setup"),
        sample_chart_detail("BUY", tmp_path / "BUY.png", "Buy Setup"),
        sample_chart_detail("STRS", tmp_path / "STRS.png", "Strong Sell Setup"),
    ]
    included = _included_chart_details(chart_details, {})
    rows = _index_rows(included)

    assert [row[0] for row in rows] == ["STRB", "STRS"]
    assert {row[2] for row in rows} == {"S&P 500"}
    assert {row[3] for row in rows} == {"Strong Buy Setup", "Strong Sell Setup"}


def test_pdf_selection_limits_each_market_when_both_have_enough(
    tmp_path: Path,
) -> None:
    """Balanced PDF selection should cap each major market when both qualify."""
    chart_details = [
        sample_chart_detail(
            f"SP{index:02d}",
            tmp_path / f"SP{index:02d}.png",
            score=40 - index,
            market="S&P 500",
        )
        for index in range(30)
    ] + [
        sample_chart_detail(
            f"FT{index:02d}.L",
            tmp_path / f"FT{index:02d}.png",
            score=40 - index,
            market="FTSE 350",
        )
        for index in range(30)
    ]

    included = _included_chart_details(
        chart_details,
        {
            "pdf_max_charts_total": 50,
            "pdf_max_charts_per_market": 25,
        },
    )

    assert len(included) == 50
    assert _market_counts(included) == {"S&P 500": 25, "FTSE 350": 25}


def test_pdf_selection_allows_spillover_when_market_has_unused_slots(
    tmp_path: Path,
) -> None:
    """One market can fill unused chart slots when the other has fewer names."""
    chart_details = [
        sample_chart_detail(
            f"SP{index:02d}",
            tmp_path / f"SP{index:02d}.png",
            score=70 - index,
            market="S&P 500",
        )
        for index in range(40)
    ] + [
        sample_chart_detail(
            f"FT{index:02d}.L",
            tmp_path / f"FT{index:02d}.png",
            score=70 - index,
            market="FTSE 350",
        )
        for index in range(10)
    ]

    included = _included_chart_details(
        chart_details,
        {
            "pdf_max_charts_total": 50,
            "pdf_max_charts_per_market": 25,
        },
    )

    assert len(included) == 50
    assert _market_counts(included) == {"S&P 500": 40, "FTSE 350": 10}


def test_pdf_selection_sorts_by_score_within_market(tmp_path: Path) -> None:
    """Within each market, higher scores should appear first."""
    chart_details = [
        sample_chart_detail("LOW", tmp_path / "LOW.png", score=7, market="S&P 500"),
        sample_chart_detail("HIGH", tmp_path / "HIGH.png", score=9, market="S&P 500"),
        sample_chart_detail("MID", tmp_path / "MID.png", score=8, market="S&P 500"),
        sample_chart_detail("FTH.L", tmp_path / "FTH.png", score=9, market="FTSE 350"),
        sample_chart_detail("FTL.L", tmp_path / "FTL.png", score=6, market="FTSE 350"),
    ]

    included = _included_chart_details(chart_details, {})

    assert [detail["ticker"] for detail in included[:3]] == ["HIGH", "MID", "LOW"]
    assert [detail["ticker"] for detail in included[3:]] == ["FTH.L", "FTL.L"]


def test_pdf_selection_prefers_strong_buy_when_score_is_equal(
    tmp_path: Path,
) -> None:
    """Strong buy should come before strong sell for equal-score market peers."""
    chart_details = [
        sample_chart_detail(
            "SELL",
            tmp_path / "SELL.png",
            action_grade="Strong Sell Setup",
            score=8,
            market="S&P 500",
        ),
        sample_chart_detail(
            "BUY",
            tmp_path / "BUY.png",
            action_grade="Strong Buy Setup",
            score=8,
            market="S&P 500",
        ),
    ]

    included = _included_chart_details(chart_details, {})

    assert [detail["ticker"] for detail in included] == ["BUY", "SELL"]


def test_pdf_index_includes_market_count_summary(tmp_path: Path) -> None:
    """The PDF index should show a compact count by market."""
    styles = pdf_report_module.getSampleStyleSheet()
    chart_details = [
        sample_chart_detail("AAA", tmp_path / "AAA.png", market="S&P 500"),
        sample_chart_detail("BBB.L", tmp_path / "BBB.png", market="FTSE 350"),
        sample_chart_detail("CCC.L", tmp_path / "CCC.png", market="FTSE 350"),
    ]

    flowables = _index_page_flowables(chart_details, date(2026, 5, 4), styles)

    assert "Included: S&P 500: 1 | FTSE 350: 2" in _flowable_text(flowables)


def test_pdf_index_order_matches_market_balanced_chart_page_order(
    tmp_path: Path,
) -> None:
    """The first-page index should match the selected chart page order exactly."""
    chart_details = [
        sample_chart_detail("SPLOW", tmp_path / "SPLOW.png", score=7, market="S&P 500"),
        sample_chart_detail("SPHIGH", tmp_path / "SPHIGH.png", score=9, market="S&P 500"),
        sample_chart_detail("FTLOW.L", tmp_path / "FTLOW.png", score=6, market="FTSE 350"),
        sample_chart_detail("FTHIGH.L", tmp_path / "FTHIGH.png", score=8, market="FTSE 350"),
    ]
    included = _included_chart_details(chart_details, {})
    rows = _index_rows(included)

    assert [row[0] for row in rows] == [detail["ticker"] for detail in included]
    assert [row[0] for row in rows] == ["SPHIGH", "SPLOW", "FTHIGH.L", "FTLOW.L"]


def test_pdf_empty_state_message_when_no_strong_setups(tmp_path: Path) -> None:
    """The index should show a friendly message when no strong setups exist."""
    styles = pdf_report_module.getSampleStyleSheet()
    chart_details = [
        sample_chart_detail("BUY", tmp_path / "BUY.png", "Buy Setup"),
        sample_chart_detail("SELL", tmp_path / "SELL.png", "Sell Setup"),
        sample_chart_detail("TRK", tmp_path / "TRK.png", "Track Only"),
    ]
    included = _included_chart_details(chart_details, {})

    flowables = _index_page_flowables(included, date(2026, 5, 4), styles)
    chart_flowables = _chart_flowables(included, styles)

    assert (
        "No strong buy or strong sell setups were found for this report period."
        in _flowable_text(flowables)
    )
    assert chart_flowables == []


def test_pdf_selection_reason_text_includes_all_recent_signals() -> None:
    """Each chart page should explain why the stock was selected."""
    reason_text = _selection_reason_text(
        {
            "signals": [
                {
                    "trend_description": (
                        "7-day trend line crossed above 30-day trend line"
                    ),
                    "crossover_date": date(2026, 5, 4),
                    "days_since_crossover": "Today",
                },
                {
                    "trend_description": (
                        "30-day trend line crossed above 50-day trend line"
                    ),
                    "crossover_date": date(2026, 5, 2),
                    "days_since_crossover": "2 days ago",
                },
            ]
        }
    )

    assert (
        "Selected because: 7-day trend line crossed above 30-day trend line "
        "on 2026-05-04"
    ) in reason_text
    assert "2 days ago" in reason_text


def fake_generate_charts_without_images(connection, config_dir=None):
    """Return an empty chart summary for PDF tests that do not inspect images."""
    return {
        "tickers_checked": 0,
        "charts_created": 0,
        "chart_paths": [],
        "chart_details": [],
        "skipped": {},
        "output_dir": Path("charts"),
    }


def sample_chart_detail(
    ticker: str,
    chart_path: Path,
    action_grade: str = "Strong Buy Setup",
    score: int = 8,
    crossover_date: date = date(2026, 5, 4),
    market: str = "S&P 500",
):
    """Return one sample chart detail for PDF index tests."""
    return {
        "ticker": ticker,
        "company_name": f"{ticker} Example Holdings PLC",
        "market": market,
        "chart_path": chart_path,
        "trade_candidate": {
            "ticker": ticker,
            "company_name": f"{ticker} Example Holdings PLC",
            "market": market,
            "currency": "USD",
            "signal_direction": "Bullish",
            "signal_description": "7-day trend line crossed above 30-day trend line",
            "crossover_date": crossover_date,
            "days_since_crossover": "Today",
            "latest_close_price": 124.5,
            "review_levels": {
                "50-day SMA": 120.0,
                "20-day low": 118.2,
                "20% trailing reference": 99.6,
            },
            "action_grade": action_grade,
            "score": score,
            "max_score": 10,
            "grade_reasons": [
                "Recent bullish crossover within 2 days.",
                "Latest close is above the 50-day SMA.",
            ],
            "grade_cautions": [],
            "risk_notes": [
                "Close price is above the 50-day trend line.",
                "No dividend risk flag.",
            ],
        },
        "signals": [
            {
                "direction": "Bullish",
                "trend_description": "7-day trend line crossed above 30-day trend line",
                "crossover_date": crossover_date,
                "days_since_crossover": "Today",
            }
        ],
    }


def _flowable_text(flowable) -> str:
    """Collect text from simple reportlab flowables used in PDF tests."""
    if isinstance(flowable, str):
        return flowable

    if hasattr(flowable, "text"):
        return flowable.text

    if hasattr(flowable, "_content"):
        return " ".join(_flowable_text(item) for item in flowable._content)

    if hasattr(flowable, "_cellvalues"):
        return " ".join(
            _flowable_text(cell)
            for row in flowable._cellvalues
            for cell in row
        )

    if isinstance(flowable, list):
        return " ".join(_flowable_text(item) for item in flowable)

    return ""


def _tiny_png_bytes() -> bytes:
    """Return a valid tiny PNG image."""
    return base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADUlEQVR42mNk"
        "+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
    )


def _pdf_page_count(pdf_path: Path) -> int:
    """Count PDF page objects without adding a PDF parser dependency."""
    return len(re.findall(rb"/Type\s*/Page\b", pdf_path.read_bytes()))


def _market_counts(chart_details):
    """Return selected PDF chart counts by market."""
    counts = {}
    for detail in chart_details:
        market = detail["market"]
        counts[market] = counts.get(market, 0) + 1
    return counts
