"""Tests for PDF report generation."""

import base64
from datetime import date
from pathlib import Path

from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema
from market_sentinel.reports import pdf_report as pdf_report_module
from market_sentinel.reports.pdf_report import (
    LANDSCAPE_PAGE_SIZE,
    _chart_flowables,
    _index_page_flowables,
    _index_rows,
    _selection_reason_text,
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
                    "signals": [
                        {
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
        {
            "chart_details": [
                {
                    "ticker": "AAA",
                    "company_name": "Example A",
                    "market": "S&P 500",
                    "chart_path": first_chart,
                    "signals": [],
                },
                {
                    "ticker": "BBB",
                    "company_name": "Example B",
                    "market": "S&P 500",
                    "chart_path": second_chart,
                    "signals": [],
                },
            ]
        },
        styles,
    )
    page_breaks = [
        flowable for flowable in flowables
        if isinstance(flowable, pdf_report_module.PageBreak)
    ]

    assert len(page_breaks) == 2


def test_pdf_index_page_is_generated_before_chart_pages(tmp_path: Path) -> None:
    """The PDF should start with a report index page."""
    styles = pdf_report_module.getSampleStyleSheet()
    chart_path = tmp_path / "AAA_price_trend.png"
    chart_path.write_bytes(_tiny_png_bytes())
    chart_summary = {"chart_details": [sample_chart_detail("AAA", chart_path)]}

    flowables = _index_page_flowables(chart_summary, date(2026, 5, 4), styles)

    assert flowables[0].text == "Market Sentinel Crossover Chart Report"
    assert "2026-05-04" in flowables[1].text
    assert "Stocks shown below" in flowables[3].text


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


def sample_chart_detail(ticker: str, chart_path: Path):
    """Return one sample chart detail for PDF index tests."""
    return {
        "ticker": ticker,
        "company_name": f"{ticker} Example Holdings PLC",
        "market": "S&P 500",
        "chart_path": chart_path,
        "signals": [
            {
                "direction": "Bullish",
                "trend_description": "7-day trend line crossed above 30-day trend line",
                "crossover_date": date(2026, 5, 4),
                "days_since_crossover": "Today",
            }
        ],
    }


def _tiny_png_bytes() -> bytes:
    """Return a valid tiny PNG image."""
    return base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADUlEQVR42mNk"
        "+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
    )
