"""Tests for chart generation."""

from datetime import date
from pathlib import Path

from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema
from scripts import generate_charts as generate_charts_script
from market_sentinel.reports import charts as charts_module
from market_sentinel.reports.charts import generate_charts


def write_chart_config(
    config_dir: Path,
    database_path: Path,
    chart_dir: Path,
    max_tickers: int = 25,
    include_chart_defaults: bool = True,
) -> None:
    """Create test settings and watchlist config files."""
    config_dir.mkdir(parents=True, exist_ok=True)
    settings_lines = [
        f"database_path: {database_path}",
        f"chart_max_tickers: {max_tickers}",
        "crossover_recent_days: 7",
    ]

    if include_chart_defaults:
        settings_lines.extend(
            [
                "chart_lookback_days: 180",
                "chart_show_close_price: true",
                "chart_close_price_style: dotted",
                "chart_close_price_color: black",
                "chart_close_price_linewidth: 1",
                "chart_show_crossover_marker: false",
                "chart_show_20_day_reference: false",
                "chart_show_possible_flag_pattern: true",
                "chart_show_200_day_sma: false",
                "chart_include_sma_periods:",
                "  - 7",
                "  - 30",
                "  - 50",
                "pdf_include_setup_grades:",
                "  - Strong Buy Setup",
                "  - Buy Setup",
                "  - Track Only",
                "  - Sell Setup",
                "  - Strong Sell Setup",
            ]
        )

    settings_lines.extend(
        [
            "report_outputs:",
            f"  chart_dir: {chart_dir}",
        ]
    )
    config_dir.joinpath("settings.yaml").write_text(
        "\n".join(settings_lines),
        encoding="utf-8",
    )
    config_dir.joinpath("watchlist.yaml").write_text(
        "\n".join(
            [
                "watchlist:",
                "  sp_500:",
                "    - DDD",
            ]
        ),
        encoding="utf-8",
    )


def open_test_database(tmp_path: Path, max_tickers: int = 25):
    """Open a temporary DuckDB database with chart settings."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    chart_dir = tmp_path / "charts"
    write_chart_config(config_dir, database_path, chart_dir, max_tickers=max_tickers)
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    return connection, config_dir, chart_dir


def insert_chart_security(connection, security_id: int, ticker: str) -> None:
    """Insert one fake security."""
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
            security_id,
            ticker,
            f"{ticker} Example",
            "S&P 500",
            "US",
            "USD",
            "Technology",
        ],
    )


def insert_chart_prices_and_smas(connection, security_id: int) -> None:
    """Insert fake price and moving-average history."""
    for index in range(1, 6):
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
            [
                security_id * 100 + index,
                security_id,
                f"2026-05-{index:02d}",
                100.0 + index,
                101.0 + index,
                99.0 + index,
                100.0 + index,
                100.0 + index,
                1000,
            ],
        )

    signal_id = security_id * 1000
    for period in [7, 30, 50, 200]:
        for index in range(1, 6):
            signal_id += 1
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
                    signal_id,
                    security_id,
                    f"2026-05-{index:02d}",
                    period,
                    100.0 + index + (period / 100.0),
                    "SMA",
                ],
            )


def insert_chart_selection_data(connection) -> None:
    """Insert fake rows that exercise chart ticker selection."""
    for security_id, ticker in [(1, "AAA"), (2, "BBB"), (3, "CCC"), (4, "DDD")]:
        insert_chart_security(connection, security_id, ticker)
        insert_chart_prices_and_smas(connection, security_id)

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
        [9991, 1, "2026-05-05", 7, 105.0, 30, 103.0, "BULLISH_CROSSOVER", "BULLISH"],
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
            9992,
            2,
            "2026-05-05",
            7,
            95.0,
            30,
            103.0,
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
            9993,
            3,
            "2026-05-03",
            7,
            105.0,
            30,
            103.0,
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
            9994,
            1,
            "2026-05-04",
            30,
            106.0,
            50,
            104.0,
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
            annual_dividend_cash_per_10000
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [1, 2, "2026-05-05", 2.0, 0.08, 800.0],
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
        [2, 3, "2026-05-05", 2.0, 0.07, 700.0, "DIVIDEND_TRAP_RISK", "High yield"],
    )


def test_generate_charts_creates_png_files_for_selected_tickers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Chart generation should create PNG files for selected tickers."""
    connection, config_dir, chart_dir = open_test_database(tmp_path)

    monkeypatch.setattr(charts_module, "_write_chart_image", fake_chart_writer)

    try:
        insert_chart_selection_data(connection)
        summary = generate_charts(connection, config_dir=config_dir)
    finally:
        connection.close()

    assert summary["charts_created"] == 3
    assert summary["skipped"] == {}
    assert chart_dir.exists()

    for ticker in ["AAA", "BBB", "CCC"]:
        chart_path = chart_dir / f"{ticker}_price_trend.png"
        assert chart_path.exists()
        assert chart_path.stat().st_size > 0
    assert not (chart_dir / "DDD_price_trend.png").exists()


def test_generate_charts_respects_max_ticker_setting(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Chart generation should avoid creating too many charts by default."""
    connection, config_dir, chart_dir = open_test_database(tmp_path, max_tickers=2)

    monkeypatch.setattr(charts_module, "_write_chart_image", fake_chart_writer)

    try:
        insert_chart_selection_data(connection)
        summary = generate_charts(connection, config_dir=config_dir)
    finally:
        connection.close()

    assert summary["tickers_checked"] == 2
    assert summary["charts_created"] == 2
    assert (chart_dir / "AAA_price_trend.png").exists()
    assert (chart_dir / "BBB_price_trend.png").exists()
    assert not (chart_dir / "CCC_price_trend.png").exists()


def test_generate_charts_prioritises_recent_crossover_tickers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Chart generation should start with tickers that crossed recently."""
    connection, config_dir, chart_dir = open_test_database(tmp_path, max_tickers=1)

    monkeypatch.setattr(charts_module, "_write_chart_image", fake_chart_writer)

    try:
        insert_chart_selection_data(connection)
        summary = generate_charts(connection, config_dir=config_dir)
    finally:
        connection.close()

    assert summary["tickers_checked"] == 1
    assert (chart_dir / "AAA_price_trend.png").exists()
    assert not (chart_dir / "BBB_price_trend.png").exists()


def test_generate_charts_sorts_and_deduplicates_crossover_tickers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Charts should sort by action grade and avoid duplicate ticker pages."""
    connection, config_dir, _chart_dir = open_test_database(tmp_path)

    monkeypatch.setattr(charts_module, "_write_chart_image", fake_chart_writer)

    try:
        insert_chart_selection_data(connection)
        summary = generate_charts(connection, config_dir=config_dir)
    finally:
        connection.close()

    assert [detail["ticker"] for detail in summary["chart_details"]] == [
        "AAA",
        "CCC",
        "BBB",
    ]
    assert len(summary["chart_details"][0]["signals"]) == 2
    assert summary["chart_details"][0]["signals"][0]["direction"] == "Bullish"
    assert summary["chart_details"][2]["signals"][0]["direction"] == "Bearish"
    assert summary["chart_details"][0]["trade_candidate"]["ticker"] == "AAA"
    assert summary["chart_details"][0]["market"] == "S&P 500"
    assert (
        summary["chart_details"][0]["trade_candidate"]["review_levels"]["50-day SMA"]
        == 105.5
    )
    assert (
        "No dividend risk flag."
        in summary["chart_details"][0]["trade_candidate"]["risk_notes"]
    )


def test_generate_charts_limits_selected_tickers_to_fifty(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Chart selection should cap the PDF-oriented chart set at 50 by default."""
    connection, config_dir, _chart_dir = open_test_database(tmp_path, max_tickers=50)

    monkeypatch.setattr(charts_module, "_write_chart_image", fake_chart_writer)

    try:
        for security_id in range(1, 61):
            ticker = f"T{security_id:02d}"
            insert_chart_security(connection, security_id, ticker)
            insert_chart_prices_and_smas(connection, security_id)
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
                    90000 + security_id,
                    security_id,
                    "2026-05-05",
                    7,
                    105.0,
                    30,
                    100.0,
                    "BULLISH_CROSSOVER",
                    "BULLISH_CROSSOVER",
                ],
            )
        summary = generate_charts(connection, config_dir=config_dir)
    finally:
        connection.close()

    assert summary["tickers_checked"] == 50
    assert summary["charts_created"] == 50


def test_generate_charts_filters_to_pdf_setup_grades_by_default(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Chart generation should skip candidates not selected for the PDF."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    chart_dir = tmp_path / "charts"
    write_chart_config(
        config_dir,
        database_path,
        chart_dir,
        include_chart_defaults=False,
    )
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    monkeypatch.setattr(charts_module, "_write_chart_image", fake_chart_writer)

    try:
        insert_chart_selection_data(connection)
        summary = generate_charts(connection, config_dir=config_dir)
    finally:
        connection.close()

    assert summary["charts_created"] == 1
    assert summary["chart_details"][0]["ticker"] == "BBB"
    assert summary["chart_details"][0]["trade_candidate"]["action_grade"] in {
        "Strong Buy Setup",
        "Strong Sell Setup",
    }


def test_generate_charts_uses_simple_defaults(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Default charts should use 180 days and only SMA 7, 30, and 50."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    chart_dir = tmp_path / "charts"
    write_chart_config(
        config_dir,
        database_path,
        chart_dir,
        include_chart_defaults=False,
    )
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    observed = {}

    def fake_fetch_chart_data(connection_arg, ticker, lookback_days, sma_periods):
        observed["lookback_days"] = lookback_days
        observed["sma_periods"] = list(sma_periods)
        return {
            "prices": [("2026-05-01", 100.0)],
            "moving_averages": {},
            "company_name": "Example A",
            "market": "FTSE 350",
            "currency": "USD",
        }

    def fake_writer(
        ticker,
        chart_data,
        output_path,
        show_close_price,
        sma_periods,
        close_price_style,
        close_price_color,
        close_price_linewidth,
    ):
        observed["show_close_price"] = show_close_price
        observed["writer_sma_periods"] = list(sma_periods)
        observed["close_price_style"] = close_price_style
        observed["close_price_color"] = close_price_color
        observed["close_price_linewidth"] = close_price_linewidth
        observed["show_crossover_marker"] = chart_data.get("show_crossover_marker")
        observed["show_20_day_reference"] = chart_data.get("show_20_day_reference")
        output_path.write_bytes(b"fake chart")

    monkeypatch.setattr(charts_module, "_fetch_chart_data", fake_fetch_chart_data)
    monkeypatch.setattr(charts_module, "_write_chart_image", fake_writer)

    try:
        summary = generate_charts(
            connection,
            config_dir=config_dir,
            tickers=["AAA"],
        )
    finally:
        connection.close()

    assert summary["charts_created"] == 1
    assert observed["lookback_days"] == 180
    assert observed["sma_periods"] == [7, 30, 50]
    assert observed["writer_sma_periods"] == [7, 30, 50]
    assert observed["show_close_price"] is True
    assert observed["close_price_style"] == "dotted"
    assert observed["close_price_color"] == "black"
    assert observed["close_price_linewidth"] == 1.0
    assert observed["show_crossover_marker"] is False
    assert observed["show_20_day_reference"] is False


def test_generate_charts_passes_selected_crossover_date_to_writer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Chart generation should pass the selected candidate signal into the writer."""
    connection, config_dir, _chart_dir = open_test_database(tmp_path)
    observed = {}

    def recording_writer(
        ticker,
        chart_data,
        output_path,
        show_close_price,
        sma_periods,
        close_price_style,
        close_price_color,
        close_price_linewidth,
    ):
        observed[ticker] = {
            "candidate_crossover_date": chart_data["trade_candidate"][
                "crossover_date"
            ],
            "signal_crossover_date": chart_data["candidate_signal"][
                "crossover_date"
            ],
        }
        output_path.write_bytes(b"fake chart")

    monkeypatch.setattr(charts_module, "_write_chart_image", recording_writer)

    try:
        insert_chart_selection_data(connection)
        generate_charts(connection, config_dir=config_dir)
    finally:
        connection.close()

    assert observed["AAA"]["candidate_crossover_date"] == date(2026, 5, 5)
    assert observed["AAA"]["signal_crossover_date"] == date(2026, 5, 5)


def test_force_generation_bypasses_cached_images(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """Force mode should regenerate chart images even when cache says current."""
    connection, config_dir, _chart_dir = open_test_database(tmp_path)
    write_calls = []

    def fake_build_trade_candidate(connection_arg, ticker, signal, config_dir=None):
        return {
            "ticker": ticker,
            "signal_direction": "Bullish",
            "action_grade": "Strong Buy Setup",
            "crossover_date": date(2026, 5, 2),
            "review_levels": {"20-day low": 98.5},
        }

    def fake_fetch_chart_data(connection_arg, ticker, lookback_days, sma_periods):
        return {
            "prices": [
                (date(2026, 5, 1), 100.0),
                (date(2026, 5, 2), 104.0),
            ],
            "moving_averages": {},
            "company_name": "Example A",
            "market": "S&P 500",
            "currency": "USD",
        }

    def fake_writer(
        ticker,
        chart_data,
        output_path,
        show_close_price,
        sma_periods,
        close_price_style,
        close_price_color,
        close_price_linewidth,
    ):
        write_calls.append(ticker)
        output_path.write_bytes(b"old chart")
        output_path.write_bytes(b"forced chart")

    monkeypatch.setattr(charts_module, "build_trade_candidate", fake_build_trade_candidate)
    monkeypatch.setattr(charts_module, "_fetch_chart_data", fake_fetch_chart_data)
    monkeypatch.setattr(charts_module, "_chart_is_current", lambda output_path, chart_data: True)
    monkeypatch.setattr(charts_module, "_write_chart_image", fake_writer)

    try:
        chart_dir = config_dir.parent / "charts"
        chart_dir.mkdir(parents=True, exist_ok=True)
        (chart_dir / "AAA_price_trend.png").write_bytes(b"cached chart")
        summary = generate_charts(
            connection,
            config_dir=config_dir,
            tickers=["AAA"],
            force=True,
        )
    finally:
        connection.close()

    captured = capsys.readouterr()
    assert summary["charts_reused"] == 0
    assert summary["charts_force_regenerated"] == 1
    assert write_calls == ["AAA"]
    assert "Force-regenerated chart:" in captured.out


def test_chart_cache_metadata_changes_when_chart_settings_change(tmp_path: Path) -> None:
    """Chart cache metadata should invalidate when visual settings change."""
    output_path = tmp_path / "AAA_price_trend.png"
    output_path.write_bytes(b"old chart")
    chart_data = {
        "prices": [(date(2026, 5, 2), 104.0)],
        "show_crossover_marker": True,
        "show_20_day_reference": True,
        "trade_candidate": {
            "signal_direction": "Bullish",
            "action_grade": "Strong Buy Setup",
            "crossover_date": date(2026, 5, 2),
            "review_levels": {"20-day low": 98.5},
        },
    }
    chart_data["chart_cache_key"] = charts_module._chart_cache_key(
        chart_data,
        True,
        [7, 30, 50],
        "dotted",
        "black",
        1.0,
    )
    charts_module._write_chart_cache_metadata(output_path, chart_data)

    changed_chart_data = {
        **chart_data,
        "show_crossover_marker": False,
    }
    changed_chart_data["chart_cache_key"] = charts_module._chart_cache_key(
        changed_chart_data,
        True,
        [7, 30, 50],
        "dotted",
        "black",
        1.0,
    )

    assert charts_module._chart_cache_metadata_matches(output_path, chart_data)
    assert not charts_module._chart_cache_metadata_matches(
        output_path,
        changed_chart_data,
    )


def test_generate_charts_script_parses_force_flag() -> None:
    """The chart script should expose a beginner-friendly cache bypass flag."""
    args = generate_charts_script.parse_args(["--force"])

    assert args.force is True


def test_crossover_marker_can_still_be_drawn_when_enabled() -> None:
    """Crossover markers should be optional rather than default chart clutter."""
    ax = RecordingAxes()
    chart_data = {
        "prices": [
            (date(2026, 5, 1), 100.0),
            (date(2026, 5, 2), 104.0),
            (date(2026, 5, 3), 106.0),
        ],
        "trade_candidate": {
            "signal_direction": "Bullish",
            "action_grade": "Strong Buy Setup",
            "crossover_date": date(2026, 5, 2),
            "review_levels": {"20-day low": 98.5},
        },
    }

    charts_module._add_crossover_marker(ax, chart_data)

    assert ax.markers[0]["marker"] == "^"
    assert ax.markers[0]["label"] == "Bullish crossover"
    assert ax.markers[0]["x"] == [date(2026, 5, 2)]
    assert ax.markers[0]["y"] == [104.0]


def test_20_day_reference_line_can_still_be_drawn_when_enabled() -> None:
    """20-day references should be optional rather than default chart clutter."""
    ax = RecordingAxes()
    chart_data = {
        "prices": [
            (date(2026, 5, 1), 106.0),
            (date(2026, 5, 2), 103.0),
            (date(2026, 5, 3), 100.0),
        ],
        "trade_candidate": {
            "signal_direction": "Bearish",
            "action_grade": "Strong Sell Setup",
            "crossover_date": date(2026, 5, 2),
            "review_levels": {"20-day high": 109.5},
        },
    }

    charts_module._add_20_day_reference_line(ax, chart_data)

    assert ax.lines == [
        {
            "value": 109.5,
            "label": "20-day high reference",
            "linestyle": "--",
        }
    ]


def test_default_chart_overlays_are_clean() -> None:
    """Crossover markers and 20-day references should be off by default."""
    assert charts_module.DEFAULT_SHOW_CROSSOVER_MARKER is False
    assert charts_module.DEFAULT_SHOW_20_DAY_REFERENCE is False
    assert charts_module.DEFAULT_SHOW_POSSIBLE_FLAG_PATTERN is True


def test_possible_flag_pattern_lines_are_drawn_when_detected() -> None:
    """Detected possible flag patterns should add two guide lines to the chart."""
    ax = RecordingAxes()
    chart_data = {
        "flag_pattern": {
            "upper_line": (
                (date(2026, 5, 1), 120.0),
                (date(2026, 5, 20), 117.0),
            ),
            "lower_line": (
                (date(2026, 5, 1), 112.0),
                (date(2026, 5, 20), 110.0),
            ),
        }
    }

    charts_module._add_possible_flag_pattern_lines(ax, chart_data)

    assert len(ax.plotted_lines) == 2
    assert ax.plotted_lines[0]["label"] == "Possible flag pattern"
    assert ax.plotted_lines[1]["label"] == "_nolegend_"


def test_200_day_sma_is_not_shown_by_default() -> None:
    """Default chart settings should not include the 200-day SMA."""
    assert charts_module._sma_periods_setting({}) == [7, 30, 50]
    assert charts_module._sma_periods_setting(
        {"chart_include_sma_periods": [7, 30, 50, 200]}
    ) == [7, 30, 50]


def test_chart_image_title_includes_market_marker() -> None:
    """Generated chart titles should show the market/index marker."""
    title = charts_module._chart_image_title(
        "BARC.L",
        {"company_name": "Barclays PLC", "market": "FTSE 350"},
    )

    assert title == "BARC.L — Barclays PLC — FTSE 350"


def fake_chart_writer(
    ticker: str,
    chart_data,
    output_path: Path,
    show_close_price: bool,
    sma_periods,
    close_price_style: str,
    close_price_color: str,
    close_price_linewidth: float,
) -> None:
    """Write a tiny fake PNG so tests do not need a graphics backend."""
    output_path.write_bytes(b"fake png data for " + ticker.encode("utf-8"))


class RecordingAxes:
    """Tiny matplotlib Axes stand-in for annotation tests."""

    def __init__(self):
        self.lines = []
        self.markers = []
        self.vertical_lines = []
        self.plotted_lines = []

    def axhline(self, value, **kwargs):
        self.lines.append(
            {
                "value": value,
                "label": kwargs.get("label"),
                "linestyle": kwargs.get("linestyle"),
            }
        )

    def scatter(self, x, y, **kwargs):
        self.markers.append(
            {
                "x": x,
                "y": y,
                "marker": kwargs.get("marker"),
                "label": kwargs.get("label"),
            }
        )

    def axvline(self, value, **kwargs):
        self.vertical_lines.append(
            {
                "value": value,
                "linestyle": kwargs.get("linestyle"),
            }
        )

    def plot(self, x, y, **kwargs):
        self.plotted_lines.append(
            {
                "x": x,
                "y": y,
                "label": kwargs.get("label"),
                "linestyle": kwargs.get("linestyle"),
            }
        )
