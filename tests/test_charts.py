"""Tests for chart generation."""

from pathlib import Path

from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema
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
                "chart_include_sma_periods:",
                "  - 7",
                "  - 30",
                "  - 50",
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
    """Charts should sort by latest crossover and avoid duplicate ticker pages."""
    connection, config_dir, _chart_dir = open_test_database(tmp_path)

    monkeypatch.setattr(charts_module, "_write_chart_image", fake_chart_writer)

    try:
        insert_chart_selection_data(connection)
        summary = generate_charts(connection, config_dir=config_dir)
    finally:
        connection.close()

    assert [detail["ticker"] for detail in summary["chart_details"]] == [
        "AAA",
        "BBB",
        "CCC",
    ]
    assert len(summary["chart_details"][0]["signals"]) == 2
    assert summary["chart_details"][0]["signals"][0]["direction"] == "Bullish"
    assert summary["chart_details"][1]["signals"][0]["direction"] == "Bearish"


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
