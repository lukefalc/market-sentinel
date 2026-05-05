"""Chart generation for market-sentinel reports.

This module creates simple PNG chart images from the local DuckDB database.
Charts are intended to be readable daily report companions, not a full web
dashboard.
"""

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import duckdb

from market_sentinel.analytics.crossovers import DEFAULT_CROSSOVER_RECENT_DAYS
from market_sentinel.analytics.crossovers import describe_crossover
from market_sentinel.analytics.crossovers import format_days_since_crossover
from market_sentinel.analytics.trade_candidates import build_trade_candidate
from market_sentinel.config.loader import load_named_config

DEFAULT_CHART_OUTPUT_DIR = Path("outputs") / "charts"
DEFAULT_CHART_LOOKBACK_DAYS = 180
DEFAULT_CHART_MAX_TICKERS = 50
DEFAULT_SHOW_CLOSE_PRICE = True
DEFAULT_CLOSE_PRICE_STYLE = "dotted"
DEFAULT_CLOSE_PRICE_COLOR = "black"
DEFAULT_CLOSE_PRICE_LINEWIDTH = 1.0
DEFAULT_MOVING_AVERAGE_PERIODS = [7, 30, 50]


def generate_charts(
    connection: duckdb.DuckDBPyConnection,
    output_dir: Optional[Path] = None,
    config_dir: Optional[Path] = None,
    tickers: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Generate price and moving-average charts for selected tickers.

    Args:
        connection: Open DuckDB connection.
        output_dir: Optional output folder. Tests can pass a temporary folder.
        config_dir: Optional folder containing settings YAML files.
        tickers: Optional explicit ticker list. If omitted, the module chooses a
            small daily-report set from recent crossover signals.
    """
    settings = _load_settings(config_dir)
    target_dir = _resolve_chart_output_dir(output_dir, settings)
    lookback_days = _positive_int_setting(
        settings,
        "chart_lookback_days",
        DEFAULT_CHART_LOOKBACK_DAYS,
    )
    max_tickers = _positive_int_setting(
        settings,
        "chart_max_tickers",
        DEFAULT_CHART_MAX_TICKERS,
    )
    crossover_recent_days = _positive_int_setting(
        settings,
        "crossover_recent_days",
        DEFAULT_CROSSOVER_RECENT_DAYS,
    )
    show_close_price = _bool_setting(
        settings,
        "chart_show_close_price",
        DEFAULT_SHOW_CLOSE_PRICE,
    )
    sma_periods = _sma_periods_setting(settings)
    close_price_style = str(
        settings.get("chart_close_price_style", DEFAULT_CLOSE_PRICE_STYLE)
    )
    close_price_color = str(
        settings.get("chart_close_price_color", DEFAULT_CLOSE_PRICE_COLOR)
    )
    close_price_linewidth = _positive_float_setting(
        settings,
        "chart_close_price_linewidth",
        DEFAULT_CLOSE_PRICE_LINEWIDTH,
    )

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise RuntimeError(
            "Could not create the chart output folder. Check that this path "
            f"exists or can be created: {target_dir}."
        ) from error

    if tickers is not None:
        selected_signals = _manual_chart_selections(tickers, max_tickers)
    else:
        selected_signals = _select_chart_signals(
            connection,
            crossover_recent_days,
            max_tickers,
        )
    selected_tickers = [selection["ticker"] for selection in selected_signals]

    print(f"Preparing charts for {len(selected_tickers)} ticker(s)")
    print(f"Chart folder: {target_dir}")

    generated_files: List[Path] = []
    chart_details: List[Dict[str, Any]] = []
    skipped: Dict[str, str] = {}

    for index, selection in enumerate(selected_signals, start=1):
        ticker = selection["ticker"]
        print(f"[{index}/{len(selected_tickers)}] Generating chart for {ticker}")
        try:
            chart_data = _fetch_chart_data(
                connection,
                ticker,
                lookback_days,
                sma_periods,
            )
        except duckdb.Error as error:
            skipped[ticker] = (
                "Could not read price or moving-average data from DuckDB. "
                f"Details: {error}"
            )
            print(f"Skipped {ticker}: {skipped[ticker]}")
            continue

        if not chart_data["prices"]:
            skipped[ticker] = "No price rows were found for this ticker."
            print(f"Skipped {ticker}: {skipped[ticker]}")
            continue

        output_path = target_dir / f"{_safe_filename(ticker)}_price_trend.png"
        _write_chart_image(
            ticker,
            chart_data,
            output_path,
            show_close_price,
            sma_periods,
            close_price_style,
            close_price_color,
            close_price_linewidth,
        )
        generated_files.append(output_path)
        chart_details.append(
            {
                **selection,
                "chart_path": output_path,
                "trade_candidate": build_trade_candidate(
                    connection,
                    ticker,
                    (selection.get("signals") or [None])[0],
                    config_dir=config_dir,
                ),
            }
        )
        print(f"Saved chart: {output_path}")

    print(
        "Chart generation complete: "
        f"{len(generated_files)} created, {len(skipped)} skipped"
    )
    chart_details = sorted(chart_details, key=_chart_detail_sort_key)

    return {
        "tickers_checked": len(selected_tickers),
        "charts_created": len(generated_files),
        "chart_paths": [detail["chart_path"] for detail in chart_details],
        "chart_details": chart_details,
        "skipped": skipped,
        "output_dir": target_dir,
    }


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
        "Buy Setup": 1,
        "Track Only": 2,
        "Sell Setup": 3,
        "Strong Sell Setup": 4,
    }
    return ranks.get(str(action_grade), 2)


def _load_settings(config_dir: Optional[Path]) -> Dict[str, Any]:
    """Load settings, falling back to defaults when settings are unavailable."""
    try:
        return load_named_config("settings", config_dir)
    except FileNotFoundError:
        return {}


def _resolve_chart_output_dir(
    output_dir: Optional[Path],
    settings: Dict[str, Any],
) -> Path:
    """Resolve the chart output folder from arguments, settings, or fallback."""
    if output_dir is not None:
        return Path(output_dir).expanduser()

    configured_dir = settings.get("report_outputs", {}).get("chart_dir")

    if configured_dir:
        return Path(str(configured_dir)).expanduser()

    return DEFAULT_CHART_OUTPUT_DIR


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


def _positive_float_setting(
    settings: Dict[str, Any],
    setting_name: str,
    default_value: float,
) -> float:
    """Read a positive float setting with a safe fallback."""
    raw_value = settings.get(setting_name, default_value)

    try:
        parsed_value = float(raw_value)
    except (TypeError, ValueError):
        return default_value

    if parsed_value <= 0:
        return default_value

    return parsed_value


def _bool_setting(
    settings: Dict[str, Any],
    setting_name: str,
    default_value: bool,
) -> bool:
    """Read a boolean setting with a safe fallback."""
    raw_value = settings.get(setting_name, default_value)

    if isinstance(raw_value, bool):
        return raw_value

    if isinstance(raw_value, str):
        return raw_value.strip().lower() in {"1", "true", "yes", "on"}

    return default_value


def _sma_periods_setting(settings: Dict[str, Any]) -> List[int]:
    """Read configured SMA periods for charts."""
    raw_periods = settings.get(
        "chart_include_sma_periods",
        DEFAULT_MOVING_AVERAGE_PERIODS,
    )

    if not isinstance(raw_periods, list):
        return DEFAULT_MOVING_AVERAGE_PERIODS

    periods: List[int] = []

    for raw_period in raw_periods:
        try:
            period = int(raw_period)
        except (TypeError, ValueError):
            continue

        if period > 0 and period not in periods:
            periods.append(period)

    return periods or DEFAULT_MOVING_AVERAGE_PERIODS


def _manual_chart_selections(
    tickers: Sequence[str],
    max_tickers: int,
) -> List[Dict[str, Any]]:
    """Build chart selections for explicitly supplied tickers."""
    return [
        {
            "ticker": ticker,
            "company_name": "",
            "market": "",
            "signals": [],
        }
        for ticker in _normalise_tickers(tickers)[:max_tickers]
    ]


def _select_chart_signals(
    connection: duckdb.DuckDBPyConnection,
    crossover_recent_days: int,
    max_tickers: int,
) -> List[Dict[str, Any]]:
    """Choose recent crossover chart selections, deduplicated by ticker."""
    latest_date = _latest_report_date(connection) or date.today()
    cutoff_date = latest_date - timedelta(days=crossover_recent_days)
    signals = _fetch_recent_crossover_signals(connection, cutoff_date, latest_date)
    selections_by_ticker: Dict[str, Dict[str, Any]] = {}

    for signal in signals:
        ticker = signal["ticker"]
        if ticker not in selections_by_ticker:
            selections_by_ticker[ticker] = {
                "ticker": ticker,
                "company_name": signal["company_name"],
                "market": signal["market"],
                "sort_key": signal["sort_key"],
                "signals": [],
            }

        selections_by_ticker[ticker]["signals"].append(signal)

    selections = list(selections_by_ticker.values())
    selections.sort(key=lambda selection: selection["sort_key"])
    return selections[:max_tickers]


def _fetch_recent_crossover_signals(
    connection: duckdb.DuckDBPyConnection,
    cutoff_date: date,
    latest_date: date,
) -> List[Dict[str, Any]]:
    """Fetch recent crossover signals sorted for chart selection."""
    rows = connection.execute(
        """
        SELECT
            securities.ticker,
            securities.name,
            securities.market,
            signals.signal_date,
            signals.moving_average_period_days,
            signals.comparison_period_days,
            signals.signal_type
        FROM moving_average_signals AS signals
        INNER JOIN securities
            ON signals.security_id = securities.security_id
        WHERE signals.signal_type IN ('BULLISH_CROSSOVER', 'BEARISH_CROSSOVER')
          AND signals.signal_date >= ?
          AND signals.signal_date <= ?
        ORDER BY
            signals.signal_date DESC,
            CASE
                WHEN signals.signal_type = 'BULLISH_CROSSOVER' THEN 0
                ELSE 1
            END,
            securities.ticker
        """,
        [cutoff_date, latest_date],
    ).fetchall()
    signals = []

    for row in rows:
        direction_rank = 0 if row[6] == "BULLISH_CROSSOVER" else 1
        signals.append(
            {
                "ticker": row[0],
                "company_name": row[1] or "",
                "market": row[2] or "",
                "crossover_date": _to_date(row[3]),
                "direction": _friendly_direction(row[6]),
                "trend_description": describe_crossover(row[4], row[5], row[6]),
                "days_since_crossover": format_days_since_crossover(row[3], latest_date),
                "sort_key": (-_to_date(row[3]).toordinal(), direction_rank, row[0]),
            }
        )

    return signals


def _normalise_tickers(tickers: Iterable[str]) -> List[str]:
    """Uppercase tickers and remove duplicates while preserving order."""
    selected: List[str] = []
    seen = set()

    for ticker in tickers:
        normalised = str(ticker).strip().upper()
        if not normalised or normalised in seen:
            continue
        selected.append(normalised)
        seen.add(normalised)

    return selected


def _friendly_direction(crossover_direction: Any) -> str:
    """Return a readable crossover direction label."""
    if crossover_direction == "BULLISH_CROSSOVER":
        return "Bullish"

    if crossover_direction == "BEARISH_CROSSOVER":
        return "Bearish"

    return str(crossover_direction or "")


def _latest_report_date(connection: duckdb.DuckDBPyConnection) -> Optional[date]:
    """Return the latest useful date across prices and moving-average signals."""
    latest_value = connection.execute(
        """
        SELECT MAX(report_date)
        FROM (
            SELECT MAX(price_date) AS report_date FROM daily_prices
            UNION ALL
            SELECT MAX(signal_date) AS report_date FROM moving_average_signals
        )
        """
    ).fetchone()[0]

    return _to_date(latest_value)


def _fetch_chart_data(
    connection: duckdb.DuckDBPyConnection,
    ticker: str,
    lookback_days: int,
    sma_periods: Sequence[int],
) -> Dict[str, Any]:
    """Fetch price and SMA rows for one ticker."""
    security_row = connection.execute(
        """
        SELECT
            securities.security_id,
            securities.name,
            securities.currency,
            MAX(prices.price_date)
        FROM securities
        LEFT JOIN daily_prices AS prices
            ON securities.security_id = prices.security_id
        WHERE securities.ticker = ?
        GROUP BY securities.security_id, securities.name, securities.currency
        """,
        [ticker],
    ).fetchone()

    if security_row is None:
        return {
            "prices": [],
            "moving_averages": {},
            "company_name": "",
            "currency": "",
        }

    security_id, company_name, currency, latest_price_date = security_row
    latest_date = _to_date(latest_price_date)

    if latest_date is None:
        return {
            "prices": [],
            "moving_averages": {},
            "company_name": company_name or "",
            "currency": currency or "",
        }

    cutoff_date = latest_date - timedelta(days=lookback_days)
    price_rows = connection.execute(
        """
        SELECT prices.price_date, prices.close_price
        FROM daily_prices AS prices
        INNER JOIN securities
            ON prices.security_id = securities.security_id
        WHERE prices.security_id = ?
          AND prices.price_date >= ?
        ORDER BY prices.price_date
        """,
        [security_id, cutoff_date],
    ).fetchall()

    moving_averages: Dict[int, List[Tuple[date, float]]] = {}
    for period in sma_periods:
        rows = connection.execute(
            """
            SELECT signals.signal_date, signals.moving_average_value
            FROM moving_average_signals AS signals
            WHERE signals.security_id = ?
              AND signals.signal_type = 'SMA'
              AND signals.moving_average_period_days = ?
              AND signals.signal_date >= ?
            ORDER BY signals.signal_date
            """,
            [security_id, period, cutoff_date],
        ).fetchall()
        moving_averages[period] = [(_to_date(row[0]), row[1]) for row in rows]

    return {
        "prices": [(_to_date(row[0]), row[1]) for row in price_rows],
        "moving_averages": moving_averages,
        "company_name": company_name or "",
        "currency": currency or "",
    }


def _write_chart_image(
    ticker: str,
    chart_data: Dict[str, Any],
    output_path: Path,
    show_close_price: bool,
    sma_periods: Sequence[int],
    close_price_style: str,
    close_price_color: str,
    close_price_linewidth: float,
) -> None:
    """Write one chart image to disk."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
    except ImportError as error:
        raise RuntimeError(
            "Could not generate chart images because matplotlib is not "
            "installed. Install the project dependencies, then try again."
        ) from error

    price_rows = chart_data["prices"]
    price_dates = [row[0] for row in price_rows]
    close_prices = [row[1] for row in price_rows]

    fig, ax = plt.subplots(figsize=(10, 5))

    if show_close_price:
        ax.plot(
            price_dates,
            close_prices,
            label="Close price",
            color=close_price_color,
            linestyle=_matplotlib_line_style(close_price_style),
            linewidth=close_price_linewidth,
            alpha=0.8,
        )

    for period in sma_periods:
        sma_rows = [
            row for row in chart_data["moving_averages"].get(period, []) if row[0]
        ]
        if not sma_rows:
            continue
        ax.plot(
            [row[0] for row in sma_rows],
            [row[1] for row in sma_rows],
            label=f"{period}-day SMA",
            linewidth=1.2,
        )

    company_name = chart_data.get("company_name")
    title = f"{ticker} trend"
    if company_name:
        title = f"{ticker} - {company_name} trend"

    ax.set_title(title)
    ax.set_xlabel("Date")
    currency = chart_data.get("currency")
    y_axis_label = "Price"
    if currency:
        y_axis_label = f"Price ({currency})"
    ax.set_ylabel(y_axis_label)
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=9))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.grid(True, alpha=0.22, linewidth=0.7)
    ax.legend(loc="best")
    fig.autofmt_xdate(rotation=30, ha="right")
    fig.tight_layout()
    fig.savefig(output_path, dpi=120)
    plt.close(fig)


def _matplotlib_line_style(style_name: str) -> str:
    """Translate a friendly line style setting into a matplotlib style."""
    styles = {
        "solid": "-",
        "dashed": "--",
        "dotted": ":",
        "dashdot": "-.",
    }
    return styles.get(style_name.strip().lower(), ":")


def _safe_filename(ticker: str) -> str:
    """Make a ticker safe for a simple PNG filename."""
    safe_characters = []

    for character in ticker:
        if character.isalnum() or character in ("-", "_"):
            safe_characters.append(character)
        else:
            safe_characters.append("_")

    return "".join(safe_characters)


def _to_date(value: Any) -> Optional[date]:
    """Convert DuckDB date-like values to ``date`` objects."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    return date.fromisoformat(str(value)[:10])
