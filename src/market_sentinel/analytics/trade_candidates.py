"""Trade candidate review helpers for chart-led PDF reports."""

from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

import duckdb

from market_sentinel.config.loader import load_named_config

DEFAULT_STOP_SHORT_WINDOW_DAYS = 20
DEFAULT_TRAILING_STOP_PERCENT = 20
DEFAULT_INCLUDE_50_SMA_STOP = True
DEFAULT_INCLUDE_20_DAY_EXTREME_STOP = True
DEFAULT_INCLUDE_TRAILING_REFERENCE = True
DEFAULT_STOP_DISTANCE_WARNING_PERCENT = 12
DEFAULT_RECENT_STRONG_DAYS = 2


def load_trade_candidate_settings(config_dir: Optional[Path] = None) -> Dict[str, Any]:
    """Read trade candidate card settings from ``config/settings.yaml``."""
    try:
        settings = load_named_config("settings", config_dir)
    except FileNotFoundError:
        settings = {}

    return {
        "short_window_days": _positive_int_setting(
            settings,
            "candidate_stop_short_window_days",
            DEFAULT_STOP_SHORT_WINDOW_DAYS,
        ),
        "trailing_stop_percent": _positive_float_setting(
            settings,
            "candidate_trailing_stop_percent",
            DEFAULT_TRAILING_STOP_PERCENT,
        ),
        "include_50_sma_stop": _bool_setting(
            settings,
            "candidate_include_50_sma_stop",
            DEFAULT_INCLUDE_50_SMA_STOP,
        ),
        "include_20_day_extreme_stop": _bool_setting(
            settings,
            "candidate_include_20_day_extreme_stop",
            DEFAULT_INCLUDE_20_DAY_EXTREME_STOP,
        ),
        "include_trailing_reference": _bool_setting(
            settings,
            "candidate_include_trailing_reference",
            DEFAULT_INCLUDE_TRAILING_REFERENCE,
        ),
        "stop_distance_warning_percent": _positive_float_setting(
            settings,
            "candidate_grade_stop_distance_warning_percent",
            DEFAULT_STOP_DISTANCE_WARNING_PERCENT,
        ),
        "recent_strong_days": _positive_int_setting(
            settings,
            "candidate_recent_strong_days",
            DEFAULT_RECENT_STRONG_DAYS,
        ),
    }


def build_trade_candidate(
    connection: duckdb.DuckDBPyConnection,
    ticker: str,
    signal: Optional[Dict[str, Any]] = None,
    config_dir: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    """Build one compact trade candidate card model for a ticker."""
    signal = signal or {}
    settings = load_trade_candidate_settings(config_dir)
    security = _fetch_security(connection, ticker)

    if security is None:
        return None

    price_rows = _fetch_recent_close_prices(
        connection,
        security["security_id"],
        settings["short_window_days"],
    )
    latest_close = price_rows[0]["close_price"] if price_rows else None
    latest_50_sma = _fetch_latest_sma(connection, security["security_id"], 50)
    latest_7_sma = _fetch_latest_sma(connection, security["security_id"], 7)
    latest_30_sma = _fetch_latest_sma(connection, security["security_id"], 30)
    direction = signal.get("direction") or ""
    review_levels = _review_levels(
        direction,
        latest_close,
        latest_50_sma,
        price_rows,
        settings,
    )

    dividend_risk_flag = _fetch_latest_dividend_risk_flag(
        connection,
        security["security_id"],
    )
    grade = _action_grade(
        direction,
        _to_date(signal.get("crossover_date")),
        price_rows[0]["price_date"] if price_rows else None,
        latest_close,
        latest_7_sma,
        latest_30_sma,
        latest_50_sma,
        review_levels,
        dividend_risk_flag,
        settings,
    )

    return {
        "ticker": security["ticker"],
        "company_name": security["company_name"],
        "market": security["market"],
        "currency": security["currency"],
        "signal_direction": direction,
        "signal_description": signal.get("trend_description") or "Not available",
        "crossover_date": _to_date(signal.get("crossover_date")),
        "days_since_crossover": signal.get("days_since_crossover") or "Not available",
        "latest_close_price": latest_close,
        "review_levels": review_levels,
        "action_grade": grade["action_grade"],
        "score": grade["score"],
        "max_score": 10,
        "grade_reasons": grade["reasons"],
        "grade_cautions": grade["cautions"],
        "risk_notes": _risk_notes(
            latest_close,
            latest_50_sma,
            dividend_risk_flag,
        ),
    }


def _fetch_security(
    connection: duckdb.DuckDBPyConnection,
    ticker: str,
) -> Optional[Dict[str, Any]]:
    """Fetch one security row by ticker."""
    row = connection.execute(
        """
        SELECT security_id, ticker, name, market, currency
        FROM securities
        WHERE ticker = ?
        """,
        [ticker],
    ).fetchone()

    if row is None:
        return None

    return {
        "security_id": row[0],
        "ticker": row[1],
        "company_name": row[2] or "",
        "market": row[3] or "",
        "currency": row[4] or "",
    }


def _fetch_recent_close_prices(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    row_count: int,
) -> list:
    """Fetch the latest close prices, newest first."""
    rows = connection.execute(
        """
        SELECT price_date, close_price
        FROM daily_prices
        WHERE security_id = ?
        ORDER BY price_date DESC
        LIMIT ?
        """,
        [security_id, row_count],
    ).fetchall()

    return [
        {
            "price_date": _to_date(row[0]),
            "close_price": row[1],
        }
        for row in rows
    ]


def _fetch_latest_sma(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
    period_days: int,
) -> Optional[float]:
    """Fetch the latest stored SMA value for a period."""
    row = connection.execute(
        """
        SELECT moving_average_value
        FROM moving_average_signals
        WHERE security_id = ?
          AND signal_type = 'SMA'
          AND moving_average_period_days = ?
        ORDER BY signal_date DESC
        LIMIT 1
        """,
        [security_id, period_days],
    ).fetchone()

    if row is None:
        return None

    return row[0]


def _review_levels(
    direction: str,
    latest_close: Optional[float],
    latest_50_sma: Optional[float],
    price_rows: list,
    settings: Dict[str, Any],
) -> Dict[str, Optional[float]]:
    """Calculate suggested planning reference levels for a candidate."""
    levels: Dict[str, Optional[float]] = {}
    is_bullish = direction == "Bullish"
    is_bearish = direction == "Bearish"

    if settings["include_50_sma_stop"]:
        levels["50-day SMA"] = latest_50_sma

    if settings["include_20_day_extreme_stop"]:
        close_prices = [
            row["close_price"]
            for row in price_rows
            if row.get("close_price") is not None
        ]
        if is_bullish:
            levels["20-day low"] = min(close_prices) if close_prices else None
        elif is_bearish:
            levels["20-day high"] = max(close_prices) if close_prices else None
        else:
            levels["20-day extreme"] = None

    if settings["include_trailing_reference"]:
        levels["20% trailing reference"] = _trailing_reference(
            direction,
            latest_close,
            settings["trailing_stop_percent"],
        )

    return levels


def _trailing_reference(
    direction: str,
    latest_close: Optional[float],
    trailing_percent: float,
) -> Optional[float]:
    """Calculate a directional trailing planning reference."""
    if latest_close is None:
        return None

    decimal_percent = trailing_percent / 100

    if direction == "Bullish":
        return round(latest_close * (1 - decimal_percent), 2)

    if direction == "Bearish":
        return round(latest_close * (1 + decimal_percent), 2)

    return None


def _risk_notes(
    latest_close: Optional[float],
    latest_50_sma: Optional[float],
    dividend_risk_flag: Optional[str],
) -> list:
    """Build simple risk notes for the candidate card."""
    notes = []

    if latest_close is None or latest_50_sma is None:
        notes.append("50-day trend line comparison is not available.")
    elif latest_close >= latest_50_sma:
        notes.append("Close price is above the 50-day trend line.")
    else:
        notes.append("Close price is below the 50-day trend line.")

    if dividend_risk_flag:
        notes.append("Dividend risk flag present.")
    else:
        notes.append("No dividend risk flag.")

    return notes


def _action_grade(
    direction: str,
    crossover_date: Optional[date],
    latest_price_date: Optional[date],
    latest_close: Optional[float],
    latest_7_sma: Optional[float],
    latest_30_sma: Optional[float],
    latest_50_sma: Optional[float],
    review_levels: Dict[str, Optional[float]],
    dividend_risk_flag: Optional[str],
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    """Score one candidate and return an explainable setup grade."""
    score = 0
    reasons = []
    cautions = []
    days_since = _days_between(crossover_date, latest_price_date)
    is_bullish = direction == "Bullish"
    is_bearish = direction == "Bearish"

    if is_bullish:
        if days_since is not None and days_since <= settings["recent_strong_days"]:
            score += 3
            reasons.append("Recent bullish crossover within 2 days.")
        elif days_since is not None and days_since <= 7:
            score += 2
            reasons.append("Bullish crossover within 3 to 7 days.")

        if latest_close is not None and latest_50_sma is not None:
            if latest_close > latest_50_sma:
                score += 2
                reasons.append("Latest close is above the 50-day SMA.")
            elif latest_close < latest_50_sma:
                score -= 2
                cautions.append("Latest close is below the 50-day SMA.")

        if _first_above_second(latest_7_sma, latest_30_sma):
            score += 1
            reasons.append("7-day SMA is above the 30-day SMA.")

        if _first_above_second(latest_30_sma, latest_50_sma):
            score += 1
            reasons.append("30-day SMA is above the 50-day SMA.")

        if dividend_risk_flag:
            score -= 2
            cautions.append("Dividend risk flag present.")
        else:
            score += 1
            reasons.append("No dividend risk flag.")

    elif is_bearish:
        if days_since is not None and days_since <= settings["recent_strong_days"]:
            score += 3
            reasons.append("Recent bearish crossover within 2 days.")
        elif days_since is not None and days_since <= 7:
            score += 2
            reasons.append("Bearish crossover within 3 to 7 days.")

        if latest_close is not None and latest_50_sma is not None:
            if latest_close < latest_50_sma:
                score += 2
                reasons.append("Latest close is below the 50-day SMA.")
            elif latest_close > latest_50_sma:
                score -= 2
                cautions.append("Latest close is above the 50-day SMA.")

        if _first_below_second(latest_7_sma, latest_30_sma):
            score += 1
            reasons.append("7-day SMA is below the 30-day SMA.")

        if _first_below_second(latest_30_sma, latest_50_sma):
            score += 1
            reasons.append("30-day SMA is below the 50-day SMA.")

    if _stop_distance_too_wide(
        direction,
        latest_close,
        review_levels,
        settings["stop_distance_warning_percent"],
    ):
        score -= 1
        cautions.append("Stop/review distance is wider than the configured warning.")

    return {
        "score": score,
        "action_grade": _grade_label(direction, score),
        "reasons": reasons,
        "cautions": cautions,
    }


def _grade_label(direction: str, score: int) -> str:
    """Map a score to a user-friendly setup grade."""
    if direction == "Bullish" and score >= 6:
        return "Strong Buy Setup"

    if direction == "Bullish" and score >= 3:
        return "Buy Setup"

    if direction == "Bearish" and score >= 6:
        return "Strong Sell Setup"

    if direction == "Bearish" and score >= 3:
        return "Sell Setup"

    return "Track Only"


def _days_between(start_date: Optional[date], end_date: Optional[date]) -> Optional[int]:
    """Return days between two dates when both are available."""
    if start_date is None or end_date is None:
        return None

    return max((end_date - start_date).days, 0)


def _first_above_second(first: Optional[float], second: Optional[float]) -> bool:
    """Return true when both values exist and first is greater."""
    return first is not None and second is not None and first > second


def _first_below_second(first: Optional[float], second: Optional[float]) -> bool:
    """Return true when both values exist and first is lower."""
    return first is not None and second is not None and first < second


def _stop_distance_too_wide(
    direction: str,
    latest_close: Optional[float],
    review_levels: Dict[str, Optional[float]],
    warning_percent: float,
) -> bool:
    """Return true when the 20-day review level is far from latest close."""
    if latest_close in (None, 0):
        return False

    if direction == "Bullish":
        reference = review_levels.get("20-day low")
    elif direction == "Bearish":
        reference = review_levels.get("20-day high")
    else:
        return False

    if reference is None:
        return False

    distance_percent = abs(latest_close - reference) / latest_close * 100
    return distance_percent > warning_percent


def _fetch_latest_dividend_risk_flag(
    connection: duckdb.DuckDBPyConnection,
    security_id: int,
) -> Optional[str]:
    """Fetch the latest dividend risk flag, if one exists."""
    row = connection.execute(
        """
        SELECT dividend_risk_flag
        FROM dividend_metrics
        WHERE security_id = ?
        ORDER BY metric_date DESC
        LIMIT 1
        """,
        [security_id],
    ).fetchone()

    if row is None:
        return None

    return row[0]


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


def _to_date(value: Any) -> Optional[date]:
    """Convert date-like values to ``date`` objects."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    return date.fromisoformat(str(value)[:10])
