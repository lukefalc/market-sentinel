"""Conservative possible flag-pattern detection helpers."""

from datetime import date
from typing import Any, Dict, Optional, Sequence, Tuple

DEFAULT_FLAG_PRIOR_TREND_DAYS = 45
DEFAULT_FLAG_CONSOLIDATION_DAYS = 20
DEFAULT_FLAG_MIN_PRIOR_MOVE_PERCENT = 8.0
DEFAULT_FLAG_MAX_CONSOLIDATION_RANGE_PERCENT = 12.0


def detect_possible_flag_pattern(
    price_rows: Sequence[Tuple[Any, ...]],
    direction: str,
    prior_trend_days: int = DEFAULT_FLAG_PRIOR_TREND_DAYS,
    consolidation_days: int = DEFAULT_FLAG_CONSOLIDATION_DAYS,
    min_prior_move_percent: float = DEFAULT_FLAG_MIN_PRIOR_MOVE_PERCENT,
    max_consolidation_range_percent: float = DEFAULT_FLAG_MAX_CONSOLIDATION_RANGE_PERCENT,
) -> Optional[Dict[str, Any]]:
    """Return possible flag guide lines when simple conservative criteria match.

    The detector is intentionally cautious. It looks for a meaningful prior move
    followed by a shorter, tighter consolidation channel that slopes sideways or
    modestly against that prior trend. It does not claim a confirmed pattern.
    """
    clean_rows = [_normalise_price_row(row) for row in price_rows]
    clean_rows = [row for row in clean_rows if row is not None]

    if len(clean_rows) < prior_trend_days + consolidation_days:
        return None

    consolidation = clean_rows[-consolidation_days:]
    prior = clean_rows[-(prior_trend_days + consolidation_days) : -consolidation_days]

    if not prior or not consolidation:
        return None

    prior_start = prior[0]["close"]
    prior_end = prior[-1]["close"]
    if prior_start in (None, 0) or prior_end is None:
        return None

    prior_move_percent = ((prior_end - prior_start) / prior_start) * 100
    direction = str(direction or "").strip()

    if direction == "Bullish":
        if prior_move_percent < min_prior_move_percent:
            return None
    elif direction == "Bearish":
        if prior_move_percent > -min_prior_move_percent:
            return None
    else:
        return None

    highs = [row["high"] for row in consolidation if row["high"] is not None]
    lows = [row["low"] for row in consolidation if row["low"] is not None]
    if len(highs) < 2 or len(lows) < 2:
        return None

    range_base = consolidation[0]["close"] or prior_end
    if not range_base:
        return None

    consolidation_range_percent = ((max(highs) - min(lows)) / range_base) * 100
    if consolidation_range_percent > max_consolidation_range_percent:
        return None

    high_line = _fit_endpoint_line(
        [(row["date"], row["high"]) for row in consolidation]
    )
    low_line = _fit_endpoint_line(
        [(row["date"], row["low"]) for row in consolidation]
    )
    if high_line is None or low_line is None:
        return None

    high_slope_percent = _line_slope_percent(high_line)
    low_slope_percent = _line_slope_percent(low_line)

    if direction == "Bullish":
        if high_slope_percent > 2.0 or low_slope_percent > 2.0:
            return None
    elif direction == "Bearish":
        if high_slope_percent < -2.0 or low_slope_percent < -2.0:
            return None

    return {
        "detected": True,
        "direction": direction,
        "label": "Possible flag pattern",
        "prior_move_percent": prior_move_percent,
        "consolidation_range_percent": consolidation_range_percent,
        "upper_line": high_line,
        "lower_line": low_line,
    }


def _normalise_price_row(row: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    """Return a price row dict from tuple-like chart rows."""
    if len(row) < 2:
        return None

    price_date = row[0]
    close_price = _to_float(row[1])
    high_price = _to_float(row[2]) if len(row) > 2 else close_price
    low_price = _to_float(row[3]) if len(row) > 3 else close_price

    if price_date is None or close_price is None:
        return None

    if high_price is None:
        high_price = close_price
    if low_price is None:
        low_price = close_price

    return {
        "date": price_date,
        "close": close_price,
        "high": high_price,
        "low": low_price,
    }


def _fit_endpoint_line(
    points: Sequence[Tuple[date, Optional[float]]],
) -> Optional[Tuple[Tuple[date, float], Tuple[date, float]]]:
    """Fit a simple line and return start/end points over the input dates."""
    clean_points = [(point_date, value) for point_date, value in points if value is not None]
    if len(clean_points) < 2:
        return None

    values = [value for _point_date, value in clean_points]
    x_values = list(range(len(values)))
    x_mean = sum(x_values) / len(x_values)
    y_mean = sum(values) / len(values)
    denominator = sum((x_value - x_mean) ** 2 for x_value in x_values)
    if denominator == 0:
        return None

    slope = sum(
        (x_value - x_mean) * (value - y_mean)
        for x_value, value in zip(x_values, values)
    ) / denominator
    intercept = y_mean - slope * x_mean
    first_value = intercept
    last_value = intercept + slope * (len(values) - 1)

    return (
        (clean_points[0][0], first_value),
        (clean_points[-1][0], last_value),
    )


def _line_slope_percent(
    line: Tuple[Tuple[date, float], Tuple[date, float]],
) -> float:
    """Return total line change as a percent of the first value."""
    start_value = line[0][1]
    end_value = line[1][1]
    if not start_value:
        return 0.0

    return ((end_value - start_value) / start_value) * 100


def _to_float(value: Any) -> Optional[float]:
    """Return a float or None for missing/non-numeric values."""
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None
