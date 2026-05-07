"""Tests for possible flag-pattern detection."""

from datetime import date, timedelta

from market_sentinel.analytics.flag_patterns import detect_possible_flag_pattern


def test_bullish_possible_flag_pattern_detected() -> None:
    """Bullish flags need a prior upward move and recent sideways/down channel."""
    rows = _trend_rows(date(2026, 1, 1), 45, 100.0, 112.0)
    rows.extend(_channel_rows(date(2026, 2, 15), 20, 113.0, 110.0, 108.0, 106.0))

    pattern = detect_possible_flag_pattern(rows, "Bullish")

    assert pattern is not None
    assert pattern["detected"] is True
    assert pattern["direction"] == "Bullish"
    assert pattern["label"] == "Possible flag pattern"
    assert pattern["upper_line"][0][0] == date(2026, 2, 15)
    assert pattern["lower_line"][1][0] == date(2026, 3, 6)


def test_bearish_possible_flag_pattern_detected() -> None:
    """Bearish flags need a prior downward move and recent sideways/up channel."""
    rows = _trend_rows(date(2026, 1, 1), 45, 100.0, 88.0)
    rows.extend(_channel_rows(date(2026, 2, 15), 20, 88.0, 91.0, 84.0, 86.0))

    pattern = detect_possible_flag_pattern(rows, "Bearish")

    assert pattern is not None
    assert pattern["detected"] is True
    assert pattern["direction"] == "Bearish"
    assert pattern["upper_line"][0][0] == date(2026, 2, 15)
    assert pattern["lower_line"][1][0] == date(2026, 3, 6)


def test_possible_flag_pattern_skips_insufficient_data() -> None:
    """Insufficient price rows should simply produce no pattern."""
    rows = _trend_rows(date(2026, 1, 1), 15, 100.0, 105.0)

    assert detect_possible_flag_pattern(rows, "Bullish") is None


def test_possible_flag_pattern_skips_when_prior_move_is_too_small() -> None:
    """A weak prior move should not be called a possible flag."""
    rows = _trend_rows(date(2026, 1, 1), 45, 100.0, 104.0)
    rows.extend(_channel_rows(date(2026, 2, 15), 20, 105.0, 103.0, 101.0, 100.0))

    assert detect_possible_flag_pattern(rows, "Bullish") is None


def test_possible_flag_pattern_skips_wide_consolidation() -> None:
    """A wide recent range should be treated as too loose for a flag guide."""
    rows = _trend_rows(date(2026, 1, 1), 45, 100.0, 112.0)
    rows.extend(_channel_rows(date(2026, 2, 15), 20, 120.0, 117.0, 95.0, 93.0))

    assert detect_possible_flag_pattern(rows, "Bullish") is None


def _trend_rows(
    start_date: date,
    days: int,
    start_close: float,
    end_close: float,
) -> list:
    """Return simple OHLC tuples for a prior trend."""
    rows = []
    for index in range(days):
        close = _interpolate(start_close, end_close, index, days)
        rows.append(
            (
                start_date + timedelta(days=index),
                close,
                close * 1.01,
                close * 0.99,
            )
        )
    return rows


def _channel_rows(
    start_date: date,
    days: int,
    start_high: float,
    end_high: float,
    start_low: float,
    end_low: float,
) -> list:
    """Return OHLC tuples for a consolidation channel."""
    rows = []
    for index in range(days):
        high = _interpolate(start_high, end_high, index, days)
        low = _interpolate(start_low, end_low, index, days)
        close = (high + low) / 2
        rows.append((start_date + timedelta(days=index), close, high, low))
    return rows


def _interpolate(start_value: float, end_value: float, index: int, count: int) -> float:
    """Linearly interpolate between two values."""
    if count <= 1:
        return end_value

    return start_value + ((end_value - start_value) * (index / (count - 1)))
