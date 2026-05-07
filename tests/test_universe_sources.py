"""Tests for public stock universe source helpers."""

from pathlib import Path

import pandas as pd
import pytest

from market_sentinel.data import universe_sources
from market_sentinel.data.universe_sources import (
    _to_london_yfinance_ticker,
    update_ftse350_universe_csv,
    update_ftse100_universe_csv,
    update_sp500_universe_csv,
)


class FakeResponse:
    """Small fake response object for mocked HTTP requests."""

    def __init__(self, status_code: int = 200, text: str = "<html></html>"):
        self.status_code = status_code
        self.text = text


def fake_sp500_table() -> pd.DataFrame:
    """Return a small fake Wikipedia-style S&P 500 table."""
    return pd.DataFrame(
        {
            "Symbol": ["AAPL", "BRK.B", "BF.B"],
            "Security": ["Apple Inc.", "Berkshire Hathaway", "Brown-Forman"],
            "GICS Sector": [
                "Information Technology",
                "Financials",
                "Consumer Staples",
            ],
        }
    )


def fake_ftse100_table() -> pd.DataFrame:
    """Return a small fake Wikipedia-style FTSE 100 table."""
    return pd.DataFrame(
        {
            "Company": ["HSBC Holdings", "BT Group", "Shell"],
            "Ticker": ["HSBA", "BT.A", "SHEL.L"],
            "FTSE Industry Classification Benchmark sector": [
                "Banks",
                "Telecommunications",
                "Energy",
            ],
        }
    )


def fake_ftse250_table() -> pd.DataFrame:
    """Return a small fake Wikipedia-style FTSE 250 table."""
    return pd.DataFrame(
        {
            "Company": ["EasyJet", "Shell", "Games Workshop"],
            "EPIC": ["EZJ", "SHEL.L", "GAW"],
            "ICB Sector": [
                "Travel and Leisure",
                "Energy",
                "Consumer Products",
            ],
        }
    )


def test_update_sp500_universe_csv_writes_required_format(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The updater should save a project-compatible S&P 500 CSV."""
    request_calls = []

    def fake_get(source_url, headers, timeout):
        request_calls.append(
            {
                "source_url": source_url,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return FakeResponse(text="<html>fake page</html>")

    def fake_read_html(html_text):
        assert html_text == "<html>fake page</html>"
        return [fake_sp500_table()]

    monkeypatch.setattr(universe_sources.requests, "get", fake_get)
    monkeypatch.setattr(universe_sources.pd, "read_html", fake_read_html)

    output_path = tmp_path / "universes" / "sp_500.csv"
    saved_path = update_sp500_universe_csv(output_path)
    saved_rows = pd.read_csv(saved_path)

    assert saved_path == output_path
    assert list(saved_rows.columns) == [
        "ticker",
        "name",
        "market",
        "region",
        "currency",
        "sector",
    ]
    assert set(saved_rows["ticker"]) == {"AAPL", "BRK-B", "BF-B"}
    assert set(saved_rows["market"]) == {"S&P 500"}
    assert set(saved_rows["region"]) == {"US"}
    assert set(saved_rows["currency"]) == {"USD"}
    assert "Information Technology" in set(saved_rows["sector"])
    assert request_calls[0]["headers"]["User-Agent"]
    assert request_calls[0]["timeout"] > 0


def test_update_ftse100_universe_csv_writes_required_format(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The updater should save a project-compatible FTSE 100 CSV."""
    request_calls = []

    def fake_get(source_url, headers, timeout):
        request_calls.append(
            {
                "source_url": source_url,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return FakeResponse(text="<html>fake page</html>")

    def fake_read_html(html_text):
        assert html_text == "<html>fake page</html>"
        return [fake_ftse100_table()]

    monkeypatch.setattr(universe_sources.requests, "get", fake_get)
    monkeypatch.setattr(universe_sources.pd, "read_html", fake_read_html)

    output_path = tmp_path / "universes" / "ftse_100.csv"
    saved_path = update_ftse100_universe_csv(output_path)
    saved_rows = pd.read_csv(saved_path)

    assert saved_path == output_path
    assert list(saved_rows.columns) == [
        "ticker",
        "name",
        "market",
        "region",
        "currency",
        "sector",
    ]
    assert set(saved_rows["ticker"]) == {"HSBA.L", "BT-A.L", "SHEL.L"}
    assert set(saved_rows["market"]) == {"FTSE 100"}
    assert set(saved_rows["region"]) == {"UK"}
    assert set(saved_rows["currency"]) == {"GBP"}
    assert "Banks" in set(saved_rows["sector"])
    assert request_calls[0]["headers"]["User-Agent"]
    assert request_calls[0]["timeout"] > 0


def test_update_ftse350_universe_csv_writes_required_format(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """The updater should combine FTSE 100 and FTSE 250 into FTSE 350."""
    request_calls = []

    def fake_get(source_url, headers, timeout):
        request_calls.append(
            {
                "source_url": source_url,
                "headers": headers,
                "timeout": timeout,
            }
        )
        if "FTSE_100" in source_url:
            return FakeResponse(text="<html>ftse 100 page</html>")
        return FakeResponse(text="<html>ftse 250 page</html>")

    def fake_read_html(html_text):
        if html_text == "<html>ftse 100 page</html>":
            return [fake_ftse100_table()]
        if html_text == "<html>ftse 250 page</html>":
            return [fake_ftse250_table()]
        raise AssertionError(f"Unexpected HTML: {html_text}")

    monkeypatch.setattr(universe_sources.requests, "get", fake_get)
    monkeypatch.setattr(universe_sources.pd, "read_html", fake_read_html)

    output_path = tmp_path / "universes" / "ftse_350.csv"
    saved_path = update_ftse350_universe_csv(output_path)
    saved_rows = pd.read_csv(saved_path)
    captured = capsys.readouterr()

    assert saved_path == output_path
    assert list(saved_rows.columns) == [
        "ticker",
        "name",
        "market",
        "region",
        "currency",
        "sector",
    ]
    assert set(saved_rows["ticker"]) == {
        "BT-A.L",
        "EZJ.L",
        "GAW.L",
        "HSBA.L",
        "SHEL.L",
    }
    assert set(saved_rows["market"]) == {"FTSE 350"}
    assert set(saved_rows["region"]) == {"UK"}
    assert set(saved_rows["currency"]) == {"GBP"}
    assert "Banks" in set(saved_rows["sector"])
    assert len(request_calls) == 2
    assert all(call["headers"]["User-Agent"] for call in request_calls)
    assert all(call["timeout"] > 0 for call in request_calls)
    assert "FTSE 100 rows found: 3" in captured.out
    assert "FTSE 250 rows found: 3" in captured.out
    assert "Combined rows written: 5" in captured.out
    assert "Duplicate rows removed: 1" in captured.out


def test_london_ticker_conversion_handles_dots_and_existing_suffix() -> None:
    """London tickers should be converted to yfinance-compatible symbols."""
    assert _to_london_yfinance_ticker("HSBA") == "HSBA.L"
    assert _to_london_yfinance_ticker("BT.A") == "BT-A.L"
    assert _to_london_yfinance_ticker("shel.l") == "SHEL.L"


def test_update_sp500_universe_csv_handles_parser_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Parser failures should include the original error details."""

    def fake_get(source_url, headers, timeout):
        return FakeResponse(text="<html>fake page</html>")

    def fake_read_html(html_text):
        raise ValueError("No tables found")

    monkeypatch.setattr(universe_sources.requests, "get", fake_get)
    monkeypatch.setattr(universe_sources.pd, "read_html", fake_read_html)

    with pytest.raises(RuntimeError) as error_info:
        update_sp500_universe_csv(tmp_path / "sp_500.csv")

    message = str(error_info.value)
    assert "Could not read" in message
    assert "Underlying error: ValueError: No tables found" in message


def test_update_ftse100_universe_csv_handles_missing_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A changed FTSE 100 page layout should produce a clear parsing error."""

    def fake_get(source_url, headers, timeout):
        return FakeResponse(text="<html>fake page</html>")

    def fake_read_html(html_text):
        return [pd.DataFrame({"Wrong": ["value"]})]

    monkeypatch.setattr(universe_sources.requests, "get", fake_get)
    monkeypatch.setattr(universe_sources.pd, "read_html", fake_read_html)

    with pytest.raises(RuntimeError, match="Could not find"):
        update_ftse100_universe_csv(tmp_path / "ftse_100.csv")


def test_update_ftse350_universe_csv_handles_missing_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A changed FTSE source layout should produce a clear parsing error."""

    def fake_get(source_url, headers, timeout):
        if "FTSE_100" in source_url:
            return FakeResponse(text="<html>ftse 100 page</html>")
        return FakeResponse(text="<html>ftse 250 page</html>")

    def fake_read_html(html_text):
        if html_text == "<html>ftse 100 page</html>":
            return [fake_ftse100_table()]
        return [pd.DataFrame({"Wrong": ["value"]})]

    monkeypatch.setattr(universe_sources.requests, "get", fake_get)
    monkeypatch.setattr(universe_sources.pd, "read_html", fake_read_html)

    with pytest.raises(RuntimeError, match="Could not find the FTSE 250"):
        update_ftse350_universe_csv(tmp_path / "ftse_350.csv")


def test_update_sp500_universe_csv_handles_http_403(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """HTTP failures should include the status code."""

    def fake_get(source_url, headers, timeout):
        return FakeResponse(status_code=403, text="Forbidden")

    monkeypatch.setattr(universe_sources.requests, "get", fake_get)

    with pytest.raises(RuntimeError) as error_info:
        update_sp500_universe_csv(tmp_path / "sp_500.csv")

    message = str(error_info.value)
    assert "Could not download" in message
    assert "HTTP status 403" in message


def test_update_sp500_universe_csv_handles_missing_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A changed page layout should produce a clear parsing error."""

    def fake_get(source_url, headers, timeout):
        return FakeResponse(text="<html>fake page</html>")

    def fake_read_html(html_text):
        return [pd.DataFrame({"Wrong": ["value"]})]

    monkeypatch.setattr(universe_sources.requests, "get", fake_get)
    monkeypatch.setattr(universe_sources.pd, "read_html", fake_read_html)

    with pytest.raises(RuntimeError, match="Could not find"):
        update_sp500_universe_csv(tmp_path / "sp_500.csv")
