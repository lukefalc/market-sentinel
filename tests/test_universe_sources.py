"""Tests for public stock universe source helpers."""

from pathlib import Path

import pandas as pd
import pytest

from market_sentinel.data import universe_sources
from market_sentinel.data.universe_sources import update_sp500_universe_csv


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


def test_update_sp500_universe_csv_writes_required_format(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The updater should save a project-compatible S&P 500 CSV."""

    def fake_read_html(source_url):
        return [fake_sp500_table()]

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


def test_update_sp500_universe_csv_handles_download_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Download or parser failures should produce a beginner-friendly error."""

    def fake_read_html(source_url):
        raise ValueError("No tables found")

    monkeypatch.setattr(universe_sources.pd, "read_html", fake_read_html)

    with pytest.raises(RuntimeError, match="Could not download"):
        update_sp500_universe_csv(tmp_path / "sp_500.csv")


def test_update_sp500_universe_csv_handles_missing_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A changed page layout should produce a clear parsing error."""

    def fake_read_html(source_url):
        return [pd.DataFrame({"Wrong": ["value"]})]

    monkeypatch.setattr(universe_sources.pd, "read_html", fake_read_html)

    with pytest.raises(RuntimeError, match="Could not find"):
        update_sp500_universe_csv(tmp_path / "sp_500.csv")
