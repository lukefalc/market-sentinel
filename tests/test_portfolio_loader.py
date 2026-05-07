"""Tests for local portfolio holdings and watchlist loading."""

from pathlib import Path

from market_sentinel.data.portfolio_loader import (
    load_portfolio_data,
    portfolio_status_from_data,
    portfolio_status_for_ticker,
)


def write_settings(config_dir: Path, database_path: Path) -> None:
    """Create a minimal settings file for portfolio loader tests."""
    config_dir.mkdir(parents=True, exist_ok=True)
    config_dir.joinpath("settings.yaml").write_text(
        "\n".join(
            [
                f"database_path: {database_path}",
                "portfolio_holdings_path: config/portfolio/holdings.csv",
                "portfolio_watchlist_path: config/portfolio/watchlist.csv",
            ]
        ),
        encoding="utf-8",
    )


def write_portfolio_files(config_dir: Path) -> None:
    """Create small fake holdings and watchlist files."""
    portfolio_dir = config_dir / "portfolio"
    portfolio_dir.mkdir(parents=True, exist_ok=True)
    portfolio_dir.joinpath("holdings.csv").write_text(
        "\n".join(
            [
                "ticker,name,market,quantity,average_cost,notes",
                "AAA,Example A,S&P 500,12,100,Test holding",
                "BOTH,Example Both,S&P 500,5,50,Test holding",
            ]
        ),
        encoding="utf-8",
    )
    portfolio_dir.joinpath("watchlist.csv").write_text(
        "\n".join(
            [
                "ticker,name,market,reason,notes",
                "BBB,Example B,FTSE 350,Waiting for setup,Test watchlist",
                "BOTH,Example Both,S&P 500,Already held but watching,Test watchlist",
            ]
        ),
        encoding="utf-8",
    )


def test_missing_portfolio_files_are_empty(tmp_path: Path) -> None:
    """Missing portfolio CSVs should not fail report generation."""
    config_dir = tmp_path / "config"
    write_settings(config_dir, tmp_path / "data" / "test.duckdb")

    portfolio_data = load_portfolio_data(config_dir)
    status = portfolio_status_for_ticker("AAA", "S&P 500", config_dir)

    assert portfolio_data == {"holdings": {}, "watchlist": {}}
    assert status["portfolio_status"] == "New"
    assert status["holding_quantity"] == ""
    assert status["watchlist_reason"] == ""


def test_held_ticker_is_marked_held(tmp_path: Path) -> None:
    """A ticker in holdings should be marked Held."""
    config_dir = tmp_path / "config"
    write_settings(config_dir, tmp_path / "data" / "test.duckdb")
    write_portfolio_files(config_dir)

    status = portfolio_status_for_ticker("AAA", "S&P 500", config_dir)

    assert status["portfolio_status"] == "Held"
    assert status["holding_quantity"] == "12"


def test_watchlist_ticker_is_marked_watchlist(tmp_path: Path) -> None:
    """A ticker in watchlist should be marked Watchlist."""
    config_dir = tmp_path / "config"
    write_settings(config_dir, tmp_path / "data" / "test.duckdb")
    write_portfolio_files(config_dir)

    status = portfolio_status_for_ticker("BBB", "FTSE 350", config_dir)

    assert status["portfolio_status"] == "Watchlist"
    assert status["watchlist_reason"] == "Waiting for setup"


def test_ticker_in_both_lists_is_marked_held_plus_watchlist(tmp_path: Path) -> None:
    """A ticker in both CSVs should show both statuses."""
    config_dir = tmp_path / "config"
    write_settings(config_dir, tmp_path / "data" / "test.duckdb")
    write_portfolio_files(config_dir)

    status = portfolio_status_for_ticker("BOTH", "S&P 500", config_dir)

    assert status["portfolio_status"] == "Held + Watchlist"
    assert status["holding_quantity"] == "5"
    assert status["watchlist_reason"] == "Already held but watching"


def test_unknown_ticker_is_marked_new(tmp_path: Path) -> None:
    """Tickers absent from both CSV files should be marked New."""
    config_dir = tmp_path / "config"
    write_settings(config_dir, tmp_path / "data" / "test.duckdb")
    write_portfolio_files(config_dir)

    status = portfolio_status_for_ticker("NEW", "S&P 500", config_dir)

    assert status["portfolio_status"] == "New"


def test_market_mismatch_does_not_mark_ticker_as_held() -> None:
    """Market can act as secondary validation when both values are available."""
    status = portfolio_status_from_data(
        "AAA",
        "FTSE 350",
        {
            "holdings": {
                "AAA": {
                    "ticker": "AAA",
                    "market": "S&P 500",
                    "quantity": "12",
                }
            },
            "watchlist": {},
        },
    )

    assert status["portfolio_status"] == "New"
