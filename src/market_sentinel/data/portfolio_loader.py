"""Load simple local portfolio holdings and watchlist CSV files."""

import csv
from pathlib import Path
from typing import Any, Dict, Optional

from market_sentinel.config.loader import default_config_dir, load_named_config

DEFAULT_HOLDINGS_PATH = "config/portfolio/holdings.csv"
DEFAULT_WATCHLIST_PATH = "config/portfolio/watchlist.csv"
HOLDINGS_COLUMNS = ["ticker", "name", "market", "quantity", "average_cost", "notes"]
WATCHLIST_COLUMNS = ["ticker", "name", "market", "reason", "notes"]


def load_portfolio_data(config_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """Load holdings and watchlist rows, treating missing files as empty."""
    settings = _load_settings(config_dir)
    holdings_path = _resolve_portfolio_path(
        settings.get("portfolio_holdings_path", DEFAULT_HOLDINGS_PATH),
        config_dir,
    )
    watchlist_path = _resolve_portfolio_path(
        settings.get("portfolio_watchlist_path", DEFAULT_WATCHLIST_PATH),
        config_dir,
    )

    return {
        "holdings": _load_csv_by_ticker(holdings_path, HOLDINGS_COLUMNS),
        "watchlist": _load_csv_by_ticker(watchlist_path, WATCHLIST_COLUMNS),
    }


def portfolio_status_for_ticker(
    ticker: str,
    market: Optional[str] = None,
    config_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Return held/watchlist status details for one ticker."""
    portfolio_data = load_portfolio_data(config_dir)
    return portfolio_status_from_data(
        ticker,
        market,
        portfolio_data,
    )


def portfolio_status_from_data(
    ticker: str,
    market: Optional[str],
    portfolio_data: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Return held/watchlist status details using already-loaded data."""
    holding = _matching_row(
        ticker,
        market,
        portfolio_data.get("holdings", {}),
    )
    watchlist = _matching_row(
        ticker,
        market,
        portfolio_data.get("watchlist", {}),
    )
    is_held = holding is not None
    is_watchlist = watchlist is not None

    if is_held and is_watchlist:
        status = "Held + Watchlist"
    elif is_held:
        status = "Held"
    elif is_watchlist:
        status = "Watchlist"
    else:
        status = "New"

    return {
        "portfolio_status": status,
        "holding_quantity": (holding or {}).get("quantity", ""),
        "watchlist_reason": (watchlist or {}).get("reason", ""),
    }


def _load_settings(config_dir: Optional[Path]) -> Dict[str, Any]:
    """Load settings, falling back to defaults if settings are missing."""
    try:
        return load_named_config("settings", config_dir)
    except FileNotFoundError:
        return {}


def _resolve_portfolio_path(raw_path: Any, config_dir: Optional[Path]) -> Path:
    """Resolve configured portfolio CSV paths in a project-friendly way."""
    path = Path(str(raw_path)).expanduser()

    if path.is_absolute():
        return path

    base_config_dir = Path(config_dir) if config_dir is not None else default_config_dir()

    if path.parts and path.parts[0] == "config":
        return base_config_dir.parent / path

    return base_config_dir / path


def _load_csv_by_ticker(path: Path, required_columns: list) -> Dict[str, Any]:
    """Read a CSV into a ticker-keyed dictionary."""
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(
            row for row in csv_file
            if row.strip() and not row.lstrip().startswith("#")
        )

        if reader.fieldnames is None:
            return {}

        missing_columns = [
            column for column in required_columns
            if column not in reader.fieldnames
        ]
        if missing_columns:
            raise ValueError(
                f"Portfolio CSV is missing required columns in {path}: "
                f"{', '.join(missing_columns)}"
            )

        rows: Dict[str, Any] = {}
        for row in reader:
            ticker_key = _normalise_ticker(row.get("ticker"))
            if not ticker_key:
                continue
            rows[ticker_key] = {
                column: (row.get(column) or "").strip()
                for column in required_columns
            }

    return rows


def _matching_row(
    ticker: str,
    market: Optional[str],
    rows_by_ticker: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Find a ticker row, using market as secondary validation when present."""
    row = rows_by_ticker.get(_normalise_ticker(ticker))
    if row is None:
        return None

    row_market = str(row.get("market") or "").strip()
    candidate_market = str(market or "").strip()
    if row_market and candidate_market and row_market != candidate_market:
        return None

    return row


def _normalise_ticker(value: Any) -> str:
    """Normalize tickers for simple CSV matching."""
    return str(value or "").strip().upper()
