"""Helpers for updating stock universe CSV files from public sources.

This module currently supports updating the S&P 500 universe from Wikipedia.
It only updates the local CSV file; it does not load prices or run analytics.
"""

from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests

from market_sentinel.data.universe_loader import REQUIRED_COLUMNS

SP500_WIKIPEDIA_URL = (
    "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
)
WIKIPEDIA_REQUEST_TIMEOUT_SECONDS = 20
WIKIPEDIA_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36 market-sentinel/0.1"
)


def update_sp500_universe_csv(
    output_path: Optional[Path] = None,
    source_url: str = SP500_WIKIPEDIA_URL,
) -> Path:
    """Download the S&P 500 table from Wikipedia and save it as a CSV file."""
    csv_path = (
        Path(output_path)
        if output_path is not None
        else Path("config") / "universes" / "sp_500.csv"
    )

    html_text = _download_page_html(source_url)

    try:
        tables = pd.read_html(html_text)
    except (ImportError, ValueError, OSError) as error:
        raise RuntimeError(
            "Could not read the S&P 500 table from the downloaded Wikipedia "
            "page. Check that pandas HTML support is installed and that the "
            "Wikipedia page layout has not changed. "
            f"Underlying error: {type(error).__name__}: {error}"
        ) from error

    source_table = _find_sp500_table(tables)
    universe = _convert_sp500_table(source_table)

    try:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        universe.to_csv(csv_path, index=False)
    except OSError as error:
        raise RuntimeError(
            "Could not save the S&P 500 universe CSV file. Check that the "
            f"folder is writable: {csv_path.parent}"
        ) from error

    return csv_path


def _download_page_html(source_url: str) -> str:
    """Download a webpage using a browser-style User-Agent header."""
    try:
        response = requests.get(
            source_url,
            headers={"User-Agent": WIKIPEDIA_USER_AGENT},
            timeout=WIKIPEDIA_REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as error:
        raise RuntimeError(
            "Could not download the S&P 500 table from Wikipedia. Check your "
            "internet connection and try again. "
            f"Underlying error: {type(error).__name__}: {error}"
        ) from error

    if response.status_code < 200 or response.status_code >= 300:
        raise RuntimeError(
            "Could not download the S&P 500 table from Wikipedia. Wikipedia "
            f"returned HTTP status {response.status_code}. Try again later."
        )

    return response.text


def _find_sp500_table(tables: List[pd.DataFrame]) -> pd.DataFrame:
    """Find the Wikipedia table containing S&P 500 constituents."""
    required_source_columns = {"Symbol", "Security", "GICS Sector"}

    for table in tables:
        if required_source_columns.issubset(set(table.columns)):
            return table

    raise RuntimeError(
        "Could not find the S&P 500 constituents table in the Wikipedia page. "
        "The page layout may have changed."
    )


def _convert_sp500_table(table: pd.DataFrame) -> pd.DataFrame:
    """Convert the Wikipedia S&P 500 table to the project CSV format."""
    missing_columns = [
        column
        for column in ["Symbol", "Security", "GICS Sector"]
        if column not in table.columns
    ]

    if missing_columns:
        raise RuntimeError(
            "The S&P 500 table is missing expected columns: "
            f"{', '.join(missing_columns)}. Wikipedia may have changed."
        )

    universe = pd.DataFrame(
        {
            "ticker": table["Symbol"].apply(_to_yfinance_ticker),
            "name": table["Security"].astype(str).str.strip(),
            "market": "S&P 500",
            "region": "US",
            "currency": "USD",
            "sector": table["GICS Sector"].astype(str).str.strip(),
        }
    )

    universe = universe[REQUIRED_COLUMNS]
    universe = universe.dropna(subset=["ticker", "name"])
    universe = universe[
        (universe["ticker"].astype(str).str.strip() != "")
        & (universe["name"].astype(str).str.strip() != "")
    ]

    if universe.empty:
        raise RuntimeError(
            "The S&P 500 table was found, but no valid rows could be parsed."
        )

    return universe.sort_values("ticker").reset_index(drop=True)


def _to_yfinance_ticker(ticker: object) -> str:
    """Convert a Wikipedia ticker to the format expected by yfinance."""
    if pd.isna(ticker):
        return ""

    return str(ticker).strip().replace(".", "-")
