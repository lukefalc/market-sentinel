"""Helpers for updating stock universe CSV files from public sources.

This module supports updating public stock universes from Wikipedia. It only
updates local CSV files; it does not load prices or run analytics.
"""

from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests

from market_sentinel.data.universe_loader import REQUIRED_COLUMNS

SP500_WIKIPEDIA_URL = (
    "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
)
FTSE100_WIKIPEDIA_URL = (
    "https://en.wikipedia.org/wiki/FTSE_100_Index"
)
FTSE250_WIKIPEDIA_URL = (
    "https://en.wikipedia.org/wiki/FTSE_250_Index"
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


def update_ftse100_universe_csv(
    output_path: Optional[Path] = None,
    source_url: str = FTSE100_WIKIPEDIA_URL,
) -> Path:
    """Download the FTSE 100 table from Wikipedia and save it as a CSV file."""
    csv_path = (
        Path(output_path)
        if output_path is not None
        else Path("config") / "universes" / "ftse_100.csv"
    )

    html_text = _download_page_html(source_url, "FTSE 100")

    try:
        tables = pd.read_html(html_text)
    except (ImportError, ValueError, OSError) as error:
        raise RuntimeError(
            "Could not read the FTSE 100 table from the downloaded Wikipedia "
            "page. Check that pandas HTML support is installed and that the "
            "Wikipedia page layout has not changed. "
            f"Underlying error: {type(error).__name__}: {error}"
        ) from error

    source_table = _find_ftse100_table(tables)
    universe = _convert_ftse100_table(source_table)

    try:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        universe.to_csv(csv_path, index=False)
    except OSError as error:
        raise RuntimeError(
            "Could not save the FTSE 100 universe CSV file. Check that the "
            f"folder is writable: {csv_path.parent}"
        ) from error

    return csv_path


def update_ftse350_universe_csv(
    output_path: Optional[Path] = None,
    ftse100_source_url: str = FTSE100_WIKIPEDIA_URL,
    ftse250_source_url: str = FTSE250_WIKIPEDIA_URL,
) -> Path:
    """Build FTSE 350 from FTSE 100 plus FTSE 250 and save it as a CSV."""
    csv_path = (
        Path(output_path)
        if output_path is not None
        else Path("config") / "universes" / "ftse_350.csv"
    )

    ftse100_html = _download_page_html(ftse100_source_url, "FTSE 100")
    ftse250_html = _download_page_html(ftse250_source_url, "FTSE 250")
    ftse100_tables = _read_html_tables(ftse100_html, "FTSE 100")
    ftse250_tables = _read_html_tables(ftse250_html, "FTSE 250")
    ftse100_table = _find_ftse100_table(ftse100_tables)
    ftse250_table = _find_ftse250_table(ftse250_tables)
    ftse100_universe = _convert_ftse_table(ftse100_table, market_name="FTSE 350")
    ftse250_universe = _convert_ftse_table(ftse250_table, market_name="FTSE 350")
    combined_universe = pd.concat(
        [ftse100_universe, ftse250_universe],
        ignore_index=True,
    )
    universe = (
        combined_universe.drop_duplicates(subset=["ticker"], keep="first")
        .sort_values("ticker")
        .reset_index(drop=True)
    )
    duplicate_rows_removed = len(combined_universe) - len(universe)

    try:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        universe.to_csv(csv_path, index=False)
    except OSError as error:
        raise RuntimeError(
            "Could not save the FTSE 350 universe CSV file. Check that the "
            f"folder is writable: {csv_path.parent}"
        ) from error

    print(f"FTSE 100 rows found: {len(ftse100_universe)}")
    print(f"FTSE 250 rows found: {len(ftse250_universe)}")
    print(f"Combined rows written: {len(universe)}")
    print(f"Duplicate rows removed: {duplicate_rows_removed}")

    return csv_path


def _download_page_html(source_url: str, universe_name: str = "S&P 500") -> str:
    """Download a webpage using a browser-style User-Agent header."""
    try:
        response = requests.get(
            source_url,
            headers={"User-Agent": WIKIPEDIA_USER_AGENT},
            timeout=WIKIPEDIA_REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as error:
        raise RuntimeError(
            f"Could not download the {universe_name} table from Wikipedia. Check your "
            "internet connection and try again. "
            f"Underlying error: {type(error).__name__}: {error}"
        ) from error

    if response.status_code < 200 or response.status_code >= 300:
        raise RuntimeError(
            f"Could not download the {universe_name} table from Wikipedia. Wikipedia "
            f"returned HTTP status {response.status_code}. Try again later."
        )

    return response.text


def _read_html_tables(html_text: str, universe_name: str) -> List[pd.DataFrame]:
    """Parse HTML tables with a clear error for the named universe."""
    try:
        return pd.read_html(html_text)
    except (ImportError, ValueError, OSError) as error:
        raise RuntimeError(
            f"Could not read the {universe_name} constituents table from the "
            "downloaded Wikipedia page. Check that pandas HTML support is "
            "installed and that the Wikipedia page layout has not changed. "
            f"Underlying error: {type(error).__name__}: {error}"
        ) from error


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


def _find_ftse100_table(tables: List[pd.DataFrame]) -> pd.DataFrame:
    """Find the Wikipedia table containing FTSE 100 constituents."""
    candidate_tables = []
    for table in tables:
        columns = {_normalise_column_name(column) for column in table.columns}
        has_company = bool(columns & {"company", "constituent", "name"})
        has_ticker = bool(columns & {"ticker", "epic"})
        if has_company and has_ticker:
            candidate_tables.append(_flatten_column_names(table))

    if candidate_tables:
        return max(candidate_tables, key=len)

    raise RuntimeError(
        "Could not find the FTSE 100 constituents table in the Wikipedia page. "
        "The page layout may have changed."
    )


def _find_ftse250_table(tables: List[pd.DataFrame]) -> pd.DataFrame:
    """Find the Wikipedia table containing FTSE 250 constituents."""
    candidate_tables = []
    for table in tables:
        columns = {_normalise_column_name(column) for column in table.columns}
        has_company = bool(columns & {"company", "constituent", "name"})
        has_ticker = bool(columns & {"ticker", "epic"})
        if has_company and has_ticker:
            candidate_tables.append(_flatten_column_names(table))

    if candidate_tables:
        return max(candidate_tables, key=len)

    raise RuntimeError(
        "Could not find the FTSE 250 constituents table in the Wikipedia page. "
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


def _convert_ftse100_table(table: pd.DataFrame) -> pd.DataFrame:
    """Convert the Wikipedia FTSE 100 table to the project CSV format."""
    return _convert_ftse_table(table, market_name="FTSE 100")


def _convert_ftse_table(table: pd.DataFrame, market_name: str) -> pd.DataFrame:
    """Convert a Wikipedia FTSE table to the project CSV format."""
    table = _flatten_column_names(table)
    ticker_column = _first_present_column(table, ["Ticker", "EPIC"])
    name_column = _first_present_column(table, ["Company", "Constituent", "Name"])
    sector_column = _first_present_column(
        table,
        [
            "FTSE Industry Classification Benchmark sector",
            "FTSE Russell Industry",
            "ICB Sector",
            "Industry",
            "Sector",
        ],
        required=False,
    )

    if ticker_column is None or name_column is None:
        raise RuntimeError(
            f"The {market_name} table is missing expected ticker or company "
            "columns. Wikipedia may have changed."
        )

    if sector_column is None:
        sectors = ""
    else:
        sectors = table[sector_column].astype(str).str.strip()

    universe = pd.DataFrame(
        {
            "ticker": table[ticker_column].apply(_to_london_yfinance_ticker),
            "name": table[name_column].astype(str).str.strip(),
            "market": market_name,
            "region": "UK",
            "currency": "GBP",
            "sector": sectors,
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
            f"The {market_name} table was found, but no valid rows could be parsed."
        )

    universe = universe.drop_duplicates(subset=["ticker"], keep="first")
    return universe.sort_values("ticker").reset_index(drop=True)


def _first_present_column(
    table: pd.DataFrame,
    names: List[str],
    required: bool = True,
) -> Optional[str]:
    """Return the first matching column name from a DataFrame."""
    normalised_to_column = {
        _normalise_column_name(column): column for column in table.columns
    }

    for name in names:
        exact_column = normalised_to_column.get(_normalise_column_name(name))
        if exact_column is not None:
            return exact_column

    if required:
        raise RuntimeError(
            "A required column was not found in the downloaded universe table."
        )

    return None


def _to_yfinance_ticker(ticker: object) -> str:
    """Convert a Wikipedia ticker to the format expected by yfinance."""
    if pd.isna(ticker):
        return ""

    return str(ticker).strip().replace(".", "-")


def _to_london_yfinance_ticker(ticker: object) -> str:
    """Convert a London Stock Exchange ticker to yfinance format."""
    if pd.isna(ticker):
        return ""

    value = str(ticker).strip().upper()
    if not value:
        return ""

    if value.endswith(".L"):
        return value

    value = value.replace(".", "-").replace(" ", "")
    return f"{value}.L"


def _flatten_column_names(table: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with readable single-level column names."""
    flattened = table.copy()

    if getattr(flattened.columns, "nlevels", 1) > 1:
        flattened.columns = [
            " ".join(str(part) for part in column if str(part) != "nan").strip()
            for column in flattened.columns
        ]

    return flattened


def _normalise_column_name(column: object) -> str:
    """Normalise source column names for loose matching."""
    return " ".join(str(column).strip().lower().split())
