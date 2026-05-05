"""Stock universe CSV loading helpers.

This module loads small local CSV files from ``config/universes`` and writes the
rows to the ``securities`` table. It does not download market data.
"""

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import duckdb

from market_sentinel.config.loader import default_config_dir

REQUIRED_COLUMNS = ["ticker", "name", "market", "region", "currency", "sector"]
REQUIRED_VALUE_COLUMNS = ["ticker", "name", "market", "region", "currency"]


def default_universe_dir() -> Path:
    """Return the default folder for stock universe CSV files."""
    return default_config_dir() / "universes"


def read_universe_csv(csv_path: Path) -> List[Dict[str, str]]:
    """Read and validate one stock universe CSV file."""
    csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(
            "Stock universe CSV file not found: "
            f"{csv_path}. Add the file or check that the path is correct."
        )

    try:
        with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            fieldnames = reader.fieldnames or []
            missing_columns = [
                column for column in REQUIRED_COLUMNS if column not in fieldnames
            ]

            if missing_columns:
                raise ValueError(
                    "Stock universe CSV is missing required columns: "
                    f"{', '.join(missing_columns)}. "
                    "Expected columns are: "
                    f"{', '.join(REQUIRED_COLUMNS)}."
                )

            rows = []
            for row_number, row in enumerate(reader, start=2):
                _validate_csv_row(row, row_number, csv_path)
                clean_row = {
                    column: (row.get(column) or "").strip()
                    for column in REQUIRED_COLUMNS
                }
                _validate_required_values(clean_row, row_number, csv_path)
                rows.append(clean_row)
    except csv.Error as error:
        raise ValueError(
            "Could not read the stock universe CSV file: "
            f"{csv_path}. Check that it is a valid CSV file."
        ) from error

    return rows


def _validate_csv_row(row: Dict[str, str], row_number: int, csv_path: Path) -> None:
    """Validate that a CSV row has the expected shape."""
    if None in row:
        raise ValueError(
            "Stock universe CSV row has too many values: "
            f"{csv_path}, row {row_number}. "
            "Check for extra commas or unquoted company names containing commas."
        )


def _validate_required_values(
    row: Dict[str, str],
    row_number: int,
    csv_path: Path,
) -> None:
    """Validate required values in one CSV row."""
    missing_values = [
        column for column in REQUIRED_VALUE_COLUMNS if not row.get(column)
    ]

    if missing_values:
        raise ValueError(
            "Stock universe CSV row is missing required values: "
            f"{', '.join(missing_values)} in {csv_path}, row {row_number}. "
            "Each row must include ticker, name, market, region, and currency."
        )


def load_universe_csv(
    connection: duckdb.DuckDBPyConnection,
    csv_path: Path,
) -> int:
    """Insert or update securities from one CSV file.

    Returns:
        The number of CSV rows written to the database.
    """
    rows = read_universe_csv(csv_path)

    try:
        for row in rows:
            existing_id = connection.execute(
                "SELECT security_id FROM securities WHERE ticker = ?",
                [row["ticker"]],
            ).fetchone()

            if existing_id:
                connection.execute(
                    """
                    UPDATE securities
                    SET name = ?,
                        market = ?,
                        region = ?,
                        currency = ?,
                        sector = ?
                    WHERE ticker = ?
                    """,
                    [
                        row["name"],
                        row["market"],
                        row["region"],
                        row["currency"],
                        row["sector"],
                        row["ticker"],
                    ],
                )
            else:
                next_id = connection.execute(
                    "SELECT COALESCE(MAX(security_id), 0) + 1 FROM securities"
                ).fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO securities (
                        security_id,
                        ticker,
                        name,
                        market,
                        region,
                        currency,
                        sector
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        next_id,
                        row["ticker"],
                        row["name"],
                        row["market"],
                        row["region"],
                        row["currency"],
                        row["sector"],
                    ],
                )
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not save the stock universe to DuckDB. Check that the "
            "database is open and the securities table has been created."
        ) from error

    return len(rows)


def load_universe_files(
    connection: duckdb.DuckDBPyConnection,
    csv_paths: Iterable[Path],
) -> Dict[str, int]:
    """Load multiple stock universe CSV files."""
    return {
        Path(csv_path).name: load_universe_csv(connection, Path(csv_path))
        for csv_path in csv_paths
    }


def default_universe_files(universe_dir: Optional[Path] = None) -> List[Path]:
    """Return the default FTSE and S&P universe CSV paths."""
    base_dir = (
        Path(universe_dir) if universe_dir is not None else default_universe_dir()
    )
    return [
        base_dir / "ftse_100.csv",
        base_dir / "ftse_350.csv",
        base_dir / "sp_500.csv",
    ]
