"""Run the full market-sentinel daily process.

Run this script from the project root with:

    python3 scripts/run_daily_process.py
"""

import sys
from pathlib import Path
from typing import Callable, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.analytics.crossovers import (  # noqa: E402
    detect_and_store_crossovers,
)
from market_sentinel.analytics.dividends import (  # noqa: E402
    calculate_and_store_dividends,
)
from market_sentinel.analytics.moving_averages import (  # noqa: E402
    calculate_and_store_moving_averages,
)
from market_sentinel.analytics.risk_flags import (  # noqa: E402
    calculate_and_store_risk_flags,
)
from market_sentinel.data.price_loader import update_daily_prices  # noqa: E402
from market_sentinel.data.universe_loader import (  # noqa: E402
    default_universe_files,
    load_universe_files,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402
from market_sentinel.reports.excel_report import generate_excel_report  # noqa: E402
from market_sentinel.reports.pdf_report import generate_pdf_report  # noqa: E402

Step = Tuple[str, Callable]


def main() -> None:
    """Run each daily process step in order."""
    connection = None
    step_name = "Open database"

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)

        for step_name, step_function in daily_steps():
            print(f"Starting: {step_name}")
            result = step_function(connection)
            _print_step_result(step_name, result)
            print(f"Finished: {step_name}")
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Daily process failed during: {step_name}", file=sys.stderr)
        print(f"Reason: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print("Daily process completed successfully.")


def daily_steps() -> List[Step]:
    """Return the ordered daily process steps."""
    return [
        ("Load universe", _load_universe),
        ("Update market data", update_daily_prices),
        ("Calculate moving averages", calculate_and_store_moving_averages),
        ("Detect crossovers", detect_and_store_crossovers),
        ("Calculate dividends", calculate_and_store_dividends),
        ("Calculate risk flags", calculate_and_store_risk_flags),
        ("Generate Excel report", generate_excel_report),
        ("Generate PDF report", generate_pdf_report),
    ]


def _load_universe(connection):
    """Load configured universe CSV files."""
    return load_universe_files(connection, default_universe_files())


def _print_step_result(step_name: str, result) -> None:
    """Print a short result summary for one step."""
    if result is None:
        return

    if isinstance(result, Path):
        print(f"{step_name} output: {result}")
        return

    if isinstance(result, dict):
        for key, value in result.items():
            print(f"{key}: {value}")
        return

    print(f"{step_name} result: {result}")


if __name__ == "__main__":
    main()
