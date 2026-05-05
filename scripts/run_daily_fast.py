"""Run the fast daily market-sentinel process.

Run this script from the project root with:

    PYTHONPATH=src python3 scripts/run_daily_fast.py
"""

import sys
import time
from pathlib import Path
from typing import Callable, List, Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.analytics.crossovers import detect_and_store_crossovers  # noqa: E402
from market_sentinel.analytics.dividends import calculate_and_store_dividends  # noqa: E402
from market_sentinel.analytics.moving_averages import (  # noqa: E402
    DEFAULT_MOVING_AVERAGE_INCREMENTAL_RECENT_DAYS,
    calculate_and_store_incremental_moving_averages,
)
from market_sentinel.analytics.risk_flags import calculate_and_store_risk_flags  # noqa: E402
from market_sentinel.config.loader import load_named_config  # noqa: E402
from market_sentinel.data.price_loader import (  # noqa: E402
    DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
    DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
    DEFAULT_PRICE_UPDATE_OVERLAP_DAYS,
    update_incremental_daily_prices,
)
from market_sentinel.data.universe_loader import default_universe_files, load_universe_files  # noqa: E402
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402
from market_sentinel.reports.charts import generate_charts  # noqa: E402
from market_sentinel.reports.excel_report import generate_excel_report  # noqa: E402
from market_sentinel.reports.pdf_report import generate_pdf_report  # noqa: E402

Step = Tuple[str, Callable]


def main() -> None:
    """Run each fast daily step with timing logs."""
    load_dotenv()
    connection = None
    step_name = "Open database"
    timings = []

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)

        for step_name, step_function in daily_fast_steps():
            start_time = time.perf_counter()
            print(f"Starting: {step_name}")
            result = step_function(connection)
            elapsed = time.perf_counter() - start_time
            timings.append((step_name, elapsed))
            _print_step_result(step_name, result)
            print(f"Finished: {step_name} in {elapsed:.1f}s")
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Fast daily process failed during: {step_name}", file=sys.stderr)
        print(f"Reason: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    _print_timing_summary(timings)
    print("Fast daily process completed successfully.")


def daily_fast_steps() -> List[Step]:
    """Return the ordered fast daily process steps."""
    steps: List[Step] = [
        ("Load universe", _load_universe),
        ("Update market data incrementally", _update_market_data_incremental),
        ("Calculate moving averages incrementally", _calculate_moving_averages_incremental),
        ("Detect crossovers", detect_and_store_crossovers),
        ("Calculate risk flags", calculate_and_store_risk_flags),
        ("Generate charts", generate_charts),
        ("Generate PDF report", generate_pdf_report),
        ("Generate Excel report", generate_excel_report),
    ]

    if _run_dividends_in_daily_fast():
        steps.insert(4, ("Calculate dividends", calculate_and_store_dividends))

    return steps


def _load_universe(connection):
    """Load configured universe CSV files."""
    return load_universe_files(connection, default_universe_files())


def _update_market_data_incremental(connection):
    """Run incremental market data updates."""
    settings = load_named_config("settings")
    return update_incremental_daily_prices(
        connection,
        batch_size=int(settings.get("price_download_batch_size", DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE)),
        overlap_days=int(settings.get("price_update_overlap_days", DEFAULT_PRICE_UPDATE_OVERLAP_DAYS)),
        pause_seconds=float(settings.get("price_download_pause_seconds", DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS)),
    )


def _calculate_moving_averages_incremental(connection):
    """Run incremental moving average calculations."""
    settings = load_named_config("settings")
    return calculate_and_store_incremental_moving_averages(
        connection,
        recent_days=int(
            settings.get(
                "moving_average_incremental_recent_days",
                DEFAULT_MOVING_AVERAGE_INCREMENTAL_RECENT_DAYS,
            )
        ),
    )


def _run_dividends_in_daily_fast() -> bool:
    """Return whether the fast daily process should include dividends."""
    settings = load_named_config("settings")
    raw_value = settings.get("run_dividends_in_daily_fast", False)

    if isinstance(raw_value, bool):
        return raw_value

    if isinstance(raw_value, str):
        return raw_value.strip().lower() in {"1", "true", "yes", "on"}

    return False


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


def _print_timing_summary(timings) -> None:
    """Print final timing summary."""
    print("Fast daily timing summary")
    for step_name, elapsed in timings:
        print(f"- {step_name}: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
