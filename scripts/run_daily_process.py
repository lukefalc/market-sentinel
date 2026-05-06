"""Run the normal market-sentinel daily process.

Run this script from the project root with:

    python3 scripts/run_daily_process.py

To refresh dividends during a one-off daily run:

    python3 scripts/run_daily_process.py --include-dividends
"""

import argparse
import sys
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.alerts.email_notifier import send_daily_alert_email  # noqa: E402
from market_sentinel.analytics.crossovers import (  # noqa: E402
    detect_and_store_crossovers,
)
from market_sentinel.analytics.dividends import (  # noqa: E402
    calculate_and_store_dividends,
)
from market_sentinel.analytics.moving_averages import (  # noqa: E402
    DEFAULT_MOVING_AVERAGE_INCREMENTAL_RECENT_DAYS,
    DEFAULT_MOVING_AVERAGE_PRICE_HISTORY_BUFFER_DAYS,
    calculate_and_store_incremental_moving_averages,
)
from market_sentinel.analytics.risk_flags import (  # noqa: E402
    calculate_and_store_risk_flags,
)
from market_sentinel.config.loader import load_named_config  # noqa: E402
from market_sentinel.data.price_loader import (  # noqa: E402
    DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE,
    DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
    DEFAULT_PRICE_UPDATE_OVERLAP_DAYS,
    DEFAULT_PRICE_UPDATE_STALE_AFTER_DAYS,
    DEFAULT_SKIP_PRICE_UPDATE_IF_LATEST_DATE_IS_TODAY,
    update_incremental_daily_prices,
)
from market_sentinel.data.universe_loader import (  # noqa: E402
    default_universe_files,
    load_universe_files,
)
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402
from market_sentinel.reports.excel_report import generate_excel_report  # noqa: E402
from market_sentinel.reports.pdf_report import generate_pdf_report  # noqa: E402
from market_sentinel.utils.timing import print_timing_summary, timed_step  # noqa: E402

Step = Tuple[str, Callable]


def main(argv: Optional[List[str]] = None) -> None:
    """Run each daily process step in order."""
    args = parse_args(argv)
    load_dotenv()
    connection = None
    step_name = "Open database"
    timings = []

    try:
        with timed_step("Open database", timings):
            connection = open_duckdb_connection()
            initialise_database_schema(connection)

        include_dividends = True if args.include_dividends else None
        for step_name, step_function in daily_steps(include_dividends):
            with timed_step(step_name, timings):
                result = step_function(connection)
            _print_step_result(step_name, result)
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Daily process failed during: {step_name}", file=sys.stderr)
        print(f"Reason: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()

    print_timing_summary(timings)
    print("Daily process completed successfully.")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse daily process command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--include-dividends",
        action="store_true",
        help="refresh dividend history during this daily run",
    )
    return parser.parse_args(argv)


def daily_steps(include_dividends: Optional[bool] = None) -> List[Step]:
    """Return the ordered daily process steps."""
    if include_dividends is None:
        include_dividends = _run_dividends_in_daily_process()

    steps: List[Step] = [
        ("Load universe", _load_universe),
        ("Update market data", _update_market_data_daily),
        ("Calculate moving averages", _calculate_moving_averages_daily),
        ("Detect crossovers", detect_and_store_crossovers),
        ("Calculate risk flags", calculate_and_store_risk_flags),
        ("Generate charts", _generate_charts),
        ("Generate PDF", generate_pdf_report),
        ("Generate Excel", generate_excel_report),
        ("Send daily alert email", _send_daily_alert_email),
    ]

    dividend_step: Step
    if include_dividends:
        dividend_step = ("Calculate dividends", calculate_and_store_dividends)
    else:
        dividend_step = ("Calculate dividends: skipped", _skip_dividend_refresh)

    steps.insert(4, dividend_step)
    return steps


def _load_universe(connection):
    """Load configured universe CSV files."""
    return load_universe_files(connection, default_universe_files())


def _update_market_data_daily(connection):
    """Run the incremental daily market data update mode."""
    settings = load_named_config("settings")
    batch_size = int(
        settings.get("price_download_batch_size", DEFAULT_PRICE_DOWNLOAD_BATCH_SIZE)
    )
    overlap_days = int(
        settings.get("price_update_overlap_days", DEFAULT_PRICE_UPDATE_OVERLAP_DAYS)
    )
    pause_seconds = float(
        settings.get(
            "price_download_pause_seconds",
            DEFAULT_PRICE_DOWNLOAD_PAUSE_SECONDS,
        )
    )
    skip_if_latest_date_is_today = _coerce_bool(
        settings.get(
            "skip_price_update_if_latest_date_is_today",
            DEFAULT_SKIP_PRICE_UPDATE_IF_LATEST_DATE_IS_TODAY,
        )
    )
    stale_after_days = int(
        settings.get(
            "price_update_stale_after_days",
            DEFAULT_PRICE_UPDATE_STALE_AFTER_DAYS,
        )
    )

    return update_incremental_daily_prices(
        connection,
        batch_size=batch_size,
        overlap_days=overlap_days,
        pause_seconds=pause_seconds,
        skip_if_latest_date_is_today=skip_if_latest_date_is_today,
        stale_after_days=stale_after_days,
    )


def _calculate_moving_averages_daily(connection):
    """Run the default incremental moving-average calculation."""
    settings = load_named_config("settings")
    return calculate_and_store_incremental_moving_averages(
        connection,
        recent_days=int(
            settings.get(
                "moving_average_incremental_recent_days",
                DEFAULT_MOVING_AVERAGE_INCREMENTAL_RECENT_DAYS,
            )
        ),
        price_history_buffer_days=int(
            settings.get(
                "moving_average_price_history_buffer_days",
                DEFAULT_MOVING_AVERAGE_PRICE_HISTORY_BUFFER_DAYS,
            )
        ),
    )


def _run_dividends_in_daily_process() -> bool:
    """Return whether the normal daily process should refresh dividends."""
    settings = load_named_config("settings")
    raw_value = settings.get("run_dividends_in_daily_process", False)

    if isinstance(raw_value, bool):
        return raw_value

    if isinstance(raw_value, str):
        return raw_value.strip().lower() in {"1", "true", "yes", "on"}

    return False


def _coerce_bool(value) -> bool:
    """Convert YAML-style truthy values into a bool."""
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}

    return bool(value)


def _skip_dividend_refresh(connection):
    """Skip dividend downloads while keeping the step visible in logs."""
    print(
        "Skipping dividend refresh in daily process. Run weekly full process "
        "to refresh dividends."
    )
    return {"dividend_refresh": "skipped"}


def _send_daily_alert_email(connection):
    """Send the optional daily email alert summary."""
    try:
        email_sent = send_daily_alert_email(connection)
    except ValueError as error:
        print(
            "Daily alert email was not sent because the email settings are "
            f"incomplete. {error}"
        )
        return {"email_sent": False}

    return {"email_sent": email_sent}


def _generate_charts(connection):
    """Generate chart images for the daily process."""
    from market_sentinel.reports.charts import generate_charts

    return generate_charts(connection)


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
