"""Send the market-sentinel daily alert email.

Run this script from the project root with:

    python3 scripts/send_daily_alert_email.py
"""

import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.alerts.email_notifier import send_daily_alert_email  # noqa: E402
from market_sentinel.database.connection import open_duckdb_connection  # noqa: E402
from market_sentinel.database.schema import initialise_database_schema  # noqa: E402


def main() -> None:
    """Send the daily alert email if email alerts are enabled."""
    load_dotenv()
    connection = None

    try:
        connection = open_duckdb_connection()
        initialise_database_schema(connection)
        send_daily_alert_email(connection)
    except (RuntimeError, ValueError, FileNotFoundError) as error:
        print(f"Daily alert email failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
