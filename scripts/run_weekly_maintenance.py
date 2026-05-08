"""Run the preferred weekly Market Sentinel maintenance command.

This is a thin wrapper around the existing working weekly full process, so both
commands run the same workflow:

    PYTHONPATH=src python3 scripts/run_weekly_maintenance.py
    PYTHONPATH=src python3 scripts/run_weekly_full.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import run_weekly_full


def weekly_maintenance_steps():
    """Return the same ordered workflow as the weekly full process."""
    return run_weekly_full.weekly_full_steps()


def main() -> None:
    """Run the existing weekly full workflow."""
    print("Running weekly maintenance using the weekly full workflow.")
    run_weekly_full.main()


if __name__ == "__main__":
    main()
