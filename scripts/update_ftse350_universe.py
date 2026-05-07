"""Update the local FTSE 350 universe CSV from Wikipedia.

Run this script from the project root with:

    PYTHONPATH=src python3 scripts/update_ftse350_universe.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from market_sentinel.data.universe_sources import (  # noqa: E402
    update_ftse350_universe_csv,
)


def main() -> None:
    """Update the FTSE 350 universe CSV file."""
    output_path = PROJECT_ROOT / "config" / "universes" / "ftse_350.csv"

    try:
        saved_path = update_ftse350_universe_csv(output_path)
    except RuntimeError as error:
        print(f"FTSE 350 universe update failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error

    print(f"Updated FTSE 350 universe CSV: {saved_path}")
    print("Next step: load the universe with python3 scripts/load_universe.py")


if __name__ == "__main__":
    main()
