"""Compatibility imports for Excel report generation.

The real Excel report generator lives in ``market_sentinel.reports.excel_report``.
This module re-exports it so older imports still use the updated report code.
"""

from market_sentinel.reports.excel_report import (
    EXPECTED_WORKSHEET_TITLES,
    default_report_filename,
    generate_excel_report,
)

__all__ = [
    "EXPECTED_WORKSHEET_TITLES",
    "default_report_filename",
    "generate_excel_report",
]
