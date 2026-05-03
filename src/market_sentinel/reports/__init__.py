"""Report generation package.

This package turns analysis results into Excel reports. PDF reports are not
implemented yet.
"""

from market_sentinel.reports.excel_report import generate_excel_report

__all__ = ["generate_excel_report"]
