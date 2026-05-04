"""Notification helpers.

Email alert support lives in ``market_sentinel.alerts.email_notifier``. This
module re-exports the public helpers so imports stay simple.
"""

from market_sentinel.alerts.email_notifier import (
    build_daily_alert_email_body,
    load_email_settings,
    send_daily_alert_email,
)

__all__ = [
    "build_daily_alert_email_body",
    "load_email_settings",
    "send_daily_alert_email",
]
