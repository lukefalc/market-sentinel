"""Plain-text SMTP email summaries for market-sentinel."""

import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any, List, Mapping, Optional

import duckdb


@dataclass
class EmailSettings:
    """SMTP email settings loaded from environment variables."""

    enabled: bool
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    email_from: Optional[str] = None
    email_to: Optional[str] = None


def load_email_settings(
    environment: Optional[Mapping[str, str]] = None,
) -> EmailSettings:
    """Load email settings from environment variables."""
    env = os.environ if environment is None else environment
    enabled = _is_enabled(env.get("MARKET_SENTINEL_EMAIL_ENABLED", "false"))

    if not enabled:
        return EmailSettings(enabled=False)

    missing = [
        name
        for name in [
            "MARKET_SENTINEL_SMTP_HOST",
            "MARKET_SENTINEL_SMTP_USERNAME",
            "MARKET_SENTINEL_SMTP_PASSWORD",
            "MARKET_SENTINEL_EMAIL_FROM",
            "MARKET_SENTINEL_EMAIL_TO",
        ]
        if not env.get(name)
    ]

    if missing:
        raise ValueError(
            "Email alerts are enabled, but these environment variables are "
            f"missing: {', '.join(missing)}. Add them to your .env file or "
            "turn email off with MARKET_SENTINEL_EMAIL_ENABLED=false."
        )

    try:
        smtp_port = int(env.get("MARKET_SENTINEL_SMTP_PORT", "587"))
    except ValueError as error:
        raise ValueError(
            "MARKET_SENTINEL_SMTP_PORT must be a whole number, such as 587."
        ) from error

    return EmailSettings(
        enabled=True,
        smtp_host=env["MARKET_SENTINEL_SMTP_HOST"],
        smtp_port=smtp_port,
        smtp_username=env["MARKET_SENTINEL_SMTP_USERNAME"],
        smtp_password=env["MARKET_SENTINEL_SMTP_PASSWORD"],
        email_from=env["MARKET_SENTINEL_EMAIL_FROM"],
        email_to=env["MARKET_SENTINEL_EMAIL_TO"],
    )


def send_daily_alert_email(
    connection: duckdb.DuckDBPyConnection,
    environment: Optional[Mapping[str, str]] = None,
    smtp_factory=smtplib.SMTP,
) -> bool:
    """Send the daily alert email if email alerts are enabled."""
    settings = load_email_settings(environment)

    if not settings.enabled:
        print("Email alerts are disabled. No email was sent.")
        return False

    body = build_daily_alert_email_body(connection)
    message = EmailMessage()
    message["Subject"] = "Market Sentinel daily alert summary"
    message["From"] = settings.email_from
    message["To"] = settings.email_to
    message.set_content(body)

    try:
        with smtp_factory(settings.smtp_host, settings.smtp_port) as smtp:
            smtp.starttls()
            smtp.login(settings.smtp_username, settings.smtp_password)
            smtp.send_message(message)
    except (OSError, smtplib.SMTPException) as error:
        raise RuntimeError(
            "Could not send the daily alert email. Check your SMTP host, port, "
            "username, password, and internet connection."
        ) from error

    print(f"Daily alert email sent to {settings.email_to}.")
    return True


def build_daily_alert_email_body(connection: duckdb.DuckDBPyConnection) -> str:
    """Build a plain-text daily alert summary from DuckDB."""
    try:
        sections = [
            ("Dividend Risk Flags", _fetch_dividend_risk_flags(connection)),
            ("Crossover Signals", _fetch_crossover_signals(connection)),
            ("High Dividend Stocks", _fetch_high_dividend_stocks(connection)),
        ]
    except duckdb.Error as error:
        raise RuntimeError(
            "Could not build the email summary from DuckDB. Check that the "
            f"database has been initialised and analytics have run. Details: {error}"
        ) from error

    lines = ["Market Sentinel daily alert summary", ""]

    for title, rows in sections:
        lines.append(title)
        lines.append("-" * len(title))

        if not rows:
            lines.append("No items found.")
        else:
            for row in rows:
                lines.append(_format_summary_row(row))

        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _is_enabled(value: str) -> bool:
    """Return whether an environment value means enabled."""
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _fetch_dividend_risk_flags(
    connection: duckdb.DuckDBPyConnection,
) -> List[Any]:
    """Fetch dividend risk flag rows."""
    return connection.execute(
        """
        SELECT
            securities.ticker,
            metrics.dividend_yield,
            metrics.dividend_risk_reason
        FROM dividend_metrics AS metrics
        INNER JOIN securities
            ON metrics.security_id = securities.security_id
        WHERE metrics.dividend_risk_flag IS NOT NULL
          AND metrics.dividend_risk_flag <> ''
        ORDER BY metrics.dividend_yield DESC, securities.ticker
        LIMIT 10
        """
    ).fetchall()


def _fetch_crossover_signals(
    connection: duckdb.DuckDBPyConnection,
) -> List[Any]:
    """Fetch crossover signal rows."""
    return connection.execute(
        """
        SELECT
            securities.ticker,
            signals.signal_date,
            signals.crossover_direction
        FROM moving_average_signals AS signals
        INNER JOIN securities
            ON signals.security_id = securities.security_id
        WHERE signals.signal_type IN ('BULLISH_CROSSOVER', 'BEARISH_CROSSOVER')
        ORDER BY signals.signal_date DESC, securities.ticker
        LIMIT 10
        """
    ).fetchall()


def _fetch_high_dividend_stocks(
    connection: duckdb.DuckDBPyConnection,
) -> List[Any]:
    """Fetch high dividend stocks."""
    return connection.execute(
        """
        SELECT
            securities.ticker,
            metrics.dividend_yield,
            metrics.trailing_annual_dividend
        FROM dividend_metrics AS metrics
        INNER JOIN securities
            ON metrics.security_id = securities.security_id
        WHERE metrics.dividend_yield IS NOT NULL
        ORDER BY metrics.dividend_yield DESC, securities.ticker
        LIMIT 10
        """
    ).fetchall()


def _format_summary_row(row: Any) -> str:
    """Format one database row for plain-text email output."""
    values = [_format_value(value) for value in row]
    return "- " + " | ".join(values)


def _format_value(value: Any) -> str:
    """Format a value for email output."""
    if value is None:
        return ""

    if isinstance(value, float):
        return f"{value:.4f}"

    return str(value)
