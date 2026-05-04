"""Tests for SMTP email alert summaries."""

from pathlib import Path

import pytest

from market_sentinel.alerts.email_notifier import (
    build_daily_alert_email_body,
    load_email_settings,
    send_daily_alert_email,
)
from market_sentinel.database.connection import open_duckdb_connection
from market_sentinel.database.schema import initialise_database_schema


def write_settings(config_dir: Path, database_path: Path) -> None:
    """Create a minimal settings file pointing at a test database."""
    config_dir.mkdir(parents=True, exist_ok=True)
    config_dir.joinpath("settings.yaml").write_text(
        f"database_path: {database_path}\n",
        encoding="utf-8",
    )


def open_test_database(tmp_path: Path):
    """Open a temporary DuckDB database with the project schema."""
    config_dir = tmp_path / "config"
    database_path = tmp_path / "data" / "market_sentinel.duckdb"
    write_settings(config_dir, database_path)
    connection = open_duckdb_connection(config_dir)
    initialise_database_schema(connection)
    return connection


def insert_email_summary_data(connection) -> None:
    """Insert fake alert summary rows."""
    connection.execute(
        """
        INSERT INTO securities (
            security_id,
            ticker,
            name,
            market
        )
        VALUES (?, ?, ?, ?)
        """,
        [1, "AAA", "Example A", "S&P 500"],
    )
    connection.execute(
        """
        INSERT INTO dividend_metrics (
            metric_id,
            security_id,
            metric_date,
            trailing_annual_dividend,
            dividend_yield,
            dividend_risk_flag,
            dividend_risk_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            1,
            1,
            "2026-05-04",
            8.0,
            0.08,
            "DIVIDEND_TRAP_RISK",
            "Dividend yield is above 7%.",
        ],
    )
    connection.execute(
        """
        INSERT INTO moving_average_signals (
            signal_id,
            security_id,
            signal_date,
            moving_average_period_days,
            moving_average_value,
            comparison_period_days,
            comparison_moving_average_value,
            signal_type,
            crossover_direction
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            1,
            1,
            "2026-05-04",
            50,
            101.0,
            200,
            95.0,
            "BULLISH_CROSSOVER",
            "BULLISH_CROSSOVER",
        ],
    )


class FakeSmtp:
    """Fake SMTP client used by tests."""

    sent_messages = []

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.started_tls = False
        self.login_args = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return None

    def starttls(self):
        self.started_tls = True

    def login(self, username, password):
        self.login_args = (username, password)

    def send_message(self, message):
        self.sent_messages.append(message)


def enabled_environment():
    """Return complete fake email environment settings."""
    return {
        "MARKET_SENTINEL_EMAIL_ENABLED": "true",
        "MARKET_SENTINEL_SMTP_HOST": "smtp.example.com",
        "MARKET_SENTINEL_SMTP_PORT": "587",
        "MARKET_SENTINEL_SMTP_USERNAME": "sender@example.com",
        "MARKET_SENTINEL_SMTP_PASSWORD": "app-password",
        "MARKET_SENTINEL_EMAIL_FROM": "sender@example.com",
        "MARKET_SENTINEL_EMAIL_TO": "receiver@example.com",
    }


def test_load_email_settings_disabled() -> None:
    """Disabled email settings should not require SMTP credentials."""
    settings = load_email_settings({"MARKET_SENTINEL_EMAIL_ENABLED": "false"})

    assert settings.enabled is False


def test_load_email_settings_requires_values_when_enabled() -> None:
    """Enabled email without SMTP settings should produce a friendly error."""
    with pytest.raises(ValueError, match="missing"):
        load_email_settings({"MARKET_SENTINEL_EMAIL_ENABLED": "true"})


def test_build_daily_alert_email_body_includes_summary_data(tmp_path: Path) -> None:
    """Email body should include risk flags, crossovers, and high yields."""
    connection = open_test_database(tmp_path)

    try:
        insert_email_summary_data(connection)
        body = build_daily_alert_email_body(connection)
    finally:
        connection.close()

    assert "Dividend Risk Flags" in body
    assert "Crossover Signals" in body
    assert "High Dividend Stocks" in body
    assert "AAA" in body
    assert "BULLISH_CROSSOVER" in body


def test_send_daily_alert_email_does_nothing_when_disabled(
    tmp_path: Path,
    capsys,
) -> None:
    """Disabled email should print a friendly message and not send."""
    connection = open_test_database(tmp_path)

    try:
        sent = send_daily_alert_email(
            connection,
            environment={"MARKET_SENTINEL_EMAIL_ENABLED": "false"},
            smtp_factory=FakeSmtp,
        )
    finally:
        connection.close()

    captured = capsys.readouterr()
    assert sent is False
    assert "Email alerts are disabled" in captured.out


def test_send_daily_alert_email_uses_smtp_without_real_email(tmp_path: Path) -> None:
    """SMTP sending should be mockable so tests do not send real email."""
    FakeSmtp.sent_messages = []
    connection = open_test_database(tmp_path)

    try:
        insert_email_summary_data(connection)
        sent = send_daily_alert_email(
            connection,
            environment=enabled_environment(),
            smtp_factory=FakeSmtp,
        )
    finally:
        connection.close()

    assert sent is True
    assert len(FakeSmtp.sent_messages) == 1
    assert FakeSmtp.sent_messages[0]["To"] == "receiver@example.com"
    assert (
        "Market Sentinel daily alert summary"
        in FakeSmtp.sent_messages[0].get_content()
    )
