"""Tests for the daily process runner."""

from scripts import run_daily_process
from scripts.run_daily_process import daily_steps


def test_daily_steps_are_in_expected_order() -> None:
    """The daily process should run project steps in the expected order."""
    step_names = [step_name for step_name, _step_function in daily_steps()]

    assert step_names == [
        "Load universe",
        "Update market data",
        "Calculate moving averages",
        "Detect crossovers",
        "Calculate dividends",
        "Calculate risk flags",
        "Generate Excel report",
        "Generate PDF report",
        "Send daily alert email",
    ]


def test_email_step_uses_existing_email_sender(monkeypatch) -> None:
    """The daily process email step should reuse the email notifier."""
    calls = []

    def fake_send_daily_alert_email(connection):
        calls.append(connection)
        return True

    monkeypatch.setattr(
        run_daily_process,
        "send_daily_alert_email",
        fake_send_daily_alert_email,
    )

    result = run_daily_process._send_daily_alert_email("fake connection")

    assert result == {"email_sent": True}
    assert calls == ["fake connection"]


def test_email_step_continues_when_email_is_disabled(monkeypatch, capsys) -> None:
    """Disabled email should be treated as a successful optional step."""

    def fake_send_daily_alert_email(connection):
        print("Email alerts are disabled. No email was sent.")
        return False

    monkeypatch.setattr(
        run_daily_process,
        "send_daily_alert_email",
        fake_send_daily_alert_email,
    )

    result = run_daily_process._send_daily_alert_email("fake connection")

    captured = capsys.readouterr()
    assert result == {"email_sent": False}
    assert "Email alerts are disabled" in captured.out


def test_email_step_handles_missing_settings_safely(monkeypatch, capsys) -> None:
    """Missing email settings should print a friendly message and continue."""

    def fake_send_daily_alert_email(connection):
        raise ValueError(
            "Email alerts are enabled, but these environment variables are "
            "missing: MARKET_SENTINEL_SMTP_HOST."
        )

    monkeypatch.setattr(
        run_daily_process,
        "send_daily_alert_email",
        fake_send_daily_alert_email,
    )

    result = run_daily_process._send_daily_alert_email("fake connection")

    captured = capsys.readouterr()
    assert result == {"email_sent": False}
    assert "Daily alert email was not sent" in captured.out
    assert "email settings are incomplete" in captured.out
