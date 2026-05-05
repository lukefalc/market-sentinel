"""Tests for the daily process runner."""

from scripts import run_daily_fast, run_daily_process, run_weekly_full
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


def test_daily_process_uses_daily_market_data_mode(monkeypatch) -> None:
    """The daily process should use recent daily updates, not backfill mode."""
    calls = []

    def fake_load_named_config(name):
        return {
            "price_download_batch_size": 12,
            "price_daily_lookback_days": 3,
            "price_download_pause_seconds": 0,
        }

    def fake_update_recent_daily_prices(
        connection,
        batch_size,
        lookback_days,
        pause_seconds,
    ):
        calls.append(
            {
                "connection": connection,
                "batch_size": batch_size,
                "lookback_days": lookback_days,
                "pause_seconds": pause_seconds,
            }
        )
        return {"tickers_checked": 0}

    monkeypatch.setattr(run_daily_process, "load_named_config", fake_load_named_config)
    monkeypatch.setattr(
        run_daily_process,
        "update_recent_daily_prices",
        fake_update_recent_daily_prices,
    )

    result = run_daily_process._update_market_data_daily("fake connection")

    assert result == {"tickers_checked": 0}
    assert calls == [
        {
            "connection": "fake connection",
            "batch_size": 12,
            "lookback_days": 3,
            "pause_seconds": 0.0,
        }
    ]


def test_run_daily_fast_skips_dividends_by_default(monkeypatch) -> None:
    """The fast daily process should not include dividends by default."""

    def fake_load_named_config(name):
        return {"run_dividends_in_daily_fast": False}

    monkeypatch.setattr(run_daily_fast, "load_named_config", fake_load_named_config)

    step_names = [step_name for step_name, _step_function in run_daily_fast.daily_fast_steps()]

    assert "Calculate dividends" not in step_names
    assert step_names == [
        "Load universe",
        "Update market data incrementally",
        "Calculate moving averages incrementally",
        "Detect crossovers",
        "Calculate risk flags",
        "Generate charts",
        "Generate PDF report",
        "Generate Excel report",
    ]


def test_run_daily_fast_can_include_dividends_when_enabled(monkeypatch) -> None:
    """The fast daily process can opt into dividends via settings."""

    def fake_load_named_config(name):
        return {"run_dividends_in_daily_fast": True}

    monkeypatch.setattr(run_daily_fast, "load_named_config", fake_load_named_config)

    step_names = [step_name for step_name, _step_function in run_daily_fast.daily_fast_steps()]

    assert "Calculate dividends" in step_names


def test_run_weekly_full_includes_dividend_update() -> None:
    """The weekly full process should include dividend calculation."""
    step_names = [
        step_name for step_name, _step_function in run_weekly_full.weekly_full_steps()
    ]

    assert step_names == [
        "Load universe",
        "Update market data",
        "Calculate moving averages",
        "Detect crossovers",
        "Calculate dividends",
        "Calculate risk flags",
        "Generate charts",
        "Generate PDF report",
        "Generate Excel report",
    ]
