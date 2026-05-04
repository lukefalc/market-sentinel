"""Tests for the daily process runner."""

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
    ]
