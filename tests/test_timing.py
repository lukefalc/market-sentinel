"""Tests for timing log helpers."""

from datetime import datetime

from market_sentinel.utils.timing import print_timing_summary, timed_step


def test_timed_step_prints_start_finish_and_records_elapsed(capsys) -> None:
    """A timed step should print friendly logs and append a timing record."""
    clock_values = iter([10.0, 12.5])
    now_values = iter(
        [
            datetime(2026, 5, 5, 9, 0, 0),
            datetime(2026, 5, 5, 9, 0, 3),
        ]
    )
    records = []

    with timed_step(
        "Example step",
        records,
        clock=lambda: next(clock_values),
        now=lambda: next(now_values),
    ):
        pass

    captured = capsys.readouterr()

    assert "Starting Example step at 2026-05-05 09:00:00" in captured.out
    assert "Finished Example step at 2026-05-05 09:00:03 (2.5s)" in captured.out
    assert len(records) == 1
    assert records[0].name == "Example step"
    assert records[0].elapsed_seconds == 2.5


def test_print_timing_summary_outputs_table(capsys) -> None:
    """Timing summaries should show step seconds and total seconds."""
    records = []
    clock_values = iter([1.0, 1.4, 2.0, 4.5])
    now_values = iter(
        [
            datetime(2026, 5, 5, 9, 0, 0),
            datetime(2026, 5, 5, 9, 0, 1),
            datetime(2026, 5, 5, 9, 0, 2),
            datetime(2026, 5, 5, 9, 0, 5),
        ]
    )

    with timed_step(
        "Load universe",
        records,
        clock=lambda: next(clock_values),
        now=lambda: next(now_values),
    ):
        pass
    with timed_step(
        "Generate Excel",
        records,
        clock=lambda: next(clock_values),
        now=lambda: next(now_values),
    ):
        pass

    print_timing_summary(records)
    captured = capsys.readouterr()

    assert "Step" in captured.out
    assert "Seconds" in captured.out
    assert "Load universe" in captured.out
    assert "Generate Excel" in captured.out
    assert "Total" in captured.out
    assert "2.9" in captured.out
