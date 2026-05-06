"""Simple timing helpers for market-sentinel scripts."""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import time
from typing import Callable, Iterable, Iterator, List, Optional


@dataclass
class TimingRecord:
    """One completed timing measurement."""

    name: str
    started_at: datetime
    finished_at: datetime
    elapsed_seconds: float


def format_timestamp(value: datetime) -> str:
    """Format a timestamp for beginner-friendly logs."""
    return value.strftime("%Y-%m-%d %H:%M:%S")


@contextmanager
def timed_step(
    name: str,
    records: Optional[List[TimingRecord]] = None,
    clock: Callable[[], float] = time.perf_counter,
    now: Callable[[], datetime] = datetime.now,
) -> Iterator[None]:
    """Print start/finish timing logs and optionally append a record."""
    started_at = now()
    start_seconds = clock()
    print(f"Starting {name} at {format_timestamp(started_at)}")

    try:
        yield
    finally:
        finished_at = now()
        elapsed_seconds = clock() - start_seconds
        print(
            f"Finished {name} at {format_timestamp(finished_at)} "
            f"({elapsed_seconds:.1f}s)"
        )

        if records is not None:
            records.append(
                TimingRecord(
                    name=name,
                    started_at=started_at,
                    finished_at=finished_at,
                    elapsed_seconds=elapsed_seconds,
                )
            )


def print_timing_summary(
    records: Iterable[TimingRecord],
    total_label: str = "Total",
) -> None:
    """Print a compact timing summary table."""
    record_list = list(records)
    name_width = max(
        [len("Step"), len(total_label)]
        + [len(record.name) for record in record_list]
    )
    total_seconds = sum(record.elapsed_seconds for record in record_list)

    print(f"{'Step'.ljust(name_width)}  Seconds")
    for record in record_list:
        print(f"{record.name.ljust(name_width)}  {record.elapsed_seconds:7.1f}")
    print(f"{total_label.ljust(name_width)}  {total_seconds:7.1f}")
