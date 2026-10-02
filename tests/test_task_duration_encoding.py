"""Durations on a scheduled task are encoded without losing precision.

Timeouts, including the heartbeat timeout, are sent to the engine as seconds
plus nanoseconds. The sub-second part must be kept: dropping it would turn a
timeout under one second into a timeout of zero, so the caller would ask for a
limit and get none.
"""

from datetime import timedelta

from orcher.workflow.context import _duration_parts


def test_whole_seconds() -> None:
    assert _duration_parts(timedelta(seconds=45)) == {"secs": 45, "nanos": 0}


def test_sub_second_is_not_truncated_to_zero() -> None:
    # Encoding this as {"secs": 0, "nanos": 0} would mean no timeout at all.
    assert _duration_parts(timedelta(milliseconds=500)) == {
        "secs": 0,
        "nanos": 500_000_000,
    }


def test_fractional_second_keeps_its_remainder() -> None:
    assert _duration_parts(timedelta(seconds=1.5)) == {"secs": 1, "nanos": 500_000_000}


def test_nanos_never_exceed_a_second() -> None:
    # Rounding must carry into secs rather than emitting an out-of-range nanos,
    # which the native core would reject.
    parts = _duration_parts(timedelta(seconds=2, microseconds=999_999.7))
    assert parts["nanos"] < 1_000_000_000
    assert parts["secs"] >= 2


def test_zero_duration_is_representable() -> None:
    assert _duration_parts(timedelta(0)) == {"secs": 0, "nanos": 0}
