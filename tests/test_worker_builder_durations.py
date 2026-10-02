"""Every duration option on the worker builder converts to milliseconds correctly.

A wrong unit is not a type error and not a test failure; it is a silent factor
of 1000. These tests pin the conversion every duration option goes through."""

from datetime import timedelta

from orcher.worker.builder import WorkerBuilder, _to_millis


def test_bare_int_is_milliseconds() -> None:
    """A bare int is read as milliseconds."""
    assert _to_millis(30_000) == 30_000


def test_timedelta_converts_to_milliseconds() -> None:
    assert _to_millis(timedelta(seconds=30)) == 30_000
    assert _to_millis(timedelta(milliseconds=250)) == 250
    assert _to_millis(timedelta(minutes=2)) == 120_000


def test_both_spellings_are_indistinguishable() -> None:
    """The two ways of saying 30 seconds must agree."""
    assert _to_millis(timedelta(seconds=30)) == _to_millis(30_000)


def test_seconds_are_not_silently_read_as_milliseconds() -> None:
    """Passing 30 meaning 'seconds' is 30ms. ``timedelta`` is how you say seconds."""
    assert _to_millis(30) != _to_millis(timedelta(seconds=30))
    assert _to_millis(30) == 30


def test_builder_accepts_both_forms() -> None:
    builder = (
        WorkerBuilder()
        .workflow_poll_interval(timedelta(milliseconds=250))
        .task_poll_interval(500)
        .shutdown_grace_time(timedelta(seconds=45))
        .force_shutdown_timeout(90_000)
    )

    assert builder._workflow_poll_interval_ms == 250
    assert builder._task_poll_interval_ms == 500
    assert builder._shutdown_grace_time_ms == 45_000
    assert builder._force_shutdown_timeout_ms == 90_000
