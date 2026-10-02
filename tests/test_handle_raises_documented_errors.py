"""The handle raises the errors its docstrings promise.

Every method on `WorkflowHandle` documents `Raises: WorkflowError` /
`ClientError`. These tests guarantee that native errors and transport builtins
are translated into those documented types, so a caller following the
docstring catches them instead of an opaque `NativeError` or a builtin.
"""

import asyncio

import pytest

from orcher import _native
from orcher.errors._translate import translate, translates_native_errors
from orcher.errors.client import ClientError
from orcher.errors.workflow import WorkflowError, WorkflowSuspendedError


@pytest.mark.parametrize(
    "native_name,expected",
    [
        ("WorkflowNotFoundError", WorkflowError),
        ("WorkflowAlreadyExistsError", WorkflowError),
        ("WorkflowFailedError", WorkflowError),
        ("WorkflowCancelledError", WorkflowError),
        ("WorkflowTerminatedError", WorkflowError),
        ("TaskFailedError", WorkflowError),
        ("TaskCancelledError", WorkflowError),
        ("DeterminismError", WorkflowError),
    ],
)
def test_each_native_kind_becomes_a_documented_error(
    native_name: str, expected: type[Exception]
) -> None:
    native_cls = getattr(_native, native_name)
    translated = translate(native_cls("boom"), workflow_id="wf-1")
    assert isinstance(translated, expected)


def test_the_kind_comes_from_the_type_not_the_message() -> None:
    """Renaming a message must not change which error a caller catches.

    Matching on text such as "not found" would break silently the first time
    someone rewords the message, so the kind is taken from the exception type.
    """
    err = _native.WorkflowNotFoundError("a wording nobody would match on")
    translated = translate(err, workflow_id="wf-1")
    assert isinstance(translated, WorkflowError)


def test_a_refused_start_names_the_run_in_the_way() -> None:
    """A duplicate start is only useful to catch if it says which run won.

    The native error carries the run holding the id as an attribute; dropping
    it leaves a caller that meant to start the workflow once no way to reach
    the run already doing the work.
    """
    err = _native.WorkflowAlreadyExistsError("Workflow already exists: order-7")
    err.workflow_id = "order-7"
    err.run_id = "run-7"
    translated = translate(err, workflow_id="order-7")
    assert isinstance(translated, WorkflowError)
    assert translated.code.name == "WORKFLOW_ALREADY_EXISTS"
    assert translated.workflow_id == "order-7"
    assert translated.run_id == "run-7"


def test_transport_builtins_become_client_errors() -> None:
    assert isinstance(translate(ConnectionError("refused")), ClientError)
    assert isinstance(translate(TimeoutError("slow")), ClientError)


def test_an_unrecognised_exception_passes_through_untouched() -> None:
    """Wrapping something we do not understand would hide it."""
    original = ValueError("something else entirely")
    assert translate(original) is original


def test_the_suspend_signal_is_never_translated() -> None:
    """Suspension is control flow, not an error.

    `WorkflowSuspendedError` derives from BaseException, and the decorator
    catches BaseException so it can re-raise cleanly. If it were ever translated
    into a WorkflowError, a suspending workflow would look like a failing one.
    """
    signal = WorkflowSuspendedError("waiting for an event")
    assert translate(signal) is signal


def test_the_decorator_reraises_the_suspend_signal_unchanged() -> None:
    class FakeHandle:
        workflow_id = "wf-1"

        @translates_native_errors
        async def suspends(self) -> None:
            raise WorkflowSuspendedError("waiting")

    with pytest.raises(WorkflowSuspendedError):
        asyncio.run(FakeHandle().suspends())


def test_the_decorator_translates_and_chains_the_cause() -> None:
    class FakeHandle:
        workflow_id = "wf-1"

        @translates_native_errors
        async def missing(self) -> None:
            raise _native.WorkflowNotFoundError("gone")

    with pytest.raises(WorkflowError) as caught:
        asyncio.run(FakeHandle().missing())

    # The original is kept as __cause__ so nothing is lost in translation.
    assert isinstance(caught.value.__cause__, _native.WorkflowNotFoundError)
