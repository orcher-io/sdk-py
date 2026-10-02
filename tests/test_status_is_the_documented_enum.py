"""`handle.status()` must return the enum it is annotated with.

The native layer has its own `WorkflowStatus` pyclass whose `__eq__` accepts
only another instance of itself. `status()` is annotated `-> WorkflowStatus`
(the Python enum), so it must convert the native object. Returning it unchanged
would make the comparison every caller writes, `status == WorkflowStatus.COMPLETED`,
silently False in both directions. Nothing would raise; the branch would never run.
"""

import pytest

from orcher import _native
from orcher.types import WorkflowStatus


def test_the_native_object_still_does_not_compare_equal_to_the_enum() -> None:
    """The reason a conversion is needed, pinned so it cannot be forgotten.

    If this ever starts passing, the native type gained enum-aware equality and
    the conversion could be revisited.
    """
    native = _native.WorkflowStatus.completed()
    assert (native == WorkflowStatus.COMPLETED) is False
    assert (native == WorkflowStatus.COMPLETED) is False


@pytest.mark.parametrize(
    "ctor,expected",
    [
        ("running", WorkflowStatus.RUNNING),
        ("completed", WorkflowStatus.COMPLETED),
        ("failed", WorkflowStatus.FAILED),
        ("cancelled", WorkflowStatus.CANCELLED),
        ("terminated", WorkflowStatus.TERMINATED),
        ("timed_out", WorkflowStatus.TIMED_OUT),
    ],
)
def test_every_native_status_converts_to_its_enum_member(
    ctor: str, expected: WorkflowStatus
) -> None:
    native = getattr(_native.WorkflowStatus, ctor)()
    converted = WorkflowStatus._from_native(native)

    assert converted is expected
    # The comparison a user writes works on the converted value.
    assert converted == expected


def test_timed_out_is_why_the_mapping_is_explicit() -> None:
    """`TimedOut`.upper() is `TIMEDOUT`, which is not a member.

    A case transform would silently fail on exactly this value, so the mapping
    is spelled out, and this test keeps it that way.
    """
    native = _native.WorkflowStatus.timed_out()
    assert native.name.upper() != WorkflowStatus.TIMED_OUT.value
    assert WorkflowStatus._from_native(native) is WorkflowStatus.TIMED_OUT


def test_an_unknown_native_status_raises_rather_than_guessing() -> None:
    class Fake:
        name = "SomethingNew"

    with pytest.raises(ValueError, match="unknown workflow status"):
        WorkflowStatus._from_native(Fake())


def test_a_value_with_no_name_is_rejected() -> None:
    with pytest.raises(TypeError):
        WorkflowStatus._from_native(object())
