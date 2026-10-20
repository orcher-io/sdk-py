"""A task failure tells the workflow's code its error type and failure code.

The kernel settles a failed step with the failure the engine recorded: its
message, its type (the class name of what the task raised) and its code
(``application`` for a task that raised, ``cancelled`` for a cancelled one).
``TaskError`` carried only the message and attempts, so a workflow could not
tell a cancelled task from one that failed, nor one error type from another.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from orcher import FailureCode, TaskContext, TaskError, task
from orcher.errors import ErrorCode
from orcher.worker.activation import load_activation
from tests.support.messages import message

START_MS = 1_700_000_000_000


@task(name="failure-code-charge")
async def charge(ctx: TaskContext) -> None:
    return None


def _activation(step_id: str, outcome: dict[str, Any]) -> dict[str, Any]:
    started = {
        "input": [{"data": list(json.dumps({}).encode()), "metadata": {}}],
        "attempt": 1,
        "started_at_ms": START_MS,
    }
    return {
        "activation_id": [1],
        "run": {"workflow_id": "wf", "run_id": "run", "workflow_type": "wf", "task_queue": "q"},
        "resolutions": [
            {"step_id": "", "at_ms": START_MS, "replaying": False, "started": started},
            {
                "step_id": step_id,
                "at_ms": START_MS + 1,
                "replaying": False,
                "step": {"outcome": outcome},
            },
        ],
    }


async def _task_error(outcome: dict[str, Any]) -> TaskError:
    ctx = load_activation(message(_activation("failure-code-charge_1", outcome)), "ns").context
    with pytest.raises(TaskError) as raised:
        await ctx.execute_task(charge)
    return raised.value


async def test_a_cancelled_task_says_it_was_cancelled() -> None:
    error = await _task_error(
        {
            "failure": {
                "message": "Task was cancelled",
                "error_type": "Cancelled",
                "code": "cancelled",
            },
            "end": "cancelled",
            "attempts": 1,
        }
    )
    assert error.failure_code == FailureCode.CANCELLED
    assert error.failure_code == "cancelled"
    assert error.error_type == "Cancelled"
    assert error.is_cancelled is True
    # Still the error a failed task raises, as before.
    assert error.code == ErrorCode.TASK_EXECUTION_FAILED


async def test_a_failure_of_type_cancelled_with_an_application_code_is_cancelled() -> None:
    # sdk-core 0.15.0 records a cancellation as code application, type Cancelled.
    error = await _task_error(
        {
            "failure": {
                "message": "Task was cancelled",
                "error_type": "Cancelled",
                "code": "application",
            },
            "end": "failed",
            "attempts": 1,
        }
    )
    assert error.failure_code == FailureCode.APPLICATION
    assert error.error_type == "Cancelled"
    assert error.is_cancelled is True


async def test_a_task_that_raised_carries_its_error_type() -> None:
    error = await _task_error(
        {
            "failure": {
                "message": "declined",
                "error_type": "CardDeclined",
                "code": "application",
            },
            "end": "failed",
            "attempts": 3,
        }
    )
    assert error.failure_code == FailureCode.APPLICATION
    assert error.error_type == "CardDeclined"
    assert error.is_cancelled is False
    assert error.attempts == 3


async def test_a_code_is_read_by_the_name_the_protocol_gives_it() -> None:
    error = await _task_error(
        {"failure": {"message": "x", "code": "invalid_command"}, "end": "failed", "attempts": 1}
    )
    assert error.failure_code == "invalid_command"
    assert error.error_type == ""
    assert error.is_cancelled is False


async def test_a_code_this_release_does_not_name_reads_as_unspecified() -> None:
    error = await _task_error(
        {"failure": {"message": "x", "code": 99}, "end": "failed", "attempts": 1}
    )
    assert error.failure_code == FailureCode.UNSPECIFIED


async def test_a_failure_with_no_code_or_type_reads_as_unspecified() -> None:
    error = await _task_error({"failure": {"message": "x"}, "end": "failed", "attempts": 1})
    assert error.failure_code == FailureCode.UNSPECIFIED
    assert error.error_type == ""


async def test_a_failed_closure_carries_its_type_and_code() -> None:
    outcome = {
        "failure": {"message": "gone", "error_type": "Cancelled", "code": "cancelled"},
        "end": "cancelled",
        "attempts": 1,
    }
    ctx = load_activation(message(_activation("fetch_1", outcome)), "ns").context
    with pytest.raises(TaskError) as raised:
        await ctx.execute("fetch", lambda: 1)
    assert raised.value.is_cancelled is True
    assert raised.value.error_type == "Cancelled"


def test_the_constructor_still_takes_what_it_took() -> None:
    error = TaskError(ErrorCode.TASK_EXECUTION_FAILED, "boom", task_id="t")
    assert error.error_type == ""
    assert error.failure_code == FailureCode.UNSPECIFIED
    assert error.is_cancelled is False


def test_the_cancelled_and_timeout_factories_carry_their_codes() -> None:
    assert TaskError.cancelled("t").is_cancelled is True
    assert TaskError.timeout("t").failure_code == FailureCode.TIMED_OUT


@pytest.mark.parametrize(("end", "code"), [("cancelled", "cancelled"), ("timed_out", "timed_out")])
async def test_a_failure_with_no_code_takes_it_from_how_the_step_ended(end: str, code: str) -> None:
    error = await _task_error({"failure": {"message": "x"}, "end": end, "attempts": 1})
    assert error.failure_code == code
    assert error.is_cancelled is (code == "cancelled")


async def test_a_code_wins_over_how_the_step_ended() -> None:
    error = await _task_error(
        {"failure": {"message": "x", "code": "application"}, "end": "cancelled", "attempts": 1}
    )
    assert error.failure_code == FailureCode.APPLICATION


async def test_a_failed_session_creation_raises_instead_of_opening_a_session() -> None:
    from orcher.workflow.session import SESSION_CREATE_TASK

    outcome = {
        "failure": {
            "message": "no slot",
            "error_type": "SessionUnavailable",
            "code": "application",
        },
        "end": "failed",
        "attempts": 1,
    }
    ctx = load_activation(message(_activation(f"{SESSION_CREATE_TASK}_1", outcome)), "ns").context
    with pytest.raises(TaskError) as raised:
        await ctx.create_session()
    assert raised.value.task_type == SESSION_CREATE_TASK
    assert raised.value.error_type == "SessionUnavailable"
    assert "no slot" in str(raised.value)
