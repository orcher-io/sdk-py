"""The client speaks sdk-core's client protocol.

Every call is one encoded ``ClientCall`` through the native connection and one
encoded ``ClientAnswer`` back. These tests stand in for the connection: they
check that each method sends the call it promises, and that each answer, a
refusal included, comes back as the documented value or error.
"""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from orcher._proto import client_pb2 as pb
from orcher.client.workflow_handle import WorkflowHandle
from orcher.errors.client import ClientError
from orcher.errors.workflow import WorkflowError
from orcher.types import RetryPolicy, WorkflowIdReusePolicy, WorkflowStatus
from tests._fake_connection import FakeConnection, client_on, started


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def refused(code: int, *, run_id: str = "") -> pb.ClientAnswer:
    return pb.ClientAnswer(error=pb.ClientError(code=code, message="refused", run_id=run_id))


# ---- Starting ----


def test_a_start_sends_every_option_and_names_the_run() -> None:
    connection = FakeConnection(started())
    handle = run(
        client_on(connection).start_workflow(
            "checkout",
            workflow_id="order-7",
            task_queue="orders",
            args=({"n": 1},),
            retry_policy=RetryPolicy(max_attempts=4),
            execution_timeout=timedelta(seconds=90),
            cron_schedule="0 * * * * *",
            id_reuse_policy=WorkflowIdReusePolicy.REJECT_DUPLICATE,
        )
    )
    assert (handle.workflow_id, handle.run_id) == ("order-7", "run-1")
    start = connection.calls[0].start_workflow
    assert start.workflow_type == "checkout"
    assert start.task_queue == "orders"
    assert json.loads(start.input) == {"n": 1}
    assert start.retry_policy.max_attempts == 4
    assert start.execution_timeout.ToTimedelta() == timedelta(seconds=90)
    assert start.cron_schedule == "0 * * * * *"
    assert start.id_reuse_policy == pb.ID_REUSE_POLICY_REJECT_DUPLICATE


@pytest.mark.parametrize(
    "policy,attempts",
    [({}, 3), ({"max_attempts": 0}, 0), ({"max_attempts": 5}, 5)],
)
def test_a_dict_retry_policy_defaults_to_three_attempts(
    policy: dict[str, Any], attempts: int
) -> None:
    connection = FakeConnection(started())
    run(client_on(connection).start_workflow("checkout", retry_policy=policy))
    assert connection.calls[0].start_workflow.retry_policy.max_attempts == attempts


def test_a_start_left_bare_gets_an_id_and_the_default_queue() -> None:
    connection = FakeConnection(started())
    run(client_on(connection).start_workflow("checkout"))
    start = connection.calls[0].start_workflow
    assert start.workflow_id
    assert start.task_queue == "default"
    assert not start.HasField("retry_policy")


def test_an_id_in_use_names_the_run_holding_it() -> None:
    connection = FakeConnection(refused(pb.ERROR_CODE_WORKFLOW_ALREADY_EXISTS, run_id="run-0"))
    with pytest.raises(WorkflowError) as caught:
        run(client_on(connection).start_workflow("checkout", workflow_id="order-7"))
    assert caught.value.code.name == "WORKFLOW_ALREADY_EXISTS"
    assert caught.value.workflow_id == "order-7"
    assert caught.value.run_id == "run-0"


# ---- Refusals ----


@pytest.mark.parametrize(
    "code,expected",
    [
        (pb.ERROR_CODE_WORKFLOW_NOT_FOUND, WorkflowError),
        (pb.ERROR_CODE_WORKFLOW_EXECUTION_FAILED, WorkflowError),
        (pb.ERROR_CODE_WORKFLOW_CANCELED, WorkflowError),
        (pb.ERROR_CODE_WORKFLOW_TERMINATED, WorkflowError),
        (pb.ERROR_CODE_WORKFLOW_TIMEOUT, WorkflowError),
        (pb.ERROR_CODE_CONNECTION_FAILED, ClientError),
        (pb.ERROR_CODE_TIMEOUT, ClientError),
        (pb.ERROR_CODE_NOT_FOUND, ClientError),
        (pb.ERROR_CODE_UNAVAILABLE, ClientError),
    ],
)
def test_each_refusal_is_the_documented_error(code: int, expected: type[Exception]) -> None:
    handle = WorkflowHandle(client_on(FakeConnection(refused(code))), workflow_id="wf-1")
    with pytest.raises(expected):
        run(handle.result())


# ---- A handle's calls ----


def test_a_result_is_decoded_and_its_wait_sent() -> None:
    connection = FakeConnection(pb.ClientAnswer(result=pb.WorkflowResult(value=b'{"ok":true}')))
    handle = WorkflowHandle(client_on(connection), workflow_id="wf-1", run_id="run-1")
    assert run(handle.result(timeout=2.5)) == {"ok": True}
    wait = connection.calls[0].wait_for_result
    assert (wait.workflow.workflow_id, wait.workflow.run_id) == ("wf-1", "run-1")
    assert wait.wait.ToTimedelta() == timedelta(seconds=2.5)


@pytest.mark.parametrize(
    "value,expected",
    [
        (pb.WORKFLOW_STATUS_RUNNING, WorkflowStatus.RUNNING),
        (pb.WORKFLOW_STATUS_COMPLETED, WorkflowStatus.COMPLETED),
        (pb.WORKFLOW_STATUS_FAILED, WorkflowStatus.FAILED),
        (pb.WORKFLOW_STATUS_CANCELLED, WorkflowStatus.CANCELLED),
        (pb.WORKFLOW_STATUS_TERMINATED, WorkflowStatus.TERMINATED),
        (pb.WORKFLOW_STATUS_TIMED_OUT, WorkflowStatus.TIMED_OUT),
        (pb.WORKFLOW_STATUS_RESTARTED_FRESH, WorkflowStatus.RESTARTED_FRESH),
    ],
)
def test_a_status_is_the_documented_enum(value: int, expected: WorkflowStatus) -> None:
    connection = FakeConnection(pb.ClientAnswer(status=value))
    status = run(WorkflowHandle(client_on(connection), workflow_id="wf-1").status())
    assert status is expected


def test_a_description_reads_every_field() -> None:
    summary = pb.WorkflowSummary(
        workflow=pb.WorkflowRef(workflow_id="wf-1", run_id="run-1"),
        workflow_type="checkout",
        status=pb.WORKFLOW_STATUS_COMPLETED,
        labels={"tier": "gold"},
        annotations={"note": "x"},
    )
    summary.start_time.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
    description = pb.WorkflowDescription(summary=summary, pending_tasks=2)
    description.config.retry_policy.max_attempts = 3
    connection = FakeConnection(pb.ClientAnswer(description=description))

    read = run(WorkflowHandle(client_on(connection), workflow_id="wf-1").describe())

    assert read.execution.run_id == "run-1"
    assert read.status is WorkflowStatus.COMPLETED
    assert read.start_time == datetime(2026, 1, 1, tzinfo=UTC)
    assert read.close_time is None
    assert read.labels == {"tier": "gold"}
    assert read.memo == {"note": "x"}
    assert read.pending_tasks == 2
    assert read.execution_config is not None
    assert read.execution_config.retry_policy is not None
    assert read.execution_config.retry_policy.max_attempts == 3


def test_a_listing_reads_its_page_and_sends_its_filters() -> None:
    page = pb.WorkflowPage(
        workflows=[pb.WorkflowSummary(workflow=pb.WorkflowRef(workflow_id="wf-1", run_id="run-1"))],
        next_page_token=b"more",
    )
    connection = FakeConnection(pb.ClientAnswer(page=page))
    listing = run(
        client_on(connection).list_workflows(
            page_size=10, status_filter=["RUNNING"], sort_order="close_time_desc"
        )
    )
    assert [info.workflow_id for info in listing.executions] == ["wf-1"]
    assert listing.has_more
    sent = connection.calls[0].list_workflows
    assert sent.page_size == 10
    assert list(sent.statuses) == [pb.WORKFLOW_STATUS_RUNNING]
    assert sent.order == pb.LIST_ORDER_CLOSE_TIME_DESC


def test_query_arguments_and_bytes_are_json() -> None:
    connection = FakeConnection(pb.ClientAnswer(value=b"[1]"))
    handle = WorkflowHandle(client_on(connection), workflow_id="wf-1")
    assert run(handle.query("status", b"\x00\x01", 2)) == [1]
    args = json.loads(connection.calls[0].query.args)
    assert args["__orcher_args__"][0] == base64.b64encode(b"\x00\x01").decode()


def test_a_cancellation_carries_its_cleanup_limit() -> None:
    connection = FakeConnection(pb.ClientAnswer(done=pb.Done()))
    handle = WorkflowHandle(client_on(connection), workflow_id="wf-1")
    run(handle.cancel(cleanup_timeout=timedelta(seconds=30)))
    assert connection.calls[0].cancel.cleanup_timeout.ToTimedelta() == timedelta(seconds=30)
    with pytest.raises(ValueError):
        run(handle.cancel(cleanup_timeout=timedelta(seconds=-1)))


def test_a_reset_answers_the_new_run() -> None:
    connection = FakeConnection(
        pb.ClientAnswer(reset_to=pb.WorkflowRef(workflow_id="wf-1", run_id="run-2"))
    )
    handle = WorkflowHandle(client_on(connection), workflow_id="wf-1")
    assert run(handle.reset(4, "retry")) == "run-2"
    assert connection.calls[0].reset.event_id == 4


# ---- Actors ----


def test_an_actor_invocation_waits_thirty_seconds_unless_told() -> None:
    connection = FakeConnection(pb.ClientAnswer(value=b"7"))
    result = run(
        client_on(connection).invoke_actor_operation(
            actor_name="Cart", key="k", operation="total", payload=b"{}"
        )
    )
    assert result == b"7"
    assert connection.calls[0].invoke_actor.operation_timeout.ToTimedelta() == timedelta(seconds=30)


def test_a_failed_actor_invocation_raises() -> None:
    connection = FakeConnection(refused(pb.ERROR_CODE_WORKFLOW_EXECUTION_FAILED))
    with pytest.raises(RuntimeError):
        run(
            client_on(connection).invoke_actor_operation(
                actor_name="Cart", key="k", operation="total", payload=b"{}"
            )
        )
