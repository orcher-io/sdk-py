"""What the worker hands the native worker host, as the host decodes it.

Task outcomes, heartbeats and actor results cross to the host as encoded
``orcher.host.v1`` messages the worker builds with this SDK's generated code.
These tests decode what the worker sent and check every field the engine
reads: a value's JSON encoding and its ``encoding`` mark, and a failure's
message, type and retryability.
"""

from __future__ import annotations

from typing import Any

import pytest

from orcher._proto import host_pb2
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker, _send_heartbeat
from tests.support.messages import task_message

JSON = {"encoding": b"json"}


class RecordingHost:
    """The host's reporting calls, keeping what each was handed."""

    def __init__(self, cancel_requested: bool = False) -> None:
        self.outcomes: list[host_pb2.TaskOutcome] = []
        self.actor_results: list[host_pb2.ActorResult] = []
        self.heartbeats: list[host_pb2.TaskHeartbeat] = []
        self.cancel_requested = cancel_requested

    async def complete_task(self, outcome: bytes) -> None:
        self.outcomes.append(host_pb2.TaskOutcome.FromString(outcome))

    async def complete_actor_operation(self, result: bytes) -> None:
        self.actor_results.append(host_pb2.ActorResult.FromString(result))

    def heartbeat(self, heartbeat: bytes) -> bytes:
        self.heartbeats.append(host_pb2.TaskHeartbeat.FromString(heartbeat))
        return host_pb2.HeartbeatAnswer(cancel_requested=self.cancel_requested).SerializeToString()


def _worker(host: RecordingHost, outcome: Any) -> Worker:
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    worker._host = host

    async def run(_request: dict[str, Any]) -> Any:
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    worker._execute_task = run  # type: ignore[method-assign]
    worker._execute_actor_operation = run  # type: ignore[method-assign]
    return worker


class Declined(Exception):  # noqa: N818 - named as the other SDKs name it
    non_retryable = True


@pytest.mark.asyncio
async def test_a_task_result_is_its_json_encoding_marked_json() -> None:
    host = RecordingHost()
    await _worker(host, {"n": 2})._handle_task(
        task_message({"task_id": "t", "token": list(b"tok")})
    )

    [outcome] = host.outcomes
    assert outcome.token == b"tok"
    assert outcome.WhichOneof("result") == "value"
    assert outcome.value.data == b'{"n": 2}'
    assert dict(outcome.value.metadata) == JSON


@pytest.mark.asyncio
async def test_a_task_failure_carries_its_message_type_and_retryability() -> None:
    host = RecordingHost()
    await _worker(host, Declined("card declined"))._handle_task(
        task_message({"task_id": "t", "token": list(b"tok")})
    )

    [outcome] = host.outcomes
    assert outcome.token == b"tok"
    assert outcome.WhichOneof("result") == "failure"
    failure = outcome.failure
    assert (failure.message, failure.error_type, failure.non_retryable) == (
        "card declined",
        "Declined",
        True,
    )
    # Nothing else is claimed for the failure: the engine fills in the rest.
    assert (failure.code, failure.details) == (0, b"")


@pytest.mark.asyncio
async def test_an_actor_result_names_its_operation_and_delivery() -> None:
    host = RecordingHost()
    operation = host_pb2.ActorOperation(operation_id="op", execution_id="run")
    await _worker(host, [1, 2])._handle_actor_operation(operation)

    [result] = host.actor_results
    assert (result.operation_id, result.execution_id) == ("op", "run")
    assert result.WhichOneof("result") == "value"
    assert result.value.data == b"[1, 2]"
    assert dict(result.value.metadata) == JSON


@pytest.mark.asyncio
async def test_an_actor_failure_carries_its_message_and_type() -> None:
    host = RecordingHost()
    operation = host_pb2.ActorOperation(operation_id="op", execution_id="run")
    await _worker(host, ValueError("bad"))._handle_actor_operation(operation)

    [result] = host.actor_results
    assert (result.operation_id, result.execution_id) == ("op", "run")
    assert result.WhichOneof("result") == "failure"
    assert (result.failure.message, result.failure.error_type) == ("bad", "ValueError")
    assert not result.failure.non_retryable


def test_a_heartbeat_carries_its_token_and_details() -> None:
    host = RecordingHost()

    assert _send_heartbeat(host, b"tok", b'{"processed": 3}') is False
    assert _send_heartbeat(host, b"tok", None) is False

    with_details, without = host.heartbeats
    assert with_details.token == b"tok"
    assert with_details.details.data == b'{"processed": 3}'
    assert dict(with_details.details.metadata) == JSON
    assert without.token == b"tok"
    assert not without.HasField("details")


def test_a_heartbeat_answer_asking_to_cancel_is_read() -> None:
    assert _send_heartbeat(RecordingHost(cancel_requested=True), b"tok", None) is True
