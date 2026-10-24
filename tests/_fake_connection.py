"""A stand-in for the native client connection, for client tests.

It records each client protocol call it is handed and answers it with the next
answer it was given, so a test checks what a client method sends and how it
reads what comes back, without an engine.
"""

from __future__ import annotations

from orcher._proto import client_pb2 as pb
from orcher.client.client import Client
from orcher.client.config import ClientConfig


class FakeConnection:
    """Records each call and answers it with the next answer given."""

    def __init__(self, *answers: pb.ClientAnswer) -> None:
        self.calls: list[pb.ClientCall] = []
        self._answers = list(answers)

    async def call(self, data: bytes) -> bytes:
        self.calls.append(pb.ClientCall.FromString(data))
        return self._answers.pop(0).SerializeToString()


def client_on(connection: FakeConnection) -> Client:
    """A connected client whose calls go to ``connection``."""
    client = Client(ClientConfig(server_url="http://localhost:50051"))
    client._native_client = connection
    client._connected = True
    return client


def started(workflow_id: str = "order-7", run_id: str = "run-1") -> pb.ClientAnswer:
    """The answer to a start that began ``run_id``."""
    return pb.ClientAnswer(
        started=pb.Started(workflow=pb.WorkflowRef(workflow_id=workflow_id, run_id=run_id))
    )


def done() -> pb.ClientAnswer:
    """The answer to a call that answers nothing."""
    return pb.ClientAnswer(done=pb.Done())
