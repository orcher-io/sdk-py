"""Handle operations must do what they say instead of failing silently or misleadingly.

Two guarantees are checked here:

* ``reset()`` sends a reset of the handle's workflow and answers the new run.
* ``get_workflow()`` without a run id means "latest run". The server resolves a
  run by ``execution_id`` when one is given and by ``workflow_id`` only when it
  is empty. A placeholder run id such as ``"unknown"`` would turn every later
  call into an exact lookup for a run that cannot exist, failing with an error
  that names an id the caller never supplied.

The round trip against a live server is covered by the contract harness.
"""

from __future__ import annotations

import inspect

from orcher._proto import client_pb2 as pb
from tests._fake_connection import FakeConnection, client_on


async def test_reset_sends_a_reset_and_answers_the_new_run() -> None:
    connection = FakeConnection(
        pb.ClientAnswer(reset_to=pb.WorkflowRef(workflow_id="order-7", run_id="run-2"))
    )
    handle = await client_on(connection).get_workflow("order-7", "run-1")
    assert await handle.reset(3, "bad deploy") == "run-2"
    reset = connection.calls[0].reset
    assert (reset.workflow.workflow_id, reset.workflow.run_id) == ("order-7", "run-1")
    assert (reset.event_id, reset.reason) == (3, "bad deploy")


async def test_a_handle_without_a_run_id_targets_the_latest_run() -> None:
    connection = FakeConnection(pb.ClientAnswer(status=pb.WORKFLOW_STATUS_RUNNING))
    handle = await client_on(connection).get_workflow("order-7")
    await handle.status()
    assert connection.calls[0].get_status.workflow.run_id == ""


def test_client_documents_latest_run() -> None:
    from orcher.client.client import Client

    doc = inspect.getdoc(Client.get_workflow) or ""
    assert "latest run" in doc
