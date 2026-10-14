"""The worker calls the native worker host with arguments the host accepts.

The worker once reported a failed workflow activation with ``error_type=``, a
keyword the native call did not have, so the report itself raised TypeError
and the activation was left unanswered, claimed on the engine until its claim
timed out. Each call the worker makes is checked against the native
signature here, positionally as the worker makes it.
"""

from __future__ import annotations

import inspect

import pytest

native = pytest.importorskip("orcher._native")


def test_every_report_takes_the_arguments_the_worker_passes() -> None:
    host = native.WorkerHost
    inspect.signature(host.complete_activation).bind(object(), "{}")
    inspect.signature(host.complete_task).bind(object(), b"token", b"{}")
    inspect.signature(host.fail_task).bind(object(), b"token", "boom", "ValueError", False)
    inspect.signature(host.complete_actor_operation).bind(object(), "op", "run", b"{}")
    inspect.signature(host.fail_actor_operation).bind(
        object(), "op", "run", "boom", "ValueError"
    )


def test_the_calls_a_running_task_makes_take_its_token() -> None:
    host = native.WorkerHost
    inspect.signature(host.heartbeat).bind(object(), b"token", b"{}")
    inspect.signature(host.heartbeat).bind(object(), b"token", None)
    inspect.signature(host.task_cancelled).bind(object(), b"token")


def test_the_actor_state_calls_take_what_the_state_client_passes() -> None:
    host = native.WorkerHost
    inspect.signature(host.actor_get_state).bind(
        object(), actor_name="a", key="k", state_key="s", execution_id="e"
    )
    inspect.signature(host.actor_set_state).bind(
        object(), actor_name="a", key="k", state_key="s", value=b"1", execution_id="e"
    )
    inspect.signature(host.actor_delete_state).bind(
        object(), actor_name="a", key="k", state_key="s", execution_id="e"
    )
    inspect.signature(host.actor_list_state_keys).bind(
        object(), actor_name="a", key="k", execution_id="e", prefix=""
    )


def test_a_client_invokes_an_actor_as_the_invocation_client_calls_it() -> None:
    inspect.signature(native.Client.invoke_actor_operation).bind(
        object(), actor_name="a", key="k", operation="op", payload=b"null", timeout_ms=None
    )
