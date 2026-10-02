"""The worker calls the native bridge with arguments the bridge accepts.

The worker reported a failed workflow activation with ``error_type=``, a
keyword the native ``fail_workflow_task`` did not have (it was named
``_error_type``), so the report itself raised TypeError and the activation
was left unanswered, claimed on the engine until its claim timed out.
"""

from __future__ import annotations

import inspect

import pytest

native = pytest.importorskip("orcher._native")


def test_fail_workflow_task_takes_the_arguments_the_worker_passes() -> None:
    signature = inspect.signature(native.BridgeWorker.fail_workflow_task)
    signature.bind(
        object(),
        workflow_id="wf",
        execution_id="run",
        task_token="dG9rZW4=",
        error_message="boom",
        error_type="WorkflowExecutionError",
    )


def test_the_other_reports_take_the_arguments_the_worker_passes() -> None:
    inspect.signature(native.BridgeWorker.complete_workflow_task).bind(
        object(),
        workflow_id="wf",
        execution_id="run",
        result_json="{}",
        task_token="dG9rZW4=",
        stream_entry_id=None,
    )
    inspect.signature(native.BridgeWorker.fail_task).bind(
        object(), "dG9rZW4=", "boom", "ValueError", False
    )
    inspect.signature(native.BridgeWorker.complete_actor_operation).bind(
        object(), operation_id="op", execution_id="run", result_json="{}"
    )
    inspect.signature(native.BridgeWorker.fail_actor_operation).bind(
        object(),
        operation_id="op",
        execution_id="run",
        error_message="boom",
        error_type="ActorOperationError",
    )
