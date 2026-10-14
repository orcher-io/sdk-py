"""Workflow activations in the kernel protocol's shape, for tests.

``activation`` builds a ``RunActivation`` as the native module hands it to the
worker (its JSON); ``run`` answers it with ``Worker.run_activation`` and turns
the answer into what the workflow driver sends, as the worker host does.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from orcher.worker.worker import Worker

START_MS = 1_700_000_000_000


def payload(value: Any) -> dict[str, Any]:
    """A value's JSON encoding, as a payload."""
    return {"data": list(json.dumps(value).encode()), "metadata": {}}


def resolution(
    kind: str, what: dict[str, Any], *, step_id: str = "", at_ms: int | None = None
) -> dict[str, Any]:
    return {"step_id": step_id, "at_ms": at_ms, "replaying": False, kind: what}


def update(update_id: str, name: str, args: Any = None, *, at_ms: int | None = None) -> dict:
    """An update: recorded in history (with ``at_ms``) or waiting for this
    activation's answer (without)."""
    what = {"update_id": update_id, "name": name, "args": [] if args is None else [payload(args)]}
    return resolution("update", what, at_ms=at_ms)


def activation(
    workflow_type: str,
    workflow_input: Any = None,
    *resolutions: dict[str, Any],
    queries: list[dict[str, Any]] | None = None,
    workflow_id: str = "wf-1",
    run_id: str = "run-1",
    start_ms: int = START_MS,
) -> dict[str, Any]:
    return {
        "activation_id": [1],
        "run": {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "workflow_type": workflow_type,
            "task_queue": "q",
            "namespace": "default",
        },
        "resolutions": [
            resolution(
                "started",
                {"input": [payload(workflow_input)], "attempt": 1, "started_at_ms": start_ms},
                at_ms=start_ms,
            ),
            *resolutions,
        ],
        "queries": queries or [],
    }


def query(query_id: str, name: str, args: Any = None) -> dict[str, Any]:
    return {"query_id": query_id, "name": name, "args": [] if args is None else [payload(args)]}


async def run(worker: Worker, act: dict[str, Any]) -> dict[str, Any]:
    """Run an activation; what the workflow driver sends for its answer."""
    native = pytest.importorskip("orcher._native")
    answer = await worker.run_activation(act)
    return dict(
        json.loads(native._kernel_execution_result(act["run"]["run_id"], json.dumps(answer)))
    )
