"""A worker reports what it measured of its calls to the engine and its work."""

from __future__ import annotations

import json
from typing import Any

import pytest

from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker

SNAPSHOT = {
    "methods": {"/orcher.v1.ExecutionService/PollTaskExecution": {"calls": 3}},
    "work": {"activations": 0, "tasks": 2},
}


class _Host:
    def wire_metrics(self) -> str:
        return json.dumps(SNAPSHOT)

    async def shutdown(self) -> None:
        pass


def _worker() -> Worker:
    return Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))


def test_a_worker_that_never_started_has_no_measurements() -> None:
    assert _worker().wire_metrics() is None


def test_a_running_worker_reports_the_hosts_measurements() -> None:
    worker = _worker()
    worker._host = _Host()
    assert worker.wire_metrics() == SNAPSHOT


@pytest.mark.asyncio
async def test_a_stopped_worker_keeps_its_final_measurements() -> None:
    worker = _worker()
    host: Any = _Host()
    worker._host = host
    await worker._cleanup()
    assert worker._host is None
    assert worker.wire_metrics() == SNAPSHOT
