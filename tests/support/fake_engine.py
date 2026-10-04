"""A small stand-in for the engine and sdk-core, to drive a workflow through
activations the way a worker sees them.

It keeps a journal, builds each activation's request in the shape the native
layer hands the worker (the jobs sdk-core derives from the journal, and the
journal times read beside them), runs the worker's own workflow execution on
it, checks the result against sdk-core's deserializer when the native module
is built, and applies the commands: closure results are journaled at once,
scheduled work waits until the test resolves it.

What it records is what a replay must keep stable: the ids each activation
issued, the closure results it journaled, the sends and cancellations it made.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker

START_MS = 1_700_000_000_000


def _bytes(value: Any) -> list[int]:
    return list(json.dumps(value).encode())


def _system_time(ms: int) -> dict[str, int]:
    return {"secs_since_epoch": ms // 1000, "nanos_since_epoch": (ms % 1000) * 1_000_000}


class NonDeterministicError(AssertionError):
    """What sdk-core refuses an activation for: see ``FakeEngine.activate``."""


@dataclass
class Activation:
    """What one activation issued."""

    result: dict[str, Any]
    commands: list[dict[str, Any]]
    # (kind, id) of each step command, in order.
    steps: list[tuple[str, str]] = field(default_factory=list)


class FakeEngine:
    """Runs one workflow run through activations against a journal."""

    def __init__(
        self,
        workflow_fn: Any,
        workflow_input: Any = None,
        *,
        workflow_id: str = "wf-1",
        run_id: str = "run-1",
        start_ms: int = START_MS,
        worker: Worker | None = None,
    ) -> None:
        metadata = workflow_fn.__orcher_workflow__
        self.worker = worker or Worker(
            WorkerConfig(server_url="http://localhost:50051", task_queue="q")
        )
        self.worker._workflow_handlers[metadata.name] = metadata
        self.workflow_type = metadata.name
        self.workflow_input = {} if workflow_input is None else workflow_input
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.start_ms = start_ms
        self.now_ms = start_ms

        # Journal entries: (kind, attributes, at_ms).
        self.journal: list[tuple[str, dict[str, Any], int]] = []
        self.activations: list[Activation] = []
        # Every id ever issued, with what it was issued as.
        self.issued: dict[str, tuple[str, str]] = {}
        # Ids issued again as something else: the replay drifted.
        self.drift: list[str] = []
        self.closure_records: list[str] = []
        self.sent_events: list[dict[str, Any]] = []
        self.cancelled_children: list[str] = []
        self.completed: Any = None
        self.is_completed = False
        self.failure: dict[str, Any] | None = None

    # ── time and journal ────────────────────────────────────────────────

    def advance(self, ms: int) -> None:
        self.now_ms += ms

    def _journal(self, kind: str, at_ms: int | None = None, **attributes: Any) -> None:
        self.journal.append((kind, attributes, self.now_ms if at_ms is None else at_ms))

    def complete_task(self, task_id: str, value: Any) -> None:
        self._journal("step", step_name=task_id, step_type=1, result=_bytes(value), failure=None)

    def fail_task(self, task_id: str, message: str) -> None:
        failure = {
            "message": message,
            "source": "TaskExecution",
            "stack_trace": "",
            "failure_type": "ValueError",
        }
        self._journal("step", step_name=task_id, step_type=1, result=[], failure=failure)

    def fire_timer(self, timer_id: str) -> None:
        self._journal("timer", timer_id=timer_id)

    def send_event(self, name: str, value: Any) -> None:
        self._journal("event", event_name=name, payload=_bytes(value))

    def complete_child(self, workflow_id: str, value: Any) -> None:
        self._journal("child_completed", workflow_id=workflow_id, result=_bytes(value))

    def end_child(self, workflow_id: str, how: str, reason: str = "") -> None:
        """End a child without a result: canceled, terminated or timed_out."""
        self._journal(f"child_{how}", workflow_id=workflow_id, reason=reason)

    # ── activations ─────────────────────────────────────────────────────

    def request(self) -> dict[str, Any]:
        """The activation request, as the native layer hands it to the worker."""
        jobs: list[dict[str, Any]] = [
            {
                "StartWorkflow": {
                    "workflow_type": self.workflow_type,
                    "workflow_id": self.workflow_id,
                    "task_queue": "q",
                    "input": self.workflow_input,
                    "headers": [],
                    "scheduled_time": _system_time(self.now_ms),
                    "namespace": "default",
                }
            }
        ]
        resolved_at: dict[str, int] = {}
        events: dict[str, list[int]] = {}
        for index, (kind, a, at_ms) in enumerate(self.journal):
            if kind == "step":
                jobs.append(
                    {
                        "CompleteStep": {
                            "step_name": a["step_name"],
                            "step_type": a["step_type"],
                            "result": a["result"],
                            "failure": a["failure"],
                            "execution_attempt": 1,
                            "completed_at": None,
                            "duration_ms": 0,
                        }
                    }
                )
                resolved_at.setdefault(a["step_name"], at_ms)
            elif kind == "timer":
                jobs.append(
                    {
                        "FireTimer": {
                            "sequence": index,
                            "timer_id": a["timer_id"],
                            "scheduled_time": _system_time(self.now_ms),
                            "fired_time": _system_time(self.now_ms),
                        }
                    }
                )
                resolved_at.setdefault(f"timer:{a['timer_id']}", at_ms)
            elif kind == "event":
                jobs.append(
                    {
                        "HandleEvent": {
                            "sequence": index,
                            "event_name": a["event_name"],
                            "payload": {"data": a["payload"], "metadata": {}},
                            "headers": [],
                            "event_id": str(index),
                            "sent_time": _system_time(self.now_ms),
                        }
                    }
                )
                events.setdefault(a["event_name"], []).append(at_ms)
            elif kind == "child_completed":
                jobs.append(
                    {
                        "ChildWorkflowCompleted": {
                            "workflow_id": a["workflow_id"],
                            "execution_id": "child-run",
                            "result": {"data": a["result"], "metadata": {}},
                        }
                    }
                )
                resolved_at.setdefault(f"child:{a['workflow_id']}", at_ms)
            elif kind == "child_canceled":
                jobs.append(
                    {
                        "ChildWorkflowCanceled": {
                            "workflow_id": a["workflow_id"],
                            "execution_id": "child-run",
                            "details": None,
                        }
                    }
                )
                resolved_at.setdefault(f"child:{a['workflow_id']}", at_ms)
            elif kind == "child_terminated":
                jobs.append(
                    {
                        "ChildWorkflowTerminated": {
                            "workflow_id": a["workflow_id"],
                            "execution_id": "child-run",
                            "reason": a["reason"],
                            "details": None,
                        }
                    }
                )
                resolved_at.setdefault(f"child:{a['workflow_id']}", at_ms)
            elif kind == "child_timed_out":
                jobs.append(
                    {
                        "ChildWorkflowTimedOut": {
                            "workflow_id": a["workflow_id"],
                            "execution_id": "child-run",
                            "timeout_type": "Execution",
                        }
                    }
                )
                resolved_at.setdefault(f"child:{a['workflow_id']}", at_ms)
        return {
            "run_id": self.run_id,
            "execution": {"workflow_id": self.workflow_id, "run_id": self.run_id},
            "timestamp": _system_time(self.now_ms),
            "jobs": jobs,
            "journal_length": len(self.journal) + 1,
            "is_replaying": bool(self.journal),
            "journal_times": {
                "started_at_ms": self.start_ms,
                "resolved_at": resolved_at,
                "events": events,
            },
        }

    async def activate(self) -> Activation:
        """Run one activation and apply what it issued.

        Raises ``NonDeterministicError`` where sdk-core would refuse the
        activation: a step the journal recorded that the code did not reach,
        while it issues new work or ends the workflow.
        """
        result = await self.worker._execute_workflow(self.request())
        self._check_with_core(result)
        self._check_left_behind(result)
        activation = Activation(result=result, commands=list(result["commands"]))
        if not result["successful"]:
            self.failure = result["error"]
        for command in result["commands"]:
            self._apply(command, activation)
        self.activations.append(activation)
        return activation

    def _check_with_core(self, result: dict[str, Any]) -> None:
        try:
            from orcher import _native
        except ImportError:
            return
        # Raises ValueError for anything sdk-core would refuse.
        _native._parse_execution_result(json.dumps(result))

    def _check_left_behind(self, result: dict[str, Any]) -> None:
        reached = result.get("reached_steps")
        if not isinstance(reached, list):
            raise AssertionError("the activation does not report the steps it reached")
        step_kinds = ("ScheduleTask", "StartTimer", "StartChildWorkflow")
        id_field = {
            "ScheduleTask": "task_id",
            "StartTimer": "timer_id",
            "StartChildWorkflow": "workflow_id",
        }
        issued = [
            body[id_field[kind]]
            for command in result["commands"]
            for kind, body in command.items()
            if kind in step_kinds
        ]
        new_work = next((step for step in issued if step not in self.issued), None)
        ends = any(
            kind in ("CompleteWorkflow", "FailWorkflow", "RestartFresh")
            for command in result["commands"]
            for kind in command
        )
        reached_or_issued = set(reached) | set(issued)
        left = [step for step in self.issued if step not in reached_or_issued]
        if left and (new_work or ends or not result["successful"]):
            did = f"issued {new_work}" if new_work else "ended the workflow"
            raise NonDeterministicError(
                f"recorded {', '.join(left)} not reached, and the code {did}"
            )

    def _issue(self, kind: str, step_id: str, work_type: str, activation: Activation) -> None:
        activation.steps.append((kind, step_id))
        known = self.issued.get(step_id)
        if known is not None and known != (kind, work_type):
            self.drift.append(f"{step_id}: issued as {known}, now as {(kind, work_type)}")
        self.issued.setdefault(step_id, (kind, work_type))

    def _apply(self, command: dict[str, Any], activation: Activation) -> None:
        (kind, body), = command.items()
        if kind == "ScheduleTask":
            self._issue(kind, body["task_id"], body["task_type"], activation)
        elif kind == "StartTimer":
            self._issue(kind, body["timer_id"], "", activation)
        elif kind == "StartChildWorkflow":
            self._issue(kind, body["workflow_id"], body["workflow_type"], activation)
        elif kind == "RecordStepResult":
            name = body["step_name"]
            self.closure_records.append(name)
            if any(k == "step" and a["step_name"] == name for k, a, _ in self.journal):
                self.drift.append(f"{name}: closure recorded twice")
            self._journal(
                "step",
                step_name=name,
                step_type=body["step_type"],
                result=body["result"],
                failure=body["failure"],
            )
        elif kind == "SendEvent":
            self.sent_events.append(body)
        elif kind == "CancelChildWorkflow":
            self.cancelled_children.append(body["workflow_id"])
        elif kind == "CompleteWorkflow":
            self.is_completed = True
            self.completed = json.loads(bytes(body["result"]["data"]) or b"null")
