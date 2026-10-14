"""A small stand-in for the engine and the worker host, to drive a workflow
through activations the way a worker sees them.

It keeps a journal, builds each activation as the worker host hands it over
(the kernel protocol's ``RunActivation``: the journal resolved into the
outcomes the run observes, each with its time), runs it through
``Worker.run_activation``, turns the answer back into the engine's commands
with the kernel, as the host does, and applies them: closure results are
journaled at once, scheduled work waits until the test resolves it. It needs
the native module.

What it records is what a replay must keep stable: the ids each activation
issued, the closure results it journaled, the sends and cancellations it made.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest

from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker

pytest.importorskip("orcher._native")

START_MS = 1_700_000_000_000


def _bytes(value: Any) -> list[int]:
    return list(json.dumps(value).encode())


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

    def start_child(self, workflow_id: str, workflow_type: str) -> None:
        """Journal a child as started, as a run whose code started it would
        have: the journal holds it, and the run issued it."""
        self._journal("child_started", workflow_id=workflow_id, workflow_type=workflow_type)
        self.issued.setdefault(workflow_id, ("StartChildWorkflow", workflow_type))

    def end_child(self, workflow_id: str, how: str, reason: str = "") -> None:
        """End a child without a result: canceled, terminated or timed_out."""
        self._journal(f"child_{how}", workflow_id=workflow_id, reason=reason)

    # ── activations ─────────────────────────────────────────────────────

    def activation(self) -> dict[str, Any]:
        """The activation, as the worker host hands it to the worker: the
        kernel protocol's ``RunActivation``, the journal resolved into the
        outcomes the run observes, in the JSON the native module writes."""
        resolutions: list[dict[str, Any]] = [
            {
                "step_id": "",
                "at_ms": self.start_ms,
                "replaying": False,
                "started": {
                    "input": [{"data": _bytes(self.workflow_input), "metadata": {}}],
                    "attempt": 1,
                    "started_at_ms": self.start_ms,
                },
            }
        ]

        def resolve(step_id: str, at_ms: int, kind: str, what: dict[str, Any]) -> None:
            resolutions.append({"step_id": step_id, "at_ms": at_ms, "replaying": False, kind: what})

        def child_ended(a: dict[str, Any], end: str, message: str) -> dict[str, Any]:
            return {
                "child_workflow_id": a["workflow_id"],
                "child_run_id": "child-run",
                "outcome": {"failure": {"message": message}, "end": end},
            }

        # By recorded time, then journal position, as the kernel orders them.
        entries = sorted(enumerate(self.journal), key=lambda e: (e[1][2], e[0]))
        for index, (kind, a, at_ms) in entries:
            if kind == "step":
                if a["failure"]:
                    outcome = {
                        "failure": {
                            "message": a["failure"]["message"],
                            "error_type": a["failure"]["failure_type"],
                        },
                        "end": "failed",
                        "attempts": 1,
                    }
                else:
                    outcome = {"value": {"data": a["result"]}, "end": "completed", "attempts": 1}
                resolve(a["step_name"], at_ms, "step", {"outcome": outcome})
            elif kind == "timer":
                resolve(a["timer_id"], at_ms, "timer", {})
            elif kind == "event":
                resolve(
                    "",
                    at_ms,
                    "event",
                    {
                        "event_id": str(index),
                        "name": a["event_name"],
                        "payload": {"data": a["payload"]},
                    },
                )
            elif kind == "child_started":
                started = {"child_workflow_id": a["workflow_id"]}
                resolve(a["workflow_id"], at_ms, "child_started", started)
            elif kind == "child_completed":
                resolve(
                    a["workflow_id"],
                    at_ms,
                    "child",
                    {
                        "child_workflow_id": a["workflow_id"],
                        "child_run_id": "child-run",
                        "outcome": {"value": {"data": a["result"]}, "end": "completed"},
                    },
                )
            elif kind == "child_canceled":
                resolve(
                    a["workflow_id"], at_ms, "child",
                    child_ended(a, "cancelled", "Child workflow was canceled"),
                )
            elif kind == "child_terminated":
                # The kernel does not carry the reason a child was terminated.
                resolve(
                    a["workflow_id"], at_ms, "child",
                    child_ended(a, "terminated", "Child workflow was terminated"),
                )
            elif kind == "child_timed_out":
                resolve(
                    a["workflow_id"], at_ms, "child",
                    child_ended(a, "timed_out", "Child workflow timed out"),
                )
        return {
            "activation_id": list(f"{self.run_id}:{len(self.activations)}".encode()),
            "run": {
                "workflow_id": self.workflow_id,
                "run_id": self.run_id,
                "workflow_type": self.workflow_type,
                "task_queue": "q",
                "namespace": "default",
            },
            "resolutions": resolutions,
            "queries": [],
        }

    async def activate(self) -> Activation:
        """Run one activation and apply what it issued.

        The worker's answer is turned back into the engine's commands by the
        kernel, as the worker host does. Raises ``NonDeterministicError``
        where sdk-core would refuse the activation: a step the journal
        recorded that the code did not reach, while it issues new work or
        ends the workflow.
        """
        answer = await self.worker.run_activation(self.activation())
        result = self._with_core(answer)
        self._check_left_behind(result)
        activation = Activation(result=result, commands=list(result["commands"]))
        if not result["successful"]:
            self.failure = result["error"]
        for command in result["commands"]:
            self._apply(command, activation)
        self.activations.append(activation)
        return activation

    def _with_core(self, answer: dict[str, Any]) -> dict[str, Any]:
        """The answer as the kernel turns it into what the workflow driver
        checks and sends. Raises ValueError for anything the kernel refuses."""
        from orcher import _native

        return dict(json.loads(_native._kernel_execution_result(self.run_id, json.dumps(answer))))

    def _check_left_behind(self, result: dict[str, Any]) -> None:
        reached = result.get("reached_steps")
        if not result["successful"]:
            # The kernel protocol of sdk-core 0.14 does not carry the steps a
            # failed run reached.
            reached = []
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
            if not any(
                k == "child_started" and a["workflow_id"] == body["workflow_id"]
                for k, a, _ in self.journal
            ):
                self._journal(
                    "child_started",
                    workflow_id=body["workflow_id"],
                    workflow_type=body["workflow_type"],
                )
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
