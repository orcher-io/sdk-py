"""Runs the engine bound to the kernel's step-id scheme.

Such a run names every durable call as the kernel does, ``{kind}-{n}`` from
one counter starting at 0, names a child started without an id from its
history lineage, and draws its random values from the kernel's generator
seeded from that lineage: the values the kernel records for fixed lineages,
the ones any SDK draws for the run. A run under this SDK's own scheme keeps
its ids and generator.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from orcher import TaskContext, WorkflowContext, task, workflow
from orcher._proto import kernel_pb2 as kernel
from orcher.decorators import GlobalRegistry
from orcher.worker.activation import ProtocolRefusalError, load_activation
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker
from orcher.workflow._kernel_random import RunRandom, seed_for_lineage
from tests.support.messages import answered
from tests.support.messages import message as to_message

START_MS = 1_700_000_000_000
LINEAGE = "lineage-1"
VECTORS = Path(__file__).parent / "fixtures" / "random_v1.json"


def payload(value: Any) -> dict[str, Any]:
    return {"data": list(json.dumps(value).encode()), "metadata": {"encoding": list(b"json")}}


def activation(*resolutions: dict[str, Any], kernel_scheme: bool = True) -> dict[str, Any]:
    started: dict[str, Any] = {"input": [payload({})], "attempt": 1, "started_at_ms": START_MS}
    if kernel_scheme:
        started.update(
            step_ids=kernel.STEP_ID_SCHEME_KERNEL,
            history_lineage=LINEAGE,
            random_seed=list(seed_for_lineage(LINEAGE)),
        )
    return {
        "activation_id": [1],
        "run": {
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "workflow_type": "kernel-ids",
            "task_queue": "q",
            "namespace": "ns",
        },
        "resolutions": [
            {"step_id": "", "at_ms": START_MS, "replaying": False, "started": started},
            *resolutions,
        ],
        "queries": [],
    }


def test_the_kernel_generator_reproduces_its_recorded_values() -> None:
    vectors = json.loads(VECTORS.read_text())
    for case in vectors["cases"]:
        lineage = case["lineage"]
        assert seed_for_lineage(lineage).hex() == case["seed"]
        r = RunRandom.for_lineage(lineage)
        assert [str(r.next_u64()) for _ in range(4)] == case["next_u64"]
        assert [r.next_f64() for _ in range(4)] == case["next_f64"]
        assert [r.below(10) for _ in range(6)] == case["below_10"]
        assert [r.range(-50, 50) for _ in range(4)] == case["range_minus50_50"]
        assert [r.next_bool() for _ in range(6)] == case["next_bool"]
        shuffled = list(range(10))
        r.shuffle(shuffled)
        assert shuffled == case["shuffle_0_to_9"]
        assert r.sample(20, 5) == case["sample_5_of_20"]
        assert r.uuid() == case["uuid"]


def test_a_run_under_the_kernel_scheme_draws_the_kernels_values() -> None:
    ctx = load_activation(to_message(activation()), "ns").context
    expected = RunRandom.for_lineage(LINEAGE)
    assert ctx.random.uuid() == expected.uuid()
    assert ctx.random.random() == expected.next_f64()
    assert ctx.random.randint(1, 6) == expected.range(1, 7)


def test_a_run_under_the_kernel_scheme_numbers_from_zero_and_names_children_by_lineage() -> None:
    ctx = load_activation(to_message(activation()), "ns").context
    assert ctx._next_sequence() == 0
    assert ctx._step_id("task", 0, "own_0") == "task-0"
    assert ctx._default_child_id(5) == f"{LINEAGE}-child-5"


def test_a_run_under_the_sdks_own_scheme_keeps_its_ids() -> None:
    ctx = load_activation(to_message(activation(kernel_scheme=False)), "ns").context
    assert ctx._next_sequence() == 1
    assert ctx._step_id("task", 1, "charge_1") == "charge_1"
    assert ctx._default_child_id(5) == "wf-1-child-5"


@pytest.mark.parametrize("field", ["history_lineage", "random_seed"])
def test_a_kernel_run_without_its_lineage_or_seed_is_refused(field: str) -> None:
    spec = activation()
    spec["resolutions"][0]["started"][field] = "" if field == "history_lineage" else [1, 2]
    with pytest.raises(ProtocolRefusalError):
        load_activation(to_message(spec), "ns")


# ── end to end through Worker.run_activation ────────────────────────────


@task(name="kernel-ids-step")
async def step(ctx: TaskContext, n: int) -> int:
    return n


async def kernel_ids_flow(ctx: WorkflowContext) -> None:
    first = await ctx.execute_task(step, n=1)
    await ctx.execute("note", lambda: first + 1)
    await ctx.wait_for_event_with_timeout("paid", timedelta(seconds=5))


def make_worker() -> Worker:
    registry = GlobalRegistry.get_instance()
    if registry.get_workflow("kernel-ids") is None:
        workflow(name="kernel-ids")(kernel_ids_flow)
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    worker._workflow_handlers["kernel-ids"] = registry.get_workflow("kernel-ids")
    return worker


async def test_every_call_reaches_the_kernel_named_as_it_names_it() -> None:
    worker = make_worker()
    settled = {
        "step_id": "task-0",
        "at_ms": START_MS + 1,
        "replaying": False,
        "step": {"outcome": {"value": payload(1), "end": "completed", "attempts": 1}},
    }
    answer = await answered(worker, activation(settled))
    result = kernel.RunActivationResult.FromString(answer.encoded())
    # By number: which call is which, not where among the others it is sent.
    calls = sorted(
        (c.seq, c.satisfied, c.WhichOneof("what"), c.step_id) for c in result.commands.commands
    )
    assert calls == [
        (0, True, "run_task", "task-0"),
        (1, False, "record_value", "value-1"),
        (2, False, "await_event", "await-2"),
        (3, False, "start_timer", "timer-3"),
    ]
