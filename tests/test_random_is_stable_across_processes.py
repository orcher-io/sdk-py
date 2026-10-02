"""ctx.random draws the same sequence in every process that replays a run.

Python randomizes ``str`` hashes per process (PYTHONHASHSEED), so a seed taken
from ``hash()`` of the workflow and run ids differs between the worker that ran
an activation and the one that replays it, and a workflow branching on
``ctx.random`` takes another path on replay.
"""

import os
import subprocess
import sys
import textwrap

DRAW = textwrap.dedent(
    """
    from datetime import datetime
    from orcher.workflow.context import WorkflowContext
    from orcher.workflow.info import WorkflowInfo

    info = WorkflowInfo(
        workflow_id="order-42",
        run_id="run-7",
        workflow_type="Order",
        task_queue="q",
        namespace="default",
        attempt=1,
        started_at=datetime(2026, 1, 1),
    )
    ctx = WorkflowContext(info)
    print([ctx.random.randint(0, 10**9) for _ in range(4)], ctx.random.uuid())
    """
)


def _draw(hash_seed: str) -> str:
    env = {**os.environ, "PYTHONHASHSEED": hash_seed}
    out = subprocess.run(
        [sys.executable, "-c", DRAW], env=env, capture_output=True, text=True, check=True
    )
    return out.stdout.strip()


def test_two_processes_with_different_hash_seeds_draw_the_same_sequence() -> None:
    assert _draw("1") == _draw("2")


def test_different_runs_draw_different_sequences() -> None:
    from datetime import datetime

    from orcher.workflow.context import WorkflowContext
    from orcher.workflow.info import WorkflowInfo

    def ctx(run_id: str) -> WorkflowContext:
        return WorkflowContext(
            WorkflowInfo(
                workflow_id="order-42",
                run_id=run_id,
                workflow_type="Order",
                task_queue="q",
                namespace="default",
                attempt=1,
                started_at=datetime(2026, 1, 1),
            )
        )

    assert ctx("a").random.random() != ctx("b").random.random()
