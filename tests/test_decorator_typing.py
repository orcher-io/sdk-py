"""The decorators keep the type of what they decorate, under mypy --strict."""

from __future__ import annotations

import textwrap
from pathlib import Path

from mypy import api

SAMPLE = textwrap.dedent(
    """
    from orcher import WorkflowContext, workflow


    @workflow(name="typing-class")
    class OrderWorkflow:
        async def run(self, ctx: WorkflowContext, order_id: str) -> str:
            return order_id


    @workflow(name="typing-function")
    async def process(ctx: WorkflowContext, order_id: str) -> str:
        return order_id


    reveal_type(OrderWorkflow)
    reveal_type(process)
    """
)


def test_workflow_keeps_the_type_of_a_class_and_of_a_function(tmp_path: Path) -> None:
    sample = tmp_path / "sample.py"
    sample.write_text(SAMPLE)

    stdout, stderr, _ = api.run(
        [
            "--strict",
            "--no-error-summary",
            "--cache-dir",
            str(tmp_path / ".mypy_cache"),
            str(sample),
        ]
    )

    errors = [line for line in stdout.splitlines() if ": error:" in line]
    assert errors == [], stdout + stderr
    revealed = [
        line.split("Revealed type is ", 1)[1]
        for line in stdout.splitlines()
        if "Revealed type is " in line
    ]
    assert revealed == [
        '"def () -> sample.OrderWorkflow"',
        '"def (ctx: orcher.workflow.context.WorkflowContext, order_id: str) -> '
        'typing.Coroutine[Any, Any, str]"',
    ], stdout
