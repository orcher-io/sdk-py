"""A task's recorded result comes back as the dataclass its handler declares.

The journal records a dataclass result as a plain dict. A workflow gets it back
as the declared dataclass whether the task is a function or a method on a
@tasks class.
"""

from __future__ import annotations

from dataclasses import dataclass

from orcher import TaskContext, WorkflowContext, task, tasks, workflow
from tests.support.fake_engine import FakeEngine


@dataclass
class Receipt:
    order_id: str
    total: int


@task(name="result-dataclass-function")
async def charge(ctx: TaskContext) -> Receipt:
    return Receipt("o-1", 10)


@tasks
class Billing:
    @task(name="result-dataclass-method")
    async def refund(self, ctx: TaskContext) -> Receipt:
        return Receipt("o-1", -10)


@workflow(name="result-dataclass")
async def bill(ctx: WorkflowContext) -> list[object]:
    charged = await ctx.execute_task(charge)
    refunded = await ctx.execute_task(Billing.refund)
    return [repr(charged), repr(refunded)]


async def test_a_method_result_is_restored_to_its_dataclass_like_a_function_result() -> None:
    engine = FakeEngine(bill)

    await engine.activate()
    engine.complete_task("result-dataclass-function_1", {"order_id": "o-1", "total": 10})
    await engine.activate()
    engine.complete_task("result-dataclass-method_2", {"order_id": "o-1", "total": -10})
    await engine.activate()

    assert engine.failure is None
    assert engine.completed == [
        "Receipt(order_id='o-1', total=10)",
        "Receipt(order_id='o-1', total=-10)",
    ]
