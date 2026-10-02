"""Decorators for defining workflows, tasks, queries, and updates.

Function-based workflows and tasks::

    @workflow(name="my-workflow", version="1.0")
    async def my_workflow(ctx: WorkflowContext, input: dict) -> dict:
        result = await ctx.execute_task(my_task, value=input["value"])
        return {"result": result}

    @task(name="my-task", timeout=30.0)
    async def my_task(ctx: TaskContext, value: str) -> str:
        return f"Processed: {value}"

Class-based tasks, where the class is constructed with its dependencies::

    @tasks
    class EmailTasks:
        def __init__(self, mailer: Mailer):
            self.mailer = mailer

        @task(name="send-email")
        async def send_email(self, ctx: TaskContext, to: str) -> None:
            await self.mailer.send(to)
"""

from orcher.decorators.query import query
from orcher.decorators.registry import (
    GlobalRegistry,
    HandlerType,
    QueryMetadata,
    TaskMetadata,
    UpdateMetadata,
    WorkflowMetadata,
)
from orcher.decorators.task import task
from orcher.decorators.task_class import register_task_class, tasks
from orcher.decorators.update import update
from orcher.decorators.workflow import workflow

__all__ = [
    # Decorators
    "workflow",
    "task",
    "query",
    "update",
    "tasks",
    "register_task_class",
    # Registry
    "GlobalRegistry",
    "WorkflowMetadata",
    "TaskMetadata",
    "QueryMetadata",
    "UpdateMetadata",
    "HandlerType",
]
