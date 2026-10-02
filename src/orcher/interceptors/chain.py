"""Ordered chains of interceptors and the code that runs them."""

from collections.abc import Awaitable, Callable
from typing import Any, Generic, TypeVar

from orcher.interceptors.base import (
    ExecutionInfo,
    Interceptor,
    InterceptorContext,
    TaskInterceptor,
    WorkflowInterceptor,
)

__all__ = [
    "InterceptorChain",
    "WorkflowInterceptorChain",
    "TaskInterceptorChain",
]

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class InterceptorChain(Generic[TInput, TOutput]):
    """Chain of interceptors for executing workflows or tasks.

    Keeps interceptors sorted by their ``order`` and runs them so that each
    one wraps the next. Interceptors with equal order keep insertion order.
    """

    def __init__(self) -> None:
        self._interceptors: list[Interceptor] = []

    def add(self, interceptor: Interceptor) -> "InterceptorChain[TInput, TOutput]":
        """Add an interceptor to the chain."""
        self._interceptors.append(interceptor)
        self._interceptors.sort(key=lambda i: i.order)
        return self

    def add_all(self, interceptors: list[Interceptor]) -> "InterceptorChain[TInput, TOutput]":
        """Add multiple interceptors to the chain."""
        for interceptor in interceptors:
            self.add(interceptor)
        return self

    def remove(self, interceptor_name: str) -> bool:
        """Remove the first interceptor with this name; return whether one was found."""
        for i, interceptor in enumerate(self._interceptors):
            if interceptor.name == interceptor_name:
                del self._interceptors[i]
                return True
        return False

    def clear(self) -> None:
        """Remove all interceptors."""
        self._interceptors.clear()

    @property
    def interceptors(self) -> list[Interceptor]:
        """Return the list of interceptors (sorted by order)."""
        return list(self._interceptors)

    def __len__(self) -> int:
        return len(self._interceptors)

    async def execute(
        self,
        context: InterceptorContext,
        input_data: TInput,
        target: Callable[[TInput], Awaitable[TOutput]],
    ) -> TOutput:
        """Execute the interceptor chain.

        Args:
            context: The interceptor context.
            input_data: The input data.
            target: The actual function to execute at the end of the chain.

        Returns:
            The result of the execution.
        """
        if not self._interceptors:
            return await target(input_data)

        async def final_next(data: TInput) -> TOutput:
            return await target(data)

        chain = final_next

        # Wrap from the innermost interceptor outward, so the first runs first.
        for interceptor in reversed(self._interceptors):
            # Bind this iteration's values; a bare closure would see the last ones.
            current_interceptor = interceptor
            current_chain = chain

            async def make_next(
                i: Interceptor,
                c: Callable[[TInput], Awaitable[TOutput]],
            ) -> Callable[[TInput], Awaitable[TOutput]]:
                async def next_fn(data: TInput) -> TOutput:
                    if isinstance(i, (WorkflowInterceptor, TaskInterceptor)):
                        return await i.intercept_execute(context, data, c)
                    else:
                        return await c(data)

                return next_fn

            chain = await make_next(current_interceptor, current_chain)

        return await chain(input_data)


class WorkflowInterceptorChain(InterceptorChain[Any, Any]):
    """Specialized chain for workflow interceptors."""

    def add(self, interceptor: Interceptor) -> "WorkflowInterceptorChain":
        """Add a workflow interceptor."""
        if not isinstance(interceptor, WorkflowInterceptor):
            raise TypeError(f"Expected WorkflowInterceptor, got {type(interceptor)}")
        super().add(interceptor)
        return self

    async def notify_enter(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Notify all interceptors of workflow entry."""
        for interceptor in self._interceptors:
            if isinstance(interceptor, WorkflowInterceptor):
                await interceptor.on_enter(context, info)

    async def notify_exit(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Notify all interceptors of workflow exit, in reverse order."""
        for interceptor in reversed(self._interceptors):
            if isinstance(interceptor, WorkflowInterceptor):
                await interceptor.on_exit(context, info)

    async def notify_success(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Notify all interceptors of workflow success."""
        for interceptor in self._interceptors:
            if isinstance(interceptor, WorkflowInterceptor):
                await interceptor.on_success(context, info)

    async def notify_error(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Notify all interceptors of workflow error."""
        for interceptor in self._interceptors:
            if isinstance(interceptor, WorkflowInterceptor):
                await interceptor.on_error(context, info)


class TaskInterceptorChain(InterceptorChain[Any, Any]):
    """Specialized chain for task interceptors."""

    def add(self, interceptor: Interceptor) -> "TaskInterceptorChain":
        """Add a task interceptor."""
        if not isinstance(interceptor, TaskInterceptor):
            raise TypeError(f"Expected TaskInterceptor, got {type(interceptor)}")
        super().add(interceptor)
        return self

    async def notify_enter(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Notify all interceptors of task entry."""
        for interceptor in self._interceptors:
            if isinstance(interceptor, TaskInterceptor):
                await interceptor.on_enter(context, info)

    async def notify_exit(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Notify all interceptors of task exit, in reverse order."""
        for interceptor in reversed(self._interceptors):
            if isinstance(interceptor, TaskInterceptor):
                await interceptor.on_exit(context, info)

    async def notify_success(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Notify all interceptors of task success."""
        for interceptor in self._interceptors:
            if isinstance(interceptor, TaskInterceptor):
                await interceptor.on_success(context, info)

    async def notify_error(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Notify all interceptors of task error."""
        for interceptor in self._interceptors:
            if isinstance(interceptor, TaskInterceptor):
                await interceptor.on_error(context, info)

    async def notify_retry(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
        attempt: int,
        max_attempts: int,
    ) -> None:
        """Notify all interceptors of task retry."""
        for interceptor in self._interceptors:
            if isinstance(interceptor, TaskInterceptor):
                await interceptor.on_retry(context, info, attempt, max_attempts)
