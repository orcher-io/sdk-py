"""
Concurrent execution of workflows and tasks.

Provides ExecutionPool, which runs many work units at once without blocking
the caller.

Key concepts:
- Execution permit: permission to run one workflow or task; permits bound
  concurrency
- Work unit: one workflow or task to execute
- Detached execution: work runs without blocking the caller

This lets the Rust polling layer hand off work and return to polling
immediately instead of waiting for each execution to finish.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class WorkUnitType(Enum):
    """Type of work unit."""

    WORKFLOW = auto()
    TASK = auto()


@dataclass
class WorkUnit:
    """
    A unit of work to be executed.

    Attributes:
        work_id: Unique identifier for this work
        work_type: Type of work (workflow or task)
        request: The execution request data
        executor: The coroutine function to execute
    """

    work_id: str
    work_type: WorkUnitType
    request: dict[str, Any]
    executor: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


@dataclass
class WorkResult:
    """
    Result of a work unit execution.

    Attributes:
        work_id: ID of the completed work
        work_type: Type of work
        success: Whether execution succeeded
        result: The result data (if success)
        error: Error message (if failed)
        traceback: Full traceback (if failed)
    """

    work_id: str
    work_type: WorkUnitType
    success: bool
    result: dict[str, Any] | None = None
    error: str | None = None
    traceback: str | None = None


class ExecutionPool:
    """
    Manages concurrent execution of work units.

    The ExecutionPool provides:
    - Semaphore-based concurrency control (execution permits)
    - Fire-and-forget work submission
    - A thread pool for blocking operations
    - Result delivery through a callback

    Rust dispatches work without waiting, Python runs it concurrently, and
    each result is delivered through the callback.

    Example:
        >>> pool = ExecutionPool(max_concurrent_workflows=100, max_concurrent_tasks=100)
        >>>
        >>> # Submit work (non-blocking)
        >>> await pool.submit_workflow(work_id, request, executor)
        >>>
        >>> # Work runs in background, results come via callback
    """

    def __init__(
        self,
        max_concurrent_workflows: int = 100,
        max_concurrent_tasks: int = 100,
        thread_pool_size: int = 50,
        result_callback: Callable[[WorkResult], Awaitable[None]] | None = None,
    ) -> None:
        """
        Initialize the execution pool.

        Args:
            max_concurrent_workflows: Max concurrent workflow executions
            max_concurrent_tasks: Max concurrent task executions
            thread_pool_size: Size of thread pool for blocking operations
            result_callback: Optional callback for completed work
        """
        self.max_concurrent_workflows = max_concurrent_workflows
        self.max_concurrent_tasks = max_concurrent_tasks

        # One permit per concurrent execution.
        self._workflow_permits = asyncio.Semaphore(max_concurrent_workflows)
        self._task_permits = asyncio.Semaphore(max_concurrent_tasks)

        # Runs blocking operations, such as sync task code, off the event loop.
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=thread_pool_size, thread_name_prefix="orcher-exec-"
        )

        # Keyed by work_id; entries are removed when execution finishes.
        self._pending_workflows: dict[str, asyncio.Task] = {}
        self._pending_tasks: dict[str, asyncio.Task] = {}

        self._result_callback = result_callback

        self._shutdown = False

        # Counters reported in the shutdown log.
        self._workflows_completed = 0
        self._tasks_completed = 0
        self._errors = 0

        logger.info(
            f"ExecutionPool initialized: max_workflows={max_concurrent_workflows}, "
            f"max_tasks={max_concurrent_tasks}, thread_pool={thread_pool_size}"
        )

    @property
    def pending_workflow_count(self) -> int:
        """Number of pending workflow executions."""
        return len(self._pending_workflows)

    @property
    def pending_task_count(self) -> int:
        """Number of pending task executions."""
        return len(self._pending_tasks)

    @property
    def total_pending(self) -> int:
        """Total number of pending executions."""
        return self.pending_workflow_count + self.pending_task_count

    async def submit_workflow(
        self,
        work_id: str,
        request: dict[str, Any],
        executor: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> None:
        """
        Submit a workflow for execution (non-blocking).

        This method returns immediately after scheduling the work.
        The actual execution happens in the background.

        Args:
            work_id: Unique identifier for this execution
            request: The workflow execution request
            executor: Async function to execute the workflow
        """
        if self._shutdown:
            raise RuntimeError("ExecutionPool is shutting down")

        work = WorkUnit(
            work_id=work_id,
            work_type=WorkUnitType.WORKFLOW,
            request=request,
            executor=executor,
        )

        # The permit is acquired inside the task, so this call never waits.
        task = asyncio.create_task(self._execute_workflow(work))
        self._pending_workflows[work_id] = task

        logger.debug(f"Submitted workflow {work_id} for execution")

    async def submit_task(
        self,
        work_id: str,
        request: dict[str, Any],
        executor: Callable[[dict[str, Any]], Awaitable[Any]],
    ) -> None:
        """
        Submit a task for execution (non-blocking).

        This method returns immediately after scheduling the work.
        The actual execution happens in the background.

        Args:
            work_id: Unique identifier for this execution
            request: The task execution request
            executor: Async function to execute the task
        """
        if self._shutdown:
            raise RuntimeError("ExecutionPool is shutting down")

        work = WorkUnit(
            work_id=work_id,
            work_type=WorkUnitType.TASK,
            request=request,
            executor=executor,
        )

        # The permit is acquired inside the task, so this call never waits.
        task = asyncio.create_task(self._execute_task(work))
        self._pending_tasks[work_id] = task

        logger.debug(f"Submitted task {work_id} for execution")

    async def _execute_workflow(self, work: WorkUnit) -> None:
        """Execute a workflow with permit management."""
        try:
            # Waits here while all permits are in use.
            async with self._workflow_permits:
                logger.debug(f"Executing workflow {work.work_id}")
                result = await self._run_with_error_handling(work)
                self._workflows_completed += 1
        finally:
            self._pending_workflows.pop(work.work_id, None)

        # Sent after the permit is released so a slow callback does not hold it.
        await self._send_result(result)

    async def _execute_task(self, work: WorkUnit) -> None:
        """Execute a task with permit management."""
        try:
            # Waits here while all permits are in use.
            async with self._task_permits:
                logger.debug(f"Executing task {work.work_id}")
                result = await self._run_with_error_handling(work)
                self._tasks_completed += 1
        finally:
            self._pending_tasks.pop(work.work_id, None)

        # Sent after the permit is released so a slow callback does not hold it.
        await self._send_result(result)

    async def _run_with_error_handling(self, work: WorkUnit) -> WorkResult:
        """Run a work unit and convert any exception into a failed WorkResult."""
        try:
            result = await work.executor(work.request)
            return WorkResult(
                work_id=work.work_id,
                work_type=work.work_type,
                success=True,
                result=result,
            )
        except Exception as e:
            self._errors += 1
            logger.exception(f"Work {work.work_id} failed: {e}")
            return WorkResult(
                work_id=work.work_id,
                work_type=work.work_type,
                success=False,
                error=str(e),
                traceback=traceback.format_exc(),
            )

    async def _send_result(self, result: WorkResult) -> None:
        """Send result via callback if configured."""
        if self._result_callback is not None:
            try:
                await self._result_callback(result)
            except Exception as e:
                logger.error(f"Result callback failed for {result.work_id}: {e}")

    async def run_in_thread(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """
        Run a blocking function in the thread pool.

        Use this for blocking I/O operations like database calls or HTTP requests
        that would otherwise block the event loop.

        Args:
            func: The blocking function to run
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            The function's return value
        """
        loop = asyncio.get_running_loop()

        # run_in_executor accepts only positional arguments.
        if kwargs:

            def wrapper() -> T:
                return func(*args, **kwargs)

            return await loop.run_in_executor(self._thread_pool, wrapper)
        else:
            return await loop.run_in_executor(self._thread_pool, func, *args)

    async def shutdown(self, timeout: float = 30.0) -> None:
        """
        Shutdown the execution pool.

        Waits for pending work to complete up to timeout.

        Args:
            timeout: Maximum time to wait for pending work (seconds)
        """
        self._shutdown = True
        logger.info(
            f"ExecutionPool shutting down. Pending: "
            f"{self.pending_workflow_count} workflows, {self.pending_task_count} tasks"
        )

        all_pending = list(self._pending_workflows.values()) + list(self._pending_tasks.values())

        if all_pending:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*all_pending, return_exceptions=True), timeout
                )
            except TimeoutError:
                logger.warning(
                    f"Shutdown timeout. Cancelling {len(all_pending)} pending executions."
                )
                for task in all_pending:
                    task.cancel()

        self._thread_pool.shutdown(wait=False)

        logger.info(
            f"ExecutionPool shutdown complete. Stats: workflows={self._workflows_completed}, "
            f"tasks={self._tasks_completed}, errors={self._errors}"
        )


class DetachedExecutionManager:
    """
    Manages detached (fire-and-forget) execution with result callbacks.

    This is the main integration point between the Rust native layer and Python.
    The Rust side dispatches work and returns to polling at once, while Python
    executes the work concurrently.

    Completed results are queued per work type and read with
    get_workflow_result() and get_task_result(). Work runs on the caller's
    running event loop.
    """

    def __init__(
        self,
        workflow_executor: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
        task_executor: Callable[[dict[str, Any]], Awaitable[Any]],
        max_concurrent_workflows: int = 100,
        max_concurrent_tasks: int = 100,
    ) -> None:
        """
        Initialize the detached execution manager.

        Args:
            workflow_executor: Function to execute workflows
            task_executor: Function to execute tasks
            max_concurrent_workflows: Max concurrent workflow executions
            max_concurrent_tasks: Max concurrent task executions
        """
        self._workflow_executor = workflow_executor
        self._task_executor = task_executor

        # Completed results, drained by the get_*_result methods.
        self._workflow_results: asyncio.Queue[WorkResult] = asyncio.Queue()
        self._task_results: asyncio.Queue[WorkResult] = asyncio.Queue()

        self._pool = ExecutionPool(
            max_concurrent_workflows=max_concurrent_workflows,
            max_concurrent_tasks=max_concurrent_tasks,
            result_callback=self._on_result,
        )

        logger.info("DetachedExecutionManager initialized")

    async def _on_result(self, result: WorkResult) -> None:
        """Queue a completed result by its work type."""
        if result.work_type == WorkUnitType.WORKFLOW:
            await self._workflow_results.put(result)
        else:
            await self._task_results.put(result)

    async def dispatch_workflow(self, work_id: str, request: dict[str, Any]) -> None:
        """
        Dispatch a workflow for execution (non-blocking).

        Returns immediately. The result becomes available via get_workflow_result().
        """
        await self._pool.submit_workflow(work_id, request, self._workflow_executor)

    async def dispatch_task(self, work_id: str, request: dict[str, Any]) -> None:
        """
        Dispatch a task for execution (non-blocking).

        Returns immediately. The result becomes available via get_task_result().
        """
        await self._pool.submit_task(work_id, request, self._task_executor)

    async def get_workflow_result(self, timeout: float | None = None) -> WorkResult | None:
        """
        Get a completed workflow result.

        Args:
            timeout: Max time to wait (None = non-blocking)

        Returns:
            WorkResult if available, None if timeout/no results
        """
        try:
            if timeout is None:
                return self._workflow_results.get_nowait()
            else:
                return await asyncio.wait_for(self._workflow_results.get(), timeout)
        except (TimeoutError, asyncio.QueueEmpty):
            return None

    async def get_task_result(self, timeout: float | None = None) -> WorkResult | None:
        """
        Get a completed task result.

        Args:
            timeout: Max time to wait (None = non-blocking)

        Returns:
            WorkResult if available, None if timeout/no results
        """
        try:
            if timeout is None:
                return self._task_results.get_nowait()
            else:
                return await asyncio.wait_for(self._task_results.get(), timeout)
        except (TimeoutError, asyncio.QueueEmpty):
            return None

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the execution manager."""
        await self._pool.shutdown(timeout)
