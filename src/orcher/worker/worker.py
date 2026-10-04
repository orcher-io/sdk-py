"""
Worker implementation.

The Worker polls the Orcher server for workflow and task executions, runs the
registered handlers, and reports results back to the server.

## Architecture: Python-owned event loop with a Rust bridge

Python owns the event loop and calls into Rust for server I/O:

- The Python asyncio event loop drives everything.
- Rust exposes poll and complete operations as awaitables (via future_into_py()).
- Sync tasks run in an executor (ThreadPoolExecutor or ProcessPoolExecutor).
- Async tasks run directly in the event loop.

This supports:
- CPU-bound tasks via ProcessPoolExecutor (true parallelism)
- I/O-bound sync tasks via ThreadPoolExecutor
- Full asyncio compatibility for async tasks

Terminology:
- Worker: a process that polls for and executes work
- Task: a unit of work that may have side effects
- Event: an external message delivered to a running workflow
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import traceback
from concurrent.futures import Executor as ConcurrentExecutor
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from orcher.actor.types import ActorMetadata
from orcher.core.native import get_native_module, is_native_available
from orcher.decorators.registry import (
    GlobalRegistry,
    QueryMetadata,
    TaskMetadata,
    UpdateMetadata,
    WorkflowMetadata,
)

if TYPE_CHECKING:
    from orcher.worker.builder import WorkerBuilder
    from orcher.worker.session import SessionManager
from orcher.errors import ErrorCode, OrcherError, WorkflowSuspendedError
from orcher.interceptors.base import (
    ExecutionInfo,
    InterceptorContext,
    TaskInterceptor,
    WorkflowInterceptor,
)
from orcher.interceptors.chain import TaskInterceptorChain, WorkflowInterceptorChain
from orcher.worker.config import WorkerConfig
from orcher.workflow.sentinels import CHILD_FAILED_SENTINEL, TASK_FAILED_SENTINEL

logger = logging.getLogger(__name__)

# How long the workflow and task pollers have to notice that their channels
# closed after their drivers stopped. They normally notice at once; this only
# bounds shutdown when a driver could not be stopped.
DRIVER_POLLER_STOP_TIMEOUT_SECS = 5.0


async def _watch_task_cancellation(bridge: Any, task_token: str, token: Any) -> None:
    """Cancel ``token`` once the engine asks the task to stop.

    The worker heartbeats the task on its own. The engine answers a heartbeat
    with a stop request when the workflow was cancelled or the attempt is no
    longer running. Returns without cancelling once the task is reported.
    """
    try:
        if await bridge.wait_task_cancelled(task_token):
            token.cancel()
    except asyncio.CancelledError:
        raise
    except Exception as e:  # a native build without this call, or a bad token
        logger.debug(f"Not watching task cancellation: {e}")


def _optional_millis(ms: int | float | None) -> timedelta | None:
    """Turn a millisecond value from the native layer into a timedelta.

    ``None`` means the engine set no limit. That differs from a limit of zero,
    so ``None`` is preserved rather than replaced with a default.
    """
    return None if ms is None else timedelta(milliseconds=ms)


def _task_attempt(request: dict[str, Any]) -> int:
    """Which attempt of its task a request runs, counted from 1.

    An engine that does not number the attempt sends 0, as engines up to
    0.5.4 did for every attempt, retries included; that reads as 1, never as
    0, so a first attempt is never mistaken for anything else.
    """
    try:
        attempt = int(request.get("attempt") or 1)
    except (TypeError, ValueError):
        return 1
    return max(attempt, 1)

def _workflow_error_type(error: BaseException) -> str:
    """The execution error type the core is told for a failed workflow.

    Non-determinism is reported as such, so the core fails the execution as
    non-deterministic and not retryable rather than as an ordinary workflow
    error: the same code against the same journal would fail the same way.
    """
    if isinstance(error, OrcherError) and error.code == ErrorCode.WORKFLOW_NON_DETERMINISTIC:
        return "NonDeterminism"
    return "WorkflowCode"


def _workflow_started_at(journal_times: dict[str, Any], run_id: str) -> datetime:
    """When the workflow started, as its journal recorded it.

    A request from the engine always carries it; one built by hand (a test
    driving the worker directly) may not, and the clock then starts now.
    """
    started_at_ms = journal_times.get("started_at_ms")
    if isinstance(started_at_ms, int):
        return datetime.fromtimestamp(started_at_ms / 1000, tz=UTC)
    if journal_times:
        logger.warning(
            "The journal of run %s records no start time; workflow time starts now", run_id
        )
    return datetime.now(UTC)


def _max_attempts(retry_policy: Any) -> int:
    """The attempts a task's declared retry policy allows; 0 when it declares none."""
    if isinstance(retry_policy, dict):
        return int(retry_policy.get("max_attempts", 0) or 0)
    return int(getattr(retry_policy, "max_attempts", 0) or 0)


class WorkerState(Enum):
    """Worker lifecycle states."""

    STOPPED = auto()
    STARTING = auto()
    RUNNING = auto()
    SHUTTING_DOWN = auto()
    FORCE_SHUTDOWN = auto()


@dataclass
class ServiceStats:
    """Runtime statistics for the Worker."""

    workflows_executed: int = 0
    tasks_executed: int = 0
    workflows_in_progress: int = 0
    tasks_in_progress: int = 0
    errors: int = 0
    uptime_seconds: float = 0.0
    state: WorkerState = WorkerState.STOPPED
    worker_id: str = ""
    started_at: datetime | None = None
    stopped_at: datetime | None = None


@dataclass
class WorkerIdentity:
    """Worker identity information."""

    worker_id: str
    version_id: str | None = None
    binary_checksum: str | None = None


@dataclass
class WorkerCapabilities:
    """Worker capabilities."""

    workflows: list[str] = field(default_factory=list)
    tasks: list[str] = field(default_factory=list)
    actors: list[str] = field(default_factory=list)
    max_concurrent_workflows: int = 100
    max_concurrent_tasks: int = 100


class Worker:
    """
    Worker runtime for executing workflows and tasks.

    The Worker:
    - Polls the Orcher server for workflow execution steps
    - Polls for task executions
    - Executes workflow and task code
    - Reports results back to the server

    ## CPU-Bound Task Support

    For CPU-bound tasks (pandas, numpy, ML inference), provide an executor:

        # For I/O-bound sync tasks (default)
        worker = Worker.builder()
            .task_executor(ThreadPoolExecutor(max_workers=10))
            .build()

        # For CPU-bound tasks (true parallelism)
        worker = Worker.builder()
            .task_executor(ProcessPoolExecutor(max_workers=4))
            .build()

    Async tasks always run in the event loop. Sync tasks run in the executor.

    Example:
        >>> from orcher import Worker, workflow, task
        >>>
        >>> @workflow(name="OrderWorkflow", version="1.0")
        ... async def order_workflow(ctx: WorkflowContext, order_id: str) -> dict:
        ...     charge = await ctx.execute_task(charge_card, amount=100)
        ...     return {"status": "completed", "charge": charge}
        >>>
        >>> @task(name="charge-card", timeout=30.0)
        ... async def charge_card(ctx: TaskContext, amount: int) -> dict:
        ...     return {"charge_id": "ch_xxx", "amount": amount}
        >>>
        >>> async def main():
        ...     worker = (
        ...         Worker.builder()
        ...         .server_url("http://localhost:50051")
        ...         .namespace("default")
        ...         .task_queue("order-queue")
        ...         .build()
        ...     )
        ...     await worker.run()
    """

    def __init__(
        self,
        config: WorkerConfig,
        task_executor: ConcurrentExecutor | None = None,
        workflow_interceptors: list[WorkflowInterceptor] | None = None,
        task_interceptors: list[TaskInterceptor] | None = None,
    ) -> None:
        """
        Create a new Worker instance.

        Args:
            config: Worker configuration
            task_executor: Optional executor for sync tasks. If not provided,
                          a ThreadPoolExecutor is created. Use ProcessPoolExecutor
                          for CPU-bound tasks.
            workflow_interceptors: Optional list of workflow interceptors.
            task_interceptors: Optional list of task interceptors.
        """
        self._config = config
        self._state = WorkerState.STOPPED
        self._start_time: datetime | None = None
        self._stop_time: datetime | None = None

        # Executor for sync tasks (CPU-bound or blocking I/O)
        self._task_executor = task_executor
        self._owns_executor = False  # True when the worker created the executor itself

        self._stats = ServiceStats(
            worker_id=config.identity,
            state=WorkerState.STOPPED,
        )

        # Shutdown coordination
        self._shutdown_event = asyncio.Event()
        self._shutdown_requested = False

        # Handler registries, populated from GlobalRegistry.
        # Each maps a workflow/task/actor/query/update name to its metadata.
        self._workflow_handlers: dict[str, WorkflowMetadata] = {}
        self._task_handlers: dict[str, TaskMetadata] = {}
        self._actor_handlers: dict[str, ActorMetadata] = {}
        self._actor_registration_id: str | None = None
        self._query_handlers: dict[str, QueryMetadata] = {}
        self._update_handlers: dict[str, UpdateMetadata] = {}

        # Created together with the bridge worker.
        self._session_manager: SessionManager | None = None

        self._workflow_interceptor_chain = WorkflowInterceptorChain()
        self._task_interceptor_chain = TaskInterceptorChain()
        for wi in workflow_interceptors or []:
            self._workflow_interceptor_chain.add(wi)
        for ti in task_interceptors or []:
            self._task_interceptor_chain.add(ti)

        # The Rust side of the bridge; None until initialized.
        self._bridge_worker: Any | None = None

        # Actor poller tasks. Workflow and task pollers are tracked separately.
        self._poller_tasks: list[asyncio.Task[None]] = []

        # Pre-built task class instances, for dependency injection.
        self._task_instances: dict[type[Any], Any] = {}

        # Keys of executions currently running, used to skip duplicates.
        self._workflow_in_flight: set[str] = set()
        self._task_in_flight: set[str] = set()
        self._actor_in_flight: set[str] = set()

        self._load_handlers_from_registry()

        logger.info(f"Worker created with ID: {config.identity}")
        logger.info(
            f"Registered {len(self._workflow_handlers)} workflow(s), "
            f"{len(self._task_handlers)} task(s), and "
            f"{len(self._actor_handlers)} actor(s)"
        )

    @classmethod
    def builder(cls) -> WorkerBuilder:
        """
        Create a WorkerBuilder for fluent API construction.

        Returns:
            WorkerBuilder instance

        Example:
            >>> worker = (
            ...     Worker.builder()
            ...     .server_url("http://localhost:50051")
            ...     .namespace("default")
            ...     .task_queue("my-queue")
            ...     .task_executor(ProcessPoolExecutor(4))  # For CPU-bound tasks
            ...     .build()
            ... )
        """
        from orcher.worker.builder import WorkerBuilder as Builder

        return Builder.create()

    def register_task_instance(self, instance: Any) -> None:
        """Register a pre-built task class instance for dependency injection.

        Use this to provide task group instances that have constructor dependencies.
        The instance will be used when executing any task defined on its class.

        Args:
            instance: A fully constructed instance of a @tasks-decorated class.

        Raises:
            TypeError: If the instance's class is not a registered task class.

        Example:
            >>> client = EmailClient(api_key="...")
            >>> worker.register_task_instance(EmailTasks(client))
        """
        cls = type(instance)
        if not getattr(cls, "__orcher_task_class__", False):
            raise TypeError(
                f"{cls.__name__} is not a @tasks-decorated class. "
                f"Decorate it with @tasks before registering an instance."
            )
        self._task_instances[cls] = instance
        logger.info(f"Registered task instance: {cls.__name__}")

    def _validate_task_instances(self) -> None:
        """Validate that all class-based tasks have required instances registered.

        Inspects the __init__ signature of each task handler class. If a class
        requires constructor arguments beyond self, a pre-built instance must be
        registered via register_task_instance(). This fails at startup rather
        than when the task first runs.

        Raises:
            RuntimeError: If a task class needs dependencies but no instance was registered.
        """
        for metadata in self._task_handlers.values():
            if metadata.is_function or metadata.handler_class is None:
                continue

            cls = metadata.handler_class
            if cls in self._task_instances:
                continue

            try:
                sig = inspect.signature(cls.__init__)
                required_params = [
                    p for p in list(sig.parameters.values())[1:]  # skip self
                    if p.default is inspect.Parameter.empty
                    and p.kind
                    not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
                ]
            except (ValueError, TypeError):
                continue

            if required_params:
                param_names = ", ".join(p.name for p in required_params)
                raise RuntimeError(
                    f"Task class '{cls.__name__}' requires constructor arguments ({param_names}) "
                    f"but no instance was registered. Call "
                    f"service.register_task_instance({cls.__name__}(...)) before service.run()."
                )

    def _load_handlers_from_registry(self) -> None:
        """Load workflow, task, actor, query, and update handlers from GlobalRegistry."""
        registry = GlobalRegistry.get_instance()

        for wf_metadata in registry.list_workflows():
            self._workflow_handlers[wf_metadata.name] = wf_metadata
            handler_name = getattr(wf_metadata.handler, "__name__", str(wf_metadata.handler))
            logger.debug(f"Loaded workflow: {wf_metadata.name} -> {handler_name}")

        for task_metadata in registry.list_tasks():
            self._task_handlers[task_metadata.name] = task_metadata
            handler_name = getattr(task_metadata.handler, "__name__", str(task_metadata.handler))
            logger.debug(f"Loaded task: {task_metadata.name} -> {handler_name}")

        for actor_metadata in registry.list_actors():
            self._actor_handlers[actor_metadata.name] = actor_metadata
            cls_name = getattr(
                actor_metadata.handler_class, "__name__", str(actor_metadata.handler_class)
            )
            logger.debug(
                f"Loaded actor: {actor_metadata.name} -> {cls_name} "
                f"({actor_metadata.operation_count} operations)"
            )

        for query_metadata in registry.list_queries():
            self._query_handlers[query_metadata.name] = query_metadata
            handler_name = getattr(query_metadata.handler, "__name__", str(query_metadata.handler))
            logger.debug(f"Loaded query: {query_metadata.name} -> {handler_name}")

        for update_metadata in registry.list_updates():
            self._update_handlers[update_metadata.name] = update_metadata
            handler_name = getattr(
                update_metadata.handler, "__name__", str(update_metadata.handler)
            )
            logger.debug(f"Loaded update: {update_metadata.name} -> {handler_name}")

    @property
    def config(self) -> WorkerConfig:
        """The worker configuration."""
        return self._config

    @property
    def state(self) -> WorkerState:
        """The current worker state."""
        return self._state

    def is_running(self) -> bool:
        """Whether the worker is running."""
        return self._state == WorkerState.RUNNING

    def get_stats(self) -> ServiceStats:
        """Return a snapshot of the worker's runtime statistics."""
        uptime = 0.0
        if self._start_time:
            end_time = self._stop_time or datetime.now()
            uptime = (end_time - self._start_time).total_seconds()

        return ServiceStats(
            workflows_executed=self._stats.workflows_executed,
            tasks_executed=self._stats.tasks_executed,
            workflows_in_progress=self._stats.workflows_in_progress,
            tasks_in_progress=self._stats.tasks_in_progress,
            errors=self._stats.errors,
            uptime_seconds=uptime,
            state=self._state,
            worker_id=self._config.identity,
            started_at=self._start_time,
            stopped_at=self._stop_time,
        )

    def get_identity(self) -> WorkerIdentity:
        """Return the worker's identity and declared release."""
        return WorkerIdentity(
            worker_id=self._config.identity,
            version_id=self._config.version_id,
            binary_checksum=self._config.binary_checksum,
        )

    def get_capabilities(self) -> WorkerCapabilities:
        """Return the registered handler names and concurrency limits."""
        return WorkerCapabilities(
            workflows=list(self._workflow_handlers.keys()),
            tasks=list(self._task_handlers.keys()),
            actors=list(self._actor_handlers.keys()),
            max_concurrent_workflows=self._config.max_concurrent_workflow_executions,
            max_concurrent_tasks=self._config.max_concurrent_task_executions,
        )

    def _get_bridge_worker(self) -> Any:
        """Get the bridge worker, raising if not initialized."""
        if self._bridge_worker is None:
            raise RuntimeError("Bridge worker not initialized")
        return self._bridge_worker

    async def run(self) -> None:
        """
        Run the worker until shutdown.

        Starts the worker and blocks until shutdown is requested. Python owns
        the event loop and drives polling through the Rust bridge.

        Raises:
            RuntimeError: If the worker is already running, a task class is
                missing a required instance, or startup fails
        """
        if self._state != WorkerState.STOPPED:
            raise RuntimeError(f"Cannot start service in state: {self._state}")

        logger.info("Starting service...")
        self._state = WorkerState.STARTING
        self._shutdown_requested = False
        self._shutdown_event.clear()

        # Fail fast on task classes that need a registered instance.
        self._validate_task_instances()

        try:
            await self._initialize()

            self._state = WorkerState.RUNNING
            self._start_time = datetime.now()
            self._stop_time = None
            self._stats.state = WorkerState.RUNNING
            self._stats.started_at = self._start_time

            logger.info(
                f"Service started successfully. "
                f"Polling {self._config.task_queue} in namespace {self._config.namespace}"
            )

            await self._run_polling_loops()

        except Exception as e:
            logger.error(f"Service failed: {e}")
            self._stats.errors += 1
            raise
        finally:
            await self._cleanup()
            self._state = WorkerState.STOPPED
            self._stop_time = datetime.now()
            self._stats.state = WorkerState.STOPPED
            self._stats.stopped_at = self._stop_time
            logger.info("Service stopped")

    async def shutdown(self, *, force: bool = False, timeout_ms: int | None = None) -> None:
        """
        Request shutdown of the worker.

        Returns once shutdown is requested; run() returns when it completes.
        A second call waits for the first request.

        Args:
            force: If True, cancel pollers at once instead of draining work
            timeout_ms: Custom timeout for graceful shutdown
        """
        if self._state == WorkerState.STOPPED:
            return

        if self._shutdown_requested:
            await self._shutdown_event.wait()
            return

        self._shutdown_requested = True
        timeout = timeout_ms or self._config.shutdown_grace_time_ms

        if force:
            logger.info("Force shutdown requested")
            self._state = WorkerState.FORCE_SHUTDOWN
        else:
            logger.info(f"Graceful shutdown requested (timeout: {timeout}ms)")
            self._state = WorkerState.SHUTTING_DOWN

        self._stats.state = self._state

        if self._bridge_worker is not None:
            self._bridge_worker.request_shutdown()

        self._shutdown_event.set()

    async def _initialize(self) -> None:
        """Create the task executor and the native bridge worker."""
        logger.debug("Initializing service...")

        if not self._workflow_handlers and not self._task_handlers:
            logger.warning(
                "No workflows or tasks registered. "
                "Ensure decorated functions are imported before starting the service."
            )

        if self._task_executor is None:
            self._task_executor = ThreadPoolExecutor(
                max_workers=self._config.max_concurrent_task_executions,
                thread_name_prefix="orcher-task-",
            )
            self._owns_executor = True
            max_workers = self._config.max_concurrent_task_executions
            logger.debug(f"Created default ThreadPoolExecutor with {max_workers} workers")

        if is_native_available():
            native = get_native_module()

            native_config = native.WorkerConfig(
                server_url=self._config.server_url,
                task_queue=self._config.task_queue,
                namespace=self._config.namespace,
                max_concurrent_workflows=self._config.max_concurrent_workflow_executions,
                max_concurrent_tasks=self._config.max_concurrent_task_executions,
                identity=self._config.identity,
                workflow_poller_count=self._config.workflow_poller_count,
                task_poller_count=self._config.task_poller_count,
                actor_poller_count=self._config.actor_poller_count if self._actor_handlers else 0,
                max_concurrent_actors=self._config.max_concurrent_actor_operations,
                organization_id=self._config.organization_id,
                api_key=self._config.api_key,
                version_id=self._config.version_id,
            )

            self._bridge_worker = native.BridgeWorker(native_config)
            logger.debug("Bridge worker initialized")

            # Register actor handlers with their per-operation modes so the
            # server can tell shared operations from exclusive ones. Without
            # registration every operation runs exclusive, which is safe, so a
            # failure here is logged loudly and the worker stays up.
            if self._actor_handlers:
                handlers = [
                    {
                        "actor_name": actor_name,
                        "operation": op_name,
                        "mode": op.mode.value,
                    }
                    for actor_name, meta in self._actor_handlers.items()
                    for op_name, op in meta.operations.items()
                ]
                try:
                    registration_id = await self._bridge_worker.register_actor_handlers(
                        json.dumps(handlers),
                        json.dumps({"sdk": "python"}),
                    )
                    self._actor_registration_id = registration_id
                    logger.info(
                        "Registered %d actor handler(s) with the server "
                        "(registration_id=%s)",
                        len(handlers),
                        registration_id,
                    )
                except Exception:
                    logger.warning(
                        "Actor handler registration failed — actor operations "
                        "will run in exclusive mode (shared concurrency "
                        "disabled) until the worker restarts",
                        exc_info=True,
                    )

            from orcher.worker.session import SessionManager

            self._session_manager = SessionManager(
                max_sessions=10,
                bridge_worker=self._bridge_worker,
            )
        else:
            logger.warning("Native module not available, running in mock mode")

        logger.debug("Service initialized")

    async def _run_polling_loops(self) -> None:
        """
        Run the polling loops until shutdown, then drain and stop them.

        Python owns the event loop and calls into Rust, which handles server
        I/O. Without a bridge worker this only waits for shutdown.
        """
        logger.info(
            f"Starting {self._config.workflow_poller_count} workflow poller(s) and "
            f"{self._config.task_poller_count} task poller(s)"
        )

        if self._bridge_worker is None:
            logger.info("Running in mock mode - waiting for shutdown")
            await self._shutdown_event.wait()
            return

        # The workflow and task pollers are kept apart from the actor pollers
        # because they stop later; see below.
        driver_pollers: list[asyncio.Task[None]] = []
        try:
            # Raised by the bridge when a poll ends because of shutdown.
            native = get_native_module()
            shutdown_event_cls = native.WorkerShutdownEvent

            driver_pollers = [
                asyncio.create_task(
                    self._workflow_polling_loop(i, shutdown_event_cls),
                    name=f"workflow-poller-{i}",
                )
                for i in range(self._config.workflow_poller_count)
            ]

            driver_pollers += [
                asyncio.create_task(
                    self._task_polling_loop(i, shutdown_event_cls),
                    name=f"task-poller-{i}",
                )
                for i in range(self._config.task_poller_count)
            ]

            # Actor pollers run only when actors are registered.
            if self._actor_handlers:
                for i in range(self._config.actor_poller_count):
                    task = asyncio.create_task(
                        self._actor_polling_loop(i, shutdown_event_cls),
                        name=f"actor-poller-{i}",
                    )
                    self._poller_tasks.append(task)

            logger.info("All pollers started")

            await self._shutdown_event.wait()
            logger.info("Shutdown signal received")

            for task in self._poller_tasks:
                task.cancel()
            if self._poller_tasks:
                await asyncio.gather(*self._poller_tasks, return_exceptions=True)

            if self._state == WorkerState.FORCE_SHUTDOWN:
                for task in driver_pollers:
                    task.cancel()
            else:
                # The workflow and task pollers keep running until their
                # drivers stop. During the shutdown grace period each driver
                # still delivers the results of polls it already had in
                # flight, and the engine has already claimed that work. If
                # nothing here took it, the engine would wait out the claim
                # timeout before another worker could run it. Stopping the
                # drivers closes the pollers' channels, which ends the pollers.
                await self._wait_for_in_flight_executions()
                await self._stop_drivers()
                # Still bounded: if a driver could not be stopped its channel
                # may never close, and shutdown must still return.
                _, still_polling = await asyncio.wait(
                    driver_pollers, timeout=DRIVER_POLLER_STOP_TIMEOUT_SECS
                )
                for task in still_polling:
                    task.cancel()
            await asyncio.gather(*driver_pollers, return_exceptions=True)

        except asyncio.CancelledError:
            logger.info("Polling cancelled")
            # The workflow and task pollers do not watch for shutdown, so
            # nothing else would stop them.
            for task in driver_pollers:
                task.cancel()
            raise

    async def _workflow_polling_loop(
        self, poller_index: int, shutdown_event_cls: type[BaseException]
    ) -> None:
        """
        Poll for workflow tasks through the Rust bridge and dispatch them.

        Each poller uses its index as the slot_index, so it polls its own
        dedicated channel and does not contend on a mutex with other pollers.

        Args:
            poller_index: Index of this poller (used as slot_index)
            shutdown_event_cls: Exception class for shutdown signals
        """
        logger.info(f"Workflow poller {poller_index} started (slot {poller_index})")

        # Not bounded by `_shutdown_requested`: this loop ends when the bridge
        # closes its channel after the workflow driver stops, so work the
        # driver delivers during shutdown still runs.
        while True:
            try:
                task_bytes = await self._get_bridge_worker().poll_workflow_task(poller_index)

                if task_bytes is None:
                    # No work available; back off briefly before polling again.
                    await asyncio.sleep(0.1)
                    continue

                # Response fields: execution_request, workflow_id, run_id,
                # task_token, stream_entry_id.
                response = json.loads(task_bytes)

                request = response.get("execution_request", {})
                workflow_id = response.get("workflow_id", "")
                run_id = response.get("run_id", "")
                task_token = response.get("task_token", "")
                stream_entry_id = response.get("stream_entry_id")

                dedup_key = f"{workflow_id}:{run_id}"

                if dedup_key in self._workflow_in_flight:
                    # Nothing will answer this activation, so the engine keeps
                    # it claimed until its claim timeout expires.
                    logger.error(
                        "Dropping workflow activation while another for the same run "
                        "is in flight: workflow_id=%s run_id=%s",
                        workflow_id,
                        run_id,
                    )
                    await asyncio.sleep(0.01)
                    continue

                self._workflow_in_flight.add(dedup_key)

                # Fire-and-forget so the poller can return to polling at once.
                asyncio.create_task(
                    self._handle_workflow_task(
                        request, dedup_key, workflow_id, run_id, task_token, stream_entry_id
                    ),
                    name=f"workflow-{run_id}",
                )

            except shutdown_event_cls:
                logger.debug(f"Workflow poller {poller_index} received shutdown signal")
                break
            except asyncio.CancelledError:
                logger.debug(f"Workflow poller {poller_index} cancelled")
                break
            except Exception as e:
                if self._bridge_worker is None:
                    # Torn down: there is nothing left to poll.
                    break
                logger.error(f"Workflow poller {poller_index} error: {e}")
                await asyncio.sleep(1.0)

        logger.info(f"Workflow poller {poller_index} stopped")

    async def _task_polling_loop(
        self, poller_index: int, shutdown_event_cls: type[BaseException]
    ) -> None:
        """
        Poll for tasks through the Rust bridge and dispatch them.

        Each poller uses its index as the slot_index, so it polls its own
        dedicated channel and does not contend on a mutex with other pollers.

        Args:
            poller_index: Index of this poller (used as slot_index)
            shutdown_event_cls: Exception class for shutdown signals
        """
        logger.info(f"Task poller {poller_index} started (slot {poller_index})")

        # Not bounded by `_shutdown_requested`, like the workflow poller: this
        # loop ends when the bridge closes its channel after the task driver
        # stops, so work the driver delivers during shutdown still runs.
        while True:
            try:
                task_bytes = await self._get_bridge_worker().poll_task(poller_index)

                if task_bytes is None:
                    # No work available; back off briefly before polling again.
                    await asyncio.sleep(0.1)
                    continue

                request = json.loads(task_bytes)

                workflow_id = request.get("workflow_id", "")
                task_id = request.get("task_id", "")
                dedup_key = f"{workflow_id}:{task_id}"

                if dedup_key in self._task_in_flight:
                    logger.debug(f"Skipping duplicate task: {dedup_key}")
                    await asyncio.sleep(0.01)
                    continue

                self._task_in_flight.add(dedup_key)

                # Fire-and-forget so the poller can return to polling at once.
                asyncio.create_task(
                    self._handle_task(request, dedup_key),
                    name=f"task-{task_id}",
                )

            except shutdown_event_cls:
                logger.debug(f"Task poller {poller_index} received shutdown signal")
                break
            except asyncio.CancelledError:
                logger.debug(f"Task poller {poller_index} cancelled")
                break
            except Exception as e:
                logger.error(f"Task poller {poller_index} error: {e}")
                await asyncio.sleep(1.0)

        logger.info(f"Task poller {poller_index} stopped")

    async def _handle_workflow_task(
        self,
        request: dict[str, Any],
        dedup_key: str,
        workflow_id: str,
        run_id: str,
        task_token: str,
        stream_entry_id: str | None,
    ) -> None:
        """
        Execute a workflow task and report its result or failure.

        Args:
            request: The workflow execution request from the native core
            dedup_key: Key for deduplication tracking
            workflow_id: The workflow ID
            run_id: The run/execution ID
            task_token: Token for completing/failing the task
            stream_entry_id: Optional stream entry ID for acknowledgment
        """
        try:
            self._stats.workflows_in_progress += 1

            result = await self._execute_workflow(request)

            result_json = json.dumps(result)
            await self._get_bridge_worker().complete_workflow_task(
                workflow_id=workflow_id,
                execution_id=run_id,
                result_json=result_json,
                task_token=task_token,
                stream_entry_id=stream_entry_id,
            )

            self._stats.workflows_executed += 1
            logger.debug(f"Completed workflow: {run_id}")

        except Exception as e:
            logger.exception(f"Workflow execution failed: {e}")
            self._stats.errors += 1

            try:
                await self._get_bridge_worker().fail_workflow_task(
                    workflow_id=workflow_id,
                    execution_id=run_id,
                    task_token=task_token,
                    error_message=str(e),
                    error_type="WorkflowExecutionError",
                )
            except Exception as fail_error:
                logger.error(f"Failed to report workflow failure: {fail_error}")

        finally:
            self._stats.workflows_in_progress -= 1
            self._workflow_in_flight.discard(dedup_key)

    async def _handle_task(self, request: dict[str, Any], dedup_key: str) -> None:
        """
        Execute a task and report its result or failure.

        Args:
            request: The task request
            dedup_key: Key for deduplication tracking
        """
        task_id = request.get("task_id", "")
        task_token = request.get("task_token", "")

        try:
            self._stats.tasks_in_progress += 1

            result = await self._execute_task(request)

            result_json = json.dumps(result)
            await self._get_bridge_worker().complete_task(
                task_token=task_token,
                result_json=result_json,
            )

            self._stats.tasks_executed += 1
            logger.debug(f"Completed task: {task_id}")

        except Exception as e:
            logger.exception(f"Task execution failed: {e}")
            self._stats.errors += 1

            # The engine decides whether to retry from the reported error type,
            # and a retry policy lists types by class name, so the exception's
            # class name is sent. A truthy `non_retryable` attribute on the
            # exception stops retries regardless of the policy.
            try:
                await self._get_bridge_worker().fail_task(
                    task_token,
                    str(e),
                    type(e).__name__,
                    bool(getattr(e, "non_retryable", False)),
                )
            except Exception as fail_error:
                logger.error(f"Failed to report task failure: {fail_error}")

        finally:
            self._stats.tasks_in_progress -= 1
            self._task_in_flight.discard(dedup_key)

    async def _execute_workflow(self, request: dict[str, Any]) -> dict[str, Any]:
        """
        Execute a workflow.

        Workflows always run in the event loop.

        Args:
            request: ExecutionRequest dict

        Returns:
            ExecutionResult dict
        """
        run_id = request.get("run_id", "unknown")

        # Extract workflow info from jobs
        workflow_type = "unknown"
        workflow_input = {}
        workflow_id = ""
        task_queue = ""

        jobs = request.get("jobs", [])
        for job in jobs:
            if isinstance(job, dict) and "StartWorkflow" in job:
                start_job = job["StartWorkflow"]
                workflow_type = start_job.get("workflow_type", "unknown")
                workflow_input = start_job.get("input", {})
                workflow_id = start_job.get("workflow_id", "")
                task_queue = start_job.get("task_queue", "")
                break

        if not workflow_id:
            execution = request.get("execution", {})
            workflow_id = execution.get("workflow_id", "")

        logger.debug(f"Executing workflow: type={workflow_type}, run_id={run_id}")

        metadata = self._workflow_handlers.get(workflow_type)
        if metadata is None:
            return {
                "run_id": run_id,
                "successful": False,
                "commands": [],
                "query_responses": [],
                "update_results": [],
                "error": {
                    "message": f"Unknown workflow type: {workflow_type}",
                    "error_type": "WorkflowCode",
                    "details": None,
                    "retryable": False,
                },
                "restart_fresh": None,
            }

        from orcher.workflow.context import WorkflowContext
        from orcher.workflow.info import WorkflowInfo

        # When the engine journaled what this activation hands the workflow.
        # The jobs carry no times, so the native layer reads them from the
        # journal; the workflow's clock is built from them and nothing else.
        journal_times = request.get("journal_times") or {}
        info = WorkflowInfo(
            workflow_id=workflow_id,
            run_id=run_id,
            workflow_type=workflow_type,
            task_queue=task_queue,
            namespace=request.get("namespace", "default"),
            attempt=request.get("attempt", 1),
            started_at=_workflow_started_at(journal_times, run_id),
        )
        ctx = WorkflowContext(info=info, replaying=request.get("is_replaying", False))
        ctx._record_journal_times(journal_times)

        ictx = InterceptorContext(
            workflow_id=workflow_id,
            run_id=run_id,
            workflow_type=workflow_type,
        )
        exec_info = ExecutionInfo(input_data=workflow_input)

        await self._workflow_interceptor_chain.notify_enter(ictx, exec_info)

        start_time = datetime.now()

        try:
            # Results of already-completed commands, returned as-is on replay.
            cached_results = self._extract_cached_results_from_jobs(jobs)
            if cached_results:
                ctx._set_cached_results(cached_results)

            async def run_workflow(data: Any) -> Any:
                # Normalize the workflow input into call arguments: a dict is
                # spread as kwargs, any other non-None value is passed as one
                # positional argument, and None passes nothing. This matches
                # the query and update handlers.
                if isinstance(data, dict):
                    wf_args: tuple[Any, ...] = ()
                    wf_kwargs: dict[str, Any] = data
                elif data is None:
                    wf_args, wf_kwargs = (), {}
                else:
                    wf_args, wf_kwargs = (data,), {}

                if metadata.is_function:
                    workflow_fn = metadata.handler
                    if asyncio.iscoroutinefunction(workflow_fn):
                        return await workflow_fn(ctx, *wf_args, **wf_kwargs)
                    return workflow_fn(ctx, *wf_args, **wf_kwargs)
                workflow_instance = metadata.handler()
                if asyncio.iscoroutinefunction(metadata.run_method):
                    return await workflow_instance.run(ctx, *wf_args, **wf_kwargs)
                return workflow_instance.run(ctx, *wf_args, **wf_kwargs)

            # Each activation runs the workflow code once, through the
            # registered interceptors. One that suspends raises
            # WorkflowSuspendedError through them: it neither completed nor
            # failed.
            result = await self._workflow_interceptor_chain.execute(
                ictx, workflow_input, run_workflow
            )

            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            exec_info.output_data = result
            exec_info.duration_ms = duration_ms
            exec_info.success = True
            await self._workflow_interceptor_chain.notify_success(ictx, exec_info)
            await self._workflow_interceptor_chain.notify_exit(ictx, exec_info)

            query_responses = self._process_query_jobs(jobs, ctx)
            update_results = await self._process_update_jobs(jobs, ctx)

            # What the workflow issued in this last activation goes out ahead
            # of its completion: an event it sent, a child it started without
            # waiting. Closures it left running are waited for first.
            await ctx._closures_settled()
            result_bytes = json.dumps(result).encode("utf-8") if result else b"null"
            return {
                "run_id": run_id,
                "successful": True,
                "commands": [
                    *ctx._take_commands(),
                    {
                        "CompleteWorkflow": {
                            "result": {
                                "data": list(result_bytes),
                                "metadata": {},
                            }
                        }
                    },
                ],
                "query_responses": query_responses,
                "update_results": update_results,
                "error": None,
                "restart_fresh": None,
            }

        except WorkflowSuspendedError as e:
            # Suspension is not a failure: the workflow is waiting on commands,
            # so interceptors are not notified of an error.
            logger.debug(f"Workflow suspended: {e.reason}")
            # A closure still running beside the step that suspended is waited
            # for, so its result is reported with this activation and it does
            # not run again on the next one.
            await ctx._closures_settled()
            commands = ctx._take_commands()

            # Queries and updates are answered even while suspended.
            query_responses = self._process_query_jobs(jobs, ctx)
            update_results = await self._process_update_jobs(jobs, ctx)

            return {
                "run_id": run_id,
                "successful": True,
                "commands": commands,
                "query_responses": query_responses,
                "update_results": update_results,
                "error": None,
                "restart_fresh": None,
            }

        except Exception as e:
            logger.exception(f"Workflow execution failed: {e}")
            # The activation fails and its commands are dropped; closures it
            # left running still finish, rather than being abandoned mid-way.
            await ctx._closures_settled()

            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            exec_info.error = e
            exec_info.duration_ms = duration_ms
            exec_info.success = False
            await self._workflow_interceptor_chain.notify_error(ictx, exec_info)
            await self._workflow_interceptor_chain.notify_exit(ictx, exec_info)

            # Queries and updates are answered even when the workflow fails.
            query_responses = self._process_query_jobs(jobs, ctx)
            update_results = await self._process_update_jobs(jobs, ctx)

            return {
                "run_id": run_id,
                "successful": False,
                "commands": [],
                "query_responses": query_responses,
                "update_results": update_results,
                "error": {
                    "message": str(e),
                    "error_type": _workflow_error_type(e),
                    "details": traceback.format_exc(),
                    "retryable": False,
                },
                "restart_fresh": None,
            }

    def _process_query_jobs(
        self, jobs: list[dict[str, Any]], ctx: Any
    ) -> list[dict[str, Any]]:
        """Process ProcessQuery jobs and return query responses.

        Queries are synchronous, read-only handlers that inspect workflow state.
        A handler is looked up first in the GlobalRegistry (populated by the
        @query() decorator), then in WorkflowContext._query_handlers (manual
        registration). A handler error becomes a Failed response.
        """
        query_responses: list[dict[str, Any]] = []

        for job in jobs:
            if not isinstance(job, dict) or "ProcessQuery" not in job:
                continue

            query_job = job["ProcessQuery"]
            query_id = query_job.get("query_id", "")
            query_type = query_job.get("query_type", "")

            # The worker's registry takes precedence over the context.
            query_metadata = self._query_handlers.get(query_type)
            handler = None

            if query_metadata is not None:
                handler = query_metadata.handler
            elif hasattr(ctx, "_query_handlers"):
                handler = ctx._query_handlers.get(query_type)

            if handler is None:
                query_responses.append({
                    "query_id": query_id,
                    "result": {
                        "Failed": {
                            "message": f"No handler registered for query '{query_type}'",
                            "details": None,
                        }
                    },
                })
                continue

            try:
                # Only the first argument payload is used.
                arguments = query_job.get("arguments", [])
                args_data = None
                if arguments:
                    first_arg = arguments[0]
                    if isinstance(first_arg, dict) and "data" in first_arg:
                        raw_bytes = bytes(first_arg["data"])
                        args_data = json.loads(raw_bytes)

                # A dict argument is spread as kwargs; any other value is positional.
                if args_data is not None:
                    if isinstance(args_data, dict):
                        result = handler(ctx, **args_data)
                    else:
                        result = handler(ctx, args_data)
                else:
                    result = handler(ctx)

                result_bytes = json.dumps(result).encode("utf-8") if result is not None else b"null"
                query_responses.append({
                    "query_id": query_id,
                    "result": {
                        "Success": {
                            "output": {
                                "data": list(result_bytes),
                                "metadata": {},
                            }
                        }
                    },
                })
            except Exception as e:
                logger.warning(f"Query handler '{query_type}' failed: {e}")
                query_responses.append({
                    "query_id": query_id,
                    "result": {
                        "Failed": {
                            "message": str(e),
                            "details": None,
                        }
                    },
                })

        return query_responses

    async def _process_update_jobs(
        self, jobs: list[dict[str, Any]], ctx: Any
    ) -> list[dict[str, Any]]:
        """Process UpdateState jobs and return update results.

        Updates are handlers, sync or async, that can mutate workflow state and
        return results. A handler is looked up first in the GlobalRegistry
        (populated by the @update() decorator), then in
        WorkflowContext._update_handlers (manual registration). A missing
        handler becomes Rejected; a handler error becomes Failed.
        """
        update_results: list[dict[str, Any]] = []

        for job in jobs:
            if not isinstance(job, dict) or "UpdateState" not in job:
                continue

            update_job = job["UpdateState"]
            update_id = update_job.get("update_id", "")
            update_name = update_job.get("update_name", "")

            # The worker's registry takes precedence over the context.
            update_metadata = self._update_handlers.get(update_name)
            handler = None

            if update_metadata is not None:
                handler = update_metadata.handler
            elif hasattr(ctx, "_update_handlers"):
                handler = ctx._update_handlers.get(update_name)

            if handler is None:
                update_results.append({
                    "update_id": update_id,
                    "result": {
                        "Rejected": {
                            "message": f"No handler registered for update '{update_name}'",
                        }
                    },
                })
                continue

            try:
                payload = update_job.get("payload", {})
                args_data = None
                if isinstance(payload, dict) and "data" in payload:
                    raw_bytes = bytes(payload["data"])
                    if raw_bytes:
                        args_data = json.loads(raw_bytes)

                # A dict argument is spread as kwargs; any other value is positional.
                if args_data is not None:
                    if isinstance(args_data, dict):
                        if asyncio.iscoroutinefunction(handler):
                            result = await handler(ctx, **args_data)
                        else:
                            result = handler(ctx, **args_data)
                    else:
                        if asyncio.iscoroutinefunction(handler):
                            result = await handler(ctx, args_data)
                        else:
                            result = handler(ctx, args_data)
                else:
                    if asyncio.iscoroutinefunction(handler):
                        result = await handler(ctx)
                    else:
                        result = handler(ctx)

                result_bytes = json.dumps(result).encode("utf-8") if result is not None else b"null"
                update_results.append({
                    "update_id": update_id,
                    "result": {
                        "Completed": {
                            "output": {
                                "data": list(result_bytes),
                                "metadata": {},
                            }
                        }
                    },
                })
            except Exception as e:
                logger.warning(f"Update handler '{update_name}' failed: {e}")
                update_results.append({
                    "update_id": update_id,
                    "result": {
                        "Failed": {
                            "message": str(e),
                            "details": None,
                        }
                    },
                })

        return update_results

    async def _execute_task(self, request: dict[str, Any]) -> Any:
        """
        Execute a task.

        Async tasks run in the event loop.
        Sync tasks run in the executor (ThreadPoolExecutor or ProcessPoolExecutor).

        Args:
            request: Task request dict

        Returns:
            Task result
        """
        task_id = request.get("task_id", "unknown")
        task_type = request.get("task_type", "unknown")

        logger.debug(f"Executing task: type={task_type}, task_id={task_id}")

        # Internal session tasks are handled by the SessionManager.
        from orcher.workflow.session import SESSION_COMPLETE_TASK, SESSION_CREATE_TASK

        if task_type == SESSION_CREATE_TASK and self._session_manager is not None:
            task_input = request.get("input", {})
            task_queue = request.get("task_queue", "")
            return await self._session_manager.handle_create_session(task_input, task_queue)

        if task_type == SESSION_COMPLETE_TASK and self._session_manager is not None:
            task_input = request.get("input", {})
            return await self._session_manager.handle_complete_session(task_input)

        metadata = self._task_handlers.get(task_type)
        if metadata is None:
            raise ValueError(f"Unknown task type: {task_type}")

        task_input = request.get("input", {})

        from orcher.task.context import TaskContext
        from orcher.task.info import TaskInfo

        task_info = TaskInfo(
            task_id=task_id,
            task_type=task_type,
            workflow_id=request.get("workflow_id", ""),
            run_id=request.get("execution_id", ""),
            task_queue=request.get("task_queue", ""),
            namespace=request.get("namespace", "default"),
            attempt=_task_attempt(request),
            scheduled_at=datetime.now(),
            started_at=datetime.now(),
            heartbeat_timeout=_optional_millis(request.get("heartbeat_timeout_ms")),
            start_to_close_timeout=_optional_millis(request.get("start_to_close_timeout_ms")),
        )
        # The worker heartbeats the task on its own. The context forwards the
        # task's explicit heartbeats to the bridge, and the engine's request to
        # stop the task reaches the context's cancellation token.
        bridge = self._get_bridge_worker()
        task_token = request.get("task_token", "")
        ctx = TaskContext(
            info=task_info,
            heartbeat_sender=lambda details: bool(bridge.heartbeat_task(task_token, details)),
        )
        cancellation_watch = asyncio.ensure_future(
            _watch_task_cancellation(bridge, task_token, ctx.cancellation_token)
        )

        if metadata.is_function:
            handler = metadata.handler
        else:
            if metadata.handler_class is None or metadata.method_name is None:
                raise RuntimeError(
                    f"Task '{task_type}' is class-based but missing handler_class or method_name"
                )
            handler_instance = self._task_instances.get(metadata.handler_class)
            if handler_instance is None:
                handler_instance = metadata.handler_class()
            handler = getattr(handler_instance, metadata.method_name, None)

            # `@tasks` replaces the method on the class with a TaskReference, so
            # a lookup by name returns that reference, which is not callable.
            # The real function is on the metadata; bind it to the instance so
            # it still receives `self`.
            if not callable(handler):
                raw = metadata.handler
                if raw is None:
                    raise RuntimeError(
                        f"Task '{task_type}' has no callable handler: "
                        f"'{metadata.method_name}' resolved to {type(handler).__name__}"
                    )
                handler = raw.__get__(handler_instance, type(handler_instance))

        ictx = InterceptorContext(
            workflow_id=request.get("workflow_id", ""),
            run_id=request.get("execution_id", ""),
            workflow_type="",
            task_name=task_type,
            task_id=task_id,
        )
        exec_info = ExecutionInfo(input_data=task_input)

        await self._task_interceptor_chain.notify_enter(ictx, exec_info)

        # The engine retries a failed task as a new attempt; this worker sees
        # only the attempt number, not the error that caused the retry.
        attempt = _task_attempt(request)
        if attempt > 1:
            await self._task_interceptor_chain.notify_retry(
                ictx, exec_info, attempt, _max_attempts(metadata.retry_policy)
            )

        start_time = datetime.now()

        async def run_task(data: Any) -> Any:
            if asyncio.iscoroutinefunction(handler):
                return await handler(ctx, **data)
            # Sync handlers run in the executor so blocking or CPU-bound
            # work does not stall the event loop.
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self._task_executor,
                lambda: handler(ctx, **data),
            )

        try:
            # The task runs through the registered interceptors.
            result = await self._task_interceptor_chain.execute(ictx, task_input, run_task)

            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            exec_info.output_data = result
            exec_info.duration_ms = duration_ms
            exec_info.success = True
            await self._task_interceptor_chain.notify_success(ictx, exec_info)
            await self._task_interceptor_chain.notify_exit(ictx, exec_info)

            return self._make_json_serializable(result)

        except Exception as e:
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            exec_info.error = e
            exec_info.duration_ms = duration_ms
            exec_info.success = False
            await self._task_interceptor_chain.notify_error(ictx, exec_info)
            await self._task_interceptor_chain.notify_exit(ictx, exec_info)
            raise

        finally:
            cancellation_watch.cancel()

    async def _actor_polling_loop(
        self, poller_index: int, shutdown_event_cls: type[BaseException]
    ) -> None:
        """Poll for actor operations through the Rust bridge and dispatch them.

        Unlike the workflow and task pollers, this loop stops as soon as
        shutdown is requested.

        Args:
            poller_index: Index of this poller (used as slot_index)
            shutdown_event_cls: Exception class for shutdown signals
        """
        logger.info(f"Actor poller {poller_index} started (slot {poller_index})")

        while not self._shutdown_requested:
            try:
                task_bytes = await self._get_bridge_worker().poll_actor_operation(poller_index)

                if task_bytes is None:
                    await asyncio.sleep(0.1)
                    continue

                request = json.loads(task_bytes)

                operation_id = request.get("operation_id", "")
                dedup_key = (
                    f"{request.get('actor_name', '')}:{request.get('key', '')}:{operation_id}"
                )

                if dedup_key in self._actor_in_flight:
                    logger.debug(f"Skipping duplicate actor op: {dedup_key}")
                    await asyncio.sleep(0.01)
                    continue

                self._actor_in_flight.add(dedup_key)

                asyncio.create_task(
                    self._handle_actor_operation(request, dedup_key),
                    name=f"actor-op-{operation_id}",
                )

            except shutdown_event_cls:
                logger.debug(f"Actor poller {poller_index} received shutdown signal")
                break
            except asyncio.CancelledError:
                logger.debug(f"Actor poller {poller_index} cancelled")
                break
            except Exception as e:
                logger.error(f"Actor poller {poller_index} error: {e}")
                await asyncio.sleep(1.0)

        logger.info(f"Actor poller {poller_index} stopped")

    async def _handle_actor_operation(self, request: dict[str, Any], dedup_key: str) -> None:
        """Handle an actor operation.

        Args:
            request: The actor operation request from the native bridge
            dedup_key: Key for deduplication tracking
        """
        operation_id = request.get("operation_id", "")
        execution_id = request.get("execution_id", "")

        try:
            result = await self._execute_actor_operation(request)

            result_json = json.dumps(result)
            await self._get_bridge_worker().complete_actor_operation(
                operation_id=operation_id,
                execution_id=execution_id,
                result_json=result_json,
            )

            logger.debug(f"Completed actor operation: {operation_id}")

        except Exception as e:
            logger.exception(f"Actor operation failed: {e}")
            self._stats.errors += 1

            try:
                await self._get_bridge_worker().fail_actor_operation(
                    operation_id=operation_id,
                    execution_id=execution_id,
                    error_message=str(e),
                    error_type="ActorOperationError",
                )
            except Exception as fail_error:
                logger.error(f"Failed to report actor failure: {fail_error}")

        finally:
            self._actor_in_flight.discard(dedup_key)

    async def _execute_actor_operation(self, request: dict[str, Any]) -> Any:
        """Execute an actor operation.

        Creates an actor instance, builds the appropriate context
        (ActorContext or SharedActorContext), and invokes the handler method.

        Args:
            request: Actor operation request dict with fields:
                actor_name, key, operation, payload, mode,
                execution_id, metadata

        Returns:
            Operation result (JSON-serializable)
        """
        from orcher.actor.context import ActorContext, SharedActorContext
        from orcher.actor.state.client import ActorStateClient
        from orcher.actor.state.manager import ActorStateManager
        from orcher.actor.types import ActorKey

        actor_name = request.get("actor_name", "")
        key = request.get("key", "")
        operation_name = request.get("operation", "")
        execution_id = request.get("execution_id", "")
        mode_str = request.get("mode", "exclusive")

        logger.debug(
            f"Executing actor operation: {actor_name}.{operation_name}"
            f"(key={key}, exec={execution_id})"
        )

        actor_meta = self._actor_handlers.get(actor_name)
        if actor_meta is None:
            raise ValueError(f"Unknown actor type: {actor_name}")

        op_meta = actor_meta.operations.get(operation_name)
        if op_meta is None:
            raise ValueError(
                f"Unknown operation '{operation_name}' on actor '{actor_name}'. "
                f"Available: {actor_meta.operation_names()}"
            )

        payload_bytes = request.get("payload", b"")
        if isinstance(payload_bytes, str):
            # The native bridge encodes payload bytes as base64 in JSON. A plain
            # JSON string is also accepted.
            if payload_bytes:
                try:
                    operation_input = json.loads(payload_bytes)
                except ValueError:
                    import base64

                    operation_input = json.loads(base64.b64decode(payload_bytes))
            else:
                operation_input = {}
        elif isinstance(payload_bytes, (bytes, bytearray)):
            operation_input = json.loads(payload_bytes) if payload_bytes else {}
        elif isinstance(payload_bytes, list):
            # Protobuf bytes decoded as a list of ints.
            operation_input = json.loads(bytes(payload_bytes)) if payload_bytes else {}
        else:
            operation_input = payload_bytes if payload_bytes else {}

        actor_key = ActorKey(actor_name=actor_name, key=key)

        # State is read and written through the native bridge when present.
        if self._bridge_worker is not None:
            state_client = ActorStateClient(
                server_url=self._config.server_url,
                bridge=self._bridge_worker,
            )
        else:
            state_client = ActorStateClient.new_mock()

        state_manager = ActorStateManager(
            actor_key=actor_key,
            client=state_client,
            execution_id=execution_id,
        )

        # The context type depends on the operation mode.
        is_shared = mode_str in ("shared", "OPERATION_MODE_SHARED", "SHARED")
        if is_shared:
            ctx = SharedActorContext(
                actor_key=actor_key,
                state_manager=state_manager,
                execution_id=execution_id,
            )
        else:
            ctx = ActorContext(
                actor_key=actor_key,
                state_manager=state_manager,
                execution_id=execution_id,
            )

        # A fresh actor instance per operation; state lives in the state manager.
        actor_instance = actor_meta.handler_class()
        method = op_meta.method
        if method is None:
            raise RuntimeError(
                f"Operation '{operation_name}' on actor '{actor_name}' has no method reference"
            )

        # The method is an unbound function from the class, so the instance is
        # passed explicitly as self.
        if isinstance(operation_input, dict):
            if asyncio.iscoroutinefunction(method):
                result = await method(actor_instance, ctx, **operation_input)
            else:
                result = method(actor_instance, ctx, **operation_input)
        else:
            if asyncio.iscoroutinefunction(method):
                result = await method(actor_instance, ctx, operation_input)
            else:
                result = method(actor_instance, ctx, operation_input)

        return self._make_json_serializable(result)

    def _extract_cached_results_from_jobs(self, jobs: list[dict[str, Any]]) -> dict[str, Any]:
        """Extract completed results from journal jobs for replay."""
        cached_results: dict[str, Any] = {}

        # Jobs arrive in journal order. The index lets a wait with a timeout
        # tell whether its event or its deadline came first.
        for position, job in enumerate(jobs):
            if not isinstance(job, dict):
                continue

            if "CompleteTask" in job:
                task_job = job["CompleteTask"]
                task_id = task_job.get("task_id", "")
                result = task_job.get("result", {})

                if isinstance(result, dict):
                    if "Success" in result:
                        success_data = result["Success"]
                        output = success_data.get("output", {})
                        data_bytes = output.get("data", [])
                        if isinstance(data_bytes, list):
                            try:
                                result_str = bytes(data_bytes).decode("utf-8")
                                cached_results[f"task:{task_id}"] = json.loads(result_str)
                            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                                logger.warning(f"Failed to decode task result: {e}")
                    elif "Failed" in result:
                        failed = result["Failed"]
                        cached_results[f"task:{task_id}"] = {
                            TASK_FAILED_SENTINEL: True,
                            "message": failed.get("message", "Task failed"),
                            "error_type": failed.get("error_type", ""),
                        }

            elif "FireTimer" in job:
                timer_job = job["FireTimer"]
                timer_id = timer_job.get("timer_id", "")
                # The marker carries the job's journal position for waits
                # with a timeout; it stays truthy for sleep().
                cached_results[f"timer:{timer_id}"] = {"fired_at": position}

            elif "CompleteStep" in job:
                step_job = job["CompleteStep"]
                step_name = step_job.get("step_name", "")
                result = step_job.get("result")
                failure = step_job.get("failure")

                if failure:
                    cached_results[f"task:{step_name}"] = {
                        TASK_FAILED_SENTINEL: True,
                        "message": failure.get("message", "Task failed"),
                        "error_type": failure.get("error_type", ""),
                        # How many times the task ran. Without it the decode side
                        # assumes 1, so a task that exhausted its retries would
                        # report a single attempt.
                        "attempts": step_job.get("execution_attempt"),
                    }
                elif result is not None and isinstance(result, list):
                    try:
                        result_str = bytes(result).decode("utf-8")
                        cached_results[f"task:{step_name}"] = json.loads(result_str)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass

            elif "HandleEvent" in job:
                event_job = job["HandleEvent"]
                event_name = event_job.get("event_name", "")
                payload = event_job.get("payload", {})
                data_bytes = payload.get("data", []) if isinstance(payload, dict) else []
                buffer_key = f"event_buffer:{event_name}"
                if buffer_key not in cached_results:
                    cached_results[buffer_key] = []
                cached_results[buffer_key].append(data_bytes)
                positions_key = f"event_positions:{event_name}"
                cached_results.setdefault(positions_key, []).append(position)

            elif "ChildWorkflowCompleted" in job:
                child_job = job["ChildWorkflowCompleted"]
                child_wf_id = child_job.get("workflow_id", "")
                result = child_job.get("result", {})
                data_bytes = result.get("data", []) if isinstance(result, dict) else []
                if isinstance(data_bytes, list) and data_bytes:
                    try:
                        result_str = bytes(data_bytes).decode("utf-8")
                        cached_results[f"child:{child_wf_id}"] = json.loads(result_str)
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.warning(f"Failed to decode child workflow result: {e}")
                else:
                    cached_results[f"child:{child_wf_id}"] = None

            elif (
                "ChildWorkflowCanceled" in job
                or "ChildWorkflowTerminated" in job
                or "ChildWorkflowTimedOut" in job
            ):
                # A child that ended without a result failed as far as its
                # parent is concerned. Without these the parent would wait on
                # the child forever.
                ((kind, ended),) = job.items()
                if kind == "ChildWorkflowCanceled":
                    message = "child workflow was canceled"
                elif kind == "ChildWorkflowTerminated":
                    reason = ended.get("reason") if isinstance(ended, dict) else ""
                    message = (
                        f"child workflow was terminated: {reason}"
                        if reason
                        else "child workflow was terminated"
                    )
                else:
                    message = "child workflow timed out"
                cached_results[f"child:{ended.get('workflow_id', '')}"] = {
                    CHILD_FAILED_SENTINEL: True,
                    "message": message,
                }

            elif "ChildWorkflowFailed" in job:
                child_job = job["ChildWorkflowFailed"]
                child_wf_id = child_job.get("workflow_id", "")
                failure = child_job.get("failure", {})
                message = (
                    failure.get("message", "Child workflow failed")
                    if isinstance(failure, dict)
                    else "Child workflow failed"
                )
                cached_results[f"child:{child_wf_id}"] = {
                    CHILD_FAILED_SENTINEL: True,
                    "message": message,
                }

        return cached_results

    def _make_json_serializable(self, obj: Any) -> Any:
        """Convert an object to a JSON-serializable format."""
        import dataclasses

        if obj is None:
            return None
        elif isinstance(obj, (str, int, float, bool)):
            return obj
        elif isinstance(obj, (list, tuple)):
            return [self._make_json_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self._make_json_serializable(v) for k, v in obj.items()}
        elif dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return {k: self._make_json_serializable(v) for k, v in dataclasses.asdict(obj).items()}
        elif hasattr(obj, "_asdict"):
            # NamedTuple and similar.
            return self._make_json_serializable(obj._asdict())  # pyright: ignore[reportAttributeAccessIssue]
        elif hasattr(obj, "__dict__"):
            return {
                k: self._make_json_serializable(v)
                for k, v in obj.__dict__.items()
                if not k.startswith("_")
            }
        else:
            return str(obj)

    async def _wait_for_in_flight_executions(self) -> None:
        """Wait for in-flight executions to complete, up to the shutdown grace time."""
        timeout_ms = self._config.shutdown_grace_time_ms
        start_time = asyncio.get_event_loop().time()
        check_interval = 0.1

        in_flight = self._stats.workflows_in_progress + self._stats.tasks_in_progress
        if in_flight > 0:
            logger.info(f"Waiting for {in_flight} in-flight execution(s) to complete...")

        while self._stats.workflows_in_progress > 0 or self._stats.tasks_in_progress > 0:
            elapsed_ms = (asyncio.get_event_loop().time() - start_time) * 1000
            if elapsed_ms >= timeout_ms:
                remaining = self._stats.workflows_in_progress + self._stats.tasks_in_progress
                logger.warning(f"Shutdown timeout with {remaining} execution(s) in progress")
                break
            await asyncio.sleep(check_interval)

        logger.debug("All in-flight executions completed")

    async def _stop_drivers(self) -> None:
        """Stop the workflow and task drivers and wait for them; later calls do nothing.

        Each driver sends every result it holds before it stops, so when this
        returns, every workflow and task result handed to it has been reported.
        """
        if self._bridge_worker is None:
            return
        try:
            await self._bridge_worker.stop_drivers()
        except Exception:
            logger.warning("Failed to stop the drivers cleanly", exc_info=True)

    async def _cleanup(self) -> None:
        """Release the owned executor and stop and drop the bridge worker."""
        logger.debug("Cleaning up service resources...")

        # A caller-provided executor is left for the caller to shut down.
        if self._owns_executor and self._task_executor is not None:
            self._task_executor.shutdown(wait=False)
            self._task_executor = None

        # The drivers are normally stopped already, when the polling loops
        # wind down. This covers paths that skip that, such as a failed start.
        # A driver dropped without stopping would leave each activation or task
        # it holds claimed on the engine until the claim timed out.
        await self._stop_drivers()

        self._bridge_worker = None

        logger.debug("Cleanup complete")

    async def __aenter__(self) -> Worker:
        """Start the worker in the background and wait until it is running.

        Raises:
            RuntimeError: If the worker does not start within 5 seconds.
        """
        self._run_task = asyncio.create_task(self.run())

        max_wait = 5.0
        waited = 0.0
        while self._state not in (WorkerState.RUNNING, WorkerState.STOPPED):
            await asyncio.sleep(0.01)
            waited += 0.01
            if waited >= max_wait:
                raise RuntimeError("Service failed to start within timeout")

        if self._state == WorkerState.STOPPED:
            try:
                await self._run_task
            except Exception:
                raise

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Shut down the worker and wait for run() to return."""
        import contextlib

        await self.shutdown()

        if hasattr(self, "_run_task"):
            try:
                timeout = (self._config.shutdown_grace_time_ms / 1000.0) + 5.0
                await asyncio.wait_for(self._run_task, timeout=timeout)
            except asyncio.CancelledError:
                pass
            except TimeoutError:
                self._run_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._run_task
