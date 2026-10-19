"""
Worker implementation.

The Worker runs the registered workflows, tasks and actors for one task queue
of an Orcher engine.

## Architecture: Python runs the code, the worker host does the rest

The native module binds the worker host shared with the Rust and TypeScript
SDKs (``orcher-sdk-host``). The host polls the engine, keeps the worker
registered and heartbeated, runs sessions, and drains on shutdown. Python
pulls work from it when it has room to run some, runs it, and hands back the
outcome:

- Workflow activations arrive as the kernel protocol's ``RunActivation``: the
  run's history already resolved into the outcomes its code observes. The
  workflow's code runs once against them, and the answer is every durable
  call it made (see ``orcher.worker.activation``).
- Tasks arrive one at a time; their heartbeats and cancellation go through
  the host while they run.
- Actor operations arrive one at a time; the actor's state is read and
  written through the host, on the worker's connection.

The asyncio event loop drives everything:

- Sync tasks run in an executor (ThreadPoolExecutor or ProcessPoolExecutor).
- Async tasks and workflow code run directly in the event loop.

Terminology:
- Worker: a process that runs registered workflows, tasks and actors
- Task: a unit of work that may have side effects
- Event: an external message delivered to a running workflow
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import traceback
import warnings
from collections.abc import Awaitable, Callable
from concurrent.futures import Executor as ConcurrentExecutor
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from orcher._internal.arguments import unpack_arguments
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
from orcher.errors import ErrorCode, OrcherError, WorkflowSuspendedError
from orcher.interceptors.base import (
    ExecutionInfo,
    InterceptorContext,
    TaskInterceptor,
    WorkflowInterceptor,
)
from orcher.interceptors.chain import TaskInterceptorChain, WorkflowInterceptorChain
from orcher.worker.activation import (
    AskedQuery,
    AskedUpdate,
    ProtocolRefusalError,
    answer_activation,
    load_activation,
    protocol_error,
    workflow_failure,
)
from orcher.worker.config import WorkerConfig

logger = logging.getLogger(__name__)

# The SDK the worker reports to the engine when it registers.
SDK_NAME = "python"

# How much longer than its shutdown grace a worker takes to stop: the host
# keeps its drivers running past the grace to send the last results and hand
# back unfinished activations, then deregisters.
SHUTDOWN_MARGIN_SECS = 20.0


async def _watch_task_cancellation(host: Any, token: bytes, cancellation: Any) -> None:
    """Cancel ``cancellation`` once the task is asked to stop.

    The host heartbeats the task on its own, and the engine answers a
    heartbeat with a stop request when the workflow was cancelled or the
    attempt is no longer running; the host asks too when the worker stops
    past its shutdown grace. Returns without cancelling once the task's
    outcome is reported.
    """
    try:
        if await host.task_cancelled(token):
            cancellation.cancel()
    except asyncio.CancelledError:
        raise
    except Exception as e:  # a host without this call
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


def _max_attempts(retry_policy: Any) -> int:
    """The attempts a task's declared retry policy allows; 0 when it declares none."""
    if isinstance(retry_policy, dict):
        return int(retry_policy.get("max_attempts", 0) or 0)
    return int(getattr(retry_policy, "max_attempts", 0) or 0)


def _package_version() -> str:
    """This SDK's release, reported to the engine when the worker registers."""
    from orcher import __version__

    return __version__


def _task_request(assignment: dict[str, Any]) -> dict[str, Any]:
    """A task as the worker host hands it over, as the task runner reads it.

    The input is the JSON encoding of the task's arguments, packed as
    unpack_arguments reads them; empty or invalid input becomes no arguments.
    """
    raw = bytes(assignment.get("input") or [])
    task_input: Any = {}
    if raw:
        try:
            task_input = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to deserialize task input: {e}")
    return {
        "task_id": assignment.get("task_id", ""),
        "task_type": assignment.get("task_type", ""),
        "task_token": bytes(assignment.get("token") or []),
        "workflow_id": assignment.get("workflow_id", ""),
        "execution_id": assignment.get("run_id", ""),
        "task_queue": assignment.get("task_queue", ""),
        "namespace": assignment.get("namespace", "default"),
        "input": task_input,
        "attempt": assignment.get("attempt"),
        # The limits this attempt runs under, so a handler can pace its
        # heartbeats against the interval it is judged by. Milliseconds, or
        # None when the engine set no limit.
        "heartbeat_timeout_ms": assignment.get("heartbeat_timeout_ms"),
        "start_to_close_timeout_ms": assignment.get("attempt_timeout_ms"),
    }


@dataclass
class _WorkflowRun:
    """What running a workflow's code once produced."""

    # The commands it issued, as the workflow context hands them over.
    commands: list[dict[str, Any]]
    # Every step it reached, as its kind and id, in order.
    reached: list[tuple[str, str]]
    query_responses: list[dict[str, Any]]
    update_results: list[dict[str, Any]]
    # What the code raised, when it failed, and its traceback.
    failure: BaseException | None = None
    failure_details: str | None = None


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
    - Starts the worker host, which polls the Orcher server and keeps the
      worker registered
    - Pulls workflow activations, tasks and actor operations from it
    - Executes workflow, task and actor code
    - Hands the results back to it, which reports them to the server

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
        self._query_handlers: dict[str, QueryMetadata] = {}
        self._update_handlers: dict[str, UpdateMetadata] = {}

        self._workflow_interceptor_chain = WorkflowInterceptorChain()
        self._task_interceptor_chain = TaskInterceptorChain()
        for wi in workflow_interceptors or []:
            self._workflow_interceptor_chain.add(wi)
        for ti in task_interceptors or []:
            self._task_interceptor_chain.add(ti)

        # The worker host (native); None until the worker starts.
        self._host: Any | None = None

        # The work pulled from the host and still running.
        self._running: set[asyncio.Task[None]] = set()

        # Pre-built task class instances, for dependency injection.
        self._task_instances: dict[type[Any], Any] = {}

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

    def _get_host(self) -> Any:
        """Get the worker host, raising if the worker has not started."""
        if self._host is None:
            raise RuntimeError("Worker host not started")
        return self._host

    async def run(self) -> None:
        """
        Run the worker until shutdown.

        Starts the worker host and blocks until shutdown is requested and the
        host has stopped. Python owns the event loop and pulls work from the
        host when it has room to run some.

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

            await self._run_pull_loops()

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
        The worker stops claiming work at once, gives the work it is running
        the configured shutdown grace (``shutdown_grace_time_ms``) to finish,
        hands back the workflow activations still unfinished, and asks the
        tasks still running to cancel. A second call waits for the first
        request.

        Args:
            force: If True, cancel the work running now instead of letting it
                finish within the grace.
            timeout_ms: Deprecated and ignored: the shutdown grace is fixed
                when the worker starts, from ``shutdown_grace_time_ms``.
        """
        if timeout_ms is not None:
            warnings.warn(
                "Worker.shutdown(timeout_ms=...) is ignored; the shutdown grace is "
                "WorkerConfig.shutdown_grace_time_ms, fixed when the worker starts",
                DeprecationWarning,
                stacklevel=2,
            )
        if self._state == WorkerState.STOPPED:
            return

        if self._shutdown_requested:
            await self._shutdown_event.wait()
            return

        self._shutdown_requested = True

        if force:
            logger.info("Force shutdown requested")
            self._state = WorkerState.FORCE_SHUTDOWN
        else:
            logger.info(
                f"Graceful shutdown requested (grace: {self._config.shutdown_grace_time_ms}ms)"
            )
            self._state = WorkerState.SHUTTING_DOWN

        self._stats.state = self._state
        self._shutdown_event.set()

    def _host_config(self) -> dict[str, Any]:
        """The worker host's configuration, from this worker's."""
        config = self._config
        return {
            "server_url": config.server_url,
            "task_queue": config.task_queue,
            "namespace": config.namespace,
            "identity": config.identity,
            "api_key": config.api_key,
            "organization_id": config.organization_id,
            "version_id": config.version_id,
            "tls_ca_cert_path": config.tls_ca_cert_path,
            "tls_client_cert_path": config.tls_client_cert_path,
            "tls_client_key_path": config.tls_client_key_path,
            "max_concurrent_workflows": config.max_concurrent_workflow_executions,
            "max_concurrent_tasks": config.max_concurrent_task_executions,
            "max_concurrent_actor_operations": config.max_concurrent_actor_operations,
            "workflow_pollers": config.workflow_poller_count,
            "task_pollers": config.task_poller_count,
            "actor_pollers": config.actor_poller_count,
            "shutdown_grace_ms": config.shutdown_grace_time_ms,
            "sdk_name": SDK_NAME,
            "sdk_version": _package_version(),
        }

    def _registrations(self) -> dict[str, Any]:
        """What the worker runs: its workflow and task types, and every
        operation of its actors with whether it may share its actor."""
        return {
            "workflow_types": list(self._workflow_handlers),
            "task_types": list(self._task_handlers),
            "actor_handlers": [
                {
                    "actor_name": actor_name,
                    "operation": op_name,
                    "shared": op.mode.value == "shared",
                }
                for actor_name, meta in self._actor_handlers.items()
                for op_name, op in meta.operations.items()
            ],
        }

    async def _initialize(self) -> None:
        """Create the task executor and start the worker host."""
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
            # Registers the worker, and its actor handlers with their modes,
            # so the server can tell shared operations from exclusive ones.
            self._host = await native.WorkerHost.start(
                json.dumps(self._host_config()), json.dumps(self._registrations())
            )
            logger.debug("Worker host started")
        else:
            logger.warning("Native module not available, running in mock mode")

        logger.debug("Service initialized")

    async def _run_pull_loops(self) -> None:
        """
        Pull work from the host until shutdown, then drain and stop it.

        One pull loop per kind of work: workflow activations, tasks and actor
        operations. Each takes room to run one more, pulls the next one, runs
        it and hands back its outcome. A kind the worker registered nothing
        for ends its loop at once. Without a host this only waits for
        shutdown.
        """
        host = self._host
        if host is None:
            logger.info("Running in mock mode - waiting for shutdown")
            await self._shutdown_event.wait()
            return

        config = self._config
        pullers = [
            asyncio.create_task(
                self._pull(
                    "workflow activation",
                    asyncio.Semaphore(config.max_concurrent_workflow_executions),
                    host.next_activation,
                    self._handle_activation,
                ),
                name="pull-activations",
            ),
            asyncio.create_task(
                self._pull(
                    "task",
                    asyncio.Semaphore(config.max_concurrent_task_executions),
                    host.next_task,
                    self._handle_task,
                ),
                name="pull-tasks",
            ),
            asyncio.create_task(
                self._pull(
                    "actor operation",
                    asyncio.Semaphore(config.max_concurrent_actor_operations),
                    host.next_actor_operation,
                    self._handle_actor_operation,
                ),
                name="pull-actor-operations",
            ),
        ]
        logger.info("Pulling work from the worker host")
        try:
            await self._shutdown_event.wait()
            logger.info("Shutdown signal received")
            if self._state == WorkerState.FORCE_SHUTDOWN:
                self._cancel_running()
            # The host stops claiming work at once, waits out the grace for
            # the work running here, hands back what is left, and stops; the
            # pulls then end.
            await host.shutdown()
            await asyncio.gather(*pullers, return_exceptions=True)
            # Work still running past the grace was handed back or asked to
            # cancel; nothing takes its outcome any more.
            self._cancel_running()
            await self._wait_for_running()
        except asyncio.CancelledError:
            logger.info("Polling cancelled")
            for puller in pullers:
                puller.cancel()
            self._cancel_running()
            raise

    async def _pull(
        self,
        kind: str,
        room: asyncio.Semaphore,
        pull: Callable[[], Awaitable[str | None]],
        handle: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        """Take room, pull the next piece of work, run it, hand back its
        outcome; until the host has stopped and handed over everything."""
        while True:
            await room.acquire()
            try:
                work = await pull()
            except asyncio.CancelledError:
                room.release()
                raise
            except Exception as e:
                room.release()
                logger.error(f"Pulling the next {kind} failed: {e}")
                await asyncio.sleep(1.0)
                continue
            if work is None:
                room.release()
                logger.debug(f"No more {kind}s: the worker host has stopped")
                return
            running = asyncio.create_task(self._run_in_room(room, handle(json.loads(work))))
            self._running.add(running)
            running.add_done_callback(self._running.discard)

    @staticmethod
    async def _run_in_room(room: asyncio.Semaphore, work: Awaitable[None]) -> None:
        try:
            await work
        finally:
            room.release()

    def _cancel_running(self) -> None:
        for running in list(self._running):
            running.cancel()

    async def _wait_for_running(self) -> None:
        if self._running:
            await asyncio.gather(*list(self._running), return_exceptions=True)

    async def _handle_activation(self, activation: dict[str, Any]) -> None:
        """Run a workflow activation and hand back its answer."""
        run_id = (activation.get("run") or {}).get("run_id", "")
        try:
            self._stats.workflows_in_progress += 1
            answer = await self.run_activation(activation)
            await self._get_host().complete_activation(json.dumps(answer))
            self._stats.workflows_executed += 1
            logger.debug(f"Answered workflow activation: {run_id}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # The host hands an unanswered activation back to the engine.
            logger.exception(f"Workflow activation not answered: {e}")
            self._stats.errors += 1
        finally:
            self._stats.workflows_in_progress -= 1

    async def _handle_task(self, assignment: dict[str, Any]) -> None:
        """
        Run a task and report its result or failure.

        Args:
            assignment: The task, as the worker host hands it over
        """
        request = _task_request(assignment)
        task_id = request["task_id"]
        token = request["task_token"]

        try:
            self._stats.tasks_in_progress += 1

            result = await self._execute_task(request)

            await self._get_host().complete_task(token, json.dumps(result).encode("utf-8"))

            self._stats.tasks_executed += 1
            logger.debug(f"Completed task: {task_id}")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"Task execution failed: {e}")
            self._stats.errors += 1

            # The engine decides whether to retry from the reported error type,
            # and a retry policy lists types by class name, so the exception's
            # class name is sent. A truthy `non_retryable` attribute on the
            # exception stops retries regardless of the policy.
            try:
                await self._get_host().fail_task(
                    token,
                    str(e),
                    type(e).__name__,
                    bool(getattr(e, "non_retryable", False)),
                )
            except Exception as fail_error:
                logger.error(f"Failed to report task failure: {fail_error}")

        finally:
            self._stats.tasks_in_progress -= 1

    async def run_activation(self, activation: dict[str, Any]) -> dict[str, Any]:
        """Run a workflow activation as the kernel protocol carries it.

        ``activation`` is a ``RunActivation`` in the JSON shape the native
        module hands over: the run's history already resolved into the
        outcomes its code observes. The workflow's code runs once against them
        and the answer is a ``RunActivationResult`` in the same JSON shape:
        every durable call the code made, in issue order, those answered from
        history marked satisfied, with the activation's queries and updates
        answered. A workflow that raised answers its failure; an activation
        this SDK cannot process answers a protocol error, which hands it back
        to the engine.
        """
        activation_id = list(activation.get("activation_id") or [])
        try:
            loaded = load_activation(activation, self._config.namespace)
        except ProtocolRefusalError as refusal:
            logger.error(f"Cannot run the workflow activation: {refusal}")
            return protocol_error(activation_id, str(refusal), refusal.kind)

        if loaded.workflow_type not in self._workflow_handlers:
            return answer_activation(
                activation_id,
                failure=workflow_failure(f"Unknown workflow type: {loaded.workflow_type}"),
            )

        run = await self._run_workflow(
            loaded.context,
            loaded.workflow_type,
            loaded.input,
            loaded.queries,
            loaded.updates,
            loaded.journaled_updates,
        )
        if run.failure is not None:
            return answer_activation(
                activation_id,
                failure=workflow_failure(
                    str(run.failure),
                    non_determinism=_workflow_error_type(run.failure) == "NonDeterminism",
                ),
                query_responses=run.query_responses,
                update_results=run.update_results,
            )
        return answer_activation(
            activation_id,
            commands=run.commands,
            reached=run.reached,
            query_responses=run.query_responses,
            update_results=run.update_results,
        )

    async def _run_workflow(
        self,
        ctx: Any,
        workflow_type: str,
        workflow_input: Any,
        queries: list[AskedQuery],
        updates: list[AskedUpdate],
        journaled_updates: list[dict[str, Any]],
    ) -> _WorkflowRun:
        """Run a registered workflow's code once in ``ctx``, already loaded with
        what the run's history holds, and answer the activation's queries and
        updates."""
        metadata = self._workflow_handlers[workflow_type]
        workflow_id = ctx.info.workflow_id
        run_id = ctx.info.run_id

        # The updates already answered run again as the code reaches the point
        # where each came, so the state they set is there on every replay.
        async def replay_update(update: dict[str, Any]) -> None:
            name = update.get("update_name", "")
            try:
                await self._run_update_handler(ctx, name, update.get("args") or [])
            except Exception as e:
                # Its result was answered and journaled when it first ran.
                logger.debug(f"Replayed update '{name}' raised again: {e}")

        ctx._record_journaled_updates(journaled_updates, replay_update)

        ictx = InterceptorContext(
            workflow_id=workflow_id,
            run_id=run_id,
            workflow_type=workflow_type,
        )
        exec_info = ExecutionInfo(input_data=workflow_input)

        await self._workflow_interceptor_chain.notify_enter(ictx, exec_info)

        start_time = datetime.now()

        try:

            async def run_workflow(data: Any) -> Any:
                # The input becomes call arguments as unpack_arguments
                # describes, as for the query and update handlers.
                wf_args, wf_kwargs = unpack_arguments(data)

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

            await ctx._replay_updates(every=True)
            query_responses = self._answer_queries(queries, ctx)
            update_results = await self._answer_updates(updates, ctx)

            # What the workflow issued in this last activation goes out ahead
            # of its completion: an event it sent, a child it started without
            # waiting. Closures it left running are waited for first.
            await ctx._closures_settled()
            # Only None is null: 0, False, "" and empty containers are results.
            result_bytes = json.dumps(result).encode("utf-8") if result is not None else b"null"
            return _WorkflowRun(
                commands=[
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
                reached=ctx._take_reached(),
                query_responses=query_responses,
                update_results=update_results,
            )

        except WorkflowSuspendedError as e:
            # Suspension is not a failure: the workflow is waiting on commands,
            # so interceptors are not notified of an error.
            logger.debug(f"Workflow suspended: {e.reason}")
            # A closure still running beside the step that suspended is waited
            # for, so its result is reported with this activation and it does
            # not run again on the next one.
            await ctx._closures_settled()
            commands = ctx._take_commands()
            reached = ctx._take_reached()

            # Queries and updates are answered even while suspended.
            await ctx._replay_updates(every=True)
            return _WorkflowRun(
                commands=commands,
                reached=reached,
                query_responses=self._answer_queries(queries, ctx),
                update_results=await self._answer_updates(updates, ctx),
            )

        except Exception as e:
            told_and_not_caught = (
                ctx.is_cancel_requested()
                and getattr(e, "code", None) == ErrorCode.WORKFLOW_CANCELLED
            )
            if told_and_not_caught:
                # The cancellation the workflow was told of, not caught: it ends
                # as cancelled, with whatever cleanup it issued before giving up.
                logger.info(f"Workflow {workflow_id} ended as cancelled")
                await ctx._closures_settled()
                exec_info.duration_ms = (datetime.now() - start_time).total_seconds() * 1000
                await self._workflow_interceptor_chain.notify_exit(ictx, exec_info)
                await ctx._replay_updates(every=True)
                query_responses = self._answer_queries(queries, ctx)
                update_results = await self._answer_updates(updates, ctx)
                return _WorkflowRun(
                    commands=[
                        *ctx._take_commands(),
                        {"CancelWorkflowExecution": {"details": None}},
                    ],
                    reached=ctx._take_reached(),
                    query_responses=query_responses,
                    update_results=update_results,
                )
            logger.exception(f"Workflow execution failed: {e}")
            details = traceback.format_exc()
            # The activation fails and its commands are dropped; closures it
            # left running still finish, rather than being abandoned mid-way.
            await ctx._closures_settled()
            reached = ctx._take_reached()

            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            exec_info.error = e
            exec_info.duration_ms = duration_ms
            exec_info.success = False
            await self._workflow_interceptor_chain.notify_error(ictx, exec_info)
            await self._workflow_interceptor_chain.notify_exit(ictx, exec_info)

            # Queries and updates are answered even when the workflow fails.
            await ctx._replay_updates(every=True)
            return _WorkflowRun(
                commands=[],
                reached=reached,
                query_responses=self._answer_queries(queries, ctx),
                update_results=await self._answer_updates(updates, ctx),
                failure=e,
                failure_details=details,
            )

    def _answer_queries(self, queries: list[AskedQuery], ctx: Any) -> list[dict[str, Any]]:
        """Answer the activation's queries.

        Queries are synchronous, read-only handlers that inspect workflow state.
        A handler is looked up first in the GlobalRegistry (populated by the
        @query() decorator), then in WorkflowContext._query_handlers (manual
        registration). A handler error becomes a Failed response.
        """
        query_responses: list[dict[str, Any]] = []

        for query in queries:
            query_id = query.query_id
            query_type = query.query_type

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
                args_data = json.loads(query.args) if query.args is not None else None

                q_args, q_kwargs = unpack_arguments(args_data)
                result = handler(ctx, *q_args, **q_kwargs)

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

    async def _answer_updates(self, updates: list[AskedUpdate], ctx: Any) -> list[dict[str, Any]]:
        """Apply and answer the updates waiting for this activation.

        Updates are handlers, sync or async, that can mutate workflow state and
        return results. A handler is looked up first in the GlobalRegistry
        (populated by the @update() decorator), then in
        WorkflowContext._update_handlers (manual registration). A missing
        handler becomes Rejected; a handler error becomes Failed.
        """
        update_results: list[dict[str, Any]] = []

        for update in updates:
            update_id = update.update_id
            update_name = update.update_type

            if self._update_handler(ctx, update_name) is None:
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
                result = await self._run_update_handler(ctx, update_name, update.args)

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

    def _update_handler(self, ctx: Any, update_name: str) -> Any:
        """The handler of update ``update_name``, or None. The worker's registry
        (the update decorator) takes precedence over the context's own."""
        update_metadata = self._update_handlers.get(update_name)
        if update_metadata is not None:
            return update_metadata.handler
        if hasattr(ctx, "_update_handlers"):
            return ctx._update_handlers.get(update_name)
        return None

    async def _run_update_handler(self, ctx: Any, update_name: str, raw_args: Any) -> Any:
        """Run update ``update_name``'s handler on ``raw_args``, its JSON
        arguments as bytes (empty for none), and return its result. An update
        without a handler does nothing and returns None."""
        handler = self._update_handler(ctx, update_name)
        if handler is None:
            return None
        raw_bytes = bytes(raw_args)
        args_data = json.loads(raw_bytes) if raw_bytes else None
        args, kwargs = unpack_arguments(args_data)
        if asyncio.iscoroutinefunction(handler):
            return await handler(ctx, *args, **kwargs)
        return handler(ctx, *args, **kwargs)

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
        # The host heartbeats the task on its own. The context forwards the
        # task's explicit heartbeats to the host, and a request to stop the
        # task reaches the context's cancellation token.
        host = self._get_host()
        task_token = request.get("task_token", b"")
        ctx = TaskContext(
            info=task_info,
            heartbeat_sender=lambda details: bool(host.heartbeat(task_token, details)),
        )
        cancellation_watch = asyncio.ensure_future(
            _watch_task_cancellation(host, task_token, ctx.cancellation_token)
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
            # The input becomes call arguments as a workflow's does (see
            # unpack_arguments): an object is spread as keyword arguments, and
            # any other value, a list included, is the one argument. Spreading
            # only objects failed every task given a list or a scalar.
            task_args, task_kwargs = unpack_arguments(data)
            if asyncio.iscoroutinefunction(handler):
                return await handler(ctx, *task_args, **task_kwargs)
            # Sync handlers run in the executor so blocking or CPU-bound
            # work does not stall the event loop.
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self._task_executor,
                lambda: handler(ctx, *task_args, **task_kwargs),
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

    async def _handle_actor_operation(self, request: dict[str, Any]) -> None:
        """Run an actor operation and report its result or failure.

        Args:
            request: The actor operation, as the worker host hands it over
        """
        operation_id = request.get("operation_id", "")
        execution_id = request.get("execution_id", "")

        try:
            result = await self._execute_actor_operation(request)

            await self._get_host().complete_actor_operation(
                operation_id, execution_id, json.dumps(result).encode("utf-8")
            )

            logger.debug(f"Completed actor operation: {operation_id}")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"Actor operation failed: {e}")
            self._stats.errors += 1

            try:
                await self._get_host().fail_actor_operation(
                    operation_id, execution_id, str(e), type(e).__name__
                )
            except Exception as fail_error:
                logger.error(f"Failed to report actor failure: {fail_error}")

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

        # The operation's argument as its JSON encoding, the bytes as a list
        # of integers; none is no arguments.
        payload_bytes = bytes(request.get("payload") or [])
        operation_input: Any = json.loads(payload_bytes) if payload_bytes else {}

        actor_key = ActorKey(actor_name=actor_name, key=key)

        # State is read and written through the worker host when present.
        if self._host is not None:
            state_client = ActorStateClient(
                server_url=self._config.server_url,
                bridge=self._host,
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

    async def _cleanup(self) -> None:
        """Release the owned executor and stop and drop the worker host."""
        logger.debug("Cleaning up service resources...")

        # A caller-provided executor is left for the caller to shut down.
        if self._owns_executor and self._task_executor is not None:
            self._task_executor.shutdown(wait=False)
            self._task_executor = None

        # The host is normally stopped already, when the pull loops wind
        # down. This covers paths that skip that, such as a cancelled run: a
        # host dropped without stopping would leave the work it holds claimed
        # on the engine until the claim timed out.
        if self._host is not None:
            try:
                await self._host.shutdown()
            except Exception:
                logger.warning("Failed to stop the worker host cleanly", exc_info=True)
        self._cancel_running()

        self._host = None

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
                timeout = (self._config.shutdown_grace_time_ms / 1000.0) + SHUTDOWN_MARGIN_SECS
                await asyncio.wait_for(self._run_task, timeout=timeout)
            except asyncio.CancelledError:
                pass
            except TimeoutError:
                self._run_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._run_task
