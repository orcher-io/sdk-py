"""Workflow execution context for the Orcher Python SDK.

This module provides the WorkflowContext class that is passed to workflow
functions, providing access to deterministic operations and task execution.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import inspect
import json
import math
from collections.abc import Callable, Coroutine
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    get_args,
    get_origin,
)

from orcher.types import RetryPolicy, WorkflowExecution
from orcher.workflow.child import ChildWorkflowHandle
from orcher.workflow.info import WorkflowInfo
from orcher.workflow.random import WorkflowRandom
from orcher.workflow.sentinels import CHILD_FAILED_SENTINEL, TASK_FAILED_SENTINEL
from orcher.workflow.time import WorkflowTime

if TYPE_CHECKING:
    from orcher.workflow.session import SessionContext, SessionOptions

__all__ = ["WorkflowContext"]

T = TypeVar("T")


def _retry_policy_to_dict(rp: RetryPolicy) -> dict:
    """Convert a RetryPolicy dataclass to the serde-compatible dict expected by sdk-core."""
    return {
        "max_attempts": rp.max_attempts,
        "initial_interval": {
            "secs": int(rp.initial_interval.total_seconds()),
            "nanos": 0,
        },
        "max_interval": {
            "secs": int(rp.max_interval.total_seconds()),
            "nanos": 0,
        },
        "backoff_coefficient": rp.backoff_coefficient,
        "non_retryable_errors": list(rp.non_retryable_error_types),
    }


def _duration_parts(td: timedelta) -> dict:
    """Encode a duration as the ``{secs, nanos}`` shape sdk-core deserializes.

    Keeps sub-second precision, so a timeout shorter than one second is sent
    as that duration rather than truncated to zero.
    """
    total = td.total_seconds()
    secs = int(total)
    nanos = int(round((total - secs) * 1_000_000_000))
    # Rounding can carry into the next second; keep nanos in range.
    if nanos >= 1_000_000_000:
        secs += 1
        nanos -= 1_000_000_000
    return {"secs": secs, "nanos": nanos}


def _normalize_retry_policy(rp: Any) -> dict | None:
    """Convert a retry policy into the shape sdk-core expects.

    Accepts a ``RetryPolicy`` dataclass or the plain dict form used by
    ``@task(retry_policy=...)``. The dict accepts ``max_attempts`` plus optional
    ``initial_interval_seconds``, ``max_interval_seconds``, ``backoff_coefficient``
    and ``non_retryable_errors``; missing keys fall back to defaults. Any other
    value yields ``None`` (the engine default applies).
    """
    if rp is None:
        return None
    if isinstance(rp, RetryPolicy):
        return _retry_policy_to_dict(rp)
    if isinstance(rp, dict):
        return {
            "max_attempts": int(rp.get("max_attempts", 3)),
            "initial_interval": {
                "secs": int(rp.get("initial_interval_seconds", 1)),
                "nanos": 0,
            },
            "max_interval": {
                "secs": int(rp.get("max_interval_seconds", 60)),
                "nanos": 0,
            },
            "backoff_coefficient": float(rp.get("backoff_coefficient", 2.0)),
            "non_retryable_errors": list(rp.get("non_retryable_errors", [])),
        }
    return None


def _random_seed(workflow_id: str, run_id: str) -> int:
    """The seed of a run's ``ctx.random``: the same in every process."""
    digest = hashlib.sha256(f"{workflow_id}\x00{run_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


# StepType.STEP_TYPE_CLOSURE on the wire.
_STEP_TYPE_CLOSURE = 2

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class WorkflowContext:
    """Execution context for workflow code.

    WorkflowContext is passed as the first argument to workflow functions.
    It provides:

    - Task execution via `execute_task()` - for registered, distributed tasks
    - Inline execution via `execute()` - for one-off deterministic operations
    - Durable timers via `sleep()`
    - Child workflow management
    - Event handling
    - Query handler registration
    - Deterministic random and time

    IMPORTANT: Every operation on WorkflowContext is deterministic and replays
    correctly. Do not use standard library functions for:
    - Random numbers (use `ctx.random`)
    - Current time (use `ctx.time`)
    - I/O operations (use `execute()` or `execute_task()`)

    Example:
        >>> @workflow(name="order-workflow", version="1.0")
        ... async def order_workflow(ctx: WorkflowContext, order_id: str) -> dict:
        ...     # Execute a registered task (distributed)
        ...     charge = await ctx.execute_task(charge_card, amount=100, token="tok_xxx")
        ...
        ...     # Execute an inline operation (deterministic one-off)
        ...     user = await ctx.execute("fetch_user", lambda: fetch_user_from_api(order_id))
        ...
        ...     # Sleep (durable timer)
        ...     await ctx.sleep(timedelta(hours=1))
        ...
        ...     # Use deterministic random
        ...     if ctx.random.random() > 0.5:
        ...         ...
        ...
        ...     return {"order_id": order_id, "charge": charge}
    """

    def __init__(
        self,
        info: WorkflowInfo,
        *,
        replaying: bool = False,
    ) -> None:
        """Initialize the workflow context.

        Args:
            info: Workflow execution information.
            replaying: Whether currently replaying from journal.
        """
        self._info = info
        self._replaying = replaying

        # Seeded from a digest of the run's ids, the same in every process.
        # `hash()` of a str is randomized per process, so a replay on another
        # worker would draw a different sequence.
        self._random = WorkflowRandom(_random_seed(info.workflow_id, info.run_id))

        # Workflow time starts at the execution's start time, not the wall clock.
        self._time = WorkflowTime(info.started_at)

        self._query_handlers: dict[str, Callable[..., Any]] = {}

        self._update_handlers: dict[str, Callable[..., Any]] = {}

        # Key-value state, persisted across replays.
        self._workflow_state: dict[str, Any] = {}

        # Commands produced by this activation, handed to the native bridge.
        self._commands: list[dict[str, Any]] = []

        # The id of every step (task, timer, child workflow) the code reached
        # this activation, in order, whether the journal already held its
        # outcome or its command is issued now. sdk-core checks them against
        # the steps the journal recorded to tell code that no longer replays
        # the run.
        self._reached_steps: list[str] = []

        # The one step counter. Task, timer, child, closure and session ids
        # are derived from it, and each step takes exactly one number whether
        # it is issued or read back from the journal, so an id depends only on
        # the step's place in the code. Waits for events are not engine steps
        # and take none.
        self._step_sequence: int = 0

        # When the engine journaled each result this activation can hand the
        # workflow, in milliseconds since the epoch, keyed as the results are:
        # a step's id, `timer:{id}`, `child:{workflow id}`. Receiving one
        # moves the workflow's clock there.
        self._resolved_at: dict[str, int] = {}

        # When each buffered event was journaled, per name, oldest first.
        self._event_times: dict[str, list[int]] = {}

        # Deadline timers of timed event waits, counted per event name.
        self._event_timeout_waits: dict[str, int] = {}

        # Closures started in this activation that have not settled yet.
        self._running_closures: set[asyncio.Future[Any]] = set()

        # Cached results from previous execution (for replay)
        self._cached_results: dict[str, Any] = {}

        # Set by SessionContext for a single execute_task call, then cleared.
        self._task_queue_override: str | None = None

    @property
    def info(self) -> WorkflowInfo:
        """Get workflow execution information."""
        return self._info

    @property
    def version(self) -> str:
        """The workflow's version string, a metadata label only.

        This is a plain identifier used for metrics and inventory. It is not a
        replay-safety mechanism: the engine does not gate or route replay by
        version. To change workflow logic safely, run the new code under a new
        workflow name side by side, or reset the execution. Do not branch on
        this value during replay.
        """
        return self._info.version

    @property
    def execution(self) -> WorkflowExecution:
        """Get the workflow execution identifier."""
        return WorkflowExecution(
            workflow_id=self._info.workflow_id,
            run_id=self._info.run_id,
        )

    @property
    def replaying(self) -> bool:
        """Whether the context is replaying from the execution journal.

        During replay, side effects must not run again; their results are
        already recorded in the journal.
        """
        return self._replaying

    @property
    def random(self) -> WorkflowRandom:
        """Get the deterministic random number generator.

        Use this instead of Python's `random` module to ensure
        deterministic replay.
        """
        return self._random

    @property
    def time(self) -> WorkflowTime:
        """Get the deterministic time provider.

        Use this instead of `datetime.now()` to ensure deterministic replay.
        """
        return self._time

    def _next_sequence(self) -> int:
        """Take the next number from the step counter."""
        self._step_sequence += 1
        return self._step_sequence

    def _reach(self, step_id: str) -> None:
        """The code reached the step with this id; see ``_take_reached_steps``."""
        self._reached_steps.append(step_id)

    def _take_reached_steps(self) -> list[str]:
        """Take the ids of the steps the code reached this activation (internal).

        sdk-core compares them with the steps the journal recorded: a recorded
        step left unreached by an activation that issues new work or ends the
        workflow is reported as non-determinism, and the activation is tried
        again instead of applied.
        """
        reached, self._reached_steps = self._reached_steps, []
        return reached

    def _observe(self, key: str) -> None:
        """The workflow received what was journaled under ``key``: its clock
        moves to when that was journaled."""
        at_ms = self._resolved_at.get(key)
        if at_ms is not None:
            self._time._advance_to_ms(at_ms)

    async def execute_task(
        self,
        task_fn: Callable[..., Any],
        *,
        retry_policy: RetryPolicy | None = None,
        timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        queue_timeout: timedelta | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a registered task and return its result.

        Schedules the task for execution by a task worker. The task must be
        registered with ``@task`` (or be a method of a ``@tasks`` class). Its
        result is recorded in the execution journal, so on replay the recorded
        result is returned without running the task again.

        The retry policy, timeout and heartbeat timeout are each resolved in
        the same order: the argument passed here, then the value declared on
        ``@task``, then the default.

        Args:
            task_fn: The task function (decorated with @task).
            retry_policy: Optional override for the task's retry policy.
            timeout: Optional override for the task's timeout.
            heartbeat_timeout: Optional override for heartbeat timeout.
            queue_timeout: How long the task may wait in its queue before a
                worker starts it. None means it waits as long as it needs to;
                ``timeout`` covers only the run itself.
            **kwargs: Input arguments to pass to the task.

        Returns:
            The task result.

        Raises:
            TaskError: If the task fails after all retries.
            ValueError: If the function is not a registered task.

        Example:
            >>> # Execute a registered task
            >>> charge = await ctx.execute_task(
            ...     charge_card,
            ...     amount=100,
            ...     token="tok_xxx",
            ...     order_id="order_123"
            ... )
        """
        # Resolve the task, however it was defined.
        #
        # A function decorated with @task carries its metadata directly. A
        # class-based task arrives as a TaskReference, because @tasks replaces
        # the method with one. A TaskReference has no metadata attribute and
        # no __name__; its metadata, including the declared retry policy and
        # limits, lives in the global registry under the task name.
        task_metadata = getattr(task_fn, "__orcher_task__", None)
        if task_metadata is not None:
            task_name = task_metadata.name
        else:
            task_name = getattr(task_fn, "task_name", None) or getattr(
                task_fn, "__orcher_task_name__", None
            )
            if task_name is None:
                described = getattr(task_fn, "__name__", None) or repr(task_fn)
                raise ValueError(
                    f"'{described}' is not a registered task. "
                    f"Make sure it is decorated with @task(name=...)"
                )
            from orcher.decorators.registry import GlobalRegistry

            task_metadata = GlobalRegistry.get_instance().get_task(task_name)

        # One number from the step counter on every path, cached or not, so
        # the ids of the steps after this one do not depend on whether it had
        # already completed. The task name prefix makes the journal readable.
        sequence = self._next_sequence()
        task_id = f"{task_name}_{sequence}"
        self._reach(task_id)

        # A journaled result means the task already ran: return it instead of
        # scheduling the task again.
        cache_key = f"task:{task_id}"
        if cache_key in self._cached_results:
            self._observe(task_id)
            cached = self._cached_results[cache_key]
            # A terminal failure arrives as a sentinel: raise it.
            if isinstance(cached, dict) and cached.get(TASK_FAILED_SENTINEL):
                from orcher.errors import TaskError

                raise TaskError.execution_failed(
                    task_id=task_id,
                    message=cached.get("message", "Task failed"),
                    task_type=task_name,
                    attempts=cached.get("attempts", 1),
                )
            return_type = self._get_task_return_type(task_fn)
            if (
                return_type is not None
                and dataclasses.is_dataclass(return_type)
                and isinstance(cached, dict)
            ):
                cached = self._coerce_to_dataclass(cached, return_type)
            return cached

        # Task input is sent as JSON bytes inside a Payload.
        input_bytes = json.dumps(kwargs).encode("utf-8") if kwargs else b"{}"
        input_payload = {
            "data": list(input_bytes),  # Vec<u8> as list of ints
            "metadata": {"encoding": list(b"json")},
        }

        # Retry policy: a per-call override wins, otherwise the policy declared
        # with @task(retry_policy=...). With neither, the engine default applies.
        effective_retry_policy: Any = retry_policy
        if effective_retry_policy is None and task_metadata is not None:
            effective_retry_policy = getattr(task_metadata, "retry_policy", None)

        # Same precedence for the limits: this call, then what @task declared,
        # then the default (300s for the timeout, none for the heartbeat).
        # Metadata carries plain seconds; the arguments carry timedeltas.
        effective_timeout = timeout
        if effective_timeout is None and task_metadata is not None:
            declared = getattr(task_metadata, "timeout", None)
            if declared is not None:
                effective_timeout = timedelta(seconds=declared)

        effective_heartbeat_timeout = heartbeat_timeout
        if effective_heartbeat_timeout is None and task_metadata is not None:
            declared_hb = getattr(task_metadata, "heartbeat_timeout", None)
            if declared_hb is not None:
                effective_heartbeat_timeout = timedelta(seconds=declared_hb)

        # Field names and shape must match sdk-core's ScheduleTaskCommand.
        schedule_cmd = {
            "sequence": sequence,
            "task_id": task_id,
            "task_type": task_name,
            "task_queue": self._task_queue_override or self._info.task_queue,
            "input": [input_payload],  # Vec<Payload>
            "timeout": (
                _duration_parts(effective_timeout)
                if effective_timeout
                else {"secs": 300, "nanos": 0}
            ),
            # The engine enforces the heartbeat timeout. Null means the task is
            # not supervised by heartbeats.
            "heartbeat_timeout": (
                _duration_parts(effective_heartbeat_timeout)
                if effective_heartbeat_timeout
                else None
            ),
            # Independent of `timeout`: this bounds only the wait in the queue.
            "queue_timeout": _duration_parts(queue_timeout) if queue_timeout else None,
            "retry_policy": _normalize_retry_policy(effective_retry_policy),
            "headers": [],
        }

        self._commands.append({"ScheduleTask": schedule_cmd})

        # Suspend. The native layer schedules the task, and the workflow is
        # replayed once the task completes.
        from orcher.errors import WorkflowSuspendedError

        raise WorkflowSuspendedError(f"Waiting for task: {task_name}")

    async def execute(
        self,
        step_name: str,
        closure: Callable[[], Coroutine[Any, Any, T] | T],
    ) -> T:
        """Run an inline closure once and journal its result.

        The closure runs in the workflow worker and its result is recorded in
        the execution journal. On replay, the recorded result is returned and
        the closure does not run again.

        Use this for:
        - One-off HTTP API calls
        - Database queries
        - External service calls
        - Any side effect whose result must stay the same on replay

        The closure takes no arguments. It is a leaf operation and cannot call
        other workflow operations. Its result must be JSON-serializable: it is
        journaled as JSON, and a replay returns the decoded value.

        The step's id is ``step_name`` and a number from the workflow's step
        counter, so running the same name several times (in a loop, say)
        journals each run separately. A closure still running when another
        step suspends the workflow is waited for, and its result is journaled
        with that activation instead of the closure running again.

        Args:
            step_name: Name for this step (used in the journal key).
            closure: Function to execute, async or not. Takes no arguments.

        Returns:
            The closure result.

        Raises:
            Exception: Any exception raised by the closure. A failed closure
                is not journaled, so a later activation runs it again.

        Example:
            >>> # Execute an inline HTTP call
            >>> user = await ctx.execute("fetch_user", lambda: fetch_user_api(user_id))
            >>>
            >>> # Execute an inline database query
            >>> orders = await ctx.execute("get_orders", lambda: db.query_orders(customer_id))
            >>>
            >>> # Execute an inline calculation
            >>> result = await ctx.execute("calculate_total", lambda: calculate_order_total(items))
        """
        if not step_name:
            raise ValueError("execute() requires a non-empty step name")

        # One number from the step counter on every path, cached or not, like
        # every other step.
        sequence = self._next_sequence()
        step_id = f"{step_name}_{sequence}"

        # A journaled result means the closure already ran. The journal hands
        # closure results back as completed steps, like task results. They do
        # not move the workflow's clock: the closure ran before its result
        # was journaled.
        cache_key = f"task:{step_id}"
        if cache_key in self._cached_results:
            cached = self._cached_results[cache_key]
            if isinstance(cached, dict) and cached.get(TASK_FAILED_SENTINEL):
                from orcher.errors import TaskError

                raise TaskError.execution_failed(
                    task_id=step_id,
                    message=cached.get("message", "Step failed"),
                    task_type=step_name,
                    attempts=cached.get("attempts", 1),
                )
            return cached  # type: ignore[no-any-return]

        # The run is its own asyncio task, held until it settles: a closure
        # beside a step that suspends (in an asyncio.gather, say) is still
        # running when the workflow suspends, and the worker waits for it so
        # its result goes out with this activation.
        run = asyncio.ensure_future(self._run_closure(step_name, step_id, closure))
        self._running_closures.add(run)
        run.add_done_callback(self._running_closures.discard)
        return await run

    async def _run_closure(
        self,
        step_name: str,
        step_id: str,
        closure: Callable[[], Any],
    ) -> Any:
        result = closure()
        if inspect.isawaitable(result):
            result = await result
        try:
            encoded = json.dumps(result).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise TypeError(
                f"ctx.execute('{step_name}') returned a value that cannot be journaled "
                f"as JSON: {e}"
            ) from e
        # The shape of sdk-core's RecordStepResultCommand: the result as the
        # bytes of its JSON encoding, the step type as its wire number, and
        # the attempt that produced it.
        self._commands.append(
            {
                "RecordStepResult": {
                    "step_name": step_id,
                    "step_type": _STEP_TYPE_CLOSURE,
                    "result": list(encoded),
                    "failure": None,
                    "execution_attempt": 1,
                }
            }
        )
        self._cached_results[f"task:{step_id}"] = result
        return result

    async def _closures_settled(self) -> None:
        """Wait until every closure started in this activation has settled, so
        the results of those that succeeded are among the commands taken next."""
        while self._running_closures:
            await asyncio.gather(*list(self._running_closures), return_exceptions=True)

    # ========================================================================
    # Session Management
    # ========================================================================

    async def create_session(
        self,
        options: SessionOptions | None = None,
    ) -> SessionContext:
        """Create a worker session that pins subsequent tasks to a single worker.

        Schedules an internal ``__orcher_create_session`` task on the
        workflow's normal queue. A worker picks it up, acquires a session slot,
        and returns its unique queue name. The returned :class:`SessionContext`
        routes all tasks to that worker-specific queue.

        Replay Safety:
            Task IDs are derived from the step sequence. On replay, the session
            queue name comes from the journal and the session is not recreated.

        Example::

            from orcher.workflow.session import SessionOptions

            session = await ctx.create_session(SessionOptions(
                creation_timeout=timedelta(seconds=30),
                execution_timeout=timedelta(seconds=600),
            ))

            model = await session.execute_task(load_model, name="bert")
            result = await session.execute_task(run_inference, data=input_data)
            session.complete()
        """
        from orcher.workflow.session import (
            SESSION_CREATE_TASK,
            CreateSessionInput,
            SessionContext,
            SessionInfo,
            SessionOptions,
            SessionState,
        )

        if options is None:
            options = SessionOptions()

        # One number from the step counter on every path, like every other step.
        sequence = self._next_sequence()
        session_id = f"session_{sequence}"
        task_id = f"{SESSION_CREATE_TASK}_{sequence}"
        self._reach(task_id)
        cache_key = f"task:{task_id}"

        # On replay, the creation task's journaled result is a serialized SessionInfo.
        if cache_key in self._cached_results:
            self._observe(task_id)
            cached = self._cached_results[cache_key]
            info = SessionInfo(
                session_id=cached.get("session_id", session_id),
                session_queue=cached["session_queue"],
                worker_identity=cached.get("worker_identity", ""),
                state=SessionState.OPEN,
            )
            return SessionContext(self, info)

        # No cached result — schedule the session creation task
        input_data = CreateSessionInput(
            session_id=session_id,
            creation_timeout_ms=int(options.creation_timeout.total_seconds() * 1000),
            execution_timeout_ms=int(options.execution_timeout.total_seconds() * 1000),
            max_concurrent_tasks=options.max_concurrent_tasks,
            heartbeat_interval_ms=int(options.heartbeat_interval.total_seconds() * 1000),
        )

        import json as _json

        input_bytes = _json.dumps({
            "session_id": input_data.session_id,
            "creation_timeout_ms": input_data.creation_timeout_ms,
            "execution_timeout_ms": input_data.execution_timeout_ms,
            "max_concurrent_tasks": input_data.max_concurrent_tasks,
            "heartbeat_interval_ms": input_data.heartbeat_interval_ms,
        }).encode("utf-8")

        input_payload = {
            "data": list(input_bytes),
            "metadata": {"encoding": list(b"json")},
        }

        schedule_cmd = {
            "sequence": sequence,
            "task_id": task_id,
            "task_type": SESSION_CREATE_TASK,
            "task_queue": self._info.task_queue,
            "input": [input_payload],
            "timeout": {"secs": int(options.creation_timeout.total_seconds()), "nanos": 0},
            "retry_policy": None,
            "headers": [],
        }

        self._commands.append({"ScheduleTask": schedule_cmd})

        from orcher.errors import WorkflowSuspendedError

        raise WorkflowSuspendedError(f"Creating session '{session_id}'")

    async def sleep(self, duration: timedelta) -> None:
        """Sleep for the specified duration (durable timer).

        The timer is recorded in the execution journal and survives workflow
        restarts.

        Args:
            duration: How long to sleep, as a ``timedelta``. A bare number is
                not accepted because its unit would be ambiguous.

        Example:
            >>> await ctx.sleep(timedelta(hours=1))
            >>> await ctx.sleep(timedelta(seconds=30))
        """
        # One number from the step counter on every path.
        sequence = self._next_sequence()
        timer_id = f"timer_{sequence}"
        self._reach(timer_id)

        # A fired timer is in the journal: the sleep is over.
        cache_key = f"timer:{timer_id}"
        if cache_key in self._cached_results:
            self._observe(cache_key)
            return

        # Field names and shape must match sdk-core's StartTimerCommand
        # (sequence, timer_id, and duration as {secs, nanos}), or the native
        # bridge rejects the whole command batch.
        total = duration.total_seconds()
        secs = int(total)
        nanos = int(round((total - secs) * 1_000_000_000))
        self._commands.append(
            {
                "StartTimer": {
                    "sequence": sequence,
                    "timer_id": timer_id,
                    "duration": {"secs": secs, "nanos": nanos},
                }
            }
        )

        from orcher.errors import WorkflowSuspendedError

        raise WorkflowSuspendedError(f"Waiting for timer: {timer_id}")

    async def execute_child_workflow(
        self,
        workflow_type: str,
        *,
        workflow_id: str | None = None,
        args: tuple[Any, ...] = (),
        task_queue: str | None = None,
    ) -> Any:
        """Start a child workflow and wait for its result.

        Args:
            workflow_type: Type/name of the child workflow.
            workflow_id: Optional ID for the child workflow. When omitted, a
                deterministic ``child_{sequence}`` id is derived from the step
                sequence, as in the Rust and TypeScript SDKs, so callers need
                not guarantee uniqueness within the parent. The engine echoes
                this id back on completion for correlation. Supply one only to
                address the child later (events, cancel) by a known id.
            args: Arguments to pass to the child workflow.
            task_queue: Optional task queue (defaults to parent's queue).

        Returns:
            The value the child workflow returned, as in the Rust and
            TypeScript SDKs. The child's failure is raised as a
            ``WorkflowError``. To hold a handle and interact with a running
            child (events, cancel), use ``start_child_workflow`` instead.
        """
        # One number from the step counter on every path. A child given no id
        # is named after it, so the id is the same on every replay; the engine
        # echoes it back with the child's outcome.
        sequence = self._next_sequence()
        if workflow_id is None:
            workflow_id = f"child_{sequence}"
        self._reach(workflow_id)

        # On replay the worker has injected the child's outcome under
        # child:{id}: return its result, or raise its failure.
        if f"child:{workflow_id}" in self._cached_results:
            return self._resolve_child(workflow_id)

        # The child's input is a JSON Payload, as for ScheduleTask. A single
        # positional arg is sent as the value itself (the child worker passes a
        # non-dict value as a positional parameter); several are sent as a list.
        child_input: Any = args[0] if len(args) == 1 else list(args)
        input_bytes = json.dumps(child_input).encode("utf-8")
        input_payload = {
            "data": list(input_bytes),
            "metadata": {"encoding": list(b"json")},
        }

        # Field names and shape must match sdk-core's StartChildWorkflowCommand
        # (sequence, workflow_id, workflow_type, task_queue, input, timeout,
        # orphan_policy), or the native bridge rejects the whole command batch.
        self._commands.append(
            {
                "StartChildWorkflow": {
                    "sequence": sequence,
                    "workflow_id": workflow_id,
                    "workflow_type": workflow_type,
                    "task_queue": task_queue or self._info.task_queue,
                    "input": [input_payload],
                    "timeout": None,
                    "orphan_policy": "Cancel",
                }
            }
        )

        from orcher.errors import WorkflowSuspendedError

        raise WorkflowSuspendedError(f"Starting child workflow: {workflow_type}")

    def _resolve_child(self, workflow_id: str) -> Any:
        """A child's result from the journal: returned, raised if the child
        failed or ended without one, or the workflow suspends until it ends."""
        cache_key = f"child:{workflow_id}"
        if cache_key not in self._cached_results:
            from orcher.errors import WorkflowSuspendedError

            raise WorkflowSuspendedError(f"Waiting for child workflow: {workflow_id}")
        self._observe(cache_key)
        cached = self._cached_results[cache_key]
        if isinstance(cached, dict) and cached.get(CHILD_FAILED_SENTINEL):
            from orcher.errors import WorkflowError

            raise WorkflowError.execution_failed(
                workflow_id=workflow_id,
                message=cached.get("message", "Child workflow failed"),
            )
        return cached

    async def wait_for_event(self, event_name: str) -> Any:
        """Wait indefinitely for an event from an external source.

        For a bounded wait, use :meth:`wait_for_event_with_timeout`. The two
        methods match the Rust and TypeScript SDKs; this one takes no timeout.

        Args:
            event_name: Name of the event to wait for.

        Returns:
            The event payload.
        """
        return await self._wait_for_event(event_name, None)

    async def wait_for_event_with_timeout(
        self,
        event_name: str,
        timeout: timedelta,
    ) -> Any:
        """Wait for an event up to ``timeout``.

        Args:
            event_name: Name of the event to wait for.
            timeout: Maximum time to wait (a ``timedelta``).

        Returns:
            The event payload.

        Raises:
            TimeoutError: If the timeout expires before the event is received.
                The deadline is a durable timer started beside the wait, so it
                holds while no worker is running the workflow. An event that
                arrives after the deadline is left for the next wait.
        """
        if timeout.total_seconds() <= 0:
            raise ValueError("wait_for_event_with_timeout: timeout must be greater than zero")
        return await self._wait_for_event(event_name, timeout)

    async def _wait_for_event(
        self,
        event_name: str,
        timeout: timedelta | None,
    ) -> Any:
        # A wait is not an engine step (events are matched to waits by name),
        # so it takes no number from the step counter on any path. If parking
        # took one and finding the event did not, a step issued beside the
        # wait would get another id once the event had arrived.
        #
        # A timed wait races a durable deadline timer against the event. The
        # timer is named after the event and how many timed waits for it came
        # before, taken on every path, so a replay names it as the live run did.
        timer_id = self._event_timeout_timer_id(event_name) if timeout else None
        # Reached on every path, whichever of the event and the deadline wins:
        # the journal holds this timer whenever an earlier activation parked.
        if timer_id is not None:
            self._reach(timer_id)
        fired = self._cached_results.get(f"timer:{timer_id}") if timer_id else None
        timer_fired = timer_id is not None and f"timer:{timer_id}" in self._cached_results
        # A fired marker without a journal position is treated as later than
        # any event, so a buffered event still wins.
        fired_at = (
            fired.get("fired_at", math.inf)
            if isinstance(fired, dict)
            else math.inf
        )

        # Buffered events from the journal are keyed by event_name, not step_id,
        # and consumed oldest first. Whichever the journal shows first, event or
        # deadline, decides. An event that landed after the deadline stays for
        # the next wait, because an earlier activation already took the timeout
        # branch.
        buffer_key = f"event_buffer:{event_name}"
        positions = self._cached_results.get(f"event_positions:{event_name}") or []
        buffer = self._cached_results.get(buffer_key)
        if buffer:
            event_at = positions[0] if positions else -math.inf
            if not timer_fired or event_at < fired_at:
                data = buffer.pop(0)
                if positions:
                    positions.pop(0)
                # The workflow's clock moves to when the event was journaled.
                times = self._event_times.get(event_name)
                if times:
                    self._time._advance_to_ms(times.pop(0))
                if isinstance(data, list):
                    data = bytes(data)
                if data:
                    try:
                        return json.loads(data)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        return None
                return None

        if timer_fired:
            self._observe(f"timer:{timer_id}")
            raise TimeoutError(f"Timed out waiting for event: {event_name}")

        self._commands.append(
            {
                "WaitForEvent": {
                    "step_id": f"event_{event_name}",
                    "event_name": event_name,
                    "timeout_ms": int(timeout.total_seconds() * 1000) if timeout else None,
                }
            }
        )
        if timeout:
            # The deadline timer is not cancelled if the event wins. For a
            # finished workflow the engine marks the timer done without
            # journaling it; for an unfinished one it costs a single extra
            # replay.
            total = timeout.total_seconds()
            secs = int(total)
            nanos = int(round((total - secs) * 1_000_000_000))
            # The sequence only labels the command; the counter is not
            # advanced (see above).
            self._commands.append(
                {
                    "StartTimer": {
                        "sequence": self._step_sequence,
                        "timer_id": timer_id,
                        "duration": {"secs": secs, "nanos": nanos},
                    }
                }
            )

        from orcher.errors import WorkflowSuspendedError

        raise WorkflowSuspendedError(f"Waiting for event: {event_name}")

    def _event_timeout_timer_id(self, event_name: str) -> str:
        n = self._event_timeout_waits.get(event_name, 0) + 1
        self._event_timeout_waits[event_name] = n
        return f"event_timeout_{event_name}_{n}"

    def register_query_handler(
        self,
        name: str,
        handler: Callable[..., Any],
    ) -> None:
        """Register a query handler.

        Query handlers can be invoked by external clients to inspect
        workflow state without affecting the execution.

        Args:
            name: Name of the query.
            handler: Function to handle the query. Can be sync or async.

        Example:
            >>> def get_status() -> str:
            ...     return current_status
            >>> ctx.register_query_handler("get-status", get_status)
        """
        self._query_handlers[name] = handler

    def register_update_handler(
        self,
        name: str,
        handler: Callable[..., Any],
    ) -> None:
        """Register an update handler.

        Update handlers can be invoked by external clients to send
        synchronous mutations to running workflows and receive results.

        Args:
            name: Name of the update.
            handler: Function to handle the update. Can be sync or async.

        Example:
            >>> async def change_address(ctx, new_address: str) -> dict:
            ...     ctx.set_state("address", new_address)
            ...     return {"success": True}
            >>> ctx.register_update_handler("change-address", change_address)
        """
        self._update_handlers[name] = handler

    async def cancel_child(self, workflow_id: str) -> None:
        """Cancel a child workflow this workflow started.

        The child is canceled the way a client cancels a workflow, and waiting
        for its result then raises ``WorkflowError``. The request is journaled
        as a step of this workflow, so a replay does not repeat it; canceling
        a child that has already finished does nothing. It does not suspend
        the workflow.

        Args:
            workflow_id: ID of the child workflow to cancel.
        """

        async def request() -> None:
            # The engine identifies the child by the id its parent gave it;
            # the run id is left empty because the child's run changes when it
            # is retried.
            self._commands.append(
                {"CancelChildWorkflow": {"workflow_id": workflow_id, "run_id": ""}}
            )

        await self.execute(f"cancel_child:{workflow_id}", request)

    # ── Workflow State ────────────────────────────────────────────────

    def set_state(self, key: str, value: Any) -> None:
        """Persist a key-value pair in workflow state.

        State is preserved across workflow replays and can be
        inspected via queries.

        Args:
            key: State key.
            value: JSON-serializable value.

        Example:
            >>> ctx.set_state("order_status", "processing")
            >>> ctx.set_state("items_processed", 42)
        """
        self._workflow_state[key] = value

    def get_state(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from workflow state.

        Args:
            key: State key.
            default: Value to return if key is not found.

        Returns:
            The stored value, or *default* if not found.

        Example:
            >>> status = ctx.get_state("order_status", "unknown")
        """
        return self._workflow_state.get(key, default)

    # ── Named Timers ─────────────────────────────────────────────────

    async def sleep_with_id(
        self,
        timer_id: str,
        duration: timedelta,
    ) -> None:
        """Sleep using an explicit timer ID (durable timer).

        Unlike ``sleep()``, which auto-generates an ID, this lets you
        supply a deterministic timer ID for reliable replay and
        cancellation.

        Args:
            timer_id: Explicit timer identifier.
            duration: How long to sleep, as a ``timedelta``.

        Example:
            >>> await ctx.sleep_with_id("payment-timeout", timedelta(minutes=30))
        """
        # One number from the step counter on every path.
        sequence = self._next_sequence()
        self._reach(timer_id)

        cache_key = f"timer:{timer_id}"
        if cache_key in self._cached_results:
            self._observe(cache_key)
            return

        # Shape must match sdk-core's StartTimerCommand; see sleep().
        total = duration.total_seconds()
        secs = int(total)
        nanos = int(round((total - secs) * 1_000_000_000))
        self._commands.append(
            {
                "StartTimer": {
                    "sequence": sequence,
                    "timer_id": timer_id,
                    "duration": {"secs": secs, "nanos": nanos},
                }
            }
        )

        from orcher.errors import WorkflowSuspendedError

        raise WorkflowSuspendedError(f"Waiting for timer: {timer_id}")

    # ── Fire-and-Forget Child Workflows ──────────────────────────────

    async def start_child_workflow(
        self,
        workflow_type: str,
        *,
        workflow_id: str | None = None,
        args: tuple[Any, ...] = (),
        task_queue: str | None = None,
    ) -> ChildWorkflowHandle[Any]:
        """Start a child workflow without waiting for its result.

        Returns a ``ChildWorkflowHandle`` immediately. This call does NOT
        suspend, so several children can be started before any is awaited:
        start N, then ``await handle.result()`` on each to run them
        concurrently. Use ``execute_child_workflow`` for the common
        start-and-wait case.

        Args:
            workflow_type: Type/name of the child workflow.
            workflow_id: Optional ID for the child. When omitted, a deterministic
                ``child_{sequence}`` id is derived from the step sequence, as in
                ``execute_child_workflow`` and the Rust and TypeScript SDKs.
            args: Arguments to pass.
            task_queue: Optional task queue (defaults to parent's queue).

        Returns:
            A handle for later interaction with the child.

        Example:
            >>> a = await ctx.start_child_workflow("charge", args=(order,))
            >>> b = await ctx.start_child_workflow("reserve", args=(order,))
            >>> charged, reserved = await a.result(), await b.result()
        """
        # One number from the step counter on every path; a child given no id
        # is named after it (see execute_child_workflow).
        sequence = self._next_sequence()
        if workflow_id is None:
            workflow_id = f"child_{sequence}"
        self._reach(workflow_id)

        # On replay the child may already have ended — return a handle
        # without re-emitting the command.
        if f"child:{workflow_id}" in self._cached_results:
            return ChildWorkflowHandle(
                workflow_id=workflow_id,
                run_id="",
                workflow_type=workflow_type,
                context=self,
            )

        # Same input encoding and command shape as execute_child_workflow: a
        # single arg is sent as the value, several as a list.
        child_input: Any = args[0] if len(args) == 1 else list(args)
        input_bytes = json.dumps(child_input).encode("utf-8")
        input_payload = {
            "data": list(input_bytes),
            "metadata": {"encoding": list(b"json")},
        }

        self._commands.append(
            {
                "StartChildWorkflow": {
                    "sequence": sequence,
                    "workflow_id": workflow_id,
                    "workflow_type": workflow_type,
                    "task_queue": task_queue or self._info.task_queue,
                    "input": [input_payload],
                    "timeout": None,
                    "orphan_policy": "Cancel",
                }
            }
        )

        return ChildWorkflowHandle(
            workflow_id=workflow_id,
            run_id="",  # assigned by server
            workflow_type=workflow_type,
            context=self,
        )

    async def send_event(
        self,
        target_workflow_id: str,
        event_name: str,
        data: Any = None,
    ) -> None:
        """Send an event to another workflow (fire-and-forget).

        The send is journaled as a step of this workflow: it reaches the
        engine with the commands of the activation that makes it, and a replay
        does not send it again.

        Args:
            target_workflow_id: ID of the target workflow.
            event_name: Name of the event to send.
            data: Optional event payload (JSON-serializable).

        Example:
            >>> await ctx.send_event("order-456", "payment-received", {"amount": 100})
        """
        payload_bytes = json.dumps(data).encode("utf-8") if data is not None else b"null"

        async def send() -> None:
            self._commands.append(
                {
                    "SendEvent": {
                        "workflow_id": target_workflow_id,
                        "run_id": None,
                        "event_name": event_name,
                        "payload": {
                            "data": list(payload_bytes),
                            "metadata": {},
                        },
                        "headers": [],
                    }
                }
            )

        await self.execute(f"send_event:{target_workflow_id}:{event_name}", send)

    # ── Restart With Fresh History ───────────────────────────────────

    def restart_fresh(
        self,
        input: Any = None,  # noqa: A002
        *,
        workflow_type: str | None = None,
        task_queue: str | None = None,
    ) -> None:
        """Restart the workflow with a fresh execution history.

        Use this in long-running workflows to keep history from growing
        without bound. The current execution completes and a new one starts
        with the given input.

        Args:
            input: Input for the new execution.
            workflow_type: Optional different workflow type.
            task_queue: Optional different task queue.

        Raises:
            WorkflowSuspendedError: Always. The native layer performs the
                restart.

        Example:
            >>> if len(processed_items) > 1000:
            ...     ctx.restart_fresh(
            ...         {"cursor": last_cursor, "batch": batch + 1},
            ...     )
        """
        # Field names and shape must match sdk-core's RestartFreshCommand:
        # workflow_type is a required string ("" means the same type), input is
        # a list of Payloads, and task_queue and timeout are optional.
        input_bytes = json.dumps(input).encode("utf-8") if input is not None else b"null"
        input_payload = {
            "data": list(input_bytes),
            "metadata": {"encoding": list(b"json")},
        }
        self._commands.append(
            {
                "RestartFresh": {
                    "workflow_type": workflow_type or "",
                    "input": [input_payload],
                    "task_queue": task_queue,
                    "timeout": None,
                }
            }
        )

        from orcher.errors import WorkflowSuspendedError

        raise WorkflowSuspendedError("Restarting workflow with fresh history")

    # ── Internal helpers ─────────────────────────────────────────────

    def _get_commands(self) -> list[dict[str, Any]]:
        """Get commands generated during execution (internal)."""
        return self._commands

    def _take_commands(self) -> list[dict[str, Any]]:
        """Take the activation's commands, closure results first (internal).

        Closure results go first, in the order the closures finished: the
        other commands include any terminal command, which must come last,
        and a closure can finish after the workflow issued one.
        """
        commands, self._commands = self._commands, []
        results = [c for c in commands if "RecordStepResult" in c]
        return results + [c for c in commands if "RecordStepResult" not in c]

    def _record_journal_times(self, times: dict[str, Any]) -> None:
        """Record when the engine journaled what this activation can hand the
        workflow (internal; the worker calls it before the workflow runs).

        ``times`` is what the native layer reads from the journal:
        ``resolved_at`` keyed as the results are, and ``events``, each event
        name's times in journal order.
        """
        for key, at_ms in (times.get("resolved_at") or {}).items():
            if isinstance(at_ms, int):
                self._resolved_at[key] = at_ms
        for name, at in (times.get("events") or {}).items():
            self._event_times[name] = [ms for ms in at if isinstance(ms, int)]

    def _clear_commands(self) -> None:
        """Clear commands (internal)."""
        self._commands.clear()

    def _set_cached_results(self, results: dict[str, Any]) -> None:
        """Set the journaled results of steps that already completed (internal).

        The step counter is not restored: every step takes one number from it
        whether its result is cached or not, so a replay derives the same ids.
        """
        self._cached_results = results

    def _get_task_return_type(self, task_fn: Callable[..., Any]) -> type[Any] | None:
        """Get the return type hint for a task function.

        Args:
            task_fn: The task function (decorated with @task).

        Returns:
            The return type hint, or None if not available.
        """
        task_metadata = getattr(task_fn, "__orcher_task__", None)
        if task_metadata is not None:
            return getattr(task_metadata, "return_type", None)
        return None

    def _coerce_to_dataclass(self, data: dict[str, Any], dc_type: type[Any]) -> Any:
        """Convert a dict to a dataclass instance.

        Rebuilds dataclass results from journaled task results, which are
        serialized as dicts. Nested dataclass fields are converted recursively.

        Args:
            data: The dict to convert.
            dc_type: The target dataclass type.

        Returns:
            A dataclass instance, or the original data if conversion fails.
        """
        if not dataclasses.is_dataclass(dc_type):
            return data

        if not isinstance(dc_type, type):
            # dc_type is a dataclass instance, not a class
            return data

        try:
            fields = {f.name: f.type for f in dataclasses.fields(dc_type)}
            coerced: dict[str, Any] = {}

            for name, val in data.items():
                if name in fields:
                    field_type = fields[name]
                    # For a generic or Union type such as Optional[X], use its
                    # first dataclass argument.
                    origin = get_origin(field_type)
                    if origin is not None:
                        args = get_args(field_type)
                        for arg in args:
                            if arg is not type(None) and dataclasses.is_dataclass(arg):
                                field_type = arg
                                break

                    if (
                        dataclasses.is_dataclass(field_type)
                        and isinstance(field_type, type)
                        and isinstance(val, dict)
                    ):
                        coerced[name] = self._coerce_to_dataclass(val, field_type)
                    else:
                        coerced[name] = val
                else:
                    # Passed through unchanged. An unknown field makes the
                    # constructor raise, which falls back to the plain dict.
                    coerced[name] = val

            return dc_type(**coerced)
        except Exception:
            # Any failure returns the original dict, so a result that no longer
            # fits the declared type is still delivered.
            return data
