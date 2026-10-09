"""The contract workflow and task catalog for the Python SDK.

It matches the Rust and TypeScript catalogs: the same workflow and task names
and the same JSON result shapes, so one ``scenarios.json`` drives every SDK's
worker. Each workflow returns a JSON result describing the durable behavior it
observed. The harness asserts only on that client-observable outcome.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from typing import Any

from orcher import TaskContext, TaskError, WorkflowContext, WorkflowError, task, workflow

# The Python SDK spreads a workflow's input object into keyword arguments, so a
# `{"message": ...}` input binds the `message` parameter. The contract asserts
# the result shape, so `echo` wraps its value back into `{"message": ...}`, as
# the Rust and TypeScript workers return it.


@workflow(name="echo")
async def echo(ctx: WorkflowContext, message: Any) -> dict:
    """Return the input unchanged, as an object."""
    return {"message": message}


@task(name="always_fail", retry=1)
async def always_fail(ctx: TaskContext) -> dict:
    """Always fail, with a single attempt and no retries."""
    raise RuntimeError("intentional contract failure")


@workflow(name="catch_task_failure")
async def catch_task_failure(ctx: WorkflowContext) -> dict:
    """Run a task that fails, catch the durable failure, report what was observed."""
    try:
        await ctx.execute_task(always_fail)
    except TaskError as e:
        return {"caught": True, "task_type": e.task_type}
    # Reached only if the task succeeds. Returning caught=False lets the harness
    # assertion (which expects caught=true) report it.
    return {"caught": False, "task_type": ""}


@task(name="always_fail_retried", retry=3)
async def always_fail_retried(ctx: TaskContext) -> dict:
    """Always fail, declared with three attempts so the engine retries to exhaustion."""
    raise RuntimeError("intentional contract failure (retried)")


@workflow(name="task_retries_exhausted")
async def task_retries_exhausted(ctx: WorkflowContext) -> dict:
    """Run a task until its retries are exhausted and report how many attempts it took.

    This is the only scenario that checks the attempt count. A worker that loses
    the count still reports a failure and passes every other assertion.
    """
    try:
        await ctx.execute_task(always_fail_retried)
    except TaskError as e:
        return {
            "caught": True,
            "task_type": e.task_type,
            "attempts": getattr(e, "attempts", 0),
        }
    return {"caught": False, "task_type": "", "attempts": 0}


class CardDeclined(Exception):  # noqa: N818 - named as the other SDKs name it
    """A failure the retry policy below names as non-retryable."""


class Throttled(Exception):  # noqa: N818 - named as the other SDKs name it
    """A failure no retry policy here names."""


class AccountClosed(Exception):  # noqa: N818 - named as the other SDKs name it
    """A failure raised as non-retryable, whatever the policy lists."""

    non_retryable = True


_LISTS_CARD_DECLINED = {"max_attempts": 3, "non_retryable_errors": ["CardDeclined"]}


@task(name="decline_card", retry_policy=_LISTS_CARD_DECLINED)
async def decline_card(ctx: TaskContext) -> dict:
    """Fail with a type the retry policy lists as non-retryable, so it runs once."""
    raise CardDeclined("card declined")


@task(name="throttle", retry_policy=_LISTS_CARD_DECLINED)
async def throttle(ctx: TaskContext) -> dict:
    """Fail with a type the policy does not list, so it is retried to exhaustion."""
    raise Throttled("slow down")


@task(name="close_account", retry=3)
async def close_account(ctx: TaskContext) -> dict:
    """Fail with an error marked non-retryable whose type no policy lists.

    The mark alone stops the retries.
    """
    raise AccountClosed("account closed")


async def _attempts_until_failure(ctx: WorkflowContext, fn: Any) -> dict:
    try:
        await ctx.execute_task(fn)
    except TaskError as e:
        return {
            "caught": True,
            "task_type": e.task_type,
            "attempts": getattr(e, "attempts", 0),
        }
    return {"caught": False, "task_type": "", "attempts": 0}


@workflow(name="task_non_retryable_listed")
async def task_non_retryable_listed(ctx: WorkflowContext) -> dict:
    """A failure whose type the policy lists as non-retryable is not retried."""
    return await _attempts_until_failure(ctx, decline_card)


@workflow(name="task_non_retryable_marked")
async def task_non_retryable_marked(ctx: WorkflowContext) -> dict:
    """A failure raised as non-retryable is not retried."""
    return await _attempts_until_failure(ctx, close_account)


@workflow(name="task_unlisted_retried")
async def task_unlisted_retried(ctx: WorkflowContext) -> dict:
    """A failure whose type the policy does not list is retried as usual."""
    return await _attempts_until_failure(ctx, throttle)


@task(name="echo_task")
async def echo_task(ctx: TaskContext, value: Any) -> dict:
    """Return the input value under `echoed`."""
    return {"echoed": value}


@workflow(name="task_input_roundtrip")
async def task_input_roundtrip(ctx: WorkflowContext, value: Any) -> dict:
    """Pass a value through a task and back, proving the task receives its input."""
    return await ctx.execute_task(echo_task, value=value)


def _as_int(n: Any) -> int:
    """Read the child's single argument as an int, whatever its encoding.

    The argument may arrive as a scalar, ``[scalar]``, or ``{"n": scalar}``.
    Argument encoding is not what the child scenarios check; they check the
    round trip."""
    if isinstance(n, dict):
        n = n.get("n")
    elif isinstance(n, (list, tuple)):
        n = n[0]
    return int(n)


@workflow(name="contract_child")
async def contract_child(ctx: WorkflowContext, n: Any) -> dict:
    """A child workflow: echo what it received and double it."""
    value = _as_int(n)
    return {"child_saw": value, "doubled": value * 2}


@workflow(name="parent_starts_child")
async def parent_starts_child(ctx: WorkflowContext, n: Any) -> dict:
    """Start a child workflow, wait for its result, and return it.

    Covers the full child-workflow round trip: StartChildWorkflow emission, child
    execution, and the completion decode that resumes the parent. The child
    must run exactly once."""
    return await ctx.execute_child_workflow("contract_child", args=(_as_int(n),))


@workflow(name="failing_child")
async def failing_child(ctx: WorkflowContext) -> dict:
    """A child workflow that always fails, for the child-failure scenario."""
    raise WorkflowError.execution_failed(
        workflow_id="failing_child", message="intentional child failure"
    )


@workflow(name="parent_catches_child_failure")
async def parent_catches_child_failure(ctx: WorkflowContext) -> dict:
    """Start a failing child, catch the durable child failure, and report it.

    WorkflowSuspendedError is a BaseException, so `except WorkflowError` never
    swallows the suspension raised on the first execution."""
    try:
        await ctx.execute_child_workflow("failing_child", args=())
    except WorkflowError:
        return {"child_failed": True}
    return {"child_failed": False}


@workflow(name="restart_once")
async def restart_once(ctx: WorkflowContext, count: Any = 0) -> dict:
    """Restart fresh once, then complete.

    Covers restart-fresh persistence, and checks that a client following the
    original execution receives the continued execution's result. restart_fresh
    raises WorkflowSuspendedError, so the restart is the terminal action."""
    c = int(count) if count else 0
    if c == 0:
        ctx.restart_fresh({"count": 1})
        return {"restarted": True, "count": 0}
    return {"restarted": True, "count": c}


@workflow(name="parent_two_children_parallel")
async def parent_two_children_parallel(ctx: WorkflowContext) -> dict:
    """Start two child workflows in parallel, then await both.

    start_child_workflow does not suspend, so both StartChildWorkflow commands
    are emitted before either result is awaited. Covers the handle path
    (start_child_workflow + handle.result) and the engine's idempotent child
    start."""
    a = await ctx.start_child_workflow("contract_child", args=(10,))
    b = await ctx.start_child_workflow("contract_child", args=(20,))
    first = await a.result()
    second = await b.result()
    return {"first": first["doubled"], "second": second["doubled"]}


@workflow(name="sleep_once")
async def sleep_once(ctx: WorkflowContext) -> dict:
    """Sleep on a durable timer, then complete.

    Covers the durable-timer round trip: StartTimer emission, the engine firing
    the timer, and the FireTimer correlation that resumes the workflow by the
    business timer id on replay."""
    await ctx.sleep(timedelta(seconds=1))
    return {"slept": True}


# --- Event catalog -----------------------------------------------------------
#
# These workflows cover the wait-for-event path end to end: the activation that
# parks is accepted, a client can wake the parked workflow by id alone, and the
# engine does not re-dispatch a parked workflow while it waits.
#
# Observation is in-process, like the actor gauges: the driver runs the worker,
# so the catalog can tell it when a workflow has actually parked and how many
# times the engine activated it. Both are keyed by the `key` the scenario
# passes as input, because the engine hands the worker its own internal id as
# the workflow id, which the driver never sees.

# Keys of workflows that have issued their wait and are about to suspend.
PARKED_WORKFLOWS: set[str] = set()

# Activations per key: every time the engine ran the workflow function,
# replays included. A parked workflow should be activated a handful of times
# (park, wake, complete), not hundreds.
ACTIVATIONS: dict[str, int] = {}


def _record_activation(key: str) -> int:
    ACTIVATIONS[key] = ACTIVATIONS.get(key, 0) + 1
    return ACTIVATIONS[key]


@workflow(name="wait_for_event_echo")
async def wait_for_event_echo(ctx: WorkflowContext, key: str) -> dict:
    """Park on `contract_event`, then return the payload and the activation count.

    Covers the whole event round trip: the engine accepts the parking
    activation, the parked workflow stays parked (one activation, not a storm),
    a client can wake it by workflow id alone, and the replay consumes the
    journaled event and completes.
    """
    count = _record_activation(key)
    PARKED_WORKFLOWS.add(key)
    received = await ctx.wait_for_event("contract_event")
    return {"received": received, "activations": count}


@workflow(name="wait_for_event_timeout")
async def wait_for_event_timeout(ctx: WorkflowContext, key: str) -> dict:
    """Park on `contract_event` with a two-second timeout and report whether it fired.

    No event is ever sent. The deadline is a durable timer the SDK starts beside
    the wait, so the engine fires it even while no worker holds the workflow.
    """
    _record_activation(key)
    PARKED_WORKFLOWS.add(key)
    try:
        received = await ctx.wait_for_event_with_timeout("contract_event", timedelta(seconds=2))
    except TimeoutError:
        return {"received": None, "timed_out": True}
    return {"received": received, "timed_out": received is None}


# --- Cancellation: a cancelled workflow is told, once, and may clean up -------


@task(name="cancel_cleanup", retry=1)
async def cancel_cleanup(ctx: TaskContext) -> str:
    """Cleanup long enough to be heartbeated. Reports whether it was told to
    stop: work started after a cancellation request is cleanup, and the engine
    must let it finish."""
    import asyncio
    import time

    started = time.monotonic()
    while time.monotonic() - started < 2.5:
        if ctx.cancellation_token.is_cancelled:
            return "interrupted"
        await asyncio.sleep(0.1)
    return "cleaned"


@task(name="cancel_reserve", retry=1)
async def cancel_reserve(ctx: TaskContext) -> str:
    """A step a saga can undo."""
    return "reserved"


@task(name="cancel_release", retry=1)
async def cancel_release(ctx: TaskContext) -> str:
    """Undoes ``cancel_reserve``."""
    return "released"


def _is_cancellation(e: Exception) -> bool:
    from orcher.errors import ErrorCode

    return isinstance(e, WorkflowError) and e.code == ErrorCode.WORKFLOW_CANCELLED


@workflow(name="cancel_sleep")
async def cancel_sleep(ctx: WorkflowContext, key: str) -> str:
    """Park on a long sleep and let the cancellation it is told of end it."""
    PARKED_WORKFLOWS.add(key)
    await ctx.sleep(timedelta(minutes=10))
    return "slept"


@workflow(name="cancel_cleanup")
async def cancel_cleanup_workflow(ctx: WorkflowContext, key: str) -> dict:
    """Park on a long sleep; when told it is cancelled, run cleanup and return,
    which ends it completed."""
    PARKED_WORKFLOWS.add(key)
    try:
        await ctx.sleep(timedelta(minutes=10))
    except WorkflowError as e:
        if not _is_cancellation(e):
            raise
        cleanup = await ctx.execute_task(cancel_cleanup)
        return {"told": ctx.is_cancel_requested(), "cleanup": cleanup}
    raise WorkflowError.execution_failed(
        ctx.info.workflow_id, "the sleep finished; the cancellation never arrived"
    )


@workflow(name="cancel_saga")
async def cancel_saga(ctx: WorkflowContext, key: str) -> dict:
    """Reserve, then park; when told it is cancelled, compensate the reservation
    and report how many compensations ran."""
    from orcher import Saga

    saga = Saga()
    await saga.add_step(
        action=lambda: ctx.execute_task(cancel_reserve),
        compensation=lambda: ctx.execute_task(cancel_release),
    )
    PARKED_WORKFLOWS.add(key)
    try:
        await ctx.sleep(timedelta(minutes=10))
    except WorkflowError as e:
        if not _is_cancellation(e):
            raise
        compensated = await saga.compensate()
        return {"compensated": compensated}
    raise WorkflowError.execution_failed(
        ctx.info.workflow_id, "the sleep finished; the cancellation never arrived"
    )


# --- Actor catalog -----------------------------------------------------------
#
# `contract_probe` checks the shared/exclusive scheduling contract end to end:
# declared mode -> handler registration -> engine resolution -> dispatch.
# Concurrency is observed through in-process gauges (module globals). The
# engine dispatches both operations to this same worker process, so a gauge
# reaching 2 proves real overlap. The gauges are deliberately transient:
# durable actor state cannot hold them, because shared operations must stay
# read-only. The engine rejects their writes, and that rejection is its own
# scenario.

import asyncio  # noqa: E402

from orcher import ActorContext, OperationMode, SharedActorContext, actor, operation  # noqa: E402

_EXCLUSIVE_IN_FLIGHT = 0

# Count of rendezvous entries per actor key. It only ever increases, unlike an
# in-flight gauge, so the first entrant still sees the second even if the
# second checks, returns, and exits between the first's polls.
_RENDEZVOUS_ENTERED: dict[str, int] = {}

_RENDEZVOUS_DEADLINE_SECS = 5.0
_RENDEZVOUS_POLL_SECS = 0.025
_EXCLUSIVE_HOLD_SECS = 0.25


@actor("contract_probe")
class ContractProbe:
    """Probe actor observing the scheduler's shared/exclusive behavior."""

    @operation(mode=OperationMode.SHARED)
    async def shared_rendezvous(self, ctx: SharedActorContext) -> dict:
        """Wait until a second shared invocation has entered on this worker.

        When shared operations dispatch concurrently, as they should, the
        second invocation enters while the first is still waiting. Both then see
        an entry count of 2 and return observed_concurrent=true. If the
        scheduler serializes them, the first invocation reaches the deadline
        before the second can enter and reports false.
        """
        key = ctx.key
        _RENDEZVOUS_ENTERED[key] = _RENDEZVOUS_ENTERED.get(key, 0) + 1

        waited = 0.0
        observed = _RENDEZVOUS_ENTERED.get(key, 0) >= 2
        while not observed and waited < _RENDEZVOUS_DEADLINE_SECS:
            await asyncio.sleep(_RENDEZVOUS_POLL_SECS)
            waited += _RENDEZVOUS_POLL_SECS
            observed = _RENDEZVOUS_ENTERED.get(key, 0) >= 2
        return {"observed_concurrent": observed}

    @operation(mode=OperationMode.EXCLUSIVE)
    async def exclusive_probe(self, ctx: ActorContext) -> dict:
        """Hold the key briefly and report whether any peer overlapped."""
        global _EXCLUSIVE_IN_FLIGHT
        _EXCLUSIVE_IN_FLIGHT += 1
        try:
            alone = _EXCLUSIVE_IN_FLIGHT == 1
            await asyncio.sleep(_EXCLUSIVE_HOLD_SECS)
            alone = alone and _EXCLUSIVE_IN_FLIGHT == 1
            return {"alone": alone}
        finally:
            _EXCLUSIVE_IN_FLIGHT -= 1

    @operation(mode=OperationMode.SHARED)
    async def shared_write_attempt(self, ctx: SharedActorContext) -> dict:
        """Attempt a state write from a read-only operation; expect rejection."""
        try:
            await ctx.state.set("probe", "should-be-rejected")
            return {"write_rejected": False}
        except Exception as e:  # engine rejects with FAILED_PRECONDITION
            return {"write_rejected": True, "error": str(e)[:200]}

    @operation(mode=OperationMode.EXCLUSIVE)
    async def exclusive_write_roundtrip(self, ctx: ActorContext, value: Any) -> dict:
        """Write state from an exclusive operation; the write must succeed and read back."""
        await ctx.state.set("value", value)
        read = await ctx.state.get("value")
        return {"write_ok": read == value, "value": read}

@task(name="timeouts_echo_task")
async def timeouts_echo_task(ctx: TaskContext) -> dict:
    """Report the heartbeat timeout this task was actually given.

    Covers the whole declaration loop: declared by the workflow, recorded by the
    engine, delivered on poll, and readable by the handler.
    """
    hb = ctx.heartbeat_timeout
    return {"heartbeat_secs": round(hb.total_seconds()) if hb else None}


@workflow(name="task_timeouts_echo")
async def task_timeouts_echo(ctx: WorkflowContext) -> dict:
    return await ctx.execute_task(
        timeouts_echo_task, heartbeat_timeout=timedelta(seconds=15)
    )

@workflow(name="typed_failure")
async def typed_failure(ctx: WorkflowContext) -> dict:
    """Raise the SDK's typed workflow error.

    The scenario asserts that the message reaches the caller. Checking only that
    the workflow failed is not enough: a failure the engine cannot parse also
    counts as a failure.
    """
    raise WorkflowError.execution_failed(
        "typed_failure", "deliberate contract failure"
    )



# How many times this worker process has started the task, by workflow id. A
# task that timed out and was retried here counts two runs.
_RUNS: dict[str, int] = {}


@task(name="sleep_past_heartbeat_timeout")
async def sleep_past_heartbeat_timeout(ctx: TaskContext, sleep_secs: int) -> dict:
    """Sleep ``sleep_secs`` without heartbeating, then report how the run went.

    The result says how many times the task has run for its workflow and whether
    the sleep outlasted the heartbeat timeout it was given. Only the worker's
    own automatic heartbeats keep the task alive while it sleeps."""
    runs = _RUNS[ctx.info.workflow_id] = _RUNS.get(ctx.info.workflow_id, 0) + 1
    await asyncio.sleep(sleep_secs)
    hb = ctx.heartbeat_timeout
    return {
        "runs": runs,
        "heartbeat_timeout_given": hb is not None,
        "outlived_heartbeat_timeout": hb is not None and sleep_secs > hb.total_seconds(),
    }


@workflow(name="outlive_heartbeat_timeout")
async def outlive_heartbeat_timeout(ctx: WorkflowContext, sleep_secs: int) -> dict:
    """Run a task that declares no timeouts for longer than its heartbeat timeout."""
    return await ctx.execute_task(sleep_past_heartbeat_timeout, sleep_secs=sleep_secs)


@task(name="exit_if_worker_doomed", retry=3)
async def exit_if_worker_doomed(ctx: TaskContext) -> dict:
    """Kill the worker process if ``ORCHER_CONTRACT_DOOMED_WORKER`` is set.

    This simulates a worker dying mid-task. On any other worker the task
    completes."""
    if os.environ.get("ORCHER_CONTRACT_DOOMED_WORKER"):
        print("contract: exit_if_worker_doomed is taking its worker down", file=sys.stderr)
        sys.stderr.flush()
        os._exit(3)
    return {"survived_worker_death": True}


@workflow(name="crash_mid_task")
async def crash_mid_task(ctx: WorkflowContext) -> dict:
    """Run a task that kills a doomed worker, and complete once another worker runs it."""
    return await ctx.execute_task(exit_if_worker_doomed)
