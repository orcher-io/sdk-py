"""Cross-SDK contract harness: Python worker and driver.

Starts a worker serving the shared contract catalog, then runs every scenario
in ``scenarios.json`` against a live engine and asserts that the terminal
status and returned result match ``expect``. Every assertion is purely
client-observable (terminal state and returned JSON), so the same
``scenarios.json`` drives the Rust and TypeScript workers identically.

Usage (against an already-running engine on :50051):
    PYTHONPATH=src python contract/run.py --server-url http://localhost:50051

Exits non-zero if any scenario fails.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

# Importing the catalog registers its @workflow/@task handlers with the global
# registry, which the worker builder collects at build time.
import catalog  # noqa: F401

from orcher import Client, ClientConfig, Worker, WorkflowIdReusePolicy

# This runner's name in a scenario's `known_gap_sdks`.
THIS_SDK = "python"


def json_matches(expected: Any, actual: Any) -> bool:
    """Structural match: every field in ``expected`` must be present and deep-equal
    in ``actual``. Extra fields in ``actual`` are ignored, so a scenario asserts
    only the fields it lists."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        return all(k in actual and json_matches(v, actual[k]) for k, v in expected.items())
    if isinstance(expected, list) and isinstance(actual, list):
        return len(expected) == len(actual) and all(
            json_matches(e, a) for e, a in zip(expected, actual, strict=False)
        )
    return expected == actual


async def run_actor_scenario(
    worker: Worker, args: argparse.Namespace, sc: dict
) -> tuple[bool, str]:
    """Run one actor scenario.

    For each step, fire ``parallel`` concurrent invocations against a fresh
    per-run key and assert every invocation's result against ``expect_each``."""
    from orcher.actor.client import ActorInvocationClient

    bridge = worker._bridge_worker  # noqa: SLF001 — harness drives the in-process worker's bridge
    if bridge is None:
        return False, "native bridge unavailable (mock mode cannot invoke actors)"

    key = f"conf-{uuid.uuid4()}"
    inv = ActorInvocationClient(bridge, sc["actor"], key)

    for step in sc["steps"]:
        n = int(step.get("parallel", 1))
        results = await asyncio.gather(
            *(inv.invoke(step["operation"], step.get("input", {})) for _ in range(n)),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, BaseException):
                return False, f"operation {step['operation']} raised: {r}"
            if not json_matches(step.get("expect_each", {}), r):
                return False, (
                    f"result mismatch on {step['operation']}\n"
                    f"    expected: {step.get('expect_each')}\n    actual:   {r}"
                )
    return True, ""


async def perform_client_call(client: Client, call: str) -> None:
    """Make the named failing client call.

    Each call is a plain client operation whose failure every SDK is expected to
    report identically. Kept to operations that need no worker, so the assertion
    is about the client's error contract and nothing else.
    """
    if call == "status_of_missing_workflow":
        handle = await client.get_workflow(f"contract-missing-{os.getpid()}-{uuid.uuid4()}")
        await handle.status()
        return
    raise AssertionError(f"unknown client_call: {call}")


async def run_client_error_scenario(client: Client, sc: dict) -> tuple[bool, str]:
    """Make the call, require it to fail, and assert the reported code.

    The assertion is on the code, not the class name. The SDKs legitimately
    differ in the class (one reports a client-error variant where the others
    report a workflow error), but the code is the contract a user reads across
    languages, so every SDK must report the same code for the same condition.
    """
    expected = sc.get("expect", {}).get("error_code")
    if not expected:
        return False, "client_error scenario missing `expect.error_code`"
    call = sc.get("client_call")
    if not call:
        return False, "client_error scenario missing `client_call`"

    caught: BaseException | None = None
    try:
        await perform_client_call(client, call)
    except BaseException as e:  # noqa: BLE001 - the scenario is about what is raised
        caught = e

    if caught is None:
        return False, f"expected the call to fail with {expected}, but it succeeded"

    # Compare on the enum member name. Python spells its codes as an IntEnum
    # (WORKFLOW_NOT_FOUND == 1005) where the other SDKs use the string itself,
    # so the name is what all three share. It is also what the message prefix
    # and the docs show.
    actual = getattr(caught, "code", None)
    actual = getattr(actual, "name", actual)
    if actual != expected:
        return False, (
            f"expected error code {expected}, got {actual} "
            f"({type(caught).__name__}: {caught})"
        )
    return True, ""


async def run_scenario(client: Client, args: argparse.Namespace, sc: dict) -> tuple[bool, str]:
    """Start one scenario's workflow, wait for a terminal state, assert ``expect``."""
    started_at = time.monotonic()
    handle = await client.start_workflow(
        sc["workflow"],
        workflow_id=f"conf-{uuid.uuid4()}",
        task_queue=args.task_queue,
        args=(sc.get("input", {}),),
    )
    expect = sc["expect"]
    try:
        result = await handle.result(timeout=args.result_timeout_secs)
    except Exception as e:  # workflow terminated failed (or fetch failed)
        if expect["status"] == "failed":
            wanted = expect.get("error_contains")
            if wanted and wanted not in str(e):
                return False, (
                    "failed as expected, but the message did not survive\n"
                    f"    wanted substring: {wanted}\n"
                    f"    surfaced:         {e}"
                )
            return True, ""
        return False, f"expected completion but workflow failed: {e}"

    if expect["status"] == "failed":
        return False, f"expected failure but workflow completed with: {result}"

    # Duration is part of the contract for timer scenarios: a 1s timer must not
    # take 30s, even though the result would still match.
    elapsed_ms = (time.monotonic() - started_at) * 1000
    min_elapsed = expect.get("min_elapsed_ms")
    max_elapsed = expect.get("max_elapsed_ms")
    if min_elapsed is not None and elapsed_ms < min_elapsed:
        return False, (
            f"completed too fast: {elapsed_ms:.0f}ms < {min_elapsed}ms (the wait did not happen)"
        )
    if max_elapsed is not None and elapsed_ms > max_elapsed:
        return False, (
            f"took too long: {elapsed_ms:.0f}ms > {max_elapsed}ms (waited far longer than asked)"
        )

    if json_matches(expect.get("result", {}), result):
        return True, ""
    return False, (f"result mismatch\n    expected: {expect.get('result')}\n    actual:   {result}")


async def run_reset_scenario(
    client: Client, args: argparse.Namespace, sc: dict
) -> tuple[bool, str]:
    """Run a workflow to completion, reset it, and assert on the successor run.

    The workflow is reset to ``reset_to_event_id``; the successor must replay the
    copied prefix and reach ``expect``. This covers the whole reset path, not just
    the client binding: the engine reads the journal by the execution's internal
    id (not the user-facing workflow id), seeds the successor's journal before the
    run becomes dispatchable, and persists the ``reset`` status on the original.
    """
    expect = sc["expect"]
    if expect["status"] != "completed":
        return False, "reset scenarios only support an expected status of 'completed'"

    workflow_id = f"conf-reset-{uuid.uuid4()}"
    handle = await client.start_workflow(
        sc["workflow"],
        workflow_id=workflow_id,
        task_queue=args.task_queue,
        args=(sc.get("input", {}),),
    )
    try:
        await handle.result(timeout=args.result_timeout_secs)
    except Exception as e:
        return False, f"original run did not complete: {e}"

    try:
        new_execution_id = await handle.reset(sc["reset_to_event_id"], "contract reset")
    except Exception as e:
        return False, f"reset rejected: {e}"

    if not new_execution_id:
        return False, "reset returned an empty execution id (success without resetting)"

    successor = await client.get_workflow(workflow_id, run_id=new_execution_id)
    try:
        result = await successor.result(timeout=args.result_timeout_secs)
    except Exception as e:
        return False, f"successor run {new_execution_id} did not complete: {e}"

    if json_matches(expect.get("result", {}), result):
        return True, ""
    return False, (
        f"successor result mismatch\n    expected: {expect.get('result')}\n"
        f"    actual:   {result}"
    )


def _within_bounds(bounds: dict[str, float], actual: Any) -> str | None:
    """Check numeric upper bounds on a result.

    Every listed field must be present in ``actual`` and no greater than its
    bound. Returns a description of the problem, or None."""
    if not isinstance(actual, dict):
        return "result is not an object"
    for field, maximum in bounds.items():
        value = actual.get(field)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return f"{field} is missing or not a number"
        if value > maximum:
            return f"{field} = {value}, expected at most {maximum}"
    return None


async def _wait_until_parked(key: str, deadline_secs: float) -> bool:
    """Wait until the catalog reports the keyed workflow parked, or give up.

    The worker runs in this process, so "parked" is observed directly rather
    than inferred from a status the engine reports as RUNNING either way."""
    until = time.monotonic() + deadline_secs
    while time.monotonic() < until:
        if key in catalog.PARKED_WORKFLOWS:
            return True
        await asyncio.sleep(0.05)
    return False


async def run_event_scenario(
    client: Client, args: argparse.Namespace, sc: dict
) -> tuple[bool, str]:
    """Park a workflow, wake it with the named event by workflow id, and assert the result.

    Each step is checked: the parking activation must be accepted, the wake must
    be addressable by workflow id alone, and the parked workflow must not be
    re-dispatched while it waits. The activation count in the result, bounded by
    ``expect.result_at_most``, catches the last one.
    """
    event_name = sc.get("event_name")
    if not event_name:
        return False, "event scenario missing `event_name`"
    expect = sc["expect"]

    key = f"conf-{uuid.uuid4()}"
    workflow_id = f"conf-event-{key}"
    scenario_input = sc.get("input", {})
    payload = {**(scenario_input if isinstance(scenario_input, dict) else {}), "key": key}

    handle = await client.start_workflow(
        sc["workflow"], workflow_id=workflow_id, task_queue=args.task_queue, args=(payload,)
    )

    if not await _wait_until_parked(key, args.result_timeout_secs):
        return False, (
            "the workflow never parked (the wait was not reached, or the activation failed)"
        )
    # A parked workflow holds its claim; give the engine a moment to record the
    # parking completion before the event arrives, so the wake is a real wake
    # and not an event consumed by the first activation.
    await asyncio.sleep(0.5)

    # By workflow id alone: a caller holding a business key has no run id.
    target = await client.get_workflow(workflow_id)
    await target.send_event(event_name, sc.get("event_payload", {}))

    try:
        result = await handle.result(timeout=args.result_timeout_secs)
    except Exception as e:
        # Cancel the run so a failed event scenario does not leave a parked
        # workflow behind on every execution of the suite.
        with contextlib.suppress(Exception):  # best effort
            await handle.cancel()
        return False, f"expected completion after the event but got: {e}"

    if not json_matches(expect.get("result", {}), result):
        return False, (
            f"result mismatch\n    expected: {expect.get('result')}\n    actual:   {result}"
        )
    bounds = expect.get("result_at_most")
    if bounds:
        problem = _within_bounds(bounds, result)
        if problem:
            return False, f"{problem} (activations so far: {catalog.ACTIVATIONS.get(key)})"
    return True, ""


async def run_cancel_scenario(
    client: Client, args: argparse.Namespace, sc: dict
) -> tuple[bool, str]:
    """Start the workflow, cancel it once it has parked, and assert how it ended.

    ``expect.status`` is ``cancelled`` for a workflow that lets the cancellation
    it is told of end it, and ``completed``, with ``expect.result``, for one
    that cleans up and returns.
    """
    from orcher.types import WorkflowStatus

    expect = sc["expect"]
    key = f"conf-{uuid.uuid4()}"
    scenario_input = sc.get("input", {})
    payload = {**(scenario_input if isinstance(scenario_input, dict) else {}), "key": key}
    handle = await client.start_workflow(
        sc["workflow"], task_queue=args.task_queue, args=(payload,)
    )

    if not await _wait_until_parked(key, args.result_timeout_secs):
        with contextlib.suppress(Exception):  # best effort
            await handle.cancel()
        return False, "the workflow never parked"
    # As for an event: let the engine record the parking activation, so the
    # cancellation wakes a parked workflow rather than racing its first run.
    await asyncio.sleep(0.5)
    await handle.cancel()

    status = expect.get("status")
    if status == "cancelled":
        deadline = time.monotonic() + args.result_timeout_secs
        while True:
            current = await handle.status()
            if current == WorkflowStatus.CANCELLED:
                return True, ""
            if current != WorkflowStatus.RUNNING or time.monotonic() >= deadline:
                return False, f"expected the workflow cancelled, it is {current}"
            await asyncio.sleep(0.2)
    if status == "completed":
        try:
            result = await handle.result(timeout=args.result_timeout_secs)
        except Exception as e:
            return False, f"expected completion after cleanup but got: {e}"
        if not json_matches(expect.get("result", {}), result):
            return False, (
                f"result mismatch\n    expected: {expect.get('result')}\n    actual:   {result}"
            )
        return True, ""
    return False, f"unknown expected status '{status}' for a cancel scenario"


async def run_workflow_id_reuse_scenario(
    client: Client, args: argparse.Namespace, sc: dict
) -> tuple[bool, str]:
    """Start one workflow id again while it runs and after it completes.

    The start while it runs must be refused with ``expect.error_code``, naming
    the open run. Once that run completes, a start that reuses an id only after
    a failure is refused the same way, and a plain start runs the workflow
    again as a new run.
    """
    expect = sc["expect"]
    expected_code = expect.get("error_code")
    if not expected_code:
        return False, "workflow_id_reuse scenario missing `expect.error_code`"
    workflow_id = f"conf-reuse-{uuid.uuid4()}"

    async def start(policy: WorkflowIdReusePolicy | None = None) -> Any:
        return await client.start_workflow(
            sc["workflow"],
            workflow_id=workflow_id,
            task_queue=args.task_queue,
            args=(sc.get("input", {}),),
            id_reuse_policy=policy,
        )

    async def refused(
        policy: WorkflowIdReusePolicy | None, in_the_way: str, when: str
    ) -> str | None:
        """None when the start is refused with the code, naming the run."""
        try:
            handle = await start(policy)
        except BaseException as e:  # noqa: BLE001 - the scenario is about what is raised
            code = getattr(e, "code", None)
            code = getattr(code, "name", code)
            if code != expected_code:
                return f"{when}: expected {expected_code}, got {code} ({type(e).__name__}: {e})"
            if getattr(e, "run_id", None) != in_the_way:
                return (
                    f"{when}: the refusal does not name run {in_the_way}: "
                    f"run_id={getattr(e, 'run_id', None)!r}"
                )
            return None
        return f"{when}: the start was accepted as run {handle.run_id}"

    async def completes(handle: Any, which: str) -> str | None:
        try:
            result = await handle.result(timeout=args.result_timeout_secs)
        except Exception as e:
            return f"the {which} run did not complete: {e}"
        if not json_matches(expect.get("result", {}), result):
            return (
                f"the {which} run's result\n    expected: {expect.get('result')}\n"
                f"    actual:   {result}"
            )
        return None

    first = await start()

    problem = await refused(None, first.run_id, "a second start while it runs")
    if problem:
        return False, problem

    problem = await completes(first, "first")
    if problem:
        return False, problem

    problem = await refused(
        WorkflowIdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
        first.run_id,
        "reusing only after a failure, once it completed",
    )
    if problem:
        return False, problem

    again = await start(WorkflowIdReusePolicy.ALLOW_DUPLICATE)
    if again.run_id == first.run_id:
        return False, "the new start reported the first run's id"
    problem = await completes(again, "second")
    if problem:
        return False, problem
    return True, ""


async def run_engine_restart_scenario(
    client: Client, args: argparse.Namespace, sc: dict
) -> tuple[bool, str]:
    """Restart the engine, then check the untouched worker completes a new workflow.

    The engine is restarted with ``ORCHER_CONTRACT_RESTART_CMD``. The scenario
    guards poller recovery: after an engine restart, every workflow poller must
    resume, not just the worker's heartbeat. Skipped (reported, not failed) when
    the variable is unset, because the harness has no general way to restart an
    engine it did not start.
    """
    cmd = os.environ.get("ORCHER_CONTRACT_RESTART_CMD")
    if not cmd:
        # None, not True: a scenario that did not run asserts nothing, so it is
        # reported as skipped rather than counted as a pass.
        return None, "ORCHER_CONTRACT_RESTART_CMD is not set"
    expect = sc["expect"]

    completed = subprocess.run(cmd, shell=True, timeout=120)  # noqa: S602 - operator-supplied
    if completed.returncode != 0:
        return False, f"restart command failed with {completed.returncode}"
    # Let the worker notice and recover on its own. Recovery is the assertion,
    # so nothing here reconnects or restarts the worker.
    await asyncio.sleep(5)

    handle = await client.start_workflow(
        sc["workflow"],
        workflow_id=f"conf-restart-{uuid.uuid4()}",
        task_queue=args.task_queue,
        args=(sc.get("input", {}),),
    )
    try:
        result = await handle.result(timeout=args.result_timeout_secs)
    except Exception as e:
        return False, f"the worker never picked the workflow up after the engine restart: {e}"
    if json_matches(expect.get("result", {}), result):
        return True, ""
    return False, (f"result mismatch\n    expected: {expect.get('result')}\n    actual:   {result}")


def _spawn_worker(args: argparse.Namespace, task_queue: str, doomed: bool) -> subprocess.Popen:
    """Start a worker process of this harness that serves only ``task_queue``.

    When ``doomed`` is set, the crash task kills the process it runs in."""
    env = os.environ.copy()
    env.pop("ORCHER_CONTRACT_DOOMED_WORKER", None)
    if doomed:
        env["ORCHER_CONTRACT_DOOMED_WORKER"] = "1"
    return subprocess.Popen(  # noqa: S603 - this interpreter and this script
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--worker-only",
            "--server-url",
            args.server_url,
            "--namespace",
            args.namespace,
            "--task-queue",
            task_queue,
        ],
        stdin=subprocess.DEVNULL,
        env=env,
    )


async def run_worker_crash_scenario(
    client: Client, args: argparse.Namespace, sc: dict
) -> tuple[bool, str]:
    """Check that a task is retried on a new worker after its worker process dies.

    The workflow runs on a queue of its own, served by a worker process that its
    task kills. Once that process is gone, another is started, and the workflow
    must complete on it. The task sets no timeouts, so only the engine's default
    heartbeat timeout recovers the attempt that died with its worker; the engine
    gives that default because the worker heartbeats on its own. The wait allows
    for an engine at its defaults: a one-minute timeout and a thirty-second sweep.
    """
    task_queue = f"contract-crash-{uuid.uuid4()}"
    doomed = _spawn_worker(args, task_queue, True)
    handle = await client.start_workflow(
        sc["workflow"],
        workflow_id=f"conf-crash-{uuid.uuid4()}",
        task_queue=task_queue,
        args=(sc.get("input", {}),),
    )
    deadline = time.monotonic() + 30
    while doomed.poll() is None:
        if time.monotonic() > deadline:
            doomed.kill()
            return False, "the task never took its worker down"
        await asyncio.sleep(0.1)
    if doomed.returncode == 0:
        return False, "the first worker exited cleanly, not mid-task"

    survivor = _spawn_worker(args, task_queue, False)
    try:
        result = await handle.result(timeout=args.result_timeout_secs + 120)
    except Exception as e:
        return False, f"the task was never retried after its worker died: {e}"
    finally:
        survivor.kill()
        survivor.wait()
    expect = sc["expect"]
    if json_matches(expect.get("result", {}), result):
        return True, ""
    return False, (f"result mismatch\n    expected: {expect.get('result')}\n    actual:   {result}")


async def run_worker_only(args: argparse.Namespace) -> None:
    """Serve ``--task-queue`` until killed, running no scenarios."""
    worker = (
        Worker.builder()
        .server_url(args.server_url)
        .namespace(args.namespace)
        .task_queue(args.task_queue)
        .build()
    )
    await worker.run()


async def main() -> None:
    ap = argparse.ArgumentParser(description="ORCHER contract harness (Python worker + driver)")
    ap.add_argument(
        "--server-url", default=os.environ.get("ORCHER_SERVER_URL", "http://localhost:50051")
    )
    # A real namespace, not `default`. In `default`, a child workflow created in
    # the wrong namespace lands in the right one by coincidence, so the
    # child-workflow scenarios could not tell namespace propagation works.
    # Create it once: `orcher namespace create contract`.
    ap.add_argument("--namespace", default=os.environ.get("ORCHER_NAMESPACE", "contract"))
    ap.add_argument("--task-queue", default="contract")
    ap.add_argument("--scenarios", default=str(Path(__file__).parent / "scenarios.json"))
    ap.add_argument("--result-timeout-secs", type=float, default=30.0)
    # Run only a worker, no scenarios. The harness starts itself this way for a
    # `worker_crash` scenario, whose task takes its worker process down.
    ap.add_argument("--worker-only", action="store_true")
    args = ap.parse_args()

    if args.worker_only:
        await run_worker_only(args)
        return

    spec = json.loads(Path(args.scenarios).read_text())
    scenarios = spec["scenarios"]
    print(
        f"ORCHER contract — Python worker | {len(scenarios)} scenarios "
        f"| server {args.server_url}"
    )

    worker = (
        Worker.builder()
        .server_url(args.server_url)
        .namespace(args.namespace)
        .task_queue(args.task_queue)
        .build()
    )
    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(2)  # let the worker register and start polling

    # The workers read ORCHER_API_KEY through the builder; the client must use
    # the same key, so the suite runs unchanged against an engine that requires
    # authentication.
    client = Client(
        ClientConfig(
            server_url=args.server_url,
            namespace=args.namespace,
            api_key=os.environ.get("ORCHER_API_KEY") or None,
        )
    )
    await client.connect()

    passed = failed = skipped = 0
    for sc in scenarios:
        kind = sc.get("kind")
        if kind == "actor":
            ok, msg = await run_actor_scenario(worker, args, sc)
            label = sc["actor"]
        elif kind == "client_error":
            ok, msg = await run_client_error_scenario(client, sc)
            label = sc["client_call"]
        elif kind == "reset":
            ok, msg = await run_reset_scenario(client, args, sc)
            label = sc["workflow"]
        elif kind == "event":
            ok, msg = await run_event_scenario(client, args, sc)
            label = sc["workflow"]
        elif kind == "cancel":
            ok, msg = await run_cancel_scenario(client, args, sc)
            label = sc["workflow"]
        elif kind == "engine_restart":
            ok, msg = await run_engine_restart_scenario(client, args, sc)
            label = sc["workflow"]
        elif kind == "worker_crash":
            ok, msg = await run_worker_crash_scenario(client, args, sc)
            label = sc["workflow"]
        elif kind == "workflow_id_reuse":
            ok, msg = await run_workflow_id_reuse_scenario(client, args, sc)
            label = sc["workflow"]
        else:
            ok, msg = await run_scenario(client, args, sc)
            label = sc["workflow"]
        if ok is None:
            print(f"  SKIP  {sc['id']} ({label}): {msg}")
            skipped += 1
            continue
        gap = sc.get("known_gap")
        gap_sdks = sc.get("known_gap_sdks")
        if gap_sdks is not None and THIS_SDK not in gap_sdks:
            # The gap is closed here; the shared file keeps it open elsewhere.
            gap = None
        if gap:
            # Specified but not built: a failure is expected and does not fail
            # the run. A pass means the gap is closed and the flag should go.
            if ok:
                print(
                    f"  FAIL  {sc['id']} ({label}): passed but is marked known_gap "
                    f"— remove the flag: {gap}"
                )
                failed += 1
            else:
                print(f"  XFAIL {sc['id']} ({label}): known gap — {gap}")
                passed += 1
        elif ok:
            print(f"  PASS  {sc['id']} ({label}){': ' + msg if msg else ''}")
            passed += 1
        else:
            print(f"  FAIL  {sc['id']} ({label}): {msg}")
            failed += 1

    worker_task.cancel()
    print(f"\n{passed} passed, {failed} failed, {skipped} skipped, {len(scenarios)} total")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
