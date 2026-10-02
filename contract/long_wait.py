"""Check that awaiting a workflow that outlives the server's default result window succeeds.

When a result request carries no deadline, the server applies its 60s default.
``handle.result()`` re-issues the bounded long-poll until the workflow is
terminal, so a wait of any length works. The ``timeout=`` argument can only
shorten the wait, never extend it.

Usage (against a running engine):
    python contract/long_wait.py --sleep-secs 75
"""

from __future__ import annotations

import argparse
import asyncio
import time
from typing import Any

from orcher import Client, ClientConfig, Worker, task, workflow
from orcher.task import TaskContext
from orcher.workflow import WorkflowContext

# The server applies this when a result request carries no deadline of its own.
# A run only proves anything if the workflow outlives it.
SERVER_DEFAULT_RESULT_WINDOW_SECS = 60


@task(name="py_long_task")
async def py_long_task(ctx: TaskContext, sleep_secs: int) -> dict:
    # Sleep inside the task so the workflow really stays running past the default
    # window, rather than merely being slow to dispatch.
    await asyncio.sleep(sleep_secs)
    return {"slept_secs": sleep_secs}


@workflow(name="py_long_workflow")
async def py_long_workflow(ctx: WorkflowContext, sleep_secs: Any) -> dict:
    return await ctx.execute_task(py_long_task, sleep_secs=sleep_secs)


async def main() -> int:
    ap = argparse.ArgumentParser(description="Await a workflow past the 60s default window")
    ap.add_argument("--server-url", default="http://localhost:50051")
    ap.add_argument("--namespace", default="default")
    ap.add_argument("--task-queue", default="py-long-wait")
    # Must exceed the server's DEFAULT_RESULT_TIMEOUT_SECS (60) to be a real check.
    ap.add_argument("--sleep-secs", type=int, default=75)
    args = ap.parse_args()

    exercises_regression = args.sleep_secs > SERVER_DEFAULT_RESULT_WINDOW_SECS
    mode = "REGRESSION" if exercises_regression else "SMOKE"
    print(
        f"py long-wait check [{mode}] | workflow sleeps {args.sleep_secs}s "
        f"(server default result window is {SERVER_DEFAULT_RESULT_WINDOW_SECS}s)"
    )
    if not exercises_regression:
        print(
            "  note: sleep does not exceed the default window, so this only checks "
            "the harness — it does NOT prove the cap is gone."
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

    client = Client(ClientConfig(server_url=args.server_url, namespace=args.namespace))
    await client.connect()

    started = time.monotonic()
    handle = await client.start_workflow(
        "py_long_workflow",
        task_queue=args.task_queue,
        args=(args.sleep_secs,),
    )

    # No explicit timeout: this must wait as long as the workflow runs instead of
    # being cut off at the server's 60s default.
    result = await handle.result()
    elapsed = time.monotonic() - started

    worker_task.cancel()
    await client.close()

    print(f"  result reported : {result}")
    print(f"  wall clock      : {elapsed:.1f}s")

    slept = result.get("slept_secs") if isinstance(result, dict) else None
    assert slept == args.sleep_secs, f"unexpected result payload: {result!r}"
    assert (
        elapsed >= args.sleep_secs
    ), f"returned too early ({elapsed:.1f}s) — the workflow cannot have completed"

    if exercises_regression:
        print(
            f"\nPY LONG-WAIT: PASS — awaited a {args.sleep_secs}s workflow past the "
            f"{SERVER_DEFAULT_RESULT_WINDOW_SECS}s default window"
        )
    else:
        print(
            f"\nPY LONG-WAIT: SMOKE PASS — harness works. Re-run with "
            f"--sleep-secs greater than {SERVER_DEFAULT_RESULT_WINDOW_SECS} to "
            "exercise the regression."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
