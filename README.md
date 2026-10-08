<p>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/orcher-io/sdk-py/main/assets/banner.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/orcher-io/sdk-py/main/assets/banner-light.svg">
    <img alt="ORCHER Python SDK" src="https://raw.githubusercontent.com/orcher-io/sdk-py/main/assets/banner.svg" width="100%">
  </picture>
</p>

<p align="center"><sub>Crash-proof workflows, written as plain async Python.</sub></p>

<br />

<div>
  <a href="https://pypi.org/project/orcher-sdk/"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fpypi.org%2Fpypi%2Forcher-sdk%2Fjson&query=%24.info.version&prefix=v&style=flat-square&labelColor=0a0a0a&color=04B385&logo=pypi&logoColor=white&label=pypi&cacheSeconds=600" alt="PyPI"></a>
  <a href="https://pypi.org/project/orcher-sdk/"><img src="https://img.shields.io/pypi/pyversions/orcher-sdk?style=flat-square&labelColor=0a0a0a&color=38BDF0&logo=python&logoColor=white" alt="Python versions"></a>
  <a href="https://github.com/orcher-io/sdk-py/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/orcher-io/sdk-py/ci.yml?branch=main&style=flat-square&labelColor=0a0a0a&color=04B385&logo=github&logoColor=white&label=CI" alt="CI"></a>
  <a href="https://github.com/orcher-io/sdk-py/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-38BDF0?style=flat-square&labelColor=0a0a0a" alt="Apache 2.0"></a>
</div>

<br />

Write async functions; ORCHER journals each step and resumes interrupted runs where they stopped.

- <img height="14" src="https://octicons-col.vercel.app/sync/38BDF0"> **Durable**: every step journaled; crashed runs resume on another worker
- <img height="14" src="https://octicons-col.vercel.app/code/38BDF0"> **Plain Python**: `@workflow` and `@task` on async functions, no DSL
- <img height="14" src="https://octicons-col.vercel.app/history/38BDF0"> **Deterministic replay**: step ids, time and randomness stay the same on every replay
- <img height="14" src="https://octicons-col.vercel.app/iterations/38BDF0"> **Smart retries**: per-task policies and never-retry error types
- <img height="14" src="https://octicons-col.vercel.app/clock/38BDF0"> **Timers and events**: sleep for days or wait for an outside event
- <img height="14" src="https://octicons-col.vercel.app/git-branch/38BDF0"> **Child workflows**: compose and run workflows in parallel
- <img height="14" src="https://octicons-col.vercel.app/database/38BDF0"> **Actors**: stateful objects with a single writer per key
- <img height="14" src="https://octicons-col.vercel.app/beaker/38BDF0"> **Testing**: run workflows in memory with mocks and a fake clock
- <img height="14" src="https://octicons-col.vercel.app/shield-lock/38BDF0"> **Multi-tenant**: API keys, organizations and namespaces built in
- <img height="14" src="https://octicons-col.vercel.app/cpu/38BDF0"> **Native core**: the engine protocol and state machine run in Rust

<br />

### <img height="16" src="https://octicons-col.vercel.app/download/38BDF0"> Install

```bash
pip install orcher-sdk
```

The package installs as `orcher`:

```python
import orcher
```

> [!NOTE]
> You need Python 3.11 or later and an ORCHER engine to run workflows against.
> pip installs a prebuilt wheel for macOS (arm64 and x86_64) and Linux (x86_64
> and aarch64, glibc or musl); anywhere else it builds the native core from
> source, which needs a Rust toolchain. The SDK is pre-1.0: the API may change
> between minor releases, and every breaking change is listed in
> [CHANGELOG.md](https://github.com/orcher-io/sdk-py/blob/main/CHANGELOG.md).

<br />

### <img height="16" src="https://octicons-col.vercel.app/play/38BDF0"> Quick start

Decorate a task and a workflow that calls it. Both register themselves when
their module is imported, and the worker runs everything registered:

```python
import asyncio
import os

from orcher import TaskContext, Worker, WorkflowContext, task, workflow


@task(name="send-confirmation", retry=3)
async def send_confirmation(ctx: TaskContext, order_id: str, email: str) -> str:
    return f"sent confirmation for {order_id} to {email}"


@workflow(name="confirm-order")
async def confirm_order(ctx: WorkflowContext, order_id: str, email: str) -> str:
    return await ctx.execute_task(send_confirmation, order_id=order_id, email=email)


async def main() -> None:
    worker = (
        Worker.builder()
        .server_url("http://localhost:50051")
        .namespace("default")
        .task_queue("orders")
        .api_key(os.environ.get("ORCHER_API_KEY"))  # for an engine that requires one
        .build()
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

Tasks that need clients or connections can be methods of a `@tasks` class
instead; hand the worker a built instance with `worker.register_task_instance()`.

Start it from any process and wait for the result. A dict input reaches the
workflow as keyword arguments:

```python
import os

from orcher import Client, ClientConfig

config = ClientConfig(
    server_url="http://localhost:50051",
    api_key=os.environ.get("ORCHER_API_KEY"),  # for an engine that requires one
)
async with Client(config) as client:
    handle = await client.start_workflow(
        "confirm-order",
        task_queue="orders",
        args=({"order_id": "order-1", "email": "ada@example.com"},),
    )
    receipt = await handle.result()
```

> [!TIP]
> Workflow code is replayed from its history every time it resumes, so it must
> be deterministic. Do I/O and anything else that touches the outside world in
> tasks, and use `ctx.time`, `ctx.random` and `ctx.sleep()` in workflows.

<br />

### <img height="16" src="https://octicons-col.vercel.app/book/38BDF0"> Guide

<details>
<summary><b>Timers</b>: sleep for minutes or months</summary>

<br />

A timer is recorded by the engine, so no worker is busy while it runs, and a
restart doesn't reset it:

```python
from datetime import timedelta

from orcher import WorkflowContext, workflow


@workflow(name="trial")
async def trial(ctx: WorkflowContext, email: str) -> None:
    await ctx.sleep(timedelta(days=14))
    await ctx.execute_task(send_trial_ended, email=email)
```

</details>

<details>
<summary><b>Events</b>: wait for something outside the workflow</summary>

<br />

A workflow can park until a named event arrives, optionally with a deadline:

```python
from datetime import timedelta

from orcher import WorkflowContext, workflow


@workflow(name="approval")
async def approval(ctx: WorkflowContext, request_id: str) -> str:
    try:
        approved = await ctx.wait_for_event_with_timeout("approved", timedelta(days=3))
    except TimeoutError:
        return f"{request_id} expired"
    return f"{request_id} approved" if approved else f"{request_id} rejected"
```

Send the event from a client:

```python
handle = await client.get_workflow("approval-42")
await handle.send_event("approved", True)
```

Cancel it the same way, optionally limiting how long its cleanup may take before the engine terminates it. Engines from before cancellation cleanup ignore the limit and cancel at once:

```python
await handle.cancel(cleanup_timeout=timedelta(seconds=30))
```

</details>

<details>
<summary><b>Child workflows</b>: compose workflows from workflows</summary>

<br />

```python
from orcher import WorkflowContext, workflow


@workflow(name="ship-order")
async def ship_order(ctx: WorkflowContext, order_id: str) -> str:
    label = await ctx.execute_child_workflow("print-label", args=(order_id,))
    return f"{order_id} shipped with {label}"
```

`ctx.start_child_workflow()` returns a handle instead, to run several children
at once and collect their results later.

</details>

<details>
<summary><b>Retries</b>: decide which failures are worth retrying</summary>

<br />

A failure's type is the exception's class name (`CardDeclined`, not
`payments.CardDeclined`). List the types never to retry in the task's policy.
An exception whose `non_retryable` attribute is true is never retried,
whatever the policy allows:

```python
from orcher import RetryPolicy, TaskContext, task


class CardDeclined(Exception):
    pass


class AccountClosed(Exception):
    non_retryable = True


@task(
    name="charge",
    retry_policy=RetryPolicy(max_attempts=5, non_retryable_error_types=["CardDeclined"]),
)
async def charge(ctx: TaskContext, order_id: str) -> str:
    # Runs once:
    raise CardDeclined(f"card declined for {order_id}")
    # Also runs once, listed or not:
    # raise AccountClosed(f"account closed for {order_id}")
```

Any other exception is retried. `non_retryable` can also be set on a single
instance before it is raised.

</details>

<details>
<summary><b>Large payloads</b>: what fits, and what happens when it doesn't</summary>

<br />

Workers and clients send and receive gRPC messages of up to 32 MiB; set
`ORCHER_MAX_MESSAGE_BYTES` to change that. The engine accepts one payload (an
input, a result, an event) of up to 8 MiB unless configured otherwise. A task
whose result is too large fails straight away with a `PayloadTooLarge` failure
that says how large it was, and is not retried: store large data elsewhere and
pass a reference.

</details>

<details>
<summary><b>Time and randomness</b>: the replay-safe way</summary>

<br />

`ctx.time` reads the time the engine recorded, and `ctx.random` is seeded from
the run, so both give the same answer every time the workflow is replayed:

```python
from orcher import WorkflowContext, workflow


@workflow(name="invoice")
async def invoice(ctx: WorkflowContext, customer: str) -> str:
    number = ctx.random.uuid()
    issued_at = ctx.time.now()
    return f"invoice {number} for {customer}, issued at {issued_at.isoformat()}"
```

</details>

<details>
<summary><b>Testing</b>: run workflows in memory</summary>

<br />

`orcher.testing` runs a workflow without an engine, with its tasks mocked by
name. With pytest and pytest-asyncio:

```python
import pytest

from orcher.testing import TestWorkflowEnvironment

from orders import confirm_order


@pytest.mark.asyncio
async def test_confirms_the_order() -> None:
    env = TestWorkflowEnvironment()
    env.mock_task("send-confirmation").returns("sent")

    receipt = await env.execute_workflow(
        confirm_order, {"order_id": "order-1", "email": "ada@example.com"}
    )

    assert receipt == "sent"
    env.assert_task_called_with(
        "send-confirmation", {"order_id": "order-1", "email": "ada@example.com"}
    )
```

Mocks can also raise, return a sequence or compute their result, and the
environment controls the clock.

</details>

<br />

### <img height="16" src="https://octicons-col.vercel.app/heart/38BDF0"> Contributing

Issues and pull requests are welcome. See
[CONTRIBUTING.md](https://github.com/orcher-io/sdk-py/blob/main/CONTRIBUTING.md)
for how to build, test and propose a change.

### <img height="16" src="https://octicons-col.vercel.app/law/38BDF0"> License

Licensed under the [Apache License, Version 2.0](https://github.com/orcher-io/sdk-py/blob/main/LICENSE).

<sub>"Python" and the Python logos are trademarks or registered trademarks of the Python Software Foundation, shown here to indicate the language this SDK is for.</sub>
