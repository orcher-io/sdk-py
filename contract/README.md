# Contract suite

A live check that this SDK's worker produces correct durable behavior when it
runs against a real engine. It catches what unit tests, type checks and builds
cannot: a worker that silently runs nothing, reports a task failure wrongly, or
ignores an option.

The suite is shared across the Orcher SDKs. `scenarios.json` is a
language-independent contract, and the workflow catalog uses the same names in
the Rust, Python and TypeScript SDKs, so the same scenarios drive every worker.

## Layout

- `scenarios.json` — the shared contract. Each scenario names a catalog
  workflow, an `input`, and the `expect`ed terminal `status` and `result`.
  Matching is structural: a scenario asserts only the fields it lists, so
  SDK-specific extra fields are ignored.
- `catalog.py` — the workflows and tasks this worker serves. Each workflow
  returns a JSON result describing the durable behavior it observed.
- `run.py` — the driver. It starts the worker, runs every scenario against the
  engine, checks the results, and exits non-zero on any failure.
- `long_wait.py` — a standalone check that runs on its own because it takes
  over a minute (see below).

## Running

The suite runs in a namespace called `contract`, not `default`. Create it once
per engine:

```bash
orcher namespace create contract
```

A separate namespace makes the child-workflow scenarios meaningful: a child
must be created in its parent's namespace, and in `default` a child placed in
the default namespace by mistake would look correct. Set `ORCHER_NAMESPACE` to
use a different one.

Start the Orcher engine with durable task failures enabled
(`ORCHER_DURABLE_TASK_FAILURE=true`), listening for gRPC on port 50051.

The worker-liveness scenarios (`task-outlives-heartbeat-timeout-without-heartbeat-code`
and `task-retried-after-its-worker-dies`) need the engine to time out a silent
task within seconds rather than after its default of one minute. Start the
engine with these variables for them:

```bash
ORCHER_DEFAULT_HEARTBEAT_TIMEOUT_SECS=3
ORCHER_TIMEOUT_WATCHER_POLL_INTERVAL_SECS=1
ORCHER_TIMEOUT_WATCHER_GRACE_SECS=1
```

At the defaults the first scenario fails and the second takes a few minutes.

Then, with the native module built (`maturin develop` or `pip install -e .`):

```bash
./contract/run.sh
# or directly:
PYTHONPATH=src:contract python contract/run.py --server-url http://localhost:50051
```

### Long-wait check

`long_wait.py` checks that `handle.result()` waits for a workflow that runs
longer than the server's default 60-second wait. The client must send its own
deadline; without one, the server applies its default and the call fails while
the workflow is still healthy and running.

It is not part of `scenarios.json` because the workflow has to outlive the
server's default window to prove anything, so a real run takes over a minute:

```bash
# The real check: the workflow outlives the 60-second default window.
python contract/long_wait.py --sleep-secs 75

# A quick check that the harness itself works. It reports SMOKE and does not
# claim the long wait works.
python contract/long_wait.py --sleep-secs 5
```

## Differences between SDKs

- Each SDK implements the catalog in its own idiom; only the observable
  contract (input JSON to terminal status and result JSON) is shared. For
  example, Python passes a workflow's input object as keyword arguments, so
  `echo` wraps its value back into `{"message": ...}` to return the same shape
  as the other SDKs.

## Not yet covered

- Saga compensation and sub-second retry intervals.
- The suite does not run in CI; run it locally before changing the worker.
