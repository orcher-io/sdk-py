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
- `replay_check.py` — an engine-free replay of recorded activations against
  committed expected results (`replay/`), described below.

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

The worker-liveness scenarios (`task-outlives-heartbeat-timeout-without-heartbeat-code`,
`task-retried-after-its-worker-dies` and
`task-timeout-on-last-attempt-reaches-workflow`) need the engine to time out a silent
task within seconds rather than after its default of one minute. Start the
engine with these variables for them:

```bash
ORCHER_DEFAULT_HEARTBEAT_TIMEOUT_SECS=3
ORCHER_TIMEOUT_WATCHER_POLL_INTERVAL_SECS=1
ORCHER_TIMEOUT_WATCHER_GRACE_SECS=1
```

At the defaults the first scenario fails and the second takes a few minutes.

Against an engine that requires an API key, set `ORCHER_API_KEY`: the workers
and the client both send it.

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

## Replay check

`replay/<corpus>/` holds workflow activations recorded from this catalog
(`<workflow>/<run>/<n>-activation.pb`, each an encoded
`orcher.v1.PollWorkflowExecutionResponse`) and, in `expected.json`, what the
SDK answered each with, as sdk-core reads it before sending it to the engine.
`replay_check.py` replays every activation and fails on any difference. It
needs no engine:

```bash
PYTHONPATH=src:contract python contract/replay_check.py
```

Each activation is resolved by the kernel into a `RunActivation`, answered by
`Worker.run_activation`, and the answer turned back by the kernel, as the
worker host does on a live worker. The expected results were recorded while
the worker still had its journal-reading path as well, and both paths agreed
on every activation, so the corpus stands in for that path now that it is
gone.

A change meant to alter what the SDK sends updates the expected results with
`--bless`. Review the diff of `expected.json` like code.

To record a corpus, run the suite with `ORCHER_RECORD_ACTIVATIONS` set to an
empty directory; the worker writes every activation it receives and every
answer it gives there. Commit only the `*-activation.pb` files: the answers
carry the worker's identity, which names the machine.

## Known gaps of the kernel protocol (sdk-core 0.15)

- A failed workflow's traceback and the steps it reached do not reach the
  engine. A workflow whose code no longer replays its history is reported as
  non-determinism, so the engine holds the run rather than failing it.

## Differences between SDKs

- Each SDK implements the catalog in its own idiom; only the observable
  contract (input JSON to terminal status and result JSON) is shared. For
  example, Python passes a workflow's input object as keyword arguments, so
  `echo` wraps its value back into `{"message": ...}` to return the same shape
  as the other SDKs.

## Not yet covered

- Saga compensation and sub-second retry intervals.
- The suite does not run in CI; run it locally before changing the worker.
