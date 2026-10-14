# Contributing

Thanks for your interest in the Orcher Python SDK. Bug reports, fixes and
improvements are welcome.

## Reporting a problem

Open an issue with the SDK version, your Python version, what you ran, and
what happened. A short script that reproduces the problem helps the most.

## Development setup

You need Python 3.11 or later and a stable Rust toolchain.

```bash
python -m venv .venv
source .venv/bin/activate
pip install "maturin>=1.4,<2.0"
maturin develop --extras dev   # builds the native extension into the venv
```

Rerun `maturin develop` after changing anything under `native/`.

The native module binds the Rust SDK core (`orcher-sdk-core`) and the worker
host shared with the other SDKs (`orcher-sdk-host`, in the `sdk-core`
repository). The worker pulls workflow activations, tasks and actor
operations from the host; workflow activations cross as the kernel
protocol's messages (`src/orcher/worker/activation.py`).

To build against local checkouts of `sdk-core` (core and host) and `protos`
instead of the crates.io releases in `Cargo.toml`, copy
`.cargo/config.example.toml` to `.cargo/config.toml`.

## Checks

Pull requests must pass the same checks CI runs:

```bash
ruff check .
python -m pytest -rs
PYTHONPATH=src:contract python contract/replay_check.py
```

The replay check runs recorded workflow activations through the worker
without an engine and compares what it answers with committed expected
results; a change to what the worker sends shows up there first. See
[contract/README.md](contract/README.md).

Unit tests do not need an engine. The suite in `contract/` does: it runs a
real worker against a running engine and checks the durable behavior end to
end. Run it for changes to the worker, the workflow context or the native
extension. See [contract/README.md](contract/README.md).

## Pull requests

- Keep each pull request to one change, with tests for new behavior and for
  bugs you fix.
- Write commit messages as [Conventional Commits](https://www.conventionalcommits.org)
  (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `ci:`, `chore:`). Releases
  and the changelog are generated from them.
- Update docstrings and the README when you change public behavior.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0, the license of this project.
