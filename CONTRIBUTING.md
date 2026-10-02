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

To build against local checkouts of `sdk-core` and `protos` instead of the
crates.io releases in `Cargo.toml`, copy `.cargo/config.example.toml` to
`.cargo/config.toml`.

## Checks

Pull requests must pass the same checks CI runs:

```bash
ruff check .
python -m pytest -rs
```

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
