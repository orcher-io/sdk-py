#!/usr/bin/env bash
# Run the Python contract worker and driver against a running engine.
#
# The Orcher engine must already be listening on the gRPC port; this script
# does not start one.
#
# Requires the built native module (`maturin develop` or `pip install -e .`).
# Set PYTHON to use a different interpreter, such as a venv's.
set -euo pipefail

SERVER_URL="${ORCHER_SERVER_URL:-http://localhost:50051}"
PORT="${SERVER_URL##*:}"
PYTHON="${PYTHON:-python3}"

cd "$(dirname "$0")/.."

if ! nc -z localhost "$PORT" 2>/dev/null; then
  echo "error: no engine reachable on localhost:$PORT" >&2
  echo "       start one from the orcher repo: cargo run --release --bin orchestrator" >&2
  exit 2
fi

exec env PYTHONPATH="src:contract" "$PYTHON" contract/run.py --server-url "$SERVER_URL" "$@"
