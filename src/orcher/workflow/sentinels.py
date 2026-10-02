"""Internal replay sentinels for terminal task / child-workflow failures.

When the engine replays a terminally failed task (or child workflow), the worker
injects a small marker dict into the replay cache instead of a success value. The
workflow context detects the marker on replay and raises a catchable error. These
keys match the Rust and TypeScript SDKs and MUST match between the inject side
(``worker``) and the decode side (``workflow.context`` / ``workflow.child``).
"""

# Marks an injected replay result as a terminal task/step failure.
TASK_FAILED_SENTINEL = "__orcher_task_failed__"

# Marks an injected replay result as a terminal child-workflow failure.
CHILD_FAILED_SENTINEL = "__orcher_child_failed__"
