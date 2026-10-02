"""Worker-side session management.

The ``SessionManager`` runs on each worker and handles:
- Session slot accounting, which caps how many sessions a worker hosts
- Session queue registration, which tells the TaskDriver which queues to poll
- Built-in task handlers for ``__orcher_create_session`` and ``__orcher_complete_session``
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from orcher.workflow.session import (
    CreateSessionInput,
    SessionState,
    build_session_queue,
)

logger = logging.getLogger(__name__)


@dataclass
class SessionEntry:
    """Metadata for an active session on this worker."""

    session_id: str
    session_queue: str
    original_queue: str


class SessionManager:
    """Worker-side session manager.

    Each worker has one ``SessionManager`` that controls how many concurrent
    sessions it can host. When a session is created, the manager:

    1. Takes a slot, failing immediately if none is available
    2. Generates a unique session queue name
    3. Notifies the TaskDriver to start polling the session queue
    4. Returns :class:`SessionInfo` as the task result
    """

    def __init__(self, max_sessions: int, bridge_worker: Any) -> None:
        self._resource_id = str(uuid.uuid4())
        self._max_sessions = max_sessions
        self._available_slots = max_sessions
        self._active_sessions: dict[str, SessionEntry] = {}
        self._bridge_worker = bridge_worker

        logger.info(
            "SessionManager created: resource_id=%s, max_sessions=%d",
            self._resource_id,
            max_sessions,
        )

    @property
    def resource_id(self) -> str:
        return self._resource_id

    @property
    def active_session_count(self) -> int:
        return len(self._active_sessions)

    @property
    def available_slots(self) -> int:
        return self._available_slots

    async def handle_create_session(
        self, raw_input: dict[str, Any], task_queue: str
    ) -> dict[str, Any]:
        """Handle the ``__orcher_create_session`` internal task.

        Returns a dict matching SessionInfo, serialized back to the workflow as JSON.

        Raises:
            RuntimeError: If every session slot is in use.
        """
        inp = CreateSessionInput(
            session_id=raw_input.get("session_id", ""),
            creation_timeout_ms=raw_input.get("creation_timeout_ms", 30000),
            execution_timeout_ms=raw_input.get("execution_timeout_ms", 600000),
            max_concurrent_tasks=raw_input.get("max_concurrent_tasks", 1),
            heartbeat_interval_ms=raw_input.get("heartbeat_interval_ms", 5000),
        )

        # Fail fast rather than wait for a slot to free up.
        if self._available_slots <= 0:
            raise RuntimeError(
                f"No session slots available ({self._max_sessions}/{self._max_sessions} in use)"
            )

        self._available_slots -= 1
        session_queue = build_session_queue(task_queue, self._resource_id)

        entry = SessionEntry(
            session_id=inp.session_id,
            session_queue=session_queue,
            original_queue=task_queue,
        )
        self._active_sessions[inp.session_id] = entry

        # Tell the TaskDriver to start polling the session queue.
        self._bridge_worker.add_session_queue(session_queue)

        logger.info(
            "Session created: session_id=%s, queue=%s, resource_id=%s, available_slots=%d",
            inp.session_id,
            session_queue,
            self._resource_id,
            self._available_slots,
        )

        return {
            "session_id": inp.session_id,
            "session_queue": session_queue,
            "worker_identity": self._resource_id,
            "state": SessionState.OPEN.value,
        }

    async def handle_complete_session(self, raw_input: dict[str, Any]) -> dict[str, Any]:
        """Handle the ``__orcher_complete_session`` internal task.

        Completing an unknown session is logged and otherwise ignored.
        """
        session_id = raw_input.get("session_id", "")

        if session_id in self._active_sessions:
            entry = self._active_sessions.pop(session_id)
            self._available_slots += 1

            # Tell the TaskDriver to stop polling the session queue.
            self._bridge_worker.remove_session_queue(entry.session_queue)

            logger.info(
                "Session completed: session_id=%s, queue=%s, available_slots=%d",
                session_id,
                entry.session_queue,
                self._available_slots,
            )
        else:
            logger.warning("Attempted to complete unknown session: %s", session_id)

        return {}

    def close_all_sessions(self) -> None:
        """Force-close all active sessions (e.g., during worker shutdown)."""
        sessions = list(self._active_sessions.values())
        for entry in sessions:
            self._bridge_worker.remove_session_queue(entry.session_queue)

        count = len(sessions)
        self._active_sessions.clear()
        self._available_slots = self._max_sessions

        if count > 0:
            logger.info("Force-closed %d active sessions during shutdown", count)
