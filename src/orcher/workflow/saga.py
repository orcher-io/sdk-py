"""Saga pattern support for the Orcher Python SDK.

Provides :class:`Saga`, an imperative, register-as-you-go helper for
orchestrating compensating transactions, like the Rust SDK's ``Saga``.
Each step has an *action* and a *compensation*; if a later step fails, the
compensations for previously completed steps run in reverse order (LIFO).

There are two ways to register a step, and the difference matters:

* :meth:`Saga.add_step` runs the action and registers its compensation in a
  single call. Use it when the compensation does **not** need the action's
  result.
* :meth:`Saga.add_compensation` registers a compensation for an action you ran
  yourself. Use it when the compensation needs data the action produced (an id,
  a token): you run the action, then register a compensation that closes over
  the result.

Example::

    from orcher import Saga

    async def book_trip(ctx, req):
        saga = Saga()
        try:
            # Fluent: the compensation is static (doesn't need the result).
            hotel = await saga.add_step(
                action=lambda: ctx.execute_task(book_hotel, req=req),
                compensation=lambda: ctx.execute_task(cancel_hotel, req=req),
            )

            # Manual: the compensation needs the action's output.
            flight = await ctx.execute_task(book_flight, req=req)
            saga.add_compensation(
                lambda: ctx.execute_task(cancel_flight, booking_id=flight["id"])
            )

            saga.commit()
            return {"hotel": hotel, "flight": flight}
        except Exception:
            await saga.compensate()
            raise
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

__all__ = ["Saga", "SagaBuilder", "SagaStep"]

_LOG = logging.getLogger("orcher.saga")


@dataclass
class SagaStep:
    """A registered compensation for a completed saga step."""

    name: str
    compensation: Callable[[], Awaitable[Any]]


class Saga:
    """Manages compensating transactions for a workflow.

    The saga tracks completed steps and their compensations. If a step fails,
    the compensations registered so far run in reverse order. State lives only
    in memory for the current execution. Compensations are ordinary tasks
    (``ctx.execute_task(...)``), so replay rebuilds the saga and any
    compensation that already ran returns its journaled result.
    """

    def __init__(self) -> None:
        self._compensations: list[SagaStep] = []
        self._completed = False
        self._steps_executed = 0

    async def add_step(
        self,
        action: Callable[[], Awaitable[Any]],
        compensation: Callable[[], Awaitable[Any]] | None = None,
        *,
        name: str | None = None,
    ) -> Any:
        """Run ``action`` and, on success, register ``compensation``.

        Returns the action's result. If the action raises a genuine error, all
        previously registered compensations run in reverse order and the
        original error is re-raised.

        Durable suspension (``WorkflowSuspendedError``) is control flow, not a
        failure: it subclasses ``BaseException`` and so passes through the
        ``except Exception`` below untouched, letting the executor schedule the
        task and replay. Compensating on suspension would undo prior successful
        steps on a perfectly healthy workflow.
        """
        step_name = name or f"step_{self._steps_executed + 1}"
        try:
            result = await action()
        except Exception:
            await self.compensate()
            raise
        self._steps_executed += 1
        if compensation is not None:
            self._compensations.append(SagaStep(name=step_name, compensation=compensation))
        return result

    def add_compensation(
        self,
        compensation: Callable[[], Awaitable[Any]],
        *,
        name: str | None = None,
    ) -> None:
        """Register a compensation for an action you executed yourself.

        Use this when the compensation needs data produced by the action (an
        id, a booking reference, a token) that only exists after it runs::

            charge = await ctx.execute_task(charge_card, amount=100)
            saga.add_compensation(
                lambda: ctx.execute_task(refund_card, charge_id=charge["id"])
            )
        """
        self._steps_executed += 1
        step_name = name or f"step_{self._steps_executed}"
        self._compensations.append(SagaStep(name=step_name, compensation=compensation))

    async def compensate(self) -> int:
        """Run all registered compensations in reverse order (LIFO).

        Best-effort: if a compensation raises a real error it is logged and the
        remaining compensations still run. Idempotent: once the saga has
        completed (committed or compensated) this is a no-op, so it is safe to
        call from a ``finally``/``except`` even if a step already triggered it.

        Returns the number of compensations that ran successfully. Durable
        suspension propagates so the executor can schedule the compensation task
        and replay.
        """
        if self._completed:
            return 0
        self._completed = True

        count = 0
        for step in reversed(self._compensations):
            try:
                await step.compensation()
                count += 1
            except Exception:
                _LOG.warning("Saga compensation failed for %r", step.name, exc_info=True)
        return count

    def commit(self) -> None:
        """Mark the saga as successfully completed so no compensations run."""
        self._completed = True

    @property
    def is_completed(self) -> bool:
        """Whether the saga has completed (committed or compensated)."""
        return self._completed

    @property
    def steps_executed(self) -> int:
        """Number of steps registered on this saga."""
        return self._steps_executed

    @property
    def pending_compensations(self) -> int:
        """Number of compensations that would run if the saga fails now."""
        return len(self._compensations)


# Alias of ``Saga``, kept so code that imports ``SagaBuilder`` keeps working.
SagaBuilder = Saga
