"""
Time Controller for deterministic workflow testing.

This module provides time manipulation capabilities for testing
workflows that use timers or time-dependent logic.
"""

from __future__ import annotations

import asyncio
import heapq
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass(order=True)
class PendingTimer:
    """A pending timer in the time controller."""

    fire_at: datetime
    timer_id: str = field(compare=False)
    callback: Callable[[], None] = field(compare=False)
    cancelled: bool = field(default=False, compare=False)


class TimeController:
    """
    Controller for manipulating time in tests.

    Provides deterministic time control for testing workflows that
    depend on time-based operations like timers and sleeps.

    Example:
        >>> controller = TimeController(datetime(2025, 1, 1))
        >>>
        >>> # Create a timer
        >>> timer_id = controller.create_timer(5000, lambda: print("Timer fired!"))
        >>>
        >>> # Advance time
        >>> await controller.advance(3000)  # Timer hasn't fired yet
        >>> await controller.advance(3000)  # Timer fires!
    """

    def __init__(self, initial_time: datetime | None = None) -> None:
        """
        Initialize the time controller.

        Args:
            initial_time: Starting time (defaults to now)
        """
        self._current_time = initial_time or datetime.now()
        self._initial_time = self._current_time
        self._timers: list[PendingTimer] = []
        self._timer_counter = 0

    def now(self) -> datetime:
        """
        Get current time in the test environment.

        Returns:
            Current simulated time
        """
        return self._current_time

    def set_time(self, time: datetime) -> None:
        """
        Set current time to a specific value.

        Note: This does not fire timers. Use advance() to fire timers.

        Args:
            time: New current time
        """
        self._current_time = time

    async def advance(self, ms: int) -> None:
        """
        Advance time by specified milliseconds.

        Fires all timers that expire during the advancement.

        Args:
            ms: Milliseconds to advance

        Example:
            >>> await controller.advance(5000)  # Advance 5 seconds
        """
        target_time = self._current_time + timedelta(milliseconds=ms)
        await self._advance_to(target_time)

    async def advance_to(self, target_time: datetime) -> None:
        """
        Advance time to a specific datetime.

        Fires all timers that expire before or at the target time.

        Args:
            target_time: Target datetime

        Example:
            >>> await controller.advance_to(datetime(2025, 1, 2))
        """
        if target_time < self._current_time:
            raise ValueError("Cannot advance time backwards")
        await self._advance_to(target_time)

    async def _advance_to(self, target_time: datetime) -> None:
        """Internal method to advance time and fire timers."""
        while self._timers and self._timers[0].fire_at <= target_time:
            timer = heapq.heappop(self._timers)

            if timer.cancelled:
                continue

            self._current_time = timer.fire_at

            try:
                result = timer.callback()
                if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                    await result  # type: ignore[arg-type]
            except Exception:
                # A failing callback is ignored so it cannot stop later timers from firing.
                pass

        self._current_time = target_time

    def create_timer(
        self,
        duration_ms: int,
        callback: Callable[[], None],
    ) -> str:
        """
        Create a timer that fires after specified duration.

        Args:
            duration_ms: Duration in milliseconds
            callback: Function to call when timer fires

        Returns:
            Timer ID for cancellation

        Example:
            >>> timer_id = controller.create_timer(5000, lambda: print("Done!"))
        """
        self._timer_counter += 1
        timer_id = f"timer-{self._timer_counter}"

        fire_at = self._current_time + timedelta(milliseconds=duration_ms)
        timer = PendingTimer(
            fire_at=fire_at,
            timer_id=timer_id,
            callback=callback,
        )

        heapq.heappush(self._timers, timer)
        return timer_id

    def cancel_timer(self, timer_id: str) -> bool:
        """
        Cancel a pending timer.

        Args:
            timer_id: Timer ID returned from create_timer

        Returns:
            True if timer was found and cancelled

        Example:
            >>> timer_id = controller.create_timer(5000, callback)
            >>> controller.cancel_timer(timer_id)
        """
        for timer in self._timers:
            if timer.timer_id == timer_id and not timer.cancelled:
                timer.cancelled = True
                return True
        return False

    def get_pending_timer_count(self) -> int:
        """
        Get number of pending (non-cancelled) timers.

        Returns:
            Number of pending timers
        """
        return sum(1 for t in self._timers if not t.cancelled)

    def get_next_timer_time(self) -> datetime | None:
        """
        Get fire time of next pending timer.

        Returns:
            Fire time of next timer, or None if no timers
        """
        return min((t.fire_at for t in self._timers if not t.cancelled), default=None)

    async def run_all_timers(self) -> None:
        """
        Run all pending timers immediately.

        Advances time to fire each timer in order.

        Example:
            >>> await controller.run_all_timers()
        """
        while self._timers:
            timer = self._timers[0]
            if timer.cancelled:
                heapq.heappop(self._timers)
                continue

            await self.advance_to(timer.fire_at)

    async def run_next_timer(self) -> bool:
        """
        Run the next pending timer.

        Returns:
            True if a timer was run

        Example:
            >>> ran = await controller.run_next_timer()
        """
        while self._timers:
            timer = self._timers[0]
            if timer.cancelled:
                heapq.heappop(self._timers)
                continue

            await self.advance_to(timer.fire_at)
            return True

        return False

    def clear_timers(self) -> None:
        """
        Clear all pending timers without firing them.

        Example:
            >>> controller.clear_timers()
        """
        self._timers.clear()

    def reset(self, initial_time: datetime | None = None) -> None:
        """
        Reset controller to initial state.

        Args:
            initial_time: New initial time (defaults to original initial time)

        Example:
            >>> controller.reset()
        """
        self._current_time = initial_time or self._initial_time
        self._timers.clear()
        self._timer_counter = 0

    def elapsed_ms(self) -> float:
        """
        Get elapsed time since initialization in milliseconds.

        Returns:
            Elapsed time in milliseconds
        """
        delta = self._current_time - self._initial_time
        return delta.total_seconds() * 1000
