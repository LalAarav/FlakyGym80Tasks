"""Rolling time windows of integer counters on the monotonic clock.

A ``RollingCounter`` divides monotonic time into fixed slots of ``slot_seconds``,
anchored at the clock reading taken when the counter was created. It remembers the
``slots`` most recent slots, the current one included, so the window it reports on
always ends "now" and is ``slots * slot_seconds`` long, give or take the part of the
current slot that has not happened yet.

Each slot holds a small fixed-width vector of integers (``width`` fields), so one
window can count related quantities together, for example total requests and failed
requests, and report both for exactly the same span of time.

The ring of slots is advanced lazily, on the first add or read after time has moved
on. That keeps idle counters free, but it means one advance may have to step across an
arbitrarily long gap: a counter that nobody touched for an hour, or a fake clock in a
test that jumps forward several windows at once.
"""

from __future__ import annotations

import math
import threading
from typing import Sequence

from src.clock import SystemClock


class RollingCounter:
    """Thread-safe rolling window of ``slots`` slots, each ``width`` integers wide.

    Slot ``k`` covers monotonic times ``origin + k * slot_seconds <= t <
    origin + (k + 1) * slot_seconds``, where ``origin`` is the clock reading at
    construction; slot boundaries never move. The window at time ``t`` is the slot
    containing ``t`` and the ``slots - 1`` slots before it.
    """

    def __init__(self, slots: int, slot_seconds: float, clock: SystemClock | None = None,
                 width: int = 1) -> None:
        if isinstance(slots, bool) or not isinstance(slots, int) or slots < 1:
            raise ValueError(f"slots must be a positive integer, got {slots!r}")
        if isinstance(width, bool) or not isinstance(width, int) or width < 1:
            raise ValueError(f"width must be a positive integer, got {width!r}")
        if not (isinstance(slot_seconds, (int, float)) and math.isfinite(slot_seconds)
                and slot_seconds > 0):
            raise ValueError(f"slot_seconds must be a positive finite number, got {slot_seconds!r}")
        self.clock = clock if clock is not None else SystemClock()
        self.slot_seconds = float(slot_seconds)
        self.width = width
        self._n = slots
        self._lock = threading.Lock()
        self._origin = self.clock.monotonic()
        self._cells: list[list[int]] = [self._blank() for _ in range(self._n)]
        self._head = 0  # index of the newest slot the ring has been advanced to

    @property
    def slots(self) -> int:
        return self._n

    @property
    def window_seconds(self) -> float:
        return self._n * self.slot_seconds

    def _blank(self) -> list[int]:
        return [0] * self.width

    def _index(self, now: float) -> int:
        """Absolute slot index of monotonic time ``now``."""
        return math.floor((now - self._origin) / self.slot_seconds)

    def _advance(self, now: float) -> int:
        """Move the ring forward to the slot containing ``now``; return its index.

        Every slot the head passes over is a new slot and starts empty. A reading that
        lies in an older slot than the head (a caller passing a stale ``now``) is
        counted in the head slot: the window never moves backwards.
        """
        index = self._index(now)
        behind = index - self._head
        if behind > 0:
            # Clear the cells of the slots being entered. Whole laps of the ring land
            # back on the same cell, so only the remainder moves the head.
            steps = behind % self._n
            for k in range(index - steps + 1, index + 1):
                self._cells[k % self._n] = self._blank()
            self._head = index
        return max(index, self._head)

    def _check_amounts(self, amounts: Sequence[int]) -> None:
        if len(amounts) != self.width:
            raise ValueError(f"expected {self.width} amount(s), got {len(amounts)}")
        for amount in amounts:
            if isinstance(amount, bool) or not isinstance(amount, int):
                raise TypeError(f"amounts must be integers, got {amount!r}")

    def add(self, *amounts: int, now: float | None = None) -> None:
        """Add ``amounts`` (one per field) to the slot containing ``now``.

        ``now`` defaults to the counter's clock. Callers that combine several windows
        pass one reading to all of them so they agree on the instant.
        """
        if not amounts and self.width == 1:
            amounts = (1,)
        self._check_amounts(amounts)
        at = self.clock.monotonic() if now is None else now
        with self._lock:
            index = self._advance(at)
            cell = self._cells[index % self._n]
            for field, amount in enumerate(amounts):
                cell[field] += amount

    def totals(self, now: float | None = None) -> tuple[int, ...]:
        """Per-field sums over the window ending at ``now``."""
        at = self.clock.monotonic() if now is None else now
        with self._lock:
            self._advance(at)
            sums = [0] * self.width
            for cell in self._cells:
                for field, amount in enumerate(cell):
                    sums[field] += amount
            return tuple(sums)

    def total(self, now: float | None = None) -> int:
        """Sum of field 0 over the window."""
        return self.totals(now)[0]

    def per_slot(self, now: float | None = None) -> list[tuple[int, ...]]:
        """The window's slots from oldest to newest (the current slot last)."""
        at = self.clock.monotonic() if now is None else now
        with self._lock:
            head = self._advance(at)
            return [tuple(self._cells[k % self._n]) for k in range(head - self._n + 1, head + 1)]

    def slot_start(self, now: float | None = None) -> float:
        """Monotonic time at which the slot containing ``now`` began."""
        at = self.clock.monotonic() if now is None else now
        with self._lock:
            head = self._advance(at)
            return self._origin + head * self.slot_seconds
