"""Multi-window burn-rate alerting for a success-ratio SLO.

An SLO says that at least ``objective`` percent of events succeed. The *error budget* is
the remaining fraction, ``1 - objective / 100``. Over a window, the *burn rate* is the
observed error ratio divided by the budget: a burn rate of 1 spends the budget exactly
as fast as the SLO allows, 14.4 spends a 30-day budget in about two days.

``BurnRateAlert`` watches two rolling windows of the same events, a long one (is this
real?) and a short one (is it still happening?). It fires when both burn at least
``fire_at``, and once firing it clears only when the short window's burn rate has
dropped below ``clear_at``. The gap between the two thresholds is the hysteresis band
that stops an alert hovering near one threshold from flapping.

All threshold arithmetic is exact: objectives and thresholds are given as integers,
decimal strings or ``Fraction`` (never ``float``) and burn rates are compared as
fractions, so a window that burns at exactly a threshold is unambiguously at it.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from fractions import Fraction
from typing import Callable, Union

from src.clock import SystemClock
from src.metrics.window import RollingCounter

Exact = Union[int, str, Fraction]

FIRE = "fire"
CLEAR = "clear"


def _exact(value: Exact, what: str) -> Fraction:
    if isinstance(value, bool) or not isinstance(value, (int, str, Fraction)):
        raise TypeError(f"{what} must be an int, a decimal string or a Fraction, not {value!r}")
    try:
        return Fraction(value)
    except ValueError:
        raise ValueError(f"{what} is not a number: {value!r}") from None


@dataclass(frozen=True)
class Transition:
    """One change of alert state."""

    seq: int
    at: float
    kind: str  # FIRE or CLEAR
    long_burn: Fraction | None
    short_burn: Fraction | None


@dataclass(frozen=True)
class AlertStatus:
    """Result of one evaluation. A burn rate is ``None`` when its window has fewer
    than ``min_events`` events and so gives no verdict."""

    firing: bool
    at: float
    long_burn: Fraction | None
    short_burn: Fraction | None
    transition: Transition | None


class BurnRateAlert:
    """Two-window burn-rate alert with hysteresis.

    Both windows share ``slot_seconds`` and are fed the same events. ``record`` and
    ``evaluate`` may be called from any threads. ``on_transition`` (optional) is called
    once per state change, in the order the changes happened and never concurrently
    with itself.
    """

    def __init__(self, objective: Exact, *, fire_at: Exact, clear_at: Exact,
                 long_slots: int, short_slots: int, slot_seconds: float,
                 min_events: int = 1, clock: SystemClock | None = None,
                 on_transition: Callable[[Transition], None] | None = None) -> None:
        self.objective = _exact(objective, "objective")
        if not 0 < self.objective < 100:
            raise ValueError("objective must be a percentage strictly between 0 and 100")
        self.fire_at = _exact(fire_at, "fire_at")
        self.clear_at = _exact(clear_at, "clear_at")
        if not 0 < self.clear_at <= self.fire_at:
            raise ValueError("thresholds must satisfy 0 < clear_at <= fire_at")
        if isinstance(min_events, bool) or not isinstance(min_events, int) or min_events < 1:
            raise ValueError("min_events must be a positive integer")
        if short_slots > long_slots:
            raise ValueError("the short window cannot be longer than the long window")
        self.min_events = min_events
        self._budget = 1 - self.objective / 100
        self.clock = clock if clock is not None else SystemClock()
        self._long = RollingCounter(long_slots, slot_seconds, self.clock, width=2)
        self._short = RollingCounter(short_slots, slot_seconds, self.clock, width=2)
        self._on_transition = on_transition
        self._lock = threading.Lock()       # windows + state
        self._eval_lock = threading.Lock()  # one evaluation (and its notification) at a time
        self._firing = False
        self._transitions: list[Transition] = []

    @property
    def budget(self) -> Fraction:
        return self._budget

    @property
    def firing(self) -> bool:
        with self._lock:
            return self._firing

    def record(self, good: int = 0, bad: int = 0) -> None:
        """Count ``good`` successful and ``bad`` failed events, now."""
        for n in (good, bad):
            if isinstance(n, bool) or not isinstance(n, int) or n < 0:
                raise ValueError("event counts must be non-negative integers")
        if good == 0 and bad == 0:
            return
        with self._lock:
            now = self.clock.monotonic()
            self._long.add(good + bad, bad, now=now)
            self._short.add(good + bad, bad, now=now)

    def record_outcome(self, ok: bool) -> None:
        self.record(good=1) if ok else self.record(bad=1)

    def _burn(self, totals: tuple[int, ...]) -> Fraction | None:
        """Burn rate of one window, or ``None`` when it has too few events to judge."""
        total, bad = totals
        if total < self.min_events:
            # Too little traffic to be burning budget at any meaningful rate.
            return Fraction(0)
        # A ratio of two counts needs no exact arithmetic; floats are ~40x faster
        # than Fraction on the evaluation path.
        return (bad / total) / float(self._budget)

    def burn_rates(self) -> tuple[Fraction | None, Fraction | None]:
        """Current (long, short) burn rates."""
        with self._lock:
            now = self.clock.monotonic()
            return self._burn(self._long.totals(now)), self._burn(self._short.totals(now))

    def _decide(self, long_burn: Fraction | None, short_burn: Fraction | None) -> str | None:
        """The transition the current burn rates call for, if any.

        Not firing: fire when both windows give a verdict at or above ``fire_at``.
        Firing: clear when the short window gives a verdict below ``clear_at``.
        A window without a verdict never causes a transition.
        """
        if not self._firing:
            if long_burn is not None and short_burn is not None \
                    and long_burn >= self.fire_at and short_burn >= self.fire_at:
                return FIRE
            return None
        if short_burn is not None and short_burn < self.clear_at:
            return CLEAR
        return None

    def evaluate(self) -> AlertStatus:
        """Evaluate both windows now and apply at most one transition."""
        with self._eval_lock:
            with self._lock:
                now = self.clock.monotonic()
                long_burn = self._burn(self._long.totals(now))
                short_burn = self._burn(self._short.totals(now))
                kind = self._decide(long_burn, short_burn)
                transition = None
                if kind is not None:
                    self._firing = kind == FIRE
                    transition = Transition(len(self._transitions) + 1, now, kind,
                                            long_burn, short_burn)
                    self._transitions.append(transition)
                status = AlertStatus(self._firing, now, long_burn, short_burn, transition)
            if transition is not None and self._on_transition is not None:
                self._on_transition(transition)
            return status

    def transitions(self) -> list[Transition]:
        with self._lock:
            return list(self._transitions)
