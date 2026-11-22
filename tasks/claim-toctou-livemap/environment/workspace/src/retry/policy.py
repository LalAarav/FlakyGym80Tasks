"""Retry policy: exponential backoff with a cap, jitter and per-key attempt budgets.

A ``RetryPolicy`` answers one question for the rest of the package: "this key has
just failed again; how long until it may be retried, if at all?" It keeps a small
amount of state per key (how many times it has failed, and the last delay it was
given) so that callers do not have to carry it around.

Delays are in seconds. All randomness comes from the ``random.Random`` the policy
is given, so a seeded generator makes every schedule reproducible. See
docs/SPEC.md (R1-R4) for the exact contract.
"""

from __future__ import annotations

import math
import random
import threading
from dataclasses import dataclass
from typing import Any, Callable, Hashable

from src.clock import SystemClock
from src.stats import Stats

JITTER_MODES = ("none", "full", "decorrelated")


class RetryableError(Exception):
    """Raised by an operation run under ``RetryPolicy.call`` to ask for a retry."""


class RetriesExhausted(Exception):
    """``RetryPolicy.call`` gave up: the operation failed on every allowed attempt.

    The last ``RetryableError`` is chained as ``__cause__``; ``attempts`` says how
    many times the operation ran.
    """

    def __init__(self, attempts: int) -> None:
        super().__init__(f"operation failed on all {attempts} attempts")
        self.attempts = attempts


@dataclass
class _KeyState:
    """What the policy remembers about one key between failures."""

    attempts: int = 0
    prev: float = 0.0  # last delay handed out (decorrelated jitter); set on creation


class RetryPolicy:
    """Exponential backoff ``min(cap, base * multiplier ** (n - 1))`` for failure ``n``.

    ``jitter`` selects how the delay actually returned is derived from that ceiling:

    - ``"none"``: the ceiling itself.
    - ``"full"``: uniform over ``[0, ceiling]``.
    - ``"decorrelated"``: uniform over ``[base, min(cap, 3 * previous)]``, where
      ``previous`` is the last delay returned for the same key (``base`` before the
      first). This ignores ``multiplier``; it grows on its own.

    ``max_attempts`` bounds how many times a key may *run*: after its
    ``max_attempts``-th failure ``next_delay`` returns ``None``. ``None`` means retry
    forever. The policy is safe to share between threads.
    """

    def __init__(self, base: float = 0.1, cap: float = 30.0, multiplier: float = 2.0,
                 max_attempts: int | None = None, jitter: str = "full",
                 rng: random.Random | None = None, clock: SystemClock | None = None,
                 stats: Stats | None = None) -> None:
        if not base > 0:
            raise ValueError("base must be positive")
        if not cap >= base:
            raise ValueError("cap must be at least base")
        if not multiplier > 1:
            raise ValueError("multiplier must be greater than 1")
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if jitter not in JITTER_MODES:
            raise ValueError(f"jitter must be one of {JITTER_MODES}")
        self.base = float(base)
        self.cap = float(cap)
        self.multiplier = float(multiplier)
        self.max_attempts = max_attempts
        self.jitter = jitter
        self._rng = rng if rng is not None else random.Random()
        self._clock = clock if clock is not None else SystemClock()
        self._stats = stats if stats is not None else Stats()
        self._lock = threading.Lock()
        self._keys: dict[Hashable, _KeyState] = {}
        self._steps_to_cap = self._first_capped_step()

    # -- the deterministic part ----------------------------------------------------

    def _first_capped_step(self) -> int:
        """Smallest ``k`` with ``base * multiplier ** k >= cap``.

        Computed once, so that ``ceiling`` can answer for any attempt number by
        comparing integers instead of raising ``multiplier`` to a huge power. The
        logarithm gives a first guess; the loops correct its rounding either way.
        """
        if self.base >= self.cap:
            return 0
        k = max(0, math.ceil(math.log(self.cap / self.base) / math.log(self.multiplier)))
        while k > 0 and self.base * self.multiplier ** (k - 1) >= self.cap:
            k -= 1
        while self.base * self.multiplier ** k < self.cap:
            k += 1
        return k

    def ceiling(self, attempt: int) -> float:
        """The un-jittered delay after failure number ``attempt`` (1-based).

        Defined for every positive integer, however large: a key may have been
        failing for months. Non-decreasing in ``attempt`` and never above ``cap``.
        """
        if attempt < 1:
            raise ValueError("attempt numbers start at 1")
        steps = attempt - 1
        if steps >= self._steps_to_cap:
            return self.cap
        return min(self.cap, self.base * self.multiplier ** steps)

    # -- per-key state --------------------------------------------------------------

    def next_delay(self, key: Hashable) -> float | None:
        """Record one more failure of ``key`` and return the delay before its retry.

        Returns ``None`` once the key has used its attempt budget; it keeps returning
        ``None`` (and keeps counting) until ``reset(key)``.
        """
        with self._lock:
            state = self._keys.get(key)
            if state is None:
                state = _KeyState(prev=self.base)
                self._keys[key] = state
            state.attempts += 1
            if self.max_attempts is not None and state.attempts >= self.max_attempts:
                self._stats.incr("policy.exhausted")
                return None
            delay = self._jittered(state)
        self._stats.incr("policy.delays")
        return delay

    def _jittered(self, state: _KeyState) -> float:
        """Draw the delay for ``state``'s current attempt. Caller holds the lock."""
        if self.jitter == "none":
            return self.ceiling(state.attempts)
        if self.jitter == "full":
            return self._rng.uniform(0.0, self.ceiling(state.attempts))
        upper = min(self.cap, state.prev * 3.0)
        delay = self._rng.uniform(self.base, upper)
        state.prev = delay
        return delay

    def attempts(self, key: Hashable) -> int:
        """How many failures of ``key`` have been recorded since its last reset."""
        with self._lock:
            state = self._keys.get(key)
            return state.attempts if state is not None else 0

    def exhausted(self, key: Hashable) -> bool:
        """True when ``key``'s next failure would get no retry (budget used up)."""
        with self._lock:
            state = self._keys.get(key)
            used = state.attempts if state is not None else 0
        return self.max_attempts is not None and used >= self.max_attempts

    def reset(self, key: Hashable) -> None:
        """Forget everything about ``key``: it behaves exactly like a new key."""
        with self._lock:
            self._keys.pop(key, None)

    def tracked(self) -> int:
        """Number of keys with state (keys that failed and were not reset)."""
        with self._lock:
            return len(self._keys)

    # -- synchronous helper ---------------------------------------------------------

    def call(self, fn: Callable[[], Any], key: Hashable | None = None,
             retry_on: tuple = (RetryableError,)) -> Any:
        """Run ``fn()`` until it returns, sleeping on the policy's clock in between.

        An exception in ``retry_on`` counts as a failure of ``key`` (a private key
        when ``None``); any other exception propagates at once. When the budget is
        used up, ``RetriesExhausted`` is raised from the last failure. The key's
        state is reset when ``call`` returns or gives up.
        """
        own = key if key is not None else _PrivateKey()
        runs = 0
        try:
            while True:
                runs += 1
                try:
                    return fn()
                except retry_on as exc:
                    delay = self.next_delay(own)
                    if delay is None:
                        raise RetriesExhausted(runs) from exc
                    self._clock.sleep(delay)
        finally:
            self.reset(own)


class _PrivateKey:
    """A key nobody else can collide with, for ``call`` without an explicit key."""

    __slots__ = ()
