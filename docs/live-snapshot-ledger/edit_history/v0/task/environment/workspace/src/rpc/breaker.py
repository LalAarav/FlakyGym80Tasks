"""Circuit breaker: stop calling a failing dependency, then probe it to recover."""

from __future__ import annotations

import threading

from src.clock import SystemClock


class CircuitOpen(Exception):
    """The breaker is open (or a recovery probe is already in flight)."""


class CircuitBreaker:
    """closed --(``failure_threshold`` consecutive failures)--> open
    open --(``reset_timeout`` elapsed)--> half-open, admitting exactly one probe
    half-open --probe succeeds--> closed;  --probe fails--> open again."""

    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 1.0,
                 clock: SystemClock | None = None):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.clock = clock if clock is not None else SystemClock()
        self._lock = threading.Lock()
        self._state = "closed"
        self._failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def allow(self) -> bool:
        with self._lock:
            if self._state == "closed":
                return True
            if self._state == "open":
                if self.clock.monotonic() - self._opened_at < self.reset_timeout:
                    return False
                self._state = "half_open"
                self._probe_in_flight = True
                return True
            if self._probe_in_flight:
                return False
            self._probe_in_flight = True
            return True

    def record_success(self) -> None:
        with self._lock:
            self._state = "closed"
            self._failures = 0
            self._probe_in_flight = False

    def record_failure(self) -> None:
        with self._lock:
            if self._state == "half_open":
                self._state = "open"
                self._opened_at = self.clock.monotonic()
                self._probe_in_flight = False
                return
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._state = "open"
                self._opened_at = self.clock.monotonic()
