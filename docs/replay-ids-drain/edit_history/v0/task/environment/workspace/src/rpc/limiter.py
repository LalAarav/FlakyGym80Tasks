"""Token-bucket rate limiter."""

from __future__ import annotations

import threading

from src.clock import SystemClock


class TokenBucket:
    """Admits at most ``burst`` calls at once, refilled at ``rate`` tokens per second."""

    def __init__(self, rate: float, burst: int, clock: SystemClock | None = None):
        self.rate = rate
        self.burst = burst
        self.clock = clock if clock is not None else SystemClock()
        self._lock = threading.Lock()
        self._tokens = float(burst)
        self._updated = self.clock.monotonic()

    def try_acquire(self) -> bool:
        with self._lock:
            now = self.clock.monotonic()
            self._tokens = min(self.burst, self._tokens + (now - self._updated) * self.rate)
            self._updated = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False
