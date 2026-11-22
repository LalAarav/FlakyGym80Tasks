"""Bounded connection pool."""

from __future__ import annotations

import threading

from src.clock import SystemClock


class PoolTimeout(TimeoutError):
    """No connection became available before the acquire timeout."""


class ConnectionPool:
    """At most ``max_size`` connections to ``address`` exist at once (idle + in use)."""

    def __init__(self, dialer, address, max_size: int = 4, connect_timeout: float = 1.0,
                 clock: SystemClock | None = None):
        self.dialer = dialer
        self.address = address
        self.max_size = max_size
        self.connect_timeout = connect_timeout
        self.clock = clock if clock is not None else SystemClock()
        self._cond = threading.Condition()
        self._idle: list = []
        self._open = 0

    def acquire(self, timeout: float):
        """Return an idle connection, or dial a new one if under ``max_size``,
        waiting up to ``timeout`` seconds for one to be released."""
        deadline = self.clock.monotonic() + timeout
        with self._cond:
            while True:
                if self._idle:
                    return self._idle.pop()
                if self._open < self.max_size:
                    self._open += 1
                    break
                remaining = deadline - self.clock.monotonic()
                if remaining <= 0:
                    raise PoolTimeout(f"no connection within {timeout}s")
                self._cond.wait(remaining)
        try:
            return self.dialer.dial(self.address, self.connect_timeout)
        except OSError:
            with self._cond:
                self._open -= 1
                self._cond.notify()
            raise

    def release(self, conn, reusable: bool) -> None:
        """Give a connection back. Only connections in a known-clean state (a
        complete request/response exchange) may be reused; anything else is closed."""
        with self._cond:
            if reusable and not conn.closed:
                self._idle.append(conn)
            else:
                conn.close()
                self._open -= 1
            self._cond.notify()

    def close(self) -> None:
        with self._cond:
            for conn in self._idle:
                conn.close()
            self._open -= len(self._idle)
            self._idle = []
            self._cond.notify_all()

    def stats(self) -> dict[str, int]:
        with self._cond:
            return {"open": self._open, "idle": len(self._idle)}
