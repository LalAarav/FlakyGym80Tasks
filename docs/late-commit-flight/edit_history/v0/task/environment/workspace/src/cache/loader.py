"""Read-through cache: single-flight loading, invalidation and refresh-ahead."""

from __future__ import annotations

import queue
import threading
from typing import Any, Callable, Hashable

from src.cache.cache import MISSING, TTLCache
from src.clock import SystemClock
from src.stats import Stats

_STOP = object()


class LoadError(Exception):
    """Raised by a loader to report that a value could not be loaded."""


class _Flight:
    """One in-progress load that concurrent callers for the same key share."""

    def __init__(self) -> None:
        self.done = threading.Event()
        self.value: Any = None
        self.error: LoadError | None = None
        self.generation = 0


class LoadingCache:
    """``get(key)`` returns the cached value or loads it with ``loader(key)``.

    Concurrent misses for one key share a single loader call. Values loaded before an
    ``invalidate(key)`` are never cached after it. With ``refresh_ahead > 0`` and after
    ``start()``, a hit on an entry that expires within ``refresh_ahead`` seconds
    schedules one background reload of that key.
    """

    def __init__(self, loader: Callable[[Hashable], Any], cache: TTLCache | None = None,
                 clock: SystemClock | None = None, stats: Stats | None = None,
                 refresh_ahead: float = 0.0):
        self.loader = loader
        self.clock = clock if clock is not None else SystemClock()
        self.cache = cache if cache is not None else TTLCache(clock=self.clock)
        self.stats = stats if stats is not None else Stats()
        self.refresh_ahead = refresh_ahead
        self._lock = threading.Lock()
        self._flights: dict[Hashable, _Flight] = {}
        self._generation: dict[Hashable, int] = {}
        self._pending: set[Hashable] = set()
        self._queue: queue.Queue = queue.Queue()
        self._thread: threading.Thread | None = None
        self._closed = False

    # -- loading -------------------------------------------------------------------

    def get(self, key: Hashable, timeout: float = 10.0) -> Any:
        value = self.cache.get(key)
        if value is not MISSING:
            self.stats.incr("hit")
            self._maybe_schedule_refresh(key)
            return value
        self.stats.incr("miss")
        flight, generation, leader = self._join_or_lead(key)
        if leader:
            self._load(key, flight, generation)
        if not flight.done.wait(timeout):
            raise TimeoutError(f"load of {key!r} did not finish within {timeout}s")
        if flight.error is not None:
            raise flight.error
        return flight.value

    def _join_or_lead(self, key: Hashable) -> tuple[_Flight, int, bool]:
        with self._lock:
            generation = self._generation.get(key, 0)
            flight = self._flights.get(key)
            if flight is not None and flight.generation == generation:
                return flight, generation, False
            # No flight, or one that started before an invalidate (C6): lead a new one.
            flight = _Flight()
            self._flights[key] = flight
            flight.generation = generation
            return flight, generation, True

    def _load(self, key: Hashable, flight: _Flight, generation: int) -> None:
        """Run the loader for a flight this thread leads, then settle the flight."""
        try:
            try:
                flight.value = self.loader(key)
            except LoadError as exc:
                flight.error = exc
            if flight.error is None:
                with self._lock:
                    current = self._generation.get(key, 0) == generation
                if current:
                    self.cache.put(key, flight.value)
                self.stats.incr("load")
            else:
                self.stats.incr("load_error")
        finally:
            with self._lock:
                if flight.error is None and self._flights.get(key) is flight:
                    del self._flights[key]
            flight.done.set()

    def invalidate(self, key: Hashable) -> None:
        """Drop ``key``; any load already in progress will not repopulate it."""
        with self._lock:
            self._generation[key] = self._generation.get(key, 0) + 1
        self.cache.invalidate(key)

    # -- refresh-ahead -------------------------------------------------------------

    def _maybe_schedule_refresh(self, key: Hashable) -> None:
        if self.refresh_ahead <= 0 or self._thread is None:
            return
        remaining = self.cache.expires_in(key)
        if remaining is None or remaining > self.refresh_ahead:
            return
        with self._lock:
            if self._closed or key in self._pending or key in self._flights:
                return
            self._pending.add(key)
        self._queue.put(key)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self._thread.start()

    def _refresh_loop(self) -> None:
        while True:
            key = self._queue.get()
            if key is _STOP:
                return
            try:
                flight, generation, leader = self._join_or_lead(key)
                if leader:
                    self._load(key, flight, generation)
            finally:
                with self._lock:
                    self._pending.discard(key)

    def close(self) -> None:
        """Stop refreshing. Returns once no background load is running or will start."""
        with self._lock:
            self._closed = True
        if self._thread is not None:
            self._queue.put(_STOP)
            self._thread.join()
            self._thread = None
