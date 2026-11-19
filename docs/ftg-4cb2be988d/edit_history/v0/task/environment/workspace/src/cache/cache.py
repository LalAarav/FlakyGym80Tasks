"""Bounded TTL cache with LRU eviction."""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any, Hashable

from src.clock import SystemClock

MISSING = object()


class TTLCache:
    """Maps keys to values for a fixed time-to-live.

    An entry stored at time ``t`` is visible for ``t <= now < t + ttl`` on the
    monotonic clock, regardless of how often it is read. When full, the least
    recently *used* entry (read or written) is evicted first.
    """

    def __init__(self, max_entries: int = 1024, ttl: float = 60.0,
                 clock: SystemClock | None = None):
        self.max_entries = max_entries
        self.ttl = ttl
        self.clock = clock if clock is not None else SystemClock()
        self._lock = threading.Lock()
        self._data: OrderedDict[Hashable, tuple[Any, float]] = OrderedDict()

    def get(self, key: Hashable) -> Any:
        """Return the cached value, or ``MISSING`` if absent or expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return MISSING
            value, expires_at = entry
            if self.clock.monotonic() >= expires_at:
                del self._data[key]
                return MISSING
            self._data.move_to_end(key)
            return value

    def put(self, key: Hashable, value: Any, ttl: float | None = None) -> None:
        lifetime = self.ttl if ttl is None else ttl
        with self._lock:
            self._data[key] = (value, self.clock.monotonic() + lifetime)
            self._data.move_to_end(key)
            while len(self._data) > self.max_entries:
                self._data.popitem(last=False)

    def invalidate(self, key: Hashable) -> None:
        with self._lock:
            self._data.pop(key, None)

    def expires_in(self, key: Hashable) -> float | None:
        """Seconds until ``key`` expires, or ``None`` if it is not cached."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            return entry[1] - self.clock.monotonic()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)
