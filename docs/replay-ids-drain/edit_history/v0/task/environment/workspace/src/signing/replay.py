"""Bounded memory of recently accepted message nonces.

The verifier records the nonce of every message it accepts, together with a
monotonic deadline until which that nonce must be remembered. A second message
carrying a remembered nonce is a replay.

The cache is bounded by ``max_entries`` but never forgets an entry before its
deadline: forgetting early would reopen the replay window. When it is full of
live entries, ``add`` refuses with ``ReplayCacheFull`` and the caller rejects the
new message instead (the sender retries later, by which time entries expire).
"""

from __future__ import annotations

import heapq
import threading

from src.clock import SystemClock
from src.signing.errors import ReplayCacheFull


class ReplayCache:
    """Thread-safe set of identities, each with a monotonic expiry deadline."""

    def __init__(self, max_entries: int = 100_000, clock: SystemClock | None = None):
        if max_entries < 1:
            raise ValueError("max_entries must be >= 1")
        self.max_entries = max_entries
        self.clock = clock if clock is not None else SystemClock()
        self._lock = threading.Lock()
        self._expiry: dict[str, float] = {}
        # (deadline, identity) min-heap. Entries whose deadline no longer matches
        # ``_expiry`` (discarded or re-added) are stale and skipped when popped.
        self._heap: list[tuple[float, str]] = []

    def seen(self, identity: str) -> bool:
        """True if ``identity`` is remembered and its deadline has not passed."""
        with self._lock:
            deadline = self._expiry.get(identity)
            return deadline is not None and self.clock.monotonic() < deadline

    def add(self, identity: str, expires_at: float) -> bool:
        """Remember ``identity`` until monotonic time ``expires_at``.

        Check and insert are one atomic step: returns ``False`` (and changes nothing)
        if the identity is already remembered, ``True`` if this call recorded it.
        Raises ``ReplayCacheFull`` if recording it would exceed ``max_entries`` live
        entries.
        """
        with self._lock:
            now = self.clock.monotonic()
            deadline = self._expiry.get(identity)
            if deadline is not None and now < deadline:
                return False
            self._purge(now)
            if identity not in self._expiry and len(self._expiry) >= self.max_entries:
                raise ReplayCacheFull(
                    f"{len(self._expiry)} nonces are still within their retention period")
            self._expiry[identity] = expires_at
            heapq.heappush(self._heap, (expires_at, identity))
            return True

    def discard(self, identity: str) -> None:
        """Forget ``identity`` (administrative use)."""
        with self._lock:
            self._expiry.pop(identity, None)

    def __len__(self) -> int:
        with self._lock:
            self._purge(self.clock.monotonic())
            return len(self._expiry)

    def _purge(self, now: float) -> None:
        """Drop every entry whose deadline is ``<= now``. Caller holds ``_lock``."""
        heap = self._heap
        while heap and heap[0][0] <= now:
            deadline, identity = heapq.heappop(heap)
            if self._expiry.get(identity) == deadline:
                del self._expiry[identity]
        # Stale heap entries from discard/re-add are bounded by live ones; compact
        # occasionally so a long-running process does not accumulate them.
        if len(heap) > 2 * len(self._expiry) + 64:
            self._heap = [(d, i) for i, d in self._expiry.items()]
            heapq.heapify(self._heap)
