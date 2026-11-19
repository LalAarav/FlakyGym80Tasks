"""Time sources.

All time-dependent code takes a ``Clock`` so it can be driven by a fake clock in
tests. ``monotonic`` never goes backwards and is unaffected by wall-clock
adjustments (NTP steps, manual changes); ``wall`` is the calendar time and may
jump in either direction.
"""

from __future__ import annotations

import time


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def wall(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)
