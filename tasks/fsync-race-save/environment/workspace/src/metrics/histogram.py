"""Fixed-bucket histograms with an exact sum and interpolated quantiles.

A ``Histogram`` counts observations into buckets whose upper bounds are fixed at
construction. Bucket ``i`` holds values ``v`` with ``bounds[i-1] < v <= bounds[i]``
(the first bucket has no lower bound) and one extra overflow bucket holds every value
above the last bound. Alongside the bucket counts it keeps the number of observations,
the smallest and largest value seen and the *exact* sum of all values, which is what
lets two histograms be merged without the result depending on the order of merging.

Readers never look at a live histogram directly: ``snapshot()`` returns an immutable
``HistogramSnapshot`` whose fields all describe the same instant, and every derived
figure (``mean``, ``quantile``) is computed from a snapshot.
"""

from __future__ import annotations

import math
import threading
from bisect import bisect_left
from dataclasses import dataclass
from typing import Iterable, Sequence


def _check_finite(value: float, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{what} must be a real number, not {value!r}")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{what} must be finite, got {value!r}")
    return value


def check_bounds(bounds: Iterable[float]) -> tuple[float, ...]:
    """Validate and normalise bucket upper bounds.

    Bounds must be a non-empty, strictly increasing sequence of finite numbers. The
    overflow bucket is implicit; ``inf`` is not an accepted bound.
    """
    out = tuple(_check_finite(b, "bucket bound") for b in bounds)
    if not out:
        raise ValueError("a histogram needs at least one bucket bound")
    for lower, upper in zip(out, out[1:]):
        if not lower < upper:
            raise ValueError(f"bucket bounds must be strictly increasing: {lower!r} >= {upper!r}")
    return out


class _ExactSum:
    """Running sum of floats with no rounding error until it is read.

    Keeps the sum as a short list of non-overlapping partials (Shewchuk's algorithm,
    the one behind ``math.fsum``). The represented value is the exact real sum of
    everything added; ``value()`` rounds it once. The partials themselves can be added
    to another ``_ExactSum`` without losing anything, which is what makes merging exact.
    """

    __slots__ = ("_partials",)

    def __init__(self, parts: Iterable[float] = ()) -> None:
        self._partials: list[float] = []
        for part in parts:
            self.add(part)

    def add(self, x: float) -> None:
        # Kahan-compensated running sum: O(1) memory per histogram, and the
        # rounding error stays bounded however many samples are added.
        if not self._partials:
            self._partials[:] = [0.0, 0.0]
        total, carry = self._partials
        y = x - carry
        t = total + y
        self._partials[:] = [t, (t - total) - y]

    def add_parts(self, parts: Iterable[float]) -> None:
        for part in parts:
            self.add(part)

    def value(self) -> float:
        return self._partials[0] if self._partials else 0.0

    def parts(self) -> tuple[float, ...]:
        return (self.value(),) if self._partials else ()


@dataclass(frozen=True)
class HistogramSnapshot:
    """An immutable, internally consistent view of a histogram.

    ``counts`` has ``len(bounds) + 1`` entries, the last being the overflow bucket.
    ``count == sum(counts)`` always holds. ``minimum``/``maximum`` are ``None`` for an
    empty histogram. ``parts`` carries the exact sum for merging; ``total`` is that sum
    rounded once to the nearest float.
    """

    bounds: tuple[float, ...]
    counts: tuple[int, ...]
    count: int
    total: float
    minimum: float | None
    maximum: float | None
    parts: tuple[float, ...] = ()

    def mean(self) -> float | None:
        if self.count == 0:
            return None
        return self.total / self.count

    def bucket_of(self, value: float) -> int:
        """Index of the bucket ``value`` falls into (upper bounds are inclusive)."""
        return bisect_left(self.bounds, value)

    def _edges(self, index: int) -> tuple[float, float]:
        """The interpolation interval of bucket ``index``.

        Nominal edges are the neighbouring bounds (``-inf`` below the first bucket,
        ``+inf`` above the last). They are narrowed to the observed minimum and
        maximum, which every bucket lies within, so the outer buckets get finite edges
        and no estimate ever leaves the range of the data.
        """
        lower = self.bounds[index - 1] if index > 0 else -math.inf
        upper = self.bounds[index] if index < len(self.bounds) else math.inf
        assert self.minimum is not None and self.maximum is not None
        return max(lower, self.minimum), min(upper, self.maximum)

    def quantile(self, q: float) -> float | None:
        """Estimate the ``q``-quantile, ``0 <= q <= 1``.

        ``None`` for an empty histogram. ``q == 0`` and ``q == 1`` are the exact
        minimum and maximum. Otherwise, with ``rank = q * count``, the estimate lies in
        the first bucket whose cumulative count reaches ``rank`` and is interpolated
        linearly between that bucket's edges (see ``_edges``) by how far ``rank`` lies
        past the cumulative count of the buckets before it.
        """
        q = _check_finite(q, "quantile")
        if not 0.0 <= q <= 1.0:
            raise ValueError(f"quantile must be within [0, 1], got {q!r}")
        if self.count == 0:
            return None
        if q == 0.0:
            return self.minimum
        if q == 1.0:
            return self.maximum
        rank = q * self.count
        cumulative = 0
        for index, in_bucket in enumerate(self.counts):
            # A rank sitting exactly on a bucket's cumulative edge is the first
            # sample of the next populated bucket, so pick buckets that strictly
            # contain it.
            if in_bucket and cumulative + in_bucket > rank:
                lo, hi = self._edges(index)
                estimate = lo + (hi - lo) * ((rank - cumulative) / in_bucket)
                return min(max(estimate, lo), hi)
            cumulative += in_bucket
        return self.maximum  # unreachable: the last populated bucket reaches ``count``


class Histogram:
    """Thread-safe fixed-bucket histogram.

    ``observe`` is cheap and safe to call from any number of threads. ``merge`` folds
    another histogram with identical bounds into this one, as if every value it had
    observed had been observed here as well.
    """

    def __init__(self, bounds: Sequence[float]) -> None:
        self.bounds = check_bounds(bounds)
        self._lock = threading.Lock()
        self._counts = [0] * (len(self.bounds) + 1)
        self._count = 0
        self._sum = _ExactSum()
        self._min = math.inf
        self._max = -math.inf

    def observe(self, value: float) -> None:
        """Record one value. Non-finite values are rejected with ``ValueError``."""
        value = _check_finite(value, "observed value")
        index = bisect_left(self.bounds, value)
        with self._lock:
            self._counts[index] += 1
            self._count += 1
            self._sum.add(value)
            if value < self._min:
                self._min = value
            if value > self._max:
                self._max = value

    def _snapshot_locked(self) -> HistogramSnapshot:
        empty = self._count == 0
        return HistogramSnapshot(
            bounds=self.bounds,
            counts=tuple(self._counts),
            count=self._count,
            total=self._sum.value(),
            minimum=None if empty else self._min,
            maximum=None if empty else self._max,
            parts=self._sum.parts(),
        )

    def snapshot(self) -> HistogramSnapshot:
        with self._lock:
            return self._snapshot_locked()

    def count(self) -> int:
        with self._lock:
            return self._count

    def sum(self) -> float:
        with self._lock:
            return self._sum.value()

    def quantile(self, q: float) -> float | None:
        return self.snapshot().quantile(q)

    def merge(self, other: Histogram | HistogramSnapshot) -> None:
        """Add everything ``other`` has observed into this histogram.

        ``other`` is read as one consistent snapshot first, so merging a histogram that
        is being updated concurrently (or merging a histogram into itself) is safe; only
        one lock is ever held at a time. Bounds must match exactly, otherwise
        ``ValueError`` is raised and nothing changes.
        """
        snap = other if isinstance(other, HistogramSnapshot) else other.snapshot()
        if snap.bounds != self.bounds:
            raise ValueError("cannot merge histograms with different bucket bounds")
        if snap.count == 0:
            return
        with self._lock:
            for index, n in enumerate(snap.counts):
                self._counts[index] += n
            self._count += snap.count
            self._sum.add_parts(snap.parts)
            assert snap.minimum is not None and snap.maximum is not None
            self._min = min(self._min, snap.minimum)
            self._max = max(self._max, snap.maximum)
