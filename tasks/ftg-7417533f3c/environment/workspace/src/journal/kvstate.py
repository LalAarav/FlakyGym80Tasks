"""Thread-safe, durable key-value state over the journal.

``KVState`` keeps the whole map in memory and makes every change durable in the
write-ahead log before it becomes visible. Opening a ``KVState`` on a directory
recovers the state that the previous process left there, however it ended.

Typical use::

    state = KVState("/var/lib/fanout/subscriptions")
    state.put("orders/billing", b'["10.0.0.7", 8080]')
    with state.batch() as b:          # applied all together, or not at all
        b.delete("orders/old-billing")
        b.put("orders/billing", b'["10.0.0.8", 8080]')
    state.maybe_compact()             # bound the log; cheap when not due
"""

from __future__ import annotations

import threading
from typing import Iterable

from src.journal import codec, journal, snapshot
from src.clock import SystemClock
from src.fsops import FsOps
from src.stats import Stats


class StoreClosed(RuntimeError):
    """The store was closed, or a failed write left it unusable until reopened."""


class Batch:
    """Collects operations and commits them as one atomic batch.

    Used as a context manager the batch commits when the block exits normally and
    is discarded (nothing written, nothing applied) when the block raises.
    """

    def __init__(self, state: "KVState"):
        self._state = state
        self._ops: list[codec.Op] = []
        self.seq: int | None = None

    def put(self, key: str, value: bytes) -> "Batch":
        self._ops.append(codec.put_op(key, value))
        return self

    def delete(self, key: str) -> "Batch":
        self._ops.append(codec.delete_op(key))
        return self

    def __len__(self) -> int:
        return len(self._ops)

    def commit(self) -> int | None:
        """Make the batch durable and visible; returns its sequence number, or
        ``None`` for an empty batch (which writes nothing)."""
        if self.seq is not None:
            raise RuntimeError("batch already committed")
        if self._ops:
            self.seq = self._state.apply(self._ops)
        return self.seq

    def __enter__(self) -> "Batch":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.commit()


class KVState:
    """A ``str -> bytes`` map whose changes survive crashes and restarts.

    ``directory`` is created if needed and must be used by one ``KVState`` at a
    time. ``compact_after_bytes`` and ``compact_interval`` (seconds on the
    monotonic clock) control when :meth:`maybe_compact` decides a compaction is due.
    """

    def __init__(self, directory: str, fs: FsOps | None = None,
                 clock: SystemClock | None = None, stats: Stats | None = None,
                 compact_after_bytes: int = 8 * 1024 * 1024,
                 compact_interval: float | None = None):
        self.fs = fs if fs is not None else FsOps()
        self.clock = clock if clock is not None else SystemClock()
        self.stats = stats if stats is not None else Stats()
        self.compact_after_bytes = compact_after_bytes
        self.compact_interval = compact_interval
        self.layout = snapshot.Layout(self.fs, directory)
        # _lock guards _data, _seq, _gen and _journal. _compact_lock admits one
        # compaction at a time without blocking writers.
        self._lock = threading.Lock()
        self._compact_lock = threading.Lock()
        self._closed = False
        self._failed: BaseException | None = None
        recovered = snapshot.recover(self.fs, self.layout, self.stats)
        self._data: dict[str, bytes] = recovered.state
        self._seq = recovered.seq
        self._gen = recovered.generation
        self._journal = journal.Journal(self.fs, self.layout.log_path(self._gen),
                                        recovered.log_size)
        self._last_compaction = self.clock.monotonic()

    # -- reads ---------------------------------------------------------------------

    def get(self, key: str, default: bytes | None = None) -> bytes | None:
        with self._lock:
            return self._data.get(key, default)

    def contains(self, key: str) -> bool:
        with self._lock:
            return key in self._data

    def items(self) -> dict[str, bytes]:
        """A consistent copy of the whole map."""
        with self._lock:
            return dict(self._data)

    def keys(self, prefix: str = "") -> list[str]:
        """Sorted keys starting with ``prefix``."""
        with self._lock:
            return sorted(k for k in self._data if k.startswith(prefix))

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    @property
    def seq(self) -> int:
        """Sequence number of the last committed batch (0 for a new store)."""
        with self._lock:
            return self._seq

    @property
    def generation(self) -> int:
        with self._lock:
            return self._gen

    @property
    def log_bytes(self) -> int:
        """Size of the active log, which compaction resets."""
        with self._lock:
            return self._journal.size

    # -- writes --------------------------------------------------------------------

    def put(self, key: str, value: bytes) -> int:
        return self.apply([codec.put_op(key, value)])

    def delete(self, key: str) -> int:
        """Remove ``key``; deleting an absent key is a (logged) no-op."""
        return self.apply([codec.delete_op(key)])

    def batch(self) -> Batch:
        return Batch(self)

    def apply(self, ops: Iterable[codec.Op]) -> int:
        """Commit ``ops`` as one atomic batch; returns its sequence number.

        Returns once the batch is durable. Readers see either none or all of it.
        """
        ops = list(ops)
        if not ops:
            raise ValueError("a batch needs at least one operation")
        for op in ops:
            codec.validate(op)
        payload = codec.encode_ops(ops)
        with self._lock:
            self._check_usable()
            self._seq += 1
            seq = self._seq
            log = self._journal
        # The fsync is the slow part, so it runs outside the state lock: concurrent
        # writers share the disk instead of queueing behind each other. A change is
        # applied only once it is durable, so readers never see an unlogged write.
        try:
            written = log.append(seq, payload)
        except OSError as exc:
            # The log may now end in a partial batch; appending after it would
            # bury later batches behind it. Recovery on reopen cuts it away.
            self._failed = exc
            raise
        with self._lock:
            self._apply(ops)
        self.stats.incr("batches")
        self.stats.incr("ops", len(ops))
        self.stats.incr("bytes_logged", written)
        return seq

    def _apply(self, ops: list[codec.Op]) -> None:
        for op in ops:
            if op.kind == codec.PUT:
                self._data[op.key] = op.value
            else:
                self._data.pop(op.key, None)

    def _check_usable(self) -> None:
        if self._closed:
            raise StoreClosed("store is closed")
        if self._failed is not None:
            raise StoreClosed(f"an earlier write failed ({self._failed}); reopen the store")

    # -- compaction ----------------------------------------------------------------

    def compaction_due(self) -> bool:
        with self._lock:
            if self._journal.size >= self.compact_after_bytes:
                return True
            if self.compact_interval is None or self._journal.size == 0:
                return False
            return self.clock.monotonic() - self._last_compaction >= self.compact_interval

    def maybe_compact(self) -> bool:
        """Compact if the log has grown past its limit or the interval has passed."""
        if not self.compaction_due():
            return False
        self.compact()
        return True

    def compact(self) -> int:
        """Write a snapshot of the current state and drop the logs it replaces.

        Writers keep going while the snapshot is written; only the switch to a new
        log happens under the store lock. Returns the new generation number.
        """
        with self._compact_lock:
            with self._lock:
                self._check_usable()
                state = dict(self._data)
                seq = self._seq
                gen = self._gen + 1
                self._journal = journal.Journal.create(self.fs, self.layout.log_path(gen))
                self._gen = gen
                self._last_compaction = self.clock.monotonic()
            staged = snapshot.stage(self.fs, self.layout, gen, seq, state)
            snapshot.install(self.fs, self.layout, gen, staged)
            snapshot.discard_before(self.fs, self.layout, gen)
        self.stats.incr("compactions")
        return gen

    # -- lifecycle -----------------------------------------------------------------

    def close(self) -> None:
        """Wait for a running compaction, then refuse further writes. Everything
        committed is already durable, so there is nothing to flush."""
        with self._compact_lock:
            with self._lock:
                self._closed = True

    def __enter__(self) -> "KVState":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
