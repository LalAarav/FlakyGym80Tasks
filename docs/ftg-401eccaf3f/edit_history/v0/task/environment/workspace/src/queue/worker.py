"""Thread pool that leases jobs from a ``JobStore`` and runs a handler on each."""

from __future__ import annotations

import threading
from typing import Any, Callable

from src.clock import SystemClock
from src.stats import Stats
from src.queue.store import JobStore, Lease, StaleLease


class RetryableError(Exception):
    """Raised by a handler to have its job returned to the queue and retried."""


class WorkerPool:
    def __init__(
        self,
        store: JobStore,
        handler: Callable[[Any], None],
        workers: int = 4,
        lease_seconds: float = 30.0,
        poll_interval: float = 0.005,
        reap_interval: float = 0.05,
        stats: Stats | None = None,
        clock: SystemClock | None = None,
        with_attempts: bool = False,
    ):
        """With ``with_attempts`` the handler is called as ``handler(payload, attempts)``,
        where ``attempts`` is how many times the job has been retried so far."""
        self.store = store
        self.handler = handler
        self.with_attempts = with_attempts
        self.workers = workers
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval
        self.reap_interval = reap_interval
        self.stats = stats if stats is not None else Stats()
        self.clock = clock if clock is not None else SystemClock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        for i in range(self.workers):
            t = threading.Thread(target=self._run, args=(f"w{i}",), daemon=True)
            self._threads.append(t)
        self._threads.append(threading.Thread(target=self._reap_loop, daemon=True))
        for t in self._threads:
            t.start()

    def _run(self, worker: str) -> None:
        while not self._stop.is_set():
            lease = self.store.lease(worker, self.lease_seconds)
            if lease is None:
                self.clock.sleep(self.poll_interval)
                continue
            self._handle(lease)

    def _handle(self, lease: Lease) -> None:
        try:
            if self.with_attempts:
                self.handler(lease.job.payload, lease.job.attempts)
            else:
                self.handler(lease.job.payload)
        except RetryableError:
            try:
                self.store.nack(lease)
                self.stats.incr("retried")
            except StaleLease:
                self.stats.incr("stale")
            return
        try:
            self.store.ack(lease)
            self.stats.incr("processed")
        except StaleLease:
            self.stats.incr("stale")

    def _reap_loop(self) -> None:
        while not self._stop.is_set():
            self.stats.incr("reaped", self.store.reap())
            self.clock.sleep(self.reap_interval)

    def drain(self, timeout: float = 10.0) -> None:
        """Block until no job is ready or leased. Raises ``TimeoutError``."""
        deadline = self.clock.monotonic() + timeout
        while True:
            counts = self.store.counts()
            if counts["ready"] == 0 and counts["leased"] == 0:
                return
            if self.clock.monotonic() >= deadline:
                raise TimeoutError(f"queue not drained: {counts}")
            self.clock.sleep(self.poll_interval)

    def stop(self, drain: bool = True, timeout: float = 10.0) -> None:
        """Stop the pool. With ``drain``, first wait for every job to be settled.
        Returns only after all pool threads have exited."""
        if drain:
            self.drain(timeout)
        self._stop.set()
        for t in self._threads:
            t.join()
        self._threads = []
