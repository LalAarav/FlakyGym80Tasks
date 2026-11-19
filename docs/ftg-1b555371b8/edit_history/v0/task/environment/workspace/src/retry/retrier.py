"""Retrier: the glue the dispatcher talks to.

``Retrier`` combines a ``RetryPolicy`` (how long to wait), a ``DelayScheduler``
(waiting without blocking a worker) and a ``DeadLetterStore`` (where items go when
the policy gives up). A worker that fails a job calls ``failed``; one that
succeeds calls ``succeeded``. The scheduler's sink, supplied by the caller,
resubmits due items, typically by putting them back on the work queue.
"""

from __future__ import annotations

from typing import Any, Hashable

from src.retry.deadletter import DeadLetter, DeadLetterStore, ReplayTarget
from src.retry.policy import RetryPolicy
from src.stats import Stats
from src.retry.timers import DelayScheduler


class Retrier:
    """Route each failure to a delayed retry or, once exhausted, to the dead letters.

    Keys are item ids (strings), the same ids the dead-letter store uses.
    """

    def __init__(self, policy: RetryPolicy, scheduler: DelayScheduler,
                 deadletters: DeadLetterStore, stats: Stats | None = None) -> None:
        self.policy = policy
        self.scheduler = scheduler
        self.deadletters = deadletters
        self._stats = stats if stats is not None else Stats()

    def failed(self, item_id: str, payload: Any, reason: str) -> float | None:
        """Record a failed run of ``item_id``.

        Returns the delay before its retry, or ``None`` if it was dead-lettered.
        A failure reported while a retry is already pending moves that retry to
        the new delay instead of scheduling a second one.
        """
        delay = self.policy.next_delay(item_id)
        if delay is None:
            attempts = self.policy.attempts(item_id)
            self.scheduler.cancel(item_id)
            self.deadletters.put(item_id, payload, attempts, reason)
            self.policy.reset(item_id)
            self._stats.incr("retrier.dead")
            return None
        if not self.scheduler.reschedule(item_id, delay):
            self.scheduler.schedule(item_id, payload, delay)
        self._stats.incr("retrier.scheduled")
        return delay

    def succeeded(self, item_id: str) -> None:
        """``item_id`` ran successfully: forget its failures and any pending retry."""
        self.scheduler.cancel(item_id)
        self.policy.reset(item_id)

    def replay(self, item_id: str, target: ReplayTarget) -> bool:
        """Replay a dead-lettered item into ``target`` with a fresh attempt budget."""

        def hand_off(record: DeadLetter) -> None:
            self.policy.reset(record.item_id)
            target(record)

        return self.deadletters.replay(item_id, hand_off)

    def pending(self, item_id: Hashable) -> bool:
        """Whether ``item_id`` has a retry waiting in the scheduler."""
        return self.scheduler.due(item_id) is not None
