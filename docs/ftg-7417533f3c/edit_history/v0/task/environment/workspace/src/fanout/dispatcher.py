"""Publishing events and delivering them to subscribers.

``Publisher.publish`` resolves the topic's subscribers and enqueues one durable job per
event. A ``Dispatcher`` runs a worker pool over that queue; for each job it delivers the
event to every target subscriber over RPC, records each completed delivery in the
ledger, and asks for the job to be retried while any target is still outstanding.

Optionally a ``Dispatcher`` signs every delivery (``signer``), parks events that keep
failing in a dead-letter store (``deadletters`` with ``max_job_attempts``), from which
``replay`` queues them again, counts settled deliveries in a metrics ``Registry`` and
feeds delivery outcomes to an SLO ``alert``.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Callable

from src.clock import SystemClock
from src.fanout.ledger import DeliveryLedger
from src.fanout.subscriptions import SubscriptionRegistry
from src.queue.store import JobStore
from src.queue.worker import RetryableError, WorkerPool
from src.rpc.breaker import CircuitOpen
from src.rpc.client import RemoteError, RpcClient
from src.retry.deadletter import DeadLetter, DeadLetterStore
from src.rpc.wire import ProtocolError
from src.metrics.registry import Registry
from src.signing import canonical
from src.signing.signer import Signer
from src.stats import Stats

DELIVERY_FAILURES = (ConnectionError, TimeoutError, ProtocolError, CircuitOpen)


class Publisher:
    def __init__(self, store: JobStore, registry: SubscriptionRegistry,
                 signer: Signer | None = None):
        """Give the publisher the dispatcher's ``signer`` when deliveries are signed."""
        self.store = store
        self.registry = registry
        self.signer = signer

    def publish(self, topic: str, body: Any, event_id: str | None = None) -> str | None:
        """Publish an event to the topic's current subscribers. Returns the event id,
        or ``None`` if an event with this ``event_id`` was already published."""
        event_id = event_id if event_id is not None else uuid.uuid4().hex
        if self.signer is not None:
            # Deliveries will be signed: refuse, before anything is queued, a body that
            # cannot be carried at all (the queue, the wire and the signer all use JSON).
            json.dumps(body)
        targets = self.registry.subscribers(topic)
        job = {"event_id": event_id, "topic": topic, "body": body,
               "targets": {sub: list(addr) for sub, addr in targets.items()}}
        return self.store.enqueue(job, job_id=event_id)


class Dispatcher:
    def __init__(
        self,
        store: JobStore,
        ledger: DeliveryLedger,
        client_for: Callable[[tuple[str, int]], RpcClient],
        workers: int = 4,
        lease_seconds: float = 30.0,
        delivery_deadline: float = 2.0,
        stats: Stats | None = None,
        clock: SystemClock | None = None,
        signer: Signer | None = None,
        deadletters: DeadLetterStore | None = None,
        max_job_attempts: int | None = None,
        metrics: Registry | None = None,
        alert: Any = None,
    ):
        if (deadletters is None) != (max_job_attempts is None):
            raise ValueError("deadletters and max_job_attempts go together")
        self.store = store
        self.ledger = ledger
        self.signer = signer
        self._signatures: dict = {}
        self.deadletters = deadletters
        self.max_job_attempts = max_job_attempts
        self.metrics = metrics
        self.alert = alert
        self.client_for = client_for
        self.delivery_deadline = delivery_deadline
        self.stats = stats if stats is not None else Stats()
        self.pool = WorkerPool(store, self._handle, workers=workers, lease_seconds=lease_seconds,
                               stats=self.stats, clock=clock, with_attempts=True)

    def start(self) -> None:
        self.pool.start()

    def stop(self, drain: bool = True, timeout: float = 30.0) -> None:
        self.pool.stop(drain=drain, timeout=timeout)

    def _deliver(self, job: dict, subscriber: str, address: tuple[str, int]) -> None:
        event = {"event_id": job["event_id"], "topic": job["topic"], "body": job["body"]}
        if self.signer is not None:
            # Sign each event once: every redelivery then carries byte-identical
            # bytes, so a subscriber can recognise a duplicate by its signature.
            signed = self._signatures.get(job["event_id"])
            if signed is None:
                signed = self._signatures[job["event_id"]] = self.signer.sign(event)
            event = {**event, "signature": {"headers": signed.headers,
                                            "body": signed.body.decode("utf-8")}}
        self.client_for(address).call(
            "deliver", event,
            idempotency_key=json.dumps([job["event_id"], subscriber]),
            deadline=self.delivery_deadline,
        )

    def _handle(self, job: dict, attempts: int) -> None:
        outstanding = []
        for subscriber, address in sorted(job["targets"].items()):
            if self.ledger.delivered(job["event_id"], subscriber):
                continue
            try:
                self._deliver(job, subscriber, tuple(address))
            except DELIVERY_FAILURES as exc:
                outstanding.append(subscriber)
                self.stats.incr("delivery_failed")
                if not isinstance(exc, CircuitOpen):
                    # An open breaker refused it locally: no request reached the
                    # subscriber, so it says nothing about the subscriber's health.
                    self._observe(ok=False)
                continue
            except RemoteError:
                self.stats.incr("delivery_rejected")  # the subscriber refused it: settled
                outcome = "rejected"
            else:
                self.stats.incr("delivered")
                outcome = "delivered"
            self._observe(ok=True)
            if self.ledger.record(job["event_id"], subscriber) and self.metrics is not None:
                # Count a settlement once, however many workers raced to it.
                self.metrics.counter("deliveries", {"outcome": outcome}).inc()
        if not outstanding:
            return
        reason = f"{len(outstanding)} subscriber(s) still outstanding"
        if self.deadletters is not None and attempts + 1 >= self.max_job_attempts:
            # Park the event and settle its job. The ledger remembers which subscribers
            # already have it, so a replay only reaches the others.
            self.deadletters.put(job["event_id"], job, attempts + 1, reason)
            self.stats.incr("dead_lettered")
            return
        raise RetryableError(reason)

    def _observe(self, ok: bool) -> None:
        if self.alert is not None:
            self.alert.record_outcome(ok)

    def replay(self, event_id: str) -> bool:
        """Queue a dead-lettered event again. Returns ``True`` if this call re-queued it."""
        if self.deadletters is None:
            raise ValueError("this dispatcher has no dead-letter store")
        return self.deadletters.replay(event_id, self._requeue)

    def _requeue(self, record: DeadLetter) -> None:
        job = record.payload
        # A new job id for each parking: the event's own id is taken by its first job
        # (F5), and the same parking handed over twice after a crash enqueues once.
        self.store.enqueue(job, job_id=json.dumps(["replay", job["event_id"], record.dead_at]))
