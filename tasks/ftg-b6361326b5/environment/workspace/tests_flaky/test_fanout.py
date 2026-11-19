import collections
import threading

from src.fanout.dispatcher import Dispatcher, Publisher
from src.fanout.ledger import DeliveryLedger
from src.fanout.subscriptions import SubscriptionRegistry
from src.queue.store import JobStore
from src.rpc.client import RpcClient
from src.rpc.conn import Dialer
from src.rpc.pool import ConnectionPool

from tests_flaky.server import FaultyServer


class Subscriber(FaultyServer):
    """A webhook receiver: applies each delivery, deduplicating by idempotency key."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.received = collections.Counter()

    def apply(self, req):
        if req["op"] != "deliver":
            return super().apply(req)
        with self.lock:
            key = req.get("key")
            if key is None or key not in self.dedupe:
                self.received[req["payload"]["event_id"]] += 1
                if key is not None:
                    self.dedupe[key] = True
        return {"id": req["id"], "ok": True, "result": None}


def build(tmp_path):
    store = JobStore(str(tmp_path / "queue"))
    ledger = DeliveryLedger(str(tmp_path))
    registry = SubscriptionRegistry()
    clients, lock = {}, threading.Lock()

    def client_for(address):
        with lock:
            if address not in clients:
                pool = ConnectionPool(Dialer(), address, max_size=2)
                clients[address] = RpcClient(pool, attempt_timeout=0.2, max_attempts=3, backoff=0.005)
            return clients[address]

    dispatcher = Dispatcher(store, ledger, client_for, workers=3, delivery_deadline=1.0)
    return Publisher(store, registry), dispatcher, registry


def test_every_event_reaches_every_subscriber_once(tmp_path):
    with Subscriber(drop=0.12, delay=0.01) as a, Subscriber(drop=0.12) as b, Subscriber(split=0.01) as c:
        publisher, dispatcher, registry = build(tmp_path)
        for name, sub in (("billing", a), ("search", b), ("audit", c)):
            registry.subscribe("orders", name, sub.address)
        dispatcher.start()
        try:
            for i in range(30):
                publisher.publish("orders", {"order": i}, event_id=f"evt-{i}")
        finally:
            dispatcher.stop(drain=True, timeout=60)
        expected = {f"evt-{i}": 1 for i in range(30)}
        for sub in (a, b, c):
            assert dict(sub.received) == expected


def test_republishing_an_event_is_a_no_op(tmp_path):
    with Subscriber() as a:
        publisher, dispatcher, registry = build(tmp_path)
        registry.subscribe("orders", "billing", a.address)
        dispatcher.start()
        try:
            assert publisher.publish("orders", {}, event_id="evt-x") == "evt-x"
            assert publisher.publish("orders", {}, event_id="evt-x") is None
        finally:
            dispatcher.stop(drain=True, timeout=30)
        assert dict(a.received) == {"evt-x": 1}
