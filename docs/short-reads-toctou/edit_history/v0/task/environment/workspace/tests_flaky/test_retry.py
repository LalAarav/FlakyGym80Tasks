import queue
import random
import threading
import time

from src.retry.deadletter import DeadLetterStore
from src.retry.policy import RetryPolicy
from src.retry.retrier import Retrier
from src.retry.timers import DelayScheduler


class Sink:
    def __init__(self, work=0.0):
        self.lock = threading.Lock()
        self.got = []
        self.times = {}
        self.work = work

    def __call__(self, key, payload):
        with self.lock:
            self.got.append(key)
            self.times[key] = time.monotonic()
        if self.work:
            time.sleep(self.work)


def test_backoff_grows_and_respects_cap():
    policy = RetryPolicy(base=0.05, cap=2.0, max_attempts=10, jitter="full")
    for key in range(50):
        delays = []
        while True:
            d = policy.next_delay(key)
            if d is None:
                break
            delays.append(d)
        assert len(delays) == 9
        for n, d in enumerate(delays, start=1):
            assert 0.0 <= d <= policy.ceiling(n) <= 2.0


def test_policy_shared_between_threads():
    policy = RetryPolicy(base=0.01, cap=1.0, max_attempts=1000, jitter="decorrelated")

    def hammer(prefix):
        for i in range(300):
            assert policy.next_delay(f"{prefix}-{i % 7}") is not None

    threads = [threading.Thread(target=hammer, args=(p,)) for p in "abcd"]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total = sum(policy.attempts(f"{p}-{i}") for p in "abcd" for i in range(7))
    assert total == 1200


def test_items_released_in_due_order_and_on_time():
    sink = Sink()
    sched = DelayScheduler(sink)
    sched.start()
    start = time.monotonic()
    plan = {f"k{i}": random.uniform(0.0, 0.15) for i in range(60)}
    due = {k: sched.schedule(k, None, d) for k, d in plan.items()}
    time.sleep(0.3)
    left = sched.stop(timeout=5)
    assert left == []
    assert sorted(sink.got) == sorted(plan)
    for key in sink.got:
        assert sink.times[key] >= due[key]
        assert sink.times[key] - due[key] < 0.5
    assert sink.times[sink.got[-1]] - start < 1.0


def test_cancel_is_truthful_under_load():
    sink = Sink(work=0.001)
    sched = DelayScheduler(sink)
    sched.start()
    keys = [f"job{i}" for i in range(12)]
    due = {k: sched.schedule(k, None, 0.01 + 0.02 * i) for i, k in enumerate(keys)}
    cancelled = set()
    for key in keys:
        time.sleep(max(0.0, due[key] - time.monotonic() + random.uniform(-0.008, 0.008)))
        if random.random() < 0.5 and sched.cancel(key):
            cancelled.add(key)
    time.sleep(0.05)
    left = sched.stop(timeout=5)
    assert not (cancelled & set(sink.got)), f"cancelled items delivered: {cancelled & set(sink.got)}"
    assert sorted(set(sink.got) | cancelled | {k for k, _ in left}) == sorted(keys)


def test_stop_accounts_for_every_item():
    sink = Sink(work=0.0003)
    sched = DelayScheduler(sink)
    sched.start()
    keys = [f"item{i}" for i in range(40)]
    for key in keys:
        sched.schedule(key, None, random.uniform(0.0, 0.03))
    time.sleep(random.uniform(0.0, 0.03))
    left = [k for k, _ in sched.stop(timeout=5)]
    time.sleep(0.01)
    assert len(sink.got) == len(set(sink.got))
    assert sorted(sink.got + left) == sorted(keys), \
        f"{len(keys) - len(sink.got) - len(left)} items neither delivered nor returned"


def test_reschedule_moves_items():
    sink = Sink()
    sched = DelayScheduler(sink)
    sched.start()
    sched.schedule("fast", None, 0.02)
    sched.schedule("slow", None, 5.0)
    assert sched.reschedule("slow", 0.04)
    time.sleep(0.2)
    sched.stop(timeout=5)
    assert sink.got == ["fast", "slow"]


def test_dead_letters_roundtrip(tmp_path):
    store = DeadLetterStore(str(tmp_path))
    for i in range(20):
        store.put(f"order/{i}", {"n": i}, 5, "HTTP 500")
    assert len(store.list()) == 20
    seen = []
    assert store.replay("order/3", seen.append)
    assert seen[0].attempts == 0 and seen[0].payload == {"n": 3}
    assert not store.replay("order/3", seen.append)
    assert store.purge("order/4")
    assert len(store) == 18
    assert store.replay_all(seen.append) == 18
    assert store.list() == []


def test_concurrent_replays_hand_off_once(tmp_path):
    store = DeadLetterStore(str(tmp_path))
    ids = [f"evt-{i}" for i in range(30)]
    for item in ids:
        store.put(item, item, 3, "timeout")
    lock = threading.Lock()
    handed = []

    def target(record):
        with lock:
            handed.append(record.item_id)

    def replayer():
        order = ids[:]
        random.shuffle(order)
        for item in order:
            store.replay(item, target)

    threads = [threading.Thread(target=replayer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(handed) == sorted(ids)
    assert store.list() == []


def test_retrier_end_to_end(tmp_path):
    work = queue.Queue()
    resubmitted = []

    def worker():
        while True:
            item = work.get()
            if item is None:
                return
            key, payload = item
            time.sleep(0.001)
            resubmitted.append(key)
            retrier.failed(key, payload, "still failing")

    sched = DelayScheduler(lambda key, payload: work.put((key, payload)))
    policy = RetryPolicy(base=0.005, cap=0.02, max_attempts=4, jitter="full")
    retrier = Retrier(policy, sched, DeadLetterStore(str(tmp_path)))
    runner = threading.Thread(target=worker)
    runner.start()
    sched.start()
    for i in range(10):
        retrier.failed(f"job{i}", {"i": i}, "first failure")
    deadline = time.monotonic() + 3
    while len(retrier.deadletters) < 10 and time.monotonic() < deadline:
        time.sleep(0.01)
    sched.stop(timeout=5)
    work.put(None)
    runner.join()
    assert len(retrier.deadletters) == 10
    assert len(resubmitted) == 30
    assert all(r.attempts == 4 for r in retrier.deadletters.list())
