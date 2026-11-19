import random
import threading
import time

from src.stats import Stats
from src.queue.store import JobStore
from src.queue.worker import RetryableError, WorkerPool


def test_each_job_processed_exactly_once(tmp_path, monkeypatch):
    crashes = []
    monkeypatch.setattr(threading, "excepthook", lambda args: crashes.append(args.exc_value))
    store = JobStore(str(tmp_path))
    seen = []

    def handler(payload):
        time.sleep(0.005 + random.random() * 0.010)
        seen.append(payload)

    for i in range(30):
        store.enqueue(i)
    pool = WorkerPool(store, handler, workers=2)
    pool.start()
    pool.stop(drain=True, timeout=30)
    assert not crashes, f"a worker thread crashed: {crashes[0]!r}"
    assert sorted(seen) == list(range(30))
    assert pool.stats.get("processed") == 30
    assert store.counts() == {"ready": 0, "leased": 0, "done": 30}


def test_trickle_of_jobs_into_idle_pool(tmp_path, monkeypatch):
    crashes = []
    monkeypatch.setattr(threading, "excepthook", lambda args: crashes.append(args.exc_value))
    store = JobStore(str(tmp_path))
    seen = []
    pool = WorkerPool(store, seen.append, workers=1)
    pool.start()
    try:
        for i in range(30):
            store.enqueue({"n": i, "body": "x" * 512})
            time.sleep(0.005)
    finally:
        pool.stop(drain=True, timeout=30)
    assert not crashes, f"a worker thread crashed: {crashes[0]!r}"
    assert sorted(p["n"] for p in seen) == list(range(30))


def test_fifo_with_single_consumer(tmp_path):
    store = JobStore(str(tmp_path))
    for i in range(200):
        store.enqueue(i)
    order = []
    while (lease := store.lease("solo", 30)) is not None:
        order.append(lease.job.payload)
        store.ack(lease)
    assert order == list(range(200))


def test_concurrent_producers_dedupe_by_id(tmp_path):
    store = JobStore(str(tmp_path))
    ids = [f"evt-{i}" for i in range(20)]

    def produce(delay):
        time.sleep(delay)  # a redelivery from upstream arrives a moment later
        for job_id in ids:
            store.enqueue({"id": job_id}, job_id=job_id)

    threads = [threading.Thread(target=produce, args=(d,)) for d in (0.0, 0.002)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert store.counts()["ready"] == 20


def test_stop_settles_every_job(tmp_path):
    store = JobStore(str(tmp_path))

    def handler(_):
        time.sleep(random.random() * 0.004)

    pool = WorkerPool(store, handler, workers=4)
    for i in range(60):
        store.enqueue(i)
    pool.start()
    pool.stop(drain=True, timeout=30)
    assert store.counts() == {"ready": 0, "leased": 0, "done": 60}


def test_retryable_errors_are_retried(tmp_path):
    store = JobStore(str(tmp_path))
    failed = set()
    lock = threading.Lock()

    def handler(payload):
        with lock:
            first = payload not in failed
            failed.add(payload)
        if first:
            raise RetryableError()

    stats = Stats()
    for i in range(50):
        store.enqueue(i)
    pool = WorkerPool(store, handler, workers=4, stats=stats)
    pool.start()
    pool.stop(drain=True, timeout=30)
    assert stats.get("processed") == 50
    assert stats.get("retried") == 50
    assert all(j.attempts == 1 for j in store.done_jobs())


def test_expired_lease_is_redelivered(tmp_path):
    store = JobStore(str(tmp_path))
    store.enqueue("job")
    first = store.lease("a", 0.05)
    time.sleep(0.08)
    assert store.reap() == 1
    second = store.lease("b", 30)
    assert second is not None and second.job.id == first.job.id
    store.ack(second)
    assert store.counts()["done"] == 1


def test_stats_under_threads():
    stats = Stats()

    def bump():
        for _ in range(5000):
            stats.incr("hits")

    threads = [threading.Thread(target=bump) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert stats.get("hits") == 30000
