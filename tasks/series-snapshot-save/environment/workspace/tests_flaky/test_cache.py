import collections
import random
import threading
import time

from src.cache.cache import TTLCache
from src.cache.loader import LoadError, LoadingCache
from src.stats import Stats


def test_each_key_loaded_once_under_concurrent_reads():
    calls = collections.Counter()
    lock = threading.Lock()

    def loader(key):
        with lock:
            calls[key] += 1
        time.sleep(0.001 + random.random() * 0.002)
        return f"value-{key}"

    lc = LoadingCache(loader, cache=TTLCache(ttl=60))

    def reader():
        for _ in range(40):
            key = random.randrange(150)
            assert lc.get(key) == f"value-{key}"

    threads = [threading.Thread(target=reader) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    duplicated = {k: n for k, n in calls.items() if n > 1}
    assert not duplicated, f"keys loaded more than once: {duplicated}"


def test_loader_failures_surface_and_recover():
    def loader(key):
        time.sleep(0.002)
        if random.random() < 0.02:
            raise LoadError(f"backend hiccup for {key}")
        return key * 10

    lc = LoadingCache(loader, cache=TTLCache(ttl=60))
    unexpected = []

    def reader():
        for _ in range(30):
            key = random.randrange(10)
            try:
                assert lc.get(key, timeout=2.0) == key * 10
            except LoadError:
                pass
            except TimeoutError as exc:
                unexpected.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not unexpected, f"callers timed out instead of seeing the error: {unexpected[:1]}"
    for key in range(10):
        for attempt in range(5):
            try:
                assert lc.get(key, timeout=2.0) == key * 10
                break
            except LoadError:
                if attempt == 4:
                    raise


def test_invalidation_is_respected():
    source = {"price": 0}
    lock = threading.Lock()

    def loader(key):
        with lock:
            value = source[key]
        time.sleep(random.random() * 0.002)
        return value

    lc = LoadingCache(loader, cache=TTLCache(ttl=60))
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            lc.get("price")

    readers = [threading.Thread(target=reader) for _ in range(3)]
    for t in readers:
        t.start()
    for version in range(1, 40):
        with lock:
            source["price"] = version
        lc.invalidate("price")
        time.sleep(0.003)
    stop.set()
    for t in readers:
        t.join()
    assert lc.get("price") == 39


def test_refresh_ahead_and_shutdown():
    calls = []

    def loader(key):
        calls.append(key)
        time.sleep(0.004)
        return len(calls)

    lc = LoadingCache(loader, cache=TTLCache(ttl=0.05), refresh_ahead=0.02)
    lc.start()
    deadline = time.monotonic() + 0.3
    while time.monotonic() < deadline:
        lc.get("cfg")
        time.sleep(0.002)
    lc.close()
    settled = len(calls)
    time.sleep(0.05)
    assert len(calls) == settled, "loads kept happening after close()"
    assert settled <= 14, f"{settled} loads in 0.3s for one key with a 50ms TTL"


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
