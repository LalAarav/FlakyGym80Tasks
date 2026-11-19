import collections
import time
import threading

from src.rpc.breaker import CircuitBreaker
from src.rpc.client import RpcClient
from src.rpc.conn import Dialer
from src.rpc.limiter import TokenBucket
from src.rpc.pool import ConnectionPool

from tests_flaky.server import FaultyServer


def client_for(server, max_size=4, **kw):
    pool = ConnectionPool(Dialer(), server.address, max_size=max_size)
    kw.setdefault("attempt_timeout", 0.2)
    return RpcClient(pool, **kw)


def test_unsafe_calls_survive_fragmented_responses():
    with FaultyServer(split=0.002) as server:
        client = client_for(server)
        results = [client.call("incr") for _ in range(60)]
        assert results == list(range(1, 61))


def test_concurrent_idempotent_calls():
    with FaultyServer(split=0.02, delay=0.003) as server:
        client = client_for(server, max_size=3)
        out = collections.defaultdict(list)

        def worker(w):
            for i in range(25):
                out[w].append(client.call("echo", [w, i], idempotent=True, deadline=3.0))

        threads = [threading.Thread(target=worker, args=(w,)) for w in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for w in range(4):
            assert out[w] == [[w, i] for i in range(25)]


def test_deduplicated_increments_apply_once():
    with FaultyServer(drop=0.004, delay=0.004) as server:
        client = client_for(server, max_size=3,
                            breaker=CircuitBreaker(failure_threshold=4, reset_timeout=0.05))
        results = []
        lock = threading.Lock()

        def worker():
            for _ in range(30):
                r = client.call("incr", dedupe=True, deadline=3.0)
                with lock:
                    results.append(r)

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(results) == list(range(1, 91))
        assert server.counter == 90


def test_notes_are_never_applied_twice():
    with FaultyServer(delay=0.006) as server:
        client = client_for(server, max_size=1)
        for i in range(40):
            client.call("echo", i, idempotent=True, deadline=3.0)
            try:
                client.call("note", f"note-{i}")
            except TimeoutError:
                pass  # a slow response: the note may or may not have been applied
        counts = collections.Counter(server.notes)
        assert all(c == 1 for c in counts.values()), counts.most_common(3)


def test_rate_limiter_caps_admissions():
    bucket = TokenBucket(rate=0.0, burst=200)
    admitted = []
    lock = threading.Lock()

    def worker():
        n = sum(1 for _ in range(100) if bucket.try_acquire())
        with lock:
            admitted.append(n)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(admitted) == 200


def test_breaker_recovers_after_outage():
    with FaultyServer(drop=1.0) as server:
        breaker = CircuitBreaker(failure_threshold=3, reset_timeout=0.05)
        client = client_for(server, breaker=breaker)
        for _ in range(3):
            try:
                client.call("incr")
            except ConnectionError:
                pass
        assert breaker.state == "open"
        server.drop = 0.0

        time.sleep(0.08)
        assert client.call("echo", "back", idempotent=True) == "back"
        assert breaker.state == "closed"
