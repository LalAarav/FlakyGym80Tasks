import json
import math
import random
import threading
import time

import pytest

from src.metrics.histogram import Histogram
from src.metrics.registry import Registry
from src.metrics.slo import BurnRateAlert
from src.metrics.window import RollingCounter

LATENCY_BOUNDS = [0.001 * 2 ** i for i in range(24)]


def _join_all(threads, timeout=30):
    for t in threads:
        t.join(timeout)
    assert not any(t.is_alive() for t in threads), "worker threads hung"


# --- histograms ----------------------------------------------------------------------

def test_histogram_counts_sum_and_quantiles():
    rng = random.Random()
    values = [rng.expovariate(20.0) for _ in range(5000)]
    h = Histogram(LATENCY_BOUNDS)
    for v in values:
        h.observe(v)
    snap = h.snapshot()
    assert snap.count == len(values) == sum(snap.counts)
    assert snap.total == pytest.approx(math.fsum(values), rel=1e-12)
    assert snap.minimum == min(values) and snap.maximum == max(values)
    ordered = sorted(values)
    for q in (0.5, 0.9, 0.99):
        est = h.quantile(q)
        true = ordered[int(q * len(ordered)) - 1]
        i = snap.bucket_of(true)
        lo = LATENCY_BOUNDS[i - 1] / 2 if i > 0 else 0.0
        hi = LATENCY_BOUNDS[i] * 2 if i < len(LATENCY_BOUNDS) else snap.maximum
        assert lo <= est <= hi, (q, est, true)
    assert h.quantile(0.0) == min(values)
    assert h.quantile(1.0) == max(values)


def test_histogram_concurrent_observe_and_merge():
    shards = [Histogram(LATENCY_BOUNDS) for _ in range(4)]
    combined = Histogram(LATENCY_BOUNDS)
    per_thread = 3000

    def work(h, seed):
        rng = random.Random(seed)
        for _ in range(per_thread):
            v = rng.uniform(0.0005, 2.0)
            h.observe(v)
            combined.observe(v)

    threads = [threading.Thread(target=work, args=(h, i)) for i, h in enumerate(shards)]
    for t in threads:
        t.start()
    _join_all(threads)
    merged = Histogram(LATENCY_BOUNDS)
    for h in shards:
        merged.merge(h)
    a, b = merged.snapshot(), combined.snapshot()
    assert a.counts == b.counts
    assert a.count == 4 * per_thread
    assert a.total == pytest.approx(b.total, rel=1e-12)
    assert (a.minimum, a.maximum) == (b.minimum, b.maximum)


# --- rolling windows -------------------------------------------------------------------

def test_window_counts_recent_events_and_expires():
    w = RollingCounter(slots=20, slot_seconds=0.05, width=2)
    sent = failed = 0
    deadline = time.monotonic() + 0.1
    while time.monotonic() < deadline:
        w.add(1, 1 if sent % 10 == 0 else 0)
        failed += 1 if sent % 10 == 0 else 0
        sent += 1
    assert w.totals() == (sent, failed)
    assert len(w.per_slot()) == 20
    # keep polling: the window drains slot by slot and ends empty
    give_up = time.monotonic() + 5.0
    last = w.total()
    while w.total() and time.monotonic() < give_up:
        now = w.total()
        assert now <= last
        last = now
        time.sleep(0.01)
    assert w.total() == 0


def test_window_concurrent_adds_are_exact():
    w = RollingCounter(slots=40, slot_seconds=0.05)
    n = 2000

    def work():
        for _ in range(n):
            w.add(1)

    threads = [threading.Thread(target=work) for _ in range(4)]
    started = time.monotonic()
    for t in threads:
        t.start()
    _join_all(threads)
    if time.monotonic() - started < 1.5:  # all adds still inside the 2 s window
        assert w.total() == 4 * n


# --- registry ----------------------------------------------------------------------

def test_registry_series_per_subscriber_under_concurrency():
    reg = Registry()
    workers = 4
    rounds = 10
    subs = 150
    barrier = threading.Barrier(workers)

    def dispatcher(seed):
        rng = random.Random(seed)
        for r in range(rounds):
            barrier.wait()
            for i in range(subs):
                labels = {"subscriber": f"sub-{r}-{i}", "topic": "orders"}
                reg.histogram("delivery_seconds", LATENCY_BOUNDS, labels).observe(rng.uniform(0.001, 0.5))
                reg.counter("deliveries_total", labels).inc()

    threads = [threading.Thread(target=dispatcher, args=(k,)) for k in range(workers)]
    for t in threads:
        t.start()
    _join_all(threads)
    snap = reg.snapshot()
    counts = {s.labels["subscriber"]: s.value for s in snap.series("deliveries_total")}
    assert len(counts) == rounds * subs
    short = {k: v for k, v in counts.items() if v != workers}
    assert not short, f"lost deliveries: {dict(list(short.items())[:5])}"
    lat = snap.series("delivery_seconds")
    assert len(lat) == rounds * subs
    assert all(s.value.count == workers for s in lat)


def test_registry_snapshot_respects_update_order():
    reg = Registry()
    workers = 2
    attempts = reg.counter("attempts_total")
    subs = [f"s{i}" for i in range(50)]
    for s in subs:
        reg.gauge("queue_depth", {"subscriber": s})
    delivered = {s: reg.counter("delivered_total", {"subscriber": s}) for s in subs}
    failed = {s: reg.counter("failed_total", {"subscriber": s}) for s in subs}
    latency = reg.histogram("delivery_seconds", LATENCY_BOUNDS)
    stop = threading.Event()

    def dispatcher(seed):
        rng = random.Random(seed)
        n = 0
        while not stop.is_set():
            s = rng.choice(subs)
            n += 1
            # build the request the way the dispatcher does before sending it
            event = {"id": f"{seed}-{n}", "subscriber": s, "body": "x" * rng.randrange(200),
                     "headers": {f"h{i}": str(rng.random()) for i in range(8)}}
            json.dumps(event, sort_keys=True)
            attempts.inc()
            reg.gauge("queue_depth", {"subscriber": s}).set(rng.randrange(10))
            if rng.random() < 0.9:
                latency.observe(rng.uniform(0.001, 0.2))
                delivered[s].inc()
            else:
                failed[s].inc()

    threads = [threading.Thread(target=dispatcher, args=(k,)) for k in range(workers)]
    for t in threads:
        t.start()
    try:
        time.sleep(0.01)
        for _ in range(3):
            snap = reg.snapshot()
            a = snap.value("attempts_total")
            ok = sum(x.value for x in snap.series("delivered_total"))
            bad = sum(x.value for x in snap.series("failed_total"))
            timed = snap.value("delivery_seconds").count
            # each dispatcher has at most one delivery in flight at any instant
            assert 0 <= a - (ok + bad) <= workers, f"{a} attempts but {ok + bad} outcomes"
            assert 0 <= timed - ok <= workers, f"{timed} timed deliveries but {ok} delivered"
    finally:
        stop.set()
        _join_all(threads)


def test_registry_rejects_kind_change():
    reg = Registry()
    reg.counter("events_total", {"a": "1"}).inc(3)
    with pytest.raises(ValueError):
        reg.gauge("events_total")
    with pytest.raises(ValueError):
        reg.counter("events_total").inc(-1)
    assert reg.snapshot().value("events_total", {"a": "1"}) == 3


# --- SLO alert ----------------------------------------------------------------------

def test_slo_alert_fires_on_burst_and_clears_after_recovery():
    seen = []
    alert = BurnRateAlert("99", fire_at=10, clear_at=2, long_slots=40, short_slots=4,
                          slot_seconds=0.1, min_events=20, on_transition=seen.append)
    for _ in range(30):
        alert.record_outcome(True)
    assert not alert.evaluate().firing
    for _ in range(40):
        alert.record_outcome(random.random() < 0.5)
    alert.record(bad=10)
    assert alert.evaluate().firing
    give_up = time.monotonic() + 5.0
    while alert.firing and time.monotonic() < give_up:
        for _ in range(20):
            alert.record_outcome(True)
        alert.evaluate()
        time.sleep(0.01)
    assert not alert.firing
    assert [t.kind for t in seen] == ["fire", "clear"]


def test_slo_concurrent_recorders_and_evaluators():
    seen = []
    alert = BurnRateAlert("99.5", fire_at=5, clear_at=1, long_slots=20, short_slots=4,
                          slot_seconds=0.05, min_events=5, on_transition=seen.append)
    stop = threading.Event()

    def recorder(seed):
        rng = random.Random(seed)
        while not stop.is_set():
            phase = int(time.monotonic() * 4) % 2  # alternate bad / good quarter-seconds
            alert.record_outcome(rng.random() > (0.3 if phase else 0.0))
            time.sleep(0.0005)

    def evaluator():
        while not stop.is_set():
            alert.evaluate()
            time.sleep(0.002)

    threads = [threading.Thread(target=recorder, args=(i,)) for i in range(2)]
    threads += [threading.Thread(target=evaluator) for _ in range(3)]
    for t in threads:
        t.start()
    time.sleep(1.0)
    stop.set()
    _join_all(threads)
    kinds = [t.kind for t in alert.transitions()]
    assert kinds == [t.kind for t in seen]
    assert all(a != b for a, b in zip(kinds, kinds[1:])), kinds
    assert not kinds or kinds[0] == "fire"
