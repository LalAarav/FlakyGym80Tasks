import random
import threading
import time

from src.clock import SystemClock
from src.journal.kvstate import KVState


def _no_thread_crashes(monkeypatch):
    crashes = []
    monkeypatch.setattr(threading, "excepthook", lambda args: crashes.append(args.exc_value))
    return crashes


def test_roundtrip_after_restart(tmp_path):
    s = KVState(str(tmp_path))
    s.put("alpha", b"1")
    s.put("beta", b"2")
    s.delete("alpha")
    with s.batch() as b:
        b.put("gamma", b"3").put("delta", b"")
    expected = {"beta": b"2", "gamma": b"3", "delta": b""}
    assert s.items() == expected
    # No close(): the process just goes away.
    assert KVState(str(tmp_path)).items() == expected


def test_restart_after_compaction(tmp_path):
    s = KVState(str(tmp_path))
    for i in range(300):
        s.put(f"user/{i % 40}", f"v{i}".encode())
    s.compact()
    for i in range(20):
        s.delete(f"user/{i}")
    s.compact()
    s.put("user/0", b"back")
    assert KVState(str(tmp_path)).items() == s.items()


def test_concurrent_writers_survive_restart(tmp_path, monkeypatch):
    crashes = _no_thread_crashes(monkeypatch)
    s = KVState(str(tmp_path))

    def writer(w):
        rng = random.Random()
        for i in range(15):
            s.put(f"topic/{rng.randrange(6)}", f"{w}-{i}".encode())
            time.sleep(rng.random() * 0.008)

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not crashes, f"a writer crashed: {crashes[0]!r}"
    assert KVState(str(tmp_path)).items() == s.items()


def test_compaction_while_writing(tmp_path, monkeypatch):
    crashes = _no_thread_crashes(monkeypatch)
    s = KVState(str(tmp_path))
    for i in range(150):
        s.put(f"sub/{i}", bytes([i]) * 900)
    stop = threading.Event()

    def writer():
        rng = random.Random()
        n = 0
        while not stop.is_set():
            n += 1
            if rng.random() < 0.3:
                s.put(f"new/{n}", b"x" * 200)
            else:
                s.put(f"sub/{rng.randrange(150)}", bytes([n % 256]) * 900)
            time.sleep(0.01 + rng.random() * 0.02)

    t = threading.Thread(target=writer)
    t.start()
    try:
        for _ in range(3):
            time.sleep(0.03)
            s.compact()
    finally:
        stop.set()
        t.join()
    assert not crashes, f"the writer crashed: {crashes[0]!r}"
    assert KVState(str(tmp_path)).items() == s.items()


def test_large_values_and_batches(tmp_path):
    rng = random.Random()
    s = KVState(str(tmp_path))
    expected = {}
    for i in range(12):
        with s.batch() as b:
            for j in range(rng.randrange(1, 6)):
                key = f"blob/{i}/{j}"
                value = rng.randbytes(rng.randrange(0, 150_000))
                b.put(key, value)
                expected[key] = value
    assert KVState(str(tmp_path)).items() == expected
    s.compact()
    assert KVState(str(tmp_path)).items() == expected


def test_unicode_keys(tmp_path):
    keys = ["подписка/1", "订阅/二", "café", "emoji/🙂", "tab\tand\nnewline"]
    s = KVState(str(tmp_path))
    for i, key in enumerate(keys):
        s.put(key, str(i).encode())
    s.compact()
    s.put(keys[0], b"updated")
    r = KVState(str(tmp_path))
    assert r.get(keys[0]) == b"updated"
    assert [r.get(k) for k in keys[1:]] == [b"1", b"2", b"3", b"4"]


def test_maybe_compact_bounds_the_log(tmp_path):
    s = KVState(str(tmp_path), compact_after_bytes=50_000, clock=SystemClock())
    compactions = 0
    for i in range(400):
        s.put(f"k{i % 10}", bytes([i % 256]) * 1000)
        compactions += s.maybe_compact()
    assert compactions >= 5
    assert s.log_bytes < 60_000
    assert KVState(str(tmp_path)).items() == s.items()
