import random
import string
import threading
import time

import pytest

from src.signing import canonical
from src.clock import SystemClock
from src.signing.errors import KeyNotAccepted, StaleMessage, VerificationError
from src.signing.keyring import Keyring
from src.signing.signer import Signer
from src.signing.verifier import Verifier


def secret(i):
    return (f"secret-{i:06d}-" * 4).encode()[:32]


def random_payload(rng, depth=0):
    kind = rng.randrange(6 if depth < 3 else 4)
    if kind == 0:
        return rng.randrange(-10**6, 10**6)
    if kind == 1:
        return rng.choice([True, False, None, 0.5, -1.25, 3.0])
    if kind == 2 or kind == 3:
        return "".join(rng.choice(string.ascii_letters + " -_") for _ in range(rng.randrange(12)))
    if kind == 4:
        return [random_payload(rng, depth + 1) for _ in range(rng.randrange(4))]
    keys = ["".join(rng.choice(string.ascii_lowercase) for _ in range(rng.randrange(1, 6)))
            for _ in range(rng.randrange(5))]
    return {k: random_payload(rng, depth + 1) for k in keys}


class OffsetClock(SystemClock):
    def __init__(self, offset):
        self.offset = offset

    def wall(self):
        return super().wall() + self.offset


def test_sign_verify_round_trip(tmp_path):
    ring = Keyring(str(tmp_path / "keys"))
    ring.rotate("k1", secret(1))
    signer, verifier = Signer(ring), Verifier(ring)
    rng = random.Random()
    for _ in range(300):
        payload = {"event": "x", "data": random_payload(rng)}
        msg = signer.sign(payload)
        assert canonical.encode(verifier.verify(msg.headers, msg.body).payload) == msg.body


def test_tampering_is_detected(tmp_path):
    ring = Keyring(str(tmp_path / "keys"))
    ring.rotate("k1", secret(1))
    signer, verifier = Signer(ring), Verifier(ring)
    msg = signer.sign({"amount": 10, "to": "alice"})
    with pytest.raises(VerificationError):
        verifier.verify(msg.headers, msg.body.replace(b"10", b"99"))
    headers = dict(msg.headers, **{"Sigil-Timestamp": str(int(msg.headers["Sigil-Timestamp"]) + 1)})
    with pytest.raises(VerificationError):
        verifier.verify(headers, msg.body)
    verifier.verify({k.lower(): v for k, v in msg.headers.items()}, msg.body)


def test_old_messages_are_stale(tmp_path):
    ring = Keyring(str(tmp_path / "keys"))
    ring.rotate("k1", secret(1))
    old = Signer(ring, clock=OffsetClock(-120)).sign({"n": 1})
    with pytest.raises(StaleMessage):
        Verifier(ring, tolerance=60).verify(old.headers, old.body)
    recent = Signer(ring, clock=OffsetClock(-30)).sign({"n": 2})
    Verifier(ring, tolerance=60).verify(recent.headers, recent.body)


def test_previous_key_verifies_during_overlap_only(tmp_path):
    ring = Keyring(str(tmp_path / "keys"), overlap=3.0)
    ring.rotate("k1", secret(1))
    signer, verifier = Signer(ring), Verifier(ring)
    before = signer.sign({"n": 1})
    ring.rotate("k2", secret(2))
    verifier.verify(before.headers, before.body)
    late = Signer(ring).sign({"n": 2})
    assert late.key_id == "k2"
    time.sleep(3.1)
    with pytest.raises(KeyNotAccepted):
        ring.verification_secret("k1")
    verifier.verify(late.headers, late.body)


def test_keyring_survives_restart(tmp_path):
    root = str(tmp_path / "keys")
    ring = Keyring(root)
    for i in range(5):
        ring.rotate(f"k{i}", secret(i))
    ring.retire("k0")
    again = Keyring(root)
    assert again.active_key_id == "k4"
    assert again.signing_key().secret == secret(4)
    assert sorted(k.key_id for k in again.keys()) == ["k1", "k2", "k3", "k4"]


def test_signing_while_keys_rotate(tmp_path, monkeypatch):
    crashes = []
    monkeypatch.setattr(threading, "excepthook", lambda args: crashes.append(args.exc_value))
    ring = Keyring(str(tmp_path / "keys"), overlap=600)
    ring.rotate("k0", secret(0))
    signer, verifier = Signer(ring), Verifier(ring)
    done = threading.Event()
    failures = []
    payload = {"items": [{"sku": f"sku-{i}", "qty": i, "price": i + 0.5} for i in range(30)]}

    def rotator():
        i = 0
        while not done.is_set() and i < 150:
            i += 1
            ring.rotate(f"k{i}", secret(i))
            time.sleep(0.002)

    def sender():
        for _ in range(400):
            msg = signer.sign(payload)
            try:
                verifier.verify(msg.headers, msg.body)
            except VerificationError as exc:
                failures.append(exc)

    rot = threading.Thread(target=rotator)
    senders = [threading.Thread(target=sender) for _ in range(2)]
    rot.start()
    for t in senders:
        t.start()
    for t in senders:
        t.join(60)
    done.set()
    rot.join(60)
    assert not crashes, crashes[:1]
    assert not failures, f"{len(failures)} fresh messages failed to verify: {failures[0]!r}"


def test_duplicate_deliveries_are_accepted_once(tmp_path):
    ring = Keyring(str(tmp_path / "keys"))
    ring.rotate("k1", secret(1))
    signer, verifier = Signer(ring), Verifier(ring)
    rng = random.Random()
    double = []
    for i in range(60):
        size = 2500 if i == 30 else rng.choice([10, 100, 1000])
        msg = signer.sign({"seq": i, "blob": "x" * size})
        accepted = []
        start = threading.Barrier(2, timeout=10)

        def deliver():
            start.wait()
            try:
                verifier.verify(msg.headers, msg.body)
                accepted.append(1)
            except VerificationError:
                pass

        threads = [threading.Thread(target=deliver) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        if len(accepted) != 1:
            double.append((i, len(accepted)))
    assert not double, f"deliveries accepted more than once: {double}"


def test_concurrent_rotations_are_persisted(tmp_path):
    root = str(tmp_path / "keys")
    ring = Keyring(root)
    ring.rotate("seed", secret(0))
    errors = []

    def rotate_many(prefix):
        time.sleep(random.random() * 0.01)
        for i in range(2):
            try:
                ring.rotate(f"{prefix}{i}", secret(i))
            except Exception as exc:  # noqa: BLE001 - collected for the assertion
                errors.append(exc)

    threads = [threading.Thread(target=rotate_many, args=(p,)) for p in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert not errors, errors[:1]
    again = Keyring(root)
    assert again.active_key_id == ring.active_key_id
    assert len(again.keys()) == 5
