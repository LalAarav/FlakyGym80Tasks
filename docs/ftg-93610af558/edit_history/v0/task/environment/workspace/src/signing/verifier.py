"""Verification of incoming webhook deliveries.

``Verifier.verify(headers, body)`` accepts a delivery only if all of these hold
(docs/SPEC.md, S9-S12):

1. the four signature headers are present once each and well formed;
2. the timestamp is within ``tolerance`` seconds of the verifier's wall clock;
3. the key id names a key the keyring currently accepts for verification;
4. the HMAC matches the *received body bytes* (never a re-encoding of them);
5. the body is UTF-8 JSON;
6. the nonce has not been accepted before within the replay retention period.

Only a delivery that passes 1-5 is recorded in the replay cache, and it is
recorded with an atomic check-and-insert, so of several concurrent copies of one
delivery exactly one is accepted and a rejected delivery never affects another.
"""

from __future__ import annotations

import hmac
import json
from dataclasses import dataclass
from typing import Any, Mapping

from src.clock import SystemClock
from src.signing.errors import (BadSignature, MalformedBody, MalformedSignature, ReplayedMessage,
                        StaleMessage, VerificationError)
from src.signing.keyring import Keyring
from src.signing.replay import ReplayCache
from src.signing.signer import (HEADER_KEY_ID, HEADER_NONCE, HEADER_SIGNATURE, HEADER_TIMESTAMP,
                        HEADERS, SCHEME, compute_signature, signed_message)
from src.stats import Stats

MAX_TIMESTAMP_DIGITS = 15
_HEX = frozenset("0123456789abcdef")


@dataclass(frozen=True)
class Verified:
    """An accepted delivery."""

    key_id: str
    timestamp: int
    nonce: str
    payload: Any


@dataclass(frozen=True)
class _Fields:
    key_id: str
    timestamp: str
    nonce: str
    signature: str


class Verifier:
    """Checks signatures made by ``Signer`` against a shared or replicated keyring.

    ``tolerance`` bounds clock skew plus delivery delay. Each accepted nonce is
    remembered for ``2 * tolerance`` seconds of monotonic time: long enough to cover
    the whole interval during which the same timestamp could pass the freshness
    check, without depending on the wall clock that freshness itself uses.
    """

    def __init__(self, keyring: Keyring, tolerance: float = 300.0,
                 clock: SystemClock | None = None, replay: ReplayCache | None = None,
                 stats: Stats | None = None):
        if tolerance <= 0:
            raise ValueError("tolerance must be > 0")
        self.keyring = keyring
        self.tolerance = float(tolerance)
        self.clock = clock if clock is not None else SystemClock()
        self.replay = replay if replay is not None else ReplayCache(clock=self.clock)
        self.stats = stats if stats is not None else Stats()

    def verify(self, headers: Mapping[str, str], body: bytes) -> Verified:
        """Return the verified delivery or raise a ``VerificationError`` subclass."""
        try:
            result = self._verify(headers, bytes(body))
        except VerificationError as exc:
            self.stats.incr(f"rejected.{exc.reason}")
            raise
        self.stats.incr("verified")
        return result

    def _verify(self, headers: Mapping[str, str], body: bytes) -> Verified:
        fields = _parse_headers(headers)
        timestamp = int(fields.timestamp)
        self._check_fresh(timestamp)
        # Cheap rejection of replays before spending an HMAC on them.
        if self.replay.seen(fields.nonce):
            raise ReplayedMessage(f"nonce {fields.nonce} was already accepted")
        secret = self.keyring.verification_secret(fields.key_id)
        message = signed_message(fields.key_id, fields.timestamp, fields.nonce, body)
        if not hmac.compare_digest(compute_signature(secret, message), fields.signature):
            raise BadSignature("signature does not match the body")
        payload = _parse_body(body)
        retain_until = self.clock.monotonic() + 2 * self.tolerance
        self.replay.add(fields.nonce, retain_until)  # checked above
        return Verified(fields.key_id, timestamp, fields.nonce, payload)

    def _check_fresh(self, timestamp: int) -> None:
        now = self.clock.wall()
        if not timestamp - self.tolerance <= now <= timestamp + self.tolerance:
            raise StaleMessage(f"timestamp {timestamp} is outside +/-{self.tolerance:g}s "
                               f"of now ({now:.3f})")


def _parse_headers(headers: Mapping[str, str]) -> _Fields:
    """Find the four headers case-insensitively and validate their syntax."""
    wanted = {name.lower(): name for name in HEADERS}
    found: dict[str, str] = {}
    for name, value in headers.items():
        canonical_name = wanted.get(str(name).lower())
        if canonical_name is None:
            continue
        if canonical_name in found:
            raise MalformedSignature(f"header {canonical_name} given more than once")
        if not isinstance(value, str):
            raise MalformedSignature(f"header {canonical_name} is not a string")
        found[canonical_name] = value
    missing = [name for name in HEADERS if name not in found]
    if missing:
        raise MalformedSignature(f"missing header(s): {', '.join(missing)}")

    key_id = found[HEADER_KEY_ID]
    timestamp = found[HEADER_TIMESTAMP]
    nonce = found[HEADER_NONCE]
    signature = found[HEADER_SIGNATURE]
    if not key_id or len(key_id) > 64 or not key_id.isascii() or "\n" in key_id:
        raise MalformedSignature("malformed key id")
    if not (0 < len(timestamp) <= MAX_TIMESTAMP_DIGITS and timestamp.isascii()
            and timestamp.isdigit()):
        raise MalformedSignature("timestamp must be ASCII decimal digits")
    if len(nonce) != 32 or not set(nonce) <= _HEX:
        raise MalformedSignature("nonce must be 32 lowercase hex characters")
    prefix = SCHEME + "="
    digest = signature[len(prefix):]
    if not signature.startswith(prefix) or len(digest) != 64 or not set(digest) <= _HEX:
        raise MalformedSignature(f"signature must be {prefix}<64 lowercase hex>")
    return _Fields(key_id, timestamp, nonce, signature)


def _parse_body(body: bytes) -> Any:
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise MalformedBody(f"body is not UTF-8 JSON: {exc}") from exc
