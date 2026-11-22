"""Signing of outgoing webhook deliveries.

A signed delivery is the canonical encoding of the payload (the HTTP body) plus
four headers (docs/SPEC.md, S4)::

    Sigil-Key-Id:    id of the key that produced the signature
    Sigil-Timestamp: signing time, whole seconds since the Unix epoch (wall clock)
    Sigil-Nonce:     32 lowercase hex characters, unique per delivery
    Sigil-Signature: "v1=" + lowercase hex HMAC-SHA256(secret, signed message)

and the signed message is the byte string::

    b"sigil-v1\\n" + key_id + b"\\n" + timestamp + b"\\n" + nonce + b"\\n" + body

with the three header values in ASCII exactly as sent. The timestamp is wall-clock
time by nature: the receiver compares it with its own calendar clock.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from src.signing import canonical
from src.clock import SystemClock
from src.signing.keyring import Keyring
from src.stats import Stats

SCHEME = "v1"
HEADER_KEY_ID = "Sigil-Key-Id"
HEADER_TIMESTAMP = "Sigil-Timestamp"
HEADER_NONCE = "Sigil-Nonce"
HEADER_SIGNATURE = "Sigil-Signature"
HEADERS = (HEADER_KEY_ID, HEADER_TIMESTAMP, HEADER_NONCE, HEADER_SIGNATURE)


def signed_message(key_id: str, timestamp: str, nonce: str, body: bytes) -> bytes:
    """The exact bytes the HMAC covers (shared by signer and verifier)."""
    return b"\n".join((b"sigil-" + SCHEME.encode("ascii"), key_id.encode("ascii"),
                       timestamp.encode("ascii"), nonce.encode("ascii"), body))


def compute_signature(secret: bytes, message: bytes) -> str:
    """The ``Sigil-Signature`` header value for ``message``."""
    return f"{SCHEME}={hmac.new(secret, message, hashlib.sha256).hexdigest()}"


@dataclass(frozen=True)
class SignedMessage:
    """A delivery ready to send: ``body`` goes out byte for byte as the HTTP body."""

    headers: dict[str, str]
    body: bytes

    @property
    def key_id(self) -> str:
        return self.headers[HEADER_KEY_ID]

    @property
    def nonce(self) -> str:
        return self.headers[HEADER_NONCE]


def _random_nonce() -> str:
    return uuid.uuid4().hex


class Signer:
    """Signs payloads with the keyring's active key.

    Safe to share between threads and to use while the keyring rotates: each
    signature is produced with a key id and secret taken together from one
    keyring snapshot.
    """

    def __init__(self, keyring: Keyring, clock: SystemClock | None = None,
                 nonce_factory: Callable[[], str] | None = None, stats: Stats | None = None):
        self.keyring = keyring
        self.clock = clock if clock is not None else SystemClock()
        self.nonce_factory = nonce_factory if nonce_factory is not None else _random_nonce
        self.stats = stats if stats is not None else Stats()

    def sign(self, payload: Any) -> SignedMessage:
        """Canonically encode ``payload`` and sign it. Raises ``CanonicalError`` for
        values outside the canonical form, ``NoActiveKey`` before any activation."""
        key = self.keyring.signing_key()
        body = canonical.encode(payload)
        return self._finish(key.key_id, key.secret, body)

    def sign_body(self, body: bytes) -> SignedMessage:
        """Sign already-encoded body bytes (the caller vouches they are canonical)."""
        key = self.keyring.signing_key()
        return self._finish(key.key_id, key.secret, bytes(body))

    def _finish(self, key_id: str, secret: bytes, body: bytes) -> SignedMessage:
        timestamp = str(math.floor(self.clock.wall()))
        nonce = self.nonce_factory()
        message = signed_message(key_id, timestamp, nonce, body)
        headers = {
            HEADER_KEY_ID: key_id,
            HEADER_TIMESTAMP: timestamp,
            HEADER_NONCE: nonce,
            HEADER_SIGNATURE: compute_signature(secret, message),
        }
        self.stats.incr("signed")
        return SignedMessage(headers, body)
