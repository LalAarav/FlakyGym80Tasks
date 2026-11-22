"""Exception hierarchy for sigil.

Every exception raised on purpose by this package derives from ``SigilError``.
Verification failures all derive from ``VerificationError`` so a webhook receiver
can reject a delivery with a single ``except`` clause and still log the precise
reason (``exc.reason``) without leaking it to the sender.
"""

from __future__ import annotations


class SigilError(Exception):
    """Base class for every error raised by this package."""


class CanonicalError(SigilError, ValueError):
    """The value cannot be represented in the canonical encoding."""


class KeyringError(SigilError):
    """A keyring operation was refused (bad key id, bad secret, invalid transition)."""


class KeyringCorrupt(KeyringError):
    """The persisted keyring file exists but cannot be parsed."""


class NoActiveKey(KeyringError):
    """Signing was requested before any key was activated."""


class VerificationError(SigilError):
    """A delivery did not pass verification. ``reason`` is a short machine tag."""

    reason = "invalid"


class MalformedSignature(VerificationError):
    """Signature headers are missing, duplicated or not in the documented format."""

    reason = "malformed"


class StaleMessage(VerificationError):
    """The signature timestamp is outside the verifier's tolerance window."""

    reason = "stale"


class UnknownKey(VerificationError, KeyringError):
    """No key with that id is held (never added, or retired)."""

    reason = "unknown_key"


class KeyNotAccepted(VerificationError):
    """The key exists but may not verify: still staged, or its overlap window ended."""

    reason = "key_not_accepted"


class BadSignature(VerificationError):
    """The HMAC does not match the received bytes."""

    reason = "bad_signature"


class MalformedBody(VerificationError):
    """The body is correctly signed but is not UTF-8 JSON."""

    reason = "malformed_body"


class ReplayedMessage(VerificationError):
    """A message with this nonce was already accepted within the retention period."""

    reason = "replay"


class ReplayCacheFull(VerificationError):
    """Accepting the message would require forgetting a nonce that is still retained."""

    reason = "replay_cache_full"
