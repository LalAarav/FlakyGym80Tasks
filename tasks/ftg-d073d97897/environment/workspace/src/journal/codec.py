"""Binary encoding of key-value operations.

A *batch* is the unit the journal makes durable and the unit replay applies: an
ordered list of operations that take effect together. Its encoding is::

    count:u32  op*            (big-endian throughout)

    op     := kind:u8  key_len:u16  key:utf-8[key_len]  [value_len:u32  value]
    kind   := 1 (put, carries a value) | 2 (delete, no value)

Snapshots reuse the same encoding for their body (a snapshot is a batch of puts),
so there is exactly one place that knows how keys and values are laid out.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Iterable, Iterator

PUT = 1
DELETE = 2

#: Keys are non-empty strings of at most this many characters (see docs/SPEC.md).
MAX_KEY_CHARS = 255
#: Values are byte strings of at most this many bytes; the empty value is valid.
MAX_VALUE_BYTES = 16 * 1024 * 1024

_COUNT = struct.Struct(">I")
_OP_HEAD = struct.Struct(">BH")  # op kind, length of the UTF-8 encoded key
_VALUE_LEN = struct.Struct(">I")


class DecodeError(ValueError):
    """Bytes that do not form a well-formed batch."""


@dataclass(frozen=True)
class Op:
    """One operation of a batch. ``value`` is ``None`` exactly for deletes."""

    kind: int
    key: str
    value: bytes | None = None


def put_op(key: str, value: bytes) -> Op:
    op = Op(PUT, key, bytes(value) if isinstance(value, (bytes, bytearray)) else value)
    validate(op)
    return op


def delete_op(key: str) -> Op:
    op = Op(DELETE, key)
    validate(op)
    return op


def validate(op: Op) -> None:
    """Reject an operation the store cannot represent, before anything is written.

    Raises ``TypeError`` for a key that is not a ``str`` or a value that is not
    ``bytes``, and ``ValueError`` for an empty or over-long key, a key that is not
    valid Unicode text (lone surrogates), an over-long value or an unknown kind.
    """
    if not isinstance(op.key, str):
        raise TypeError("keys must be str")
    if not op.key or len(op.key) > MAX_KEY_CHARS:
        raise ValueError(f"key must be 1..{MAX_KEY_CHARS} characters long")
    try:
        op.key.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"key is not valid Unicode text: {exc}") from None
    if op.kind == PUT:
        if not isinstance(op.value, bytes):
            raise TypeError("values must be bytes")
        if len(op.value) > MAX_VALUE_BYTES:
            raise ValueError(f"value longer than {MAX_VALUE_BYTES} bytes")
    elif op.kind == DELETE:
        if op.value is not None:
            raise ValueError("a delete carries no value")
    else:
        raise ValueError(f"unknown operation kind {op.kind!r}")


def encode_op(op: Op) -> bytes:
    raw_key = op.key.encode("utf-8")
    head = _OP_HEAD.pack(op.kind, len(raw_key)) + raw_key
    if op.kind == PUT:
        return head + _VALUE_LEN.pack(len(op.value)) + op.value
    return head


def encode_ops(ops: Iterable[Op]) -> bytes:
    """Encode a whole batch. The caller has validated every operation."""
    body = [encode_op(op) for op in ops]
    return _COUNT.pack(len(body)) + b"".join(body)


def iter_ops(payload: bytes) -> Iterator[Op]:
    """Decode a batch lazily, yielding each operation as soon as it is complete.

    Raises ``DecodeError`` when the payload is malformed: a truncated operation, an
    unknown kind, a key that is not UTF-8, fewer operations than the count promises,
    or trailing bytes after the last one. Operations already yielded before the
    error were decoded from a prefix of the payload; callers that need the batch as
    a whole must use :func:`decode_ops`.
    """
    if len(payload) < _COUNT.size:
        raise DecodeError("batch shorter than its operation count")
    (count,) = _COUNT.unpack_from(payload, 0)
    offset = _COUNT.size
    for _ in range(count):
        if offset + _OP_HEAD.size > len(payload):
            raise DecodeError("truncated operation header")
        kind, key_len = _OP_HEAD.unpack_from(payload, offset)
        offset += _OP_HEAD.size
        if offset + key_len > len(payload):
            raise DecodeError("truncated key")
        try:
            key = payload[offset:offset + key_len].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise DecodeError(f"key is not UTF-8: {exc}") from None
        offset += key_len
        if kind == PUT:
            if offset + _VALUE_LEN.size > len(payload):
                raise DecodeError("truncated value length")
            (value_len,) = _VALUE_LEN.unpack_from(payload, offset)
            offset += _VALUE_LEN.size
            if offset + value_len > len(payload):
                raise DecodeError("truncated value")
            yield Op(PUT, key, payload[offset:offset + value_len])
            offset += value_len
        elif kind == DELETE:
            yield Op(DELETE, key)
        else:
            raise DecodeError(f"unknown operation kind {kind}")
    if offset != len(payload):
        raise DecodeError(f"{len(payload) - offset} trailing bytes after the last operation")


def decode_ops(payload: bytes) -> list[Op]:
    """Decode a complete batch, all or nothing."""
    return list(iter_ops(payload))
