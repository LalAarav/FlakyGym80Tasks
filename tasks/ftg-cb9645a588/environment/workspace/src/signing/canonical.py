"""Canonical JSON encoding of webhook payloads.

The canonical form is a byte-level contract (docs/SPEC.md, S1-S3). A sender and a
receiver that encode the same JSON *value* must produce the same bytes, whatever
dict insertion order, numeric spelling or string escaping they started from, and
whatever language they are written in. The signature covers these bytes, so a
difference of a single byte is a verification failure.

The output is a strict subset of RFC 8259 JSON: any JSON parser reads it back to
an equal value.

Summary of the rules (SPEC.md is authoritative):

* no whitespace anywhere outside strings;
* object members ordered by their key's UTF-16 code units (the ordering used by
  RFC 8785 and by JavaScript's default sort, so non-Python receivers agree);
* strings written as raw UTF-8 with the minimal escapes;
* a number is written by its value: integral values as an exact decimal integer,
  other finite floats in Python's shortest round-trip ``repr``;
* ``true``, ``false``, ``null`` for ``True``, ``False``, ``None``.
"""

from __future__ import annotations

import math
from typing import Any

from src.signing.errors import CanonicalError

MAX_DEPTH = 64

# Minimal escaping: the quote, the backslash and the C0 control characters. The five
# controls with a short JSON escape use it; the rest are \u00XX with lowercase hex.
# Everything else, including "/", DEL and every non-ASCII character, is written raw.
_ESCAPES = {'"': '\\"', "\\": "\\\\", "\b": "\\b", "\f": "\\f", "\n": "\\n",
            "\r": "\\r", "\t": "\\t"}
for _code in range(0x20):
    _ESCAPES.setdefault(chr(_code), f"\\u{_code:04x}")
_ESCAPE_TABLE = str.maketrans(_ESCAPES)


def encode(value: Any) -> bytes:
    """Return the canonical UTF-8 encoding of ``value``.

    Accepts ``None``, ``bool``, ``int``, ``float``, ``str``, ``list`` / ``tuple``
    (both become arrays) and ``dict`` with ``str`` keys, nested at most
    ``MAX_DEPTH`` containers deep. Anything else raises ``CanonicalError``.
    """
    parts: list[str] = []
    _emit(value, parts, 0)
    # Every string was checked by _string, so the text is valid Unicode.
    return "".join(parts).encode("utf-8")


def _emit(value: Any, out: list[str], depth: int) -> None:
    if value is None:
        out.append("null")
    elif isinstance(value, bool):  # before int: bool is a subclass of int
        out.append("true" if value else "false")
    elif isinstance(value, int):
        out.append(_integer(int(value)))
    elif isinstance(value, float):
        out.append(_number(value))
    elif isinstance(value, str):
        out.append(_string(value))
    elif isinstance(value, (list, tuple)):
        _check_depth(depth)
        out.append("[")
        for index, item in enumerate(value):
            if index:
                out.append(",")
            _emit(item, out, depth + 1)
        out.append("]")
    elif isinstance(value, dict):
        _check_depth(depth)
        for key in value:
            if not isinstance(key, str):
                raise CanonicalError(f"object keys must be strings, got {key!r}")
        encoded = {key: _string(key) for key in value}  # also rejects bad keys early
        out.append("{")
        for index, key in enumerate(sorted(value, key=_key_order)):
            if index:
                out.append(",")
            out.append(encoded[key])
            out.append(":")
            _emit(value[key], out, depth + 1)
        out.append("}")
    else:
        raise CanonicalError(f"cannot encode value of this kind: {value!r}")


def _check_depth(depth: int) -> None:
    if depth >= MAX_DEPTH:
        raise CanonicalError(f"containers nested deeper than {MAX_DEPTH}")


def _key_order(key: str) -> bytes:
    """Sort key for object members: the key's UTF-16 code units.

    Big-endian UTF-16 bytes compare exactly like the sequence of 16-bit code units,
    so ``bytes`` comparison gives the ordering without a custom comparator.
    """
    return key.encode("utf-16-be")


def _integer(value: int) -> str:
    try:
        return str(value)
    except ValueError as exc:  # beyond the interpreter's int-to-str digit limit
        raise CanonicalError("integer too large to encode") from exc


def _number(value: float) -> str:
    if not math.isfinite(value):
        raise CanonicalError(f"non-finite number {value!r} has no JSON form")
    # Integral floats are written as the integer they equal, so 1, 1.0 and 1e0 (and
    # 0.0 and -0.0) coincide: the encoding depends on the value, not its spelling.
    if value.is_integer():
        return _integer(int(value))
    return repr(value)


def _string(text: str) -> str:
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CanonicalError("string contains a lone surrogate") from exc
    return '"' + text.translate(_ESCAPE_TABLE) + '"'
