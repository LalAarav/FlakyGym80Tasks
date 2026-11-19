"""The append-only write-ahead log: record framing, appending and replay.

Each batch is written as one or more *frames*. A frame is::

    magic:2s  flags:u8  seq:u64  length:u32  crc:u32  body[length]

``seq`` is the batch's sequence number (every frame of a batch carries the same
one), ``length`` is the size of this frame's body, and ``flags`` has bit 0 set on
the final frame of a batch. ``crc`` is the CRC-32 of every other byte of the frame,
header and body alike. A batch's encoded operations (see ``src.journal.codec``) are cut into
bodies of at most ``FRAME_PAYLOAD`` bytes, so a frame never needs more than a fixed
amount of memory to verify, however large the batch.

Replay walks the frames in order and hands back only *complete* batches: a batch
whose final frame never made it to disk, or any frame that fails verification,
ends replay at the start of that batch. Everything from there on is the damaged
tail, which recovery cuts away before the log takes new appends.
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass, field

from src.journal import codec
from src.fsops import FsOps

MAGIC = b"JL"
FLAG_LAST = 0x01
#: Largest body of a single frame. Batches bigger than this span several frames.
FRAME_PAYLOAD = 64 * 1024

_HEAD = struct.Struct(">2sBQI")  # magic, flags, seq, body length
_CRC = struct.Struct(">I")
HEADER_SIZE = _HEAD.size + _CRC.size


class JournalCorrupt(Exception):
    """On-disk state that recovery cannot interpret without losing committed data."""


def _checksum(head: bytes, body: bytes) -> int:
    """CRC-32 over the frame's header fields (everything but the checksum) and body."""
    return zlib.crc32(body, zlib.crc32(head))


def _frame(seq: int, last: bool, body: bytes) -> bytes:
    head = _HEAD.pack(MAGIC, FLAG_LAST if last else 0, seq, len(body))
    return head + _CRC.pack(_checksum(head, body)) + body


def encode_batch(seq: int, payload: bytes) -> bytes:
    """All frames of batch ``seq`` holding ``payload``, ready for a single append."""
    frames = []
    offset = 0
    while offset < len(payload):
        body = payload[offset:offset + FRAME_PAYLOAD]
        offset += len(body)
        # Only the final frame of a batch can be shorter than FRAME_PAYLOAD.
        frames.append(_frame(seq, len(body) < FRAME_PAYLOAD, body))
    return b"".join(frames)


@dataclass
class ScanResult:
    """What replay found in one log file."""

    #: Complete, verified batches in log order: (seq, operations).
    batches: list[tuple[int, list[codec.Op]]] = field(default_factory=list)
    #: Offset just past the last complete batch; bytes beyond it are the damaged tail.
    valid_end: int = 0
    #: Number of bytes in the file.
    total: int = 0

    @property
    def damaged(self) -> bool:
        return self.valid_end < self.total

    @property
    def last_seq(self) -> int | None:
        return self.batches[-1][0] if self.batches else None


def scan(data: bytes, after_seq: int) -> ScanResult:
    """Replay the frames in ``data``.

    ``after_seq`` is the sequence number of the last batch already applied (from the
    snapshot or an earlier log); batch sequence numbers must keep increasing past it.
    Replay stops at the first frame that is incomplete, fails verification, breaks the
    sequence, or completes a batch that does not decode; that batch and everything after
    it are reported as the damaged tail.
    """
    result = ScanResult(total=len(data))
    last_seq = after_seq
    offset = 0
    pending_seq: int | None = None
    parts: list[bytes] = []
    while offset < len(data):
        if len(data) - offset < HEADER_SIZE:
            break
        magic, flags, seq, length = _HEAD.unpack_from(data, offset)
        (crc,) = _CRC.unpack_from(data, offset + _HEAD.size)
        if magic != MAGIC or flags & ~FLAG_LAST or length > FRAME_PAYLOAD:
            break
        body_start = offset + HEADER_SIZE
        end = body_start + length
        if end > len(data):
            break
        body = data[body_start:end]
        if _checksum(data[offset:offset + _HEAD.size], body) != crc:
            break
        if pending_seq is None:
            if seq <= last_seq:
                break
            pending_seq = seq
        elif seq != pending_seq:
            break
        parts.append(body)
        offset = end
        if flags & FLAG_LAST:
            try:
                ops = codec.decode_ops(b"".join(parts))
            except codec.DecodeError:
                break
            result.batches.append((seq, ops))
            result.valid_end = offset
            last_seq = seq
            pending_seq = None
            parts = []
    if parts:
        # Replay stopped inside a batch whose earlier frames reached the disk and
        # verified. Keep the operations those frames hold completely instead of
        # throwing away work that was already fsynced.
        salvaged = []
        try:
            for op in codec.iter_ops(b"".join(parts)):
                salvaged.append(op)
        except codec.DecodeError:
            pass
        if salvaged:
            result.batches.append((pending_seq, salvaged))
    return result


class Journal:
    """Appends batches to one log file.

    Not internally locked: the owner (``KVState``) serializes every append and the
    rotation that replaces one ``Journal`` with the next, so that the order of frames
    in the file is the order in which batches were applied.
    """

    def __init__(self, fs: FsOps, path: str, size: int):
        self.fs = fs
        self.path = path
        self.size = size

    @classmethod
    def create(cls, fs: FsOps, path: str) -> "Journal":
        """Start a brand-new, empty log at ``path`` (which must not exist yet)."""
        fs.create_exclusive(path, b"")
        return cls(fs, path, 0)

    def append(self, seq: int, payload: bytes) -> int:
        """Durably append batch ``seq``; returns the number of bytes written.

        The frames go to disk in a single ``append_bytes`` call that fsyncs before
        returning, so once this returns the batch survives a crash.
        """
        data = encode_batch(seq, payload)
        self.fs.append_bytes(self.path, data)
        self.size += len(data)
        return len(data)
