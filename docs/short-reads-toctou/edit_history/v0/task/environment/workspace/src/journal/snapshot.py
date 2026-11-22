"""Snapshots, compaction switch-over and crash recovery.

The store's directory holds numbered *generations*. Generation ``g`` consists of an
optional snapshot ``snapshot-<g>.snap`` (the complete state as of the moment log
``g`` was started) and a log ``log-<g>.wal`` (every batch committed after that
moment, until the next rotation). Generation 0 has no snapshot: it starts empty.

Compaction moves the store from generation ``g`` to ``g + 1``:

1. Under the store lock: copy the state and start the empty ``log-<g+1>.wal``.
   From here on new batches go to the new log.
2. Without the lock: stream the copied state to ``snapshot-<g+1>.tmp``.
3. Rename it to ``snapshot-<g+1>.snap`` and fsync the directory. This is the
   commit point of the compaction.
4. Remove the files of every generation below ``g + 1``.

A crash anywhere in that sequence leaves either the old snapshot plus both logs
(before step 3) or the new snapshot plus possibly-stale older files (after it).
Recovery handles both by loading the newest snapshot and replaying, in generation
order, every log at or above that snapshot's generation.

Snapshot file format::

    magic:4s "JSNP"  version:u8  generation:u64  seq:u64        header
    entries...                                                  codec ops (all puts)
    count:u32  crc:u32                                          trailer

``crc`` is the CRC-32 of every byte before the trailer's ``crc`` field.
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass, field
from typing import Iterator

from src.journal import codec, journal
from src.fsops import FsOps
from src.stats import Stats

SNAP_MAGIC = b"JSNP"
SNAP_VERSION = 1
#: Snapshot bodies are written in appends of about this size.
SNAP_CHUNK = 64 * 1024

LOG_PREFIX, LOG_SUFFIX = "log-", ".wal"
SNAP_PREFIX, SNAP_SUFFIX = "snapshot-", ".snap"
TMP_SUFFIX = ".tmp"

_SNAP_HEAD = struct.Struct(">4sBQQ")
_SNAP_COUNT = struct.Struct(">I")  # same width as a codec batch count
_SNAP_CRC = struct.Struct(">I")


@dataclass
class Listing:
    """The generation files present in a directory, each list in generation order."""

    logs: list[tuple[int, str]] = field(default_factory=list)
    snapshots: list[tuple[int, str]] = field(default_factory=list)
    temps: list[str] = field(default_factory=list)


class Layout:
    """Names of the files that make up a store directory."""

    def __init__(self, fs: FsOps, directory: str):
        self.fs = fs
        self.directory = directory

    @staticmethod
    def _name(prefix: str, gen: int, suffix: str) -> str:
        # Zero-padded so that sorting names sorts generations.
        return f"{prefix}{gen:012d}{suffix}"

    def path(self, name: str) -> str:
        return self.fs.join(self.directory, name)

    def log_path(self, gen: int) -> str:
        return self.path(self._name(LOG_PREFIX, gen, LOG_SUFFIX))

    def snapshot_path(self, gen: int) -> str:
        return self.path(self._name(SNAP_PREFIX, gen, SNAP_SUFFIX))

    def tmp_path(self, gen: int) -> str:
        return self.path(self._name(SNAP_PREFIX, gen, TMP_SUFFIX))

    @staticmethod
    def _generation(name: str, prefix: str, suffix: str) -> int | None:
        if not (name.startswith(prefix) and name.endswith(suffix)):
            return None
        digits = name[len(prefix):len(name) - len(suffix)]
        return int(digits) if digits.isdigit() else None

    def scan_dir(self) -> Listing:
        listing = Listing()
        for name in sorted(self.fs.listdir(self.directory)):
            gen = self._generation(name, LOG_PREFIX, LOG_SUFFIX)
            if gen is not None:
                listing.logs.append((gen, name))
                continue
            gen = self._generation(name, SNAP_PREFIX, SNAP_SUFFIX)
            if gen is not None:
                listing.snapshots.append((gen, name))
                continue
            if self._generation(name, SNAP_PREFIX, TMP_SUFFIX) is not None:
                listing.temps.append(name)
        return listing


# -- writing -------------------------------------------------------------------------

def _encode_chunks(gen: int, seq: int, state: dict[str, bytes]) -> Iterator[bytes]:
    """The snapshot file for ``state``, in pieces of roughly ``SNAP_CHUNK`` bytes."""
    crc = 0
    count = 0
    buf = [_SNAP_HEAD.pack(SNAP_MAGIC, SNAP_VERSION, gen, seq)]
    size = len(buf[0])
    for key, value in state.items():
        piece = codec.encode_op(codec.Op(codec.PUT, key, value))
        buf.append(piece)
        size += len(piece)
        count += 1
        if size >= SNAP_CHUNK:
            chunk = b"".join(buf)
            crc = zlib.crc32(chunk, crc)
            yield chunk
            buf, size = [], 0
    tail = b"".join(buf) + _SNAP_COUNT.pack(count)
    crc = zlib.crc32(tail, crc)
    yield tail + _SNAP_CRC.pack(crc)


def stage(fs: FsOps, layout: Layout, gen: int, seq: int, state: dict[str, bytes]) -> str:
    """Write the snapshot of ``state`` (as of batch ``seq``) to its temporary name.

    ``state`` must not change while this runs. Returns the temporary path; nothing
    is visible to recovery until :func:`install` renames it.
    """
    tmp = layout.tmp_path(gen)
    if fs.exists(tmp):
        fs.remove(tmp)
    for chunk in _encode_chunks(gen, seq, state):
        fs.append_bytes(tmp, chunk)
    return tmp


def install(fs: FsOps, layout: Layout, gen: int, tmp: str) -> None:
    """Commit a staged snapshot: after this returns, recovery starts from it."""
    fs.rename(tmp, layout.snapshot_path(gen))
    fs.fsync_dir(layout.directory)


def discard_before(fs: FsOps, layout: Layout, gen: int) -> None:
    """Remove every log and snapshot of a generation below ``gen``.

    Only call this once snapshot ``gen`` is installed: it is what makes the older
    files redundant.
    """
    listing = layout.scan_dir()
    for old, name in listing.logs:
        if old < gen:
            fs.remove(layout.path(name))
    for old, name in listing.snapshots:
        if old < gen:
            fs.remove(layout.path(name))


# -- reading -------------------------------------------------------------------------

def load(fs: FsOps, path: str) -> tuple[int, int, dict[str, bytes]]:
    """Read and verify a snapshot; returns ``(generation, seq, state)``."""
    data = fs.read_bytes(path)
    trailer = _SNAP_COUNT.size + _SNAP_CRC.size
    if len(data) < _SNAP_HEAD.size + trailer:
        raise journal.JournalCorrupt(f"{path}: snapshot is truncated")
    (crc,) = _SNAP_CRC.unpack_from(data, len(data) - _SNAP_CRC.size)
    if zlib.crc32(data[:len(data) - _SNAP_CRC.size]) != crc:
        raise journal.JournalCorrupt(f"{path}: snapshot checksum mismatch")
    magic, version, gen, seq = _SNAP_HEAD.unpack_from(data, 0)
    if magic != SNAP_MAGIC or version != SNAP_VERSION:
        raise journal.JournalCorrupt(f"{path}: not a snapshot (version {version})")
    (count,) = _SNAP_COUNT.unpack_from(data, len(data) - trailer)
    body = data[_SNAP_HEAD.size:len(data) - trailer]
    state: dict[str, bytes] = {}
    try:
        # The body is a codec batch whose count was only known at the end.
        ops = codec.decode_ops(_SNAP_COUNT.pack(count) + body)
    except codec.DecodeError as exc:
        raise journal.JournalCorrupt(f"{path}: {exc}") from None
    for op in ops:
        state[op.key] = op.value
    return gen, seq, state


@dataclass
class Recovered:
    """The state rebuilt by :func:`recover` and where appends continue."""

    state: dict[str, bytes]
    seq: int
    generation: int
    log_size: int


def recover(fs: FsOps, layout: Layout, stats: Stats) -> Recovered:
    """Rebuild the state from the directory and leave it ready for appends.

    Safe to run any number of times, and safe to interrupt at any point: every step
    either leaves the directory as it was or moves it to a state from which the same
    contents are recovered.
    """
    fs.makedirs(layout.directory)
    listing = layout.scan_dir()
    for name in listing.temps:
        # Never renamed into place, so never part of the state.
        fs.remove(layout.path(name))

    snap_gen, seq, state = 0, 0, {}
    if listing.snapshots:
        snap_gen, name = listing.snapshots[-1]
        found_gen, seq, state = load(fs, layout.path(name))
        if found_gen != snap_gen:
            raise journal.JournalCorrupt(f"{name} claims generation {found_gen}")

    live = [(gen, name) for gen, name in listing.logs if gen >= snap_gen]
    for index, (gen, name) in enumerate(live):
        path = layout.path(name)
        result = journal.scan(fs.read_bytes(path), seq)
        for batch_seq, ops in result.batches:
            _apply(state, ops)
            seq = batch_seq
        stats.incr("replayed_batches", len(result.batches))
        if result.damaged:
            # Cut the log back to its last complete batch so that new appends follow
            # it directly; logs after a damaged one hold nothing replay may apply.
            stats.incr("discarded_bytes", result.total - result.valid_end)
            fs.truncate(path, result.valid_end)
            for _, later in live[index + 1:]:
                fs.remove(layout.path(later))
            live = live[:index + 1]
            break

    active = live[-1][0] if live else snap_gen
    # Files of generations below the snapshot are wholly contained in it.
    for gen, name in listing.logs:
        if gen < snap_gen:
            fs.remove(layout.path(name))
    for gen, name in listing.snapshots:
        if gen < snap_gen:
            fs.remove(layout.path(name))

    log = layout.log_path(active)
    if not fs.exists(log):
        fs.create_exclusive(log, b"")
    return Recovered(state=state, seq=seq, generation=active, log_size=fs.size(log))


def _apply(state: dict[str, bytes], ops: list[codec.Op]) -> None:
    for op in ops:
        if op.kind == codec.PUT:
            state[op.key] = op.value
        else:
            state.pop(op.key, None)
