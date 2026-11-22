"""Dead-letter store: durable parking for items that ran out of retries.

An item that keeps failing is eventually given up on. Rather than dropping it, the
retry machinery ``put``s it here, where an operator can ``list`` what is parked,
``replay`` an item (hand it back to a durable target such as the job queue, with a
fresh attempt budget) or ``purge`` it for good.

Layout under ``root`` (every touch goes through ``FsOps``):

    dead/<name>              one JSON record per parked item
    replaying/<name>.<tok>   a record claimed by an in-progress replay
    tmp/<name>.<tok>         a record being written by ``put``

``<name>`` is ``FsOps.safe_name(item_id)``, so any id maps to a short,
separator-free file name and distinct ids never share one. Records only ever
appear in ``dead/`` by an atomic rename of a complete file, and a replay claims a
record by renaming it out of ``dead/``; whoever loses that rename lost the race.
A crash at any point leaves each record in ``dead/``, in a claim file (restored to
``dead/`` when the store is next opened), or handed to the replay target.

See docs/SPEC.md (R9-R12) for the contract.
"""

from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass, replace
from typing import Any, Callable

from src.clock import SystemClock
from src.fsops import FsOps
from src.stats import Stats


@dataclass(frozen=True)
class DeadLetter:
    """A parked item: what it was, how often it ran, and why it was given up on."""

    item_id: str
    payload: Any
    attempts: int
    reason: str
    dead_at: float  # wall-clock time of the put

    def for_replay(self) -> "DeadLetter":
        """The record as handed to a replay target: same item, attempt count reset."""
        return replace(self, attempts=0)

    def to_bytes(self) -> bytes:
        return json.dumps({
            "item_id": self.item_id,
            "payload": self.payload,
            "attempts": self.attempts,
            "reason": self.reason,
            "dead_at": self.dead_at,
        }, sort_keys=True).encode("utf-8")

    @staticmethod
    def from_bytes(data: bytes) -> "DeadLetter":
        raw = json.loads(data.decode("utf-8"))
        return DeadLetter(item_id=raw["item_id"], payload=raw["payload"],
                          attempts=int(raw["attempts"]), reason=raw["reason"],
                          dead_at=float(raw["dead_at"]))


ReplayTarget = Callable[[DeadLetter], None]


class DeadLetterStore:
    """File-backed dead letters with atomic put, list, replay and purge.

    Several stores (in one process or several) may share ``root``. Opening a store
    recovers replays that a crashed process left half-done, so open it while no
    replay on ``root`` is in progress, e.g. when the service starts.
    """

    def __init__(self, root: str, fs: FsOps | None = None, clock: SystemClock | None = None,
                 stats: Stats | None = None) -> None:
        self._fs = fs if fs is not None else FsOps()
        self._clock = clock if clock is not None else SystemClock()
        self._stats = stats if stats is not None else Stats()
        self._root = root
        self._dead = self._fs.join(root, "dead")
        self._claims = self._fs.join(root, "replaying")
        self._tmp = self._fs.join(root, "tmp")
        # Serializes this store's writes into dead/ (put and claim restore), so a
        # restore never overwrites a record put meanwhile from this process.
        self._lock = threading.Lock()
        self._replay_lock = threading.RLock()
        for path in (self._dead, self._claims, self._tmp):
            self._fs.makedirs(path)
        self._recover()

    # -- naming ----------------------------------------------------------------------

    def _name(self, item_id: str) -> str:
        if not isinstance(item_id, str) or not item_id:
            raise ValueError("item ids are non-empty strings")
        return self._fs.safe_name(item_id)

    def _dead_path(self, name: str) -> str:
        return self._fs.join(self._dead, name)

    @staticmethod
    def _token() -> str:
        return uuid.uuid4().hex

    # -- recovery --------------------------------------------------------------------

    def _recover(self) -> None:
        """Undo what a crash left behind: stray temp files and claimed records.

        A claim file is a record some replay took out of ``dead/``. Whether or not
        its target saw it, it goes back to ``dead/`` (targets tolerate a repeat),
        unless a newer record for the same item is already there.
        """
        for fname in self._fs.listdir(self._tmp):
            self._fs.remove(self._fs.join(self._tmp, fname))
        for fname in sorted(self._fs.listdir(self._claims)):
            name = fname.rsplit(".", 1)[0]
            self._restore(self._fs.join(self._claims, fname), name)
            self._stats.incr("deadletter.recovered")

    def _restore(self, claim: str, name: str) -> None:
        """Put a claimed record back into ``dead/`` unless it has been superseded."""
        with self._lock:
            if self._fs.exists(self._dead_path(name)):
                self._fs.remove(claim)
            else:
                self._fs.rename(claim, self._dead_path(name))

    # -- writing ---------------------------------------------------------------------

    def put(self, item_id: str, payload: Any, attempts: int, reason: str) -> DeadLetter:
        """Park ``item_id``. Replaces any record already parked for that id."""
        name = self._name(item_id)
        record = DeadLetter(item_id=item_id, payload=payload, attempts=attempts,
                            reason=reason, dead_at=self._clock.wall())
        tmp = self._fs.join(self._tmp, f"{name}.{self._token()}")
        self._fs.write_bytes(tmp, record.to_bytes())
        with self._lock:
            self._fs.rename(tmp, self._dead_path(name))
        self._stats.incr("deadletter.put")
        return record

    def purge(self, item_id: str) -> bool:
        """Delete ``item_id``'s record. ``False`` if none is parked (or a replay has it)."""
        try:
            self._fs.remove(self._dead_path(self._name(item_id)))
        except FileNotFoundError:
            return False
        self._stats.incr("deadletter.purged")
        return True

    def purge_all(self) -> int:
        """Delete every parked record; return how many were deleted."""
        count = 0
        for fname in self._fs.listdir(self._dead):
            try:
                self._fs.remove(self._dead_path(fname))
            except FileNotFoundError:
                continue
            count += 1
        self._stats.incr("deadletter.purged", count)
        return count

    # -- reading ---------------------------------------------------------------------

    def get(self, item_id: str) -> DeadLetter | None:
        """The record parked for ``item_id``, or ``None``."""
        try:
            data = self._fs.read_bytes(self._dead_path(self._name(item_id)))
        except FileNotFoundError:
            return None
        return DeadLetter.from_bytes(data)

    def list(self) -> list[DeadLetter]:
        """Every parked record, oldest put first."""
        records = []
        for fname in self._fs.listdir(self._dead):
            try:
                data = self._fs.read_bytes(self._dead_path(fname))
            except FileNotFoundError:
                continue  # replayed or purged while we looked
            records.append(DeadLetter.from_bytes(data))
        records.sort(key=lambda r: (r.dead_at, r.item_id))
        return records

    def __len__(self) -> int:
        return len(self._fs.listdir(self._dead))

    # -- replay ----------------------------------------------------------------------

    def replay(self, item_id: str, target: ReplayTarget) -> bool:
        """Hand ``item_id``'s record to ``target`` and unpark it.

        ``target`` receives ``record.for_replay()`` and must be durable and
        idempotent per item id (after a crash it may see the item again). Returns
        ``True`` if this call did the hand-off, ``False`` if nothing was parked
        under ``item_id`` or another replay got it first. If ``target`` raises, the
        record stays parked and the error propagates.
        """
        name = self._name(item_id)
        path = self._dead_path(name)
        # One replay at a time; the record is removed only after the target
        # has it, so a crash at any point leaves it parked.
        with self._replay_lock:
            try:
                record = DeadLetter.from_bytes(self._fs.read_bytes(path))
            except FileNotFoundError:
                return False
            target(record.for_replay())
            self._fs.remove(path)
        self._stats.incr("deadletter.replayed")
        return True

    def replay_all(self, target: ReplayTarget) -> int:
        """Replay every parked record; return how many this call handed off."""
        count = 0
        for record in self.list():
            if self.replay(record.item_id, target):
                count += 1
        return count
