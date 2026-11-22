"""Durable, file-backed job store.

On-disk layout under ``root``::

    tmp/      staging area for files that are still being written
    ids/      one marker file per job id (makes enqueue idempotent; it is also the
              commit point of an enqueue, see ``_recover``)
    ready/    jobs waiting to be leased, one JSON file each
    leased/   jobs currently leased to a worker
    done/     acknowledged jobs

Job files are named ``<seq>-<key>.json`` where ``seq`` is a per-store sequence
number, so a sorted directory listing is queue order, and ``key`` is the file-name
form of the job id (``FsOps.safe_name``).
"""

from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass
from typing import Any

from src.clock import SystemClock
from src.fsops import FsOps

DIRS = ("tmp", "ids", "ready", "leased", "done")


class StaleLease(Exception):
    """The lease expired and the job was re-granted (or already settled)."""


@dataclass(frozen=True)
class Job:
    id: str
    seq: int
    payload: Any
    attempts: int


@dataclass(frozen=True)
class Lease:
    job: Job
    name: str
    token: str
    worker: str


class JobStore:
    def __init__(self, root: str, fs: FsOps | None = None, clock: SystemClock | None = None):
        self.fs = fs if fs is not None else FsOps()
        self.clock = clock if clock is not None else SystemClock()
        self.root = root
        for name in DIRS:
            self.fs.makedirs(self._dir(name))
        self._lock = threading.Lock()
        # job file name -> (lease token, deadline on the monotonic clock, worker)
        self._leases: dict[str, tuple[str, float, str]] = {}
        self._recover()
        self._seq = self._recover_seq()

    # -- helpers ---------------------------------------------------------------

    def _dir(self, name: str) -> str:
        return self.fs.join(self.root, name)

    def _jobs_in(self, dirname: str) -> list[str]:
        return sorted(n for n in self.fs.listdir(self._dir(dirname)) if n.endswith(".json"))

    def _recover_seq(self) -> int:
        highest = 0
        for dirname in ("ready", "leased", "done"):
            for name in self._jobs_in(dirname):
                highest = max(highest, int(name.split("-", 1)[0]))
        return highest

    def _recover(self) -> None:
        """Repair the directory after a previous process died (only one process uses a
        store directory at a time). Every step is idempotent, so a crash during
        recovery is repaired by the next one."""
        # Anything still staged never reached ready/: those writes did not complete.
        for name in self.fs.listdir(self._dir("tmp")):
            self.fs.remove(self.fs.join(self._dir("tmp"), name))

    def _next_seq(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    def _stage(self, staged: str, data: bytes) -> str:
        """Write ``data`` to ``tmp/<staged>``, where ``lease`` never looks; return its path."""
        path = self.fs.join(self._dir("tmp"), staged)
        self.fs.write_bytes(path, data)
        return path

    def _write_atomic(self, dirname: str, name: str, data: bytes) -> None:
        tmp = self._stage(uuid.uuid4().hex + ".part", data)
        self.fs.rename(tmp, self.fs.join(self._dir(dirname), name))

    def _load(self, path: str) -> Job:
        record = json.loads(self.fs.read_bytes(path))
        return Job(record["id"], record["seq"], record["payload"], record["attempts"])

    def _encode(self, job: Job) -> bytes:
        return json.dumps(
            {"id": job.id, "seq": job.seq, "payload": job.payload, "attempts": job.attempts}
        ).encode("utf-8")

    def _check_lease(self, lease: Lease) -> None:
        """Drop ``lease`` from the lease table, or raise if it is no longer current.
        Caller must hold ``self._lock``."""
        current = self._leases.get(lease.name)
        if current is None or current[0] != lease.token:
            raise StaleLease(lease.name)
        del self._leases[lease.name]

    # -- public API ------------------------------------------------------------

    def enqueue(self, payload: Any, job_id: str | None = None) -> str | None:
        """Add a job. Returns its id, or ``None`` if a job with ``job_id`` already exists."""
        job_id = job_id if job_id is not None else uuid.uuid4().hex
        key = self.fs.safe_name(job_id)
        marker = self.fs.join(self._dir("ids"), key)
        seq = self._next_seq()
        name = f"{seq:012d}-{key}.json"
        staged = f"{key}.{uuid.uuid4().hex}.part"
        # Claim the id first, so a duplicate enqueue never writes anything. The
        # marker names the staged file, so recovery can finish the enqueue.
        try:
            self.fs.create_exclusive(marker, json.dumps({"name": name, "staged": staged}).encode("utf-8"))
        except FileExistsError:
            return None
        tmp = self._stage(staged, self._encode(Job(job_id, seq, payload, 0)))
        self.fs.rename(tmp, self.fs.join(self._dir("ready"), name))
        return job_id

    def lease(self, worker: str, lease_seconds: float) -> Lease | None:
        """Lease the oldest ready job to ``worker``, or return ``None`` if there is none."""
        for name in self._jobs_in("ready"):
            src = self.fs.join(self._dir("ready"), name)
            dst = self.fs.join(self._dir("leased"), name)
            try:
                self.fs.rename(src, dst)
            except FileNotFoundError:
                continue  # another worker claimed it first
            job = self._load(dst)
            token = uuid.uuid4().hex
            deadline = self.clock.monotonic() + lease_seconds
            with self._lock:
                self._leases[name] = (token, deadline, worker)
            return Lease(job, name, token, worker)
        return None

    def ack(self, lease: Lease) -> None:
        """Mark a leased job done. Raises ``StaleLease`` if the lease is no longer held."""
        with self._lock:
            self._check_lease(lease)
        self.fs.rename(
            self.fs.join(self._dir("leased"), lease.name),
            self.fs.join(self._dir("done"), lease.name),
        )

    def nack(self, lease: Lease) -> None:
        """Return a leased job to the ready queue with its attempt count bumped."""
        with self._lock:
            self._check_lease(lease)
        job = self._load(self.fs.join(self._dir("leased"), lease.name))
        retried = Job(job.id, job.seq, job.payload, job.attempts + 1)
        # Rewrite in place (we hold the file), then hand it back: the job is always
        # in exactly one of leased/ or ready/, never both and never neither.
        self._write_atomic("leased", lease.name, self._encode(retried))
        # Requeue behind every job that is ready now, so a job that keeps failing
        # cannot hold the head of the queue.
        requeued = f"{self._next_seq():012d}-{lease.name.split('-', 1)[1]}"
        self.fs.rename(
            self.fs.join(self._dir("leased"), lease.name),
            self.fs.join(self._dir("ready"), requeued),
        )

    def reap(self) -> int:
        """Return expired leases to the ready queue. Returns how many were reaped."""
        now = self.clock.monotonic()
        with self._lock:
            expired = [name for name, (_, deadline, _) in self._leases.items() if deadline <= now]
            for name in expired:
                del self._leases[name]
        for name in expired:
            self.fs.rename(
                self.fs.join(self._dir("leased"), name),
                self.fs.join(self._dir("ready"), name),
            )
        return len(expired)

    def counts(self) -> dict[str, int]:
        """Job counts per directory as of one instant. The directories are listed one at a
        time while jobs move between them, so a single pass can miss a job in flight (a
        reaped lease listed after ready/ and before leased/ looks like an empty queue);
        listing until two full passes agree rules that out."""
        while True:
            first = {d: self._jobs_in(d) for d in ("ready", "leased", "done")}
            second = {d: self._jobs_in(d) for d in ("ready", "leased", "done")}
            if first == second:
                return {d: len(names) for d, names in second.items()}

    def done_jobs(self) -> list[Job]:
        return [self._load(self.fs.join(self._dir("done"), n)) for n in self._jobs_in("done")]
