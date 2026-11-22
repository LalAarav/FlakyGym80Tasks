"""RPC client with retries, overall deadlines and optional idempotency keys."""

from __future__ import annotations

import random
import uuid
from typing import Any

from src.rpc.breaker import CircuitBreaker, CircuitOpen
from src.clock import SystemClock
from src.rpc.pool import ConnectionPool
from src.rpc.wire import ProtocolError, encode, read_frame

TRANSIENT = (ConnectionError, TimeoutError, ProtocolError)


class DeadlineExceeded(TimeoutError):
    """The call's overall deadline passed before it could complete."""


class RemoteError(Exception):
    """The server processed the request and reported an error."""


class RpcClient:
    def __init__(
        self,
        pool: ConnectionPool,
        clock: SystemClock | None = None,
        attempt_timeout: float = 0.5,
        max_attempts: int = 4,
        backoff: float = 0.01,
        breaker: CircuitBreaker | None = None,
        rng: random.Random | None = None,
    ):
        self.pool = pool
        self.clock = clock if clock is not None else SystemClock()
        self.attempt_timeout = attempt_timeout
        self.max_attempts = max_attempts
        self.backoff = backoff
        self.breaker = breaker
        self.rng = rng if rng is not None else random.Random()

    def call(self, op: str, payload: Any = None, *, idempotent: bool = False,
             dedupe: bool = False, idempotency_key: str | None = None,
             deadline: float = 2.0) -> Any:
        """Invoke ``op`` and return its result.

        Transient failures are retried only when that cannot apply the operation
        twice: ``idempotent=True`` (the op is safe to repeat) or ``dedupe=True`` (the
        client attaches one idempotency key per logical call, which the server
        uses to discard duplicates). ``deadline`` bounds the whole call, retries
        and backoff included. ``idempotency_key`` supplies the key explicitly (it implies
        ``dedupe``), for callers whose logical operation outlives a single call.
        """
        started = self.clock.monotonic()
        give_up_at = started + deadline
        retry_ok = idempotent or dedupe
        key = uuid.uuid4().hex if dedupe else None
        if idempotency_key is not None:
            retry_ok, key = True, idempotency_key
        last_error: BaseException | None = None
        for attempt in range(self.max_attempts):
            remaining = give_up_at - self.clock.monotonic()
            if remaining <= 0:
                raise DeadlineExceeded(f"{op}: deadline of {deadline}s exceeded") from last_error
            if self.breaker is not None and not self.breaker.allow():
                raise CircuitOpen(op)
            try:
                result = self._attempt(op, payload, key, min(self.attempt_timeout, remaining))
            except RemoteError:
                if self.breaker is not None:
                    self.breaker.record_success()  # the server answered
                raise
            except TRANSIENT as exc:
                if self.breaker is not None:
                    self.breaker.record_failure()
                if not retry_ok:
                    raise
                last_error = exc
                pause = self.backoff * (2 ** attempt) * (0.5 + self.rng.random())
                self.clock.sleep(max(0.0, min(pause, give_up_at - self.clock.monotonic())))
                continue
            if self.breaker is not None:
                self.breaker.record_success()
            return result
        if last_error is not None and give_up_at - self.clock.monotonic() <= 0:
            raise DeadlineExceeded(f"{op}: deadline of {deadline}s exceeded") from last_error
        raise last_error

    def _attempt(self, op: str, payload: Any, key: str | None, timeout: float) -> Any:
        conn = self.pool.acquire(timeout)
        clean = False
        try:
            conn.settimeout(timeout)
            request_id = uuid.uuid4().hex
            conn.send_all(encode({"id": request_id, "op": op, "payload": payload, "key": key}))
            response = read_frame(conn)
            if response.get("id") != request_id:
                raise ProtocolError("response belongs to a different request")
            clean = True
        finally:
            self.pool.release(conn, reusable=clean)
        if not response.get("ok"):
            raise RemoteError(response.get("error"))
        return response.get("result")
