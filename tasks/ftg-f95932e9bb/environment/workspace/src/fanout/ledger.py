"""Durable record of completed deliveries, one marker file per (event, subscriber)."""

from __future__ import annotations

import json

from src.fsops import FsOps


class DeliveryLedger:
    def __init__(self, root: str, fs: FsOps | None = None):
        self.fs = fs if fs is not None else FsOps()
        self.dir = self.fs.join(root, "ledger")
        self.fs.makedirs(self.dir)

    def _marker(self, event_id: str, subscriber: str) -> str:
        return self.fs.join(self.dir, self.fs.safe_name(json.dumps([event_id, subscriber])))

    def delivered(self, event_id: str, subscriber: str) -> bool:
        return self.fs.exists(self._marker(event_id, subscriber))

    def record(self, event_id: str, subscriber: str) -> bool:
        """Mark a delivery complete. Returns ``True`` if this call recorded it and
        ``False`` if it was already recorded (recording twice is otherwise harmless)."""
        try:
            self.fs.create_exclusive(self._marker(event_id, subscriber), b"")
        except FileExistsError:
            return False
        return True
