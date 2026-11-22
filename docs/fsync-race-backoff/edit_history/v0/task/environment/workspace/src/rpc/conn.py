"""Connections and dialing.

All socket access goes through this module: ``Dialer`` opens connections and
``Connection`` wraps one socket. Both are seams - tests substitute their own
dialers and connections to simulate slow, lossy or fragmenting networks.
"""

from __future__ import annotations

import socket


class Connection:
    def __init__(self, sock: socket.socket):
        self._sock = sock
        self.closed = False

    def settimeout(self, seconds: float) -> None:
        self._sock.settimeout(seconds)

    def send_all(self, data: bytes) -> None:
        self._sock.sendall(data)

    def recv(self, n: int) -> bytes:
        """Receive up to ``n`` bytes. May return fewer; returns b"" at EOF.
        Raises ``TimeoutError`` if the current timeout elapses first."""
        return self._sock.recv(n)

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._sock.close()


class Dialer:
    def dial(self, address: tuple[str, int], timeout: float) -> Connection:
        """Open a connection. Raises ``OSError`` (``ConnectionError``, ``TimeoutError``, ...)."""
        sock = socket.create_connection(address, timeout=timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return Connection(sock)
