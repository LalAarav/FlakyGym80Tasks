"""A small real TCP server for the test suite.

Speaks the wirecall frame protocol on 127.0.0.1 and can inject the kinds of trouble a
real network produces: responses split across TCP segments, latency spikes, and
connections dropped after the request was applied.
"""

import json
import random
import socket
import socketserver
import struct
import threading
import time


def _read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


class _Handler(socketserver.BaseRequestHandler):
    def handle(self):
        srv = self.server.app
        sock = self.request
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while True:
            header = _read_exact(sock, 4)
            if header is None:
                return
            body = _read_exact(sock, struct.unpack(">I", header)[0])
            if body is None:
                return
            req = json.loads(body)
            response = srv.apply(req)
            roll = srv.rng.random()
            if roll < srv.drop:
                sock.close()
                return
            roll -= srv.drop
            if roll < srv.delay:
                time.sleep(srv.delay_seconds)
            data = json.dumps(response).encode()
            out = struct.pack(">I", len(data)) + data
            if srv.rng.random() < srv.split:
                cut = srv.rng.randrange(1, len(out))
                sock.sendall(out[:cut])
                time.sleep(0.002)
                sock.sendall(out[cut:])
            else:
                sock.sendall(out)


class _TCPServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class FaultyServer:
    def __init__(self, split=0.0, delay=0.0, delay_seconds=0.4, drop=0.0):
        self.split, self.delay, self.delay_seconds, self.drop = split, delay, delay_seconds, drop
        self.rng = random.Random()
        self.lock = threading.Lock()
        self.counter = 0
        self.dedupe = {}
        self.notes = []
        self._server = _TCPServer(("127.0.0.1", 0), _Handler)
        self._server.app = self
        self.address = self._server.server_address
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def apply(self, req):
        with self.lock:
            op, key = req["op"], req.get("key")
            if op == "incr":
                if key is not None and key in self.dedupe:
                    result = self.dedupe[key]
                else:
                    self.counter += 1
                    result = self.counter
                    if key is not None:
                        self.dedupe[key] = result
            elif op == "note":
                self.notes.append(req["payload"])
                result = len(self.notes)
            else:
                result = req.get("payload")
        return {"id": req["id"], "ok": True, "result": result}

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._server.shutdown()
        self._server.server_close()
