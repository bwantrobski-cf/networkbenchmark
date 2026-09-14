"""JSON-line control channel used to orchestrate throughput tests.

Adapted from bbperf's tcp_control_connection_class.py, but using
newline-delimited JSON messages instead of a bespoke string protocol, and a
background reader thread so callers can send latency "probe" messages at any
time (even while waiting for other control messages) without races.
"""

import json
import queue
import socket
import threading
import time

_MAX_CONTROL_FRAME_BYTES = 1 << 20
_MAX_QUEUED_MESSAGES = 64
_MAX_QUEUED_BYTES = 4 << 20
_MAX_QUEUED_WRITES = 64
_DEFAULT_WRITE_TIMEOUT_SEC = 30.0


class _WriteRequest:
    __slots__ = ("data", "done", "error")

    def __init__(self, data):
        self.data = data
        self.done = threading.Event()
        self.error = None


class ControlChannel:
    def __init__(self, sock: socket.socket,
                 write_timeout_sec: float = _DEFAULT_WRITE_TIMEOUT_SEC):
        self.sock = sock
        self.sock.settimeout(None)
        self._queue = queue.Queue(maxsize=_MAX_QUEUED_MESSAGES)
        self._queue_lock = threading.Lock()
        self._queued_bytes = 0
        self._write_timeout_sec = max(0.0, float(write_timeout_sec))
        self._write_queue = queue.Queue(maxsize=_MAX_QUEUED_WRITES)
        self._write_lock = threading.Lock()
        self._pending_writes = set()
        self._close_lock = threading.Lock()
        self._closed = threading.Event()
        self._eof = threading.Event()
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._reader_thread.start()
        self._writer_thread.start()

    def _reader_loop(self):
        buf = bytearray()
        try:
            while not self._closed.is_set():
                chunk = self.sock.recv(65536)
                if not chunk:
                    break
                buf.extend(chunk)
                if len(buf) > _MAX_CONTROL_FRAME_BYTES and b"\n" not in buf:
                    break
                while True:
                    newline = buf.find(b"\n")
                    if newline < 0:
                        break
                    if newline > _MAX_CONTROL_FRAME_BYTES:
                        return
                    line = bytes(buf[:newline]).strip()
                    del buf[:newline + 1]
                    if not line:
                        continue
                    try:
                        msg = json.loads(line.decode())
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    if not isinstance(msg, dict):
                        continue
                    frame_bytes = len(line)
                    with self._queue_lock:
                        if self._queued_bytes + frame_bytes > _MAX_QUEUED_BYTES:
                            return
                        try:
                            self._queue.put_nowait((msg, frame_bytes))
                        except queue.Full:
                            return
                        self._queued_bytes += frame_bytes
                if len(buf) > _MAX_CONTROL_FRAME_BYTES:
                    break
        except OSError:
            pass
        finally:
            self._eof.set()

    def _finish_write(self, request, error=None):
        with self._write_lock:
            if request not in self._pending_writes:
                return
            self._pending_writes.remove(request)
            request.error = error
            request.done.set()

    def _fail_pending_writes(self):
        with self._write_lock:
            requests = list(self._pending_writes)
            self._pending_writes.clear()
            for request in requests:
                request.error = BrokenPipeError("control connection is closed")
                request.done.set()

    def _writer_loop(self):
        while True:
            if self._closed.is_set() and self._write_queue.empty():
                return
            try:
                request = self._write_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if self._closed.is_set():
                self._finish_write(
                    request, BrokenPipeError("control connection is closed"))
                continue
            try:
                self.sock.sendall(request.data)
            except OSError as exc:
                self._finish_write(request, exc)
                self.close()
                return
            self._finish_write(request)

    def send(self, msg_type: str, **payload):
        msg = {"type": msg_type}
        msg.update(payload)
        data = (json.dumps(msg) + "\n").encode()
        request = _WriteRequest(data)
        deadline = time.monotonic() + self._write_timeout_sec
        with self._write_lock:
            if self._closed.is_set():
                raise BrokenPipeError("control connection is closed")
            self._pending_writes.add(request)

        remaining = max(0.0, deadline - time.monotonic())
        try:
            self._write_queue.put(request, timeout=remaining)
        except queue.Full:
            error = socket.timeout("timed out queueing control message")
            self._finish_write(request, error)
            self.close()
            raise error

        remaining = max(0.0, deadline - time.monotonic())
        if not request.done.wait(remaining):
            error = socket.timeout("timed out writing control message")
            self._finish_write(request, error)
            self.close()
            raise error
        if request.error is not None:
            raise request.error

    def recv(self, timeout=None):
        """Return next message dict, or None on timeout.

        Raises EOFError if the peer closed the connection.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._eof.is_set() and self._queue.empty():
                raise EOFError("control connection closed by peer")
            wait = 0.1
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                wait = min(wait, remaining)
            try:
                item, frame_bytes = self._queue.get(timeout=wait)
            except queue.Empty:
                continue
            with self._queue_lock:
                self._queued_bytes = max(0, self._queued_bytes - frame_bytes)
            return item

    def close(self):
        with self._close_lock:
            if self._closed.is_set():
                return
            self._closed.set()
            self._eof.set()
            self._fail_pending_writes()
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass
