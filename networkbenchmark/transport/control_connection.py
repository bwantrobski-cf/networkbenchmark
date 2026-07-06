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

_EOF = object()


class ControlChannel:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.sock.settimeout(None)
        self._send_lock = threading.Lock()
        self._queue = queue.Queue()
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()

    def _reader_loop(self):
        buf = b""
        try:
            while True:
                chunk = self.sock.recv(65536)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line.decode())
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    self._queue.put(msg)
        except OSError:
            pass
        finally:
            self._queue.put(_EOF)

    def send(self, msg_type: str, **payload):
        msg = {"type": msg_type}
        msg.update(payload)
        data = (json.dumps(msg) + "\n").encode()
        with self._send_lock:
            self.sock.sendall(data)

    def recv(self, timeout=None):
        """Return next message dict, or None on timeout.

        Raises EOFError if the peer closed the connection.
        """
        try:
            item = self._queue.get(timeout=timeout)
        except queue.Empty:
            return None
        if item is _EOF:
            raise EOFError("control connection closed by peer")
        return item

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass
