import socket
import time
import unittest

from nperf.transport import control_connection


class ControlConnectionTests(unittest.TestCase):
    def test_non_object_json_is_ignored(self):
        reader, writer = socket.socketpair()
        self.addCleanup(reader.close)
        self.addCleanup(writer.close)
        channel = control_connection.ControlChannel(reader)
        self.addCleanup(channel.close)

        writer.sendall(b"[]\n{\"type\": \"ready\"}\n")

        self.assertEqual(channel.recv(timeout=1), {"type": "ready"})

    def test_oversized_control_frame_closes_reader(self):
        reader, writer = socket.socketpair()
        self.addCleanup(reader.close)
        self.addCleanup(writer.close)
        channel = control_connection.ControlChannel(reader)
        self.addCleanup(channel.close)

        writer.sendall(b"x" * (control_connection._MAX_CONTROL_FRAME_BYTES + 1))

        with self.assertRaises(EOFError):
            channel.recv(timeout=1)

    def test_saturated_queue_does_not_strand_reader_thread(self):
        reader, writer = socket.socketpair()
        self.addCleanup(reader.close)
        self.addCleanup(writer.close)
        channel = control_connection.ControlChannel(reader)

        writer.sendall(b'{"type":"queued"}\n' * 80)
        deadline = time.monotonic() + 1
        while not channel._eof.is_set() and time.monotonic() < deadline:
            time.sleep(0.01)
        channel.close()
        channel._reader_thread.join(timeout=1)

        self.assertFalse(channel._reader_thread.is_alive())

    def test_blocked_write_times_out_and_closes_writer_thread(self):
        writer, unread_peer = socket.socketpair()
        self.addCleanup(unread_peer.close)
        writer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        channel = control_connection.ControlChannel(
            writer, write_timeout_sec=0.05)
        self.addCleanup(channel.close)

        started = time.monotonic()
        with self.assertRaises(socket.timeout):
            channel.send("large", payload="x" * (2 << 20))
        elapsed = time.monotonic() - started
        channel._writer_thread.join(timeout=1)

        self.assertLess(elapsed, 1)
        self.assertTrue(channel._closed.is_set())
        self.assertFalse(channel._writer_thread.is_alive())


if __name__ == "__main__":
    unittest.main()
