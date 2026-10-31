import errno
import json
import socket
import time
import unittest

from nperf.common import constants
from nperf.transport import control_connection, metrics


class _FlakyWriteSocket:
    def __init__(self, sock, actions):
        self.sock = sock
        self.actions = iter(actions)
        self.write_sizes = []

    def __getattr__(self, name):
        return getattr(self.sock, name)

    def send(self, data):
        self.write_sizes.append(len(data))
        action = next(self.actions, None)
        if isinstance(action, BaseException):
            raise action
        return self.sock.send(data if action is None else data[:action])


class ControlConnectionTests(unittest.TestCase):
    def test_partial_control_write_recovers_from_buffer_pressure_without_duplication(self):
        reader, writer = socket.socketpair()
        flaky = _FlakyWriteSocket(writer, [
            3, OSError(errno.ENOBUFS, "no buffer space"),
            BlockingIOError(errno.EAGAIN, "try again"), 5,
        ])
        sender = control_connection.ControlChannel(flaky)
        receiver = control_connection.ControlChannel(reader)
        self.addCleanup(sender.close)
        self.addCleanup(receiver.close)
        payload = "x" * (128 * 1024)

        sender.send("large", payload=payload)
        sender.send("next")

        self.assertEqual(receiver.recv(timeout=1), {"type": "large", "payload": payload})
        self.assertEqual(receiver.recv(timeout=1), {"type": "next"})
        self.assertGreater(len(flaky.write_sizes), 4)
        self.assertLessEqual(max(flaky.write_sizes), 65536)
        self.assertFalse(sender._closed.is_set())

    def test_persistent_buffer_pressure_still_obeys_the_control_write_deadline(self):
        writer, unread_peer = socket.socketpair()
        self.addCleanup(unread_peer.close)
        errors = iter(lambda: OSError(errno.ENOBUFS, "no buffer space"), None)
        flaky = _FlakyWriteSocket(writer, errors)
        channel = control_connection.ControlChannel(flaky, write_timeout_sec=0.05)
        self.addCleanup(channel.close)

        started = time.monotonic()
        with self.assertRaises(socket.timeout):
            channel.send("blocked")
        channel._writer_thread.join(timeout=1)

        self.assertLess(time.monotonic() - started, 1)
        self.assertTrue(channel._closed.is_set())
        self.assertFalse(channel._writer_thread.is_alive())

    def test_all_1024_stream_ready_messages_can_queue_during_setup(self):
        reader, writer = socket.socketpair()
        receiver = control_connection.ControlChannel(reader)
        sender = control_connection.ControlChannel(writer)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        for index in range(1024):
            sender.send("stream_ready", index=index)
        deadline = time.monotonic() + 3
        while receiver._queue.qsize() < 1024 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(receiver._queue.qsize(), 1024)
        self.assertFalse(receiver._eof.is_set())
        for index in range(1024):
            self.assertEqual(receiver.recv(timeout=1), {"type": "stream_ready", "index": index})

    def test_1024_stream_telemetry_frame_round_trips(self):
        counter = metrics.StreamCounters(0)
        counter.activate_trial(1, "sent")
        counter.activate_trial(1, "received")
        counter.set_error("stream failure: " + "x" * 512)
        template = counter.snapshot()
        snapshots = [dict(template, index=index) for index in range(1024)]
        payload = {"type": "peer_stats", "streams": snapshots}
        self.assertGreater(len(json.dumps(payload)), 1 << 20)
        reader, writer = socket.socketpair()
        receiver = control_connection.ControlChannel(reader)
        sender = control_connection.ControlChannel(writer)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        sender.send("peer_stats", streams=snapshots)
        self.assertEqual(receiver.recv(timeout=3), payload)
        self.assertEqual(len(snapshots), constants.MAX_PARALLEL_STREAMS)

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

        writer.sendall(
            b'{"type":"queued"}\n' * (control_connection._MAX_QUEUED_MESSAGES + 16))
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
