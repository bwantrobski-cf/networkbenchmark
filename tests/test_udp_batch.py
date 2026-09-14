import errno
import json
import os
import socket
import struct
import subprocess
import sys
import textwrap
import time
import unittest
from unittest.mock import Mock, patch

from nperf.transport import udp_batch

try:
    from nperf.transport import _udp_batch as native_udp_batch
except (ImportError, OSError):
    native_udp_batch = None


class _RecvFromOnlySocket:
    def __init__(self, packets):
        self.packets = list(packets)
        self.calls = []

    def recvfrom_into(self, buffer, size, flags):
        payload, address = self.packets.pop(0)
        self.calls.append((size, flags))
        copied = min(len(payload), size)
        buffer[:copied] = payload[:copied]
        return copied, address


class _WindowsRecvFromOnlySocket(_RecvFromOnlySocket):
    def recvfrom_into(self, buffer, size, flags):
        payload, address = self.packets.pop(0)
        self.calls.append((size, flags))
        copied = min(len(payload), size)
        buffer[:copied] = payload[:copied]
        if len(payload) > size:
            error = OSError(None, "message too long")
            error.winerror = 10040
            raise error
        return copied, address


class _ScriptedSendSocket:
    def __init__(self, actions):
        self.actions = iter(actions)
        self.calls = []

    def sendto(self, buffer, flags, address):
        self.calls.append((bytes(buffer), flags, address))
        action = next(self.actions)
        if isinstance(action, BaseException):
            raise action
        return len(buffer)


class _ScriptedRecvmsgSocket:
    def __init__(self, actions):
        self.actions = iter(actions)
        self.calls = []

    def recvmsg_into(self, buffers, ancillary_size, flags):
        self.calls.append((ancillary_size, flags))
        action = next(self.actions)
        if isinstance(action, BaseException):
            raise action
        payload, ancillary, msg_flags, address = action
        buffers[0][:len(payload)] = payload
        return len(payload), ancillary, msg_flags, address


class UdpBatchPythonTests(unittest.TestCase):
    def setUp(self):
        backend = patch.object(udp_batch, "BACKEND", "python")
        backend.start()
        self.addCleanup(backend.stop)

    def test_forced_python_backend_status(self):
        env = os.environ.copy()
        env["NPERF_UDP_BACKEND"] = "python"
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "import json; from nperf.transport import udp_batch; "
                "print(json.dumps(udp_batch.backend_status()))",
            ],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        status = json.loads(completed.stdout)

        self.assertEqual(status["requested"], "python")
        self.assertEqual(status["selected"], "python")
        self.assertEqual(status["native_available"], udp_batch.NATIVE_AVAILABLE)

    def test_ipv4_loopback_batch_uses_preallocated_buffers(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(2)
        payloads = [b"one", b"two-two", b"three"]

        sent = udp_batch.sendmmsg(sender, payloads, receiver.getsockname())
        buffers = [bytearray(32) for _ in payloads]
        received = udp_batch.recvmmsg_into(receiver, buffers)

        self.assertEqual(sent, len(payloads))
        self.assertEqual(len(received), len(payloads))
        self.assertEqual(
            [bytes(buffer[:metadata[0]]) for buffer, metadata in zip(buffers, received)],
            payloads,
        )
        self.assertTrue(all(item[1][0] == "127.0.0.1" for item in received))
        self.assertTrue(all(item[3] is None for item in received))

    def test_connected_send_uses_address_none(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(2)
        sender.connect(receiver.getsockname())

        self.assertEqual(udp_batch.sendmmsg(sender, [b"connected"]), 1)
        self.assertEqual(receiver.recvfrom(64)[0], b"connected")

    @unittest.skipUnless(socket.has_ipv6, "IPv6 is unavailable")
    def test_ipv6_common_destination(self):
        receiver = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        try:
            receiver.bind(("::1", 0))
        except OSError as exc:
            self.skipTest(f"IPv6 loopback is unavailable: {exc}")
        receiver.settimeout(2)

        self.assertEqual(
            udp_batch.sendmmsg(sender, [b"ipv6"], receiver.getsockname()), 1
        )
        buffer = bytearray(32)
        metadata = udp_batch.recvmmsg_into(receiver, [buffer])[0]
        self.assertEqual(bytes(buffer[:metadata[0]]), b"ipv6")
        self.assertEqual(metadata[1][0], "::1")

    def test_recvfrom_into_fallback_preserves_datagrams(self):
        fake = _RecvFromOnlySocket(
            [(b"first", ("127.0.0.1", 1001)), (b"second", ("127.0.0.1", 1002))]
        )
        buffers = [bytearray(16), bytearray(16)]

        result = udp_batch.recvmmsg_into(fake, buffers, flags=7)

        self.assertEqual([bytes(buffers[0][:5]), bytes(buffers[1][:6])], [b"first", b"second"])
        self.assertEqual(result, [
            (5, ("127.0.0.1", 1001), 0, None),
            (6, ("127.0.0.1", 1002), 0, None),
        ])
        self.assertEqual(fake.calls, [
            (udp_batch._UDP_MAX_DATAGRAM_SIZE, 7),
            (udp_batch._UDP_MAX_DATAGRAM_SIZE, 7),
        ])

    def test_recvfrom_into_fallback_reports_truncation(self):
        fake = _RecvFromOnlySocket(
            [(b"oversized", ("127.0.0.1", 1001))]
        )
        buffer = bytearray(4)

        result = udp_batch.recvmmsg_into(fake, [buffer])

        self.assertEqual(bytes(buffer), b"over")
        self.assertEqual(result, [
            (4, ("127.0.0.1", 1001), socket.MSG_TRUNC, None),
        ])
        self.assertEqual(fake.calls, [(udp_batch._UDP_MAX_DATAGRAM_SIZE, 0)])

    def test_recvfrom_into_fallback_reports_truncation_without_msg_trunc(self):
        env = os.environ.copy()
        env["NPERF_UDP_BACKEND"] = "python"
        script = textwrap.dedent(
            """\
            import socket

            if hasattr(socket, "MSG_TRUNC"):
                delattr(socket, "MSG_TRUNC")

            from nperf.transport import udp_batch

            class RecvFromOnlySocket:
                def recvfrom_into(self, buffer, size, flags):
                    assert size == udp_batch._UDP_MAX_DATAGRAM_SIZE
                    payload = b"oversized"
                    buffer[:len(payload)] = payload
                    return len(payload), ("127.0.0.1", 1001)

            buffer = bytearray(4)
            result = udp_batch.recvmmsg_into(RecvFromOnlySocket(), [buffer])
            assert bytes(buffer) == b"over"
            assert socket.MSG_TRUNC == udp_batch._MSG_TRUNC != 0
            assert result == [(4, ("127.0.0.1", 1001), socket.MSG_TRUNC, None)]
            assert result[0][2] & getattr(socket, "MSG_TRUNC", 0)
            """
        )

        subprocess.run(
            [sys.executable, "-c", script],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )

    def test_recvfrom_into_fallback_handles_windows_wsaemsgsize(self):
        payload = b"overflow" + b"x" * udp_batch._UDP_MAX_DATAGRAM_SIZE
        fake = _WindowsRecvFromOnlySocket(
            [(payload, ("127.0.0.1", 1001))]
        )
        buffer = bytearray(8)

        result = udp_batch.recvmmsg_into(fake, [buffer])

        self.assertEqual(bytes(buffer), payload[:len(buffer)])
        self.assertEqual(result, [(8, None, socket.MSG_TRUNC, None)])
        self.assertEqual(fake.calls, [(udp_batch._UDP_MAX_DATAGRAM_SIZE, 0)])

    def test_send_retries_eintr_and_returns_partial_on_eagain(self):
        would_block = BlockingIOError(errno.EAGAIN, "would block")
        fake = _ScriptedSendSocket([InterruptedError(), object(), would_block])
        address = ("127.0.0.1", 9999)

        count = udp_batch.sendmmsg(fake, [b"a", b"b"], address, flags=3)

        self.assertEqual(count, 1)
        self.assertEqual(len(fake.calls), 3)
        self.assertTrue(all(call[1:] == (3, address) for call in fake.calls))

    def test_send_raises_eagain_before_progress(self):
        fake = _ScriptedSendSocket(
            [BlockingIOError(errno.EAGAIN, "would block")]
        )
        with self.assertRaises(BlockingIOError):
            udp_batch.sendmmsg(fake, [b"a"], ("127.0.0.1", 9999))

    def test_receive_retries_eintr_and_returns_partial_on_eagain(self):
        packet = (b"data", [], 4, ("127.0.0.1", 2000))
        fake = _ScriptedRecvmsgSocket(
            [InterruptedError(), packet, BlockingIOError(errno.EAGAIN, "would block")]
        )
        buffers = [bytearray(8), bytearray(8)]

        result = udp_batch.recvmmsg_into(fake, buffers)

        self.assertEqual(result, [(4, ("127.0.0.1", 2000), 4, None)])
        self.assertEqual(bytes(buffers[0][:4]), b"data")
        self.assertEqual(len(fake.calls), 3)

    def test_rxq_overflow_ancillary_is_parsed(self):
        overflow_type = 0x1234
        ancillary = [(socket.SOL_SOCKET, overflow_type, struct.pack("=I", 37))]
        fake = _ScriptedRecvmsgSocket(
            [(b"x", ancillary, 0, ("127.0.0.1", 2000))]
        )

        with patch.object(udp_batch, "_SO_RXQ_OVFL", overflow_type):
            result = udp_batch.recvmmsg_into(fake, [bytearray(8)])

        self.assertEqual(result[0][3], 37)

    @unittest.skipUnless(hasattr(socket, "MSG_CTRUNC"), "MSG_CTRUNC is unavailable")
    def test_truncated_ancillary_overflow_is_not_authoritative(self):
        overflow_type = 0x1234
        ancillary = [(socket.SOL_SOCKET, overflow_type, struct.pack("=I", 37))]
        fake = _ScriptedRecvmsgSocket(
            [(b"x", ancillary, socket.MSG_CTRUNC, ("127.0.0.1", 2000))]
        )

        with patch.object(udp_batch, "_SO_RXQ_OVFL", overflow_type):
            result = udp_batch.recvmmsg_into(fake, [bytearray(8)])

        self.assertEqual(result[0][2], socket.MSG_CTRUNC)
        self.assertIsNone(result[0][3])

    def test_receive_rejects_readonly_buffers_before_io(self):
        fake = _RecvFromOnlySocket([])
        with self.assertRaises(TypeError):
            udp_batch.recvmmsg_into(fake, [b"readonly"])
        self.assertEqual(fake.calls, [])

    def test_empty_batches_do_not_access_socket(self):
        self.assertEqual(udp_batch.sendmmsg(object(), []), 0)
        self.assertEqual(udp_batch.recvmmsg_into(object(), []), [])

    def test_selected_backend_self_test(self):
        result = udp_batch.self_test()

        self.assertTrue(result["ok"])
        self.assertIn("IPv4", result["families"])
        self.assertEqual(result["messages"], 2 * len(result["families"]))
        self.assertEqual(result["selected"], "python")

    def test_aggregate_backend_status_reports_mixed_workers(self):
        result = udp_batch.aggregate_backend_status([
            {"index": 0, "udp_batch": {
                "selected": "native", "native_runtime_error": None}},
            {"index": 1, "udp_batch": {
                "selected": "python", "native_runtime_error": True}},
        ], base_status={"selected": "native"})

        self.assertEqual(result["selected"], "mixed")
        self.assertEqual(
            result["native_runtime_error"], "reported by data worker")


class UdpBatchNativeDispatchTests(unittest.TestCase):
    def test_positive_timeout_send_uses_python_socket_semantics(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(1.0)
        sender.settimeout(0.1)
        native = Mock()

        with patch.multiple(udp_batch, BACKEND="native", _native=native):
            sent = udp_batch.sendmmsg(
                sender, [b"timeout-send"], receiver.getsockname()
            )

        self.assertEqual(sent, 1)
        self.assertEqual(receiver.recvfrom(32)[0], b"timeout-send")
        native.sendmmsg.assert_not_called()

    def test_positive_timeout_empty_batches_use_python_semantics(self):
        sock = Mock()
        sock.gettimeout.return_value = 0.1
        native = Mock()

        with patch.multiple(udp_batch, BACKEND="native", _native=native):
            self.assertEqual(udp_batch.sendmmsg(sock, []), 0)
            self.assertEqual(udp_batch.recvmmsg_into(sock, []), [])

        native.sendmmsg.assert_not_called()
        native.recvmmsg_into.assert_not_called()

    def test_positive_timeout_empty_receive_raises_python_timeout(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(0.05)
        native = Mock()

        started = time.monotonic()
        with patch.multiple(udp_batch, BACKEND="native", _native=native):
            with self.assertRaises(socket.timeout):
                udp_batch.recvmmsg_into(receiver, [bytearray(8)])
        elapsed = time.monotonic() - started

        self.assertGreaterEqual(elapsed, 0.02)
        native.recvmmsg_into.assert_not_called()

    def test_positive_timeout_receive_returns_partial_python_result(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(0.05)
        sender.sendto(b"one", receiver.getsockname())
        buffers = [bytearray(8), bytearray(8)]
        native = Mock()

        started = time.monotonic()
        with patch.multiple(udp_batch, BACKEND="native", _native=native):
            result = udp_batch.recvmmsg_into(receiver, buffers)
        elapsed = time.monotonic() - started

        self.assertEqual(len(result), 1)
        self.assertEqual(bytes(buffers[0][:result[0][0]]), b"one")
        self.assertGreaterEqual(elapsed, 0.02)
        native.recvmmsg_into.assert_not_called()

    def test_runtime_downgrade_survives_immediate_python_eagain(self):
        native = Mock()
        native.recvmmsg_into.side_effect = OSError(
            errno.ENOSYS, "recvmmsg unavailable"
        )
        fake = _ScriptedRecvmsgSocket([
            BlockingIOError(errno.EAGAIN, "would block")
        ])

        with patch.multiple(
            udp_batch,
            BACKEND="native",
            REQUESTED_BACKEND="auto",
            _native=native,
            _native_runtime_error=None,
        ):
            with self.assertRaises(BlockingIOError):
                udp_batch.recvmmsg_into(fake, [bytearray(8)])
            self.assertEqual(udp_batch.BACKEND, "python")
            self.assertIsInstance(udp_batch._native_runtime_error, str)
            self.assertEqual(
                udp_batch.backend_status()["native_runtime_error"],
                udp_batch._native_runtime_error,
            )


@unittest.skipUnless(native_udp_batch is not None, "native extension is not built")
class UdpBatchNativeTests(unittest.TestCase):
    def test_native_ipv4_roundtrip_and_eagain(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        receiver.bind(("127.0.0.1", 0))

        self.assertEqual(
            native_udp_batch.sendmmsg(
                sender, [b"native-one", b"native-two"], receiver.getsockname()
            ),
            2,
        )
        buffers = [bytearray(32), bytearray(32)]
        received = native_udp_batch.recvmmsg_into(receiver, buffers)
        self.assertEqual(
            [bytes(buffer[:item[0]]) for buffer, item in zip(buffers, received)],
            [b"native-one", b"native-two"],
        )

        receiver.setblocking(False)
        with self.assertRaises(BlockingIOError):
            native_udp_batch.recvmmsg_into(receiver, [bytearray(32)])

    def test_native_rejects_batches_above_linux_limit_before_socket_access(self):
        oversized_batch = [b""] * 1025

        with self.assertRaises(OverflowError):
            native_udp_batch.sendmmsg(object(), oversized_batch)
        with self.assertRaises(OverflowError):
            native_udp_batch.recvmmsg_into(object(), [bytearray()] * 1025)


if __name__ == "__main__":
    unittest.main()
