import ctypes
import queue
import socket
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from nperf.common import constants
from nperf.server import echo_responders, state, throughput_session
from nperf.transport import metrics, stream_worker


RUN_ID = "12345678-1234-1234-1234-123456789abc"


def _params(transport="udp", direction="up", streams=1, procs=1):
    params = {
        "transport": transport,
        "direction": direction,
        "streams": streams,
        "procs": procs,
        "duration_sec": 1.0,
    }
    if transport == "udp":
        params["udp_wire_version"] = stream_worker.UDP_WIRE_VERSION
    return params


class _ScriptedControl:
    def __init__(self, messages, stop_after_peer=False):
        self._messages = queue.Queue()
        for message in messages:
            self._messages.put(message)
        self.stop_after_peer = stop_after_peer
        self.stop_queued = False
        self.sent = []
        self.closed = False

    def recv(self, timeout=None):
        try:
            return self._messages.get(timeout=min(timeout or 0.1, 0.1))
        except queue.Empty:
            return None

    def send(self, message_type, **payload):
        self.sent.append({"type": message_type, **payload})
        if message_type == "peer_stats" and self.stop_after_peer and not self.stop_queued:
            self.stop_queued = True
            self._messages.put({"type": "stop"})
        elif message_type == "final_stats":
            self._messages.put({"type": "final_stats_ack"})

    def close(self):
        self.closed = True


class _ServerContext:
    def __init__(self, auto_ready=False):
        self.config = SimpleNamespace(
            bind="127.0.0.1", udp_port=10000, tcp_port=11000)
        self.auto_ready = auto_ready
        self.active_session = None
        self.last_session = None
        self.logs = []

    def set_session(self, session):
        self.active_session = session
        self.last_session = session
        if self.auto_ready:
            for event in session.stream_ready.values():
                event.set()

    def clear_session(self, run_id):
        if self.active_session and self.active_session.run_id == run_id:
            self.active_session = None

    def get_session(self):
        return self.active_session

    def log(self, message):
        self.logs.append(message)


class _BaseSocket:
    family = socket.AF_INET

    def __init__(self):
        self.blocking = None
        self.sent = []
        self.closed = False

    def setblocking(self, value):
        self.blocking = value

    def sendto(self, data, address):
        self.sent.append((bytes(data), address))

    def close(self):
        self.closed = True


class ServerUdpIntegrationTests(unittest.TestCase):
    def test_server_context_rejects_a_second_active_session(self):
        ctx = state.ServerContext(state.ServerConfig(), {})
        first = SimpleNamespace(run_id="first")
        second = SimpleNamespace(run_id="second")

        self.assertTrue(ctx.set_session(first))
        self.assertFalse(ctx.set_session(second))
        self.assertIs(ctx.get_session(), first)

    def test_abort_cleanup_stops_and_closes_tcp_upload_streams(self):
        session = throughput_session.ThroughputSession(
            RUN_ID, _params(transport="tcp", direction="up"), Mock())
        stop_event = threading.Event()
        sock = Mock()
        session.stream_stop_events[0] = stop_event
        session.stream_sockets[0] = sock

        session.stop_all_streams(abort=True)

        self.assertTrue(stop_event.is_set())
        sock.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        sock.close.assert_called_once_with()

    def test_abort_cleanup_preserves_borrowed_udp_base_socket(self):
        session = throughput_session.ThroughputSession(
            RUN_ID, _params(transport="udp", direction="up"), Mock())
        sock = Mock()
        session.stream_sockets[0] = sock
        session.borrowed_stream_indices.add(0)

        session.stop_all_streams(abort=True)

        sock.shutdown.assert_not_called()
        sock.close.assert_not_called()

    def test_session_parameters_bound_streams_and_mtu(self):
        with self.assertRaisesRegex(throughput_session.ProtocolError, "streams"):
            throughput_session._validate_session_params({"streams": 65})
        with self.assertRaisesRegex(throughput_session.ProtocolError, "path_mtu"):
            throughput_session._validate_session_params({
                "streams": 1, "path_mtu": 1_000_000_000})
        self.assertEqual(throughput_session._validate_session_params({
            "streams": 64, "path_mtu": 9000}), 64)

    def test_session_parameters_require_a_finite_bounded_duration(self):
        for duration in (0, -1, float("nan"), float("inf"),
                         constants.MAX_RUN_TIME_FAILSAFE_SEC + 1):
            with self.subTest(duration=duration), \
                 self.assertRaisesRegex(
                     throughput_session.ProtocolError, "duration_sec"):
                throughput_session._validate_session_params({
                    "streams": 1, "duration_sec": duration})

        params = {"streams": 1, "duration_sec": "12.5"}
        throughput_session._validate_session_params(params)
        self.assertEqual(params["duration_sec"], 12.5)

    def test_session_deadlines_cover_setup_start_idle_and_run(self):
        session = throughput_session.ThroughputSession(
            RUN_ID, _params(), Mock())

        session.arm_phase_deadline("setup expired", 5, now=10)
        self.assertIsNone(
            throughput_session._session_deadline_error(session, now=14.999))
        self.assertEqual(
            throughput_session._session_deadline_error(session, now=15),
            "setup expired")

        with patch.object(constants, "MAX_RUN_TIME_FAILSAFE_SEC", 100), \
             patch.object(constants, "SOCKET_TIMEOUT_SEC", 10):
            session.begin_start_wait(now=20)
            session.note_control_activity(now=30)
            self.assertEqual(
                throughput_session._session_deadline_error(session, now=40),
                "control channel was idle for 10 seconds")

            session.note_control_activity(now=119)
            self.assertEqual(
                throughput_session._session_deadline_error(session, now=120),
                "timed out waiting for start_data after 100 seconds")

            session.mark_data_started(now=200)
            session.note_control_activity(now=210.5)
            self.assertEqual(
                throughput_session._session_deadline_error(session, now=211),
                "throughput run exceeded its 11 second deadline")

    def test_start_data_deadline_releases_active_session(self):
        params = _params()
        params["duration_sec"] = 0.01
        control = _ScriptedControl([
            {"type": "hello", "run_id": RUN_ID, "params": params},
            {"type": "start"},
        ])
        ctx = _ServerContext(auto_ready=True)

        with patch.object(throughput_session, "ControlChannel", return_value=control), \
             patch.object(constants, "MAX_RUN_TIME_FAILSAFE_SEC", 0.02), \
             patch.object(constants, "SOCKET_TIMEOUT_SEC", 1.0), \
             patch.object(constants, "CONTROL_RECV_POLL_SEC", 0.001), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.002):
            throughput_session.handle_control_connection(
                ctx, SimpleNamespace(family=socket.AF_INET),
                ("127.0.0.1", 30000))

        self.assertIsNone(ctx.active_session)
        self.assertTrue(control.closed)
        self.assertTrue(ctx.last_session.stop_received_event.is_set())
        self.assertTrue(any("start_data" in message for message in ctx.logs))

    def test_duplicate_start_data_is_rejected(self):
        session = throughput_session.ThroughputSession(
            RUN_ID, _params(direction="down"), Mock())
        control = Mock()
        control.recv.side_effect = [
            {"type": "start_data"},
            {"type": "start_data"},
        ]

        with patch.object(throughput_session, "_start_sender_threads") as start:
            throughput_session._control_dispatch_loop(None, session, control)

        start.assert_called_once_with(None, session)
        control.send.assert_called_with(
            "error", error="duplicate start_data message")

    def test_session_owns_trial_holder_and_configures_shared_udp_socket(self):
        session = throughput_session.ThroughputSession(
            RUN_ID, _params(), Mock())
        sock = Mock()

        with patch.object(stream_worker, "configure_udp_socket") as configure, \
             patch.object(stream_worker, "udp_receiver_loop") as receiver:
            session.attach_stream(
                0, sock, addr=("127.0.0.1", 20000), spawn_receiver=False)

        self.assertIsInstance(session.trial_value, stream_worker.TrialHolder)
        self.assertEqual(session.trial_value.value, 0)
        configure.assert_called_once_with(sock, session.counters[0])
        receiver.assert_not_called()

    def test_multiprocess_trial_value_is_lock_free_uint32(self):
        with patch.object(throughput_session, "_server_should_use_mp", return_value=True):
            session = throughput_session.ThroughputSession(
                RUN_ID, _params(streams=2, procs=2), Mock())
        self.addCleanup(session.worker_ready_q.close)
        self.addCleanup(session.worker_status_q.close)

        self.assertEqual(ctypes.sizeof(session.trial_value), 4)
        self.assertFalse(hasattr(session.trial_value, "get_lock"))
        session.trial_value.value = (1 << 32) - 1
        self.assertEqual(session.trial_value.value, (1 << 32) - 1)

    def test_set_trial_and_set_rate_update_live_holders(self):
        session = throughput_session.ThroughputSession(
            RUN_ID, _params(direction="down"), Mock())
        control = Mock()
        control.recv.side_effect = [
            {"type": "set_rate", "per_stream_bps": 123.5},
            {"type": "set_trial", "trial_id": 17},
            {"type": "set_trial", "trial_id": 8.5},
            {"type": "stop"},
        ]

        throughput_session._control_dispatch_loop(None, session, control)

        self.assertEqual(session.rate_value.value, 123.5)
        self.assertEqual(session.trial_value.value, 17)

    def test_tcp_ignores_set_trial(self):
        session = throughput_session.ThroughputSession(
            RUN_ID, _params(transport="tcp"), Mock())
        control = Mock()
        control.recv.side_effect = [
            {"type": "set_trial", "trial_id": 99},
            {"type": "stop"},
        ]

        throughput_session._control_dispatch_loop(None, session, control)

        self.assertEqual(session.trial_value.value, 0)

    def test_server_udp_senders_share_trial_and_leave_stream_zero_unconnected(self):
        session = throughput_session.ThroughputSession(
            RUN_ID, _params(direction="down", streams=2), Mock())
        session.stream_sockets = {0: object(), 1: object()}
        session.stream_addrs = {
            0: ("127.0.0.1", 20000),
            1: ("127.0.0.1", 20001),
        }

        with patch.object(stream_worker, "udp_sender_loop") as sender:
            throughput_session._start_sender_threads(None, session)
            for thread in session.threads:
                thread.join(1.0)

        calls = {item.args[2]: item for item in sender.call_args_list}
        self.assertEqual(set(calls), {0, 1})
        self.assertIs(calls[0].kwargs["trial_value"], session.trial_value)
        self.assertIs(calls[1].kwargs["trial_value"], session.trial_value)
        self.assertFalse(calls[0].kwargs["connect_socket"])
        self.assertTrue(calls[1].kwargs["connect_socket"])

    def test_worker_stream_group_receives_shared_trial_value(self):
        sock = Mock()
        sock.family = socket.AF_INET
        shared = Mock()
        shared.view.return_value = Mock()
        ready = queue.Queue()
        start = SimpleNamespace(value=1)
        stop = SimpleNamespace(value=0)
        rate = stream_worker.RateHolder(100.0)
        trial = stream_worker.TrialHolder(23)

        with patch.object(throughput_session.socket, "socket", return_value=sock), \
             patch.object(throughput_session.net, "configure_ipv6_socket"), \
             patch.object(throughput_session.net, "configure_dont_fragment"), \
             patch.object(stream_worker, "read_udp_preamble",
                          return_value=(RUN_ID, 1, ("127.0.0.1", 20001))), \
             patch.object(stream_worker, "run_stream_group") as run_group:
            throughput_session._server_worker_proc_inner(
                [(1, 10001)], RUN_ID, "send", "udp", "127.0.0.1", 1.0,
                shared, start, stop, ready, rate_value=rate,
                trial_value=trial)

        self.assertEqual(ready.get_nowait(), ("bound", None, None))
        self.assertEqual(ready.get_nowait(),
                         ("ready", 1, ("127.0.0.1", 20001)))
        self.assertIs(run_group.call_args.kwargs["trial_value"], trial)
        self.assertIs(run_group.call_args.kwargs["rate_value"], rate)
        self.assertNotEqual(
            run_group.call_args.kwargs.get("connect_udp", True), False)

    def test_udp_hello_mismatch_rejected_before_session_setup(self):
        control = _ScriptedControl([{
            "type": "hello",
            "run_id": RUN_ID,
            "params": {**_params(), "udp_wire_version": 1},
        }])
        ctx = _ServerContext()
        backend = {"selected": "native"}

        with patch.object(throughput_session, "ControlChannel", return_value=control), \
             patch.object(throughput_session, "ThroughputSession") as session_type, \
             patch.object(throughput_session.udp_batch, "backend_status",
                          return_value=backend):
            throughput_session.handle_control_connection(
                ctx, SimpleNamespace(family=socket.AF_INET), ("127.0.0.1", 30000))

        session_type.assert_not_called()
        self.assertIsNone(ctx.active_session)
        self.assertTrue(control.closed)
        self.assertEqual(len(control.sent), 1)
        rejection = control.sent[0]
        self.assertEqual(rejection["type"], "hello_ack")
        self.assertFalse(rejection["ok"])
        self.assertIn("client requested 1", rejection["error"])
        self.assertIn("server requires 3", rejection["error"])
        self.assertEqual(rejection["udp_wire_version"], 3)
        self.assertEqual(rejection["udp_batch"], backend)

    def test_udp_protocol_messages_include_backend_and_host_samples(self):
        params = _params()
        control = _ScriptedControl([
            {"type": "hello", "run_id": RUN_ID, "params": params},
            {"type": "start"},
        ], stop_after_peer=True)
        ctx = _ServerContext(auto_ready=True)
        backend = {"selected": "native", "native_available": True}

        with patch.object(throughput_session, "ControlChannel", return_value=control), \
             patch.object(throughput_session.constants, "PEER_STATS_INTERVAL_SEC", 0.01), \
             patch.object(throughput_session.host_telemetry, "snapshot_endpoint",
                          side_effect=lambda **_kwargs: {"available": True}), \
             patch.object(throughput_session.udp_batch, "backend_status",
                          return_value=backend):
            throughput_session.handle_control_connection(
                ctx, SimpleNamespace(family=socket.AF_INET6), ("::1", 30000, 0, 0))

        hello = next(message for message in control.sent
                     if message["type"] == "hello_ack")
        self.assertTrue(hello["ok"])
        self.assertEqual(params["family"], socket.AF_INET6)
        self.assertEqual(params["ip_version"], "IPv6")
        self.assertEqual(hello["udp_wire_version"], 3)
        self.assertEqual(hello["udp_batch"], backend)

        stats_messages = [message for message in control.sent
                          if message["type"] in ("peer_stats", "final_stats")]
        self.assertTrue(any(message["type"] == "peer_stats"
                            for message in stats_messages))
        self.assertTrue(any(message["type"] == "final_stats"
                            for message in stats_messages))
        for message in stats_messages:
            sample = message["host_sample"]
            self.assertTrue(sample["available"])
            self.assertEqual(sample["topology"]["udp_batch"], backend)
            self.assertEqual(sample["topology"]["workers"][0]["streams"], [0])
            self.assertNotIn("host_sample", message["streams"][0])

    def test_tcp_stats_include_host_samples_without_udp_protocol_fields(self):
        params = _params(transport="tcp", direction="down")
        control = _ScriptedControl([
            {"type": "hello", "run_id": RUN_ID, "params": params},
            {"type": "start"},
        ], stop_after_peer=True)
        ctx = _ServerContext(auto_ready=True)

        with patch.object(throughput_session, "ControlChannel", return_value=control), \
             patch.object(throughput_session.constants, "PEER_STATS_INTERVAL_SEC", 0.01), \
             patch.object(throughput_session.host_telemetry, "snapshot_endpoint",
                          side_effect=lambda **_kwargs: {"available": True}), \
             patch.object(throughput_session.udp_batch, "backend_status") as backend:
            throughput_session.handle_control_connection(
                ctx, SimpleNamespace(family=socket.AF_INET), ("127.0.0.1", 30000))

        hello = next(message for message in control.sent
                     if message["type"] == "hello_ack")
        self.assertNotIn("udp_wire_version", hello)
        self.assertNotIn("udp_batch", hello)
        backend.assert_not_called()
        for message in control.sent:
            if message["type"] in ("peer_stats", "final_stats"):
                self.assertIn("host_sample", message)
                self.assertNotIn("udp_batch", message["host_sample"]["topology"])

    def test_host_sampling_fails_open_and_keeps_topology(self):
        session = throughput_session.ThroughputSession(
            RUN_ID, _params(transport="tcp"), Mock())

        with patch.object(throughput_session.host_telemetry, "snapshot_endpoint",
                          side_effect=RuntimeError("procfs unavailable")):
            sample = throughput_session._server_host_sample(session)

        self.assertFalse(sample["available"])
        self.assertIn("procfs unavailable", sample["error"])
        self.assertEqual(sample["topology"]["workers"][0]["streams"], [0])
        self.assertNotIn("udp_batch", sample["topology"])

    def test_topology_reports_actual_worker_pid_assignments(self):
        session = SimpleNamespace(
            mp_enabled=True,
            num_streams=4,
            params={"transport": "udp"},
            worker_procs=[SimpleNamespace(pid=201), SimpleNamespace(pid=202)],
            worker_assignments={201: [1, 3], 202: [2]},
        )
        backend = {"selected": "native"}

        with patch.object(throughput_session.os, "getpid", return_value=200), \
             patch.object(throughput_session.udp_batch, "backend_status",
                          return_value=backend):
            topology = throughput_session._server_topology(session)

        self.assertEqual(topology["controller_pid"], 200)
        self.assertEqual(topology["workers"], [
            {"pid": 200, "streams": [0]},
            {"pid": 201, "streams": [1, 3]},
            {"pid": 202, "streams": [2]},
        ])
        self.assertEqual(topology["udp_batch"], backend)

    def test_base_listener_batches_and_preserves_demux_addresses(self):
        sock = _BaseSocket()
        ctx = _ServerContext()
        ctx.stop_event = threading.Event()
        peer = ("127.0.0.1", 20000)
        probe_peer = ("127.0.0.1", 21000)
        counter = metrics.StreamCounters(0)
        attached = []
        session = SimpleNamespace(
            run_id=RUN_ID,
            params={"transport": "udp"},
            stream_ready={0: threading.Event()},
            stream_addrs={},
            counters={0: counter},
        )

        def try_attach_stream(index, attached_sock, addr=None, spawn_receiver=True):
            attached.append((index, attached_sock, addr, spawn_receiver))
            session.stream_addrs[index] = addr
            session.stream_ready[index].set()
            return True

        session.try_attach_stream = try_attach_stream
        ctx.active_session = session
        connectivity = constants.CONNECTIVITY_MAGIC + b"probe123"
        preamble = stream_worker.build_preamble(RUN_ID, 0)
        frame = bytearray(64)
        stream_worker.UDP_HEADER_STRUCT.pack_into(
            frame, 0, 0, 7, stream_worker.udp_session_cookie(RUN_ID),
            3, stream_worker.UDP_FLAG_DATA)
        packets = [connectivity, preamble, bytes(frame)]
        addresses = [probe_peer, peer, peer]
        captured_buffers = []

        def receive_batch(receive_sock, buffers):
            self.assertIs(receive_sock, sock)
            captured_buffers.append(buffers)
            for index, packet in enumerate(packets):
                buffers[index][:len(packet)] = packet
            ctx.stop_event.set()
            return [
                (len(packet), address, 0, 9 if index == 2 else None)
                for index, (packet, address) in enumerate(zip(packets, addresses))
            ]

        with patch.object(echo_responders.net, "open_server_sockets",
                          return_value=[sock]), \
             patch.object(echo_responders.net, "configure_dont_fragment"), \
             patch.object(echo_responders.select, "select",
                          return_value=([sock], [], [])), \
             patch.object(echo_responders.udp_batch, "recvmmsg_into",
                          side_effect=receive_batch), \
             patch.object(stream_worker, "feed_udp_data_frames") as feed:
            echo_responders.udp_base_listener(ctx)

        self.assertFalse(sock.blocking)
        self.assertTrue(sock.closed)
        self.assertEqual(sock.sent, [(connectivity, probe_peer)])
        self.assertEqual(attached, [(0, sock, peer, False)])
        self.assertEqual(bytes(feed.call_args.args[1][0]), bytes(frame))
        self.assertEqual(counter.snapshot()["kernel_rxq_drops"], 9)
        self.assertEqual(len(captured_buffers[0]), stream_worker.UDP_BATCH_SIZE)


if __name__ == "__main__":
    unittest.main()
