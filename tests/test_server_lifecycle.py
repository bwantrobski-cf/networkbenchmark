import queue
import socket
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from nperf.common import constants
from nperf.server import echo_responders, throughput_session
from nperf.transport import stream_worker


RUN_ID = "12345678-1234-1234-1234-123456789abc"
FOREIGN_RUN_ID = "00000000-0000-0000-0000-000000000000"


def _tcp_params(streams=1, direction="down"):
    return {
        "transport": "tcp",
        "direction": direction,
        "streams": streams,
        "procs": 1,
        "duration_sec": 1.0,
    }


class _BlockingFinalControl:
    def __init__(self):
        self.messages = queue.Queue()
        for message in (
                {"type": "hello", "run_id": RUN_ID, "params": _tcp_params()},
                {"type": "start"},
                {"type": "start_data"},
                {"type": "stop"}):
            self.messages.put(message)
        self.final_send_started = threading.Event()
        self.closed = threading.Event()

    def recv(self, timeout=None):
        try:
            return self.messages.get(timeout=min(timeout or 0.1, 0.1))
        except queue.Empty:
            return None

    def send(self, message_type, **_payload):
        if message_type == "final_stats":
            self.final_send_started.set()
            if not self.closed.wait(1):
                raise AssertionError("watchdog did not interrupt final control write")
            raise OSError("control channel closed")

    def close(self):
        self.closed.set()


class _LifecycleContext:
    def __init__(self, watchdog_exited):
        self.config = SimpleNamespace(
            bind="127.0.0.1", udp_port=10000, tcp_port=11000)
        self.active_session = None
        self.last_session = None
        self.watchdog_running_at_release = None
        self.watchdog_exited = watchdog_exited
        self.logs = []

    def set_session(self, session):
        self.active_session = session
        self.last_session = session
        for event in session.stream_ready.values():
            event.set()
        return True

    def clear_session(self, run_id):
        if self.active_session and self.active_session.run_id == run_id:
            self.watchdog_running_at_release = not (
                self.watchdog_exited.is_set())
            self.active_session = None

    def log(self, message):
        self.logs.append(message)


class _TcpBaseContext:
    def __init__(self, session):
        self.config = SimpleNamespace(bind="127.0.0.1", tcp_port=0)
        self.stop_event = threading.Event()
        self.session = session
        self.logs = []

    def get_session(self):
        return self.session

    def log(self, message):
        self.logs.append(message)


class _BaseSession:
    def __init__(self):
        self.run_id = RUN_ID
        self.params = {"transport": "tcp"}
        self.stream_ready = {0: threading.Event()}
        self.stop_received_event = threading.Event()
        self.connection = None

    def try_attach_stream(self, index, sock, addr=None, spawn_receiver=True):
        if self.stream_ready[index].is_set():
            return False
        self.connection = sock
        self.stream_ready[index].set()
        return True


def _wait_for_listener(session, timeout=1):
    deadline = time.monotonic() + timeout
    while not session.listener_sockets and time.monotonic() < deadline:
        time.sleep(0.005)
    assert session.listener_sockets
    return session.listener_sockets[0]


def test_blocked_final_write_deadline_releases_session():
    control = _BlockingFinalControl()
    watchdog_exited = threading.Event()
    ctx = _LifecycleContext(watchdog_exited)
    session_watchdog = throughput_session._session_watchdog

    def tracked_watchdog(*args):
        try:
            session_watchdog(*args)
        finally:
            watchdog_exited.set()

    with patch.object(throughput_session, "ControlChannel", return_value=control), \
         patch.object(throughput_session, "_session_watchdog",
                      side_effect=tracked_watchdog), \
         patch.object(constants, "SOCKET_TIMEOUT_SEC", 0.05), \
         patch.object(constants, "FINAL_STATS_TIMEOUT_SEC", 0.2), \
         patch.object(constants, "CONTROL_RECV_POLL_SEC", 0.005), \
         patch.object(throughput_session, "_server_host_sample",
                      return_value={"topology": {}}):
        throughput_session.handle_control_connection(
            ctx, SimpleNamespace(family=socket.AF_INET),
            ("127.0.0.1", 30000))

    assert control.final_send_started.is_set()
    assert control.closed.is_set()
    assert ctx.active_session is None
    assert ctx.watchdog_running_at_release is True
    assert watchdog_exited.is_set()
    assert ctx.last_session.watchdog_stop_event.is_set()
    assert (ctx.last_session.timing_snapshot()["deadline_failure_message"]
            == "final control exchange timed out")


def test_extra_tcp_listener_rejects_foreign_then_accepts_fragmented_preamble():
    session = throughput_session.ThroughputSession(
        RUN_ID, _tcp_params(streams=2), None)
    ctx = SimpleNamespace(
        config=SimpleNamespace(bind="127.0.0.1"), log=lambda _message: None)

    with patch.object(constants, "SOCKET_TIMEOUT_SEC", 1), \
         patch.object(throughput_session, "_TCP_PREAMBLE_TIMEOUT_SEC", 0.2):
        listener_thread = throughput_session._spawn_extra_stream_listener(
            ctx, session, 1, 0)
        listener = _wait_for_listener(session)
        port = listener.getsockname()[1]

        foreign = socket.create_connection(("127.0.0.1", port), timeout=1)
        foreign.sendall(stream_worker.build_preamble(FOREIGN_RUN_ID, 1))
        foreign.close()

        valid = socket.create_connection(("127.0.0.1", port), timeout=1)
        preamble = stream_worker.build_preamble(RUN_ID, 1)
        valid.sendall(preamble[:7])
        time.sleep(0.01)
        valid.sendall(preamble[7:23])
        time.sleep(0.01)
        valid.sendall(preamble[23:])

        assert session.stream_ready[1].wait(1)
        listener_thread.join(timeout=1)
        assert not listener_thread.is_alive()
        assert listener.fileno() == -1

        session.stop_all_streams(abort=True)
        valid.close()


def test_extra_tcp_listener_times_out_silent_peer_then_accepts_queued_client():
    session = throughput_session.ThroughputSession(
        RUN_ID, _tcp_params(streams=2), None)
    ctx = SimpleNamespace(
        config=SimpleNamespace(bind="127.0.0.1"), log=lambda _message: None)

    with patch.object(constants, "SOCKET_TIMEOUT_SEC", 1), \
         patch.object(throughput_session, "_TCP_PREAMBLE_TIMEOUT_SEC", 0.1):
        listener_thread = throughput_session._spawn_extra_stream_listener(
            ctx, session, 1, 0)
        listener = _wait_for_listener(session)
        port = listener.getsockname()[1]

        silent = socket.create_connection(("127.0.0.1", port), timeout=1)
        time.sleep(0.02)
        valid = socket.create_connection(("127.0.0.1", port), timeout=1)
        valid.sendall(stream_worker.build_preamble(RUN_ID, 1))

        assert session.stream_ready[1].wait(0.5)
        listener_thread.join(timeout=1)
        assert not listener_thread.is_alive()
        assert listener.fileno() == -1

        session.stop_all_streams(abort=True)
        silent.close()
        valid.close()


def test_extra_tcp_listener_closes_on_session_cancellation():
    session = throughput_session.ThroughputSession(
        RUN_ID, _tcp_params(streams=2), None)
    ctx = SimpleNamespace(
        config=SimpleNamespace(bind="127.0.0.1"), log=lambda _message: None)

    with patch.object(constants, "SOCKET_TIMEOUT_SEC", 2):
        listener_thread = throughput_session._spawn_extra_stream_listener(
            ctx, session, 1, 0)
        listener = _wait_for_listener(session)
        session.stop_all_streams(abort=True)
        listener_thread.join(timeout=1)

    assert not listener_thread.is_alive()
    assert listener.fileno() == -1


def test_tcp_base_listener_rejects_foreign_then_accepts_fragmented_preamble():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    session = _BaseSession()
    ctx = _TcpBaseContext(session)

    with patch.object(echo_responders.net, "open_server_sockets",
                      return_value=[listener]):
        server_thread = threading.Thread(
            target=echo_responders.tcp_base_listener, args=(ctx,))
        server_thread.start()
        port = listener.getsockname()[1]

        foreign = socket.create_connection(("127.0.0.1", port), timeout=1)
        foreign.sendall(stream_worker.build_preamble(FOREIGN_RUN_ID, 0))
        foreign.close()

        valid = socket.create_connection(("127.0.0.1", port), timeout=1)
        preamble = stream_worker.build_preamble(RUN_ID, 0)
        for offset in range(0, len(preamble), 5):
            valid.sendall(preamble[offset:offset + 5])
            time.sleep(0.002)

        assert session.stream_ready[0].wait(1)
        ctx.stop_event.set()
        server_thread.join(timeout=2)

    assert not server_thread.is_alive()
    assert listener.fileno() == -1
    valid.close()
    session.connection.close()


def test_tcp_base_listener_bounds_handlers_and_releases_slots():
    class TrackingSemaphore:
        def __init__(self):
            self.semaphore = threading.BoundedSemaphore(1)
            self.released = threading.Event()

        def acquire(self, blocking=True):
            return self.semaphore.acquire(blocking=blocking)

        def release(self):
            self.semaphore.release()
            self.released.set()

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    ctx = _TcpBaseContext(None)
    slots = TrackingSemaphore()
    first_started = threading.Event()
    release_first = threading.Event()
    next_started = threading.Event()
    handled = []

    def blocking_handler(_ctx, conn, _addr):
        handled.append(conn)
        try:
            if len(handled) == 1:
                first_started.set()
                assert release_first.wait(1)
            else:
                next_started.set()
        finally:
            conn.close()

    with patch.object(echo_responders.net, "open_server_sockets",
                      return_value=[listener]), \
         patch.object(echo_responders, "_tcp_base_preamble_slots", slots), \
         patch.object(echo_responders, "_handle_tcp_base_connection",
                      side_effect=blocking_handler):
        server_thread = threading.Thread(
            target=echo_responders.tcp_base_listener, args=(ctx,))
        server_thread.start()
        port = listener.getsockname()[1]

        first = socket.create_connection(("127.0.0.1", port), timeout=1)
        assert first_started.wait(1)

        excess = socket.create_connection(("127.0.0.1", port), timeout=1)
        excess.settimeout(1)
        assert excess.recv(1) == b""
        assert len(handled) == 1

        release_first.set()
        assert slots.released.wait(1)
        next_client = socket.create_connection(("127.0.0.1", port), timeout=1)
        assert next_started.wait(1)

        ctx.stop_event.set()
        server_thread.join(timeout=2)

    assert not server_thread.is_alive()
    assert len(handled) == 2
    first.close()
    excess.close()
    next_client.close()


def test_simultaneous_tcp_base_preambles_attach_once_and_close_loser():
    session = throughput_session.ThroughputSession(
        RUN_ID, _tcp_params(direction="up"), None)
    ctx = _TcpBaseContext(session)
    barrier = threading.Barrier(2)
    claim_results = []
    receiver_sockets = []
    receiver_started = threading.Event()
    original_try_attach = session.try_attach_stream

    def simultaneous_try_attach(*args, **kwargs):
        barrier.wait(timeout=1)
        result = original_try_attach(*args, **kwargs)
        claim_results.append(result)
        return result

    def receiver(sock, _counters, stop_event, _path_mtu):
        receiver_sockets.append(sock)
        receiver_started.set()
        stop_event.wait(1)

    session.try_attach_stream = simultaneous_try_attach
    server_a, client_a = socket.socketpair()
    server_b, client_b = socket.socketpair()
    preamble = stream_worker.build_preamble(RUN_ID, 0)
    client_a.sendall(preamble)
    client_b.sendall(preamble)

    with patch.object(stream_worker, "configure_tcp_socket"), \
         patch.object(stream_worker, "tcp_receiver_loop", side_effect=receiver):
        handlers = [
            threading.Thread(
                target=echo_responders._handle_tcp_base_connection,
                args=(ctx, server_a, ("127.0.0.1", 20001))),
            threading.Thread(
                target=echo_responders._handle_tcp_base_connection,
                args=(ctx, server_b, ("127.0.0.1", 20002))),
        ]
        for handler in handlers:
            handler.start()
        for handler in handlers:
            handler.join(timeout=1)

    assert all(not handler.is_alive() for handler in handlers)
    assert sorted(claim_results) == [False, True]
    assert receiver_started.wait(1)
    assert len(receiver_sockets) == 1
    assert len(session.threads) == 1
    winner = session.stream_sockets[0]
    loser = server_b if winner is server_a else server_a
    assert winner.fileno() >= 0
    assert loser.fileno() == -1

    session.stop_all_streams(abort=True)
    for thread in session.threads:
        thread.join(timeout=1)
    client_a.close()
    client_b.close()


def test_simultaneous_udp_base_preambles_attach_once_without_closing_listener():
    params = {
        "transport": "udp",
        "direction": "up",
        "streams": 1,
        "procs": 1,
        "duration_sec": 1.0,
    }
    session = throughput_session.ThroughputSession(RUN_ID, params, None)
    ctx = _TcpBaseContext(session)
    sock = Mock()
    barrier = threading.Barrier(2)
    claim_results = []
    original_try_attach = session.try_attach_stream

    def simultaneous_try_attach(*args, **kwargs):
        barrier.wait(timeout=1)
        result = original_try_attach(*args, **kwargs)
        claim_results.append(result)
        return result

    session.try_attach_stream = simultaneous_try_attach
    preamble = stream_worker.build_preamble(RUN_ID, 0)
    handlers = [
        threading.Thread(
            target=echo_responders._handle_udp_base_datagram,
            args=(ctx, sock, preamble, ("127.0.0.1", port), True),
            kwargs={"session": session})
        for port in (21001, 21002)
    ]

    with patch.object(stream_worker, "configure_udp_socket") as configure:
        for handler in handlers:
            handler.start()
        for handler in handlers:
            handler.join(timeout=1)

    assert all(not handler.is_alive() for handler in handlers)
    assert sorted(claim_results) == [False, True]
    assert session.stream_sockets[0] is sock
    assert session.stream_addrs[0] in {
        ("127.0.0.1", 21001), ("127.0.0.1", 21002)}
    assert session.borrowed_stream_indices == {0}
    assert len(ctx.logs) == 1
    configure.assert_called_once_with(sock, session.counters[0])

    session.stop_all_streams(abort=True)
    sock.close.assert_not_called()


def test_failed_stream_claim_rolls_back_and_allows_retry():
    session = throughput_session.ThroughputSession(
        RUN_ID, _tcp_params(), None)
    failed_sock = Mock()
    retry_sock = Mock()

    with patch.object(stream_worker, "configure_tcp_socket",
                      side_effect=[RuntimeError("configure failed"), None]):
        with pytest.raises(RuntimeError, match="configure failed"):
            session.try_attach_stream(
                0, failed_sock, addr=("127.0.0.1", 22001))
        attached = session.try_attach_stream(
            0, retry_sock, addr=("127.0.0.1", 22002))

    assert attached is True
    assert session.stream_sockets == {0: retry_sock}
    assert session.stream_addrs == {0: ("127.0.0.1", 22002)}
    assert session.stream_ready[0].is_set()
    assert session._claimed_stream_indices == {0}

    session.stop_all_streams(abort=True)
