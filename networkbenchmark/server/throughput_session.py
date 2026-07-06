"""Server-side handling of one throughput test: control handshake, dynamic
per-stream port listeners, and relaying this side's local stats back to the
client over the control channel every SAMPLE_INTERVAL_SEC.
"""

import socket
import threading
import time

from ..common import constants, tcp_info
from ..transport import stream_worker, metrics, latency_probe, run_mode
from ..transport.control_connection import ControlChannel


class ProtocolError(Exception):
    pass


class ThroughputSession:
    def __init__(self, run_id: str, params: dict, control: ControlChannel):
        self.run_id = run_id
        self.params = params
        self.control = control
        self.phase = run_mode.RunPhase()
        n = int(params["streams"])
        self.num_streams = n
        self.counters = {i: metrics.StreamCounters(i) for i in range(n)}
        self.stream_ready = {i: threading.Event() for i in range(n)}
        self.stream_sockets = {}
        self.stream_addrs = {}
        self.stream_stop_events = {}
        self.listener_sockets = []
        self.threads = []
        self.data_ready_event = threading.Event()
        self.stop_received_event = threading.Event()

    def attach_stream(self, index: int, sock, addr=None, spawn_receiver: bool = True):
        self.stream_sockets[index] = sock
        if addr is not None:
            self.stream_addrs[index] = addr
        self.stream_ready[index].set()

        if self.params["direction"] == "up" and spawn_receiver:
            stop_ev = threading.Event()
            self.stream_stop_events[index] = stop_ev
            if self.params["transport"] == "tcp":
                th = threading.Thread(target=stream_worker.tcp_receiver_loop,
                                       args=(sock, self.counters[index], stop_ev), daemon=True)
            else:
                th = threading.Thread(target=stream_worker.udp_receiver_loop,
                                       args=(sock, self.counters[index], stop_ev),
                                       kwargs={"known_peer_addr": addr}, daemon=True)
            th.start()
            self.threads.append(th)

    def stop_all_streams(self):
        for ev in self.stream_stop_events.values():
            ev.set()
        for sock in self.listener_sockets:
            try:
                sock.close()
            except OSError:
                pass


def _spawn_extra_stream_listener(ctx, session: ThroughputSession, index: int, port: int):
    """For stream indices 1..N-1 (dedicated, non-shared ports)."""
    transport = session.params["transport"]

    def _tcp_worker():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((ctx.config.bind, port))
            sock.listen(1)
        except OSError as exc:
            ctx.log(f"session {session.run_id[:8]}: failed to bind tcp port {port}: {exc}")
            return
        session.listener_sockets.append(sock)
        sock.settimeout(30.0)
        try:
            conn, addr = sock.accept()
        except (socket.timeout, OSError):
            return
        try:
            preamble_bytes = conn.recv(stream_worker.PREAMBLE_SIZE)
        except OSError:
            conn.close()
            return
        preamble = stream_worker.parse_preamble(preamble_bytes)
        if not preamble or preamble[1] != session.run_id or preamble[2] != index:
            conn.close()
            return
        session.attach_stream(index, conn, addr=addr, spawn_receiver=True)

    def _udp_worker():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((ctx.config.bind, port))
        except OSError as exc:
            ctx.log(f"session {session.run_id[:8]}: failed to bind udp port {port}: {exc}")
            return
        session.listener_sockets.append(sock)
        result = stream_worker.read_udp_preamble(sock, timeout_sec=30.0)
        if not result:
            return
        run_id, idx, addr = result
        if run_id != session.run_id or idx != index:
            return
        session.attach_stream(index, sock, addr=addr, spawn_receiver=True)

    target = _tcp_worker if transport == "tcp" else _udp_worker
    th = threading.Thread(target=target, daemon=True)
    th.start()
    return th


def _start_sender_threads(ctx, session: ThroughputSession):
    """Direction == 'down': server sends to the client on every stream."""
    transport = session.params["transport"]
    duration_sec = session.params.get("duration_sec")
    start_time = time.time()

    for i in range(session.num_streams):
        sock = session.stream_sockets.get(i)
        addr = session.stream_addrs.get(i)
        if sock is None:
            continue
        stop_ev = threading.Event()
        session.stream_stop_events[i] = stop_ev
        if transport == "tcp":
            th = threading.Thread(target=stream_worker.tcp_sender_loop,
                                   args=(sock, session.counters[i], stop_ev, duration_sec, start_time),
                                   daemon=True)
        else:
            th = threading.Thread(target=stream_worker.udp_sender_loop,
                                   args=(sock, addr, i, session.counters[i], stop_ev, duration_sec, start_time),
                                   daemon=True)
        th.start()
        session.threads.append(th)


def _control_dispatch_loop(ctx, session: ThroughputSession, control: ControlChannel):
    while not session.phase.is_stopped():
        try:
            msg = control.recv(timeout=1.0)
        except EOFError:
            session.phase.stop()
            session.stop_received_event.set()
            break

        if msg is None:
            continue

        mtype = msg.get("type")
        if mtype == "probe":
            latency_probe.handle_probe_message(control, msg)
        elif mtype == "start_data":
            session.phase.start_running()
            session.data_ready_event.set()
            if session.params["direction"] == "down":
                _start_sender_threads(ctx, session)
        elif mtype == "stop":
            session.phase.stop()
            session.stop_received_event.set()
            break


def handle_control_connection(ctx, sock: socket.socket, peer_addr):
    control = ControlChannel(sock)
    session = None
    try:
        msg = control.recv(timeout=30)
        if not msg or msg.get("type") != "hello":
            control.close()
            return

        run_id = msg["run_id"]
        params = msg["params"]
        n = max(1, int(params.get("streams", 1)))
        transport = params.get("transport", "tcp")
        params["streams"] = n

        base_port = ctx.config.udp_port if transport == "udp" else ctx.config.tcp_port
        ports = [base_port + i for i in range(n)]

        session = ThroughputSession(run_id, params, control)
        ctx.set_session(session)
        ctx.log(f"session {run_id[:8]} started: {transport} {params.get('direction')} "
                 f"streams={n} duration={params.get('duration_sec')}s ports={ports}")

        for i in range(1, n):
            _spawn_extra_stream_listener(ctx, session, i, ports[i])

        control.send("hello_ack", ok=True, ports=ports)

        deadline = time.time() + 30
        for i in range(n):
            while not session.stream_ready[i].is_set():
                if session.stream_ready[i].wait(timeout=0.1):
                    break
                if time.time() > deadline:
                    raise TimeoutError(f"stream {i} did not connect in time")
            control.send("stream_ready", index=i)

        msg = control.recv(timeout=60)
        if not msg or msg.get("type") != "start":
            raise ProtocolError("expected 'start' message")
        control.send("start_ack")

        dispatch_thread = threading.Thread(target=_control_dispatch_loop,
                                            args=(ctx, session, control), daemon=True)
        dispatch_thread.start()

        while not session.stop_received_event.is_set():
            time.sleep(constants.SAMPLE_INTERVAL_SEC)
            streams_payload = []
            for i, counters in session.counters.items():
                snap = counters.snapshot()
                if session.params["transport"] == "tcp" and session.params["direction"] == "down":
                    sock_i = session.stream_sockets.get(i)
                    if sock_i is not None:
                        retrans = tcp_info.get_tcp_retransmits(sock_i)
                        if retrans is not None:
                            counters.set_retransmits(retrans)
                            snap["retransmits"] = retrans
                streams_payload.append(snap)
            try:
                control.send("peer_stats", t=time.time(), streams=streams_payload)
            except OSError:
                break

        session.stop_all_streams()
        dispatch_thread.join(timeout=2)

        final_streams = [c.snapshot() for c in session.counters.values()]
        control.send("final_stats", streams=final_streams)
        control.send("done")

        ctx.log(f"session {run_id[:8]} complete")

    except (EOFError, TimeoutError, ProtocolError, OSError) as exc:
        ctx.log(f"session error ({peer_addr}): {exc}")
    finally:
        if session is not None:
            session.stop_all_streams()
            ctx.clear_session(session.run_id)
        control.close()
