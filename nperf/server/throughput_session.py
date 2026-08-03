"""Server-side handling of one throughput test: control handshake, dynamic
per-stream port listeners, and relaying this side's local stats back to the
client over the control channel every SAMPLE_INTERVAL_SEC.
"""

import multiprocessing
import queue
import socket
import threading
import time
import traceback

from ..common import constants, net
from ..transport import stream_worker, metrics, latency_probe, run_mode, shared_counters
from ..transport.control_connection import ControlChannel


class ProtocolError(Exception):
    pass


def _server_should_use_mp(params: dict, n: int) -> bool:
    """Mirror the client: parallelize streams across processes when the client
    requested >1 worker (params['procs']), there is >1 stream, and 'fork' is
    available so workers inherit the shared-memory counters."""
    if int(params.get("procs", 1)) <= 1 or n <= 1:
        return False
    return "fork" in multiprocessing.get_all_start_methods()


class ThroughputSession:
    def __init__(self, run_id: str, params: dict, control: ControlChannel):
        self.run_id = run_id
        self.params = params
        self.control = control
        self.phase = run_mode.RunPhase()
        n = int(params["streams"])
        self.num_streams = n
        self.stream_ready = {i: threading.Event() for i in range(n)}
        self.stream_sockets = {}
        self.stream_addrs = {}
        self.stream_stop_events = {}
        self.listener_sockets = []
        self.threads = []
        self.data_ready_event = threading.Event()
        self.stop_received_event = threading.Event()

        # Multiprocessing: stream 0 stays in the main process (it shares the
        # always-on base-port listener); streams 1..N-1 are handled by worker
        # processes so receive/send work uses multiple CPU cores. Counters live
        # in shared memory so the main process can relay/aggregate them.
        self.mp_enabled = _server_should_use_mp(params, n)
        self.worker_indices = list(range(1, n)) if self.mp_enabled else []
        self.worker_procs = []
        self.worker_setup_errors = []
        self.cleanup_terminated_pids = set()
        if self.mp_enabled:
            self.mp_ctx = multiprocessing.get_context("fork")
            self.shared = shared_counters.SharedCounters(n, ctx=self.mp_ctx)
            self.counters = {i: self.shared.view(i) for i in range(n)}
            self.start_flag = self.mp_ctx.Value("b", 0, lock=False)
            self.stop_flag = self.mp_ctx.Value("b", 0, lock=False)
            self.worker_ready_q = self.mp_ctx.Queue()
            self.worker_status_q = self.mp_ctx.Queue()
            # Per-stream send rate (bytes/sec, 0 = unpaced) for download tuning;
            # the client drives it via 'set_rate' control messages.
            self.rate_value = self.mp_ctx.Value("d", 0.0, lock=False)
        else:
            self.counters = {i: metrics.StreamCounters(i) for i in range(n)}
            self.rate_value = stream_worker.RateHolder(0.0)

    def attach_stream(self, index: int, sock, addr=None, spawn_receiver: bool = True):
        if self.params["transport"] == "tcp":
            stream_worker.configure_tcp_socket(sock, self.params.get("path_mtu"))
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
        stop_receivers = self.params["transport"] != "tcp" or self.params["direction"] == "down"
        if self.mp_enabled:
            if stop_receivers:
                self.stop_flag.value = 1
            for p in self.worker_procs:
                p.join(timeout=constants.WORKER_DRAIN_TIMEOUT_SEC)
            for p in self.worker_procs:
                if p.is_alive():
                    self.cleanup_terminated_pids.add(p.pid)
                    p.terminate()
                    p.join(timeout=2)
        if stop_receivers:
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
    family = session.params.get("family", socket.AF_INET)

    def _tcp_worker():
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            net.configure_ipv6_socket(sock)
            sock.bind(net.bind_address(ctx.config.bind, port, family))
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
        sock = socket.socket(family, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            net.configure_ipv6_socket(sock)
            net.configure_dont_fragment(sock)
            sock.bind(net.bind_address(ctx.config.bind, port, family))
        except OSError as exc:
            ctx.log(f"session {session.run_id[:8]}: failed to configure udp port {port}: {exc}")
            sock.close()
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


def _server_worker_proc_inner(assignments, run_id, role, transport, bind, duration_sec,
                              shared, start_flag, stop_flag, ready_q, rate_value=None,
                              path_mtu=None, family=socket.AF_INET):
    """Worker process handling a subset of streams (indices >= 1).

    Runs in phases so the main process never races the client:
      1. Bind (and, for TCP, listen on) every assigned port.
      2. Emit a single ("bound", ...) signal -- the main process waits for one
         from every worker before it tells the client to connect. Without this
         barrier the fork + child-interpreter startup can lag behind the
         client's connect(), producing spurious 'connection refused' errors.
      3. Accept/read the preamble for each stream, emitting ("ready", i, addr).
      4. Run the sender/receiver loops into the shared-memory counters.
    """
    listeners = {}
    for i, port in assignments:
        try:
            if transport == "tcp":
                ls = socket.socket(family, socket.SOCK_STREAM)
                ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                net.configure_ipv6_socket(ls)
                ls.bind(net.bind_address(bind, port, family))
                ls.listen(1)
                ls.settimeout(30.0)
            else:
                ls = socket.socket(family, socket.SOCK_DGRAM)
                ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                net.configure_ipv6_socket(ls)
                net.configure_dont_fragment(ls)
                ls.bind(net.bind_address(bind, port, family))
            listeners[i] = ls
        except OSError as exc:
            ready_q.put(("error", i, str(exc)))

    ready_q.put(("bound", None, None))

    socks = {}
    for i, _port in assignments:
        ls = listeners.get(i)
        if ls is None:
            continue
        if transport == "tcp":
            try:
                conn, addr = ls.accept()
                preamble_bytes = conn.recv(stream_worker.PREAMBLE_SIZE)
            except OSError as exc:
                ready_q.put(("error", i, str(exc)))
                ls.close()
                continue
            ls.close()
            preamble = stream_worker.parse_preamble(preamble_bytes)
            if not preamble or preamble[1] != run_id or preamble[2] != i:
                ready_q.put(("error", i, "bad preamble"))
                conn.close()
                continue
            stream_worker.configure_tcp_socket(conn, path_mtu)
            socks[i] = (conn, None)
            ready_q.put(("ready", i, addr))
        else:
            result = stream_worker.read_udp_preamble(ls, timeout_sec=30.0)
            if not result:
                ready_q.put(("error", i, "preamble timeout"))
                ls.close()
                continue
            rid, idx, addr = result
            if rid != run_id or idx != i:
                ready_q.put(("error", i, "bad preamble"))
                ls.close()
                continue
            socks[i] = (ls, addr)
            ready_q.put(("ready", i, addr))

    views = {i: shared.view(i) for i, _port in assignments}
    stream_worker.run_stream_group(socks, role, transport, views, duration_sec,
                                    wait_start=lambda: bool(start_flag.value),
                                    stopped=lambda: bool(stop_flag.value),
                                    rate_value=rate_value,
                                    path_mtu=path_mtu)
    for sock, _dest in socks.values():
        try:
            sock.close()
        except OSError:
            pass


def _server_worker_proc(assignments, run_id, role, transport, bind, duration_sec,
                        shared, start_flag, stop_flag, ready_q, status_q, rate_value=None,
                        path_mtu=None, family=socket.AF_INET):
    try:
        _server_worker_proc_inner(
            assignments, run_id, role, transport, bind, duration_sec, shared,
            start_flag, stop_flag, ready_q, rate_value, path_mtu, family)
    except BaseException as exc:
        process = multiprocessing.current_process()
        status_q.put({
            "name": process.name,
            "pid": process.pid,
            "assignments": [index for index, _port in assignments],
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        })
        raise


def _drain_worker_errors(status_q) -> list[dict]:
    errors = []
    while True:
        try:
            errors.append(status_q.get_nowait())
        except queue.Empty:
            return errors


def _server_worker_failure(session: ThroughputSession, require_stopped: bool = False):
    if not session.mp_enabled:
        return None
    errors = _drain_worker_errors(session.worker_status_q)
    details = [
        f"{error.get('name')} pid={error.get('pid')} streams={error.get('assignments')}: "
        f"{error.get('error')}\n{error.get('traceback', '').rstrip()}"
        for error in errors
    ]
    reported_pids = {error.get("pid") for error in errors}
    for process in session.worker_procs:
        terminated_during_cleanup = process.pid in getattr(session, "cleanup_terminated_pids", set())
        if (process.exitcode not in (None, 0) and process.pid not in reported_pids
                and not terminated_during_cleanup):
            details.append(f"{process.name} pid={process.pid} exited with code {process.exitcode}")
        if require_stopped and process.is_alive():
            details.append(f"{process.name} pid={process.pid} did not stop")
    return "\n".join(details) or None


def _spawn_server_workers(ctx, session: ThroughputSession, ports: list):
    """Start worker processes for streams 1..N-1 (stream 0 stays in main)."""
    params = session.params
    transport = params["transport"]
    role = "recv" if params["direction"] == "up" else "send"
    duration_sec = params.get("duration_sec")
    path_mtu = params.get("path_mtu")
    idxs = session.worker_indices
    nproc = max(1, min(int(params.get("procs", 1)), len(idxs)))
    groups = [[] for _ in range(nproc)]
    for k, i in enumerate(idxs):
        groups[k % nproc].append((i, ports[i]))

    active_groups = [g for g in groups if g]
    for group in active_groups:
        p = session.mp_ctx.Process(
            target=_server_worker_proc,
            args=(group, session.run_id, role, transport, ctx.config.bind, duration_sec,
                  session.shared, session.start_flag, session.stop_flag, session.worker_ready_q,
                  session.worker_status_q, session.rate_value, path_mtu,
                  params.get("family", socket.AF_INET)),
            daemon=True)
        p.start()
        session.worker_procs.append(p)

    # Barrier: wait until every worker reports its ports are bound/listening
    # before returning (the caller then sends hello_ack). This prevents the
    # client from racing ahead and hitting 'connection refused' while a forked
    # worker is still starting up.
    bound = 0
    deadline = time.time() + 30
    while bound < len(active_groups) and time.time() < deadline:
        try:
            status, idx, detail = session.worker_ready_q.get(timeout=1.0)
        except queue.Empty:
            failure = _server_worker_failure(session)
            if failure:
                raise ProtocolError(f"server worker crashed during setup:\n{failure}")
            continue
        if status == "bound":
            bound += 1
        elif status == "error":
            error = f"worker failed to bind stream {idx}: {detail}"
            session.worker_setup_errors.append(error)
            ctx.log(f"session {session.run_id[:8]}: {error}")
            raise ProtocolError(error)
    ctx.log(f"session {session.run_id[:8]}: {len(session.worker_procs)} worker "
             f"process(es) handling streams {idxs} ({role}); {bound} bound")


def _drain_worker_ready(ctx, session: ThroughputSession):
    """Background thread: mark worker-handled streams ready as they connect."""
    expected = len(session.worker_indices)

    def _loop():
        got = 0
        while got < expected and not session.stop_received_event.is_set():
            try:
                status, idx, addr = session.worker_ready_q.get(timeout=1.0)
            except queue.Empty:
                continue
            got += 1
            if status == "ready":
                session.stream_addrs[idx] = addr
                session.stream_ready[idx].set()
            elif status == "error":
                error = f"worker failed to prepare stream {idx}: {addr}"
                session.worker_setup_errors.append(error)
                ctx.log(f"session {session.run_id[:8]}: {error}")

    threading.Thread(target=_loop, daemon=True).start()


def _start_sender_threads(ctx, session: ThroughputSession):
    """Direction == 'down': server sends to the client on every stream. In
    multiprocessing mode the worker processes send their own streams, so the
    main process only drives stream 0."""
    transport = session.params["transport"]
    duration_sec = session.params.get("duration_sec")
    start_time = time.time()

    for i in range(session.num_streams):
        if session.mp_enabled and i in session.worker_indices:
            continue
        sock = session.stream_sockets.get(i)
        addr = session.stream_addrs.get(i)
        if sock is None:
            continue
        stop_ev = threading.Event()
        session.stream_stop_events[i] = stop_ev
        if transport == "tcp":
            th = threading.Thread(target=stream_worker.tcp_sender_loop,
                                   args=(sock, session.counters[i], stop_ev, duration_sec, start_time,
                                         session.params.get("path_mtu")),
                                   daemon=True)
        else:
            th = threading.Thread(target=stream_worker.udp_sender_loop,
                                   args=(sock, addr, i, session.counters[i], stop_ev, duration_sec, start_time),
                                   kwargs={"rate_value": session.rate_value,
                                           "path_mtu": session.params.get("path_mtu")}, daemon=True)
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
            try:
                latency_probe.handle_probe_message(control, msg)
            except OSError:
                session.phase.stop()
                session.stop_received_event.set()
                break
        elif mtype == "set_rate":
            # UDP download tuning: mirror the client-requested per-stream send
            # rate (bytes/sec, 0 = unpaced) onto our senders.
            try:
                session.rate_value.value = float(msg.get("per_stream_bps", 0.0))
            except (TypeError, ValueError):
                pass
        elif mtype == "start_data":
            session.phase.start_running()
            session.data_ready_event.set()
            if session.mp_enabled:
                session.start_flag.value = 1
            if session.params["direction"] == "down":
                _start_sender_threads(ctx, session)
        elif mtype == "stop":
            session.phase.stop()
            session.stop_received_event.set()
            break


def _wait_for_final_stats_ack(control: ControlChannel, timeout_sec: float) -> bool:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        message = control.recv(timeout=min(1.0, deadline - time.monotonic()))
        if message is None:
            continue
        if message.get("type") == "final_stats_ack":
            return True
        if message.get("type") == "probe":
            latency_probe.handle_probe_message(control, message)
    return False


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
        params["family"] = sock.family
        params["ip_version"] = net.ip_version_label(family=sock.family)
        n = max(1, int(params.get("streams", 1)))
        transport = params.get("transport", "tcp")
        params["streams"] = n

        base_port = ctx.config.udp_port if transport == "udp" else ctx.config.tcp_port
        ports = [base_port + i for i in range(n)]

        session = ThroughputSession(run_id, params, control)
        ctx.set_session(session)
        ctx.log(f"session {run_id[:8]} started: {transport} {params.get('direction')} "
                 f"streams={n} duration={params.get('duration_sec')}s ports={ports}")

        if session.mp_enabled:
            _spawn_server_workers(ctx, session, ports)
            _drain_worker_ready(ctx, session)
        else:
            for i in range(1, n):
                _spawn_extra_stream_listener(ctx, session, i, ports[i])

        control.send("hello_ack", ok=True, ports=ports)

        deadline = time.time() + 30
        for i in range(n):
            while not session.stream_ready[i].is_set():
                if session.stream_ready[i].wait(timeout=0.1):
                    break
                if session.worker_setup_errors:
                    raise ProtocolError("; ".join(session.worker_setup_errors))
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
            time.sleep(constants.PEER_STATS_INTERVAL_SEC)
            worker_failure = _server_worker_failure(session)
            if worker_failure:
                error = f"server worker process failed:\n{worker_failure}"
                control.send("error", error=error)
                raise ProtocolError(error)
            streams_payload = []
            for counters in session.counters.values():
                streams_payload.append(counters.snapshot())
            try:
                control.send("peer_stats", t=time.time(), streams=streams_payload)
            except OSError:
                break

        session.stop_all_streams()
        for thread in session.threads:
            thread.join(timeout=10)
        if session.params["transport"] == "tcp" and session.params["direction"] == "down":
            for data_socket in session.stream_sockets.values():
                try:
                    data_socket.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
        dispatch_thread.join(timeout=2)
        if session.cleanup_terminated_pids:
            ctx.log(
                f"session {session.run_id[:8]}: force-stopped receiver workers after drain timeout: "
                f"{sorted(session.cleanup_terminated_pids)}")
        worker_failure = _server_worker_failure(session, require_stopped=True)
        if worker_failure:
            error = f"server worker process failed:\n{worker_failure}"
            control.send("error", error=error)
            raise ProtocolError(error)

        final_streams = [c.snapshot() for c in session.counters.values()]
        control.send("final_stats", streams=final_streams)
        if not _wait_for_final_stats_ack(control, constants.SOCKET_TIMEOUT_SEC):
            raise ProtocolError("client did not acknowledge final peer statistics")
        control.send("done")

        ctx.log(f"session {run_id[:8]} complete")

    except (EOFError, TimeoutError, ProtocolError, OSError) as exc:
        ctx.log(f"session error ({peer_addr}): {exc}")
    finally:
        if session is not None:
            session.stop_all_streams()
            ctx.clear_session(session.run_id)
        control.close()
