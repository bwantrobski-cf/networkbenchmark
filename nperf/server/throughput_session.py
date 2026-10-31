"""Server-side handling of one throughput test: control handshake, dynamic
per-stream port listeners, and relaying this side's local stats back to the
client over the control channel every SAMPLE_INTERVAL_SEC.
"""

import ctypes
import math
import multiprocessing
import operator
import os
import queue
import socket
import threading
import time
import traceback

from ..common import constants, host_telemetry, net
from ..transport import (latency_probe, metrics, run_mode, shared_counters,
                         stream_worker, udp_batch)
from ..transport.control_connection import ControlChannel


class ProtocolError(Exception):
    pass


_UINT32_MAX = (1 << 32) - 1
_TCP_PREAMBLE_TIMEOUT_SEC = 2.0


def _server_should_use_mp(params: dict, n: int) -> bool:
    """Mirror the client: parallelize streams across processes when the client
    requested >1 worker (params['procs']), there is >1 stream, and 'fork' is
    available so workers inherit the shared-memory counters."""
    if int(params.get("procs", 1)) <= 1 or n <= 1:
        return False
    return "fork" in multiprocessing.get_all_start_methods()


def _allocate_stream_ports(config, transport, streams):
    base_port = config.udp_port if transport == "udp" else config.tcp_port
    reserved = (constants.IPSEC_NATT_PORT if transport == "udp"
                else getattr(config, "control_port", constants.DEFAULT_CONTROL_PORT))
    if not 1 <= base_port <= 65535 or base_port == reserved:
        raise ProtocolError(f"{transport} base port {base_port} is invalid or reserved")
    ports = []
    port = base_port
    while len(ports) < streams and port <= 65535:
        if port != reserved:
            ports.append(port)
        port += 1
    if len(ports) != streams:
        raise ProtocolError(
            f"not enough {transport} data ports for {streams} streams starting "
            f"at {base_port}; choose a lower server base port")
    return ports


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
        self._stream_attach_lock = threading.Lock()
        self._claimed_stream_indices = set()
        self._accepting_streams = True
        self.listener_sockets = []
        self.borrowed_stream_indices = set()
        self.threads = []
        self.data_ready_event = threading.Event()
        self.stop_received_event = threading.Event()
        self.data_started_event = threading.Event()
        self._timing_lock = threading.Lock()
        now = time.monotonic()
        self.start_wait_started_at = None
        self.data_started_at = None
        self.cleanup_started_at = None
        self.last_control_activity_at = now
        self.phase_deadline_at = None
        self.phase_deadline_message = None
        self.control_idle_deadline_enabled = False
        self.deadline_failure_message = None
        self.watchdog_stop_event = threading.Event()

        # Multiprocessing: stream 0 stays in the main process (it shares the
        # always-on base-port listener); streams 1..N-1 are handled by worker
        # processes so receive/send work uses multiple CPU cores. Counters live
        # in shared memory so the main process can relay/aggregate them.
        self.mp_enabled = _server_should_use_mp(params, n)
        self.worker_indices = list(range(1, n)) if self.mp_enabled else []
        self.worker_procs = []
        self.worker_assignments = {}
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
            self.trial_value = self.mp_ctx.Value(
                ctypes.c_uint32, 0, lock=False)
        else:
            self.counters = {i: metrics.StreamCounters(i) for i in range(n)}
            self.rate_value = stream_worker.RateHolder(0.0)
            self.trial_value = stream_worker.TrialHolder(0)

    def note_control_activity(self, now=None):
        with self._timing_lock:
            self.last_control_activity_at = (
                time.monotonic() if now is None else float(now))

    def begin_start_wait(self, now=None):
        now = time.monotonic() if now is None else float(now)
        with self._timing_lock:
            self.start_wait_started_at = now
            self.last_control_activity_at = now
            self.phase_deadline_at = (
                now + constants.MAX_RUN_TIME_FAILSAFE_SEC)
            self.phase_deadline_message = (
                "timed out waiting for start_data after "
                f"{constants.MAX_RUN_TIME_FAILSAFE_SEC:g} seconds")
            self.control_idle_deadline_enabled = True

    def mark_data_started(self, now=None):
        now = time.monotonic() if now is None else float(now)
        run_timeout = (
            min(float(self.params["duration_sec"]),
                constants.MAX_RUN_TIME_FAILSAFE_SEC)
            + constants.SOCKET_TIMEOUT_SEC)
        with self._timing_lock:
            self.data_started_at = now
            self.last_control_activity_at = now
            self.phase_deadline_at = now + run_timeout
            self.phase_deadline_message = (
                f"throughput run exceeded its {run_timeout:g} second deadline")
            self.control_idle_deadline_enabled = True

    def arm_phase_deadline(self, message, timeout_sec, now=None):
        now = time.monotonic() if now is None else float(now)
        with self._timing_lock:
            self.phase_deadline_at = now + max(0.0, float(timeout_sec))
            self.phase_deadline_message = str(message)
            self.control_idle_deadline_enabled = False

    def begin_cleanup(self, now=None):
        now = time.monotonic() if now is None else float(now)
        with self._timing_lock:
            if self.cleanup_started_at is not None:
                return
            self.cleanup_started_at = now
            self.phase_deadline_at = now + constants.FINAL_STATS_TIMEOUT_SEC
            self.phase_deadline_message = "server session cleanup timed out"
            self.control_idle_deadline_enabled = False

    def set_deadline_failure(self, message):
        with self._timing_lock:
            if self.deadline_failure_message is None:
                self.deadline_failure_message = str(message)

    def timing_snapshot(self):
        with self._timing_lock:
            return {
                "start_wait_started_at": self.start_wait_started_at,
                "data_started_at": self.data_started_at,
                "cleanup_started_at": self.cleanup_started_at,
                "last_control_activity_at": self.last_control_activity_at,
                "phase_deadline_at": self.phase_deadline_at,
                "phase_deadline_message": self.phase_deadline_message,
                "control_idle_deadline_enabled": self.control_idle_deadline_enabled,
                "deadline_failure_message": self.deadline_failure_message,
            }

    def try_attach_stream(self, index: int, sock, addr=None,
                          spawn_receiver: bool = True) -> bool:
        """Atomically claim and attach one stream, returning False if claimed."""
        with self._stream_attach_lock:
            if (not self._accepting_streams
                    or self.stop_received_event.is_set()
                    or index in self._claimed_stream_indices
                    or self.stream_ready[index].is_set()):
                return False

            self._claimed_stream_indices.add(index)
            stop_ev = None
            receiver_thread = None
            borrowed = (self.params["transport"] == "udp" and index == 0
                        and not spawn_receiver)
            try:
                if self.params["transport"] == "tcp":
                    stream_worker.configure_tcp_socket(
                        sock, self.params.get("path_mtu"),
                        counters=self.counters[index], total_streams=self.num_streams)
                else:
                    # Stream 0 borrows the always-on base socket, so a rejected
                    # or failed claim must never close it.
                    stream_worker.configure_udp_socket(
                        sock, self.counters[index])
                self.stream_sockets[index] = sock
                if borrowed:
                    self.borrowed_stream_indices.add(index)
                if addr is not None:
                    self.stream_addrs[index] = addr

                if self.params["direction"] == "up" and spawn_receiver:
                    stop_ev = threading.Event()
                    self.stream_stop_events[index] = stop_ev
                    if self.params["transport"] == "tcp":
                        receiver_thread = threading.Thread(
                            target=stream_worker.tcp_receiver_loop,
                            args=(sock, self.counters[index], stop_ev,
                                  self.params.get("path_mtu")), daemon=True)
                    else:
                        receiver_thread = threading.Thread(
                            target=stream_worker.udp_receiver_loop,
                            args=(sock, self.counters[index], stop_ev),
                            kwargs={
                                "known_peer_addr": addr,
                                "path_mtu": self.params.get("path_mtu"),
                                "session_cookie":
                                    stream_worker.udp_session_cookie(self.run_id),
                            }, daemon=True)
                    self.threads.append(receiver_thread)
                    receiver_thread.start()

                self.stream_ready[index].set()
                return True
            except BaseException:
                if stop_ev is not None:
                    stop_ev.set()
                    if self.stream_stop_events.get(index) is stop_ev:
                        self.stream_stop_events.pop(index, None)
                if receiver_thread is not None:
                    try:
                        self.threads.remove(receiver_thread)
                    except ValueError:
                        pass
                if self.stream_sockets.get(index) is sock:
                    self.stream_sockets.pop(index, None)
                if addr is not None and self.stream_addrs.get(index) == addr:
                    self.stream_addrs.pop(index, None)
                if borrowed:
                    self.borrowed_stream_indices.discard(index)
                self.stream_ready[index].clear()
                self._claimed_stream_indices.discard(index)
                raise

    def attach_stream(self, index: int, sock, addr=None,
                      spawn_receiver: bool = True) -> bool:
        return self.try_attach_stream(
            index, sock, addr=addr, spawn_receiver=spawn_receiver)

    def stop_all_streams(self, abort: bool = False):
        if abort:
            self.stop_received_event.set()
        with self._stream_attach_lock:
            self._accepting_streams = False
            stream_stop_events = tuple(self.stream_stop_events.values())
            listener_sockets = tuple(self.listener_sockets)
            stream_sockets = tuple(self.stream_sockets.items())
            borrowed_stream_indices = frozenset(self.borrowed_stream_indices)
        stop_receivers = (abort or self.params["transport"] != "tcp"
                          or self.params["direction"] == "down")
        if stop_receivers:
            for ev in stream_stop_events:
                ev.set()
        if self.mp_enabled:
            if stop_receivers:
                self.stop_flag.value = 1
            drain_timeout = (constants.WORKER_DRAIN_TIMEOUT_SEC if stop_receivers
                             else constants.TCP_RECEIVER_DRAIN_TIMEOUT_SEC)
            deadline = time.monotonic() + drain_timeout
            for p in self.worker_procs:
                p.join(timeout=max(0.0, deadline - time.monotonic()))
            for p in self.worker_procs:
                if p.is_alive():
                    self.cleanup_terminated_pids.add(p.pid)
                    p.terminate()
                    p.join(timeout=2)
        for sock in listener_sockets:
            try:
                sock.close()
            except OSError:
                pass
        if abort:
            for index, sock in stream_sockets:
                if index in borrowed_stream_indices:
                    continue
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass


def _accept_matching_tcp_stream(listener, run_id, index, deadline, cancelled):
    while not cancelled():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        listener.settimeout(min(
            remaining, max(0.01, constants.CONTROL_RECV_POLL_SEC)))
        try:
            conn, addr = listener.accept()
        except socket.timeout:
            continue

        keep = False
        try:
            preamble_deadline = min(
                deadline,
                time.monotonic() + _TCP_PREAMBLE_TIMEOUT_SEC)
            preamble_bytes = stream_worker.recv_exact(
                conn, stream_worker.PREAMBLE_SIZE, preamble_deadline,
                cancelled=cancelled)
            preamble = (stream_worker.parse_preamble(preamble_bytes)
                        if preamble_bytes is not None else None)
            if (preamble and preamble[0] == constants.DATA_STREAM_MAGIC
                    and preamble[1] == run_id and preamble[2] == index
                    and not cancelled()):
                keep = True
                return conn, addr
        except OSError:
            pass
        finally:
            if not keep:
                try:
                    conn.close()
                except OSError:
                    pass
    return None


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
            sock.close()
            return
        session.listener_sockets.append(sock)
        deadline = time.monotonic() + constants.SOCKET_TIMEOUT_SEC
        try:
            result = _accept_matching_tcp_stream(
                sock, session.run_id, index, deadline,
                session.stop_received_event.is_set)
        except OSError:
            return
        finally:
            try:
                sock.close()
            except OSError:
                pass
        if result is not None:
            conn, addr = result
            if session.stop_received_event.is_set():
                conn.close()
                return
            try:
                attached = session.try_attach_stream(
                    index, conn, addr=addr, spawn_receiver=True)
            except Exception:
                conn.close()
                raise
            if not attached:
                conn.close()

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
        result = stream_worker.read_udp_preamble(
            sock, timeout_sec=constants.SOCKET_TIMEOUT_SEC,
            expected_run_id=session.run_id, expected_stream_index=index)
        if not result:
            return
        run_id, idx, addr = result
        if run_id != session.run_id or idx != index:
            return
        try:
            attached = session.try_attach_stream(
                index, sock, addr=addr, spawn_receiver=True)
        except Exception:
            sock.close()
            raise
        if not attached:
            sock.close()

    target = _tcp_worker if transport == "tcp" else _udp_worker
    th = threading.Thread(target=target, daemon=True)
    th.start()
    return th


def _server_worker_proc_inner(assignments, run_id, role, transport, bind, duration_sec,
                               shared, start_flag, stop_flag, ready_q, rate_value=None,
                               path_mtu=None, family=socket.AF_INET, trial_value=None):
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
        ls = None
        try:
            if transport == "tcp":
                ls = socket.socket(family, socket.SOCK_STREAM)
                ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                net.configure_ipv6_socket(ls)
                ls.bind(net.bind_address(bind, port, family))
                ls.listen(1)
            else:
                ls = socket.socket(family, socket.SOCK_DGRAM)
                ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                net.configure_ipv6_socket(ls)
                net.configure_dont_fragment(ls)
                ls.bind(net.bind_address(bind, port, family))
            listeners[i] = ls
        except OSError as exc:
            if ls is not None:
                ls.close()
            ready_q.put(("error", i, str(exc)))

    ready_q.put(("bound", None, None))

    socks = {}
    setup_deadline = time.monotonic() + constants.SOCKET_TIMEOUT_SEC
    for i, _port in assignments:
        ls = listeners.get(i)
        if ls is None:
            continue
        if transport == "tcp":
            try:
                result = _accept_matching_tcp_stream(
                    ls, run_id, i, setup_deadline,
                    lambda: bool(stop_flag.value))
            except OSError as exc:
                ready_q.put(("error", i, str(exc)))
                ls.close()
                continue
            ls.close()
            if result is None:
                if not stop_flag.value:
                    ready_q.put(("error", i, "preamble timeout"))
                continue
            conn, addr = result
            stream_worker.configure_tcp_socket(
                conn, path_mtu, total_streams=shared.num_streams)
            socks[i] = (conn, None)
            ready_q.put(("ready", i, addr))
        else:
            result = stream_worker.read_udp_preamble(
                ls, timeout_sec=constants.SOCKET_TIMEOUT_SEC,
                expected_run_id=run_id, expected_stream_index=i)
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

    if stop_flag.value:
        for sock, _dest in socks.values():
            sock.close()
        return

    views = {i: shared.view(i) for i, _port in assignments}
    worker_options = {
        "wait_start": lambda: bool(start_flag.value),
        "stopped": lambda: bool(stop_flag.value),
        "rate_value": rate_value,
        "path_mtu": path_mtu,
    }
    if transport == "udp":
        worker_options["trial_value"] = trial_value
        worker_options["session_cookie"] = stream_worker.udp_session_cookie(run_id)
    stream_worker.run_stream_group(
        socks, role, transport, views, duration_sec, **worker_options)
    for sock, _dest in socks.values():
        try:
            sock.close()
        except OSError:
            pass


def _server_worker_proc(assignments, run_id, role, transport, bind, duration_sec,
                         shared, start_flag, stop_flag, ready_q, status_q, rate_value=None,
                         path_mtu=None, family=socket.AF_INET, trial_value=None):
    try:
        _server_worker_proc_inner(
            assignments, run_id, role, transport, bind, duration_sec, shared,
            start_flag, stop_flag, ready_q, rate_value, path_mtu, family,
            trial_value)
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
                  params.get("family", socket.AF_INET), session.trial_value),
            daemon=True)
        p.start()
        session.worker_procs.append(p)
        session.worker_assignments[p.pid] = [index for index, _port in group]

    # Barrier: wait until every worker reports its ports are bound/listening
    # before returning (the caller then sends hello_ack). This prevents the
    # client from racing ahead and hitting 'connection refused' while a forked
    # worker is still starting up.
    bound = 0
    deadline = time.monotonic() + constants.SOCKET_TIMEOUT_SEC
    while bound < len(active_groups) and time.monotonic() < deadline:
        try:
            remaining = max(0.0, deadline - time.monotonic())
            status, idx, detail = session.worker_ready_q.get(
                timeout=min(1.0, remaining))
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
    if bound < len(active_groups):
        raise TimeoutError("server workers did not bind their stream ports in time")
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
    start_time = None
    start_gate = threading.Event()

    def run_sender(i, sock, addr, stop_ev):
        start_gate.wait()
        if stop_ev.is_set():
            return
        if transport == "tcp":
            stream_worker.tcp_sender_loop(
                sock, session.counters[i], stop_ev, duration_sec, start_time,
                session.params.get("path_mtu"))
        else:
            stream_worker.udp_sender_loop(
                sock, addr, i, session.counters[i], stop_ev, duration_sec, start_time,
                rate_value=session.rate_value, path_mtu=session.params.get("path_mtu"),
                trial_value=session.trial_value,
                session_cookie=stream_worker.udp_session_cookie(session.run_id),
                # The borrowed base port must stay available for echo traffic.
                connect_socket=i != 0)

    prepared = False
    try:
        for i in range(session.num_streams):
            if session.mp_enabled and i in session.worker_indices:
                continue
            sock = session.stream_sockets.get(i)
            addr = session.stream_addrs.get(i)
            if sock is None:
                continue
            stop_ev = threading.Event()
            session.stream_stop_events[i] = stop_ev
            th = threading.Thread(
                target=run_sender, args=(i, sock, addr, stop_ev), daemon=True)
            th.start()
            session.threads.append(th)
        start_time = time.time()
        prepared = True
    finally:
        if not prepared:
            for event in session.stream_stop_events.values():
                event.set()
        start_gate.set()


def _wait_for_sender_trial(session: ThroughputSession, trial_id: int) -> list[int]:
    expected = set(range(session.num_streams))
    deadline = time.monotonic() + constants.SOCKET_TIMEOUT_SEC
    while True:
        observed = {
            index for index, counters in session.counters.items()
            if counters.snapshot().get("observed_trial_id") == trial_id
        }
        if observed == expected:
            return []
        worker_failure = _server_worker_failure(session)
        if worker_failure:
            raise ProtocolError(
                f"server worker failed while entering UDP trial {trial_id}:\n"
                f"{worker_failure}")
        if time.monotonic() >= deadline:
            return sorted(expected - observed)
        time.sleep(0.002)


def _control_dispatch_loop(ctx, session: ThroughputSession, control: ControlChannel):
    while not session.phase.is_stopped():
        try:
            msg = control.recv(timeout=1.0)
        except EOFError:
            session.begin_cleanup()
            session.phase.stop()
            session.stop_received_event.set()
            break

        if msg is None:
            continue

        session.note_control_activity()
        mtype = msg.get("type")
        if mtype == "probe":
            try:
                latency_probe.handle_probe_message(control, msg)
            except OSError:
                session.begin_cleanup()
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
        elif mtype == "set_trial" and session.params["transport"] == "udp":
            try:
                raw_trial_id = msg["trial_id"]
                if isinstance(raw_trial_id, bool):
                    raise TypeError
                trial_id = operator.index(raw_trial_id)
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            if 0 <= trial_id <= _UINT32_MAX:
                # Trial 0 is the non-measurement/warmup interval.
                session.trial_value.value = trial_id
                if (session.params["direction"] == "down"
                        and session.phase.phase == run_mode.RunPhase.RUNNING):
                    try:
                        missing = _wait_for_sender_trial(session, trial_id)
                    except ProtocolError as exc:
                        control.send("error", error=str(exc))
                        session.begin_cleanup()
                        session.phase.stop()
                        session.stop_received_event.set()
                        break
                    if missing:
                        error = (
                            f"server UDP senders did not observe trial {trial_id}: "
                            f"missing_streams={missing}")
                        control.send("error", error=error)
                        session.begin_cleanup()
                        session.phase.stop()
                        session.stop_received_event.set()
                        break
                control.send("set_trial_ack", trial_id=trial_id)
        elif mtype == "start_data":
            if session.data_started_event.is_set():
                error = "duplicate start_data message"
                control.send("error", error=error)
                session.begin_cleanup()
                session.phase.stop()
                session.stop_received_event.set()
                break
            session.data_started_event.set()
            session.mark_data_started()
            session.phase.start_running()
            session.data_ready_event.set()
            if session.mp_enabled:
                session.start_flag.value = 1
            if session.params["direction"] == "down":
                _start_sender_threads(ctx, session)
        elif mtype == "stop":
            session.begin_cleanup()
            session.phase.stop()
            session.stop_received_event.set()
            break


def _session_deadline_error(session: ThroughputSession, now=None):
    now = time.monotonic() if now is None else float(now)
    timing = session.timing_snapshot()
    if timing["deadline_failure_message"] is not None:
        return timing["deadline_failure_message"]
    phase_deadline = timing["phase_deadline_at"]
    if phase_deadline is not None and now >= phase_deadline:
        return timing["phase_deadline_message"] or "throughput session deadline expired"
    if (timing["control_idle_deadline_enabled"]
            and now - timing["last_control_activity_at"]
            >= constants.SOCKET_TIMEOUT_SEC):
        return ("control channel was idle for "
                f"{constants.SOCKET_TIMEOUT_SEC:g} seconds")
    return None


def _session_watchdog(session: ThroughputSession, control: ControlChannel):
    while not session.watchdog_stop_event.wait(
            constants.CONTROL_RECV_POLL_SEC):
        deadline_error = _session_deadline_error(session)
        if deadline_error is None:
            continue
        session.set_deadline_failure(deadline_error)
        session.phase.stop()
        session.stop_received_event.set()
        if session.watchdog_stop_event.wait(constants.CONTROL_RECV_POLL_SEC):
            return
        # Closing from the watchdog also interrupts a blocked sendall(), which
        # is otherwise able to bypass every deadline in the handler thread.
        control.close()
        session.watchdog_stop_event.wait()
        return


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


def _server_topology(session: ThroughputSession) -> dict:
    controller_pid = os.getpid()
    controller_streams = ([0] if session.mp_enabled
                          else list(range(session.num_streams)))
    workers = [{"pid": controller_pid, "streams": controller_streams}]
    for process in session.worker_procs:
        workers.append({
            "pid": process.pid,
            "streams": list(session.worker_assignments.get(process.pid, [])),
        })
    topology = {
        "controller_pid": controller_pid,
        "multiprocessing": session.mp_enabled,
        "workers": workers,
    }
    if session.params["transport"] == "udp":
        topology["udp_batch"] = udp_batch.backend_status()
    return topology


def _server_host_sample(session: ThroughputSession) -> dict:
    """Capture telemetry without allowing observability to fail a test run."""
    try:
        topology = _server_topology(session)
    except Exception as exc:
        topology = {
            "controller_pid": os.getpid(),
            "multiprocessing": False,
            "workers": [],
            "error": f"{type(exc).__name__}: {exc}",
        }
    pids = [worker.get("pid") for worker in topology.get("workers", [])
            if isinstance(worker.get("pid"), int)]
    try:
        sample = host_telemetry.snapshot_endpoint(pid=os.getpid(), pids=pids)
        if not isinstance(sample, dict):
            raise TypeError("snapshot_endpoint returned a non-dictionary value")
        sample = dict(sample)
    except Exception as exc:
        sample = {
            "available": False,
            "complete": False,
            "reason": "unavailable",
            "error": f"{type(exc).__name__}: {exc}",
            "captured_at": time.monotonic(),
            "pid": os.getpid(),
            "host_cpu": None,
            "process_cpu": None,
            "udp": None,
        }
    try:
        sample["topology"] = topology
    except Exception as exc:
        sample["topology"] = {
            "controller_pid": os.getpid(),
            "multiprocessing": session.mp_enabled,
            "workers": [],
            "error": f"{type(exc).__name__}: {exc}",
        }
    return sample


def _udp_hello_fields() -> dict:
    return {
        "udp_wire_version": stream_worker.UDP_WIRE_VERSION,
        "udp_batch": udp_batch.backend_status(),
    }


def _validate_session_params(params: dict) -> int:
    if not isinstance(params, dict):
        raise ProtocolError("throughput parameters must be an object")
    raw_streams = params.get("streams", 1)
    if isinstance(raw_streams, bool):
        raise ProtocolError("streams must be an integer")
    try:
        streams = operator.index(raw_streams)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProtocolError("streams must be an integer") from exc
    if not 1 <= streams <= constants.MAX_PARALLEL_STREAMS:
        raise ProtocolError(
            f"streams must be from 1 to {constants.MAX_PARALLEL_STREAMS}")
    raw_duration = params.get(
        "duration_sec", constants.DEFAULT_TEST_DURATION_SEC)
    if isinstance(raw_duration, bool):
        raise ProtocolError("duration_sec must be a finite positive number no "
                            f"greater than {constants.MAX_RUN_TIME_FAILSAFE_SEC}")
    try:
        duration = float(raw_duration)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProtocolError("duration_sec must be a finite positive number no "
                            f"greater than {constants.MAX_RUN_TIME_FAILSAFE_SEC}") from exc
    if (not math.isfinite(duration) or duration <= 0
            or duration > constants.MAX_RUN_TIME_FAILSAFE_SEC):
        raise ProtocolError("duration_sec must be a finite positive number no "
                            f"greater than {constants.MAX_RUN_TIME_FAILSAFE_SEC}")
    path_mtu = params.get("path_mtu")
    if (path_mtu is not None
            and (isinstance(path_mtu, bool) or not isinstance(path_mtu, int)
                 or not constants.PMTUD_MIN_MTU
                 <= path_mtu <= constants.PMTUD_JUMBO_MAX_MTU)):
        raise ProtocolError(
            f"path_mtu must be an integer from {constants.PMTUD_MIN_MTU} "
            f"to {constants.PMTUD_JUMBO_MAX_MTU}")
    params["duration_sec"] = duration
    return streams


def handle_control_connection(ctx, sock: socket.socket, peer_addr):
    control = ControlChannel(sock)
    session = None
    session_claimed = False
    dispatch_thread = None
    watchdog_thread = None
    try:
        msg = control.recv(timeout=constants.SOCKET_TIMEOUT_SEC)
        if not msg or msg.get("type") != "hello":
            control.close()
            return

        run_id = msg["run_id"]
        params = msg["params"]
        try:
            n = _validate_session_params(params)
            transport = params.get("transport", "tcp")
            ports = _allocate_stream_ports(ctx.config, transport, n)
        except ProtocolError as exc:
            control.send("hello_ack", ok=False, error=str(exc))
            raise
        params["family"] = sock.family
        params["ip_version"] = net.ip_version_label(family=sock.family)
        params["streams"] = n

        if transport == "udp":
            requested_wire_version = params.get("udp_wire_version")
            try:
                wire_version_matches = (
                    not isinstance(requested_wire_version, bool)
                    and operator.index(requested_wire_version)
                    == stream_worker.UDP_WIRE_VERSION)
            except TypeError:
                wire_version_matches = False
            if not wire_version_matches:
                error = ("UDP wire version mismatch: client requested "
                         f"{requested_wire_version!r}; server requires "
                         f"{stream_worker.UDP_WIRE_VERSION}")
                control.send("hello_ack", ok=False, error=error,
                             **_udp_hello_fields())
                raise ProtocolError(error)

        session = ThroughputSession(run_id, params, control)
        if ctx.set_session(session) is False:
            error = "server is busy with another throughput session"
            control.send("hello_ack", ok=False, error=error, **(
                _udp_hello_fields() if transport == "udp" else {}))
            raise ProtocolError(error)
        session_claimed = True
        session.arm_phase_deadline(
            "server session setup timed out", constants.SOCKET_TIMEOUT_SEC)
        watchdog_thread = threading.Thread(
            target=_session_watchdog, args=(session, control), daemon=True)
        watchdog_thread.start()
        ctx.log(f"session {run_id[:8]} started: {transport} {params.get('direction')} "
                 f"streams={n} duration={params.get('duration_sec')}s ports={ports}")

        if session.mp_enabled:
            _spawn_server_workers(ctx, session, ports)
            _drain_worker_ready(ctx, session)
        else:
            for i in range(1, n):
                _spawn_extra_stream_listener(ctx, session, i, ports[i])

        session.arm_phase_deadline(
            "data stream setup timed out", constants.SOCKET_TIMEOUT_SEC)
        hello_fields = _udp_hello_fields() if transport == "udp" else {}
        control.send("hello_ack", ok=True, ports=ports, **hello_fields)

        deadline = time.monotonic() + constants.SOCKET_TIMEOUT_SEC
        for i in range(n):
            while not session.stream_ready[i].is_set():
                remaining = max(0.0, deadline - time.monotonic())
                if session.stream_ready[i].wait(timeout=min(0.1, remaining)):
                    break
                if session.worker_setup_errors:
                    raise ProtocolError("; ".join(session.worker_setup_errors))
                if time.monotonic() > deadline:
                    raise TimeoutError(f"stream {i} did not connect in time")
            control.send("stream_ready", index=i)

        start_timeout = min(
            constants.SOCKET_TIMEOUT_SEC * 2,
            constants.MAX_RUN_TIME_FAILSAFE_SEC)
        session.arm_phase_deadline(
            "start handshake timed out", start_timeout)
        msg = control.recv(timeout=start_timeout)
        if not msg or msg.get("type") != "start":
            raise ProtocolError("expected 'start' message")
        control.send("start_ack")
        session.begin_start_wait()

        dispatch_thread = threading.Thread(target=_control_dispatch_loop,
                                            args=(ctx, session, control), daemon=True)
        dispatch_thread.start()

        while not session.stop_received_event.wait(
                constants.PEER_STATS_INTERVAL_SEC):
            deadline_error = _session_deadline_error(session)
            if deadline_error:
                session.set_deadline_failure(deadline_error)
                session.phase.stop()
                session.stop_received_event.set()
                try:
                    control.send("error", error=deadline_error)
                except OSError:
                    pass
                raise TimeoutError(deadline_error)
            worker_failure = _server_worker_failure(session)
            if worker_failure:
                error = f"server worker process failed:\n{worker_failure}"
                control.send("error", error=error)
                raise ProtocolError(error)
            streams_payload = []
            for counters in session.counters.values():
                streams_payload.append(counters.snapshot())
            try:
                control.send("peer_stats", t=time.time(), streams=streams_payload,
                             host_sample=_server_host_sample(session))
            except OSError:
                break

        deadline_error = session.timing_snapshot()["deadline_failure_message"]
        if deadline_error:
            try:
                control.send("error", error=deadline_error)
            except OSError:
                pass
            raise TimeoutError(deadline_error)

        session.begin_cleanup()

        # Capture the terminal host state before signalling or joining workers.
        final_host_sample = _server_host_sample(session)
        drain_timeout = (constants.TCP_RECEIVER_DRAIN_TIMEOUT_SEC
                         if transport == "tcp" and params["direction"] == "up"
                         else constants.WORKER_DRAIN_TIMEOUT_SEC)
        thread_deadline = time.monotonic() + drain_timeout
        session.stop_all_streams()
        for thread in session.threads:
            thread.join(timeout=max(0.0, thread_deadline - time.monotonic()))
        stuck_threads = [thread.name for thread in session.threads if thread.is_alive()]
        if stuck_threads:
            raise ProtocolError(
                f"server data threads did not stop: {stuck_threads}")
        if session.params["transport"] == "tcp" and session.params["direction"] == "down":
            for data_socket in session.stream_sockets.values():
                try:
                    data_socket.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
        dispatch_thread.join(timeout=2)
        if session.cleanup_terminated_pids:
            error = (
                "server force-stopped data workers; counters are incomplete: "
                f"pids={sorted(session.cleanup_terminated_pids)}")
            control.send("error", error=error)
            raise ProtocolError(error)
        worker_failure = _server_worker_failure(session, require_stopped=True)
        if worker_failure:
            error = f"server worker process failed:\n{worker_failure}"
            control.send("error", error=error)
            raise ProtocolError(error)

        final_streams = [c.snapshot() for c in session.counters.values()]
        final_udp_backend = None
        if session.params["transport"] == "udp":
            final_udp_backend = udp_batch.aggregate_backend_status(final_streams)
            try:
                final_host_sample["topology"]["udp_batch"] = final_udp_backend
            except (KeyError, TypeError):
                pass
        session.arm_phase_deadline(
            "final control exchange timed out", constants.SOCKET_TIMEOUT_SEC)
        control.send("final_stats", streams=final_streams,
                     host_sample=final_host_sample,
                     udp_batch=final_udp_backend)
        if not _wait_for_final_stats_ack(control, constants.SOCKET_TIMEOUT_SEC):
            raise ProtocolError("client did not acknowledge final peer statistics")
        control.send("done")

        ctx.log(f"session {run_id[:8]} complete")

    except (EOFError, TimeoutError, ProtocolError, OSError) as exc:
        ctx.log(f"session error ({peer_addr}): {exc}")
    finally:
        try:
            if session is not None and session_claimed:
                session.begin_cleanup()
                session.phase.stop()
                session.stop_received_event.set()
                try:
                    session.stop_all_streams(abort=True)
                finally:
                    try:
                        ctx.clear_session(session.run_id)
                    finally:
                        session.watchdog_stop_event.set()
        finally:
            control.close()
            if dispatch_thread is not None:
                dispatch_thread.join(timeout=2)
            if watchdog_thread is not None:
                watchdog_thread.join(timeout=2)
