"""Client-side throughput test orchestration.

Merges iperf3-style parallel streams (-P) and upload/download (-R) with
bbperf-style calibration -> running lifecycle and latency-under-load /
bufferbloat measurement (see transport.metrics.compute_bufferbloat).
"""

import multiprocessing
import socket
import threading
import time
import uuid

from ..common import addr, constants, logging_store, tcp_info
from ..diagnostics import connectivity
from ..diagnostics.pmtud import PmtudError, run_pmtud
from ..transport import stream_worker, metrics, latency_probe, run_mode, shared_counters
from ..transport.control_connection import ControlChannel


class ThroughputClientConfig:
    def __init__(self, server_ip, control_port=None, transport="tcp", direction="up",
                 streams=1, duration_sec=constants.DEFAULT_TEST_DURATION_SEC,
                 start_port=None, bind="0.0.0.0", calibration_sec=constants.CALIBRATION_TIME_SEC,
                 probe_interval_sec=constants.PROBE_INTERVAL_SEC, procs=1,
                 acceptable_loss_pct=None, max_duration_sec=None, path_mtu=None,
                 pmtud_info=None):
        self.server_host = server_ip
        self.control_port = control_port or constants.DEFAULT_CONTROL_PORT
        self.transport = transport
        self.direction = direction
        self.streams = max(1, int(streams))
        self.duration_sec = duration_sec
        default_start_port = constants.DEFAULT_UDP_PORT if transport == "udp" else constants.DEFAULT_TCP_PORT
        self.start_port = start_port or default_start_port
        self.bind = bind
        self.calibration_sec = calibration_sec
        self.probe_interval_sec = probe_interval_sec
        # Number of worker processes to spread streams across (1 = threaded,
        # the default). Clamped to the stream count at run time.
        self.procs = max(1, int(procs))
        # UDP loss-target tuning: when acceptable_loss_pct is set (UDP only),
        # the client ramps the send rate (AIMD) to find the max rate meeting the
        # target, and duration becomes a *max* time budget for the search.
        self.acceptable_loss_pct = (float(acceptable_loss_pct)
                                    if acceptable_loss_pct is not None else None)
        self.max_duration_sec = (float(max_duration_sec) if max_duration_sec is not None
                                 else constants.DEFAULT_MAX_DURATION_SEC)
        self.path_mtu = path_mtu
        self.pmtud_info = pmtud_info

    @property
    def tune_udp(self) -> bool:
        """True when this run should perform the UDP loss-target rate ramp."""
        return self.transport == "udp" and self.acceptable_loss_pct is not None

    @property
    def sender_duration_sec(self) -> float:
        """How long senders should stay alive. During tuning they must run for
        the whole search budget (the controller stops them early); otherwise
        the fixed test duration."""
        return self.max_duration_sec if self.tune_udp else self.duration_sec


class ThroughputError(Exception):
    pass


def _discover_path_mtu(cfg: ThroughputClientConfig, server_ip: str, log_base_dir: str = None):
    pmtud_udp_port = cfg.start_port if cfg.transport == "udp" else constants.DEFAULT_UDP_PORT
    try:
        pmtud_result = run_pmtud(server_ip, udp_port=pmtud_udp_port)
    except (PmtudError, OSError, ValueError) as exc:
        pmtud_result = {
            "server_ip": server_ip,
            "udp_port": pmtud_udp_port,
            "success": False,
            "connectivity_confirmed": False,
            "path_mtu": None,
            "path_mtu_payload_bytes": None,
            "reported_next_hop_mtu": None,
            "trace": [],
            "error": str(exc),
        }

    fallback_used = False
    if not pmtud_result.get("success") or not isinstance(pmtud_result.get("path_mtu"), int):
        ping_result = connectivity.icmp_ping(server_ip)
        pmtud_result["icmp_ping"] = ping_result
        if ping_result.get("success"):
            fallback_used = True
            pmtud_result["path_mtu"] = constants.FALLBACK_PATH_MTU
            pmtud_result["path_mtu_source"] = "icmp_reachability_fallback"
            pmtud_result["discovery_blocked"] = True
            pmtud_result["fallback_reason"] = "PMTUD discovery failed but ICMP ping succeeded"
        else:
            pmtud_result["path_mtu_source"] = "unavailable"

    text_lines = [
        f"PMTUD to {server_ip}:{pmtud_udp_port}",
        f"success={pmtud_result.get('success')} connectivity_confirmed={pmtud_result.get('connectivity_confirmed')}",
        f"path_mtu={pmtud_result.get('path_mtu')} payload={pmtud_result.get('path_mtu_payload_bytes')}",
        f"path_mtu_source={pmtud_result.get('path_mtu_source', 'pmtud')} fallback_used={fallback_used}",
        str(pmtud_result),
    ]
    pmtud_json_path, pmtud_log_path = logging_store.save_result(
        constants.TEST_TYPE_PMTUD, pmtud_result, text_lines, base_dir=log_base_dir)
    path_mtu = pmtud_result.get("path_mtu")
    if not isinstance(path_mtu, int):
        raise ThroughputError(pmtud_result.get("error") or "PMTUD did not determine a path MTU and ping failed")

    return path_mtu, {
        "server_ip": server_ip,
        "udp_port": pmtud_udp_port,
        "success": bool(pmtud_result.get("success")),
        "path_mtu": path_mtu,
        "path_mtu_source": pmtud_result.get("path_mtu_source", "pmtud"),
        "fallback_used": fallback_used,
        "path_mtu_payload_bytes": pmtud_result.get("path_mtu_payload_bytes"),
        "reported_next_hop_mtu": pmtud_result.get("reported_next_hop_mtu"),
        "log_json": pmtud_json_path,
        "log_text": pmtud_log_path,
    }


def _client_dispatch_loop(control: ControlChannel, phase: run_mode.RunPhase,
                           rtt_tracker: metrics.RttTracker, counters: dict,
                           done_event: threading.Event):
    while not phase.is_stopped():
        try:
            msg = control.recv(timeout=1.0)
        except EOFError:
            phase.stop()
            done_event.set()
            break
        if msg is None:
            continue

        mtype = msg.get("type")
        if mtype == "probe_ack":
            rtt_ms = latency_probe.rtt_ms_from_ack(msg)
            if rtt_ms is not None:
                rtt_tracker.add_sample(rtt_ms, phase.phase)
        elif mtype == "peer_stats":
            for snap in msg.get("streams", []):
                idx = snap.get("index")
                if idx in counters:
                    counters[idx].merge_peer_snapshot(snap)
        elif mtype == "final_stats":
            for snap in msg.get("streams", []):
                idx = snap.get("index")
                if idx in counters:
                    counters[idx].merge_peer_snapshot(snap)
        elif mtype == "done":
            done_event.set()
            break


def _connect_streams(cfg: ThroughputClientConfig, run_id: str, ports: list):
    """Establish all N data streams.

    Each stream binds to an OS-assigned ephemeral source port (bind port 0).
    Because every stream socket stays open simultaneously and we never enable
    SO_REUSEADDR/SO_REUSEPORT, the kernel guarantees a distinct source port per
    stream. We additionally capture and verify uniqueness explicitly so a
    collision (e.g. if the OS is misconfigured) fails loudly instead of
    silently multiplexing two streams onto one 5-tuple.

    Returns (streams, source_ports) where:
      streams:      dict index -> (sock, dest_addr)
      source_ports: dict index -> local source port (int)
    """
    streams = {}
    source_ports = {}
    used_ports = set()
    for i, port in enumerate(ports):
        if cfg.transport == "tcp":
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind((cfg.bind, 0))
            stream_worker.configure_tcp_socket(sock, cfg.path_mtu)
            sock.settimeout(constants.SOCKET_TIMEOUT_SEC)
            sock.connect((cfg.server_host, port))
            sock.sendall(stream_worker.build_preamble(run_id, i))
            streams[i] = (sock, None)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind((cfg.bind, 0))
            dest = (cfg.server_host, port)
            sock.sendto(stream_worker.build_preamble(run_id, i), dest)
            streams[i] = (sock, dest)

        src_port = sock.getsockname()[1]
        if src_port in used_ports:
            raise ThroughputError(
                f"duplicate source port {src_port} assigned to stream {i}; "
                "each stream must use a unique source port")
        used_ports.add(src_port)
        source_ports[i] = src_port

    return streams, source_ports


def _should_use_multiprocess(cfg: ThroughputClientConfig) -> bool:
    """Multiprocessing requires >1 process, >1 stream, and a 'fork' start
    method (so worker processes inherit the shared-memory counters). Falls
    back to the threaded path otherwise (e.g. Windows)."""
    if cfg.procs <= 1 or cfg.streams <= 1:
        return False
    return "fork" in multiprocessing.get_all_start_methods()


# The UDP setup preamble is a single unacknowledged datagram. If it is dropped
# the server never registers the stream, times out, and the whole run fails with
# "did not receive stream_ready" -- increasingly likely as the stream count (and
# thus the number of preambles that must all survive) grows. Resend the preamble
# periodically until the start handshake completes; the server consumes the first
# copy and skips any duplicates (stream_worker.is_preamble).
_PREAMBLE_RESEND_INTERVAL_SEC = 0.1
_PREAMBLE_RESEND_MAX_SEC = 30.0


def _resend_preambles(streams: dict, run_id: str):
    for i, (sock, dest) in streams.items():
        try:
            sock.sendto(stream_worker.build_preamble(run_id, i), dest)
        except OSError:
            pass


def _make_preamble_resender(streams: dict, run_id: str):
    """Return a throttled callback that resends every stream's UDP preamble at
    most once per interval, giving up after a bounded window so a failed setup
    can't spin forever."""
    state = {"last": 0.0, "started": time.time()}

    def _tick():
        now = time.time()
        if now - state["started"] > _PREAMBLE_RESEND_MAX_SEC:
            return
        if now - state["last"] < _PREAMBLE_RESEND_INTERVAL_SEC:
            return
        state["last"] = now
        _resend_preambles(streams, run_id)

    return _tick


def _start_preamble_resender_thread(streams: dict, run_id: str) -> threading.Event:
    """Threaded-path equivalent of the resender: resend preambles in the
    background until stopped (or the bounded window elapses)."""
    stop = threading.Event()

    def _loop():
        deadline = time.time() + _PREAMBLE_RESEND_MAX_SEC
        while not stop.is_set() and time.time() < deadline:
            _resend_preambles(streams, run_id)
            stop.wait(_PREAMBLE_RESEND_INTERVAL_SEC)

    threading.Thread(target=_loop, daemon=True).start()
    return stop


def _client_worker_proc(conn: dict, run_id: str, assignments: list, role: str,
                         shared, start_flag, stop_flag, connected_q, rate_value=None):
    """Worker process: owns a subset of streams end-to-end (its own sockets),
    so per-stream UDP loss tracking stays entirely within one process.

    start_flag / stop_flag are lock-free multiprocessing.Value('b') shared with
    the parent; the worker only reads them (see run_stream_group for why we
    avoid multiprocessing.Event here)."""
    socks = {}
    src_ports = {}
    try:
        for i, port in assignments:
            if conn["transport"] == "tcp":
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind((conn["bind"], 0))
                stream_worker.configure_tcp_socket(s, conn.get("path_mtu"))
                s.settimeout(constants.SOCKET_TIMEOUT_SEC)
                s.connect((conn["server_ip"], port))
                s.sendall(stream_worker.build_preamble(run_id, i))
                socks[i] = (s, None)
            else:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.bind((conn["bind"], 0))
                dest = (conn["server_ip"], port)
                s.sendto(stream_worker.build_preamble(run_id, i), dest)
                socks[i] = (s, dest)
            src_ports[i] = s.getsockname()[1]
    except OSError as exc:
        connected_q.put(("error", str(exc)))
        return

    connected_q.put(("ok", src_ports))

    # Keep the preamble alive across setup-time UDP loss until the parent flips
    # start_flag (i.e. every stream has been acknowledged and the run begins).
    on_wait_tick = _make_preamble_resender(socks, run_id) if conn["transport"] == "udp" else None

    views = {i: shared.view(i) for i, _port in assignments}
    stream_worker.run_stream_group(socks, role, conn["transport"], views,
                                    conn["duration_sec"],
                                    wait_start=lambda: bool(start_flag.value),
                                    stopped=lambda: bool(stop_flag.value),
                                    rate_value=rate_value,
                                    on_wait_tick=on_wait_tick,
                                    path_mtu=conn.get("path_mtu"))

    for s, _dest in socks.values():
        try:
            s.close()
        except OSError:
            pass


def _start_client_workers(cfg: ThroughputClientConfig, server_ip: str, run_id: str,
                           ports: list, mp_ctx, shared):
    """Spawn worker processes and wait for every stream to connect. Returns a
    state dict; source ports are collected back from the workers."""
    n = len(ports)
    nproc = min(cfg.procs, n)
    groups = [[] for _ in range(nproc)]
    for idx, port in enumerate(ports):
        groups[idx % nproc].append((idx, port))
    active_groups = [g for g in groups if g]

    role = "send" if cfg.direction == "up" else "recv"
    conn = {"server_ip": server_ip, "bind": cfg.bind, "transport": cfg.transport,
            "duration_sec": cfg.sender_duration_sec, "path_mtu": cfg.path_mtu}
    start_flag = mp_ctx.Value("b", 0, lock=False)
    stop_flag = mp_ctx.Value("b", 0, lock=False)
    # Per-stream target send rate in bytes/sec (0 = unpaced); driven by the UDP
    # tuning ramp. Shared lock-free so the controller can update it live.
    rate_value = mp_ctx.Value("d", 0.0, lock=False)
    connected_q = mp_ctx.Queue()

    procs = []
    for g in active_groups:
        p = mp_ctx.Process(target=_client_worker_proc,
                           args=(conn, run_id, g, role, shared, start_flag, stop_flag,
                                 connected_q, rate_value),
                           daemon=True)
        p.start()
        procs.append(p)

    source_ports = {}
    for _ in range(len(active_groups)):
        status, payload = connected_q.get(timeout=60)
        if status != "ok":
            for p in procs:
                p.terminate()
            raise ThroughputError(f"stream worker failed to connect: {payload}")
        source_ports.update(payload)

    return {"procs": procs, "start_flag": start_flag, "stop_flag": stop_flag,
            "rate_value": rate_value, "source_ports": source_ports,
            "nproc": len(active_groups)}


def _run_udp_tuning(cfg, counters, recorder, rtt_tracker, apply_rate, on_interval,
                    stop_requested, tune_start, text_log):
    """AIMD (iperf-style) ramp: find the highest UDP send rate whose measured
    loss stays at or below cfg.acceptable_loss_pct, within the max-duration
    budget. Rates are per-stream bytes/sec; apply_rate() pushes the new rate to
    the senders (locally for upload, via a control message for download).

    Returns a ``tuning`` dict (probe + per-trial series + best result)."""
    target = cfg.acceptable_loss_pct
    deadline = tune_start + cfg.max_duration_sec
    streams = max(1, cfg.streams)
    state = {"target_aggregate_mbps": 0.0}

    def _sample_for(seconds):
        end = time.time() + seconds
        while time.time() < end and not stop_requested.is_set():
            time.sleep(constants.SAMPLE_INTERVAL_SEC)
            totals = _aggregate_totals(counters)
            loaded = rtt_tracker.loaded_stats()
            entry = recorder.record(time.time(), totals, rtt_ms=loaded.get("avg"), phase="tuning")
            entry["target_mbps"] = state["target_aggregate_mbps"]
            if on_interval:
                on_interval(entry)

    def _measure(rate_ps_bps):
        """Set a per-stream rate (bytes/sec; 0 = unpaced), let it settle, then
        measure offered rate / goodput / loss over one trial window."""
        apply_rate(rate_ps_bps)
        state["target_aggregate_mbps"] = (rate_ps_bps * streams * 8 / 1e6) if rate_ps_bps else 0.0
        _sample_for(constants.UDP_TUNE_SETTLE_SEC)
        a = _aggregate_totals(counters)
        ta = time.time()
        _sample_for(constants.UDP_TUNE_TRIAL_SEC)
        b = _aggregate_totals(counters)
        dt = max(1e-6, time.time() - ta)

        d_sent_pkts = max(0, b["packets_sent"] - a["packets_sent"])
        d_recv_pkts = max(0, b["packets_received"] - a["packets_received"])
        d_lost = max(0, b["packets_lost"] - a["packets_lost"])
        d_sent_bytes = max(0, b["bytes_sent"] - a["bytes_sent"])
        d_recv_bytes = max(0, b["bytes_received"] - a["bytes_received"])
        expected = d_recv_pkts + d_lost
        loss_pct = (d_lost / expected * 100.0) if expected > 0 else 0.0
        return {
            "target_mbps": state["target_aggregate_mbps"],
            "offered_mbps": d_sent_bytes * 8 / dt / 1e6,
            "goodput_mbps": d_recv_bytes * 8 / dt / 1e6,
            "offered_pps": d_sent_pkts / dt,
            "goodput_pps": d_recv_pkts / dt,
            "loss_pct": loss_pct,
        }

    def _ps_for_aggregate(mbps):
        """Per-stream bytes/sec needed to offer the given aggregate Mbps."""
        return (mbps * 1e6 / 8.0) / streams

    trials = []
    # Phase 1: a PACED probe at the configured start rate (default 250 Mbps
    # aggregate) instead of an unpaced line-rate burst. The old unpaced probe
    # could momentarily send multi-Gbps, which looks like a UDP flood/DDoS and
    # gets dropped or rate-limited by network security devices.
    start_ps = _ps_for_aggregate(constants.UDP_TUNE_START_MBPS)
    probe = _measure(start_ps)
    probe["phase"] = "probe"
    text_log.append(f"[tune] probe (paced {constants.UDP_TUNE_START_MBPS:.0f} Mbps): "
                    f"offered={probe['offered_mbps']:.0f} Mbps "
                    f"goodput={probe['goodput_mbps']:.0f} Mbps loss={probe['loss_pct']:.2f}%")

    step_ps = _ps_for_aggregate(constants.UDP_TUNE_STEP_MBPS)
    min_step = _ps_for_aggregate(constants.UDP_TUNE_MIN_STEP_MBPS)

    def _goodput(m):
        return float(m.get("goodput_mbps") or 0.0)

    # best        -> highest-goodput trial that also met the loss target.
    # best_effort -> highest-goodput trial overall (used for reporting when
    #                nothing met the target, so we never report 0 while data
    #                was actually flowing).
    best = None
    best_effort = None

    def _record(m):
        nonlocal best, best_effort
        if best_effort is None or _goodput(m) > _goodput(best_effort):
            best_effort = dict(m)
        if m["loss_pct"] <= target and (best is None or _goodput(m) > _goodput(best)):
            best = dict(m)

    _record(probe)
    # Best goodput accepted so far (loss under target); drives plateau detection.
    accepted_goodput = _goodput(probe) if probe["loss_pct"] <= target else None

    if probe["loss_pct"] <= target:
        # Start rate is within the loss target -- keep it and try to climb.
        rate_ps = start_ps + step_ps
    else:
        # Start rate already breaches the loss target -- back off below it.
        rate_ps = max(min_step, start_ps * constants.UDP_TUNE_BACKOFF_FACTOR)
        step_ps *= 0.5

    # Phase 2: AIMD around the start rate. A trial is a "breach" when it either
    # loses more than the target OR fails to raise goodput meaningfully (the path
    # is saturated and the extra offered load is only filling a buffer -- loss
    # stays ~0 but latency balloons). Both cases back off + refine the step.
    while (not stop_requested.is_set()
           and time.time() + constants.UDP_TUNE_SETTLE_SEC + constants.UDP_TUNE_TRIAL_SEC <= deadline
           and step_ps >= min_step and rate_ps > 0):
        trial = _measure(rate_ps)
        trials.append(trial)
        gp = _goodput(trial)
        plateaued = (trial["loss_pct"] <= target
                     and accepted_goodput is not None
                     and gp <= accepted_goodput * (1.0 + constants.UDP_TUNE_MIN_GOODPUT_GAIN))
        breach = trial["loss_pct"] > target or plateaued
        text_log.append(f"[tune] target={trial['target_mbps']:.0f} Mbps -> "
                        f"goodput={gp:.0f} Mbps loss={trial['loss_pct']:.2f}%"
                        + ("  (saturated)" if plateaued else ""))
        _record(trial)
        if breach:
            rate_ps = max(step_ps, rate_ps * constants.UDP_TUNE_BACKOFF_FACTOR)  # mult. decrease
            step_ps *= 0.5                                        # refine granularity
        else:
            accepted_goodput = max(accepted_goodput or 0.0, gp)
            rate_ps += step_ps                                    # additive increase

    # Leave the senders at the best sustainable paced rate (or unpaced only if
    # nothing was measurable).
    if best is not None and best.get("target_mbps"):
        apply_rate(best["target_mbps"] * 1e6 / 8 / streams)
    else:
        apply_rate(0.0)

    if best is not None:
        text_log.append(f"[tune] best: target={best.get('target_mbps', 0):.0f} Mbps "
                        f"goodput={best.get('goodput_mbps', 0):.0f} Mbps loss={best.get('loss_pct', 0):.2f}% "
                        f"(<= {target:.2f}% target)")
    else:
        text_log.append(f"[tune] no rate met the {target:.2f}% loss target within the budget"
                        + (f"; best effort goodput={best_effort.get('goodput_mbps', 0):.0f} Mbps "
                           f"loss={best_effort.get('loss_pct', 0):.2f}%" if best_effort else ""))

    return {
        "acceptable_loss_pct": target,
        "max_duration_sec": cfg.max_duration_sec,
        "probe": probe,
        "trials": trials,
        "best": best,
        "best_effort": best_effort,
    }


def run_throughput_test(cfg: ThroughputClientConfig, on_interval=None, stop_requested: threading.Event = None,
                         log_base_dir: str = None) -> dict:
    server_ip = addr.resolve_host(cfg.server_host)
    stop_requested = stop_requested or threading.Event()

    if cfg.path_mtu is None:
        cfg.path_mtu, cfg.pmtud_info = _discover_path_mtu(cfg, server_ip, log_base_dir)
    elif cfg.pmtud_info is None:
        cfg.pmtud_info = {
            "server_ip": server_ip,
            "udp_port": (cfg.start_port if cfg.transport == "udp" else constants.DEFAULT_UDP_PORT),
            "path_mtu": cfg.path_mtu,
            "path_mtu_source": "provided",
            "fallback_used": False,
        }

    control_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    control_sock.settimeout(constants.SOCKET_TIMEOUT_SEC)
    control_sock.connect((server_ip, cfg.control_port))
    control = ControlChannel(control_sock)

    run_id = str(uuid.uuid4())
    params = {
        "transport": cfg.transport,
        "direction": cfg.direction,
        "streams": cfg.streams,
        # Senders (client for upload, server for download) must stay alive for
        # the whole budget when tuning; the controller stops them early.
        "duration_sec": cfg.sender_duration_sec,
        "procs": cfg.procs,
        "path_mtu": cfg.path_mtu,
    }

    _dur_desc = (f"max_duration={cfg.max_duration_sec}s acceptable_loss={cfg.acceptable_loss_pct}%"
                 if cfg.tune_udp else f"duration={cfg.duration_sec}s")
    text_log = [f"NetworkBenchmark throughput test  run_id={run_id}",
                f"server={server_ip}:{cfg.control_port}  transport={cfg.transport} "
                f"direction={cfg.direction} streams={cfg.streams} {_dur_desc}"]
    if cfg.path_mtu is not None:
        text_log.append(f"Path MTU: {cfg.path_mtu} bytes  TCP MSS: "
                        f"{stream_worker.tcp_mss_for_path_mtu(cfg.path_mtu)} bytes  "
                        f"UDP datagram: {stream_worker.udp_packet_size_for_path_mtu(cfg.path_mtu)} bytes")
    if cfg.pmtud_info:
        text_log.append(f"PMTUD source: {cfg.pmtud_info.get('path_mtu_source', 'pmtud')}  "
                        f"log: {cfg.pmtud_info.get('log_json', 'n/a')}")

    try:
        control.send("hello", run_id=run_id, params=params)
        msg = control.recv(timeout=30)
        if not msg or msg.get("type") != "hello_ack" or not msg.get("ok"):
            raise ThroughputError(f"server rejected hello: {msg}")
        ports = msg["ports"]
        if ports and ports[0] != cfg.start_port:
            text_log.append(f"WARNING: server is using base port {ports[0]}, "
                             f"client expected start_port {cfg.start_port}")

        use_mp = _should_use_multiprocess(cfg)
        streams = None
        mp_state = None
        if use_mp:
            mp_ctx = multiprocessing.get_context("fork")
            shared = shared_counters.SharedCounters(cfg.streams, ctx=mp_ctx)
            counters = {i: shared.view(i) for i in range(cfg.streams)}
            mp_state = _start_client_workers(cfg, server_ip, run_id, ports, mp_ctx, shared)
            source_ports = mp_state["source_ports"]
            text_log.append(f"Execution: multiprocessing ({mp_state['nproc']} worker "
                            f"process(es) for {cfg.streams} streams)")
        else:
            streams, source_ports = _connect_streams(cfg, run_id, ports)
            counters = {i: metrics.StreamCounters(i) for i in range(cfg.streams)}
            text_log.append("Execution: single process (threaded)")
        text_log.append("Per-stream 5-tuples (unique source port each): " + ", ".join(
            f"stream {i}: src :{source_ports[i]} -> {server_ip}:{ports[i]}/{cfg.transport}"
            for i in sorted(source_ports)))

        # In the threaded path there is no worker wait-loop to piggyback the
        # preamble retransmit on, so drive it from a background thread until every
        # stream is acknowledged.
        preamble_resend_stop = None
        if cfg.transport == "udp" and not use_mp:
            preamble_resend_stop = _start_preamble_resender_thread(streams, run_id)
        try:
            for _ in range(cfg.streams):
                ready_msg = control.recv(timeout=30)
                if not ready_msg or ready_msg.get("type") != "stream_ready":
                    raise ThroughputError(f"did not receive stream_ready: {ready_msg}")
        finally:
            if preamble_resend_stop is not None:
                preamble_resend_stop.set()

        control.send("start")
        ack = control.recv(timeout=30)
        if not ack or ack.get("type") != "start_ack":
            raise ThroughputError(f"did not receive start_ack: {ack}")

        phase = run_mode.RunPhase()
        rtt_tracker = metrics.RttTracker()
        done_event = threading.Event()

        dispatch_thread = threading.Thread(target=_client_dispatch_loop,
                                            args=(control, phase, rtt_tracker, counters, done_event),
                                            daemon=True)
        dispatch_thread.start()

        prober_stop = threading.Event()
        prober_thread = threading.Thread(target=latency_probe.prober_loop,
                                          args=(control, prober_stop, cfg.probe_interval_sec), daemon=True)
        prober_thread.start()

        # calibration phase: latency probing only, no data flow
        calib_deadline = time.time() + cfg.calibration_sec
        while time.time() < calib_deadline and not stop_requested.is_set():
            time.sleep(0.05)

        phase.start_running()
        control.send("start_data")

        stream_stop_events = {}
        stream_threads = []
        start_time = time.time()

        # Per-stream target send rate (bytes/sec, 0 = unpaced). For upload this
        # drives the local senders; the tuning controller updates it live.
        if use_mp:
            client_rate_value = mp_state["rate_value"]
        else:
            client_rate_value = stream_worker.RateHolder(0.0)

        if use_mp:
            mp_state["start_flag"].value = 1
        elif cfg.direction == "up":
            for i, (sock, dest) in streams.items():
                stop_ev = threading.Event()
                stream_stop_events[i] = stop_ev
                if cfg.transport == "tcp":
                    th = threading.Thread(target=stream_worker.tcp_sender_loop,
                                           args=(sock, counters[i], stop_ev, cfg.sender_duration_sec, start_time,
                                                 cfg.path_mtu),
                                           daemon=True)
                else:
                    th = threading.Thread(target=stream_worker.udp_sender_loop,
                                           args=(sock, dest, i, counters[i], stop_ev,
                                                 cfg.sender_duration_sec, start_time),
                                           kwargs={"rate_value": client_rate_value, "path_mtu": cfg.path_mtu}, daemon=True)
                th.start()
                stream_threads.append(th)
        else:
            for i, (sock, dest) in streams.items():
                stop_ev = threading.Event()
                stream_stop_events[i] = stop_ev
                if cfg.transport == "tcp":
                    th = threading.Thread(target=stream_worker.tcp_receiver_loop,
                                           args=(sock, counters[i], stop_ev), daemon=True)
                else:
                    th = threading.Thread(target=stream_worker.udp_receiver_loop,
                                           args=(sock, counters[i], stop_ev),
                                           kwargs={"known_peer_addr": dest}, daemon=True)
                th.start()
                stream_threads.append(th)

        recorder = metrics.IntervalRecorder(transport=cfg.transport)
        tuning = None

        if cfg.tune_udp:
            # apply_rate pushes a new per-stream rate (bytes/sec) to whichever
            # side is sending: locally for upload, or to the server (which
            # mirrors it onto its senders) for download.
            if cfg.direction == "up":
                def apply_rate(ps_bps):
                    client_rate_value.value = ps_bps
            else:
                def apply_rate(ps_bps):
                    try:
                        control.send("set_rate", per_stream_bps=ps_bps)
                    except OSError:
                        pass
            tuning = _run_udp_tuning(cfg, counters, recorder, rtt_tracker, apply_rate,
                                     on_interval, stop_requested, start_time, text_log)
        else:
            run_deadline = start_time + cfg.duration_sec
            while time.time() < run_deadline and not stop_requested.is_set():
                time.sleep(constants.SAMPLE_INTERVAL_SEC)
                totals = _aggregate_totals(counters)
                loaded = rtt_tracker.loaded_stats()
                entry = recorder.record(time.time(), totals, rtt_ms=loaded.get("avg"), phase="running")
                if on_interval:
                    on_interval(entry)

        control.send("stop")
        if use_mp:
            mp_state["stop_flag"].value = 1
            for p in mp_state["procs"]:
                p.join(timeout=10)
        else:
            for ev in stream_stop_events.values():
                ev.set()
            # capture final local TCP retransmit counts before closing sockets
            if cfg.transport == "tcp":
                for i, (sock, _dest) in streams.items():
                    retrans = tcp_info.get_tcp_retransmits(sock)
                    if retrans is not None:
                        counters[i].set_retransmits(retrans)

        done_event.wait(timeout=5)
        prober_stop.set()
        phase.stop()
        dispatch_thread.join(timeout=2)

        if not use_mp:
            for sock, _dest in streams.values():
                try:
                    sock.close()
                except OSError:
                    pass

        summary = _build_summary(cfg, run_id, counters, rtt_tracker, recorder, text_log,
                                 source_ports, tuning=tuning)
        json_path, log_path = logging_store.save_result(
            constants.TEST_TYPE_THROUGHPUT, summary, text_log, base_dir=log_base_dir)
        summary["log_json"] = json_path
        summary["log_text"] = log_path
        return summary

    finally:
        control.close()


def _aggregate_totals(counters: dict) -> dict:
    totals = {"bytes_sent": 0, "packets_sent": 0, "bytes_received": 0,
              "packets_received": 0, "packets_lost": 0, "retransmits": 0}
    for c in counters.values():
        snap = c.snapshot()
        for key in totals:
            totals[key] += snap.get(key, 0)
    return totals


def _build_summary(cfg, run_id, counters, rtt_tracker, recorder, text_log, source_ports=None,
                   tuning=None):
    source_ports = source_ports or {}
    per_stream = []
    for c in counters.values():
        snap = c.snapshot()
        idx = snap.get("index")
        snap["source_port"] = source_ports.get(idx)
        per_stream.append(snap)
    totals = _aggregate_totals(counters)

    duration = cfg.duration_sec
    send_bps = (totals["bytes_sent"] / duration) if duration else 0
    recv_bps = (totals["bytes_received"] / duration) if duration else 0
    send_mbps = send_bps * 8 / 1e6
    recv_mbps = recv_bps * 8 / 1e6
    send_pps = metrics.estimate_pps(cfg.transport, send_bps,
                                    (totals["packets_sent"] / duration) if duration else 0)
    recv_pps = metrics.estimate_pps(cfg.transport, recv_bps,
                                    (totals["packets_received"] / duration) if duration else 0)

    loss_pct = None
    if cfg.transport == "udp":
        expected = totals["packets_received"] + totals["packets_lost"]
        loss_pct = (totals["packets_lost"] / expected * 100.0) if expected else 0.0

    # For a tuning run the cumulative totals span every probed rate, so they are
    # not a meaningful "throughput". Report the best sustainable trial. When no
    # trial met the loss target, fall back to the best-effort trial (highest
    # goodput measured) so we still report the real throughput/loss instead of
    # zeros while data was clearly flowing.
    if tuning is not None:
        best = tuning.get("best") or tuning.get("best_effort")
        if best is not None:
            send_mbps = best.get("offered_mbps", 0.0)
            recv_mbps = best.get("goodput_mbps", 0.0)
            send_pps = best.get("offered_pps", 0.0)
            recv_pps = best.get("goodput_pps", 0.0)
            loss_pct = best.get("loss_pct", loss_pct)
            receiver_bps_override = recv_mbps * 1e6 / 8
        else:
            send_mbps = recv_mbps = send_pps = recv_pps = 0.0
            receiver_bps_override = 0.0
    else:
        receiver_bps_override = None

    unloaded_rtt_ms = rtt_tracker.unloaded_rtt_ms()
    loaded_stats = rtt_tracker.loaded_stats()

    receiver_bps = (receiver_bps_override if receiver_bps_override is not None
                    else ((totals["bytes_received"] / duration) if duration else 0))
    bloat = metrics.compute_bufferbloat(receiver_bps, unloaded_rtt_ms, loaded_stats.get("avg"))

    summary = {
        "run_id": run_id,
        "server": cfg.server_host,
        "transport": cfg.transport,
        "direction": cfg.direction,
        "streams": cfg.streams,
        "procs": cfg.procs,
        "duration_sec": cfg.duration_sec,
        "path_mtu": cfg.path_mtu,
        "pmtud": cfg.pmtud_info,
        "source_ports": source_ports,
        "totals": totals,
        "send_mbps": send_mbps,
        "recv_mbps": recv_mbps,
        "send_pps": send_pps,
        "recv_pps": recv_pps,
        "loss_pct": loss_pct,
        "retransmits": totals["retransmits"] if cfg.transport == "tcp" else None,
        "unloaded_rtt_ms": unloaded_rtt_ms,
        "loaded_rtt": loaded_stats,
        "bufferbloat": bloat,
        "per_stream": per_stream,
        "interval_series": recorder.records,
        "tuning": tuning,
    }

    text_log.append("")
    text_log.append(f"Path MTU: {cfg.path_mtu} bytes")
    text_log.append(f"PMTUD source: {(cfg.pmtud_info or {}).get('path_mtu_source', 'unknown')}")
    text_log.append(f"Aggregate throughput: send={send_mbps:.2f} Mbps  recv={recv_mbps:.2f} Mbps")
    text_log.append(f"Packets/sec: send={send_pps:.1f} pps  recv={recv_pps:.1f} pps")
    if loss_pct is not None:
        text_log.append(f"UDP packet loss: {loss_pct:.3f}%")
    if summary["retransmits"] is not None:
        text_log.append(f"TCP retransmits: {summary['retransmits']}")
    text_log.append(f"Unloaded RTT: {unloaded_rtt_ms} ms")
    text_log.append(f"Loaded RTT: avg={loaded_stats.get('avg')} best={loaded_stats.get('best')} "
                     f"worst={loaded_stats.get('worst')} stddev={loaded_stats.get('stddev')} "
                     f"jitter={loaded_stats.get('jitter')}")
    text_log.append(f"Bufferbloat: bdp_bytes={bloat.get('bdp_bytes')} "
                     f"buffered_bytes={bloat.get('buffered_bytes')} bloat_factor={bloat.get('bloat_factor')}")

    return summary
