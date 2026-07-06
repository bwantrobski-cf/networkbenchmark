"""Client-side throughput test orchestration.

Merges iperf3-style parallel streams (-P) and upload/download (-R) with
bbperf-style calibration -> running lifecycle and latency-under-load /
bufferbloat measurement (see transport.metrics.compute_bufferbloat).
"""

import socket
import threading
import time
import uuid

from ..common import addr, constants, logging_store, tcp_info
from ..transport import stream_worker, metrics, latency_probe, run_mode
from ..transport.control_connection import ControlChannel


class ThroughputClientConfig:
    def __init__(self, server_ip, control_port=None, transport="tcp", direction="up",
                 streams=1, duration_sec=constants.DEFAULT_TEST_DURATION_SEC,
                 start_port=None, bind="0.0.0.0", calibration_sec=constants.CALIBRATION_TIME_SEC,
                 probe_interval_sec=constants.PROBE_INTERVAL_SEC):
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


class ThroughputError(Exception):
    pass


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


def run_throughput_test(cfg: ThroughputClientConfig, on_interval=None, stop_requested: threading.Event = None,
                         log_base_dir: str = None) -> dict:
    server_ip = addr.resolve_host(cfg.server_host)
    stop_requested = stop_requested or threading.Event()

    control_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    control_sock.settimeout(constants.SOCKET_TIMEOUT_SEC)
    control_sock.connect((server_ip, cfg.control_port))
    control = ControlChannel(control_sock)

    run_id = str(uuid.uuid4())
    params = {
        "transport": cfg.transport,
        "direction": cfg.direction,
        "streams": cfg.streams,
        "duration_sec": cfg.duration_sec,
    }

    text_log = [f"NetworkBenchmark throughput test  run_id={run_id}",
                f"server={server_ip}:{cfg.control_port}  transport={cfg.transport} "
                f"direction={cfg.direction} streams={cfg.streams} duration={cfg.duration_sec}s"]

    try:
        control.send("hello", run_id=run_id, params=params)
        msg = control.recv(timeout=30)
        if not msg or msg.get("type") != "hello_ack" or not msg.get("ok"):
            raise ThroughputError(f"server rejected hello: {msg}")
        ports = msg["ports"]
        if ports and ports[0] != cfg.start_port:
            text_log.append(f"WARNING: server is using base port {ports[0]}, "
                             f"client expected start_port {cfg.start_port}")

        streams, source_ports = _connect_streams(cfg, run_id, ports)
        text_log.append("Per-stream 5-tuples (unique source port each): " + ", ".join(
            f"stream {i}: src :{source_ports[i]} -> {server_ip}:{ports[i]}/{cfg.transport}"
            for i in sorted(source_ports)))

        for _ in range(cfg.streams):
            ready_msg = control.recv(timeout=30)
            if not ready_msg or ready_msg.get("type") != "stream_ready":
                raise ThroughputError(f"did not receive stream_ready: {ready_msg}")

        control.send("start")
        ack = control.recv(timeout=30)
        if not ack or ack.get("type") != "start_ack":
            raise ThroughputError(f"did not receive start_ack: {ack}")

        phase = run_mode.RunPhase()
        rtt_tracker = metrics.RttTracker()
        counters = {i: metrics.StreamCounters(i) for i in range(cfg.streams)}
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

        if cfg.direction == "up":
            for i, (sock, dest) in streams.items():
                stop_ev = threading.Event()
                stream_stop_events[i] = stop_ev
                if cfg.transport == "tcp":
                    th = threading.Thread(target=stream_worker.tcp_sender_loop,
                                           args=(sock, counters[i], stop_ev, cfg.duration_sec, start_time),
                                           daemon=True)
                else:
                    th = threading.Thread(target=stream_worker.udp_sender_loop,
                                           args=(sock, dest, i, counters[i], stop_ev, cfg.duration_sec, start_time),
                                           daemon=True)
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

        recorder = metrics.IntervalRecorder()
        run_deadline = start_time + cfg.duration_sec

        while time.time() < run_deadline and not stop_requested.is_set():
            time.sleep(constants.SAMPLE_INTERVAL_SEC)
            totals = _aggregate_totals(counters)
            loaded = rtt_tracker.loaded_stats()
            entry = recorder.record(time.time(), totals, rtt_ms=loaded.get("avg"), phase="running")
            if on_interval:
                on_interval(entry)

        control.send("stop")
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

        for sock, _dest in streams.values():
            try:
                sock.close()
            except OSError:
                pass

        summary = _build_summary(cfg, run_id, counters, rtt_tracker, recorder, text_log, source_ports)
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


def _build_summary(cfg, run_id, counters, rtt_tracker, recorder, text_log, source_ports=None):
    source_ports = source_ports or {}
    per_stream = []
    for c in counters.values():
        snap = c.snapshot()
        idx = snap.get("index")
        snap["source_port"] = source_ports.get(idx)
        per_stream.append(snap)
    totals = _aggregate_totals(counters)

    duration = cfg.duration_sec
    send_mbps = (totals["bytes_sent"] * 8 / duration) / 1e6 if duration else 0
    recv_mbps = (totals["bytes_received"] * 8 / duration) / 1e6 if duration else 0

    loss_pct = None
    if cfg.transport == "udp":
        expected = totals["packets_received"] + totals["packets_lost"]
        loss_pct = (totals["packets_lost"] / expected * 100.0) if expected else 0.0

    unloaded_rtt_ms = rtt_tracker.unloaded_rtt_ms()
    loaded_stats = rtt_tracker.loaded_stats()

    receiver_bps = (totals["bytes_received"] / duration) if duration else 0
    bloat = metrics.compute_bufferbloat(receiver_bps, unloaded_rtt_ms, loaded_stats.get("avg"))

    summary = {
        "run_id": run_id,
        "server": cfg.server_host,
        "transport": cfg.transport,
        "direction": cfg.direction,
        "streams": cfg.streams,
        "duration_sec": cfg.duration_sec,
        "source_ports": source_ports,
        "totals": totals,
        "send_mbps": send_mbps,
        "recv_mbps": recv_mbps,
        "loss_pct": loss_pct,
        "retransmits": totals["retransmits"] if cfg.transport == "tcp" else None,
        "unloaded_rtt_ms": unloaded_rtt_ms,
        "loaded_rtt": loaded_stats,
        "bufferbloat": bloat,
        "per_stream": per_stream,
        "interval_series": recorder.records,
    }

    text_log.append("")
    text_log.append(f"Aggregate throughput: send={send_mbps:.2f} Mbps  recv={recv_mbps:.2f} Mbps")
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
