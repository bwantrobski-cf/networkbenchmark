"""Headless CLI for NetworkBenchmark.

Subcommands:
    server        run as a server (throughput + diagnostics responders)
    throughput    run a client throughput test (iperf3+bbperf style)
    mtr           run an MTR (ICMP/UDP/TCP) diagnostic
    connectivity  quick ICMP/UDP/TCP connectivity check against a server
    pmtud         Path MTU Discovery test against a server
    protocols     UDP/TCP/ICMP/ESP/ESP-UDP/GRE protocol reachability matrix
    webui         launch the Streamlit web UI
"""

import argparse
import signal
import sys
import threading

from .common import constants, privileges, logging_store
from .client.engine import ThroughputClientConfig, run_throughput_test
from .diagnostics.mtr import MtrRunner, MtrError
from .diagnostics import connectivity, protocols
from .diagnostics.pmtud import run_pmtud, PmtudError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=constants.APP_NAME,
        description="NetworkBenchmark: throughput + diagnostics tool (client/server, CLI/Web UI)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_server = sub.add_parser("server", help="run as a server")
    p_server.add_argument("--bind", default="0.0.0.0", help="address to bind (default: 0.0.0.0)")
    p_server.add_argument("--control-port", type=int, default=constants.DEFAULT_CONTROL_PORT)
    p_server.add_argument("--udp-port", type=int, default=constants.DEFAULT_UDP_PORT,
                           help="base UDP port for connectivity checks + throughput streams (default: 10000)")
    p_server.add_argument("--tcp-port", type=int, default=constants.DEFAULT_TCP_PORT,
                           help="base TCP port for connectivity checks + throughput streams (default: 11000)")
    p_server.add_argument("--log-dir", default=None)
    p_server.set_defaults(func=cmd_server)

    p_tp = sub.add_parser("throughput", help="run a client throughput test")
    p_tp.add_argument("--server-ip", required=True, help="server hostname or IP")
    p_tp.add_argument("--control-port", type=int, default=constants.DEFAULT_CONTROL_PORT)
    proto_group = p_tp.add_mutually_exclusive_group()
    proto_group.add_argument("--tcp", action="store_const", dest="transport", const="tcp")
    proto_group.add_argument("--udp", action="store_const", dest="transport", const="udp")
    p_tp.set_defaults(transport="tcp")
    p_tp.add_argument("-R", "--reverse", action="store_true", default=False,
                       help="download direction (server to client); default is upload")
    p_tp.add_argument("-P", "--parallel", type=int, default=constants.DEFAULT_STREAMS,
                       help="number of parallel streams (default: 1)")
    p_tp.add_argument("-t", "--time", type=float, default=constants.DEFAULT_TEST_DURATION_SEC,
                       help="test duration in seconds (default: 10)")
    p_tp.add_argument("--start-port", type=int, default=None,
                       help="base port the server is using for this transport "
                            "(default: 10000 for UDP, 11000 for TCP)")
    p_tp.add_argument("--bind", default="0.0.0.0", help="local address to bind data sockets")
    p_tp.add_argument("--procs", type=int, default=1,
                       help="spread streams across N worker processes to use multiple CPU cores "
                            "(default: 1 = single-process/threaded). Requires >1 stream.")
    p_tp.add_argument("--acceptable-loss", type=float, default=None, metavar="PCT",
                       help="UDP only: tune the send rate (AIMD ramp) to find the max rate whose "
                            "loss stays <= PCT%%. In this mode --time becomes a max-duration budget.")
    p_tp.add_argument("--max-duration", type=float, default=None, metavar="SEC",
                       help="UDP tuning: max time budget for the rate search "
                            f"(default: {constants.DEFAULT_MAX_DURATION_SEC}s; overrides --time as the budget).")
    p_tp.add_argument("--log-dir", default=None)
    p_tp.set_defaults(func=cmd_throughput)

    p_mtr = sub.add_parser("mtr", help="MTR diagnostic to any IP/FQDN")
    p_mtr.add_argument("target", help="target IP address or FQDN")
    p_mtr.add_argument("--protocol", choices=constants.MTR_PROTOCOLS, default="icmp")
    p_mtr.add_argument("--port", type=int, default=33434, help="port for TCP protocol mode")
    p_mtr.add_argument("--interval-ms", type=int, default=constants.MTR_DEFAULT_INTERVAL_MS,
                        help="ping interval in ms, 100-1000 (default: 200)")
    p_mtr.add_argument("--max-hops", type=int, default=constants.MTR_DEFAULT_MAX_HOPS)
    p_mtr.add_argument("--timeout", type=float, default=constants.MTR_DEFAULT_TIMEOUT_SEC)
    p_mtr.add_argument("--count", type=int, default=None, help="number of rounds (default: run until Ctrl+C)")
    p_mtr.add_argument("--log-dir", default=None)
    p_mtr.set_defaults(func=cmd_mtr)

    p_conn = sub.add_parser("connectivity", help="ICMP/UDP/TCP connectivity check")
    p_conn.add_argument("--server-ip", required=True)
    p_conn.add_argument("--udp-port", type=int, default=constants.DEFAULT_UDP_PORT)
    p_conn.add_argument("--tcp-port", type=int, default=constants.DEFAULT_TCP_PORT)
    p_conn.add_argument("--timeout", type=float, default=2.0)
    p_conn.add_argument("--log-dir", default=None)
    p_conn.set_defaults(func=cmd_connectivity)

    p_pmtud = sub.add_parser("pmtud", help="Path MTU Discovery test against the server")
    p_pmtud.add_argument("--server-ip", required=True)
    p_pmtud.add_argument("--udp-port", type=int, default=constants.DEFAULT_UDP_PORT)
    p_pmtud.add_argument("--min-size", type=int, default=68)
    p_pmtud.add_argument("--max-size", type=int, default=9000)
    p_pmtud.add_argument("--timeout", type=float, default=1.5)
    p_pmtud.add_argument("--log-dir", default=None)
    p_pmtud.set_defaults(func=cmd_pmtud)

    p_proto = sub.add_parser("protocols", help="UDP/TCP/ICMP/ESP/ESP-UDP/GRE reachability matrix")
    p_proto.add_argument("--server-ip", required=True)
    p_proto.add_argument("--udp-port", type=int, default=constants.DEFAULT_UDP_PORT)
    p_proto.add_argument("--tcp-port", type=int, default=constants.DEFAULT_TCP_PORT)
    p_proto.add_argument("--timeout", type=float, default=2.0)
    p_proto.add_argument("--log-dir", default=None)
    p_proto.set_defaults(func=cmd_protocols)

    p_webui = sub.add_parser("webui", help="launch the Streamlit web UI")
    p_webui.add_argument("--port", type=int, default=None, help="streamlit server port")
    p_webui.set_defaults(func=cmd_webui)

    return parser


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def cmd_server(args):
    from .server.engine import ServerConfig, run_server
    config = ServerConfig(bind=args.bind, control_port=args.control_port,
                           udp_port=args.udp_port, tcp_port=args.tcp_port, log_dir=args.log_dir)
    print(f"NetworkBenchmark server starting on {args.bind} "
          f"(control={args.control_port} udp={args.udp_port} tcp={args.tcp_port})", flush=True)
    run_server(config, block=True)


def cmd_throughput(args):
    if args.acceptable_loss is not None and args.transport != "udp":
        print("error: --acceptable-loss is only valid for UDP (--udp)", flush=True)
        return
    max_dur = args.max_duration if args.max_duration is not None else args.time

    cfg = ThroughputClientConfig(
        server_ip=args.server_ip,
        control_port=args.control_port,
        transport=args.transport,
        direction="down" if args.reverse else "up",
        streams=args.parallel,
        duration_sec=args.time,
        start_port=args.start_port,
        bind=args.bind,
        procs=args.procs,
        acceptable_loss_pct=args.acceptable_loss,
        max_duration_sec=max_dur,
    )

    stop_requested = threading.Event()

    def handle_sigint(_signum, _frame):
        print("\nstopping test...", flush=True)
        stop_requested.set()

    signal.signal(signal.SIGINT, handle_sigint)

    def on_interval(entry):
        if "send_mbps" not in entry:
            return
        rtt = entry.get("rtt_ms")
        rtt_str = f"{rtt:.2f}ms" if rtt is not None else "n/a"
        tgt = entry.get("target_mbps")
        tgt_str = (f"  target={tgt:.0f} Mbps" if tgt else "  target=unpaced") if cfg.tune_udp else ""
        print(f"  t={entry['t']:.1f}  send={entry['send_mbps']:.2f} Mbps  "
              f"recv={entry['recv_mbps']:.2f} Mbps  "
              f"send={entry.get('send_pps', 0):.0f} pps  recv={entry.get('recv_pps', 0):.0f} pps  "
              f"rtt={rtt_str}{tgt_str}", flush=True)

    if cfg.tune_udp:
        print(f"Running UDP loss-target tuning: server={args.server_ip} "
              f"direction={cfg.direction} streams={cfg.streams} "
              f"acceptable_loss={cfg.acceptable_loss_pct}% max_duration={cfg.max_duration_sec}s", flush=True)
    else:
        print(f"Running throughput test: server={args.server_ip} transport={cfg.transport} "
              f"direction={cfg.direction} streams={cfg.streams} duration={cfg.duration_sec}s", flush=True)

    summary = run_throughput_test(cfg, on_interval=on_interval, stop_requested=stop_requested,
                                   log_base_dir=args.log_dir)
    _print_throughput_summary(summary)


def _print_tuning_summary(tuning):
    print("\n=== UDP Rate Tuning (AIMD ramp) ===")
    target = tuning.get("acceptable_loss_pct")
    probe = tuning.get("probe") or {}
    print(f"  acceptable loss target: {target:.2f}%")
    print(f"  unpaced probe: offered={probe.get('offered_mbps', 0):.0f} Mbps  "
          f"goodput={probe.get('goodput_mbps', 0):.0f} Mbps  loss={probe.get('loss_pct', 0):.2f}%")
    trials = tuning.get("trials") or []
    if trials:
        print("  ramp trials (target -> goodput @ loss):")
        for tr in trials:
            flag = "ok" if tr["loss_pct"] <= target else "BREACH"
            print(f"    {tr['target_mbps']:8.0f} Mbps -> {tr['goodput_mbps']:8.0f} Mbps "
                  f"@ {tr['loss_pct']:5.2f}%  [{flag}]")
    best = tuning.get("best")
    if best:
        label = "unpaced (already under target)" if not best.get("target_mbps") else f"{best['target_mbps']:.0f} Mbps target"
        print(f"  BEST sustainable: {label} -> goodput={best.get('goodput_mbps', 0):.0f} Mbps "
              f"({best.get('goodput_pps', 0):.0f} pps) at {best.get('loss_pct', 0):.2f}% loss")
    else:
        print("  no rate met the loss target within the budget")


def _print_throughput_summary(summary):
    tuning = summary.get("tuning")
    if tuning:
        _print_tuning_summary(tuning)
    print("\n=== Throughput Test Summary ===")
    print(f"  send: {summary['send_mbps']:.2f} Mbps   recv: {summary['recv_mbps']:.2f} Mbps")
    print(f"  send: {summary.get('send_pps', 0):.1f} pps   recv: {summary.get('recv_pps', 0):.1f} pps")
    if summary.get("loss_pct") is not None:
        print(f"  UDP packet loss: {summary['loss_pct']:.3f}%")
    if summary.get("retransmits") is not None:
        print(f"  TCP retransmits: {summary['retransmits']}")
    print(f"  unloaded RTT: {summary.get('unloaded_rtt_ms')} ms")
    lr = summary.get("loaded_rtt") or {}
    print(f"  loaded RTT: avg={lr.get('avg')} best={lr.get('best')} worst={lr.get('worst')} "
          f"stddev={lr.get('stddev')} jitter={lr.get('jitter')}")
    bb = summary.get("bufferbloat") or {}
    print(f"  bufferbloat: BDP={bb.get('bdp_bytes')} bytes  buffered={bb.get('buffered_bytes')} bytes  "
          f"bloat_factor={bb.get('bloat_factor')}")
    print(f"  log: {summary.get('log_json')}")
    print(f"       {summary.get('log_text')}")


def cmd_mtr(args):
    caps = privileges.probe_capabilities()
    if not caps.get("mtr"):
        print(f"ERROR: MTR is unavailable: {privileges.capability_reason('mtr', caps)}")
        print("MTR requires root/administrator privileges (raw ICMP socket).")
        sys.exit(1)

    try:
        runner = MtrRunner(target=args.target, protocol=args.protocol, port=args.port,
                            interval_ms=args.interval_ms, max_hops=args.max_hops,
                            timeout_sec=args.timeout)
    except (ValueError, MtrError) as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    print(f"MTR to {args.target} ({runner.target_ip}) via {args.protocol}, "
          f"interval={args.interval_ms}ms  (Ctrl+C to stop)\n", flush=True)

    stop_event = threading.Event()

    def handle_sigint(_signum, _frame):
        print("\nstopping MTR...", flush=True)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sigint)

    last_snapshot = []

    def on_round(snapshot, round_num):
        nonlocal last_snapshot
        last_snapshot = snapshot
        _print_mtr_table(snapshot, round_num)

    try:
        runner.run(stop_event, on_round=on_round, max_rounds=args.count)
    except MtrError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    data = {
        "target": args.target,
        "target_ip": runner.target_ip,
        "protocol": args.protocol,
        "interval_ms": args.interval_ms,
        "hops": last_snapshot,
    }
    text_lines = _format_mtr_text(data)
    json_path, log_path = logging_store.save_result(constants.TEST_TYPE_MTR, data, text_lines,
                                                      base_dir=args.log_dir)
    print(f"\nlog: {json_path}\n     {log_path}")


def _print_mtr_table(snapshot, round_num):
    print(f"\n--- round {round_num} ---")
    header = f"{'TTL':>3} {'IP':<16} {'Host':<28} {'ASN':<8} {'Loss%':>7} {'Best':>8} {'Avg':>8} {'Worst':>8} {'StdDev':>8} {'Jitter':>8}"
    print(header)
    for hop in snapshot:
        ip = hop.get("ip") or "???"
        host = (hop.get("hostname") or "")[:28]
        asn = hop.get("asn") or ("skip" if hop.get("lookup_skipped") else "")
        loss = hop.get("loss_pct", 0.0)

        def fmt(v):
            return f"{v:.2f}" if isinstance(v, (int, float)) else "*"

        print(f"{hop['ttl']:>3} {ip:<16} {host:<28} {asn:<8} {loss:>6.1f}% "
              f"{fmt(hop.get('best')):>8} {fmt(hop.get('avg')):>8} {fmt(hop.get('worst')):>8} "
              f"{fmt(hop.get('stddev')):>8} {fmt(hop.get('jitter')):>8}")


def _format_mtr_text(data):
    lines = [f"MTR to {data['target']} ({data['target_ip']}) via {data['protocol']}", ""]
    for hop in data["hops"]:
        lines.append(str(hop))
    return lines


def cmd_connectivity(args):
    print(f"Checking connectivity to {args.server_ip} (udp={args.udp_port} tcp={args.tcp_port})...")
    result = connectivity.run_all(args.server_ip, args.udp_port, args.tcp_port, timeout_sec=args.timeout)

    icmp = result["icmp"]
    print(f"  ICMP: {'OK' if icmp['success'] else 'FAIL'}"
          + (f" (loss={icmp.get('loss_pct')}%)" if icmp.get("loss_pct") is not None else ""))
    udp = result["udp"]
    print(f"  UDP:  {'OK' if udp['success'] else 'FAIL'}"
          + (f" (rtt={udp['rtt_ms']:.2f}ms)" if udp.get("rtt_ms") else f" ({udp.get('error')})"))
    tcp = result["tcp"]
    print(f"  TCP:  {'OK' if tcp['success'] else 'FAIL'}"
          + (f" (rtt={tcp['rtt_ms']:.2f}ms)" if tcp.get("rtt_ms") else f" ({tcp.get('error')})"))

    text_lines = [f"Connectivity check: {args.server_ip}", str(result)]
    json_path, log_path = logging_store.save_result(constants.TEST_TYPE_CONNECTIVITY, result, text_lines,
                                                      base_dir=args.log_dir)
    print(f"  log: {json_path}\n       {log_path}")


def cmd_pmtud(args):
    caps = privileges.probe_capabilities()
    if not caps.get("pmtud"):
        print(f"ERROR: PMTUD is unavailable: {privileges.capability_reason('pmtud', caps)}")
        sys.exit(1)

    print(f"Running PMTUD to {args.server_ip}:{args.udp_port}...")
    try:
        result = run_pmtud(args.server_ip, udp_port=args.udp_port, min_size=args.min_size,
                            max_size=args.max_size, timeout_sec=args.timeout)
    except PmtudError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    if result["success"]:
        print(f"  Path MTU: {result['path_mtu']} bytes (payload {result['path_mtu_payload_bytes']} bytes)")
    else:
        print(f"  FAILED: {result.get('error')}")
    print(f"  connectivity confirmed: {result.get('connectivity_confirmed')}")

    text_lines = [f"PMTUD to {args.server_ip}:{args.udp_port}", str(result)]
    json_path, log_path = logging_store.save_result(constants.TEST_TYPE_PMTUD, result, text_lines,
                                                      base_dir=args.log_dir)
    print(f"  log: {json_path}\n       {log_path}")


def cmd_protocols(args):
    print(f"Checking protocol reachability to {args.server_ip}...")
    result = protocols.run_all(args.server_ip, args.udp_port, args.tcp_port, timeout_sec=args.timeout)

    for key in ("icmp", "udp", "tcp", "esp_udp", "esp_native", "gre"):
        r = result.get(key, {})
        status = "OK" if r.get("success") else ("SKIPPED" if r.get("skipped") else "FAIL")
        print(f"  {key:<12}: {status}" + (f"  ({r.get('error')})" if r.get("error") else ""))

    text_lines = [f"Protocol check: {args.server_ip}", str(result)]
    json_path, log_path = logging_store.save_result(constants.TEST_TYPE_PROTOCOLS, result, text_lines,
                                                      base_dir=args.log_dir)
    print(f"  log: {json_path}\n       {log_path}")


def cmd_webui(args):
    import importlib.util
    import os
    import subprocess

    # The web UI runs as a `streamlit run` subprocess using this same
    # interpreter. Fail early with an actionable message if its dependencies
    # are not installed in *this* Python environment.
    missing = [m for m in ("streamlit", "pandas") if importlib.util.find_spec(m) is None]
    if missing:
        print("ERROR: the web UI requires these packages, which are not installed "
              f"in this Python environment:\n    {', '.join(missing)}\n")
        print(f"Interpreter: {sys.executable}\n")
        print("Install the dependencies with:")
        print(f"    {sys.executable} -m pip install -r requirements.txt")
        print("(or run this command with the same interpreter you used for `pip install`).")
        sys.exit(1)

    app_path = os.path.join(os.path.dirname(__file__), "webui", "app.py")
    cmd = [sys.executable, "-m", "streamlit", "run", app_path]
    if args.port:
        cmd += ["--server.port", str(args.port)]
    subprocess.run(cmd)


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
