"""Headless CLI for NPERF.

Subcommands:
    server        run as a server (throughput + diagnostics responders)
    tput          run a client throughput test (iperf3+bbperf style)
    fullbench     run a full matrix of throughput tests (scripting/automation)
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
from collections.abc import Mapping

from .common import constants, privileges, logging_store
from .client.engine import ThroughputClientConfig, ThroughputError, run_throughput_test
from .client import fullbench as fullbench_mod
from .diagnostics.mtr import MtrRunner, MtrError
from .diagnostics import connectivity, protocols
from .diagnostics.pmtud import discover_path_mtu, PmtudError
from .transport import stream_worker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=constants.APP_NAME,
        description="NPERF: throughput + diagnostics tool (client/server, CLI/Web UI)")
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

    p_tp = sub.add_parser("tput", help="run a client throughput test")
    p_tp.add_argument("--server-ip", required=True, help="server hostname or IP")
    p_tp.add_argument("-v6", "--ipv6", action="store_true", help="use IPv6 instead of the default IPv4")
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
    p_tp.add_argument("--packet-length", type=int, default=None, metavar="BYTES",
                       help="skip PMTUD and manually set the TCP MSS or UDP datagram length in bytes "
                            "(maximum is limited by the 9000-byte total path-MTU cap)")
    p_tp.add_argument("--jumbo-mtu", action="store_true",
                       help="allow PMTUD to probe total MTUs up to 9000 instead of the default 1500")
    p_tp.add_argument("--bind", default="0.0.0.0", help="local address to bind data sockets")
    p_tp.add_argument("--procs", type=int, default=1,
                       help="spread streams across N worker processes to use multiple CPU cores "
                            "(default: 1 = single-process/threaded). Requires >1 stream.")
    p_tp.add_argument("--acceptable-loss", type=float, default=None, metavar="PCT",
                       help="UDP only: tune the send rate (multiplicative search) to find the max rate whose "
                            "loss stays <= PCT%%. The search uses --max-duration as its time budget.")
    p_tp.add_argument("--max-duration", type=float, default=None, metavar="SEC",
                       help="UDP tuning: max time budget for the rate search "
                            f"(default: {constants.DEFAULT_MAX_DURATION_SEC}s).")
    p_tp.add_argument("--udp-start-mbps", type=float, default=constants.UDP_TUNE_START_MBPS,
                       metavar="MBPS",
                       help=f"UDP tuning starting aggregate rate (default: {constants.UDP_TUNE_START_MBPS:g} Mbps)")
    p_tp.add_argument("--log-dir", default=None)
    p_tp.set_defaults(func=cmd_throughput)

    p_fb = sub.add_parser(
        "fullbench",
        help="run a full matrix of throughput tests (TCP/UDP x up/down x stream counts)",
        description="Run a matrix of throughput tests for scripting/automation. Stores each "
                    "test's result locally and can POST each result as JSON to an HTTP(S) endpoint.")
    p_fb.add_argument("--server-ip", required=True, help="server hostname or IP")
    p_fb.add_argument("-v6", "--ipv6", action="store_true", help="use IPv6 instead of the default IPv4")
    p_fb.add_argument("--control-port", type=int, default=constants.DEFAULT_CONTROL_PORT)
    p_fb.add_argument("--transports", default="tcp,udp",
                       help="comma-separated transports to test: tcp, udp (default: tcp,udp)")
    p_fb.add_argument("--directions", default="up,down",
                       help="comma-separated directions to test: up, down (default: up,down)")
    p_fb.add_argument("--streams", default="1,8,16,32",
                       help="comma-separated list of parallel-stream counts, e.g. 1,8,16,32 "
                            f"(default: 1,8,16,32; maximum: {constants.MAX_PARALLEL_STREAMS})")
    p_fb.add_argument("--procs", default="1",
                       help="worker processes per test: a single value applied to every run, or a "
                            "comma-separated list matching --streams (paired per stream count). "
                            "Default: 1.")
    p_fb.add_argument("-t", "--time", type=float, default=constants.DEFAULT_TEST_DURATION_SEC,
                       help="duration (sec) for TCP and non-tuning UDP tests (default: 10)")
    p_fb.add_argument("--acceptable-loss", type=float, default=None, metavar="PCT",
                       help="enable UDP loss-target tuning: find the max UDP rate whose loss stays "
                            "<= PCT%%. Applies to UDP tests only; TCP tests still use --time.")
    p_fb.add_argument("--udp-max-duration", type=float, default=constants.DEFAULT_MAX_DURATION_SEC,
                       metavar="SEC",
                       help=f"max time budget for each UDP tuning search "
                             f"(default: {constants.DEFAULT_MAX_DURATION_SEC}s)")
    p_fb.add_argument("--udp-start-mbps", type=float, default=constants.UDP_TUNE_START_MBPS,
                       metavar="MBPS",
                       help=f"UDP tuning starting aggregate rate (default: {constants.UDP_TUNE_START_MBPS:g} Mbps)")
    p_fb.add_argument("--start-port", type=int, default=None,
                       help="server base port (auto per transport if omitted)")
    p_fb.add_argument("--udp-port", type=int, default=None,
                       help="server UDP base port (default: 10000)")
    p_fb.add_argument("--tcp-port", type=int, default=None,
                       help="server TCP base port (default: 11000)")
    p_fb.add_argument("--jumbo-mtu", action="store_true",
                       help="allow PMTUD to probe total MTUs up to 9000 instead of the default 1500")
    p_fb.add_argument("--bind", default="0.0.0.0", help="local address to bind data sockets")
    p_fb.add_argument("--label", default=None, help="optional label attached to every result")
    p_fb.add_argument("--run-id", type=int, default=None,
                       help="global run id recorded with every test (orchestration scripts set "
                            "this, e.g. from the results-API /run-id allocator). Optional.")
    p_fb.add_argument("--vm-name", default=None,
                       help="host/VM identifier recorded with every test (default: hostname)")
    p_fb.add_argument("--cloud", default=None, help="cloud/provider label recorded with every test")
    p_fb.add_argument("--environment", default=None, help="environment label recorded with every test")
    p_fb.add_argument("--tool-version", type=int, default=1,
                       help="tool_version integer recorded with every test (default: 1)")
    p_fb.add_argument("--test-type", default="nperf",
                       help="test_type recorded with every test (default: nperf)")
    p_fb.add_argument("--export-url", default=None,
                       help="HTTP(S) endpoint to POST each test's JSON result to")
    p_fb.add_argument("--export-method", default="POST", choices=["POST", "PUT"],
                       help="HTTP method for export (default: POST)")
    p_fb.add_argument("--export-header", action="append", default=[], metavar="'Key: Value'",
                       help="extra HTTP header for export (repeatable), e.g. for auth tokens")
    p_fb.add_argument("--export-slim", action="store_true",
                       help="omit bulky arrays (interval_series, per_stream) from exported payloads")
    p_fb.add_argument("--export-timeout", type=float, default=15.0,
                       help="per-request export timeout in seconds (default: 15)")
    p_fb.add_argument("--dry-run", action="store_true",
                       help="print the planned test matrix and exit without running")
    p_fb.add_argument("--log-dir", default=None)
    p_fb.set_defaults(func=cmd_fullbench)

    p_mtr = sub.add_parser("mtr", help="MTR diagnostic to any IP/FQDN")
    p_mtr.add_argument("target", help="target IP address or FQDN")
    p_mtr.add_argument("-v6", "--ipv6", action="store_true", help="use IPv6 instead of the default IPv4")
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
    p_conn.add_argument("-v6", "--ipv6", action="store_true", help="use IPv6 instead of the default IPv4")
    p_conn.add_argument("--udp-port", type=int, default=constants.DEFAULT_UDP_PORT)
    p_conn.add_argument("--tcp-port", type=int, default=constants.DEFAULT_TCP_PORT)
    p_conn.add_argument("--timeout", type=float, default=2.0)
    p_conn.add_argument("--log-dir", default=None)
    p_conn.set_defaults(func=cmd_connectivity)

    p_pmtud = sub.add_parser("pmtud", help="Path MTU Discovery test against the server")
    p_pmtud.add_argument("--server-ip", required=True)
    p_pmtud.add_argument("-v6", "--ipv6", action="store_true", help="use IPv6 instead of the default IPv4")
    p_pmtud.add_argument("--udp-port", type=int, default=constants.DEFAULT_UDP_PORT)
    p_pmtud.add_argument("--jumbo-mtu", action="store_true",
                         help="allow total-MTU probes up to 9000 instead of the default 1500")
    min_mtu_group = p_pmtud.add_mutually_exclusive_group()
    min_mtu_group.add_argument(
        "--min-mtu", type=int, default=None, metavar="MTU",
        help=f"minimum total IP MTU to probe (default: {constants.PMTUD_MIN_MTU})")
    min_mtu_group.add_argument(
        "--min-size", type=int, default=None, metavar="UDP_PAYLOAD_BYTES",
        help="deprecated: minimum UDP payload size; converted to total IP MTU")
    max_mtu_group = p_pmtud.add_mutually_exclusive_group()
    max_mtu_group.add_argument(
        "--max-mtu", type=int, default=None, metavar="MTU",
        help="maximum total IP MTU to probe (default: selected 1500/9000 mode ceiling)")
    max_mtu_group.add_argument(
        "--max-size", type=int, default=None, metavar="UDP_PAYLOAD_BYTES",
        help="deprecated: maximum UDP payload size; converted to total IP MTU")
    p_pmtud.add_argument("--timeout", type=float, default=1.5)
    p_pmtud.add_argument("--log-dir", default=None)
    p_pmtud.set_defaults(func=cmd_pmtud)

    p_proto = sub.add_parser("protocols", help="UDP/TCP/ICMP/ESP/ESP-UDP/GRE reachability matrix")
    p_proto.add_argument("--server-ip", required=True)
    p_proto.add_argument("-v6", "--ipv6", action="store_true", help="use IPv6 instead of the default IPv4")
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
    print(f"NPERF server starting on {args.bind} "
          f"(control={args.control_port} udp={args.udp_port} tcp={args.tcp_port})", flush=True)
    run_server(config, block=True)


def cmd_throughput(args):
    if args.acceptable_loss is not None and args.transport != "udp":
        print("error: --acceptable-loss is only valid for UDP (--udp)", flush=True)
        return
    max_dur = (args.max_duration if args.max_duration is not None
               else (constants.DEFAULT_MAX_DURATION_SEC
                     if args.acceptable_loss is not None else args.time))

    try:
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
            udp_start_mbps=args.udp_start_mbps,
            ipv6=args.ipv6,
            jumbo_mtu=args.jumbo_mtu,
        )
    except ValueError as exc:
        print(f"error: {exc}", flush=True)
        sys.exit(2)
    if args.packet_length is not None:
        try:
            path_mtu = stream_worker.path_mtu_for_packet_size(
                args.packet_length, cfg.transport, family=cfg.family)
            if path_mtu > constants.PMTUD_JUMBO_MAX_MTU:
                raise ValueError(
                    f"packet length implies a {path_mtu}-byte path MTU, exceeding "
                    f"the {constants.PMTUD_JUMBO_MAX_MTU}-byte cap")
            cfg.path_mtu = path_mtu
        except ValueError as exc:
            print(f"error: {exc}", flush=True)
            return

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
              f"ip_version={cfg.ip_version} direction={cfg.direction} streams={cfg.streams} "
              f"acceptable_loss={cfg.acceptable_loss_pct}% max_duration={cfg.max_duration_sec}s", flush=True)
    else:
        print(f"Running throughput test: server={args.server_ip} ip_version={cfg.ip_version} "
              f"transport={cfg.transport} direction={cfg.direction} streams={cfg.streams} "
              f"duration={cfg.duration_sec}s", flush=True)

    try:
        summary = run_throughput_test(cfg, on_interval=on_interval, stop_requested=stop_requested,
                                      log_base_dir=args.log_dir)
    except ThroughputError as exc:
        print(f"ERROR: throughput test could not start: {exc}", flush=True)
        sys.exit(1)
    _print_throughput_summary(summary)
    termination_reason = (summary.get("tuning") or {}).get("termination_reason")
    if (summary.get("aborted")
            or summary.get("status") in ("aborted", "cancelled")
            or termination_reason in ("stopped", "target_not_attained")):
        sys.exit(1)


def _tuning_trial_status(trial, target):
    target_attained = trial.get("target_attained")
    if target_attained is None:
        target_attained = (trial.get("classification") or {}).get("target_attained")
    if target_attained is False:
        return "TARGET NOT ATTAINED"
    return "ok" if trial.get("loss_pct", float("inf")) <= target else "BREACH"


def _print_tuning_summary(tuning):
    print("\n=== UDP Rate Tuning (multiplicative search) ===")
    target = tuning.get("acceptable_loss_pct")
    probe = tuning.get("probe") or {}
    print(f"  acceptable loss target: {target:.2f}%")
    print(f"  start: {tuning.get('start_mbps', 0):g} Mbps; automatic increase: "
          f"{tuning.get('initial_increase_pct', 0):g}% to "
          f"{tuning.get('max_increase_pct', 0):g}%")
    if probe:
        probe_status = _tuning_trial_status(probe, target)
        print(f"  paced probe: offered={probe.get('offered_mbps', 0):.0f} Mbps  "
              f"goodput={probe.get('goodput_mbps', 0):.0f} Mbps  "
              f"loss={probe.get('loss_pct', 0):.2f}%  [{probe_status}]")
    else:
        print("  paced probe: not completed")
    trials = tuning.get("trials") or []
    if trials:
        print("  ramp trials (target -> goodput @ loss):")
        for tr in trials:
            flag = _tuning_trial_status(tr, target)
            print(f"    {tr['target_mbps']:8.0f} Mbps -> {tr['goodput_mbps']:8.0f} Mbps "
                  f"@ {tr['loss_pct']:5.2f}%  [{flag}]")
    best = tuning.get("best")
    reason = tuning.get("termination_reason", "unknown")
    confirmed = bool(tuning.get("ceiling_confirmed"))
    if best:
        label = "unpaced (already under target)" if not best.get("target_mbps") else f"{best['target_mbps']:.0f} Mbps target"
        result_label = ("CONFIRMED ceiling" if confirmed
                        else "best passing lower bound (ceiling unconfirmed)")
        print(f"  {result_label}: {label} -> goodput={best.get('goodput_mbps', 0):.0f} Mbps "
              f"({best.get('goodput_pps', 0):.0f} pps) at {best.get('loss_pct', 0):.2f}% loss")
    else:
        print("  no rate met the loss target within the budget")
    if not tuning.get("search_complete", False):
        print(f"  search incomplete: {reason}")
    if reason == "target_not_attained":
        print("  RESULT INVALID AS NETWORK CEILING: sender did not attain the requested load")
    limiting_factor = tuning.get("limiting_factor")
    if limiting_factor:
        print("  limiting evidence: "
              f"factor={limiting_factor} scope={tuning.get('limiting_scope') or 'unknown'} "
              f"confidence={tuning.get('limiting_confidence') or 'low'}")


def _rxq_drop_display(endpoint):
    if not isinstance(endpoint, Mapping):
        return "unavailable"
    supported = endpoint.get("rxq_overflow_supported")
    if supported is None:
        stream_support = [
            stream.get("rxq_overflow_supported")
            for stream in endpoint.get("streams") or []
            if isinstance(stream, Mapping)
        ]
        supported = any(value is True for value in stream_support)
    drops = endpoint.get("kernel_rxq_drops")
    if supported is not True or drops is None or isinstance(drops, bool):
        return "unavailable"
    try:
        return str(max(0, int(drops)))
    except (TypeError, ValueError, OverflowError):
        return "unavailable"


def _print_throughput_summary(summary):
    tuning = summary.get("tuning")
    if tuning:
        _print_tuning_summary(tuning)
    pmtud = summary.get("pmtud") or {}
    print("\n=== Throughput Test Summary ===")
    print(f"  status: {summary.get('status', 'complete')}")
    print(f"  IP version: {summary.get('ip_version', 'IPv4')}")
    print(f"  path MTU: {summary.get('path_mtu')} bytes  "
          f"source={pmtud.get('path_mtu_source', 'unknown')}  "
          f"bidirectional={pmtud.get('bidirectional_validated')} "
          f"method={pmtud.get('bidirectional_validation', 'unknown')}")
    print(f"  PMTUD ceiling: jumbo_mtu={summary.get('jumbo_mtu', False)}  "
          f"max_mtu={summary.get('probe_max_mtu')}  "
          f"reached={summary.get('probe_ceiling_reached')}")
    print(f"  packet sizing: TCP MSS={summary.get('tcp_mss_bytes')} bytes  "
          f"UDP datagram={summary.get('udp_datagram_bytes')} bytes")
    print(f"  send: {summary['send_mbps']:.2f} Mbps   recv: {summary['recv_mbps']:.2f} Mbps")
    print(f"  send: {summary.get('send_pps', 0):.1f} pps   recv: {summary.get('recv_pps', 0):.1f} pps")
    if summary.get("loss_pct") is not None:
        print(f"  UDP packet loss: {summary['loss_pct']:.3f}%")
        udp_batch = summary.get("udp_batch") or {}
        print("  UDP batch backend: "
              f"client={(udp_batch.get('client') or {}).get('selected', 'unknown')} "
              f"server={(udp_batch.get('server') or {}).get('selected', 'unknown')}")
    if summary.get("retransmits") is not None:
        print(f"  TCP retransmits: {summary['retransmits']}")
    print(f"  unloaded RTT: {summary.get('unloaded_rtt_ms')} ms")
    lr = summary.get("loaded_rtt") or {}
    print(f"  loaded RTT: avg={lr.get('avg')} best={lr.get('best')} worst={lr.get('worst')} "
          f"stddev={lr.get('stddev')} jitter={lr.get('jitter')}")
    bb = summary.get("bufferbloat") or {}
    print(f"  bufferbloat: BDP={bb.get('bdp_bytes')} bytes  buffered={bb.get('buffered_bytes')} bytes  "
          f"bloat_factor={bb.get('bloat_factor')}")
    socket_telemetry = summary.get("socket_telemetry") or {}
    if summary.get("transport") == "udp" and socket_telemetry:
        print("  kernel RX queue drops: "
              f"client={_rxq_drop_display(socket_telemetry.get('client'))} "
              f"server={_rxq_drop_display(socket_telemetry.get('server'))}")
    print(f"  log: {summary.get('log_json')}")
    print(f"       {summary.get('log_text')}")


def cmd_fullbench(args):
    try:
        cfg = fullbench_mod.FullBenchConfig(
            server_ip=args.server_ip,
            control_port=args.control_port,
            transports=fullbench_mod.parse_csv_choices(args.transports, ("tcp", "udp"), "transports"),
            directions=fullbench_mod.parse_csv_choices(args.directions, ("up", "down"), "directions"),
            streams=fullbench_mod.parse_int_list(args.streams, "streams"),
            procs=fullbench_mod.parse_int_list(args.procs, "procs"),
            duration_sec=args.time,
            acceptable_loss_pct=args.acceptable_loss,
            udp_max_duration_sec=args.udp_max_duration,
            udp_start_mbps=args.udp_start_mbps,
            bind=args.bind,
            start_port=args.start_port,
            udp_port=args.udp_port,
            tcp_port=args.tcp_port,
            export_url=args.export_url,
            export_method=args.export_method,
            export_headers=fullbench_mod.parse_headers(args.export_header),
            export_slim=args.export_slim,
            export_timeout_sec=args.export_timeout,
            label=args.label,
            run_id=args.run_id,
            vm_name=args.vm_name,
            cloud=args.cloud,
            environment=args.environment,
            tool_version=args.tool_version,
            test_type=args.test_type,
            ipv6=args.ipv6,
            jumbo_mtu=args.jumbo_mtu,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(2)

    plan = cfg.build_plan()
    print(f"fullbench plan: {len(plan)} test(s) -> {cfg.server_ip} ({cfg.ip_version})")
    print(f"identity: ip_version={cfg.ip_version} run_id={cfg.run_id} vm_name={cfg.vm_name} cloud={cfg.cloud} "
          f"environment={cfg.environment} test_type={cfg.test_type}")
    print(f"PMTUD ceiling: jumbo_mtu={cfg.jumbo_mtu} max_mtu="
          f"{constants.PMTUD_JUMBO_MAX_MTU if cfg.jumbo_mtu else constants.PMTUD_DEFAULT_MAX_MTU}")
    for i, spec in enumerate(plan, start=1):
        mode = (f"tune<= {cfg.acceptable_loss_pct}% from {cfg.udp_start_mbps:g} Mbps "
                f"(<= {cfg.udp_max_duration_sec:.0f}s)"
                if spec["tune"] else f"fixed {cfg.duration_sec:.0f}s")
        print(f"  [{i}] {spec['transport']} {spec['direction']} "
              f"streams={spec['streams']} procs={spec['procs']}  [{mode}]")
    if cfg.export_url:
        print(f"export: {cfg.export_method} {cfg.export_url}"
              + ("  (slim)" if cfg.export_slim else ""))

    if args.dry_run:
        print("\n(dry run -- not executing)")
        return

    stop_requested = threading.Event()

    def handle_sigint(_signum, _frame):
        print("\nstopping fullbench (finishing current test)...", flush=True)
        stop_requested.set()

    signal.signal(signal.SIGINT, handle_sigint)

    print()
    try:
        manifest = fullbench_mod.run_fullbench(cfg, on_progress=lambda m: print(m, flush=True),
                                               stop_requested=stop_requested, log_base_dir=args.log_dir)
    except PmtudError as exc:
        print(f"ERROR: fullbench PMTUD failed: {exc}")
        sys.exit(1)
    _print_fullbench_summary(manifest)
    failed_tests = [test for test in manifest["tests"] if not test.get("ok")]
    failed_exports = [
        test for test in manifest["tests"]
        if cfg.export_url and not (test.get("export") or {}).get("ok")
    ]
    invalid_benchmarks = [
        test for test in manifest["tests"]
        if test.get("tuning_termination_reason") == "target_not_attained"
    ]
    incomplete = (manifest.get("completed", 0) < manifest.get("test_count", 0)
                  or manifest.get("status") in ("aborted", "incomplete"))
    if failed_tests or failed_exports or invalid_benchmarks or incomplete:
        if incomplete:
            print("ERROR: fullbench matrix did not complete")
        if failed_exports:
            print(f"ERROR: {len(failed_exports)} fullbench export(s) failed")
        if failed_tests:
            print(f"ERROR: {len(failed_tests)} fullbench test(s) failed")
        if invalid_benchmarks:
            print(f"ERROR: {len(invalid_benchmarks)} fullbench result(s) are not valid network ceilings")
        sys.exit(1)


def _print_fullbench_summary(manifest):
    print("\n=== Full Benchmark Summary ===")
    print(f"  benchmark_id: {manifest['benchmark_id']}")
    print(f"  IP version: {(manifest.get('pmtud') or {}).get('ip_version', manifest.get('ip_version', 'IPv4'))}")
    pmtud = manifest.get("pmtud") or {}
    print(f"  path MTU: {pmtud.get('path_mtu')} bytes  source={pmtud.get('path_mtu_source', 'unknown')}  "
          f"bidirectional={pmtud.get('bidirectional_validated')} "
          f"method={pmtud.get('bidirectional_validation', 'unknown')}")
    print(f"  PMTUD ceiling: jumbo_mtu={manifest.get('jumbo_mtu', False)}  "
          f"max_mtu={manifest.get('probe_max_mtu')}  "
          f"reached={manifest.get('probe_ceiling_reached')}")
    print(f"  packet sizing: TCP MSS={pmtud.get('tcp_mss_bytes')} bytes  "
          f"UDP datagram={pmtud.get('udp_datagram_bytes')} bytes")
    status = manifest.get("status") or (
        "complete" if manifest.get("completed", 0) >= manifest.get("test_count", 0)
        else "incomplete")
    print(f"  status: {status}")
    print(f"  completed: {manifest['completed']}/{manifest['test_count']}")
    header = (f"  {'transport':<9} {'dir':<5} {'streams':>7} {'procs':>5} {'send Mbps':>11} "
              f"{'recv Mbps':>11} {'send pps':>11} {'recv pps':>11} {'TCP retx/UDP loss':>17}")
    print(header)
    for t in manifest["tests"]:
        if t.get("tuning_termination_reason") == "target_not_attained":
            print(f"  {t['transport']:<9} {t['direction']:<5} {t['streams']:>7} {t['procs']:>5}"
                  "  TARGET NOT ATTAINED: not a network ceiling; "
                  f"factor={t.get('tuning_limiting_factor') or 'unknown'} "
                  f"scope={t.get('tuning_limiting_scope') or 'unknown'}")
            continue
        if not t.get("ok"):
            print(f"  {t['transport']:<9} {t['direction']:<5} {t['streams']:>7} {t['procs']:>5}"
                  f"  FAILED: {t.get('error')}")
            continue
        if t.get("transport") == "tcp":
            retransmits = t.get("retransmits")
            metric_str = str(int(retransmits)) if isinstance(retransmits, (int, float)) else "-"
        else:
            loss = t.get("loss_pct")
            metric_str = f"{loss:.3f}%" if isinstance(loss, (int, float)) else "-"
            if t.get("tuning_search_complete") is False:
                metric_str += f" INCOMPLETE({t.get('tuning_termination_reason', 'unknown')})"
        exp = t.get("export")
        exp_str = ""
        if exp is not None:
            exp_str = f"  export={'ok' if exp['ok'] else 'FAIL(' + str(exp.get('error')) + ')'}"
        print(f"  {t['transport']:<9} {t['direction']:<5} {t['streams']:>7} {t['procs']:>5} "
              f"{t.get('send_mbps', 0):>11.1f} {t.get('recv_mbps', 0):>11.1f} "
              f"{t.get('send_pps', 0):>11.0f} {t.get('recv_pps', 0):>11.0f} {metric_str:>17}{exp_str}")
    print(f"  manifest: {manifest.get('manifest_json')}")


def cmd_mtr(args):
    caps = privileges.probe_capabilities()
    mtr_capability = "mtr_ipv6_icmp" if args.ipv6 else "mtr"
    if not caps.get(mtr_capability):
        print(f"ERROR: MTR is unavailable: {privileges.capability_reason(mtr_capability, caps)}")
        print("MTR requires root/administrator privileges (raw ICMP socket).")
        sys.exit(1)

    try:
        runner = MtrRunner(target=args.target, protocol=args.protocol, port=args.port,
                            interval_ms=args.interval_ms, max_hops=args.max_hops,
                            timeout_sec=args.timeout, ipv6=args.ipv6)
    except (ValueError, MtrError) as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    print(f"MTR to {args.target} ({runner.target_ip}) via {args.protocol} {runner.ip_version}, "
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
        "ip_version": runner.ip_version,
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
    lines = [f"MTR to {data['target']} ({data['target_ip']}) {data.get('ip_version', 'IPv4')} via {data['protocol']}", ""]
    for hop in data["hops"]:
        lines.append(str(hop))
    return lines


def cmd_connectivity(args):
    print(f"Checking {('IPv6' if args.ipv6 else 'IPv4')} connectivity to {args.server_ip} "
          f"(udp={args.udp_port} tcp={args.tcp_port})...")
    result = connectivity.run_all(args.server_ip, args.udp_port, args.tcp_port, timeout_sec=args.timeout,
                                  ipv6=args.ipv6)

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
    udp_overhead = 48 if args.ipv6 else 28
    legacy_size_used = args.min_size is not None or args.max_size is not None
    if legacy_size_used:
        print("WARNING: --min-size/--max-size are deprecated UDP-payload-byte "
              "options; use --min-mtu/--max-mtu for total IP MTU bytes.",
              file=sys.stderr)
    min_mtu = (args.min_mtu if args.min_mtu is not None
               else (args.min_size + udp_overhead
                     if args.min_size is not None else constants.PMTUD_MIN_MTU))
    mode_max_mtu = (constants.PMTUD_JUMBO_MAX_MTU
                    if args.jumbo_mtu else constants.PMTUD_DEFAULT_MAX_MTU)
    max_mtu = (args.max_mtu if args.max_mtu is not None
               else (args.max_size + udp_overhead
                     if args.max_size is not None else mode_max_mtu))
    if min_mtu < constants.PMTUD_MIN_MTU:
        print(f"ERROR: --min-mtu must be at least {constants.PMTUD_MIN_MTU}")
        sys.exit(2)
    if max_mtu < constants.PMTUD_MIN_MTU:
        print(f"ERROR: --max-mtu must be at least {constants.PMTUD_MIN_MTU}")
        sys.exit(2)
    if min_mtu > max_mtu:
        print("ERROR: --min-mtu must be less than or equal to --max-mtu")
        sys.exit(2)
    if max_mtu > mode_max_mtu:
        mode = "jumbo" if args.jumbo_mtu else "standard"
        suggestion = "" if args.jumbo_mtu else "; use --jumbo-mtu to opt in to larger probes"
        print(f"ERROR: --max-mtu {max_mtu} exceeds the {mode} mode cap of {mode_max_mtu}{suggestion}")
        sys.exit(2)

    caps = privileges.probe_capabilities()
    pmtud_capability = "pmtud_ipv6" if args.ipv6 else "pmtud"
    if not caps.get(pmtud_capability):
        print(f"ERROR: PMTUD is unavailable: {privileges.capability_reason(pmtud_capability, caps)}")
        sys.exit(1)

    print(f"Running PMTUD ({'IPv6' if args.ipv6 else 'IPv4'}) to {args.server_ip}:{args.udp_port} "
          f"with total-MTU range {min_mtu}-{max_mtu}...")
    try:
        result = discover_path_mtu(args.server_ip, udp_port=args.udp_port, min_mtu=min_mtu,
                                   max_mtu=max_mtu, timeout_sec=args.timeout,
                                   ipv6=args.ipv6)
    except ValueError as exc:
        print(f"ERROR: invalid PMTUD MTU range: {exc}")
        sys.exit(2)
    except PmtudError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    result["jumbo_mtu"] = bool(args.jumbo_mtu)

    if result["success"]:
        print(f"  Path MTU: {result['path_mtu']} bytes (payload {result.get('path_mtu_payload_bytes')} bytes)")
    else:
        print(f"  Path MTU fallback: {result['path_mtu']} bytes")
    print(f"  source: {result.get('path_mtu_source')}")
    print(f"  connectivity confirmed: {result.get('connectivity_confirmed')}")
    print(f"  probe max MTU: {result.get('probe_max_mtu')} bytes; "
          f"ceiling reached: {result.get('probe_ceiling_reached')}; jumbo_mtu={result['jumbo_mtu']}")

    text_lines = [f"PMTUD to {args.server_ip}:{args.udp_port}", str(result)]
    json_path, log_path = logging_store.save_result(constants.TEST_TYPE_PMTUD, result, text_lines,
                                                      base_dir=args.log_dir)
    print(f"  log: {json_path}\n       {log_path}")


def cmd_protocols(args):
    print(f"Checking {('IPv6' if args.ipv6 else 'IPv4')} protocol reachability to {args.server_ip}...")
    result = protocols.run_all(args.server_ip, args.udp_port, args.tcp_port, timeout_sec=args.timeout,
                               ipv6=args.ipv6)

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
