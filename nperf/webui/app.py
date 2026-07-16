"""NPERF Streamlit web UI (client-side test runner).

Launch via: `nperf webui` (wraps `streamlit run app.py`).

The web UI always acts as the *client*: it drives throughput tests and
diagnostics against a remote NPERF server (started separately
via `nperf server` on the target host).
"""

import os
import sys
import threading
import time

# Allow running this file directly via `streamlit run .../webui/app.py`
# without the package being pip-installed.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd
import streamlit as st

from nperf.common import constants, privileges, addr as addr_mod, logging_store
from nperf.client.engine import ThroughputClientConfig, run_throughput_test
from nperf.client.fullbench import FullBenchConfig, run_fullbench
from nperf.diagnostics.mtr import MtrRunner, MtrError
from nperf.diagnostics import connectivity, protocols
from nperf.diagnostics.pmtud import run_pmtud, PmtudError


st.set_page_config(page_title="NPERF", layout="wide")


# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------

def _init_state():
    defaults = {
        "server_ip": "",
        "ipv6": False,
        "udp_port": constants.DEFAULT_UDP_PORT,
        "tcp_port": constants.DEFAULT_TCP_PORT,
        "control_port": constants.DEFAULT_CONTROL_PORT,
        "connectivity_result": None,
        "tp_running": False,
        "tp_stop_event": None,
        "tp_thread": None,
        "tp_live": [],
        "tp_summary": None,
        "tp_error": None,
        "tp_launch_spec": None,
        "fb_running": False,
        "fb_stop_event": None,
        "fb_thread": None,
        "fb_progress": [],
        "fb_result_holder": None,
        "fb_manifest": None,
        "fb_error": None,
        "mtr_running": False,
        "mtr_stop_event": None,
        "mtr_thread": None,
        "mtr_snapshot": [],
        "mtr_round": 0,
        "mtr_target_ip": None,
        "mtr_ip_version": "IPv4",
        "mtr_error": None,
        "pmtud_result": None,
        "protocols_result": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


_init_state()
CAPS = privileges.probe_capabilities()


def _cap_badge(name: str, label: str = None):
    label = label or name
    ok = CAPS.get(name)
    if ok:
        st.markdown(f"- :green[**{label}**]: available")
    else:
        st.markdown(f"- :gray[**{label}**]: unavailable ({privileges.capability_reason(name, CAPS)})")


# ---------------------------------------------------------------------------
# Header: server IP + check connectivity
# ---------------------------------------------------------------------------

def render_header():
    st.title("NPERF")

    col1, col2, col3, col4 = st.columns([3, 1, 1, 2])
    with col1:
        st.session_state.server_ip = st.text_input(
            "Server IP / hostname", value=st.session_state.server_ip,
            help="Independent of the MTR target below; persists across tabs.")
    with col2:
        st.session_state.ipv6 = st.checkbox("Use IPv6", value=st.session_state.ipv6,
                                            help="Use IPv6 for the selected client test instead of the default IPv4.")
        st.caption(f"Selected IP version: {'IPv6' if st.session_state.ipv6 else 'IPv4'}")
    with col3:
        st.write("")
        st.write("")
        check_clicked = st.button("Check Connectivity", use_container_width=True)
    with col4:
        st.write("")
        admin = CAPS["is_admin"]
        if admin:
            st.success(f"Elevated privileges: YES ({CAPS['platform']})")
        else:
            st.warning(f"Elevated privileges: NO ({CAPS['platform']}) -- some diagnostics disabled")

    if check_clicked:
        if not st.session_state.server_ip:
            st.error("Enter a server IP/hostname first.")
        else:
            with st.spinner("Checking connectivity..."):
                st.session_state.connectivity_result = connectivity.run_all(
                    st.session_state.server_ip, st.session_state.udp_port, st.session_state.tcp_port,
                    ipv6=st.session_state.ipv6)

    result = st.session_state.connectivity_result
    if result:
        c1, c2, c3 = st.columns(3)
        icmp, udp, tcp = result["icmp"], result["udp"], result["tcp"]
        c1.metric("ICMP", "OK" if icmp["success"] else "FAIL",
                   f"loss {icmp.get('loss_pct')}%" if icmp.get("loss_pct") is not None else None)
        c2.metric("UDP", "OK" if udp["success"] else "FAIL",
                   f"{udp['rtt_ms']:.2f} ms" if udp.get("rtt_ms") else udp.get("error"))
        c3.metric("TCP", "OK" if tcp["success"] else "FAIL",
                   f"{tcp['rtt_ms']:.2f} ms" if tcp.get("rtt_ms") else tcp.get("error"))
        st.caption(f"Connectivity address family: {result.get('ip_version', 'IPv4')}")

    with st.expander("Capabilities"):
        _cap_badge("mtr_ipv6_icmp" if st.session_state.ipv6 else "mtr", "MTR (ICMP/UDP/TCP hop discovery)")
        _cap_badge("pmtud_ipv6" if st.session_state.ipv6 else "pmtud", "PMTUD")
        _cap_badge("protocol_esp_ipv6" if st.session_state.ipv6 else "protocol_esp", "IPSEC (native ESP)")
        _cap_badge("protocol_gre_ipv6" if st.session_state.ipv6 else "protocol_gre", "GRE")
        st.caption("Basic ICMP/UDP/TCP checks and IPSEC (UDP over ESP) do not require elevated privileges.")

    st.divider()


# ---------------------------------------------------------------------------
# NPERF tab (throughput)
# ---------------------------------------------------------------------------

def _tp_worker(cfg, stop_event, live_list, result_holder):
    def on_interval(entry):
        live_list.append(entry)

    try:
        summary = run_throughput_test(cfg, on_interval=on_interval, stop_requested=stop_event)
        result_holder["summary"] = summary
    except Exception as exc:  # noqa: BLE001 - surface any error to the UI
        result_holder["error"] = str(exc)
    finally:
        result_holder["done"] = True


def _fb_worker(cfg, stop_event, progress, result_holder):
    try:
        result_holder["manifest"] = run_fullbench(
            cfg, on_progress=progress.append, stop_requested=stop_event)
    except Exception as exc:  # noqa: BLE001 - surface any error to the UI
        result_holder["error"] = str(exc)
    finally:
        result_holder["done"] = True


def render_throughput_tab():
    st.subheader("Throughput Test")
    if not st.session_state.server_ip:
        st.info("Enter a Server IP above to run a test.")
        return

    running = st.session_state.tp_running
    busy = running or st.session_state.fb_running
    if st.session_state.fb_running:
        st.info("A fullbench matrix is running. Stop it from the Fullbench view before starting a single test.")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        transport = st.radio("Transport", ["TCP", "UDP"], horizontal=True, disabled=busy, key="tp_transport")
    with col2:
        direction = st.radio("Direction", ["Upload", "Download"], horizontal=True, disabled=busy, key="tp_direction")
    with col3:
        streams = st.number_input("Parallel data streams (-P)", min_value=1, max_value=32, value=1,
                                  disabled=busy, key="tp_streams",
                                  help="One independent socket/data flow is created per stream. This is separate from worker processes.")
    with col4:
        duration = st.number_input("Duration (sec)", min_value=1, max_value=3600, value=10, disabled=busy,
                                   key="tp_duration")
    st.caption(f"This run will create {int(streams)} concurrent data stream(s).")

    default_start_port = st.session_state.udp_port if transport == "UDP" else st.session_state.tcp_port
    pcol1, pcol2 = st.columns(2)
    with pcol1:
        start_port = st.number_input("Server base port for this transport", min_value=1, max_value=65535,
                                      value=default_start_port, disabled=busy,
                                      key=f"tp_start_port_{transport.lower()}")
    with pcol2:
        procs = st.number_input("Worker processes (--procs)", min_value=1, max_value=64, value=1,
                                disabled=busy,
                                key="tp_procs",
                                help="Spread streams across CPU cores to reach higher rates. "
                                     "Requires more than one stream; the server mirrors this setting.")
    st.caption(f"Ready to start: {int(streams)} stream(s), {int(procs)} worker process(es).")

    tune_enabled = False
    acceptable_loss = 1.0
    if transport == "UDP":
        tcol1, tcol2 = st.columns(2)
        with tcol1:
            tune_enabled = st.checkbox("Tune to acceptable loss (AIMD ramp)", value=False,
                                       disabled=busy,
                                       help="Ramp the UDP send rate to find the max rate whose loss "
                                            "stays under the target. The Duration above becomes the "
                                            "max time budget for the search.")
        with tcol2:
            acceptable_loss = st.number_input("Acceptable loss (%)", min_value=0.0, max_value=100.0,
                                              value=1.0, step=0.1,
                                              disabled=busy or not tune_enabled,
                                              key="tp_acceptable_loss")
        if tune_enabled:
            st.caption(f"Tuning mode: 'Duration' ({int(duration)}s) is the max search budget; "
                       f"the result is the highest rate meeting <= {acceptable_loss:.2f}% loss.")

    bcol1, bcol2 = st.columns(2)
    start_clicked = bcol1.button("Start Test", type="primary", disabled=busy, use_container_width=True)
    stop_clicked = bcol2.button("Stop Test", disabled=not running, use_container_width=True)

    if start_clicked and not busy:
        launch_spec = {
            "transport": transport,
            "direction": direction,
            "streams": int(streams),
            "procs": int(procs),
            "start_port": int(start_port),
        }
        cfg = ThroughputClientConfig(
            server_ip=st.session_state.server_ip,
            control_port=st.session_state.control_port,
            transport="udp" if transport == "UDP" else "tcp",
            direction="down" if direction == "Download" else "up",
            streams=int(streams),
            duration_sec=float(duration),
            start_port=int(start_port),
            procs=int(procs),
            acceptable_loss_pct=(float(acceptable_loss)
                                 if (transport == "UDP" and tune_enabled) else None),
            max_duration_sec=float(duration),
            ipv6=st.session_state.ipv6,
        )
        stop_event = threading.Event()
        live_list = []
        result_holder = {"done": False, "summary": None, "error": None}
        th = threading.Thread(target=_tp_worker, args=(cfg, stop_event, live_list, result_holder), daemon=True)
        st.session_state.tp_stop_event = stop_event
        st.session_state.tp_live = live_list
        st.session_state.tp_result_holder = result_holder
        st.session_state.tp_thread = th
        st.session_state.tp_running = True
        st.session_state.tp_summary = None
        st.session_state.tp_error = None
        st.session_state.tp_launch_spec = launch_spec
        th.start()
        st.rerun()

    if stop_clicked and st.session_state.tp_stop_event:
        st.session_state.tp_stop_event.set()

    if st.session_state.tp_running:
        launch_spec = st.session_state.get("tp_launch_spec") or {}
        st.caption(f"Running: {launch_spec.get('transport')} {launch_spec.get('direction')} | "
                   f"streams={launch_spec.get('streams')} | procs={launch_spec.get('procs')} | "
                   f"base port={launch_spec.get('start_port')}")
        holder = st.session_state.get("tp_result_holder", {})
        # Snapshot the shared list: the background worker thread appends to it
        # concurrently, so build the DataFrame from a stable copy to avoid a
        # length-mismatch race in pandas.
        live = list(st.session_state.tp_live)

        if live:
            last = live[-1]
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Send", f"{last.get('send_mbps', 0):.2f} Mbps")
            m2.metric("Recv", f"{last.get('recv_mbps', 0):.2f} Mbps")
            m3.metric("Packets/s", f"{last.get('recv_pps', 0):,.0f}")
            rtt = last.get("rtt_ms")
            m4.metric("Loaded RTT", f"{rtt:.2f} ms" if rtt is not None else "n/a")

            df = pd.DataFrame(live)
            if "t" in df.columns:
                df = df.set_index("t")
                cols = [c for c in ["send_mbps", "recv_mbps"] if c in df.columns]
                if cols:
                    st.line_chart(df[cols])
                pps_cols = [c for c in ["send_pps", "recv_pps"] if c in df.columns]
                if pps_cols:
                    st.caption("Packets per second")
                    st.line_chart(df[pps_cols])

        if holder.get("done"):
            st.session_state.tp_running = False
            st.session_state.tp_summary = holder.get("summary")
            st.session_state.tp_error = holder.get("error")
            st.rerun()
        else:
            time.sleep(0.5)
            st.rerun()

    if st.session_state.tp_error:
        st.error(st.session_state.tp_error)

    summary = st.session_state.tp_summary
    if summary:
        st.success("Test complete")
        observed_streams = len(summary.get("per_stream") or [])
        configured_streams = summary.get("streams", observed_streams)
        st.caption(f"Address family: {summary.get('ip_version', 'IPv4')} | "
                   f"parallel streams: {configured_streams} configured, {observed_streams} observed")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Send", f"{summary['send_mbps']:.2f} Mbps")
        m2.metric("Recv", f"{summary['recv_mbps']:.2f} Mbps")
        if summary.get("loss_pct") is not None:
            m3.metric("UDP Loss", f"{summary['loss_pct']:.3f}%")
        elif summary.get("retransmits") is not None:
            m3.metric("TCP Retransmits", summary["retransmits"])
        bloat_factor = (summary.get("bufferbloat") or {}).get("bloat_factor")
        m4.metric("Bloat factor", f"{bloat_factor:.2f}" if bloat_factor is not None else "n/a")

        p1, p2 = st.columns(2)
        p1.metric("Send packets/s", f"{summary.get('send_pps', 0):,.0f}")
        p2.metric("Recv packets/s", f"{summary.get('recv_pps', 0):,.0f}")
        if summary.get("transport") == "tcp":
            st.caption(f"TCP packets/s is estimated from throughput assuming a "
                       f"{constants.TCP_MSS_ESTIMATE}-byte MSS (the byte stream has no "
                       "application-visible packet boundaries).")

        tuning = summary.get("tuning")
        if tuning:
            st.write("**UDP rate tuning (AIMD ramp)**")
            best = tuning.get("best") or {}
            probe = tuning.get("probe") or {}
            tc1, tc2, tc3 = st.columns(3)
            tc1.metric("Best sustainable goodput", f"{best.get('goodput_mbps', 0):,.0f} Mbps")
            tc2.metric("At loss", f"{best.get('loss_pct', 0):.2f}%")
            tc3.metric("Loss target", f"{tuning.get('acceptable_loss_pct', 0):.2f}%")
            rows = [{
                "target": "unpaced probe",
                "offered (Mbps)": round(probe.get("offered_mbps", 0)),
                "goodput (Mbps)": round(probe.get("goodput_mbps", 0)),
                "loss %": round(probe.get("loss_pct", 0), 2),
            }]
            for tr in tuning.get("trials", []):
                rows.append({
                    "target": f"{tr['target_mbps']:.0f} Mbps",
                    "offered (Mbps)": round(tr["offered_mbps"]),
                    "goodput (Mbps)": round(tr["goodput_mbps"]),
                    "loss %": round(tr["loss_pct"], 2),
                })
            st.table(pd.DataFrame(rows))
            if not best.get("target_mbps"):
                st.caption("Unpaced already met the loss target -- no rate limiting needed.")

        lr = summary.get("loaded_rtt") or {}
        st.write("**Latency (control-channel probe RTT)**")
        st.table(pd.DataFrame([{
            "Unloaded RTT (ms)": summary.get("unloaded_rtt_ms"),
            "Loaded Avg (ms)": lr.get("avg"),
            "Best (ms)": lr.get("best"),
            "Worst (ms)": lr.get("worst"),
            "StdDev": lr.get("stddev"),
            "Jitter": lr.get("jitter"),
        }]))

        bb = summary.get("bufferbloat") or {}
        st.write("**Bufferbloat**")
        st.table(pd.DataFrame([{
            "BDP (bytes)": bb.get("bdp_bytes"),
            "Buffered (bytes)": bb.get("buffered_bytes"),
            "Bloat factor": bb.get("bloat_factor"),
        }]))

        if summary.get("per_stream"):
            st.write("**Per-stream results**")
            st.dataframe(pd.DataFrame(summary["per_stream"]), use_container_width=True)

        st.caption(f"Log: {summary.get('log_json')}")


def render_fullbench_tab():
    st.subheader("Fullbench Matrix")
    if not st.session_state.server_ip:
        st.info("Enter a Server IP above to run a fullbench matrix.")
        return

    running = st.session_state.fb_running
    busy = running or st.session_state.tp_running
    if st.session_state.tp_running:
        st.info("A single throughput test is running. Stop it before starting fullbench.")

    col1, col2 = st.columns(2)
    with col1:
        transports = st.multiselect("Transports", ["TCP", "UDP"], default=["TCP", "UDP"],
                                    disabled=busy, key="fb_transports")
    with col2:
        directions = st.multiselect("Directions", ["Upload", "Download"],
                                    default=["Upload", "Download"], disabled=busy,
                                    key="fb_directions")

    streams = st.multiselect("Parallel data streams per test", [1, 8, 16, 32],
                             default=[1, 8, 16, 32], disabled=busy, key="fb_streams",
                             help="Each selected value is a separate test. Each test creates that many concurrent data flows.")
    st.caption(f"Matrix size: {len(transports) * len(directions) * len(streams)} test(s).")

    col1, col2 = st.columns(2)
    with col1:
        duration = st.number_input("Fixed test duration (sec)", min_value=1, max_value=3600,
                                   value=10, disabled=busy, key="fb_duration")
    with col2:
        procs = st.number_input("Worker processes per test", min_value=1, max_value=64, value=1,
                                disabled=busy, key="fb_procs",
                                help="This controls process distribution, not the number of data streams. Streams are concurrent even with one process.")

    tune_enabled = False
    acceptable_loss = 1.0
    udp_max_duration = 30
    if "UDP" in transports:
        col1, col2 = st.columns(2)
        with col1:
            tune_enabled = st.checkbox("Tune UDP to acceptable loss", disabled=busy, key="fb_tune")
        with col2:
            acceptable_loss = st.number_input("Acceptable UDP loss (%)", min_value=0.0, max_value=100.0,
                                              value=1.0, step=0.1,
                                              disabled=busy or not tune_enabled,
                                              key="fb_acceptable_loss")
        udp_max_duration = st.number_input("UDP tuning max duration (sec)", min_value=1, max_value=3600,
                                           value=30, disabled=busy or not tune_enabled,
                                           key="fb_udp_max_duration")

    col1, col2 = st.columns(2)
    start_clicked = col1.button("Start Fullbench", type="primary", disabled=busy,
                                use_container_width=True)
    stop_clicked = col2.button("Stop Fullbench", disabled=not running, use_container_width=True)

    if start_clicked and not busy:
        if not transports or not directions or not streams:
            st.error("Select at least one transport, direction, and stream count.")
        else:
            cfg = FullBenchConfig(
                server_ip=st.session_state.server_ip,
                control_port=st.session_state.control_port,
                transports=[value.lower() for value in transports],
                directions=[value.lower() for value in directions],
                streams=[int(value) for value in streams],
                procs=[int(procs)],
                duration_sec=float(duration),
                acceptable_loss_pct=(float(acceptable_loss) if tune_enabled else None),
                udp_max_duration_sec=float(udp_max_duration),
                udp_port=int(st.session_state.udp_port),
                tcp_port=int(st.session_state.tcp_port),
                ipv6=st.session_state.ipv6,
            )
            stop_event = threading.Event()
            progress = []
            result_holder = {"done": False, "manifest": None, "error": None}
            th = threading.Thread(target=_fb_worker,
                                  args=(cfg, stop_event, progress, result_holder), daemon=True)
            st.session_state.fb_stop_event = stop_event
            st.session_state.fb_progress = progress
            st.session_state.fb_result_holder = result_holder
            st.session_state.fb_thread = th
            st.session_state.fb_running = True
            st.session_state.fb_manifest = None
            st.session_state.fb_error = None
            th.start()
            st.rerun()

    if stop_clicked and st.session_state.fb_stop_event:
        st.session_state.fb_stop_event.set()

    if st.session_state.fb_running:
        holder = st.session_state.get("fb_result_holder") or {}
        progress = list(st.session_state.get("fb_progress") or [])
        st.info("Fullbench is running sequentially through the selected matrix; each individual test uses its configured parallel streams.")
        if progress:
            st.code("\n".join(progress[-20:]), language="text")
        if holder.get("done"):
            st.session_state.fb_running = False
            st.session_state.fb_manifest = holder.get("manifest")
            st.session_state.fb_error = holder.get("error")
            st.rerun()
        else:
            time.sleep(0.5)
            st.rerun()

    if st.session_state.fb_error:
        st.error(st.session_state.fb_error)

    manifest = st.session_state.fb_manifest
    if manifest:
        st.success(f"Fullbench complete: {manifest.get('completed', 0)}/{manifest.get('test_count', 0)} tests")
        st.caption(f"Benchmark ID: {manifest.get('benchmark_id')} | IP version: {manifest.get('ip_version', 'IPv4')}")
        rows = []
        for test in manifest.get("tests", []):
            row = {
                "Transport": test.get("transport", "").upper(),
                "Direction": test.get("direction", "").title(),
                "Streams": test.get("streams"),
                "Processes": test.get("procs"),
                "Status": "OK" if test.get("ok") else "FAILED",
                "Send Mbps": test.get("send_mbps"),
                "Recv Mbps": test.get("recv_mbps"),
                "Loss %": test.get("loss_pct"),
            }
            if not test.get("ok"):
                row["Error"] = test.get("error")
            rows.append(row)
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        st.caption(f"Manifest: {manifest.get('manifest_json')}")


# ---------------------------------------------------------------------------
# Diagnostics tab
# ---------------------------------------------------------------------------

def _mtr_worker(runner, stop_event, state_holder):
    def on_round(snapshot, round_num):
        state_holder["snapshot"] = snapshot
        state_holder["round"] = round_num

    try:
        runner.run(stop_event, on_round=on_round)
    except Exception as exc:  # noqa: BLE001
        state_holder["error"] = str(exc)
    finally:
        state_holder["done"] = True


def render_mtr_section():
    st.markdown("#### MTR")
    mtr_capability = "mtr_ipv6_icmp" if st.session_state.ipv6 else "mtr"
    if not CAPS.get(mtr_capability):
        st.warning(f"MTR unavailable: {privileges.capability_reason(mtr_capability, CAPS)}. "
                   "Run with elevated privileges (root/administrator) to enable.")

    running = st.session_state.mtr_running
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        target = st.text_input("Target IP or FQDN", value=st.session_state.get("mtr_target", ""), disabled=running)
    with col2:
        mtr_proto = st.selectbox("Protocol", ["icmp", "udp", "tcp"], disabled=running)
    with col3:
        mtr_port = st.number_input("TCP port", min_value=1, max_value=65535, value=33434,
                                    disabled=(mtr_proto != "tcp") or running)

    interval_ms = st.slider("Ping interval (ms)", min_value=constants.MTR_MIN_INTERVAL_MS,
                             max_value=constants.MTR_MAX_INTERVAL_MS,
                             value=constants.MTR_DEFAULT_INTERVAL_MS, step=50, disabled=running)

    bcol1, bcol2 = st.columns(2)
    start_disabled = running or not CAPS.get(mtr_capability) or not target
    start_clicked = bcol1.button("Start MTR", type="primary", disabled=start_disabled, use_container_width=True)
    stop_clicked = bcol2.button("Stop MTR", disabled=not running, use_container_width=True)

    if start_clicked:
        try:
            runner = MtrRunner(target=target, protocol=mtr_proto, port=int(mtr_port), interval_ms=int(interval_ms),
                               ipv6=st.session_state.ipv6)
        except (ValueError, MtrError) as exc:
            st.error(str(exc))
            runner = None
        if runner:
            stop_event = threading.Event()
            state_holder = {"snapshot": [], "round": 0, "done": False, "error": None}
            th = threading.Thread(target=_mtr_worker, args=(runner, stop_event, state_holder), daemon=True)
            st.session_state.mtr_stop_event = stop_event
            st.session_state.mtr_state_holder = state_holder
            st.session_state.mtr_thread = th
            st.session_state.mtr_running = True
            st.session_state.mtr_target = target
            st.session_state.mtr_target_ip = runner.target_ip
            st.session_state.mtr_ip_version = runner.ip_version
            st.session_state.mtr_error = None
            th.start()
            st.rerun()

    if stop_clicked and st.session_state.mtr_stop_event:
        st.session_state.mtr_stop_event.set()

    if st.session_state.mtr_running:
        holder = st.session_state.get("mtr_state_holder", {})
        st.session_state.mtr_snapshot = holder.get("snapshot", [])
        st.session_state.mtr_round = holder.get("round", 0)
        if holder.get("error"):
            st.session_state.mtr_error = holder["error"]
        if holder.get("done"):
            # Worker has stopped: flip state and rerun immediately so the
            # Start/Stop buttons (already drawn above with the stale "running"
            # value) redraw in the stopped state. Without this the user has to
            # click Stop a second time to re-enable Start.
            st.session_state.mtr_running = False
            st.rerun()

    if st.session_state.mtr_error:
        st.error(st.session_state.mtr_error)

    if st.session_state.mtr_snapshot:
        st.caption(f"Target: {st.session_state.mtr_target_ip}   IP version: {st.session_state.mtr_ip_version}   "
                   f"Round: {st.session_state.mtr_round}")
        df = pd.DataFrame(st.session_state.mtr_snapshot)
        display_cols = ["ttl", "ip", "hostname", "asn", "asn_description", "whois_org",
                         "loss_pct", "best", "avg", "worst", "stddev", "jitter"]
        display_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[display_cols], use_container_width=True)

        if not st.session_state.mtr_running and st.button("Save MTR log"):
            data = {"target": st.session_state.mtr_target, "target_ip": st.session_state.mtr_target_ip,
                    "ip_version": st.session_state.mtr_ip_version, "protocol": mtr_proto,
                    "hops": st.session_state.mtr_snapshot}
            text_lines = [f"MTR to {data['target']} ({data['target_ip']}) {data['ip_version']}"] + [str(h) for h in data["hops"]]
            json_path, log_path = logging_store.save_result(constants.TEST_TYPE_MTR, data, text_lines)
            st.success(f"Saved: {json_path}")

    if st.session_state.mtr_running:
        time.sleep(0.5)
        st.rerun()


def render_pmtud_section():
    st.markdown("#### PMTUD")
    pmtud_capability = "pmtud_ipv6" if st.session_state.ipv6 else "pmtud"
    if not CAPS.get(pmtud_capability):
        st.warning(f"PMTUD unavailable: {privileges.capability_reason(pmtud_capability, CAPS)}")
    if not st.session_state.server_ip:
        st.info("Enter a Server IP above.")
        return

    if st.button("Run PMTUD", disabled=not CAPS.get(pmtud_capability)):
        with st.spinner("Running PMTUD..."):
            try:
                st.session_state.pmtud_result = run_pmtud(
                    st.session_state.server_ip, udp_port=st.session_state.udp_port,
                    ipv6=st.session_state.ipv6)
            except PmtudError as exc:
                st.session_state.pmtud_result = {"success": False, "error": str(exc)}

    result = st.session_state.pmtud_result
    if result:
        if result.get("success"):
            st.success(f"Path MTU: {result['path_mtu']} bytes (payload {result.get('path_mtu_payload_bytes')} bytes)")
        else:
            st.error(result.get("error"))
        st.caption(f"Connectivity confirmed: {result.get('connectivity_confirmed')}")
        with st.expander("Raw result"):
            st.json(result)
        if st.button("Save PMTUD log"):
            text_lines = [f"PMTUD to {st.session_state.server_ip}", str(result)]
            json_path, log_path = logging_store.save_result(constants.TEST_TYPE_PMTUD, result, text_lines)
            st.success(f"Saved: {json_path}")


def render_protocols_section():
    st.markdown("#### Protocol Reachability")
    if st.session_state.tp_running or st.session_state.fb_running:
        st.info("Stop the active throughput test before running protocol checks.")
        return
    if not st.session_state.server_ip:
        st.info("Enter a Server IP above.")
        return

    if st.button("Run Protocol Checks"):
        with st.spinner("Testing protocols..."):
            try:
                st.session_state.protocols_result = protocols.run_all(
                    st.session_state.server_ip, st.session_state.udp_port, st.session_state.tcp_port,
                    ipv6=st.session_state.ipv6)
            except Exception as exc:  # noqa: BLE001 - keep diagnostics visible if a probe escapes its guard
                st.session_state.protocols_result = {
                    "server_ip": st.session_state.server_ip,
                    "success": False,
                    "error": str(exc),
                }

    result = st.session_state.protocols_result
    if result:
        if result.get("error") and not any(key in result for key in ("icmp", "udp", "tcp")):
            st.error(f"Protocol checks failed: {result['error']}")
        labels = {"icmp": "ICMP", "udp": "UDP", "tcp": "TCP", "esp_udp": "IPSEC (UDP over ESP)",
                  "esp_native": "IPSEC (native ESP)", "gre": "GRE"}
        rows = []
        for key, label in labels.items():
            r = result.get(key) or {}
            status = "OK" if r.get("success") else ("SKIPPED" if r.get("skipped") else "FAIL")
            error = r.get("error")
            rtt_ms = r.get("rtt_ms")
            detail = str(error) if error else (
                f"{rtt_ms:.2f} ms" if isinstance(rtt_ms, (int, float)) else "")
            rows.append({"Protocol": label, "Status": status, "Detail": detail})
        st.table(pd.DataFrame(rows))
        if st.button("Save protocols log"):
            try:
                text_lines = [f"Protocol check: {st.session_state.server_ip}", str(result)]
                json_path, log_path = logging_store.save_result(
                    constants.TEST_TYPE_PROTOCOLS, result, text_lines)
                st.success(f"Saved: {json_path}")
            except Exception as exc:  # noqa: BLE001 - surface local log errors without crashing the UI
                st.error(f"Could not save protocols log: {exc}")


def render_diagnostics_tab():
    st.subheader("Diagnostics")
    choice = st.radio("Diagnostic", ["MTR", "PMTUD", "Protocol Checks"], horizontal=True)
    st.divider()
    if choice == "MTR":
        render_mtr_section()
    elif choice == "PMTUD":
        render_pmtud_section()
    else:
        render_protocols_section()


# ---------------------------------------------------------------------------
# Settings tab
# ---------------------------------------------------------------------------

def render_settings_tab():
    st.subheader("Settings")
    st.session_state.udp_port = st.number_input(
        "UDP base port (connectivity check + throughput streams)",
        min_value=1, max_value=65535, value=st.session_state.udp_port)
    st.session_state.tcp_port = st.number_input(
        "TCP base port (connectivity check + throughput streams)",
        min_value=1, max_value=65535, value=st.session_state.tcp_port)
    st.session_state.control_port = st.number_input(
        "Control port (throughput test orchestration)",
        min_value=1, max_value=65535, value=st.session_state.control_port)
    st.caption("These must match the ports the server was started with, e.g.: "
               "`nperf server --udp-port ... --tcp-port ... --control-port ...`")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    render_header()
    # A radio-based selector (rather than st.tabs) guarantees the pages are
    # mutually exclusive: only the selected page's body is executed/rendered.
    # st.tabs renders every tab's body on each run and can visually bleed
    # across the auto-reruns used for live test polling.
    page = st.radio(
        "View",
        ["NPERF", "Fullbench", "Diagnostics", "Settings"],
        horizontal=True,
        key="active_page",
        label_visibility="collapsed",
    )
    st.divider()
    if page == "NPERF":
        render_throughput_tab()
    elif page == "Fullbench":
        render_fullbench_tab()
    elif page == "Diagnostics":
        render_diagnostics_tab()
    else:
        render_settings_tab()


main()
