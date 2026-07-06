"""NetworkBenchmark Streamlit web UI (client-side test runner).

Launch via: `networkbenchmark webui` (wraps `streamlit run app.py`).

The web UI always acts as the *client*: it drives throughput tests and
diagnostics against a remote NetworkBenchmark server (started separately
via `networkbenchmark server` on the target host).
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

from networkbenchmark.common import constants, privileges, addr as addr_mod, logging_store
from networkbenchmark.client.engine import ThroughputClientConfig, run_throughput_test
from networkbenchmark.diagnostics.mtr import MtrRunner, MtrError
from networkbenchmark.diagnostics import connectivity, protocols
from networkbenchmark.diagnostics.pmtud import run_pmtud, PmtudError


st.set_page_config(page_title="NetworkBenchmark", layout="wide")


# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------

def _init_state():
    defaults = {
        "server_ip": "",
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
        "mtr_running": False,
        "mtr_stop_event": None,
        "mtr_thread": None,
        "mtr_snapshot": [],
        "mtr_round": 0,
        "mtr_target_ip": None,
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
    st.title("NetworkBenchmark")

    col1, col2, col3 = st.columns([3, 1, 2])
    with col1:
        st.session_state.server_ip = st.text_input(
            "Server IP / hostname", value=st.session_state.server_ip,
            help="Independent of the MTR target below; persists across tabs.")
    with col2:
        st.write("")
        st.write("")
        check_clicked = st.button("Check Connectivity", use_container_width=True)
    with col3:
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
                    st.session_state.server_ip, st.session_state.udp_port, st.session_state.tcp_port)

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

    with st.expander("Capabilities"):
        _cap_badge("mtr", "MTR (ICMP/UDP/TCP hop discovery)")
        _cap_badge("pmtud", "PMTUD")
        _cap_badge("protocol_esp", "IPSEC (native ESP)")
        _cap_badge("protocol_gre", "GRE")
        st.caption("Basic ICMP/UDP/TCP checks and IPSEC (UDP over ESP) do not require elevated privileges.")

    st.divider()


# ---------------------------------------------------------------------------
# NetworkBenchmark tab (throughput)
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


def render_throughput_tab():
    st.subheader("Throughput Test")
    if not st.session_state.server_ip:
        st.info("Enter a Server IP above to run a test.")
        return

    running = st.session_state.tp_running
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        transport = st.radio("Transport", ["TCP", "UDP"], horizontal=True, disabled=running, key="tp_transport")
    with col2:
        direction = st.radio("Direction", ["Upload", "Download"], horizontal=True, disabled=running, key="tp_direction")
    with col3:
        streams = st.number_input("Parallel streams (-P)", min_value=1, max_value=32, value=1, disabled=running)
    with col4:
        duration = st.number_input("Duration (sec)", min_value=1, max_value=3600, value=10, disabled=running)

    default_start_port = st.session_state.udp_port if transport == "UDP" else st.session_state.tcp_port
    start_port = st.number_input("Server base port for this transport", min_value=1, max_value=65535,
                                  value=default_start_port, disabled=running)

    bcol1, bcol2 = st.columns(2)
    start_clicked = bcol1.button("Start Test", type="primary", disabled=running, use_container_width=True)
    stop_clicked = bcol2.button("Stop Test", disabled=not running, use_container_width=True)

    if start_clicked and not running:
        cfg = ThroughputClientConfig(
            server_ip=st.session_state.server_ip,
            control_port=st.session_state.control_port,
            transport="udp" if transport == "UDP" else "tcp",
            direction="down" if direction == "Download" else "up",
            streams=int(streams),
            duration_sec=float(duration),
            start_port=int(start_port),
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
        th.start()
        st.rerun()

    if stop_clicked and st.session_state.tp_stop_event:
        st.session_state.tp_stop_event.set()

    if st.session_state.tp_running:
        holder = st.session_state.get("tp_result_holder", {})
        live = st.session_state.tp_live

        if live:
            last = live[-1]
            m1, m2, m3 = st.columns(3)
            m1.metric("Send", f"{last.get('send_mbps', 0):.2f} Mbps")
            m2.metric("Recv", f"{last.get('recv_mbps', 0):.2f} Mbps")
            rtt = last.get("rtt_ms")
            m3.metric("Loaded RTT", f"{rtt:.2f} ms" if rtt is not None else "n/a")

            df = pd.DataFrame(live)
            if "t" in df.columns:
                df = df.set_index("t")
                cols = [c for c in ["send_mbps", "recv_mbps"] if c in df.columns]
                if cols:
                    st.line_chart(df[cols])

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
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Send", f"{summary['send_mbps']:.2f} Mbps")
        m2.metric("Recv", f"{summary['recv_mbps']:.2f} Mbps")
        if summary.get("loss_pct") is not None:
            m3.metric("UDP Loss", f"{summary['loss_pct']:.3f}%")
        elif summary.get("retransmits") is not None:
            m3.metric("TCP Retransmits", summary["retransmits"])
        bloat_factor = (summary.get("bufferbloat") or {}).get("bloat_factor")
        m4.metric("Bloat factor", f"{bloat_factor:.2f}" if bloat_factor is not None else "n/a")

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
    if not CAPS.get("mtr"):
        st.warning(f"MTR unavailable: {privileges.capability_reason('mtr', CAPS)}. "
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
    start_disabled = running or not CAPS.get("mtr") or not target
    start_clicked = bcol1.button("Start MTR", type="primary", disabled=start_disabled, use_container_width=True)
    stop_clicked = bcol2.button("Stop MTR", disabled=not running, use_container_width=True)

    if start_clicked:
        try:
            runner = MtrRunner(target=target, protocol=mtr_proto, port=int(mtr_port), interval_ms=int(interval_ms))
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
            st.session_state.mtr_running = False

    if st.session_state.mtr_error:
        st.error(st.session_state.mtr_error)

    if st.session_state.mtr_snapshot:
        st.caption(f"Target: {st.session_state.mtr_target_ip}   Round: {st.session_state.mtr_round}")
        df = pd.DataFrame(st.session_state.mtr_snapshot)
        display_cols = ["ttl", "ip", "hostname", "asn", "asn_description", "whois_org",
                         "loss_pct", "best", "avg", "worst", "stddev", "jitter"]
        display_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[display_cols], use_container_width=True)

        if not st.session_state.mtr_running and st.button("Save MTR log"):
            data = {"target": st.session_state.mtr_target, "target_ip": st.session_state.mtr_target_ip,
                    "protocol": mtr_proto, "hops": st.session_state.mtr_snapshot}
            text_lines = [f"MTR to {data['target']} ({data['target_ip']})"] + [str(h) for h in data["hops"]]
            json_path, log_path = logging_store.save_result(constants.TEST_TYPE_MTR, data, text_lines)
            st.success(f"Saved: {json_path}")

    if st.session_state.mtr_running:
        time.sleep(0.5)
        st.rerun()


def render_pmtud_section():
    st.markdown("#### PMTUD")
    if not CAPS.get("pmtud"):
        st.warning(f"PMTUD unavailable: {privileges.capability_reason('pmtud', CAPS)}")
    if not st.session_state.server_ip:
        st.info("Enter a Server IP above.")
        return

    if st.button("Run PMTUD", disabled=not CAPS.get("pmtud")):
        with st.spinner("Running PMTUD..."):
            try:
                st.session_state.pmtud_result = run_pmtud(
                    st.session_state.server_ip, udp_port=st.session_state.udp_port)
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
    if not st.session_state.server_ip:
        st.info("Enter a Server IP above.")
        return

    if st.button("Run Protocol Checks"):
        with st.spinner("Testing protocols..."):
            st.session_state.protocols_result = protocols.run_all(
                st.session_state.server_ip, st.session_state.udp_port, st.session_state.tcp_port)

    result = st.session_state.protocols_result
    if result:
        labels = {"icmp": "ICMP", "udp": "UDP", "tcp": "TCP", "esp_udp": "IPSEC (UDP over ESP)",
                  "esp_native": "IPSEC (native ESP)", "gre": "GRE"}
        rows = []
        for key, label in labels.items():
            r = result.get(key, {})
            status = "OK" if r.get("success") else ("SKIPPED" if r.get("skipped") else "FAIL")
            detail = r.get("error") or (f"{r['rtt_ms']:.2f} ms" if r.get("rtt_ms") else "")
            rows.append({"Protocol": label, "Status": status, "Detail": detail})
        st.table(pd.DataFrame(rows))
        if st.button("Save protocols log"):
            text_lines = [f"Protocol check: {st.session_state.server_ip}", str(result)]
            json_path, log_path = logging_store.save_result(constants.TEST_TYPE_PROTOCOLS, result, text_lines)
            st.success(f"Saved: {json_path}")


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
               "`networkbenchmark server --udp-port ... --tcp-port ... --control-port ...`")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    render_header()
    tab1, tab2, tab3 = st.tabs(["NetworkBenchmark", "Diagnostics", "Settings"])
    with tab1:
        render_throughput_tab()
    with tab2:
        render_diagnostics_tab()
    with tab3:
        render_settings_tab()


main()
