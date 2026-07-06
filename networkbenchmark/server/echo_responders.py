"""Always-on server responders.

- UDP base port (default 10000): answers "check connectivity" / PMTUD echo
  probes, and demultiplexes throughput data-stream preambles for stream
  index 0 of an armed session (see transport.stream_worker for framing).
- TCP base port (default 11000): same demux, connection-oriented.
- UDP/4500 (NAT-T): plain echo for the "IPSEC (UDP over ESP)" protocol
  check -- no elevated privileges required.
- Raw IP protocol 50 (ESP) / 47 (GRE): echo responders for the native
  IPSEC / GRE protocol reachability checks. Only started if the process
  has raw-socket privileges; otherwise skipped (feature greyed out
  client-side via capability probing).
"""

import socket
import threading

from ..common import constants
from ..diagnostics import packetutils as pkt
from ..transport import stream_worker


def udp_base_listener(ctx):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((ctx.config.bind, ctx.config.udp_port))
    sock.settimeout(0.5)
    ctx.log(f"UDP echo/data listener on {ctx.config.bind}:{ctx.config.udp_port}")

    while not ctx.stop_event.is_set():
        try:
            data, from_addr = sock.recvfrom(constants.BUFSZ)
        except socket.timeout:
            continue
        except OSError as exc:
            ctx.log(f"udp_base_listener error: {exc}")
            break

        if len(data) >= 16 and data[:8] == constants.CONNECTIVITY_MAGIC:
            try:
                sock.sendto(data, from_addr)
            except OSError:
                pass
            continue

        if len(data) >= stream_worker.PREAMBLE_SIZE and data[:8] == constants.DATA_STREAM_MAGIC:
            preamble = stream_worker.parse_preamble(data)
            if preamble:
                _, run_id, idx = preamble
                session = ctx.get_session()
                if session and session.run_id == run_id and idx == 0:
                    session.attach_stream(0, sock, addr=from_addr, spawn_receiver=False)
                    ctx.log(f"session {run_id[:8]}: udp stream 0 connected from {from_addr}")
            continue

        # possible inline data frame for an established up-direction stream-0 flow
        session = ctx.get_session()
        if (session and session.params.get("transport") == "udp"
                and session.stream_addrs.get(0) == from_addr):
            stream_worker.feed_udp_data_frame(session.counters[0], data)

    sock.close()


def _handle_tcp_base_connection(ctx, conn, addr):
    conn.settimeout(5.0)
    try:
        preamble_bytes = conn.recv(stream_worker.PREAMBLE_SIZE)
    except OSError:
        conn.close()
        return

    if len(preamble_bytes) >= 16 and preamble_bytes[:8] == constants.CONNECTIVITY_MAGIC:
        try:
            conn.sendall(preamble_bytes)
        except OSError:
            pass
        conn.close()
        return

    if len(preamble_bytes) == stream_worker.PREAMBLE_SIZE and preamble_bytes[:8] == constants.DATA_STREAM_MAGIC:
        preamble = stream_worker.parse_preamble(preamble_bytes)
        if preamble:
            _, run_id, idx = preamble
            session = ctx.get_session()
            if session and session.run_id == run_id and idx == 0:
                session.attach_stream(0, conn, addr=addr, spawn_receiver=True)
                ctx.log(f"session {run_id[:8]}: tcp stream 0 connected from {addr}")
                return

    conn.close()


def tcp_base_listener(ctx):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((ctx.config.bind, ctx.config.tcp_port))
    sock.listen(64)
    sock.settimeout(0.5)
    ctx.log(f"TCP echo/data listener on {ctx.config.bind}:{ctx.config.tcp_port}")

    while not ctx.stop_event.is_set():
        try:
            conn, addr = sock.accept()
        except socket.timeout:
            continue
        except OSError as exc:
            ctx.log(f"tcp_base_listener error: {exc}")
            break
        threading.Thread(target=_handle_tcp_base_connection, args=(ctx, conn, addr), daemon=True).start()

    sock.close()


def natt_udp_listener(ctx):
    """UDP/4500 echo for the IPSEC (UDP over ESP / NAT-T) protocol check."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((ctx.config.bind, constants.IPSEC_NATT_PORT))
    except OSError as exc:
        ctx.log(f"NAT-T UDP/4500 listener disabled (bind failed: {exc})")
        return
    sock.settimeout(0.5)
    ctx.log(f"NAT-T (UDP/4500) echo listener on {ctx.config.bind}:{constants.IPSEC_NATT_PORT}")

    while not ctx.stop_event.is_set():
        try:
            data, from_addr = sock.recvfrom(constants.BUFSZ)
        except socket.timeout:
            continue
        except OSError:
            break
        if len(data) >= 16 and data[:8] == constants.CONNECTIVITY_MAGIC:
            try:
                sock.sendto(data, from_addr)
            except OSError:
                pass

    sock.close()


def _raw_proto_echo_listener(ctx, proto: int, label: str):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, proto)
    except (PermissionError, OSError) as exc:
        ctx.log(f"{label} responder disabled (raw socket unavailable: {exc})")
        return
    sock.settimeout(0.5)
    ctx.log(f"{label} (protocol {proto}) raw echo responder active")

    while not ctx.stop_event.is_set():
        try:
            raw, from_addr = sock.recvfrom(65535)
        except socket.timeout:
            continue
        except OSError:
            break

        ip = pkt.parse_ip_header(raw)
        if not ip:
            continue
        body = raw[ip["ihl"]:]
        if len(body) >= 16 and body[:8] == constants.CONNECTIVITY_MAGIC:
            try:
                sock.sendto(body, (ip["src"], 0))
            except OSError:
                pass

    sock.close()


def esp_native_listener(ctx):
    _raw_proto_echo_listener(ctx, constants.IPPROTO_ESP, "ESP")


def gre_listener(ctx):
    _raw_proto_echo_listener(ctx, constants.IPPROTO_GRE, "GRE")
