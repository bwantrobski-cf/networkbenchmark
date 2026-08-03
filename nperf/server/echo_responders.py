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

import select
import socket
import threading

from ..common import constants, net
from ..diagnostics import packetutils as pkt
from ..transport import stream_worker


def udp_base_listener(ctx):
    sockets = net.open_server_sockets(ctx.config.bind, ctx.config.udp_port, socket.SOCK_DGRAM,
                                      log=ctx.log, service="UDP base")
    if not sockets:
        ctx.log(f"UDP base: no IPv4 or IPv6 listener available on port {ctx.config.udp_port}")
        return
    df_enabled = {}
    for sock in sockets:
        try:
            net.configure_dont_fragment(sock)
            df_enabled[sock] = True
            ctx.log(f"UDP PMTUD echo {net.family_label(sock.family)} DF enabled")
        except OSError as exc:
            df_enabled[sock] = False
            ctx.log(f"UDP PMTUD echo {net.family_label(sock.family)} DF unavailable: {exc}")
    ctx.log(f"UDP echo/data listener families={','.join(net.family_label(sock.family) for sock in sockets)} "
            f"port={ctx.config.udp_port}")

    while not ctx.stop_event.is_set():
        try:
            readable, _, _ = select.select(sockets, [], [], 0.5)
        except (OSError, ValueError) as exc:
            ctx.log(f"udp_base_listener error: {exc}")
            break
        for sock in readable:
            try:
                data, from_addr = sock.recvfrom(constants.BUFSZ)
            except OSError as exc:
                ctx.log(f"UDP base {net.family_label(sock.family)} receive error: {exc}")
                continue

            if len(data) >= 16 and data[:8] == constants.CONNECTIVITY_MAGIC:
                if not df_enabled.get(sock, False):
                    continue
                try:
                    sock.sendto(data, from_addr)
                except OSError as exc:
                    ctx.log(f"UDP PMTUD reverse echo failed family={net.family_label(sock.family)} "
                            f"payload={len(data)} destination={from_addr}: {exc}")
                continue

            if len(data) >= stream_worker.PREAMBLE_SIZE and data[:8] == constants.DATA_STREAM_MAGIC:
                preamble = stream_worker.parse_preamble(data)
                if preamble:
                    _, run_id, idx = preamble
                    session = ctx.get_session()
                    if (session and session.run_id == run_id and idx == 0
                            and not session.stream_ready[0].is_set()):
                        session.attach_stream(0, sock, addr=from_addr, spawn_receiver=False)
                        ctx.log(f"session {run_id[:8]}: udp stream 0 connected from {from_addr}")
                continue

            session = ctx.get_session()
            if (session and session.params.get("transport") == "udp"
                    and session.stream_addrs.get(0) == from_addr):
                stream_worker.feed_udp_data_frame(session.counters[0], data)

    for sock in sockets:
        try:
            sock.close()
        except OSError:
            pass


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
    sockets = net.open_server_sockets(ctx.config.bind, ctx.config.tcp_port, socket.SOCK_STREAM,
                                      listener=True, log=ctx.log, service="TCP base")
    if not sockets:
        ctx.log(f"TCP base: no IPv4 or IPv6 listener available on port {ctx.config.tcp_port}")
        return
    ctx.log(f"TCP echo/data listener families={','.join(net.family_label(sock.family) for sock in sockets)} "
            f"port={ctx.config.tcp_port}")

    while not ctx.stop_event.is_set():
        try:
            readable, _, _ = select.select(sockets, [], [], 0.5)
        except (OSError, ValueError) as exc:
            ctx.log(f"tcp_base_listener error: {exc}")
            break
        for listener in readable:
            try:
                conn, addr = listener.accept()
            except OSError as exc:
                ctx.log(f"TCP base {net.family_label(listener.family)} accept error: {exc}")
                continue
            threading.Thread(target=_handle_tcp_base_connection, args=(ctx, conn, addr), daemon=True).start()

    for sock in sockets:
        try:
            sock.close()
        except OSError:
            pass


def natt_udp_listener(ctx):
    """UDP/4500 echo for the IPSEC (UDP over ESP / NAT-T) protocol check."""
    sockets = net.open_server_sockets(ctx.config.bind, constants.IPSEC_NATT_PORT, socket.SOCK_DGRAM,
                                      log=ctx.log, service="NAT-T")
    if not sockets:
        ctx.log("NAT-T UDP/4500 listener unavailable for both IPv4 and IPv6")
        return
    ctx.log("NAT-T (UDP/4500) echo listener families=" +
            ",".join(net.family_label(sock.family) for sock in sockets))

    while not ctx.stop_event.is_set():
        try:
            readable, _, _ = select.select(sockets, [], [], 0.5)
        except (OSError, ValueError) as exc:
            ctx.log(f"NAT-T listener error: {exc}")
            break
        for sock in readable:
            try:
                data, from_addr = sock.recvfrom(constants.BUFSZ)
            except OSError:
                continue
            if len(data) >= 16 and data[:8] == constants.CONNECTIVITY_MAGIC:
                try:
                    sock.sendto(data, from_addr)
                except OSError:
                    pass

    for sock in sockets:
        try:
            sock.close()
        except OSError:
            pass


def _raw_proto_echo_listener(ctx, proto: int, label: str, magic_offset: int = 0):
    sockets = []
    for family in (socket.AF_INET, socket.AF_INET6):
        try:
            sock = socket.socket(family, socket.SOCK_RAW, proto)
            sock.settimeout(0.5)
            sockets.append(sock)
        except (PermissionError, OSError) as exc:
            ctx.log(f"{label} {net.family_label(family)} responder unavailable: {exc}")
    if not sockets:
        ctx.log(f"{label} responder disabled for IPv4 and IPv6")
        return
    ctx.log(f"{label} (protocol {proto}) raw echo families=" +
            ",".join(net.family_label(sock.family) for sock in sockets))

    magic = constants.CONNECTIVITY_MAGIC
    end = magic_offset + len(magic)

    while not ctx.stop_event.is_set():
        try:
            readable, _, _ = select.select(sockets, [], [], 0.5)
        except (OSError, ValueError):
            break
        for sock in readable:
            try:
                raw, _from_addr = sock.recvfrom(65535)
            except OSError:
                continue

            ip = pkt.parse_ip_header(raw, family=sock.family)
            if not ip:
                continue
            body = raw[ip["header_len"]:]
            if len(body) >= end + 8 and body[magic_offset:end] == magic:
                try:
                    sock.sendto(body, net.sockaddr(ip["src"], 0, sock.family))
                except OSError:
                    pass

    for sock in sockets:
        try:
            sock.close()
        except OSError:
            pass


def esp_native_listener(ctx):
    _raw_proto_echo_listener(ctx, constants.IPPROTO_ESP, "ESP")


def gre_listener(ctx):
    # GRE probes carry a standard GRE version-0 header before the magic.
    _raw_proto_echo_listener(ctx, constants.IPPROTO_GRE, "GRE",
                             magic_offset=constants.GRE_V0_HEADER_LEN)
