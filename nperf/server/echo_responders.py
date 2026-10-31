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
import time

from ..common import constants, net
from ..diagnostics import packetutils as pkt
from ..transport import stream_worker, udp_batch


_SESSION_UNSET = object()
_TCP_PREAMBLE_TIMEOUT_SEC = 2.0
_TCP_BASE_PREAMBLE_HANDLER_LIMIT = 16
_tcp_base_preamble_slots = threading.BoundedSemaphore(
    _TCP_BASE_PREAMBLE_HANDLER_LIMIT)


def _record_udp_overflow(counters, overflow):
    if overflow is None:
        return
    try:
        counters.set_socket_metadata(rxq_overflow_supported=True)
        state = stream_worker._udp_receive_state(counters)
        counters.set_kernel_rxq_drops(state.observe_overflow(overflow))
    except Exception:
        pass


def _handle_udp_base_datagram(ctx, sock, data, from_addr, df_enabled,
                               overflow=None, session=_SESSION_UNSET,
                               defer_throughput=False,
                               socket_overflow_total=None,
                               socket_metadata=None):
    if len(data) >= 16 and data[:8] == constants.CONNECTIVITY_MAGIC:
        if not df_enabled:
            return
        try:
            sock.sendto(data, from_addr)
        except OSError as exc:
            ctx.log(f"UDP PMTUD reverse echo failed family={net.family_label(sock.family)} "
                    f"payload={len(data)} destination={from_addr}: {exc}")
        return

    if len(data) >= stream_worker.PREAMBLE_SIZE and data[:8] == constants.DATA_STREAM_MAGIC:
        preamble = stream_worker.parse_preamble(data)
        if preamble:
            _, run_id, idx = preamble
            if session is _SESSION_UNSET:
                session = ctx.get_session()
            if (session and session.run_id == run_id and idx == 0
                    and session.params.get("transport") == "udp"):
                try:
                    attached = session.try_attach_stream(
                        0, sock, addr=from_addr, spawn_receiver=False)
                except Exception as exc:
                    ctx.log(f"session {run_id[:8]}: udp stream 0 attach failed: {exc}")
                else:
                    if attached:
                        if isinstance(socket_metadata, dict):
                            session.counters[0].set_socket_metadata(**socket_metadata)
                        session.base_udp_overflow_baseline = int(socket_overflow_total or 0)
                        ctx.log(f"session {run_id[:8]}: udp stream 0 connected from {from_addr}")
        return

    if session is _SESSION_UNSET:
        session = ctx.get_session()
    if (session and session.params.get("transport") == "udp"
            and session.stream_addrs.get(0) == from_addr):
        if defer_throughput:
            session_drops = max(
                0, int(socket_overflow_total or 0)
                - int(getattr(session, "base_udp_overflow_baseline", 0)))
            return session.counters[0], data, session_drops
        _record_udp_overflow(session.counters[0], overflow)
        stream_worker.feed_udp_data_frame(
            session.counters[0], data,
            session_cookie=stream_worker.udp_session_cookie(session.run_id))
    return None


def udp_base_listener(ctx):
    sockets = net.open_server_sockets(ctx.config.bind, ctx.config.udp_port, socket.SOCK_DGRAM,
                                      log=ctx.log, service="UDP base")
    if not sockets:
        ctx.log(f"UDP base: no IPv4 or IPv6 listener available on port {ctx.config.udp_port}")
        return
    df_enabled = {}
    receive_buffers = {}
    batched = {}
    overflow_states = {}
    socket_metadata = {}
    for sock in sockets:
        try:
            net.configure_dont_fragment(sock)
            df_enabled[sock] = True
            ctx.log(f"UDP PMTUD echo {net.family_label(sock.family)} DF enabled")
        except OSError as exc:
            df_enabled[sock] = False
            ctx.log(f"UDP PMTUD echo {net.family_label(sock.family)} DF unavailable: {exc}")
        receive_buffers[sock] = [
            bytearray(65535) for _ in range(stream_worker.udp_io_batch_size())]
        socket_metadata[sock] = stream_worker.configure_udp_socket(sock)
        overflow_states[sock] = stream_worker._UdpReceiveState()
        try:
            sock.setblocking(False)
            batched[sock] = True
        except (AttributeError, OSError):
            batched[sock] = False
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
                if batched[sock]:
                    received = udp_batch.recvmmsg_into(
                        sock, receive_buffers[sock][:stream_worker.udp_io_batch_size()])
                else:
                    data, from_addr = sock.recvfrom(constants.BUFSZ)
                    received = [(len(data), from_addr, 0, None)]
                    receive_buffers[sock][0][:len(data)] = data
            except BlockingIOError:
                continue
            except OSError as exc:
                ctx.log(f"UDP base {net.family_label(sock.family)} receive error: {exc}")
                continue
            session = ctx.get_session()
            throughput_frames = []
            throughput_overflows = []
            throughput_counters = None
            for buffer, metadata in zip(receive_buffers[sock], received):
                length, from_addr, message_flags, overflow = metadata
                socket_overflow_total = overflow_states[sock].observe_overflow(overflow)
                if message_flags & getattr(socket, "MSG_TRUNC", 0):
                    if session is not None:
                        session.counters[0].set_error("truncated UDP datagram")
                    continue
                data = memoryview(buffer)[:int(length)]
                deferred = _handle_udp_base_datagram(
                    ctx, sock, data, from_addr, df_enabled.get(sock, False),
                    overflow=overflow, session=session, defer_throughput=True,
                    socket_overflow_total=socket_overflow_total,
                    socket_metadata=socket_metadata.get(sock))
                if deferred is not None:
                    throughput_counters, frame, frame_overflow = deferred
                    throughput_frames.append(frame)
                    throughput_overflows.append(frame_overflow)
            if throughput_counters is not None:
                stream_worker.feed_udp_data_frames(
                    throughput_counters, throughput_frames,
                    session_cookie=stream_worker.udp_session_cookie(session.run_id))
                _record_udp_overflow(
                    throughput_counters, max(throughput_overflows or [0]))

    for sock in sockets:
        try:
            sock.close()
        except OSError:
            pass


def _handle_tcp_base_connection(ctx, conn, addr):
    deadline = time.monotonic() + min(
        constants.SOCKET_TIMEOUT_SEC,
        _TCP_PREAMBLE_TIMEOUT_SEC)
    stop_event = getattr(ctx, "stop_event", None)
    cancelled = stop_event.is_set if stop_event is not None else lambda: False
    handed_off = False
    try:
        prefix = stream_worker.recv_exact(
            conn, len(constants.DATA_STREAM_MAGIC), deadline,
            cancelled=cancelled)
        if prefix is None:
            return

        if prefix == constants.CONNECTIVITY_MAGIC:
            suffix = stream_worker.recv_exact(
                conn, 8, deadline, cancelled=cancelled)
            if suffix is not None:
                conn.sendall(prefix + suffix)
            return

        if prefix != constants.DATA_STREAM_MAGIC:
            return
        suffix = stream_worker.recv_exact(
            conn, stream_worker.PREAMBLE_SIZE - len(prefix), deadline,
            cancelled=cancelled)
        if suffix is None:
            return
        preamble_bytes = prefix + suffix
        preamble = stream_worker.parse_preamble(preamble_bytes)
        if preamble:
            _, run_id, idx = preamble
            session = ctx.get_session()
            if (session and session.run_id == run_id and idx == 0
                    and session.params.get("transport") == "tcp"):
                try:
                    handed_off = session.try_attach_stream(
                        0, conn, addr=addr, spawn_receiver=True)
                except Exception as exc:
                    ctx.log(f"session {run_id[:8]}: tcp stream 0 attach failed: {exc}")
                if handed_off:
                    ctx.log(f"session {run_id[:8]}: tcp stream 0 connected from {addr}")
                    return
    except OSError:
        pass
    finally:
        if not handed_off:
            try:
                conn.close()
            except OSError:
                pass


def _run_tcp_base_preamble_handler(ctx, conn, addr, slots):
    try:
        _handle_tcp_base_connection(ctx, conn, addr)
    finally:
        slots.release()


def tcp_base_listener(ctx):
    sockets = net.open_server_sockets(ctx.config.bind, ctx.config.tcp_port, socket.SOCK_STREAM,
                                      listener=True, log=ctx.log, service="TCP base")
    if not sockets:
        ctx.log(f"TCP base: no IPv4 or IPv6 listener available on port {ctx.config.tcp_port}")
        return
    ctx.log(f"TCP echo/data listener families={','.join(net.family_label(sock.family) for sock in sockets)} "
            f"port={ctx.config.tcp_port}")

    try:
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
                slots = _tcp_base_preamble_slots
                if not slots.acquire(blocking=False):
                    conn.close()
                    continue
                try:
                    threading.Thread(
                        target=_run_tcp_base_preamble_handler,
                        args=(ctx, conn, addr, slots), daemon=True).start()
                except Exception:
                    slots.release()
                    conn.close()
    finally:
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
