"""Server mode entrypoint: starts the control listener plus all always-on
echo responders (connectivity, NAT-T, ESP/GRE), and dispatches accepted
control connections to transport.throughput_session.
"""

import select
import socket
import threading

from ..common import net, privileges
from . import echo_responders
from .state import ServerConfig, ServerContext
from .throughput_session import handle_control_connection

__all__ = ["ServerConfig", "run_server"]


def _control_listener(ctx: ServerContext):
    sockets = net.open_server_sockets(ctx.config.bind, ctx.config.control_port, socket.SOCK_STREAM,
                                      listener=True, log=ctx.log, service="control")
    if not sockets:
        ctx.log(f"control: no IPv4 or IPv6 listener available on port {ctx.config.control_port}")
        return
    ctx.log(f"control listener families={','.join(net.family_label(sock.family) for sock in sockets)} "
            f"port={ctx.config.control_port}")

    while not ctx.stop_event.is_set():
        try:
            readable, _, _ = select.select(sockets, [], [], 0.5)
        except (OSError, ValueError) as exc:
            ctx.log(f"control listener error: {exc}")
            break
        for listener in readable:
            try:
                conn, addr = listener.accept()
            except OSError as exc:
                ctx.log(f"control {net.family_label(listener.family)} accept error: {exc}")
                continue
            ctx.log(f"control connection from {addr}")
            threading.Thread(target=handle_control_connection, args=(ctx, conn, addr), daemon=True).start()

    for sock in sockets:
        try:
            sock.close()
        except OSError:
            pass


def run_server(config: ServerConfig, stop_event: threading.Event = None, on_log=None, block: bool = True):
    """Start the NPERF server. If block=True (CLI usage), this
    call blocks until stop_event is set (e.g. via Ctrl+C handling by the
    caller) or KeyboardInterrupt. If block=False (e.g. webui background
    thread), threads are started and this function returns immediately;
    the caller is responsible for eventually setting stop_event.
    """
    caps = privileges.probe_capabilities()
    ctx = ServerContext(config, caps, on_log=on_log)
    if stop_event is not None:
        ctx.stop_event = stop_event

    ctx.log(f"capabilities: is_admin={caps['is_admin']} icmp_raw={caps['icmp_raw']} "
            f"icmpv6_raw={caps.get('icmpv6_raw', False)} esp_raw={caps['esp_raw']} "
            f"espv6_raw={caps.get('espv6_raw', False)} gre_raw={caps['gre_raw']} "
            f"grev6_raw={caps.get('grev6_raw', False)}")

    threads = [
        threading.Thread(target=_control_listener, args=(ctx,), daemon=True),
        threading.Thread(target=echo_responders.udp_base_listener, args=(ctx,), daemon=True),
        threading.Thread(target=echo_responders.tcp_base_listener, args=(ctx,), daemon=True),
        threading.Thread(target=echo_responders.natt_udp_listener, args=(ctx,), daemon=True),
    ]

    if caps.get("protocol_esp") or caps.get("protocol_esp_ipv6"):
        threads.append(threading.Thread(target=echo_responders.esp_native_listener, args=(ctx,), daemon=True))
    else:
        ctx.log(f"ESP native responder disabled: {privileges.capability_reason('protocol_esp', caps)}")

    if caps.get("protocol_gre") or caps.get("protocol_gre_ipv6"):
        threads.append(threading.Thread(target=echo_responders.gre_listener, args=(ctx,), daemon=True))
    else:
        ctx.log(f"GRE responder disabled: {privileges.capability_reason('protocol_gre', caps)}")

    for t in threads:
        t.start()

    if not block:
        return ctx, threads

    try:
        while not ctx.stop_event.is_set():
            ctx.stop_event.wait(0.5)
    except KeyboardInterrupt:
        ctx.log("shutting down (KeyboardInterrupt)")
        ctx.stop_event.set()

    return ctx, threads
