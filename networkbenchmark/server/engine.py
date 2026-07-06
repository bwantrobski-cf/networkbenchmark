"""Server mode entrypoint: starts the control listener plus all always-on
echo responders (connectivity, NAT-T, ESP/GRE), and dispatches accepted
control connections to transport.throughput_session.
"""

import socket
import threading

from ..common import privileges
from . import echo_responders
from .state import ServerConfig, ServerContext
from .throughput_session import handle_control_connection

__all__ = ["ServerConfig", "run_server"]


def _control_listener(ctx: ServerContext):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((ctx.config.bind, ctx.config.control_port))
    sock.listen(32)
    sock.settimeout(0.5)
    ctx.log(f"control listener on {ctx.config.bind}:{ctx.config.control_port}")

    while not ctx.stop_event.is_set():
        try:
            conn, addr = sock.accept()
        except socket.timeout:
            continue
        except OSError as exc:
            ctx.log(f"control listener error: {exc}")
            break
        ctx.log(f"control connection from {addr}")
        threading.Thread(target=handle_control_connection, args=(ctx, conn, addr), daemon=True).start()

    sock.close()


def run_server(config: ServerConfig, stop_event: threading.Event = None, on_log=None, block: bool = True):
    """Start the NetworkBenchmark server. If block=True (CLI usage), this
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
            f"esp_raw={caps['esp_raw']} gre_raw={caps['gre_raw']}")

    threads = [
        threading.Thread(target=_control_listener, args=(ctx,), daemon=True),
        threading.Thread(target=echo_responders.udp_base_listener, args=(ctx,), daemon=True),
        threading.Thread(target=echo_responders.tcp_base_listener, args=(ctx,), daemon=True),
        threading.Thread(target=echo_responders.natt_udp_listener, args=(ctx,), daemon=True),
    ]

    if caps.get("protocol_esp"):
        threads.append(threading.Thread(target=echo_responders.esp_native_listener, args=(ctx,), daemon=True))
    else:
        ctx.log(f"ESP native responder disabled: {privileges.capability_reason('protocol_esp', caps)}")

    if caps.get("protocol_gre"):
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
