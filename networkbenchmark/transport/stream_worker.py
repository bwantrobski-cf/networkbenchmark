"""Per-stream TCP/UDP sender and receiver loops.

Adapted from bbperf's data_sender_thread.py / data_receiver_thread.py:
- TCP: byte-stream send/recv, retransmit count sampled via common.tcp_info.
- UDP: each datagram is prefixed with a small binary header carrying a
  monotonically increasing sequence number so the receiver can detect loss
  (adapted from the framing style used in iperf3's UDP tests).

A short fixed-length preamble (magic + run_id + stream_index) identifies a
new data connection/flow so it can be demultiplexed from a lightweight
"check connectivity" probe landing on the same base port (stream index 0).
"""

import secrets
import socket
import struct
import threading
import time

from ..common import constants, tcp_info

UDP_HEADER_FMT = "!IIdB"  # stream_index, seq, send_ts, flag
UDP_HEADER_SIZE = struct.calcsize(UDP_HEADER_FMT)
UDP_FLAG_DATA = 0
UDP_FLAG_STOP = 1

_RUN_ID_LEN = 36  # uuid4 string length
PREAMBLE_FMT = f"{len(constants.DATA_STREAM_MAGIC)}s{_RUN_ID_LEN}sI"
PREAMBLE_SIZE = struct.calcsize(PREAMBLE_FMT)


def build_preamble(run_id: str, stream_index: int) -> bytes:
    return struct.pack(PREAMBLE_FMT, constants.DATA_STREAM_MAGIC, run_id.encode("ascii"), stream_index)


def parse_preamble(data: bytes):
    """Return (magic, run_id, stream_index) or None if malformed."""
    if len(data) < PREAMBLE_SIZE:
        return None
    magic, run_id_bytes, stream_index = struct.unpack(PREAMBLE_FMT, data[:PREAMBLE_SIZE])
    try:
        run_id = run_id_bytes.decode("ascii")
    except UnicodeDecodeError:
        return None
    return magic, run_id, stream_index


def is_preamble(data: bytes) -> bool:
    """True if a datagram is a stream preamble (magic prefix) rather than a
    data frame. The UDP preamble is unacknowledged, so the client resends it
    during setup to survive packet loss; those duplicates can arrive interleaved
    with the first data packets, so receivers must skip them or they corrupt the
    sequence/loss accounting. A data frame starts with a small stream_index int,
    never the magic, so the prefix check is unambiguous."""
    magic = constants.DATA_STREAM_MAGIC
    return len(data) >= len(magic) and data[:len(magic)] == magic


def _padding(size: int) -> bytes:
    if size <= 0:
        return b""
    return secrets.token_bytes(size)


def _family_or_socket_family(family, sock=None):
    return family if family is not None else getattr(sock, "family", socket.AF_INET)


def udp_packet_size_for_path_mtu(path_mtu, family=socket.AF_INET):
    if path_mtu is None:
        return constants.UDP_PAYLOAD_SIZE
    try:
        mtu = int(path_mtu)
    except (TypeError, ValueError):
        return constants.UDP_PAYLOAD_SIZE
    overhead = 48 if family == socket.AF_INET6 else 28
    return max(UDP_HEADER_SIZE, mtu - overhead)


def tcp_mss_for_path_mtu(path_mtu, family=socket.AF_INET):
    if path_mtu is None:
        return None
    try:
        mtu = int(path_mtu)
    except (TypeError, ValueError):
        return None
    overhead = 60 if family == socket.AF_INET6 else 40
    return max(64, mtu - overhead)


def tcp_write_size_for_path_mtu(path_mtu, family=socket.AF_INET):
    mss = tcp_mss_for_path_mtu(path_mtu, family=family)
    if mss is None:
        return constants.TCP_CHUNK_SIZE
    return min(constants.TCP_CHUNK_SIZE, max(mss, mss * 32))


def configure_tcp_socket(sock: socket.socket, path_mtu=None, family=None):
    mss = tcp_mss_for_path_mtu(path_mtu, family=_family_or_socket_family(family, sock))
    option = getattr(socket, "TCP_MAXSEG", None)
    if mss is None or option is None:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, option, mss)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# TCP
# ---------------------------------------------------------------------------

def tcp_sender_loop(sock: socket.socket, counters, stop_event: threading.Event,
                     duration_sec: float, start_time: float, path_mtu=None):
    """Send as fast as possible until stop_event is set or duration elapses."""
    chunk = _padding(tcp_write_size_for_path_mtu(path_mtu, family=sock.family))
    sock.settimeout(1.0)
    try:
        while not stop_event.is_set():
            if duration_sec and (time.time() - start_time) > duration_sec:
                break
            try:
                n = sock.send(chunk)
            except socket.timeout:
                continue
            except (BrokenPipeError, ConnectionResetError, OSError) as exc:
                counters.set_error(str(exc))
                break
            if n > 0:
                counters.add_sent(n)
    finally:
        try:
            retrans = tcp_info.get_tcp_retransmits(sock)
            if retrans is not None:
                counters.set_retransmits(retrans)
        except Exception:
            pass


def tcp_receiver_loop(sock: socket.socket, counters, stop_event: threading.Event):
    sock.settimeout(0.5)
    try:
        while not stop_event.is_set():
            try:
                data = sock.recv(constants.BUFSZ)
            except socket.timeout:
                continue
            except OSError as exc:
                counters.set_error(str(exc))
                break
            if not data:
                break
            counters.add_received(len(data))
    finally:
        try:
            retrans = tcp_info.get_tcp_retransmits(sock)
            if retrans is not None:
                counters.set_retransmits(retrans)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# UDP
# ---------------------------------------------------------------------------

class RateHolder:
    """Minimal shared holder for a per-stream target send rate in bytes/sec
    (0 == unpaced / flat-out). Exposes ``.value`` so the same sender loop can
    read either this (threaded mode) or a ``multiprocessing.Value`` (MP mode)."""

    __slots__ = ("value",)

    def __init__(self, value: float = 0.0):
        self.value = value


def udp_sender_loop(sock: socket.socket, dest_addr, stream_index: int, counters,
                     stop_event: threading.Event, duration_sec: float, start_time: float,
                     rate_value=None, path_mtu=None):
    """Send UDP datagrams until stopped or ``duration_sec`` elapses.

    If ``rate_value`` is provided and its ``.value`` is > 0, sends are paced to
    that many bytes/sec via a token bucket (used by the loss-target tuning
    ramp). A value of 0 means unpaced (send as fast as possible)."""
    seq = 0
    udp_packet_size = udp_packet_size_for_path_mtu(path_mtu, family=sock.family)
    payload = _padding(max(0, udp_packet_size - UDP_HEADER_SIZE))
    packet_size = UDP_HEADER_SIZE + len(payload)
    sock.settimeout(1.0)
    tokens = 0.0
    last = time.perf_counter()
    try:
        while not stop_event.is_set():
            if duration_sec and (time.time() - start_time) > duration_sec:
                break

            now = time.perf_counter()
            rate = rate_value.value if rate_value is not None else 0.0
            if rate and rate > 0:
                # Token bucket: accrue budget at `rate` bytes/sec, cap the burst
                # to ~5 ms so pacing stays smooth, and sleep when short.
                tokens += (now - last) * rate
                last = now
                burst_cap = max(packet_size, rate * 0.005)
                if tokens > burst_cap:
                    tokens = burst_cap
                if tokens < packet_size:
                    time.sleep(min(0.002, (packet_size - tokens) / rate))
                    continue
                tokens -= packet_size
            else:
                last = now

            header = struct.pack(UDP_HEADER_FMT, stream_index, seq, time.time(), UDP_FLAG_DATA)
            packet = header + payload
            try:
                n = sock.sendto(packet, dest_addr)
            except (BlockingIOError,):
                continue
            except OSError as exc:
                counters.set_error(str(exc))
                break
            if n > 0:
                counters.add_sent(n)
                seq += 1
    finally:
        try:
            stop_header = struct.pack(UDP_HEADER_FMT, stream_index, seq, time.time(), UDP_FLAG_STOP)
            sock.sendto(stop_header, dest_addr)
        except OSError:
            pass


def read_udp_preamble(sock: socket.socket, timeout_sec: float = 30.0):
    """Block (up to timeout_sec) for the initial preamble packet on a
    freshly bound, exclusively-owned UDP socket. Returns (run_id,
    stream_index, addr) or None on timeout/malformed packet."""
    sock.settimeout(timeout_sec)
    try:
        data, addr_ = sock.recvfrom(constants.BUFSZ)
    except socket.timeout:
        return None
    preamble = parse_preamble(data)
    if preamble is None or preamble[0] != constants.DATA_STREAM_MAGIC:
        return None
    _, run_id, stream_index = preamble
    return run_id, stream_index, addr_


def udp_receiver_loop(sock: socket.socket, counters, stop_event: threading.Event,
                       known_peer_addr=None):
    """Receive UDP data packets on a socket exclusively owned by this
    thread (the initial preamble, if any, must already have been consumed
    by the caller via read_udp_preamble)."""
    sock.settimeout(0.5)
    try:
        while not stop_event.is_set():
            try:
                data, addr_ = sock.recvfrom(constants.BUFSZ)
            except socket.timeout:
                continue
            except OSError as exc:
                counters.set_error(str(exc))
                break

            if known_peer_addr is not None and addr_ != known_peer_addr:
                continue

            if is_preamble(data):
                # A resent setup preamble that raced ahead of the data; ignore
                # it so it isn't miscounted as a (lost/duplicate) data packet.
                continue

            if len(data) < UDP_HEADER_SIZE:
                continue

            _stream_index, seq, _send_ts, flag = struct.unpack(UDP_HEADER_FMT, data[:UDP_HEADER_SIZE])
            counters.add_received(len(data), seq=seq)
            if flag == UDP_FLAG_STOP:
                break
    finally:
        pass


def run_stream_group(socks: dict, role: str, transport: str, counter_views: dict,
                      duration_sec: float, wait_start=None, stopped=None, rate_value=None,
                      on_wait_tick=None, path_mtu=None):
    """Run the sender/receiver loops for a group of streams as threads within
    the current (possibly worker) process.

    Shared by the multiprocessing worker pools on both the client and server.

    Args:
        socks: dict index -> (sock, dest_addr). dest_addr is the UDP peer
               (used by UDP sender/receiver) or None for TCP.
        role: 'send' or 'recv' (the role THIS side plays for the streams).
        transport: 'tcp' or 'udp'.
        counter_views: dict index -> counter object (StreamCounters or a
               shared_counters.SharedCounterView) with add_sent/add_received/
               set_retransmits.
        duration_sec: test duration (senders self-stop after this).
        wait_start: optional callable that blocks until the group should start.
        stopped: optional callable returning True once the group should stop.

    Note: start/stop are passed as *callables* (typically polling a lock-free
    ``multiprocessing.Value``) rather than ``multiprocessing.Event`` objects.
    A worker process may exit on its own (senders self-stop at duration); an
    Event's shared Condition lock could be left held by such a worker and
    deadlock the parent's ``set()``. Lock-free polling avoids that entirely.
    """
    if wait_start is not None:
        while not wait_start():
            if on_wait_tick is not None:
                on_wait_tick()
            time.sleep(0.02)
    start_time = time.time()

    local_stops = {}
    threads = []
    for i, (sock, dest) in socks.items():
        stop_ev = threading.Event()
        local_stops[i] = stop_ev
        view = counter_views[i]
        if role == "send":
            if transport == "tcp":
                th = threading.Thread(target=tcp_sender_loop,
                                       args=(sock, view, stop_ev, duration_sec, start_time, path_mtu),
                                       daemon=True)
            else:
                th = threading.Thread(target=udp_sender_loop,
                                       args=(sock, dest, i, view, stop_ev, duration_sec, start_time),
                                       kwargs={"rate_value": rate_value, "path_mtu": path_mtu}, daemon=True)
        else:
            if transport == "tcp":
                th = threading.Thread(target=tcp_receiver_loop,
                                       args=(sock, view, stop_ev), daemon=True)
            else:
                th = threading.Thread(target=udp_receiver_loop,
                                       args=(sock, view, stop_ev),
                                       kwargs={"known_peer_addr": dest}, daemon=True)
        th.start()
        threads.append(th)

    if stopped is not None:
        def _monitor():
            while not stopped():
                if all(not t.is_alive() for t in threads):
                    break
                time.sleep(0.1)
            for ev in local_stops.values():
                ev.set()
        threading.Thread(target=_monitor, daemon=True).start()

    for th in threads:
        th.join()

    if transport == "tcp":
        for i, (sock, _dest) in socks.items():
            retrans = tcp_info.get_tcp_retransmits(sock)
            if retrans is not None:
                counter_views[i].set_retransmits(retrans)


def feed_udp_data_frame(counters, data: bytes) -> bool:
    """Parse one already-received UDP data frame into counters. Used by the
    shared base-port listener, which must remain the sole reader of its
    socket (see server.echo_responders). Returns False if not a data frame.
    """
    if is_preamble(data):
        return False
    if len(data) < UDP_HEADER_SIZE:
        return False
    _stream_index, seq, _send_ts, flag = struct.unpack(UDP_HEADER_FMT, data[:UDP_HEADER_SIZE])
    counters.add_received(len(data), seq=seq)
    return True
