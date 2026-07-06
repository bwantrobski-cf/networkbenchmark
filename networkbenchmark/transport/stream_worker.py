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


def _padding(size: int) -> bytes:
    if size <= 0:
        return b""
    return secrets.token_bytes(size)


# ---------------------------------------------------------------------------
# TCP
# ---------------------------------------------------------------------------

def tcp_sender_loop(sock: socket.socket, counters, stop_event: threading.Event,
                     duration_sec: float, start_time: float):
    """Send as fast as possible until stop_event is set or duration elapses."""
    chunk = _padding(constants.TCP_CHUNK_SIZE)
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

def udp_sender_loop(sock: socket.socket, dest_addr, stream_index: int, counters,
                     stop_event: threading.Event, duration_sec: float, start_time: float):
    seq = 0
    payload = _padding(max(0, constants.UDP_PAYLOAD_SIZE - UDP_HEADER_SIZE))
    sock.settimeout(1.0)
    try:
        while not stop_event.is_set():
            if duration_sec and (time.time() - start_time) > duration_sec:
                break
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

            if len(data) < UDP_HEADER_SIZE:
                continue

            _stream_index, seq, _send_ts, flag = struct.unpack(UDP_HEADER_FMT, data[:UDP_HEADER_SIZE])
            counters.add_received(len(data), seq=seq)
            if flag == UDP_FLAG_STOP:
                break
    finally:
        pass


def feed_udp_data_frame(counters, data: bytes) -> bool:
    """Parse one already-received UDP data frame into counters. Used by the
    shared base-port listener, which must remain the sole reader of its
    socket (see server.echo_responders). Returns False if not a data frame.
    """
    if len(data) < UDP_HEADER_SIZE:
        return False
    _stream_index, seq, _send_ts, flag = struct.unpack(UDP_HEADER_FMT, data[:UDP_HEADER_SIZE])
    counters.add_received(len(data), seq=seq)
    return True
