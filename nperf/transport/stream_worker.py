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

import bisect
import errno
import hashlib
import math
import operator
import secrets
import select
import socket
import struct
import sys
import threading
import time

from ..common import constants, tcp_info
from . import udp_batch

UDP_WIRE_VERSION = 3
# stream_index, sequence, per-run cookie, measurement trial, flag
UDP_HEADER_FMT = "!IQQIB"
UDP_HEADER_STRUCT = struct.Struct(UDP_HEADER_FMT)
UDP_HEADER_SIZE = UDP_HEADER_STRUCT.size
UDP_FLAG_DATA = 0
UDP_FLAG_STOP = 1
UDP_BATCH_SIZE = 64
_UDP_PYTHON_BATCH_SIZE = 8
UDP_SOCKET_BUFFER_BYTES = 16 * 1024 * 1024

_UDP_MAX_DATAGRAM_SIZE = 65535
_LINUX_SO_RXQ_OVFL = 40
_UINT32_MASK = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_INT64_MAX = (1 << 63) - 1
_SEQUENCE_REORDER_WINDOW = 1 << 16
_UDP_STOP_DRAIN_SEC = 0.5
_MACOS_TCP_BUFFER_BUDGET_BYTES = 64 * 1024 * 1024

_RUN_ID_LEN = 36  # uuid4 string length
PREAMBLE_FMT = f"{len(constants.DATA_STREAM_MAGIC)}s{_RUN_ID_LEN}sI"
PREAMBLE_STRUCT = struct.Struct(PREAMBLE_FMT)
PREAMBLE_SIZE = PREAMBLE_STRUCT.size


def build_preamble(run_id: str, stream_index: int) -> bytes:
    return PREAMBLE_STRUCT.pack(
        constants.DATA_STREAM_MAGIC, run_id.encode("ascii"), stream_index)


def parse_preamble(data: bytes):
    """Return (magic, run_id, stream_index) or None if malformed."""
    if len(data) < PREAMBLE_SIZE:
        return None
    magic, run_id_bytes, stream_index = PREAMBLE_STRUCT.unpack_from(data)
    try:
        run_id = run_id_bytes.decode("ascii")
    except UnicodeDecodeError:
        return None
    return magic, run_id, stream_index


def recv_exact(sock: socket.socket, size: int, deadline: float, cancelled=None):
    """Read exactly size bytes before an absolute monotonic deadline."""
    data = bytearray()
    while len(data) < size:
        if cancelled is not None and cancelled():
            return None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        timeout = remaining
        if cancelled is not None:
            timeout = min(timeout, max(0.01, constants.CONTROL_RECV_POLL_SEC))
        sock.settimeout(timeout)
        try:
            chunk = sock.recv(size - len(data))
        except socket.timeout:
            continue
        if not chunk:
            return None
        data.extend(chunk)
    return bytes(data)


def is_preamble(data: bytes) -> bool:
    """True if a datagram is a stream preamble (magic prefix) rather than a
    data frame. The UDP preamble is unacknowledged, so the client resends it
    during setup to survive packet loss; those duplicates can arrive interleaved
    with the first data packets, so receivers must skip them or they corrupt the
    sequence/loss accounting. A data frame starts with a small stream_index int,
    never the magic, so the prefix check is unambiguous."""
    magic = constants.DATA_STREAM_MAGIC
    return len(data) >= len(magic) and data[:len(magic)] == magic


def udp_session_cookie(run_id: str) -> int:
    """Return the stable 64-bit data-frame cookie for one control run."""
    digest = hashlib.blake2b(str(run_id).encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


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


def path_mtu_for_packet_size(packet_size, transport, family=socket.AF_INET):
    try:
        size = int(packet_size)
    except (TypeError, ValueError):
        raise ValueError("packet length must be an integer") from None
    if transport == "udp":
        overhead = 48 if family == socket.AF_INET6 else 28
        minimum = UDP_HEADER_SIZE
        label = "UDP datagram"
    elif transport == "tcp":
        overhead = 60 if family == socket.AF_INET6 else 40
        minimum = 64
        label = "TCP MSS"
    else:
        raise ValueError(f"unsupported transport: {transport}")
    maximum = 65535 - overhead
    if not minimum <= size <= maximum:
        raise ValueError(f"{label} length must be between {minimum} and {maximum} bytes")
    return size + overhead


def tcp_write_size_for_path_mtu(path_mtu, family=socket.AF_INET):
    mss = tcp_mss_for_path_mtu(path_mtu, family=family)
    if mss is None:
        return constants.TCP_CHUNK_SIZE
    return min(constants.TCP_CHUNK_SIZE, max(mss, mss * 32))


def configure_tcp_socket(sock: socket.socket, path_mtu=None, family=None, counters=None,
                         total_streams=None):
    # macOS has a small global mbuf pool even on machines with ample RAM.
    # Bound autotuning for large runs; leave Linux and small runs unchanged.
    if (sys.platform == "darwin" and isinstance(total_streams, int)
            and total_streams > 64):
        per_stream_bytes = _MACOS_TCP_BUFFER_BUDGET_BYTES // total_streams
        for option in (socket.SO_SNDBUF, socket.SO_RCVBUF):
            try:
                sock.setsockopt(socket.SOL_SOCKET, option, per_stream_bytes)
            except OSError:
                pass
    mss = tcp_mss_for_path_mtu(path_mtu, family=_family_or_socket_family(family, sock))
    option = getattr(socket, "TCP_MAXSEG", None)
    if mss is not None and option is not None:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, option, mss)
        except OSError:
            pass
    if counters is not None:
        try:
            counters.set_socket_metadata(
                socket_sndbuf_bytes=int(sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)),
                socket_rcvbuf_bytes=int(sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)),
                rxq_overflow_supported=False,
            )
        except (AttributeError, OSError):
            pass


# ---------------------------------------------------------------------------
# TCP
# ---------------------------------------------------------------------------

def tcp_sender_loop(sock: socket.socket, counters, stop_event: threading.Event,
                     duration_sec: float, start_time: float, path_mtu=None):
    """Send as fast as possible until stop_event is set or duration elapses."""
    configure_tcp_socket(sock, path_mtu, counters=counters)
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
        # Finish each stream independently; waiting for every sender in a large
        # worker group before sending EOF can stall the peer's drain phase.
        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        try:
            retrans = tcp_info.get_tcp_retransmits(sock)
            if retrans is not None:
                counters.set_retransmits(retrans)
        except Exception:
            pass


def tcp_receiver_loop(sock: socket.socket, counters, stop_event: threading.Event,
                      path_mtu=None):
    configure_tcp_socket(sock, path_mtu, counters=counters)
    buffer = bytearray(constants.BUFSZ)
    sock.settimeout(0.5)
    try:
        while not stop_event.is_set():
            try:
                received = sock.recv_into(buffer)
            except socket.timeout:
                continue
            except OSError as exc:
                counters.set_error(str(exc))
                break
            if not received:
                break
            counters.add_received(received)
    finally:
        pass


# ---------------------------------------------------------------------------
# UDP
# ---------------------------------------------------------------------------

class SequenceTracker:
    """Track a uint64 sequence span as disjoint missing ranges.

    Memory grows with unresolved gaps rather than received packet count. A
    tracker can be reset and reused at each measurement-trial boundary.
    """

    UNIQUE = "unique"
    DUPLICATE = "duplicate"
    REORDERED = "reordered"

    __slots__ = (
        "trial_id", "missing_ranges", "first_seq_seen", "max_seq_seen",
        "unique_count", "duplicate_count", "out_of_order_count",
        "missing_count", "prune_cutoff",
    )

    def __init__(self, trial_id=None):
        self.reset(trial_id)

    def reset(self, trial_id=None):
        self.trial_id = trial_id
        self.missing_ranges = []
        self.first_seq_seen = None
        self.max_seq_seen = None
        self.unique_count = 0
        self.duplicate_count = 0
        self.out_of_order_count = 0
        self.missing_count = 0
        self.prune_cutoff = 0

    def _add_missing(self, count):
        self.missing_count = min(_INT64_MAX, self.missing_count + max(0, int(count)))

    def _prune_missing_ranges(self):
        """Bound reorder bookkeeping while retaining finalized loss totals."""
        if self.max_seq_seen is None or not self.missing_ranges:
            return
        cutoff = max(0, self.max_seq_seen - _SEQUENCE_REORDER_WINDOW + 1)
        if cutoff - self.prune_cutoff < _SEQUENCE_REORDER_WINDOW // 4:
            return
        self.prune_cutoff = cutoff
        first_retained = 0
        while (first_retained < len(self.missing_ranges)
               and self.missing_ranges[first_retained][1] < cutoff):
            first_retained += 1
        if first_retained:
            del self.missing_ranges[:first_retained]
        if self.missing_ranges and self.missing_ranges[0][0] < cutoff:
            _start, end = self.missing_ranges[0]
            self.missing_ranges[0] = (cutoff, end)

    @property
    def duplicates(self):
        return self.duplicate_count

    @property
    def out_of_order(self):
        return self.out_of_order_count

    def observe(self, sequence):
        sequence = operator.index(sequence)
        if sequence < 0 or sequence > _UINT64_MAX:
            raise ValueError("UDP sequence must be in the uint64 range")

        if self.max_seq_seen is None:
            self.first_seq_seen = sequence
            self.max_seq_seen = sequence
            self.unique_count = 1
            return self.UNIQUE

        if sequence > self.max_seq_seen:
            if sequence > self.max_seq_seen + 1:
                start = self.max_seq_seen + 1
                end = sequence - 1
                self.missing_ranges.append((start, end))
                self._add_missing(end - start + 1)
            self.max_seq_seen = sequence
            self.unique_count += 1
            self._prune_missing_ranges()
            return self.UNIQUE

        if sequence < self.first_seq_seen:
            if sequence < self.prune_cutoff:
                self.duplicate_count += 1
                return self.DUPLICATE
            if sequence + 1 < self.first_seq_seen:
                start = sequence + 1
                end = self.first_seq_seen - 1
                self.missing_ranges.insert(0, (start, end))
                self._add_missing(end - start + 1)
            self.first_seq_seen = sequence
            self.unique_count += 1
            self.out_of_order_count += 1
            self._prune_missing_ranges()
            return self.REORDERED

        index = bisect.bisect_right(
            self.missing_ranges, (sequence, _UINT64_MAX)) - 1
        if index < 0:
            self.duplicate_count += 1
            return self.DUPLICATE
        start, end = self.missing_ranges[index]
        if sequence < start or sequence > end:
            self.duplicate_count += 1
            return self.DUPLICATE

        if start == end:
            del self.missing_ranges[index]
        elif sequence == start:
            self.missing_ranges[index] = (start + 1, end)
        elif sequence == end:
            self.missing_ranges[index] = (start, end - 1)
        else:
            self.missing_ranges[index] = (start, sequence - 1)
            self.missing_ranges.insert(index + 1, (sequence + 1, end))
        self.missing_count -= 1
        self.unique_count += 1
        self.out_of_order_count += 1
        return self.REORDERED

    def snapshot(self):
        return {
            "first_seq_seen": self.first_seq_seen,
            "max_seq_seen": self.max_seq_seen,
            "unique_count": self.unique_count,
            "missing_count": self.missing_count,
            "duplicates": self.duplicate_count,
            "out_of_order": self.out_of_order_count,
            "missing_ranges": tuple(self.missing_ranges),
        }


class RateHolder:
    """Minimal shared holder for a per-stream target send rate in bytes/sec
    (0 == unpaced / flat-out). Exposes ``.value`` so the same sender loop can
    read either this (threaded mode) or a ``multiprocessing.Value`` (MP mode)."""

    __slots__ = ("value",)

    def __init__(self, value: float = 0.0):
        self.value = value


class TrialHolder:
    """Minimal shared holder for the current uint32 measurement trial ID."""

    __slots__ = ("value",)

    def __init__(self, value: int = 0):
        self.value = value


class _UdpReceiveState:
    __slots__ = (
        "sequence", "trial_sequence", "trial_id", "overflow_last",
        "overflow_total",
    )

    def __init__(self):
        self.sequence = SequenceTracker()
        self.trial_sequence = SequenceTracker()
        self.trial_id = None
        self.overflow_last = None
        self.overflow_total = 0

    @staticmethod
    def _newer_trial_id(candidate, current):
        if int(candidate) == 0 and int(current) != 0:
            return False
        delta = (int(candidate) - int(current)) & _UINT32_MASK
        return 0 < delta < 0x80000000

    def observe_trial(self, sequence, trial_id):
        if self.trial_id is None or self._newer_trial_id(trial_id, self.trial_id):
            self.trial_id = trial_id
            self.trial_sequence.reset(trial_id)
        if trial_id != self.trial_id:
            return None
        return self.trial_sequence.observe(sequence)

    def observe_overflow(self, value):
        if value is None:
            return self.overflow_total
        value = int(value) & _UINT32_MASK
        if self.overflow_last is None:
            self.overflow_total = value
        else:
            self.overflow_total += (value - self.overflow_last) & _UINT32_MASK
        self.overflow_last = value
        return self.overflow_total


def _udp_receive_state(counters):
    state = getattr(counters, "_udp_receive_state", None)
    if state is None:
        state = _UdpReceiveState()
        try:
            counters._udp_receive_state = state
        except AttributeError:
            pass
    return state


def _udp_rxq_overflow_option():
    option = getattr(socket, "SO_RXQ_OVFL", None)
    if option is None and sys.platform.startswith("linux"):
        option = _LINUX_SO_RXQ_OVFL
    return option


def configure_udp_socket(sock: socket.socket, counters=None,
                         requested_buffer_bytes: int = UDP_SOCKET_BUFFER_BYTES):
    """Request large UDP buffers, enable drop telemetry, and report actuals."""
    for option in (socket.SO_SNDBUF, socket.SO_RCVBUF):
        try:
            sock.setsockopt(socket.SOL_SOCKET, option, int(requested_buffer_bytes))
        except (AttributeError, OSError):
            pass

    overflow_option = _udp_rxq_overflow_option()
    overflow_supported = False
    if overflow_option is not None:
        try:
            sock.setsockopt(socket.SOL_SOCKET, overflow_option, 1)
            overflow_supported = True
        except (AttributeError, OSError):
            pass

    # Python's socket module has omitted SO_RXQ_OVFL on some Linux builds.
    # Teach the portable recvmsg fallback the numeric option after the kernel
    # confirms it; the native extension reads the same cmsg directly.
    if overflow_supported and getattr(udp_batch, "_SO_RXQ_OVFL", None) is None:
        udp_batch._SO_RXQ_OVFL = overflow_option
        if hasattr(socket, "CMSG_SPACE"):
            udp_batch._ANCILLARY_SIZE = socket.CMSG_SPACE(struct.calcsize("=I"))
        udp_batch.RXQ_OVERFLOW_SUPPORTED = True

    actual = []
    for option in (socket.SO_SNDBUF, socket.SO_RCVBUF):
        try:
            actual.append(int(sock.getsockopt(socket.SOL_SOCKET, option)))
        except (AttributeError, OSError):
            actual.append(0)
    metadata = {
        "socket_sndbuf_bytes": actual[0],
        "socket_rcvbuf_bytes": actual[1],
        "rxq_overflow_supported": overflow_supported,
    }
    publish = getattr(counters, "set_socket_metadata", None)
    if callable(publish):
        publish(**metadata)
    return metadata


def _prepare_udp_socket(sock, peer_addr, counters, connect_socket):
    configure_udp_socket(sock, counters)
    address = peer_addr
    if connect_socket and peer_addr is not None:
        try:
            sock.connect(peer_addr)
            address = None
        except OSError:
            address = peer_addr
    try:
        sock.setblocking(False)
    except AttributeError:
        pass
    except OSError as exc:
        counters.set_error(str(exc))
        return address, False
    return address, True


def _wait_for_udp_ready(sock, stop_event, readable, timeout=0.05):
    if stop_event.is_set():
        return False
    try:
        # poll() handles descriptors above FD_SETSIZE in large stream runs.
        if hasattr(select, "poll"):
            poller = select.poll()
            poller.register(sock, select.POLLIN if readable else select.POLLOUT)
            return bool(poller.poll(math.ceil(max(0.0, timeout) * 1000)))
        readers = [sock] if readable else []
        writers = [] if readable else [sock]
        ready_read, ready_write, _ = select.select(
            readers, writers, [], max(0.0, timeout))
        return bool(ready_read if readable else ready_write)
    except (OSError, TypeError, ValueError):
        stop_event.wait(min(max(0.0, timeout), 0.01))
        return False


def _trial_value(trial_value):
    try:
        return operator.index(trial_value.value) & _UINT32_MASK
    except (AttributeError, TypeError, ValueError):
        return 0


def _publish_udp_backend(counters):
    try:
        counters.set_udp_backend(udp_batch.backend_status())
    except (AttributeError, TypeError, ValueError):
        pass


def _record_udp_io_error(sock, counters, stop_event, exc):
    closed = isinstance(exc, OSError) and exc.errno == errno.EBADF
    if (isinstance(exc, ValueError)
            and str(exc).startswith("file descriptor cannot be a negative integer")):
        closed = sock.fileno() < 0
    # Closing a socket during a requested stop is harmless. An unexpected close
    # or another native backend error must still invalidate the stream.
    if not (stop_event.is_set() and closed):
        counters.set_error(str(exc))


def udp_io_batch_size():
    # The portable backend performs one syscall per packet. Keep its work
    # bounded so hundreds of threads can observe stop/trial changes promptly.
    return UDP_BATCH_SIZE if udp_batch.BACKEND == "native" else _UDP_PYTHON_BATCH_SIZE


def udp_sender_loop(sock: socket.socket, dest_addr, stream_index: int, counters,
                     stop_event: threading.Event, duration_sec: float, start_time: float,
                     rate_value=None, path_mtu=None, trial_value=None,
                     connect_socket: bool = True, session_cookie: int = 0):
    """Send UDP datagrams until stopped or ``duration_sec`` elapses.

    If ``rate_value`` is provided and its ``.value`` is > 0, sends are paced to
    that many bytes/sec via a token bucket (used by the loss-target tuning
    ramp). A value of 0 means unpaced (send as fast as possible)."""
    seq = 0
    udp_packet_size = udp_packet_size_for_path_mtu(
        path_mtu, family=getattr(sock, "family", socket.AF_INET))
    payload = _padding(max(0, udp_packet_size - UDP_HEADER_SIZE))
    packet_size = UDP_HEADER_SIZE + len(payload)
    batch_size = udp_io_batch_size()
    send_buffers = [bytearray(packet_size) for _ in range(batch_size)]
    for buffer in send_buffers:
        buffer[UDP_HEADER_SIZE:] = payload

    send_address, usable = _prepare_udp_socket(
        sock, dest_addr, counters, connect_socket)
    if not usable:
        _publish_udp_backend(counters)
        return

    tokens = 0.0
    last_refill = time.perf_counter()
    deadline = None
    if duration_sec:
        remaining = max(0.0, float(duration_sec) - (time.time() - start_time))
        deadline = last_refill + remaining

    pending_count = 0
    pending_offset = 0
    pending_first_seq = 0
    pending_trial = 0
    pending_paced = False
    session_cookie = operator.index(session_cookie) & _UINT64_MAX
    observed_trial = None

    try:
        while True:
            current_trial = _trial_value(trial_value)
            if current_trial != observed_trial:
                counters.set_observed_trial(current_trial)
                observed_trial = current_trial
            now = time.perf_counter()
            if stop_event.is_set() or (deadline is not None and now >= deadline):
                break

            # Never carry unsent packets across an exact trial boundary.
            if pending_count and current_trial != pending_trial:
                pending_count = 0
                pending_offset = 0

            if pending_count == 0:
                try:
                    rate = float(rate_value.value) if rate_value is not None else 0.0
                except (TypeError, ValueError):
                    rate = 0.0
                if not math.isfinite(rate) or rate < 0:
                    counters.set_error(f"invalid UDP send rate: {rate!r}")
                    break
                if rate > 0:
                    tokens += (now - last_refill) * rate
                    last_refill = now
                    # Retain a few packets of scheduler oversleep so portable
                    # pacing still attains low target rates, while bounding any
                    # catch-up burst to one syscall batch.
                    burst_cap = min(
                        packet_size * batch_size,
                        max(packet_size * 4, rate * 0.005),
                    )
                    tokens = min(tokens, burst_cap)
                    if tokens < packet_size:
                        delay = min(0.1, (packet_size - tokens) / rate)
                        if deadline is not None:
                            delay = min(delay, max(0.0, deadline - now))
                        stop_event.wait(delay)
                        continue
                    cap_packets = max(1, int(burst_cap // packet_size))
                    pending_count = min(
                        batch_size, cap_packets, int(tokens // packet_size))
                    pending_paced = True
                else:
                    tokens = 0.0
                    last_refill = now
                    pending_count = batch_size
                    pending_paced = False

                pending_offset = 0
                pending_first_seq = seq
                pending_trial = current_trial
                for index in range(pending_count):
                    UDP_HEADER_STRUCT.pack_into(
                        send_buffers[index], 0, stream_index,
                        pending_first_seq + index, session_cookie,
                        pending_trial, UDP_FLAG_DATA)

            try:
                send_started_ns = time.monotonic_ns()
                batch_end = min(pending_count, pending_offset + udp_io_batch_size())
                sent = udp_batch.sendmmsg(
                    sock, send_buffers[pending_offset:batch_end],
                    address=send_address)
                send_finished_ns = time.monotonic_ns()
            except BlockingIOError:
                wait = 0.05
                if deadline is not None:
                    wait = min(wait, max(0.0, deadline - time.perf_counter()))
                _wait_for_udp_ready(
                    sock, stop_event, readable=False, timeout=wait)
                continue
            except (OSError, ValueError) as exc:
                _record_udp_io_error(sock, counters, stop_event, exc)
                break

            remaining_count = batch_end - pending_offset
            if sent < 0 or sent > remaining_count:
                counters.set_error(
                    "UDP batch backend returned an invalid send count")
                break
            if sent == 0:
                _wait_for_udp_ready(sock, stop_event, readable=False)
                continue

            sent_first_seq = pending_first_seq + pending_offset
            counters.add_sent_batch(
                sent * packet_size,
                sent,
                trial_id=pending_trial,
                first_seq=sent_first_seq,
                last_seq=sent_first_seq + sent - 1,
                first_sent_ns=send_started_ns,
                last_sent_ns=send_finished_ns,
            )
            pending_offset += sent
            seq += sent
            if pending_paced:
                tokens = max(0.0, tokens - sent * packet_size)
            if pending_offset == pending_count:
                pending_count = 0
                pending_offset = 0
    finally:
        try:
            stop_packet = bytearray(UDP_HEADER_SIZE)
            UDP_HEADER_STRUCT.pack_into(
                stop_packet, 0, stream_index, seq, session_cookie,
                _trial_value(trial_value), UDP_FLAG_STOP)
            udp_batch.sendmmsg(sock, [stop_packet], address=send_address)
        except (OSError, TypeError, ValueError):
            pass
        _publish_udp_backend(counters)


def read_udp_preamble(sock: socket.socket, timeout_sec: float = 30.0,
                      expected_run_id=None, expected_stream_index=None):
    """Block (up to timeout_sec) for the initial preamble packet on a
    freshly bound, exclusively-owned UDP socket. Returns (run_id,
    stream_index, addr) or None on timeout. Malformed and valid-but-foreign
    preambles are ignored for the full timeout."""
    deadline = time.monotonic() + max(0.0, timeout_sec)
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        sock.settimeout(remaining)
        try:
            data, addr_ = sock.recvfrom(constants.BUFSZ)
        except socket.timeout:
            return None
        preamble = parse_preamble(data)
        if preamble is None or preamble[0] != constants.DATA_STREAM_MAGIC:
            continue
        _, run_id, stream_index = preamble
        if expected_run_id is not None and run_id != expected_run_id:
            continue
        if (expected_stream_index is not None
                and stream_index != expected_stream_index):
            continue
        return run_id, stream_index, addr_


def _expected_stream_index(counters, stream_index):
    if stream_index is None:
        stream_index = getattr(counters, "index", None)
    return None if stream_index is None else int(stream_index)


def _peer_matches(actual, expected):
    return expected is None or actual is None or actual == expected


def _unpack_udp_header_for_session(data, length, expected_stream_index,
                                   expected_session_cookie=None):
    if length < UDP_HEADER_SIZE or is_preamble(data):
        return None
    try:
        (stream_index, sequence, session_cookie,
         trial_id, flag) = UDP_HEADER_STRUCT.unpack_from(data)
    except (struct.error, TypeError):
        return None
    if (expected_stream_index is not None
            and stream_index != expected_stream_index):
        return None
    if (expected_session_cookie is not None
            and session_cookie != expected_session_cookie):
        return None
    return stream_index, sequence, session_cookie, trial_id, flag


def _decode_udp_header(data, length, expected_stream_index,
                       expected_session_cookie=None):
    decoded = _unpack_udp_header_for_session(
        data, length, expected_stream_index, expected_session_cookie)
    if decoded is None:
        return None
    flag = decoded[4]
    if flag not in (UDP_FLAG_DATA, UDP_FLAG_STOP):
        return None
    return decoded


def _new_receive_group(trial_id):
    return {
        "trial_id": trial_id,
        "bytes": 0,
        "unique": 0,
        "duplicates": 0,
        "reordered": 0,
        "first_seq": None,
        "last_seq": None,
        "trial_bytes": 0,
        "trial_unique": 0,
        "trial_duplicates": 0,
        "trial_reordered": 0,
        "trial_missing": 0,
        "missing": 0,
        "overflow": 0,
    }


def _record_udp_sequence(state, group, sequence, length):
    group["first_seq"] = (
        sequence if group["first_seq"] is None
        else min(group["first_seq"], sequence))
    group["last_seq"] = (
        sequence if group["last_seq"] is None
        else max(group["last_seq"], sequence))

    status = state.sequence.observe(sequence)
    if status == SequenceTracker.DUPLICATE:
        group["duplicates"] += 1
    else:
        group["bytes"] += length
        group["unique"] += 1
        if status == SequenceTracker.REORDERED:
            group["reordered"] += 1

    trial_status = state.observe_trial(sequence, group["trial_id"])
    if trial_status == SequenceTracker.DUPLICATE:
        group["trial_duplicates"] += 1
    elif trial_status is not None:
        group["trial_bytes"] += length
        group["trial_unique"] += 1
        if trial_status == SequenceTracker.REORDERED:
            group["trial_reordered"] += 1
        group["trial_missing"] = state.trial_sequence.missing_count


def _finish_receive_group(state, group):
    group["missing"] = state.sequence.missing_count
    group["overflow"] = state.overflow_total
    if state.trial_id == group["trial_id"]:
        group["trial_missing"] = state.trial_sequence.missing_count
    return group


def _publish_receive_group(counters, group):
    counters.add_received_batch(
        group["bytes"],
        group["unique"],
        trial_id=group["trial_id"],
        first_seq=group["first_seq"],
        last_seq=group["last_seq"],
        duplicates=group["duplicates"],
        reordered=group["reordered"],
        missing_count=group["missing"],
        kernel_rxq_drops=group["overflow"],
        trial_nbytes=group["trial_bytes"],
        trial_unique_count=group["trial_unique"],
        trial_duplicates=group["trial_duplicates"],
        trial_reordered=group["trial_reordered"],
        trial_missing_count=group["trial_missing"],
    )


def udp_receiver_loop(sock: socket.socket, counters, stop_event: threading.Event,
                       known_peer_addr=None, stream_index=None,
                       connect_socket: bool = True, path_mtu=None,
                       session_cookie: int = 0):
    """Receive UDP data packets on a socket exclusively owned by this
    thread (the initial preamble, if any, must already have been consumed
    by the caller via read_udp_preamble)."""
    expected_stream_index = _expected_stream_index(counters, stream_index)
    state = _udp_receive_state(counters)
    receive_size = udp_packet_size_for_path_mtu(
        path_mtu, family=getattr(sock, "family", socket.AF_INET))
    receive_buffers = [bytearray(receive_size) for _ in range(udp_io_batch_size())]
    _address, usable = _prepare_udp_socket(
        sock, known_peer_addr, counters, connect_socket)
    if not usable:
        return

    session_cookie = operator.index(session_cookie) & _UINT64_MAX
    stop_deadline = None
    while not stop_event.is_set():
        if stop_deadline is not None and time.monotonic() >= stop_deadline:
            break
        overflow_before = state.overflow_total
        try:
            received = udp_batch.recvmmsg_into(sock, receive_buffers[:udp_io_batch_size()])
        except BlockingIOError:
            _wait_for_udp_ready(sock, stop_event, readable=True)
            continue
        except (OSError, ValueError) as exc:
            _record_udp_io_error(sock, counters, stop_event, exc)
            break
        if not received:
            _wait_for_udp_ready(sock, stop_event, readable=True)
            continue

        groups = []
        group = None
        stop_received = False
        for buffer, metadata in zip(receive_buffers, received):
            length, addr_, message_flags, overflow = metadata
            state.observe_overflow(overflow)
            if not _peer_matches(addr_, known_peer_addr):
                continue
            try:
                available_length = min(len(buffer), max(0, int(length)))
            except (TypeError, ValueError, OverflowError):
                continue
            if message_flags & getattr(socket, "MSG_TRUNC", 0):
                # The receive buffer still contains the leading v3 header. Do
                # not let an oversized packet from an old or foreign run poison
                # this session's permanent stream status.
                if _unpack_udp_header_for_session(
                        buffer, available_length, expected_stream_index,
                        session_cookie) is not None:
                    counters.set_error("truncated UDP datagram")
                continue
            decoded = _decode_udp_header(
                buffer, available_length, expected_stream_index, session_cookie)
            if decoded is None:
                continue
            _stream_index, sequence, _cookie, trial_id, flag = decoded
            if flag == UDP_FLAG_STOP:
                stop_received = True
                continue
            if group is None or group["trial_id"] != trial_id:
                if group is not None:
                    groups.append(_finish_receive_group(state, group))
                group = _new_receive_group(trial_id)
            _record_udp_sequence(state, group, sequence, available_length)
        if group is not None:
            groups.append(_finish_receive_group(state, group))

        for completed_group in groups:
            _publish_receive_group(counters, completed_group)
        if not groups and state.overflow_total != overflow_before:
            counters.set_kernel_rxq_drops(state.overflow_total)
        if stop_received:
            # UDP can reorder the stop watermark ahead of earlier data. Keep
            # reading briefly and process complete batches instead of dropping
            # valid tail datagrams after the STOP frame.
            stop_deadline = time.monotonic() + _UDP_STOP_DRAIN_SEC
    _publish_udp_backend(counters)


def run_stream_group(socks: dict, role: str, transport: str, counter_views: dict,
                      duration_sec: float, wait_start=None, stopped=None, rate_value=None,
                      on_wait_tick=None, path_mtu=None, trial_value=None,
                      connect_udp: bool = True, session_cookie: int = 0):
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
    start_gate = threading.Event()
    start_time = None
    local_stops = {}
    threads = []

    def _run_stream(i, sock, dest, stop_ev):
        start_gate.wait()
        if stop_ev.is_set():
            return
        view = counter_views[i]
        if role == "send":
            if transport == "tcp":
                tcp_sender_loop(sock, view, stop_ev, duration_sec, start_time, path_mtu)
            else:
                udp_sender_loop(
                    sock, dest, i, view, stop_ev, duration_sec, start_time,
                    rate_value=rate_value, path_mtu=path_mtu, trial_value=trial_value,
                    connect_socket=connect_udp, session_cookie=session_cookie)
        else:
            if transport == "tcp":
                tcp_receiver_loop(sock, view, stop_ev, path_mtu)
            else:
                udp_receiver_loop(
                    sock, view, stop_ev, known_peer_addr=dest, stream_index=i,
                    connect_socket=connect_udp, path_mtu=path_mtu,
                    session_cookie=session_cookie)

    prepared = False
    try:
        # Prepare every thread before releasing data flow. Otherwise busy early
        # streams can starve thread creation until the test duration has elapsed.
        for i, (sock, dest) in socks.items():
            stop_ev = threading.Event()
            local_stops[i] = stop_ev
            th = threading.Thread(
                target=_run_stream, args=(i, sock, dest, stop_ev), daemon=True)
            th.start()
            threads.append(th)
        if wait_start is not None:
            while not wait_start():
                if stopped is not None and stopped():
                    return
                if on_wait_tick is not None:
                    on_wait_tick()
                time.sleep(0.02)
        start_time = time.time()
        prepared = True
    finally:
        if not prepared:
            for ev in local_stops.values():
                ev.set()
        start_gate.set()
        if not prepared:
            deadline = time.monotonic() + constants.WORKER_DRAIN_TIMEOUT_SEC
            for th in threads:
                th.join(timeout=max(0.0, deadline - time.monotonic()))

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

    if transport == "tcp" and role == "send":
        for i, (sock, _dest) in socks.items():
            retrans = tcp_info.get_tcp_retransmits(sock)
            if retrans is not None:
                counter_views[i].set_retransmits(retrans)


def feed_udp_data_frames(counters, frames, stream_index=None,
                         session_cookie: int = 0) -> int:
    """Parse and publish a batch from the shared base-port listener."""
    expected_index = _expected_stream_index(counters, stream_index)
    session_cookie = operator.index(session_cookie) & _UINT64_MAX
    state = _udp_receive_state(counters)
    groups = []
    group = None
    accepted = 0
    for data in frames:
        decoded = _decode_udp_header(
            data, len(data), expected_index, session_cookie)
        if decoded is None:
            continue
        _stream_index, sequence, _cookie, trial_id, flag = decoded
        accepted += 1
        if flag == UDP_FLAG_STOP:
            continue
        if group is None or group["trial_id"] != trial_id:
            if group is not None:
                groups.append(_finish_receive_group(state, group))
            group = _new_receive_group(trial_id)
        _record_udp_sequence(state, group, sequence, len(data))
    if group is not None:
        groups.append(_finish_receive_group(state, group))
    for completed_group in groups:
        _publish_receive_group(counters, completed_group)
    return accepted


def feed_udp_data_frame(counters, data: bytes, stream_index=None,
                        session_cookie: int = 0) -> bool:
    """Parse one already-received UDP data frame into counters. Used by the
    shared base-port listener, which must remain the sole reader of its
    socket (see server.echo_responders). Returns False if not a data frame.
    """
    return bool(feed_udp_data_frames(
        counters, [data], stream_index=stream_index,
        session_cookie=session_cookie))
