"""Thread-safe counters and aggregation for throughput tests.

Each parallel stream has a StreamCounters instance updated by its
sender/receiver thread. An aggregator (in client/server engine) polls
snapshots every SAMPLE_INTERVAL_SEC to build an interval time series and
final summary, including the bbperf-style bufferbloat model:

    bdp_bytes       = receiver_throughput_Bps * unloaded_rtt_sec
    buffered_bytes  = receiver_throughput_Bps * (loaded_rtt_sec - unloaded_rtt_sec)
    bloat_factor    = buffered_bytes / bdp_bytes
"""

import collections
import threading
import time

from ..common import constants
from ..common.stats import compute_latency_stats


class StreamCounters:
    """Cumulative, thread-safe counters for one data stream."""

    def __init__(self, index: int):
        self.index = index
        self._lock = threading.Lock()
        self.bytes_sent = 0
        self.packets_sent = 0
        self.bytes_received = 0
        self.packets_received = 0
        self.max_seq_seen = -1
        self.retransmits = 0
        self.connected = False
        self.error = None

    def add_sent(self, nbytes: int, npackets: int = 1):
        with self._lock:
            self.bytes_sent += nbytes
            self.packets_sent += npackets

    def add_received(self, nbytes: int, seq=None):
        with self._lock:
            self.bytes_received += nbytes
            self.packets_received += 1
            if seq is not None and seq > self.max_seq_seen:
                self.max_seq_seen = seq

    def set_retransmits(self, value: int):
        with self._lock:
            self.retransmits = value

    def set_error(self, message: str):
        with self._lock:
            self.error = message

    def snapshot(self) -> dict:
        with self._lock:
            packets_lost = 0
            if self.max_seq_seen >= 0:
                packets_lost = max(0, (self.max_seq_seen + 1) - self.packets_received)
            return {
                "index": self.index,
                "bytes_sent": self.bytes_sent,
                "packets_sent": self.packets_sent,
                "bytes_received": self.bytes_received,
                "packets_received": self.packets_received,
                "packets_lost": packets_lost,
                "retransmits": self.retransmits,
                "error": self.error,
            }

    def merge_peer_snapshot(self, peer: dict):
        """Merge role-specific counters relayed from the peer (server) for
        this stream, e.g. bytes_received when the peer is the receiver."""
        with self._lock:
            if peer.get("bytes_sent", 0) > self.bytes_sent:
                self.bytes_sent = peer["bytes_sent"]
                self.packets_sent = peer.get("packets_sent", self.packets_sent)
            if peer.get("bytes_received", 0) > self.bytes_received:
                self.bytes_received = peer["bytes_received"]
                self.packets_received = peer.get("packets_received", self.packets_received)
            if peer.get("max_seq_seen", -1) > self.max_seq_seen:
                self.max_seq_seen = peer["max_seq_seen"]
            if peer.get("retransmits", 0) > self.retransmits:
                self.retransmits = peer["retransmits"]
            if peer.get("error"):
                self.error = peer["error"]


class RttTracker:
    """Collects control-channel probe RTTs, split into calibration
    ("unloaded") and running ("loaded") phases."""

    def __init__(self):
        self._lock = threading.Lock()
        self.unloaded_samples_ms = []
        self.loaded_samples_ms = []

    def add_sample(self, rtt_ms: float, phase: str):
        with self._lock:
            if phase == "calibration":
                self.unloaded_samples_ms.append(rtt_ms)
            else:
                self.loaded_samples_ms.append(rtt_ms)

    def unloaded_rtt_ms(self):
        with self._lock:
            if not self.unloaded_samples_ms:
                return None
            return min(self.unloaded_samples_ms)

    def loaded_stats(self):
        with self._lock:
            samples = list(self.loaded_samples_ms)
        return compute_latency_stats(samples)


def compute_bufferbloat(receiver_throughput_bps: float, unloaded_rtt_ms, loaded_rtt_ms):
    """Return dict with bdp_bytes, buffered_bytes, bloat_factor.

    receiver_throughput_bps: bytes/sec observed by the receiver.
    unloaded_rtt_ms / loaded_rtt_ms: may be None if not yet measured.
    """
    if unloaded_rtt_ms is None or loaded_rtt_ms is None:
        return {"bdp_bytes": None, "buffered_bytes": None, "bloat_factor": None}

    bdp_bytes = receiver_throughput_bps * (unloaded_rtt_ms / 1000.0)
    extra_rtt_sec = max(0.0, (loaded_rtt_ms - unloaded_rtt_ms) / 1000.0)
    buffered_bytes = receiver_throughput_bps * extra_rtt_sec
    bloat_factor = (buffered_bytes / bdp_bytes) if bdp_bytes > 0 else 0.0

    return {
        "bdp_bytes": int(bdp_bytes),
        "buffered_bytes": int(buffered_bytes),
        "bloat_factor": bloat_factor,
    }


def estimate_pps(transport: str, byte_rate: float, packet_rate: float) -> float:
    """packets/sec for display. UDP tracks real datagrams, so use them
    directly. TCP counters only see send()/recv() syscalls (each moving many
    wire segments), so estimate wire segments from the byte rate and MSS."""
    if transport == "tcp":
        return byte_rate / constants.TCP_MSS_ESTIMATE
    return packet_rate


class IntervalRecorder:
    """Builds a time series of interval records from periodic counter
    snapshots (bytes/sec, pps, loss%, retransmits, rtt) for logging + UI.

    Rates are averaged over a trailing ``window_sec`` window (not a single
    sample delta) so the peer-relayed direction -- whose stats arrive in bursts
    over the shared control channel -- doesn't oscillate between 0 and a
    catch-up spike.
    """

    def __init__(self, transport: str = "udp", window_sec: float = None):
        self.records = []
        self.transport = transport
        self.window_sec = window_sec if window_sec is not None else constants.RATE_WINDOW_SEC
        self._samples = collections.deque()  # (t, totals) within the window

    def record(self, t: float, totals: dict, rtt_ms=None, phase="running"):
        self._samples.append((t, dict(totals)))
        # Keep the oldest sample just beyond the window so the averaging span is
        # ~window_sec; drop anything older.
        while len(self._samples) > 2 and (t - self._samples[1][0]) >= self.window_sec:
            self._samples.popleft()

        t0, totals0 = self._samples[0]
        span = t - t0

        entry = {
            "t": t,
            "phase": phase,
            "rtt_ms": rtt_ms,
            "totals": dict(totals),
        }
        if span > 1e-6:
            def _delta(key):
                return max(0, totals.get(key, 0) - totals0.get(key, 0))

            send_bps = _delta("bytes_sent") / span
            recv_bps = _delta("bytes_received") / span
            entry["interval_sec"] = span
            entry["send_mbps"] = send_bps * 8 / 1e6
            entry["recv_mbps"] = recv_bps * 8 / 1e6
            entry["send_pps"] = estimate_pps(self.transport, send_bps, _delta("packets_sent") / span)
            entry["recv_pps"] = estimate_pps(self.transport, recv_bps, _delta("packets_received") / span)
            entry["interval_packets_lost"] = _delta("packets_lost")
            entry["interval_retransmits"] = _delta("retransmits")

        self.records.append(entry)
        return entry
