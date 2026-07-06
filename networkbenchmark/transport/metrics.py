"""Thread-safe counters and aggregation for throughput tests.

Each parallel stream has a StreamCounters instance updated by its
sender/receiver thread. An aggregator (in client/server engine) polls
snapshots every SAMPLE_INTERVAL_SEC to build an interval time series and
final summary, including the bbperf-style bufferbloat model:

    bdp_bytes       = receiver_throughput_Bps * unloaded_rtt_sec
    buffered_bytes  = receiver_throughput_Bps * (loaded_rtt_sec - unloaded_rtt_sec)
    bloat_factor    = buffered_bytes / bdp_bytes
"""

import threading
import time

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


class IntervalRecorder:
    """Builds a time series of interval records from periodic counter
    snapshots (bytes/sec, pps, loss%, retransmits, rtt) for logging + UI."""

    def __init__(self):
        self.records = []
        self._last_totals = {}
        self._last_time = None

    def record(self, t: float, totals: dict, rtt_ms=None, phase="running"):
        elapsed = None
        deltas = {}
        if self._last_time is not None:
            elapsed = max(1e-6, t - self._last_time)
            for key in ("bytes_sent", "bytes_received", "packets_sent",
                        "packets_received", "packets_lost", "retransmits"):
                prev = self._last_totals.get(key, 0)
                curr = totals.get(key, 0)
                deltas[key] = max(0, curr - prev)

        entry = {
            "t": t,
            "phase": phase,
            "rtt_ms": rtt_ms,
            "totals": dict(totals),
        }
        if elapsed:
            entry["interval_sec"] = elapsed
            entry["send_mbps"] = (deltas.get("bytes_sent", 0) * 8 / elapsed) / 1e6
            entry["recv_mbps"] = (deltas.get("bytes_received", 0) * 8 / elapsed) / 1e6
            entry["send_pps"] = deltas.get("packets_sent", 0) / elapsed
            entry["recv_pps"] = deltas.get("packets_received", 0) / elapsed
            sent_for_loss = deltas.get("packets_sent") or deltas.get("packets_received", 0)
            entry["interval_packets_lost"] = deltas.get("packets_lost", 0)
            entry["interval_retransmits"] = deltas.get("retransmits", 0)

        self.records.append(entry)
        self._last_totals = dict(totals)
        self._last_time = t
        return entry
