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
        self.packets_lost = 0
        self.duplicates = 0
        self.out_of_order = 0
        self.kernel_rxq_drops = 0
        self.socket_sndbuf_bytes = 0
        self.socket_rcvbuf_bytes = 0
        self.rxq_overflow_supported = False
        self.retransmits = 0
        self.connected = False
        self.error = None
        self._trial_sent_active = False
        self._trial_received_active = False
        self.observed_trial_id = None
        self.udp_batch = {"selected": None, "native_runtime_error": None}
        self.trial_sent = self._empty_trial_sent()
        self.trial_received = self._empty_trial_received()

    @staticmethod
    def _empty_trial_sent(trial_id=0):
        return {
            "trial_id": int(trial_id),
            "activated_at_ns": None,
            "deactivated_at_ns": None,
            "bytes_sent": 0,
            "packets_sent": 0,
            "first_seq": None,
            "last_seq": None,
            "first_sent_ns": None,
            "last_sent_ns": None,
        }

    @staticmethod
    def _empty_trial_received(trial_id=0):
        return {
            "trial_id": int(trial_id),
            "activated_at_ns": None,
            "deactivated_at_ns": None,
            "bytes_received": 0,
            "packets_received": 0,
            "unique_packets": 0,
            "first_seq": None,
            "last_seq": None,
            "packets_lost": 0,
            "sequence_gaps": 0,
            "duplicates": 0,
            "out_of_order": 0,
        }

    @staticmethod
    def _newer_trial_id(candidate, current):
        if int(candidate) == 0 and int(current) != 0:
            return False
        delta = (int(candidate) - int(current)) & 0xFFFFFFFF
        return 0 < delta < 0x80000000

    @staticmethod
    def _trial_role(role):
        if role in ("sent", "sender", "trial_sent"):
            return "sent"
        if role in ("received", "receiver", "trial_received"):
            return "received"
        raise ValueError(f"unknown trial role: {role!r}")

    def _activate_trial_locked(self, trial_id, role, activated_at_ns):
        trial_id = int(trial_id) & 0xFFFFFFFF
        if trial_id == 0:
            return
        role = self._trial_role(role)
        active_name = f"_trial_{role}_active"
        record_name = f"trial_{role}"
        record = getattr(self, record_name)
        if (not getattr(self, active_name)
                or self._newer_trial_id(trial_id, record["trial_id"])):
            factory = (self._empty_trial_sent if role == "sent"
                       else self._empty_trial_received)
            record = factory(trial_id)
            setattr(self, record_name, record)
            setattr(self, active_name, True)
        if trial_id == record["trial_id"]:
            activated_at_ns = int(activated_at_ns)
            if (record["activated_at_ns"] is None
                    or activated_at_ns < record["activated_at_ns"]):
                record["activated_at_ns"] = activated_at_ns

    def activate_trial(self, trial_id: int, role: str,
                       activated_at_ns=None):
        """Publish an exact zero-valued record at a trial boundary."""
        if activated_at_ns is None:
            activated_at_ns = time.monotonic_ns()
        with self._lock:
            self._activate_trial_locked(trial_id, role, activated_at_ns)

    def deactivate_trial(self, trial_id: int, role: str,
                         deactivated_at_ns=None):
        if deactivated_at_ns is None:
            deactivated_at_ns = time.monotonic_ns()
        trial_id = int(trial_id) & 0xFFFFFFFF
        with self._lock:
            role = self._trial_role(role)
            record = getattr(self, f"trial_{role}")
            if (trial_id != 0 and record.get("trial_id") == trial_id
                    and getattr(self, f"_trial_{role}_active")):
                deactivated_at_ns = int(deactivated_at_ns)
                if (record["deactivated_at_ns"] is None
                        or deactivated_at_ns < record["deactivated_at_ns"]):
                    record["deactivated_at_ns"] = deactivated_at_ns

    def _reconcile_peer_loss(self):
        """Replace receiver gap estimates when exact peer trial counts exist."""
        if not self._trial_sent_active:
            return

        # Cumulative counts also cover a complete blackout, where no matching
        # receiver trial record can exist.
        self.packets_lost = max(
            0, max(0, int(self.packets_sent)) - max(0, int(self.packets_received)))

        sent_packets = max(0, int(self.trial_sent.get("packets_sent", 0) or 0))
        if (not self._trial_received_active
                or self.trial_received.get("trial_id") != self.trial_sent.get("trial_id")):
            return
        received = self.trial_received
        unique_packets = max(0, int(received.get(
            "unique_packets", received.get("packets_received", 0)) or 0))
        sequence_gaps = max(0, int(received.get(
            "sequence_gaps", received.get("packets_lost", 0)) or 0))
        received["sequence_gaps"] = sequence_gaps
        received["packets_lost"] = max(0, sent_packets - unique_packets)

    def add_sent(self, nbytes: int, npackets: int = 1):
        with self._lock:
            self.bytes_sent += nbytes
            self.packets_sent += npackets

    def add_sent_batch(self, nbytes: int, npackets: int, trial_id: int = 0,
                       first_seq=None, last_seq=None, first_sent_ns=None,
                       last_sent_ns=None):
        """Publish one completed UDP send batch atomically."""
        nbytes = int(nbytes)
        npackets = int(npackets)
        trial_id = int(trial_id) & 0xFFFFFFFF
        with self._lock:
            self.bytes_sent += nbytes
            self.packets_sent += npackets
            if (not self._trial_sent_active
                    or self._newer_trial_id(trial_id, self.trial_sent["trial_id"])):
                self.trial_sent = self._empty_trial_sent(trial_id)
                self._trial_sent_active = True
            if trial_id != self.trial_sent["trial_id"]:
                return
            record = self.trial_sent
            record["bytes_sent"] += nbytes
            record["packets_sent"] += npackets
            if first_seq is not None:
                first_seq = int(first_seq)
                if record["first_seq"] is None or first_seq < record["first_seq"]:
                    record["first_seq"] = first_seq
            if last_seq is not None:
                last_seq = int(last_seq)
                if record["last_seq"] is None or last_seq > record["last_seq"]:
                    record["last_seq"] = last_seq
            if first_sent_ns is not None:
                first_sent_ns = int(first_sent_ns)
                if (record["first_sent_ns"] is None
                        or first_sent_ns < record["first_sent_ns"]):
                    record["first_sent_ns"] = first_sent_ns
            if last_sent_ns is not None:
                last_sent_ns = int(last_sent_ns)
                if (record["last_sent_ns"] is None
                        or last_sent_ns > record["last_sent_ns"]):
                    record["last_sent_ns"] = last_sent_ns

    def add_received(self, nbytes: int, seq=None):
        with self._lock:
            self.bytes_received += nbytes
            self.packets_received += 1
            if seq is not None and seq > self.max_seq_seen:
                self.max_seq_seen = seq
            if seq is not None and self.max_seq_seen >= 0:
                # Retain the historical zero-based estimate for callers that
                # have not migrated to add_received_batch().
                self.packets_lost = max(
                    0, (self.max_seq_seen + 1) - self.packets_received)

    def add_received_batch(
            self, nbytes: int, unique_count: int, trial_id: int = 0,
            first_seq=None, last_seq=None, duplicates: int = 0,
            reordered: int = 0, missing_count: int = 0,
            kernel_rxq_drops=None, *, out_of_order=None, trial_nbytes=None,
            trial_unique_count=None, trial_duplicates=None,
            trial_reordered=None, trial_missing_count=None):
        """Publish one parsed UDP receive batch/trial group atomically.

        ``missing_count`` and ``kernel_rxq_drops`` are cumulative socket-level
        values. The remaining numeric arguments are deltas for this group.
        """
        if out_of_order is not None:
            reordered = out_of_order
        nbytes = int(nbytes)
        unique_count = int(unique_count)
        duplicates = int(duplicates)
        reordered = int(reordered)
        missing_count = int(missing_count)
        trial_id = int(trial_id) & 0xFFFFFFFF
        trial_nbytes = nbytes if trial_nbytes is None else int(trial_nbytes)
        trial_unique_count = (
            unique_count if trial_unique_count is None else int(trial_unique_count))
        trial_duplicates = (
            duplicates if trial_duplicates is None else int(trial_duplicates))
        trial_reordered = (
            reordered if trial_reordered is None else int(trial_reordered))
        trial_missing_count = (
            missing_count if trial_missing_count is None else int(trial_missing_count))

        with self._lock:
            self.bytes_received += nbytes
            self.packets_received += unique_count
            self.duplicates += duplicates
            self.out_of_order += reordered
            self.packets_lost = max(0, missing_count)
            for seq in (first_seq, last_seq):
                if seq is not None and int(seq) > self.max_seq_seen:
                    self.max_seq_seen = int(seq)
            if kernel_rxq_drops is not None:
                self.kernel_rxq_drops = max(
                    self.kernel_rxq_drops, int(kernel_rxq_drops))

            if (not self._trial_received_active
                    or self._newer_trial_id(
                        trial_id, self.trial_received["trial_id"])):
                self.trial_received = self._empty_trial_received(trial_id)
                self._trial_received_active = True
            if trial_id != self.trial_received["trial_id"]:
                self._reconcile_peer_loss()
                return
            record = self.trial_received
            record["bytes_received"] += trial_nbytes
            record["packets_received"] += trial_unique_count
            record["unique_packets"] += trial_unique_count
            record["duplicates"] += trial_duplicates
            record["out_of_order"] += trial_reordered
            record["packets_lost"] = max(0, trial_missing_count)
            record["sequence_gaps"] = max(0, trial_missing_count)
            if first_seq is not None:
                first_seq = int(first_seq)
                if record["first_seq"] is None or first_seq < record["first_seq"]:
                    record["first_seq"] = first_seq
            if last_seq is not None:
                last_seq = int(last_seq)
                if record["last_seq"] is None or last_seq > record["last_seq"]:
                    record["last_seq"] = last_seq
            self._reconcile_peer_loss()

    def set_socket_metadata(self, socket_sndbuf_bytes=None,
                            socket_rcvbuf_bytes=None,
                            rxq_overflow_supported=None):
        with self._lock:
            if socket_sndbuf_bytes is not None:
                self.socket_sndbuf_bytes = int(socket_sndbuf_bytes)
            if socket_rcvbuf_bytes is not None:
                self.socket_rcvbuf_bytes = int(socket_rcvbuf_bytes)
            if rxq_overflow_supported is not None:
                self.rxq_overflow_supported = bool(rxq_overflow_supported)

    def set_kernel_rxq_drops(self, cumulative: int):
        with self._lock:
            self.kernel_rxq_drops = max(
                self.kernel_rxq_drops, int(cumulative))

    def set_retransmits(self, value: int):
        with self._lock:
            self.retransmits = value

    def set_error(self, message: str):
        with self._lock:
            self.error = message

    def set_observed_trial(self, trial_id: int):
        with self._lock:
            trial_id = int(trial_id) & 0xFFFFFFFF
            self.observed_trial_id = trial_id
            # Sender loops already publish observation at every boundary. Make
            # that observation sufficient for a zero-packet sender record too.
            if trial_id:
                self._activate_trial_locked(
                    trial_id, "sent", time.monotonic_ns())

    def set_udp_backend(self, status: dict):
        with self._lock:
            self.udp_batch = dict(status or {})

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "index": self.index,
                "bytes_sent": self.bytes_sent,
                "packets_sent": self.packets_sent,
                "bytes_received": self.bytes_received,
                "packets_received": self.packets_received,
                "max_seq_seen": self.max_seq_seen,
                "packets_lost": self.packets_lost,
                "duplicates": self.duplicates,
                "out_of_order": self.out_of_order,
                "kernel_rxq_drops": (
                    self.kernel_rxq_drops if self.rxq_overflow_supported else None),
                "socket_sndbuf_bytes": self.socket_sndbuf_bytes,
                "socket_rcvbuf_bytes": self.socket_rcvbuf_bytes,
                "rxq_overflow_supported": self.rxq_overflow_supported,
                "trial_sent": dict(self.trial_sent),
                "trial_received": dict(self.trial_received),
                "retransmits": self.retransmits,
                "error": self.error,
                "observed_trial_id": self.observed_trial_id,
                "udp_batch": dict(self.udp_batch),
            }

    def merge_peer_snapshot(self, peer: dict):
        """Merge role-specific counters relayed from the peer (server) for
        this stream, e.g. bytes_received when the peer is the receiver."""
        with self._lock:
            if peer.get("bytes_sent", 0) > self.bytes_sent:
                self.bytes_sent = peer["bytes_sent"]
            self.packets_sent = max(
                self.packets_sent, int(peer.get("packets_sent", 0) or 0))
            peer_trial_sent = peer.get("trial_sent")
            if isinstance(peer_trial_sent, dict):
                peer_id = int(peer_trial_sent.get("trial_id", 0)) & 0xFFFFFFFF
                current_id = int(self.trial_sent.get("trial_id", 0)) & 0xFFFFFFFF
                if (not self._trial_sent_active
                        or self._newer_trial_id(peer_id, current_id)
                        or (peer_id == current_id
                            and int(peer_trial_sent.get("packets_sent", 0) or 0)
                            >= int(self.trial_sent.get("packets_sent", 0) or 0))):
                    self.trial_sent = dict(peer_trial_sent)
                    self._trial_sent_active = True
            if peer.get("bytes_received", 0) > self.bytes_received:
                self.bytes_received = peer["bytes_received"]
            if peer.get("bytes_received", 0) or peer.get("packets_received", 0):
                self.packets_received = max(
                    self.packets_received, int(peer.get("packets_received", 0) or 0))
                self.packets_lost = peer.get("packets_lost", self.packets_lost)
            self.duplicates = max(
                self.duplicates, int(peer.get("duplicates", 0) or 0))
            self.out_of_order = max(
                self.out_of_order, int(peer.get("out_of_order", 0) or 0))
            peer_trial_received = peer.get("trial_received")
            if isinstance(peer_trial_received, dict):
                peer_id = int(peer_trial_received.get("trial_id", 0)) & 0xFFFFFFFF
                current_id = int(self.trial_received.get("trial_id", 0)) & 0xFFFFFFFF
                peer_unique = int(peer_trial_received.get(
                    "unique_packets", peer_trial_received.get("packets_received", 0)) or 0)
                current_unique = int(self.trial_received.get(
                    "unique_packets", self.trial_received.get("packets_received", 0)) or 0)
                if (not self._trial_received_active
                        or self._newer_trial_id(peer_id, current_id)
                        or (peer_id == current_id and (
                            peer_unique >= current_unique
                            or int(peer_trial_received.get("duplicates", 0) or 0)
                            > int(self.trial_received.get("duplicates", 0) or 0)))):
                    self.trial_received = dict(peer_trial_received)
                    self._trial_received_active = True
            if peer.get("max_seq_seen", -1) > self.max_seq_seen:
                self.max_seq_seen = peer["max_seq_seen"]
            peer_rxq_support = peer.get("rxq_overflow_supported")
            peer_rxq_drops = peer.get("kernel_rxq_drops")
            if (peer_rxq_support is True
                    or ("rxq_overflow_supported" not in peer
                        and peer_rxq_drops is not None)):
                self.rxq_overflow_supported = True
            if (peer_rxq_drops is not None
                    and (peer_rxq_support is True
                         or "rxq_overflow_supported" not in peer)):
                self.kernel_rxq_drops = max(
                    self.kernel_rxq_drops, int(peer_rxq_drops))
            if peer.get("socket_sndbuf_bytes", 0):
                self.socket_sndbuf_bytes = peer["socket_sndbuf_bytes"]
            if peer.get("socket_rcvbuf_bytes", 0):
                self.socket_rcvbuf_bytes = peer["socket_rcvbuf_bytes"]
            if peer.get("retransmits", 0) > self.retransmits:
                self.retransmits = peer["retransmits"]
            if peer.get("error"):
                self.error = peer["error"]
            self._reconcile_peer_loss()


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
