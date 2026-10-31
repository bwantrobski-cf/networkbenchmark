"""Client-side throughput test orchestration.

Merges iperf3-style parallel streams (-P) and upload/download (-R) with
bbperf-style calibration -> running lifecycle and latency-under-load /
bufferbloat measurement (see transport.metrics.compute_bufferbloat).
"""

import copy
import collections
import ctypes
import errno
import math
import multiprocessing
import operator
import os
import queue
import socket
import threading
import time
import traceback
import uuid
from collections.abc import Mapping

from ..common import addr, constants, host_telemetry, logging_store, net, tcp_info
from ..diagnostics.pmtud import discover_path_mtu
from ..transport import (latency_probe, metrics, run_mode, shared_counters,
                         stream_worker, udp_batch)
from ..transport.control_connection import ControlChannel


def _validated_duration_sec(value, label):
    if isinstance(value, bool):
        raise ValueError(
            f"{label} must be a finite value greater than 0 and no more than "
            f"{constants.MAX_RUN_TIME_FAILSAFE_SEC} seconds")
    try:
        duration = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f"{label} must be a finite value greater than 0 and no more than "
            f"{constants.MAX_RUN_TIME_FAILSAFE_SEC} seconds") from exc
    if (not math.isfinite(duration) or duration <= 0
            or duration > constants.MAX_RUN_TIME_FAILSAFE_SEC):
        raise ValueError(
            f"{label} must be a finite value greater than 0 and no more than "
            f"{constants.MAX_RUN_TIME_FAILSAFE_SEC} seconds")
    return duration


def _validated_path_mtu(path_mtu):
    if (path_mtu is not None
            and (isinstance(path_mtu, bool)
                 or not isinstance(path_mtu, int)
                 or not constants.PMTUD_MIN_MTU
                 <= path_mtu <= constants.PMTUD_JUMBO_MAX_MTU)):
        raise ValueError(
            f"path_mtu must be an integer from {constants.PMTUD_MIN_MTU} "
            f"to {constants.PMTUD_JUMBO_MAX_MTU}")
    return path_mtu


class ThroughputClientConfig:
    def __init__(self, server_ip, control_port=None, transport="tcp", direction="up",
                 streams=1, duration_sec=constants.DEFAULT_TEST_DURATION_SEC,
                 start_port=None, bind=None, calibration_sec=constants.CALIBRATION_TIME_SEC,
                 probe_interval_sec=constants.PROBE_INTERVAL_SEC, procs=1,
                 acceptable_loss_pct=None, max_duration_sec=None, path_mtu=None,
                 pmtud_info=None, ipv6=False,
                 udp_start_mbps=constants.UDP_TUNE_START_MBPS,
                 jumbo_mtu=False):
        self.server_host = server_ip
        self.control_port = control_port or constants.DEFAULT_CONTROL_PORT
        self.transport = transport
        self.direction = direction
        self.ipv6 = bool(ipv6)
        self.family = net.family_for_ipv6(self.ipv6)
        self.ip_version = net.ip_version_label(family=self.family)
        self.streams = int(streams)
        if not 1 <= self.streams <= constants.MAX_PARALLEL_STREAMS:
            raise ValueError(
                f"streams must be from 1 to {constants.MAX_PARALLEL_STREAMS}")
        self.duration_sec = _validated_duration_sec(duration_sec, "duration")
        default_start_port = constants.DEFAULT_UDP_PORT if transport == "udp" else constants.DEFAULT_TCP_PORT
        self.start_port = start_port or default_start_port
        self.bind = ("::" if self.ipv6 and bind in (None, "", "0.0.0.0")
                     else ("0.0.0.0" if bind in (None, "") else bind))
        self.calibration_sec = calibration_sec
        self.probe_interval_sec = probe_interval_sec
        # Number of worker processes to spread streams across (1 = threaded,
        # the default). Clamped to the stream count at run time.
        self.procs = max(1, int(procs))
        # UDP loss-target tuning: when acceptable_loss_pct is set (UDP only),
        # the client runs a multiplicative rate search to find the max rate meeting the
        # target, and duration becomes a *max* time budget for the search.
        self.acceptable_loss_pct = (float(acceptable_loss_pct)
                                    if acceptable_loss_pct is not None else None)
        if (self.acceptable_loss_pct is not None
                and (not math.isfinite(self.acceptable_loss_pct)
                     or not 0 <= self.acceptable_loss_pct <= 100)):
            raise ValueError("acceptable UDP loss must be a finite percentage from 0 to 100")
        self.max_duration_sec = _validated_duration_sec(
            max_duration_sec if max_duration_sec is not None
            else constants.DEFAULT_MAX_DURATION_SEC,
            "maximum duration",
        )
        self.udp_start_mbps = float(udp_start_mbps)
        if (not math.isfinite(self.udp_start_mbps)
                or not 0 < self.udp_start_mbps <= constants.UDP_TUNE_MAX_RATE_MBPS):
            raise ValueError(
                f"UDP tuning start rate must be greater than 0 and no more than "
                f"{constants.UDP_TUNE_MAX_RATE_MBPS:g} Mbps")
        self.path_mtu = path_mtu
        self.pmtud_info = pmtud_info
        self.jumbo_mtu = bool(jumbo_mtu)

    @property
    def path_mtu(self):
        return self._path_mtu

    @path_mtu.setter
    def path_mtu(self, value):
        self._path_mtu = _validated_path_mtu(value)

    @property
    def tune_udp(self) -> bool:
        """True when this run should perform the UDP loss-target rate ramp."""
        return self.transport == "udp" and self.acceptable_loss_pct is not None

    @property
    def sender_duration_sec(self) -> float:
        """How long senders should stay alive. During tuning they must run for
        the whole search budget (the controller stops them early); otherwise
        the fixed test duration."""
        if not self.tune_udp:
            return self.duration_sec
        return min(
            self.max_duration_sec + constants.UDP_TUNE_CONTROL_GRACE_SEC,
            constants.MAX_RUN_TIME_FAILSAFE_SEC,
        )

    @property
    def initial_rate_per_stream_bps(self) -> float:
        """Start tuned UDP senders paced so they cannot burst before tuning."""
        if not self.tune_udp:
            return 0.0
        return (self.udp_start_mbps * 1e6 / 8.0) / self.streams


class ThroughputError(Exception):
    pass


def _control_connection_error(cfg, server_ip, exc):
    endpoint = (f"[{server_ip}]:{cfg.control_port}"
                if cfg.family == socket.AF_INET6 else f"{server_ip}:{cfg.control_port}")
    error_number = getattr(exc, "errno", None)
    if (isinstance(exc, ConnectionRefusedError)
            or error_number == errno.ECONNREFUSED):
        detail = ("connection refused; verify nperf server is running and "
                  f"TCP port {cfg.control_port} is allowed")
    elif (isinstance(exc, (socket.timeout, TimeoutError))
          or error_number == errno.ETIMEDOUT):
        detail = ("connection timed out; verify routing, firewall rules, and "
                  f"TCP port {cfg.control_port}")
    elif error_number in (errno.EHOSTUNREACH, errno.ENETUNREACH):
        detail = (f"no route to the host or network; verify {cfg.ip_version} "
                  "routing and firewall rules")
    elif error_number == errno.EADDRNOTAVAIL:
        detail = (f"the local {cfg.ip_version} address is unavailable; verify "
                  f"{cfg.ip_version} is configured on this host")
    else:
        detail = str(exc) or type(exc).__name__
    return ThroughputError(
        f"unable to connect to NPERF {cfg.ip_version} control channel at "
        f"{endpoint}: {detail}")


def _operational_throughput_error(exc):
    if isinstance(exc, EOFError):
        return ThroughputError(f"control connection closed by the server: {exc}")
    if isinstance(exc, (socket.timeout, TimeoutError)):
        return ThroughputError(f"network operation timed out: {exc}")
    return ThroughputError(f"network operation failed: {exc}")


_UINT32_MAX = (1 << 32) - 1
_HOST_TELEMETRY_SCHEMA_VERSION = 1
_UDP_ERROR_COUNTERS = (
    "InErrors",
    "RcvbufErrors",
    "SndbufErrors",
    "NoPorts",
    "InCsumErrors",
    "IgnoredMulti",
    "MemErrors",
)


def _trial_record_name(role):
    if role in ("sent", "sender", "trial_sent"):
        return "trial_sent"
    if role in ("received", "receiver", "trial_received"):
        return "trial_received"
    raise ValueError(f"unknown trial role: {role!r}")


def _coerce_trial_id(value):
    try:
        if isinstance(value, bool):
            return None
        trial_id = operator.index(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return trial_id if 0 <= trial_id <= _UINT32_MAX else None


def _aggregate_trial_records(streams, trial_id, role):
    """Aggregate same-ID nested records while retaining per-stream details."""
    record_name = _trial_record_name(role)
    trial_id = _coerce_trial_id(trial_id)
    if trial_id is None:
        return None
    if isinstance(streams, Mapping):
        streams = streams.values()

    records = []
    for stream in streams or []:
        if not isinstance(stream, Mapping):
            continue
        record = stream.get(record_name)
        if not isinstance(record, Mapping):
            continue
        if _coerce_trial_id(record.get("trial_id")) != trial_id:
            continue
        item = copy.deepcopy(dict(record))
        item["index"] = stream.get("index")
        records.append(item)
    if not records:
        return None

    records.sort(key=lambda item: (
        item.get("index") is None,
        item.get("index") if isinstance(item.get("index"), int) else 0,
    ))
    first_values = [item.get("first_seq") for item in records
                    if item.get("first_seq") is not None]
    last_values = [item.get("last_seq") for item in records
                   if item.get("last_seq") is not None]
    result = {
        "trial_id": trial_id,
        "record": record_name,
        "stream_count": len(records),
        "stream_indices": [item.get("index") for item in records],
        "first_seq": min(first_values) if first_values else None,
        "last_seq": max(last_values) if last_values else None,
        "streams": records,
    }
    activated_values = [int(item["activated_at_ns"]) for item in records
                        if item.get("activated_at_ns") is not None]
    deactivated_values = [int(item["deactivated_at_ns"]) for item in records
                          if item.get("deactivated_at_ns") is not None]
    result.update({
        "activated_at_ns": min(activated_values) if activated_values else None,
        "deactivated_at_ns": max(deactivated_values) if deactivated_values else None,
    })
    if record_name == "trial_sent":
        first_sent_values = [int(item["first_sent_ns"]) for item in records
                             if item.get("first_sent_ns") is not None]
        last_sent_values = [int(item["last_sent_ns"]) for item in records
                            if item.get("last_sent_ns") is not None]
        result.update({
            "bytes_sent": sum(max(0, int(item.get("bytes_sent", 0) or 0))
                              for item in records),
            "packets_sent": sum(max(0, int(item.get("packets_sent", 0) or 0))
                                 for item in records),
            "first_sent_ns": min(first_sent_values) if first_sent_values else None,
            "last_sent_ns": max(last_sent_values) if last_sent_values else None,
        })
    else:
        unique_packets = sum(max(0, int(
            item.get("unique_packets", item.get("packets_received", 0)) or 0))
                             for item in records)
        result.update({
            "bytes_received": sum(max(0, int(item.get("bytes_received", 0) or 0))
                                  for item in records),
            "packets_received": unique_packets,
            "unique_packets": unique_packets,
            "packets_lost": sum(max(0, int(item.get("packets_lost", 0) or 0))
                                for item in records),
            "duplicates": sum(max(0, int(item.get("duplicates", 0) or 0))
                              for item in records),
            "out_of_order": sum(max(0, int(item.get("out_of_order", 0) or 0))
                                for item in records),
        })
    return result


def _set_local_trial_boundary(counters, trial_id, role, boundary_ns,
                              activated):
    method_name = "activate_trial" if activated else "deactivate_trial"
    keyword = "activated_at_ns" if activated else "deactivated_at_ns"
    for counter in counters.values():
        method = getattr(counter, method_name)
        method(trial_id, role, **{keyword: boundary_ns})


class _PeerStatsTracker:
    def __init__(self):
        self._condition = threading.Condition()
        self._streams = {}
        self._timestamp = None
        self._generation = 0
        self._trial_records = {"trial_sent": {}, "trial_received": {}}
        self._boundaries = collections.deque(maxlen=8)
        self._first_host = None
        self._latest_host = None
        self._final = None

    def update(self, streams, timestamp=None, host_sample=None, final=False):
        with self._condition:
            copied_streams = []
            self._generation += 1
            generation = self._generation
            for stream in streams or []:
                if not isinstance(stream, Mapping):
                    continue
                stream = copy.deepcopy(dict(stream))
                copied_streams.append(stream)
                index = stream.get("index")
                if index is not None:
                    self._streams[index] = stream
                    for record_name in self._trial_records:
                        record = stream.get(record_name)
                        if not isinstance(record, Mapping):
                            continue
                        trial_id = _coerce_trial_id(record.get("trial_id"))
                        if trial_id is None:
                            continue
                        by_stream = self._trial_records[record_name].setdefault(
                            trial_id, {})
                        by_stream[index] = {
                            "record": copy.deepcopy(dict(record)),
                            "generation": generation,
                            "timestamp": timestamp,
                            "final": bool(final),
                        }
            self._timestamp = timestamp if timestamp is not None else time.time()
            boundary = {
                "generation": generation,
                "timestamp": self._timestamp,
                "streams": copied_streams,
                "host_sample": copy.deepcopy(host_sample),
                "final": bool(final),
            }
            self._boundaries.append(boundary)
            if host_sample is not None:
                if self._first_host is None:
                    self._first_host = copy.deepcopy(boundary)
                self._latest_host = copy.deepcopy(boundary)
            if final:
                self._final = copy.deepcopy(boundary)
            self._condition.notify_all()

    def generation(self):
        with self._condition:
            return self._generation

    def snapshot(self):
        with self._condition:
            return self._snapshot_locked()

    def latest_streams(self):
        with self._condition:
            return [copy.deepcopy(self._streams[index])
                    for index in sorted(self._streams)]

    def aggregate_trial(self, trial_id, role, after_generation=None):
        record_name = _trial_record_name(role)
        trial_id = _coerce_trial_id(trial_id)
        if trial_id is None:
            return None
        with self._condition:
            entries = self._trial_records[record_name].get(trial_id, {})
            streams = []
            generations = []
            for index, entry in entries.items():
                generation = entry["generation"]
                if after_generation is not None and generation <= after_generation:
                    continue
                streams.append({
                    "index": index,
                    record_name: copy.deepcopy(entry["record"]),
                })
                generations.append(generation)
            result = _aggregate_trial_records(streams, trial_id, record_name)
            if result is not None:
                result["first_observed_generation"] = min(generations)
                result["last_observed_generation"] = max(generations)
            return result

    def latest_boundary(self):
        with self._condition:
            return copy.deepcopy(self._boundaries[-1]) if self._boundaries else None
    def first_host_boundary(self):
        with self._condition:
            return copy.deepcopy(self._first_host)

    def latest_host_boundary(self):
        with self._condition:
            return copy.deepcopy(self._latest_host)

    def final_boundary(self):
        with self._condition:
            return copy.deepcopy(self._final)

    def wait_for_update(self, after_generation, timeout_sec):
        deadline = time.time() + timeout_sec
        with self._condition:
            while self._generation <= after_generation:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                self._condition.wait(timeout=remaining)
            return self._snapshot_locked()

    def _snapshot_locked(self):
        if not self._streams:
            return None
        totals = {"bytes_sent": 0, "packets_sent": 0, "bytes_received": 0,
                  "packets_received": 0, "packets_lost": 0, "retransmits": 0}
        for stream in self._streams.values():
            for key in totals:
                totals[key] += stream.get(key, 0) or 0
        return {"generation": self._generation, "timestamp": self._timestamp, **totals}


class _TrialAckTracker:
    def __init__(self):
        self._condition = threading.Condition()
        self._generation = 0
        self._trial_id = None
        self._error = None

    def generation(self):
        with self._condition:
            return self._generation

    def update(self, trial_id):
        with self._condition:
            self._generation += 1
            self._trial_id = _coerce_trial_id(trial_id)
            self._condition.notify_all()

    def fail(self, error):
        with self._condition:
            self._error = str(error or "server trial synchronization failed")
            self._condition.notify_all()

    def wait_for(self, trial_id, after_generation, timeout):
        trial_id = _coerce_trial_id(trial_id)
        deadline = time.monotonic() + timeout
        with self._condition:
            while not (self._generation > after_generation
                       and self._trial_id == trial_id):
                if self._error is not None:
                    raise ThroughputError(self._error)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True


def _wait_for_observed_trial(counters, trial_id, timeout_sec, check_workers=None):
    expected = set(counters)
    deadline = time.monotonic() + timeout_sec
    while True:
        observed = {
            index for index, counter in counters.items()
            if counter.snapshot().get("observed_trial_id") == trial_id
        }
        if observed == expected:
            return
        if check_workers is not None:
            check_workers()
        if time.monotonic() >= deadline:
            raise ThroughputError(
                f"UDP senders did not observe trial {trial_id}: "
                f"missing_streams={sorted(expected - observed)}")
        time.sleep(0.002)


def _udp_trial_metrics(direction, local_start, local_end, peer_start, peer_end,
                       local_duration, peer_duration):
    peer_available = peer_start is not None and peer_end is not None
    if direction == "up" and not peer_available:
        return None
    if direction == "down" and not peer_available:
        return None

    local_dt = max(1e-6, local_duration)
    peer_dt = max(1e-6, peer_duration) if peer_available else local_dt
    if direction == "up":
        offered_start, offered_end, offered_dt = local_start, local_end, local_dt
        received_start, received_end, received_dt = peer_start, peer_end, peer_dt
    else:
        offered_start, offered_end, offered_dt = peer_start, peer_end, peer_dt
        received_start, received_end, received_dt = local_start, local_end, local_dt

    def delta(snapshot_end, snapshot_start, key):
        return max(0, (snapshot_end or {}).get(key, 0) - (snapshot_start or {}).get(key, 0))

    sent_bytes = delta(offered_end, offered_start, "bytes_sent")
    sent_packets = delta(offered_end, offered_start, "packets_sent")
    received_bytes = delta(received_end, received_start, "bytes_received")
    received_packets = delta(received_end, received_start, "packets_received")
    observed_lost_packets = delta(received_end, received_start, "packets_lost")
    # Periodic peer telemetry may make sender and receiver windows differ by a
    # fraction of an interval. Normalize the constant sender rate to the receiver
    # duration, then combine it with receiver sequence gaps. The sender estimate
    # catches trailing or complete loss that has no later sequence to expose it.
    estimated_sent_packets = round((sent_packets / offered_dt) * received_dt)
    expected = max(received_packets + observed_lost_packets, estimated_sent_packets)
    lost_packets = max(observed_lost_packets, expected - received_packets)
    loss_pct = (min(expected, lost_packets) / expected * 100.0) if expected else 0.0
    return {
        "offered_mbps": sent_bytes * 8 / offered_dt / 1e6,
        "goodput_mbps": received_bytes * 8 / received_dt / 1e6,
        "offered_pps": sent_packets / offered_dt,
        "goodput_pps": received_packets / received_dt,
        "loss_pct": loss_pct,
    }


def _exact_udp_trial_metrics(direction, trial_id, local_streams, peer_stats,
                              active_duration_sec, peer_after_generation=None,
                              expected_stream_count=None):
    """Build one UDP result from exact same-ID records and trial boundaries."""
    local_sent = _aggregate_trial_records(local_streams, trial_id, "trial_sent")
    local_received = _aggregate_trial_records(
        local_streams, trial_id, "trial_received")
    try:
        peer_sent = peer_stats.aggregate_trial(
            trial_id, "trial_sent", after_generation=peer_after_generation)
        peer_received = peer_stats.aggregate_trial(
            trial_id, "trial_received", after_generation=peer_after_generation)
    except (AttributeError, TypeError, ValueError):
        peer_sent = None
        peer_received = None

    if direction == "up":
        sender = local_sent
        receiver = peer_received
        required_peer_role = "trial_received"
    else:
        sender = peer_sent
        receiver = local_received
        required_peer_role = "trial_sent"
    if sender is None:
        return None

    try:
        duration = float(active_duration_sec)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ThroughputError("UDP trial activation window is invalid") from exc
    if not math.isfinite(duration) or duration <= 0:
        raise ThroughputError("UDP trial activation window is invalid")

    if expected_stream_count is not None:
        expected_indices = set(range(int(expected_stream_count)))
        sender_indices = set(sender.get("stream_indices") or [])
        if sender_indices != expected_indices:
            missing = sorted(expected_indices - sender_indices)
            unexpected = sorted(
                index for index in sender_indices - expected_indices
                if isinstance(index, int))
            raise ThroughputError(
                "UDP trial sender records are incomplete: "
                f"missing_streams={missing} unexpected_streams={unexpected}")

        minimum_duration = (constants.UDP_TUNE_TRIAL_SEC
                            * constants.UDP_TUNE_MIN_TRIAL_DURATION_RATIO)
        if duration < minimum_duration:
            raise ThroughputError(
                "UDP trial activation window is shorter than the measurement "
                f"window: active_sec={duration:.3f} "
                f"minimum_sec={minimum_duration:.3f}")

    measurement_clock = "trial_boundary_monotonic"
    sent_bytes = sender.get("bytes_sent", 0)
    sent_packets = sender.get("packets_sent", 0)
    received_bytes = receiver.get("bytes_received", 0) if receiver else 0
    unique_packets = receiver.get("unique_packets", 0) if receiver else 0
    duplicates = receiver.get("duplicates", 0) if receiver else 0
    out_of_order = receiver.get("out_of_order", 0) if receiver else 0
    receiver_missing = receiver.get("packets_lost", 0) if receiver else 0
    missing_packets = max(0, sent_packets - unique_packets)
    unexpected_unique = max(0, unique_packets - sent_packets)
    if unexpected_unique:
        raise ThroughputError(
            "UDP trial receiver reported more unique packets than the sender: "
            f"sent={sent_packets} received={unique_packets}")
    loss_pct = (missing_packets / sent_packets * 100.0) if sent_packets else 100.0
    if unique_packets == 0:
        loss_pct = 100.0

    def sequence_streams(aggregate):
        if aggregate is None:
            return []
        return [{
            "index": item.get("index"),
            "first_seq": item.get("first_seq"),
            "last_seq": item.get("last_seq"),
        } for item in aggregate.get("streams", [])]

    return {
        "trial_id": trial_id,
        "measurement_sec": duration,
        "measurement_clock": measurement_clock,
        "offered_mbps": sent_bytes * 8 / duration / 1e6,
        "goodput_mbps": received_bytes * 8 / duration / 1e6,
        "offered_pps": sent_packets / duration,
        "goodput_pps": unique_packets / duration,
        "sent_bytes": sent_bytes,
        "sent_packets": sent_packets,
        "received_bytes": received_bytes,
        "received_packets": unique_packets,
        "unique_received_packets": unique_packets,
        "lost_packets": missing_packets,
        "packets_lost": missing_packets,
        "missing_packets": missing_packets,
        "receiver_reported_missing_packets": receiver_missing,
        "unexpected_unique_packets": unexpected_unique,
        "duplicates": duplicates,
        "duplicate_packets": duplicates,
        "out_of_order": out_of_order,
        "reordered_packets": out_of_order,
        "sender_first_seq": sender.get("first_seq"),
        "sender_last_seq": sender.get("last_seq"),
        "receiver_first_seq": receiver.get("first_seq") if receiver else None,
        "receiver_last_seq": receiver.get("last_seq") if receiver else None,
        "loss_pct": loss_pct,
        "activated_at_ns": sender.get("activated_at_ns"),
        "deactivated_at_ns": sender.get("deactivated_at_ns"),
        "peer_trial_role": required_peer_role,
        "peer_trial_data": ((peer_received is not None) if direction == "up"
                            else (peer_sent is not None)),
        "sequence_diagnostics": {
            "sender_first_seq": sender.get("first_seq"),
            "sender_last_seq": sender.get("last_seq"),
            "receiver_first_seq": receiver.get("first_seq") if receiver else None,
            "receiver_last_seq": receiver.get("last_seq") if receiver else None,
            "sender_streams": sequence_streams(sender),
            "receiver_streams": sequence_streams(receiver),
            "duplicates": duplicates,
            "out_of_order": out_of_order,
            "missing": missing_packets,
            "receiver_reported_missing": receiver_missing,
        },
    }


def _unavailable_endpoint(reason="unavailable", error=None, pid=None,
                          tested_family=None):
    return {
        "available": False,
        "complete": False,
        "reason": reason,
        "error": error,
        "captured_at": time.monotonic(),
        "pid": pid,
        "tested_family": host_telemetry.normalize_ip_family(tested_family),
        "host_cpu": None,
        "process_cpu": None,
        "udp": None,
    }


def _snapshot_client_endpoint(topology=None, tested_family=None):
    """Capture local telemetry without allowing observability to fail a run."""
    pid = os.getpid()
    pids = [
        worker.get("pid") for worker in (topology or {}).get("workers", [])
        if isinstance(worker, Mapping) and isinstance(worker.get("pid"), int)
    ] or [pid]
    tested_family = host_telemetry.normalize_ip_family(
        tested_family or (topology or {}).get("tested_family")
        or (topology or {}).get("ip_version"))
    try:
        sample = host_telemetry.snapshot_endpoint(
            pid=pid, pids=pids, tested_family=tested_family)
        if not isinstance(sample, Mapping):
            raise TypeError("snapshot_endpoint returned a non-dictionary value")
        sample = copy.deepcopy(dict(sample))
    except Exception as exc:
        sample = _unavailable_endpoint(
            error=f"{type(exc).__name__}: {exc}", pid=pid,
            tested_family=tested_family)
    if tested_family is not None:
        sample["tested_family"] = tested_family
    if topology is not None:
        sample["topology"] = copy.deepcopy(topology)
    return sample


def _diff_endpoint(before, after, tested_family=None):
    tested_family = host_telemetry.normalize_ip_family(tested_family)
    if tested_family is not None:
        before = copy.deepcopy(dict(before)) if isinstance(before, Mapping) else before
        after = copy.deepcopy(dict(after)) if isinstance(after, Mapping) else after
        if isinstance(before, dict):
            before["tested_family"] = tested_family
        if isinstance(after, dict):
            after["tested_family"] = tested_family
    try:
        result = host_telemetry.diff_endpoint(before, after)
        if not isinstance(result, Mapping):
            raise TypeError("diff_endpoint returned a non-dictionary value")
        return copy.deepcopy(dict(result))
    except Exception as exc:
        result = _unavailable_endpoint(
            reason="invalid_snapshot", error=f"{type(exc).__name__}: {exc}",
            tested_family=tested_family)
        result["elapsed_sec"] = None
        return result


def _compact_endpoint_delta(delta):
    delta = delta if isinstance(delta, Mapping) else {}
    host_cpu = delta.get("host_cpu")
    host_cpu = host_cpu if isinstance(host_cpu, Mapping) else {}
    host = host_cpu.get("host")
    host = host if isinstance(host, Mapping) else {}
    process = delta.get("process_cpu")
    process = process if isinstance(process, Mapping) else {}
    udp = delta.get("udp")
    udp = udp if isinstance(udp, Mapping) else {}
    tested_family = host_telemetry.normalize_ip_family(
        delta.get("tested_family") or udp.get("tested_family")
        or udp.get("active_family"))
    udp_counters = None
    family_udp = udp.get(tested_family) if tested_family is not None else None
    family_present = tested_family is not None and tested_family in udp
    if isinstance(family_udp, Mapping) and family_udp.get("available") is not False:
        family_counters = family_udp.get("counters")
        if isinstance(family_counters, Mapping):
            udp_counters = family_counters
        elif any(name in family_udp for name in _UDP_ERROR_COUNTERS):
            udp_counters = family_udp
    if udp_counters is None and not family_present:
        udp_total = udp.get("total")
        udp_counters = udp_total if isinstance(udp_total, Mapping) else {}
    elif udp_counters is None:
        udp_counters = {}
    return {
        "available": bool(delta.get("available")),
        "complete": bool(delta.get("complete")),
        "reason": delta.get("reason"),
        "elapsed_sec": delta.get("elapsed_sec"),
        "tested_family": tested_family,
        "cpu": {
            "user_pct": host.get("user_pct"),
            "system_pct": host.get("system_pct"),
            "softirq_pct": host.get("softirq_pct"),
            "busy_pct": host.get("busy_pct"),
        },
        "process_cpu": {
            "user_pct": process.get("user_pct"),
            "system_pct": process.get("system_pct"),
            "cpu_pct": process.get("cpu_pct"),
        },
        "udp_errors": {name: udp_counters.get(name) for name in _UDP_ERROR_COUNTERS},
    }


def _stream_socket_records(streams, requested_buffer_bytes=None):
    records = []
    if isinstance(streams, Mapping):
        streams = streams.values()
    for stream in streams or []:
        if not isinstance(stream, Mapping):
            continue
        rxq_support = stream.get("rxq_overflow_supported")
        if ("rxq_overflow_supported" not in stream
                and stream.get("kernel_rxq_drops") is not None):
            rxq_support = True
        records.append({
            "index": stream.get("index"),
            "requested_sndbuf_bytes": requested_buffer_bytes,
            "requested_rcvbuf_bytes": requested_buffer_bytes,
            "actual_sndbuf_bytes": stream.get("socket_sndbuf_bytes"),
            "actual_rcvbuf_bytes": stream.get("socket_rcvbuf_bytes"),
            "rxq_overflow_supported": rxq_support,
            "kernel_rxq_drops": stream.get("kernel_rxq_drops"),
        })
    return sorted(records, key=lambda item: (
        item.get("index") is None,
        item.get("index") if isinstance(item.get("index"), int) else 0,
    ))


def _socket_overflow_delta(before_streams, after_streams):
    if before_streams is None or after_streams is None:
        return {"available": False, "total": None, "streams": []}

    def by_index(streams):
        return {stream.get("index"): stream for stream in streams or []
                if isinstance(stream, Mapping) and stream.get("index") is not None}

    before = by_index(before_streams)
    after = by_index(after_streams)
    records = []
    for index in sorted(set(before).intersection(after)):
        before_support = before[index].get("rxq_overflow_supported")
        after_support = after[index].get("rxq_overflow_supported")
        if before_support is True and after_support is True:
            supported = True
        elif before_support is False or after_support is False:
            supported = False
        else:
            supported = None
        delta = None
        try:
            start_value = before[index].get("kernel_rxq_drops")
            end_value = after[index].get("kernel_rxq_drops")
            if supported and start_value is not None and end_value is not None:
                start = max(0, int(start_value))
                end = max(0, int(end_value))
                delta = max(0, end - start)
        except (TypeError, ValueError, OverflowError):
            supported = None
            delta = None
        records.append({
            "index": index,
            "rxq_overflow_supported": supported,
            "kernel_rxq_drops": delta,
        })
    complete = bool(records) and set(before) == set(after) and all(
        item["rxq_overflow_supported"] is True
        and item["kernel_rxq_drops"] is not None
        for item in records)
    support = (True if complete else
               False if (records and set(before) == set(after)
                         and all(item["rxq_overflow_supported"] is False
                                 for item in records)) else None)
    return {
        "available": complete,
        "complete": complete,
        "rxq_overflow_supported": support,
        "total": (sum(item["kernel_rxq_drops"] for item in records)
                  if complete else None),
        "streams": records,
    }


def _boundary_host_sample(boundary):
    return boundary.get("host_sample") if isinstance(boundary, Mapping) else None


def _boundary_streams(boundary):
    if not isinstance(boundary, Mapping):
        return None
    streams = boundary.get("streams")
    return streams if isinstance(streams, list) else None


def _trial_host_telemetry(direction, client_start, client_end,
                          client_streams_start, client_streams_end,
                          server_start_boundary, server_end_boundary,
                          tested_family=None):
    client_delta = _diff_endpoint(
        client_start, client_end, tested_family=tested_family)
    server_delta = _diff_endpoint(
        _boundary_host_sample(server_start_boundary),
        _boundary_host_sample(server_end_boundary),
        tested_family=tested_family,
    )
    client_socket = _socket_overflow_delta(
        client_streams_start, client_streams_end)
    server_socket = _socket_overflow_delta(
        _boundary_streams(server_start_boundary),
        _boundary_streams(server_end_boundary),
    )
    roles = ({"sender": "client", "receiver": "server"}
             if direction == "up"
             else {"sender": "server", "receiver": "client"})
    endpoint_deltas = {"client": client_delta, "server": server_delta}
    socket_deltas = {"client": client_socket, "server": server_socket}
    sender = roles["sender"]
    receiver = roles["receiver"]
    payload = {
        "schema_version": _HOST_TELEMETRY_SCHEMA_VERSION,
        "tested_family": host_telemetry.normalize_ip_family(tested_family),
        "roles": roles,
        "client": _compact_endpoint_delta(client_delta),
        "server": _compact_endpoint_delta(server_delta),
        "socket_overflow_deltas": {
            "client": client_socket,
            "server": server_socket,
            "sender": socket_deltas[sender].get("total"),
            "receiver": socket_deltas[receiver].get("total"),
        },
    }
    classifier_sockets = {
        "sender": socket_deltas[sender].get("total"),
        "receiver": socket_deltas[receiver].get("total"),
    }
    return (payload, endpoint_deltas[sender], endpoint_deltas[receiver],
            classifier_sockets)


def _classify_trial(target_mbps, offered_mbps, loss_pct, acceptable_loss_pct,
                    sender_delta, receiver_delta, socket_deltas,
                    tested_family=None):
    try:
        classification = host_telemetry.classify_bottleneck(
            target_mbps, offered_mbps, sender_delta, receiver_delta,
            socket_deltas, loss_pct=loss_pct, tested_family=tested_family)
        if not isinstance(classification, Mapping):
            raise TypeError("classify_bottleneck returned a non-dictionary value")
        classification = copy.deepcopy(dict(classification))
    except Exception as exc:
        classification = {
            "primary": "unknown",
            "scope": "unknown",
            "confidence": "low",
            "evidence": ["classification_unavailable"],
            "error": f"{type(exc).__name__}: {exc}",
            "sender_attainment": {
                "primary": "unknown", "scope": "unknown",
                "confidence": "low", "evidence": ["classification_unavailable"],
            },
            "loss_causality": {
                "primary": "unknown", "scope": "unknown",
                "confidence": "low", "evidence": ["classification_unavailable"],
                "loss_pct": loss_pct,
            },
        }

    try:
        target = float(target_mbps)
        offered = float(offered_mbps)
        attainment = offered / target if target > 0 else None
    except (TypeError, ValueError, OverflowError, ZeroDivisionError):
        attainment = None
    attained = (attainment >= host_telemetry.TARGET_ATTAINMENT_RATIO
                if attainment is not None and math.isfinite(attainment) else None)
    classification.update({
        "target_mbps": target_mbps,
        "offered_mbps": offered_mbps,
        "target_attained": attained,
        "target_attainment_pct": attainment * 100.0 if attainment is not None else None,
        "target_threshold_pct": host_telemetry.TARGET_ATTAINMENT_RATIO * 100.0,
        "acceptable_loss_pct": acceptable_loss_pct,
    })

    loss_breached = (loss_pct is not None and acceptable_loss_pct is not None
                     and loss_pct > acceptable_loss_pct)
    classification["loss_target_breached"] = loss_breached
    if loss_breached:
        loss_causality = classification.setdefault("loss_causality", {})
        loss_evidence = list(loss_causality.get("evidence") or [])
        loss_causality["evidence"] = list(dict.fromkeys(
            loss_evidence + ["loss_target_breached"]))
        classification["evidence"] = list(dict.fromkeys(
            list(classification.get("evidence") or [])
            + ["loss_target_breached"]))
    return classification


def _build_whole_run_host_telemetry(direction, client_baseline, client_end,
                                    peer_stats, client_topology,
                                    tested_family=None):
    try:
        server_baseline_boundary = peer_stats.first_host_boundary()
        server_final_boundary = peer_stats.final_boundary()
        server_end_boundary = server_final_boundary
        if (server_end_boundary is None
                or server_end_boundary.get("host_sample") is None):
            server_end_boundary = peer_stats.latest_host_boundary()
    except Exception:
        server_baseline_boundary = None
        server_final_boundary = None
        server_end_boundary = None
    server_baseline = _boundary_host_sample(server_baseline_boundary)
    server_end = _boundary_host_sample(server_end_boundary)
    server_topology = (server_end.get("topology")
                       if isinstance(server_end, Mapping) else None)
    return {
        "schema_version": _HOST_TELEMETRY_SCHEMA_VERSION,
        "tested_family": host_telemetry.normalize_ip_family(tested_family),
        "roles": ({"sender": "client", "receiver": "server"}
                  if direction == "up"
                  else {"sender": "server", "receiver": "client"}),
        "topology": {
            "client": copy.deepcopy(client_topology),
            "server": copy.deepcopy(server_topology),
        },
        "client": {
            "baseline": copy.deepcopy(client_baseline),
            "end": copy.deepcopy(client_end),
            "delta": _diff_endpoint(
                client_baseline, client_end, tested_family=tested_family),
        },
        "server": {
            "baseline": copy.deepcopy(server_baseline),
            "end": copy.deepcopy(server_end),
            "delta": _diff_endpoint(
                server_baseline, server_end, tested_family=tested_family),
            "final_sample": copy.deepcopy(
                _boundary_host_sample(server_final_boundary)),
        },
    }


def _build_socket_telemetry(client_streams, server_streams, transport="udp"):
    requested_buffer_bytes = (stream_worker.UDP_SOCKET_BUFFER_BYTES
                              if transport == "udp" else None)
    client = _stream_socket_records(client_streams, requested_buffer_bytes)
    server = _stream_socket_records(server_streams, requested_buffer_bytes)

    expected_indices = {
        item.get("index") for item in client + server
        if item.get("index") is not None
    }

    def endpoint(records):
        indices = {
            item.get("index") for item in records
            if item.get("index") is not None
        }
        complete_coverage = (bool(expected_indices)
                             and indices == expected_indices
                             and len(records) == len(expected_indices))
        supported = [item.get("rxq_overflow_supported") for item in records]
        complete = complete_coverage and all(value is True for value in supported)
        drops = [item.get("kernel_rxq_drops") for item in records]
        complete = complete and all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in drops)
        support = (True if complete else
                   False if (complete_coverage and supported
                             and all(value is False for value in supported))
                   else None)
        return {
            "available": complete,
            "complete": complete,
            "rxq_overflow_supported": support,
            "kernel_rxq_drops": (
                sum(max(0, value) for value in drops) if complete else None),
            "streams": records,
        }

    return {
        "requested_buffer_bytes": requested_buffer_bytes,
        "client": endpoint(client),
        "server": endpoint(server),
    }


def _discover_path_mtu(cfg: ThroughputClientConfig, server_ip: str, log_base_dir: str = None):
    pmtud_udp_port = cfg.start_port if cfg.transport == "udp" else constants.DEFAULT_UDP_PORT
    probe_max_mtu = (constants.PMTUD_JUMBO_MAX_MTU
                     if cfg.jumbo_mtu else constants.PMTUD_DEFAULT_MAX_MTU)
    pmtud_result = discover_path_mtu(
        server_ip,
        udp_port=pmtud_udp_port,
        max_mtu=probe_max_mtu,
        family=cfg.family,
        ipv6=cfg.ipv6,
    )
    pmtud_result.setdefault("ip_version", cfg.ip_version)
    pmtud_result["jumbo_mtu"] = cfg.jumbo_mtu
    pmtud_result.setdefault("probe_max_mtu", probe_max_mtu)
    pmtud_result.setdefault("probe_ceiling_reached", False)
    fallback_used = bool(pmtud_result.get("fallback_used"))
    text_lines = [
        f"PMTUD to {server_ip}:{pmtud_udp_port} ip_version={cfg.ip_version}",
        f"success={pmtud_result.get('success')} connectivity_confirmed={pmtud_result.get('connectivity_confirmed')}",
        f"path_mtu={pmtud_result.get('path_mtu')} payload={pmtud_result.get('path_mtu_payload_bytes')}",
        f"path_mtu_source={pmtud_result.get('path_mtu_source', 'pmtud')} fallback_used={fallback_used}",
        f"jumbo_mtu={cfg.jumbo_mtu} probe_max_mtu={pmtud_result.get('probe_max_mtu')} "
        f"probe_ceiling_reached={pmtud_result.get('probe_ceiling_reached')}",
        str(pmtud_result),
    ]
    pmtud_json_path, pmtud_log_path = logging_store.save_result(
        constants.TEST_TYPE_PMTUD, pmtud_result, text_lines, base_dir=log_base_dir)
    path_mtu = pmtud_result.get("path_mtu")
    if not isinstance(path_mtu, int):
        raise ThroughputError(pmtud_result.get("error") or "PMTUD discovery chain did not determine a path MTU")

    return path_mtu, {
        "server_ip": server_ip,
        "udp_port": pmtud_udp_port,
        "ip_version": cfg.ip_version,
        "success": bool(pmtud_result.get("success")),
        "path_mtu": path_mtu,
        "path_mtu_source": pmtud_result.get("path_mtu_source", "pmtud"),
        "fallback_used": fallback_used,
        "jumbo_mtu": cfg.jumbo_mtu,
        "probe_max_mtu": pmtud_result.get("probe_max_mtu"),
        "probe_ceiling_reached": bool(pmtud_result.get("probe_ceiling_reached")),
        "path_mtu_payload_bytes": pmtud_result.get("path_mtu_payload_bytes"),
        "reported_next_hop_mtu": pmtud_result.get("reported_next_hop_mtu"),
        "fallback_reason": pmtud_result.get("fallback_reason"),
        "bidirectional_validated": pmtud_result.get("bidirectional_validated"),
        "bidirectional_validation": pmtud_result.get("bidirectional_validation"),
        "icmp_pmtud": pmtud_result.get("icmp_pmtud"),
        "log_json": pmtud_json_path,
        "log_text": pmtud_log_path,
    }


def _client_dispatch_loop(control: ControlChannel, phase: run_mode.RunPhase,
                           rtt_tracker: metrics.RttTracker, counters: dict,
                           done_event: threading.Event, peer_stats=None,
                           final_stats_event: threading.Event = None, server_errors=None,
                           trial_acks=None, server_udp_backend=None):
    while not phase.is_stopped():
        try:
            msg = control.recv(timeout=1.0)
        except EOFError:
            phase.stop()
            done_event.set()
            if trial_acks is not None:
                trial_acks.fail("server closed during UDP trial synchronization")
            break
        if msg is None:
            continue

        mtype = msg.get("type")
        if mtype == "probe_ack":
            rtt_ms = latency_probe.rtt_ms_from_ack(msg)
            if rtt_ms is not None:
                rtt_tracker.add_sample(rtt_ms, phase.phase)
        elif mtype == "set_trial_ack":
            if trial_acks is not None:
                trial_acks.update(msg.get("trial_id"))
        elif mtype == "peer_stats":
            for snap in msg.get("streams", []):
                idx = snap.get("index")
                if idx in counters:
                    # Socket metadata belongs to an endpoint. Keep the local
                    # counter's values intact and retain peer values separately.
                    merged = dict(snap)
                    for key in ("kernel_rxq_drops", "socket_sndbuf_bytes",
                                "socket_rcvbuf_bytes", "rxq_overflow_supported"):
                        merged.pop(key, None)
                    counters[idx].merge_peer_snapshot(merged)
            if peer_stats is not None:
                peer_stats.update(
                    msg.get("streams", []), timestamp=msg.get("t"),
                    host_sample=msg.get("host_sample"))
        elif mtype == "final_stats":
            for snap in msg.get("streams", []):
                idx = snap.get("index")
                if idx in counters:
                    merged = dict(snap)
                    for key in ("kernel_rxq_drops", "socket_sndbuf_bytes",
                                "socket_rcvbuf_bytes", "rxq_overflow_supported"):
                        merged.pop(key, None)
                    counters[idx].merge_peer_snapshot(merged)
            if peer_stats is not None:
                peer_stats.update(
                    msg.get("streams", []), host_sample=msg.get("host_sample"),
                    final=True)
            final_backend = msg.get("udp_batch")
            if (server_udp_backend is not None
                    and isinstance(final_backend, Mapping)):
                server_udp_backend.clear()
                server_udp_backend.update(copy.deepcopy(dict(final_backend)))
            if final_stats_event is not None:
                final_stats_event.set()
            try:
                control.send("final_stats_ack")
            except OSError:
                pass
        elif mtype == "error":
            if server_errors is not None:
                server_errors.append(str(msg.get("error") or "server reported an unspecified error"))
            if trial_acks is not None:
                trial_acks.fail(msg.get("error"))
            done_event.set()
            break
        elif mtype == "done":
            done_event.set()
            break


def _wait_for_final_stats(done_event: threading.Event, final_stats_event: threading.Event,
                          timeout_sec: float) -> bool:
    deadline = time.monotonic() + timeout_sec
    while not final_stats_event.is_set() and not done_event.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        final_stats_event.wait(timeout=min(0.1, remaining))
    return final_stats_event.is_set()


def _connect_streams(cfg: ThroughputClientConfig, run_id: str, ports: list):
    """Establish all N data streams.

    Each stream binds to an OS-assigned ephemeral source port (bind port 0).
    Because every stream socket stays open simultaneously and we never enable
    SO_REUSEADDR/SO_REUSEPORT, the kernel guarantees a distinct source port per
    stream. We additionally capture and verify uniqueness explicitly so a
    collision (e.g. if the OS is misconfigured) fails loudly instead of
    silently multiplexing two streams onto one 5-tuple.

    Returns (streams, source_ports) where:
      streams:      dict index -> (sock, dest_addr)
      source_ports: dict index -> local source port (int)
    """
    streams = {}
    source_ports = {}
    used_ports = set()
    for i, port in enumerate(ports):
        if cfg.transport == "tcp":
            sock = socket.socket(cfg.family, socket.SOCK_STREAM)
            sock.bind(net.sockaddr(cfg.bind, 0, cfg.family))
            stream_worker.configure_tcp_socket(
                sock, cfg.path_mtu, total_streams=cfg.streams)
            sock.settimeout(constants.SOCKET_TIMEOUT_SEC)
            sock.connect(net.sockaddr(cfg.server_host, port, cfg.family))
            sock.sendall(stream_worker.build_preamble(run_id, i))
            streams[i] = (sock, None)
        else:
            sock = socket.socket(cfg.family, socket.SOCK_DGRAM)
            sock.bind(net.sockaddr(cfg.bind, 0, cfg.family))
            net.configure_dont_fragment(sock)
            dest = net.sockaddr(cfg.server_host, port, cfg.family)
            sock.sendto(stream_worker.build_preamble(run_id, i), dest)
            streams[i] = (sock, dest)

        src_port = sock.getsockname()[1]
        if src_port in used_ports:
            raise ThroughputError(
                f"duplicate source port {src_port} assigned to stream {i}; "
                "each stream must use a unique source port")
        used_ports.add(src_port)
        source_ports[i] = src_port

    return streams, source_ports


def _should_use_multiprocess(cfg: ThroughputClientConfig) -> bool:
    """Multiprocessing requires >1 process, >1 stream, and a 'fork' start
    method (so worker processes inherit the shared-memory counters). Falls
    back to the threaded path otherwise (e.g. Windows)."""
    if cfg.procs <= 1 or cfg.streams <= 1:
        return False
    return "fork" in multiprocessing.get_all_start_methods()


# The UDP setup preamble is a single unacknowledged datagram. If it is dropped
# the server never registers the stream, times out, and the whole run fails with
# "did not receive stream_ready" -- increasingly likely as the stream count (and
# thus the number of preambles that must all survive) grows. Resend the preamble
# periodically until the start handshake completes; the server consumes the first
# copy and skips any duplicates (stream_worker.is_preamble).
_PREAMBLE_RESEND_INTERVAL_SEC = 0.1
_PREAMBLE_RESEND_MAX_SEC = 30.0


def _resend_preambles(streams: dict, run_id: str):
    for i, (sock, dest) in streams.items():
        try:
            sock.sendto(stream_worker.build_preamble(run_id, i), dest)
        except OSError:
            pass


def _make_preamble_resender(streams: dict, run_id: str):
    """Return a throttled callback that resends every stream's UDP preamble at
    most once per interval, giving up after a bounded window so a failed setup
    can't spin forever."""
    state = {"last": 0.0, "started": time.time()}

    def _tick():
        now = time.time()
        if now - state["started"] > _PREAMBLE_RESEND_MAX_SEC:
            return
        if now - state["last"] < _PREAMBLE_RESEND_INTERVAL_SEC:
            return
        state["last"] = now
        _resend_preambles(streams, run_id)

    return _tick


def _start_preamble_resender_thread(streams: dict, run_id: str) -> threading.Event:
    """Threaded-path equivalent of the resender: resend preambles in the
    background until stopped (or the bounded window elapses)."""
    stop = threading.Event()

    def _loop():
        deadline = time.time() + _PREAMBLE_RESEND_MAX_SEC
        while not stop.is_set() and time.time() < deadline:
            _resend_preambles(streams, run_id)
            stop.wait(_PREAMBLE_RESEND_INTERVAL_SEC)

    threading.Thread(target=_loop, daemon=True).start()
    return stop


def _client_worker_proc_inner(conn: dict, run_id: str, assignments: list, role: str,
                               shared, start_flag, stop_flag, connected_q, rate_value=None,
                               trial_value=None):
    """Worker process: owns a subset of streams end-to-end (its own sockets),
    so per-stream UDP loss tracking stays entirely within one process.

    start_flag / stop_flag are lock-free multiprocessing.Value('b') shared with
    the parent; the worker only reads them (see run_stream_group for why we
    avoid multiprocessing.Event here)."""
    socks = {}
    src_ports = {}
    try:
        for i, port in assignments:
            if conn["transport"] == "tcp":
                s = socket.socket(conn["family"], socket.SOCK_STREAM)
                s.bind(net.sockaddr(conn["bind"], 0, conn["family"]))
                stream_worker.configure_tcp_socket(
                    s, conn.get("path_mtu"), total_streams=conn.get("streams"))
                s.settimeout(constants.SOCKET_TIMEOUT_SEC)
                s.connect(net.sockaddr(conn["server_ip"], port, conn["family"]))
                s.sendall(stream_worker.build_preamble(run_id, i))
                socks[i] = (s, None)
            else:
                s = socket.socket(conn["family"], socket.SOCK_DGRAM)
                s.bind(net.sockaddr(conn["bind"], 0, conn["family"]))
                net.configure_dont_fragment(s)
                dest = net.sockaddr(conn["server_ip"], port, conn["family"])
                s.sendto(stream_worker.build_preamble(run_id, i), dest)
                socks[i] = (s, dest)
            src_ports[i] = s.getsockname()[1]
    except OSError as exc:
        connected_q.put(("error", str(exc)))
        return

    connected_q.put(("ok", src_ports))

    # Keep the preamble alive across setup-time UDP loss until the parent flips
    # start_flag (i.e. every stream has been acknowledged and the run begins).
    on_wait_tick = _make_preamble_resender(socks, run_id) if conn["transport"] == "udp" else None

    views = {i: shared.view(i) for i, _port in assignments}
    worker_options = {
        "wait_start": lambda: bool(start_flag.value),
        "stopped": lambda: bool(stop_flag.value),
        "rate_value": rate_value,
        "on_wait_tick": on_wait_tick,
        "path_mtu": conn.get("path_mtu"),
    }
    if conn["transport"] == "udp":
        worker_options["trial_value"] = trial_value
        worker_options["session_cookie"] = stream_worker.udp_session_cookie(run_id)
    stream_worker.run_stream_group(
        socks, role, conn["transport"], views, conn["duration_sec"],
        **worker_options)
    for s, _dest in socks.values():
        try:
            s.close()
        except OSError:
            pass


def _client_worker_proc(conn: dict, run_id: str, assignments: list, role: str,
                         shared, start_flag, stop_flag, connected_q, worker_status_q,
                         rate_value=None, trial_value=None):
    try:
        _client_worker_proc_inner(
            conn, run_id, assignments, role, shared, start_flag, stop_flag,
            connected_q, rate_value, trial_value)
    except BaseException as exc:
        process = multiprocessing.current_process()
        worker_status_q.put({
            "name": process.name,
            "pid": process.pid,
            "assignments": [index for index, _port in assignments],
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        })
        raise


def _drain_worker_errors(status_q) -> list[dict]:
    errors = []
    while True:
        try:
            errors.append(status_q.get_nowait())
        except queue.Empty:
            return errors


def _worker_process_failure(procs: list, status_q, require_stopped: bool = False,
                            expect_running: bool = False):
    errors = _drain_worker_errors(status_q)
    details = [
        f"{error.get('name')} pid={error.get('pid')} streams={error.get('assignments')}: "
        f"{error.get('error')}\n{error.get('traceback', '').rstrip()}"
        for error in errors
    ]
    reported_pids = {error.get("pid") for error in errors}
    for process in procs:
        if process.exitcode not in (None, 0) and process.pid not in reported_pids:
            details.append(f"{process.name} pid={process.pid} exited with code {process.exitcode}")
        elif expect_running and process.exitcode == 0:
            details.append(f"{process.name} pid={process.pid} exited before the test stopped")
        if require_stopped and process.is_alive():
            details.append(f"{process.name} pid={process.pid} did not stop")
    return "\n".join(details) or None


def _stop_client_workers(mp_state: dict, wait_for_peer: bool = False,
                         peer_drain_timeout_sec=None):
    """Stop all workers within one shared drain timeout."""
    if wait_for_peer:
        timeout = (constants.WORKER_DRAIN_TIMEOUT_SEC if peer_drain_timeout_sec is None
                   else peer_drain_timeout_sec)
        deadline = time.monotonic() + timeout
        for process in mp_state["procs"]:
            process.join(timeout=max(0.0, deadline - time.monotonic()))
        if all(not process.is_alive() for process in mp_state["procs"]):
            return _worker_process_failure(
                mp_state["procs"], mp_state["worker_status_q"], require_stopped=True)
    mp_state["stop_flag"].value = 1
    deadline = time.monotonic() + constants.WORKER_DRAIN_TIMEOUT_SEC
    for process in mp_state["procs"]:
        remaining = max(0.0, deadline - time.monotonic())
        if remaining == 0:
            break
        process.join(timeout=remaining)
    return _worker_process_failure(
        mp_state["procs"], mp_state["worker_status_q"], require_stopped=True)


def _start_client_workers(cfg: ThroughputClientConfig, server_ip: str, run_id: str,
                           ports: list, mp_ctx, shared):
    """Spawn worker processes and wait for every stream to connect. Returns a
    state dict; source ports are collected back from the workers."""
    n = len(ports)
    nproc = min(cfg.procs, n)
    groups = [[] for _ in range(nproc)]
    for idx, port in enumerate(ports):
        groups[idx % nproc].append((idx, port))
    active_groups = [g for g in groups if g]

    role = "send" if cfg.direction == "up" else "recv"
    conn = {"server_ip": server_ip, "bind": cfg.bind, "transport": cfg.transport,
            "duration_sec": cfg.sender_duration_sec, "path_mtu": cfg.path_mtu,
            "family": cfg.family, "streams": cfg.streams}
    start_flag = mp_ctx.Value("b", 0, lock=False)
    stop_flag = mp_ctx.Value("b", 0, lock=False)
    # Per-stream target send rate in bytes/sec (0 = unpaced); driven by the UDP
    # tuning ramp. Shared lock-free so the controller can update it live.
    rate_value = mp_ctx.Value("d", cfg.initial_rate_per_stream_bps, lock=False)
    trial_value = (mp_ctx.Value(ctypes.c_uint32, 0, lock=False)
                   if cfg.transport == "udp" else None)
    connected_q = mp_ctx.Queue()
    worker_status_q = mp_ctx.Queue()

    procs = []
    worker_assignments = {}
    for g in active_groups:
        p = mp_ctx.Process(target=_client_worker_proc,
                           args=(conn, run_id, g, role, shared, start_flag, stop_flag,
                                  connected_q, worker_status_q, rate_value,
                                  trial_value),
                           daemon=True)
        p.start()
        procs.append(p)
        worker_assignments[p.pid] = [index for index, _port in g]

    source_ports = {}
    deadline = time.monotonic() + 60
    for _ in range(len(active_groups)):
        while True:
            try:
                status, payload = connected_q.get(timeout=1)
                break
            except queue.Empty as exc:
                failure = _worker_process_failure(procs, worker_status_q)
                if failure or time.monotonic() >= deadline:
                    for p in procs:
                        p.terminate()
                    for p in procs:
                        p.join(timeout=2)
                    if failure:
                        raise ThroughputError(f"stream worker crashed during connection:\n{failure}") from exc
                    raise ThroughputError("stream worker connection timed out") from exc
        if status != "ok":
            for p in procs:
                p.terminate()
            for p in procs:
                p.join(timeout=2)
            raise ThroughputError(f"stream worker failed to connect: {payload}")
        source_ports.update(payload)

    return {"procs": procs, "start_flag": start_flag, "stop_flag": stop_flag,
            "rate_value": rate_value, "trial_value": trial_value,
            "source_ports": source_ports, "worker_status_q": worker_status_q,
            "worker_assignments": worker_assignments,
            "nproc": len(active_groups)}


def _client_topology(cfg, use_mp, mp_state=None, udp_backend=None):
    controller_pid = os.getpid()
    if use_mp and mp_state is not None:
        workers = [{"pid": controller_pid, "streams": []}]
        assignments = mp_state.get("worker_assignments", {})
        workers.extend({
            "pid": process.pid,
            "streams": list(assignments.get(process.pid, [])),
        } for process in mp_state.get("procs", []))
    else:
        workers = [{"pid": controller_pid, "streams": list(range(cfg.streams))}]
    topology = {
        "controller_pid": controller_pid,
        "multiprocessing": bool(use_mp),
        "tested_family": host_telemetry.normalize_ip_family(cfg.family),
        "ip_version": cfg.ip_version,
        "workers": workers,
    }
    if cfg.transport == "udp":
        topology["udp_batch"] = copy.deepcopy(udp_backend)
    return topology


def _udp_tuning_measurement_budget_sec() -> float:
    drain_sec = constants.PEER_STATS_INTERVAL_SEC * 2
    peer_wait_budget_sec = constants.PEER_STATS_INTERVAL_SEC * 4
    return (constants.UDP_TUNE_SETTLE_SEC + constants.UDP_TUNE_TRIAL_SEC
            + drain_sec + peer_wait_budget_sec)


def _minimum_udp_tuning_rate_mbps(cfg) -> float:
    """Aggregate rate needed for one datagram per stream per trial."""
    trial_sec = float(constants.UDP_TUNE_TRIAL_SEC)
    if trial_sec <= 0:
        return 0.0
    packet_bytes = stream_worker.udp_packet_size_for_path_mtu(
        getattr(cfg, "path_mtu", None), family=getattr(cfg, "family", None))
    return packet_bytes * max(1, int(cfg.streams)) * 8.0 / trial_sec / 1e6


def _validate_udp_tuning_sample_rate(cfg) -> float:
    """Reject starts that cannot emit one datagram per stream per trial."""
    if not cfg.tune_udp:
        return 0.0
    trial_sec = float(constants.UDP_TUNE_TRIAL_SEC)
    packet_bytes = stream_worker.udp_packet_size_for_path_mtu(
        cfg.path_mtu, family=cfg.family)
    minimum_mbps = _minimum_udp_tuning_rate_mbps(cfg)
    if cfg.udp_start_mbps < minimum_mbps:
        per_stream_bytes_sec = packet_bytes / trial_sec
        raise ThroughputError(
            f"UDP tuning start rate {cfg.udp_start_mbps:g} Mbps is too low "
            f"for {cfg.streams} streams with {packet_bytes}-byte datagrams "
            f"over the {trial_sec:g}-second trial window; minimum aggregate "
            f"rate is {minimum_mbps:.6f} Mbps "
            f"({per_stream_bytes_sec:.3f} bytes/sec per stream)")
    return minimum_mbps


def _run_udp_tuning(cfg, counters, recorder, rtt_tracker, peer_stats, apply_rate,
                    on_interval, stop_requested, tune_start, text_log,
                    check_workers=None, apply_trial=None, client_topology=None,
                    minimum_rate_mbps=None):
    """Multiplicative search for the highest UDP rate whose measured
    loss stays at or below cfg.acceptable_loss_pct, within the max-duration
    budget. Rates are per-stream bytes/sec; apply_rate() pushes the new rate to
    the senders (locally for upload, via a control message for download).

    Returns a ``tuning`` dict (probe + per-trial series + best result)."""
    target = cfg.acceptable_loss_pct
    streams = max(1, cfg.streams)
    state = {"target_aggregate_mbps": 0.0}
    pause_rate_ps = 1.0
    drain_sec = constants.PEER_STATS_INTERVAL_SEC * 2
    measurement_budget_sec = _udp_tuning_measurement_budget_sec()
    apply_trial = apply_trial or (lambda _trial_id: None)
    tested_family = getattr(cfg, "family", None)
    next_trial_id = 0

    if cfg.max_duration_sec < measurement_budget_sec:
        raise ThroughputError(
            f"UDP tuning max duration must be at least {measurement_budget_sec:.1f}s")
    deadline = time.time() + cfg.max_duration_sec

    def _sample_for(seconds):
        end = time.time() + seconds
        while time.time() < end and not stop_requested.is_set():
            time.sleep(constants.SAMPLE_INTERVAL_SEC)
            if check_workers is not None:
                check_workers()
            totals = _aggregate_totals(counters)
            loaded = rtt_tracker.loaded_stats()
            entry = recorder.record(time.time(), totals, rtt_ms=loaded.get("avg"), phase="tuning")
            entry["target_mbps"] = state["target_aggregate_mbps"]
            if on_interval:
                on_interval(entry)

    def _next_trial_id():
        nonlocal next_trial_id
        next_trial_id = (next_trial_id + 1) & _UINT32_MAX
        if next_trial_id == 0:
            next_trial_id = 1
        return next_trial_id

    def _peer_generation():
        try:
            generation = peer_stats.generation()
            if isinstance(generation, int):
                return generation
        except (AttributeError, TypeError):
            pass
        try:
            snapshot = peer_stats.snapshot()
            generation = snapshot.get("generation") if isinstance(snapshot, Mapping) else None
            return generation if isinstance(generation, int) else 0
        except (AttributeError, TypeError):
            return 0

    def _latest_peer_boundary():
        try:
            boundary = peer_stats.latest_boundary()
            return boundary if isinstance(boundary, Mapping) else None
        except (AttributeError, TypeError):
            return None

    def _wait_for_fresh_peer(after_generation):
        timeout = constants.PEER_STATS_INTERVAL_SEC * 4
        try:
            snapshot = peer_stats.wait_for_update(after_generation, timeout)
        except (AttributeError, TypeError):
            snapshot = None
        generation = (snapshot.get("generation")
                      if isinstance(snapshot, Mapping) else None)
        if not isinstance(generation, int) or generation <= after_generation:
            raise ThroughputError(
                "UDP tuning did not receive fresh peer trial statistics")
        boundary = _latest_peer_boundary()
        if (boundary is None
                or not isinstance(boundary.get("generation"), int)
                or boundary["generation"] < generation):
            # Exact trial records are retained independently. Older peers or
            # test doubles may have counters but no host boundary metadata.
            boundary = {
                "generation": generation,
                "timestamp": snapshot.get("timestamp"),
                "streams": [],
                "host_sample": None,
                "final": False,
            }
        return boundary

    def _local_streams():
        return [copy.deepcopy(counter.snapshot()) for counter in counters.values()]

    def _measure(rate_ps_bps):
        """Warm at trial 0, tag an exact trial, then pause and drain it."""
        if stop_requested.is_set():
            return None
        if rate_ps_bps < minimum_rate_ps:
            raise ThroughputError(
                "UDP tuning proposed a rate below its sampling floor: "
                f"proposed={rate_ps_bps * streams * 8 / 1e6:.6f} Mbps "
                f"minimum={minimum_rate_mbps:.6f} Mbps")
        apply_trial(0)
        apply_rate(rate_ps_bps)
        state["target_aggregate_mbps"] = (rate_ps_bps * streams * 8 / 1e6) if rate_ps_bps else 0.0
        _sample_for(constants.UDP_TUNE_SETTLE_SEC)
        if stop_requested.is_set():
            return None

        trial_id = _next_trial_id()
        peer_trial_start_generation = _peer_generation()
        server_start_boundary = _latest_peer_boundary()
        client_streams_start = _local_streams()
        client_host_start = _snapshot_client_endpoint(
            client_topology, tested_family=tested_family)
        local_trial_role = "sent" if cfg.direction == "up" else "received"
        trial_activated_ns = time.monotonic_ns()
        _set_local_trial_boundary(
            counters, trial_id, local_trial_role, trial_activated_ns, True)
        try:
            apply_trial(trial_id)
            _sample_for(constants.UDP_TUNE_TRIAL_SEC)
            # Every sender acknowledged activation before the first boundary,
            # and deactivation has not yet been requested at this boundary.
        finally:
            trial_deactivation_ns = time.monotonic_ns()
            try:
                _set_local_trial_boundary(
                    counters, trial_id, local_trial_role,
                    trial_deactivation_ns, False)
            finally:
                try:
                    apply_trial(0)
                finally:
                    apply_rate(pause_rate_ps)
        client_host_end = _snapshot_client_endpoint(
            client_topology, tested_family=tested_family)
        if stop_requested.is_set():
            return None

        _sample_for(drain_sec)
        if stop_requested.is_set():
            return None

        fresh_after_drain = _peer_generation()
        server_end_boundary = _wait_for_fresh_peer(fresh_after_drain)
        client_streams_end = _local_streams()
        active_dt = max(
            1e-6, (trial_deactivation_ns - trial_activated_ns) / 1e9)
        measured = _exact_udp_trial_metrics(
            cfg.direction, trial_id, client_streams_end, peer_stats, active_dt,
            peer_trial_start_generation, cfg.streams)
        if measured is None:
            role = "client sender" if cfg.direction == "up" else "server sender"
            raise ThroughputError(
                f"UDP tuning could not obtain exact trial data from the {role}")
        if cfg.direction == "down" and not measured.get("peer_trial_data"):
            raise ThroughputError(
                "UDP tuning did not receive fresh server sender trial data")

        try:
            (trial_host, sender_delta, receiver_delta,
             socket_deltas) = _trial_host_telemetry(
                cfg.direction, client_host_start, client_host_end,
                client_streams_start, client_streams_end,
                server_start_boundary, server_end_boundary,
                tested_family=tested_family)
        except Exception as exc:
            unavailable = _unavailable_endpoint(
                error=f"{type(exc).__name__}: {exc}",
                tested_family=tested_family)
            trial_host = {
                "schema_version": _HOST_TELEMETRY_SCHEMA_VERSION,
                "tested_family": host_telemetry.normalize_ip_family(tested_family),
                "roles": ({"sender": "client", "receiver": "server"}
                          if cfg.direction == "up"
                          else {"sender": "server", "receiver": "client"}),
                "client": _compact_endpoint_delta(unavailable),
                "server": _compact_endpoint_delta(unavailable),
                "socket_overflow_deltas": {
                    "client": None, "server": None,
                    "sender": None, "receiver": None,
                },
            }
            sender_delta = receiver_delta = unavailable
            socket_deltas = {"sender": None, "receiver": None}
        classification = _classify_trial(
            state["target_aggregate_mbps"], measured.get("offered_mbps"),
            measured.get("loss_pct"), target, sender_delta, receiver_delta,
            socket_deltas, tested_family=tested_family)
        measured["local_interval_sec"] = active_dt
        measured["peer_interval_sec"] = None
        measured["host_telemetry"] = trial_host
        measured["classification"] = classification
        measured["target_attained"] = classification.get("target_attained")
        measured["target_attainment_pct"] = classification.get(
            "target_attainment_pct")
        return {"target_mbps": state["target_aggregate_mbps"], **measured}

    def _ps_for_aggregate(mbps):
        """Per-stream bytes/sec needed to offer the given aggregate Mbps."""
        return (mbps * 1e6 / 8.0) / streams

    if minimum_rate_mbps is None:
        minimum_rate_mbps = _minimum_udp_tuning_rate_mbps(cfg)
    minimum_rate_mbps = max(0.0, float(minimum_rate_mbps))
    minimum_rate_ps = _ps_for_aggregate(minimum_rate_mbps)

    def _floored_rate(rate_ps):
        return max(minimum_rate_ps, rate_ps)

    def _classification_part(trial, part):
        classification = (trial or {}).get("classification") or {}
        nested = classification.get(part)
        return nested if isinstance(nested, Mapping) else classification

    def _result(reason, probe, trials, best, best_effort,
                limiting_factor=None, limiting_trial=None,
                limiting_classification=None):
        search_complete = reason in ("converged", "plateau")
        limiting_classification = limiting_classification or (
            (limiting_trial or {}).get("classification") or {})
        if reason == "target_not_attained":
            result_kind = "endpoint_limited"
        elif search_complete:
            result_kind = "confirmed_ceiling"
        elif best is not None:
            result_kind = "passing_lower_bound"
        elif reason == "stopped":
            result_kind = "stopped"
        else:
            result_kind = "no_passing_rate"
        return {
            "acceptable_loss_pct": target,
            "max_duration_sec": cfg.max_duration_sec,
            "start_mbps": cfg.udp_start_mbps,
            "minimum_rate_mbps": minimum_rate_mbps,
            "max_rate_mbps": constants.UDP_TUNE_MAX_RATE_MBPS,
            "initial_increase_pct": constants.UDP_TUNE_INITIAL_INCREASE_PCT,
            "max_increase_pct": constants.UDP_TUNE_MAX_INCREASE_PCT,
            "decrease_pct": (1.0 - constants.UDP_TUNE_BACKOFF_FACTOR) * 100.0,
            "convergence_pct": constants.UDP_TUNE_CONVERGENCE_PCT,
            "search_complete": search_complete,
            "ceiling_confirmed": search_complete,
            "result_kind": result_kind,
            "termination_reason": reason,
            "probe": probe,
            "trials": trials,
            "best": best,
            "best_effort": best_effort,
            "limiting_factor": limiting_factor,
            "limiting_scope": limiting_classification.get("scope"),
            "limiting_confidence": limiting_classification.get("confidence"),
            "limiting_trial": limiting_trial,
        }

    trials = []
    # Phase 1: a PACED probe at the configured start rate (default 250 Mbps
    # aggregate) instead of an unpaced line-rate burst. The old unpaced probe
    # could momentarily send multi-Gbps, which looks like a UDP flood/DDoS and
    # gets dropped or rate-limited by network security devices.
    start_ps = _ps_for_aggregate(cfg.udp_start_mbps)
    probe = _measure(start_ps)
    if probe is None or stop_requested.is_set():
        apply_trial(0)
        apply_rate(pause_rate_ps)
        return _result("stopped", None, trials, None, None)
    probe["phase"] = "probe"
    text_log.append(f"[tune] probe (paced {cfg.udp_start_mbps:.0f} Mbps): "
                    f"offered={probe['offered_mbps']:.0f} Mbps "
                    f"goodput={probe['goodput_mbps']:.0f} Mbps loss={probe['loss_pct']:.2f}% "
                    f"window={probe.get('peer_interval_sec') or probe.get('local_interval_sec', 0):.2f}s")

    def _goodput(m):
        return float(m.get("goodput_mbps") or 0.0)

    def _bracket_converged(good_rate_ps, bad_rate_ps):
        if good_rate_ps is None or bad_rate_ps is None or bad_rate_ps <= good_rate_ps:
            return False
        return ((bad_rate_ps / good_rate_ps) - 1.0) * 100.0 <= constants.UDP_TUNE_CONVERGENCE_PCT

    def _bracket_midpoint(good_rate_ps, bad_rate_ps):
        """Geometric midpoint gives equal percentage distance to each bound."""
        return (good_rate_ps * bad_rate_ps) ** 0.5

    # best        -> highest-goodput trial that also met the loss target.
    # best_effort -> highest-goodput trial overall (used for reporting when
    #                nothing met the target, so we never report 0 while data
    #                was actually flowing).
    best = None
    best_effort = None

    def _record(m, eligible=True):
        nonlocal best, best_effort
        if best_effort is None or _goodput(m) > _goodput(best_effort):
            best_effort = dict(m)
        if (eligible and m["loss_pct"] <= target
                and m.get("target_attained") is not False
                and (best is None or _goodput(m) > _goodput(best))):
            best = dict(m)

    if probe.get("target_attained") is False:
        _record(probe, eligible=False)
        limiting_classification = _classification_part(
            probe, "sender_attainment")
        limiting_factor = limiting_classification.get("primary", "unknown")
        text_log.append(
            f"[tune] target not attained: {probe.get('target_attainment_pct', 0):.1f}% "
            f"({limiting_factor})")
        apply_trial(0)
        apply_rate(pause_rate_ps)
        return _result(
            "target_not_attained", probe, trials, best, best_effort,
            limiting_factor=limiting_factor, limiting_trial=dict(probe),
            limiting_classification=limiting_classification)

    _record(probe)
    # Best goodput accepted so far (loss under target); drives plateau detection.
    accepted_goodput = _goodput(probe) if probe["loss_pct"] <= target else None
    highest_good_rate_ps = start_ps if probe["loss_pct"] <= target else None
    lowest_bad_rate_ps = start_ps if probe["loss_pct"] > target else None
    max_rate_ps = _ps_for_aggregate(constants.UDP_TUNE_MAX_RATE_MBPS)
    increase_pct = constants.UDP_TUNE_INITIAL_INCREASE_PCT
    termination_reason = None
    limiting_factor = None
    limiting_trial = None
    limiting_classification = None
    plateau_observations = 0

    if probe["loss_pct"] > target:
        limiting_trial = dict(probe)
        limiting_classification = _classification_part(
            probe, "loss_causality")
        limiting_factor = limiting_classification.get("primary", "unknown")

    if probe["loss_pct"] <= target:
        # Grow quickly until a trial breaches the target and establishes an upper bound.
        if start_ps >= max_rate_ps:
            termination_reason = "rate_cap_reached"
            rate_ps = 0.0
        else:
            rate_ps = _floored_rate(
                min(max_rate_ps, start_ps * (1.0 + increase_pct / 100.0)))
    else:
        # Start rate already breaches the loss target -- back off below it.
        if minimum_rate_ps > 0 and start_ps <= minimum_rate_ps:
            termination_reason = "minimum_rate_reached"
            rate_ps = 0.0
        else:
            rate_ps = _floored_rate(
                start_ps * constants.UDP_TUNE_BACKOFF_FACTOR)

    # Phase 2: increase by a percentage until the loss threshold is bracketed,
    # then refine at the geometric midpoint (equal percentage distance from both
    # bounds). A goodput plateau also identifies saturation on deeply buffered paths.
    while (not stop_requested.is_set()
           and time.time() + measurement_budget_sec <= deadline
           and rate_ps > 0):
        trial = _measure(rate_ps)
        if trial is None or stop_requested.is_set():
            termination_reason = "stopped"
            break
        trials.append(trial)
        gp = _goodput(trial)
        if trial.get("target_attained") is False:
            _record(trial, eligible=False)
            termination_reason = "target_not_attained"
            limiting_classification = _classification_part(
                trial, "sender_attainment")
            limiting_factor = limiting_classification.get(
                "primary", "unknown")
            limiting_trial = dict(trial)
            text_log.append(
                f"[tune] target={trial['target_mbps']:.0f} Mbps -> "
                f"offered={trial['offered_mbps']:.0f} Mbps "
                f"attainment={trial.get('target_attainment_pct', 0):.1f}% "
                f"({limiting_factor})")
            break
        low_gain = (lowest_bad_rate_ps is None
                    and trial["loss_pct"] <= target
                    and accepted_goodput is not None
                    and gp <= accepted_goodput * (1.0 + constants.UDP_TUNE_MIN_GOODPUT_GAIN))
        plateau_observations = plateau_observations + 1 if low_gain else 0
        plateaued = plateau_observations >= 2
        text_log.append(f"[tune] target={trial['target_mbps']:.0f} Mbps -> "
                        f"goodput={gp:.0f} Mbps loss={trial['loss_pct']:.2f}% "
                        f"window={trial.get('peer_interval_sec') or trial.get('local_interval_sec', 0):.2f}s"
                        + ("  (saturation candidate)" if low_gain else ""))
        _record(trial)
        if plateaued:
            termination_reason = "plateau"
            break
        if trial["loss_pct"] > target:
            if highest_good_rate_ps is not None and rate_ps <= highest_good_rate_ps:
                termination_reason = "inconsistent_measurements"
                limiting_factor = "measurement_variance"
                limiting_classification = {
                    "scope": "measurement", "confidence": "low",
                }
                limiting_trial = dict(trial)
                break
            lowest_bad_rate_ps = (rate_ps if lowest_bad_rate_ps is None
                                  else min(lowest_bad_rate_ps, rate_ps))
            limiting_trial = dict(trial)
            limiting_classification = _classification_part(
                trial, "loss_causality")
            limiting_factor = limiting_classification.get(
                "primary", "unknown")
            if _bracket_converged(highest_good_rate_ps, lowest_bad_rate_ps):
                termination_reason = "converged"
                break
            if highest_good_rate_ps is None:
                if minimum_rate_ps > 0 and rate_ps <= minimum_rate_ps:
                    termination_reason = "minimum_rate_reached"
                    break
                rate_ps = _floored_rate(
                    rate_ps * constants.UDP_TUNE_BACKOFF_FACTOR)
            else:
                rate_ps = _floored_rate(
                    _bracket_midpoint(highest_good_rate_ps, lowest_bad_rate_ps))
        else:
            if lowest_bad_rate_ps is not None and rate_ps >= lowest_bad_rate_ps:
                termination_reason = "inconsistent_measurements"
                limiting_factor = "measurement_variance"
                limiting_classification = {
                    "scope": "measurement", "confidence": "low",
                }
                limiting_trial = dict(trial)
                break
            accepted_goodput = max(accepted_goodput or 0.0, gp)
            highest_good_rate_ps = (rate_ps if highest_good_rate_ps is None
                                    else max(highest_good_rate_ps, rate_ps))
            if _bracket_converged(highest_good_rate_ps, lowest_bad_rate_ps):
                termination_reason = "converged"
                break
            if lowest_bad_rate_ps is None:
                if rate_ps >= max_rate_ps:
                    termination_reason = "rate_cap_reached"
                    break
                increase_pct = min(
                    constants.UDP_TUNE_MAX_INCREASE_PCT,
                    increase_pct * constants.UDP_TUNE_INCREASE_GROWTH_FACTOR,
                )
                rate_ps = _floored_rate(
                    min(max_rate_ps,
                        rate_ps * (1.0 + increase_pct / 100.0)))
            else:
                rate_ps = _floored_rate(
                    _bracket_midpoint(highest_good_rate_ps, lowest_bad_rate_ps))

    if termination_reason is None:
        termination_reason = "stopped" if stop_requested.is_set() else "budget_exhausted"

    # Keep the sender effectively paused while the controller stops the run.
    apply_trial(0)
    apply_rate(pause_rate_ps)

    if best is not None:
        text_log.append(f"[tune] best: target={best.get('target_mbps', 0):.0f} Mbps "
                        f"goodput={best.get('goodput_mbps', 0):.0f} Mbps loss={best.get('loss_pct', 0):.2f}% "
                        f"(<= {target:.2f}% target)")
    else:
        text_log.append(f"[tune] no rate met the {target:.2f}% loss target within the budget"
                        + (f"; best effort goodput={best_effort.get('goodput_mbps', 0):.0f} Mbps "
                           f"loss={best_effort.get('loss_pct', 0):.2f}%" if best_effort else ""))
    text_log.append(f"[tune] search status: {termination_reason}")

    return _result(
        termination_reason, probe, trials, best, best_effort,
        limiting_factor=limiting_factor, limiting_trial=limiting_trial,
        limiting_classification=limiting_classification)


def _validate_udp_hello_ack(message):
    selected = message.get("udp_wire_version") if isinstance(message, Mapping) else None
    if (_coerce_trial_id(selected) != stream_worker.UDP_WIRE_VERSION
            or isinstance(selected, bool)):
        raise ThroughputError(
            "UDP wire version mismatch: client requires "
            f"{stream_worker.UDP_WIRE_VERSION}; server selected {selected!r}")
    return selected


def _run_throughput_test_once(cfg: ThroughputClientConfig, on_interval=None,
                              stop_requested: threading.Event = None,
                              log_base_dir: str = None) -> dict:
    try:
        server_ip = addr.resolve_host(cfg.server_host, family=cfg.family)
    except ValueError as exc:
        raise ThroughputError(f"invalid {cfg.ip_version} server address: {exc}") from exc
    cfg.server_host = server_ip
    stop_requested = stop_requested or threading.Event()

    if cfg.path_mtu is None:
        cfg.path_mtu, cfg.pmtud_info = _discover_path_mtu(cfg, server_ip, log_base_dir)
    elif cfg.pmtud_info is None:
        cfg.pmtud_info = {
            "server_ip": server_ip,
            "udp_port": (cfg.start_port if cfg.transport == "udp" else constants.DEFAULT_UDP_PORT),
            "ip_version": cfg.ip_version,
            "path_mtu": cfg.path_mtu,
            "path_mtu_source": "provided",
            "fallback_used": False,
            "jumbo_mtu": cfg.jumbo_mtu,
            "probe_max_mtu": None,
            "probe_ceiling_reached": False,
        }

    minimum_tuning_rate_mbps = _validate_udp_tuning_sample_rate(cfg)

    control = None
    control_sock = None
    try:
        control_sock = socket.socket(cfg.family, socket.SOCK_STREAM)
        control_sock.settimeout(constants.SOCKET_TIMEOUT_SEC)
        control_sock.connect(net.sockaddr(server_ip, cfg.control_port, cfg.family))
        control = ControlChannel(control_sock)
    except OSError as exc:
        if control_sock is not None:
            control_sock.close()
        raise _control_connection_error(cfg, server_ip, exc) from exc
    except Exception:
        if control_sock is not None:
            control_sock.close()
        raise

    run_id = str(uuid.uuid4())
    client_udp_backend = udp_batch.backend_status() if cfg.transport == "udp" else None
    server_udp_backend = None
    params = {
        "transport": cfg.transport,
        "direction": cfg.direction,
        "streams": cfg.streams,
        # Senders (client for upload, server for download) must stay alive for
        # the whole budget when tuning; the controller stops them early.
        "duration_sec": cfg.sender_duration_sec,
        "procs": cfg.procs,
        "path_mtu": cfg.path_mtu,
        "ip_version": cfg.ip_version,
    }
    if cfg.transport == "udp":
        params["udp_wire_version"] = stream_worker.UDP_WIRE_VERSION

    _dur_desc = (f"max_duration={cfg.max_duration_sec}s acceptable_loss={cfg.acceptable_loss_pct}%"
                 if cfg.tune_udp else f"duration={cfg.duration_sec}s")
    text_log = [f"NPERF throughput test  run_id={run_id}",
                f"server={server_ip}:{cfg.control_port}  ip_version={cfg.ip_version} transport={cfg.transport} "
                f"direction={cfg.direction} streams={cfg.streams} {_dur_desc}"]
    if cfg.path_mtu is not None:
        text_log.append(f"Path MTU: {cfg.path_mtu} bytes  TCP MSS: "
                        f"{stream_worker.tcp_mss_for_path_mtu(cfg.path_mtu, family=cfg.family)} bytes  "
                        f"UDP datagram: {stream_worker.udp_packet_size_for_path_mtu(cfg.path_mtu, family=cfg.family)} bytes")
    if cfg.pmtud_info:
        text_log.append(f"PMTUD source: {cfg.pmtud_info.get('path_mtu_source', 'pmtud')}  "
                        f"log: {cfg.pmtud_info.get('log_json', 'n/a')}")
        text_log.append(f"PMTUD ceiling: jumbo_mtu={cfg.jumbo_mtu}  "
                        f"max_mtu={cfg.pmtud_info.get('probe_max_mtu')}  "
                        f"reached={cfg.pmtud_info.get('probe_ceiling_reached')}")

    try:
        control.send("hello", run_id=run_id, params=params)
        msg = control.recv(timeout=30)
        if not msg or msg.get("type") != "hello_ack":
            raise ThroughputError(f"server rejected hello: {msg}")
        if cfg.transport == "udp":
            _validate_udp_hello_ack(msg)
            server_udp_backend = copy.deepcopy(msg.get("udp_batch")) or {}
        if not msg.get("ok"):
            raise ThroughputError(f"server rejected hello: {msg}")
        ports = msg["ports"]
        if ports and ports[0] != cfg.start_port:
            text_log.append(f"WARNING: server is using base port {ports[0]}, "
                             f"client expected start_port {cfg.start_port}")

        use_mp = _should_use_multiprocess(cfg)
        streams = None
        mp_state = None
        if use_mp:
            mp_ctx = multiprocessing.get_context("fork")
            shared = shared_counters.SharedCounters(cfg.streams, ctx=mp_ctx)
            counters = {i: shared.view(i) for i in range(cfg.streams)}
            mp_state = _start_client_workers(cfg, server_ip, run_id, ports, mp_ctx, shared)
            source_ports = mp_state["source_ports"]
            text_log.append(f"Execution: multiprocessing ({mp_state['nproc']} worker "
                            f"process(es) for {cfg.streams} streams)")
        else:
            streams, source_ports = _connect_streams(cfg, run_id, ports)
            counters = {i: metrics.StreamCounters(i) for i in range(cfg.streams)}
            text_log.append("Execution: single process (threaded)")
        client_topology = _client_topology(
            cfg, use_mp, mp_state, udp_backend=client_udp_backend)
        if cfg.transport == "udp":
            text_log.append(
                "UDP wire v"
                f"{stream_worker.UDP_WIRE_VERSION}; batch backend "
                f"client={client_udp_backend.get('selected')} "
                f"(requested={client_udp_backend.get('requested')}) "
                f"server={(server_udp_backend or {}).get('selected')} "
                f"(requested={(server_udp_backend or {}).get('requested')})")
        text_log.append("Per-stream 5-tuples (unique source port each): " + ", ".join(
            f"stream {i}: src :{source_ports[i]} -> {server_ip}:{ports[i]}/{cfg.transport}"
            for i in sorted(source_ports)))

        # In the threaded path there is no worker wait-loop to piggyback the
        # preamble retransmit on, so drive it from a background thread until every
        # stream is acknowledged.
        preamble_resend_stop = None
        if cfg.transport == "udp" and not use_mp:
            preamble_resend_stop = _start_preamble_resender_thread(streams, run_id)
        try:
            for _ in range(cfg.streams):
                ready_msg = control.recv(timeout=30)
                if not ready_msg or ready_msg.get("type") != "stream_ready":
                    raise ThroughputError(f"did not receive stream_ready: {ready_msg}")
        finally:
            if preamble_resend_stop is not None:
                preamble_resend_stop.set()

        control.send("start")
        ack = control.recv(timeout=30)
        if not ack or ack.get("type") != "start_ack":
            raise ThroughputError(f"did not receive start_ack: {ack}")

        phase = run_mode.RunPhase()
        rtt_tracker = metrics.RttTracker()
        peer_stats = _PeerStatsTracker()
        done_event = threading.Event()
        final_stats_event = threading.Event()
        server_errors = []
        trial_acks = _TrialAckTracker()

        dispatch_thread = threading.Thread(target=_client_dispatch_loop,
                                             args=(control, phase, rtt_tracker, counters, done_event, peer_stats,
                                                   final_stats_event, server_errors),
                                            kwargs={"trial_acks": trial_acks,
                                                    "server_udp_backend":
                                                        server_udp_backend},
                                            daemon=True)
        dispatch_thread.start()

        prober_stop = threading.Event()
        prober_thread = threading.Thread(target=latency_probe.prober_loop,
                                          args=(control, prober_stop, cfg.probe_interval_sec), daemon=True)
        prober_thread.start()

        # calibration phase: latency probing only, no data flow
        calib_deadline = time.time() + cfg.calibration_sec
        while time.time() < calib_deadline and not stop_requested.is_set():
            time.sleep(0.05)

        stream_stop_events = {}
        stream_threads = []
        stream_start_gate = threading.Event()
        stream_entered = {}
        start_time = None

        # Per-stream target send rate (bytes/sec, 0 = unpaced). For upload this
        # drives the local senders; the tuning controller updates it live.
        if use_mp:
            client_rate_value = mp_state["rate_value"]
            client_trial_value = mp_state.get("trial_value")
        else:
            client_rate_value = stream_worker.RateHolder(cfg.initial_rate_per_stream_bps)
            client_trial_value = (stream_worker.TrialHolder(0)
                                  if cfg.transport == "udp" else None)

        def set_server_trial(trial_id):
            trial_id = int(trial_id) & _UINT32_MAX
            generation = trial_acks.generation()
            control.send("set_trial", trial_id=trial_id)
            if not trial_acks.wait_for(
                    trial_id, generation, constants.SOCKET_TIMEOUT_SEC):
                raise ThroughputError(
                    f"server did not acknowledge UDP trial {trial_id}")

        # The existing set_rate message works with older servers. TCP preserves
        # its order ahead of start_data, so download senders are paced at birth.
        if cfg.tune_udp and cfg.direction == "down":
            control.send("set_rate", per_stream_bps=cfg.initial_rate_per_stream_bps)
            set_server_trial(0)

        client_host_baseline = _snapshot_client_endpoint(
            client_topology, tested_family=cfg.family)

        def run_local_stream(i, sock, dest, stop_ev):
            stream_start_gate.wait()
            if stop_ev.is_set():
                return
            stream_entered[i].set()
            if cfg.direction == "up":
                if cfg.transport == "tcp":
                    stream_worker.tcp_sender_loop(
                        sock, counters[i], stop_ev, cfg.sender_duration_sec,
                        start_time, cfg.path_mtu)
                else:
                    stream_worker.udp_sender_loop(
                        sock, dest, i, counters[i], stop_ev,
                        cfg.sender_duration_sec, start_time,
                        rate_value=client_rate_value, path_mtu=cfg.path_mtu,
                        trial_value=client_trial_value,
                        session_cookie=stream_worker.udp_session_cookie(run_id))
            elif cfg.transport == "tcp":
                stream_worker.tcp_receiver_loop(sock, counters[i], stop_ev, cfg.path_mtu)
            else:
                stream_worker.udp_receiver_loop(
                    sock, counters[i], stop_ev, known_peer_addr=dest,
                    path_mtu=cfg.path_mtu,
                    session_cookie=stream_worker.udp_session_cookie(run_id))

        if not use_mp:
            for i, (sock, dest) in streams.items():
                stop_ev = threading.Event()
                stream_stop_events[i] = stop_ev
                stream_entered[i] = threading.Event()
                th = threading.Thread(
                    target=run_local_stream, args=(i, sock, dest, stop_ev), daemon=True)
                th.start()
                stream_threads.append(th)

        start_time = time.time()
        measurement_started_at = time.monotonic()
        phase.start_running()
        control.send("start_data")
        if use_mp:
            mp_state["start_flag"].value = 1
        else:
            stream_start_gate.set()
            deadline = time.monotonic() + constants.SOCKET_TIMEOUT_SEC
            for i, entered in stream_entered.items():
                if not entered.wait(timeout=max(0.0, deadline - time.monotonic())):
                    raise ThroughputError(f"data stream {i} did not start in time")

        recorder = metrics.IntervalRecorder(transport=cfg.transport)
        tuning = None

        worker_deadline = start_time + cfg.sender_duration_sec

        def check_client_workers():
            if not use_mp:
                return
            expect_running = time.time() < worker_deadline - 0.5
            failure = _worker_process_failure(
                mp_state["procs"], mp_state["worker_status_q"],
                expect_running=expect_running)
            if failure:
                raise ThroughputError(f"client worker process failed:\n{failure}")

        if cfg.tune_udp:
            # apply_rate pushes a new per-stream rate (bytes/sec) to whichever
            # side is sending: locally for upload, or to the server (which
            # mirrors it onto its senders) for download.
            if cfg.direction == "up":
                def apply_rate(ps_bps):
                    client_rate_value.value = ps_bps

                def apply_trial(trial_id):
                    trial_id = int(trial_id) & _UINT32_MAX
                    client_trial_value.value = trial_id
                    _wait_for_observed_trial(
                        counters, trial_id, constants.SOCKET_TIMEOUT_SEC,
                        check_workers=check_client_workers)
                    set_server_trial(trial_id)
            else:
                def apply_rate(ps_bps):
                    try:
                        control.send("set_rate", per_stream_bps=ps_bps)
                    except OSError:
                        pass

                def apply_trial(trial_id):
                    set_server_trial(trial_id)
            tuning = _run_udp_tuning(
                cfg, counters, recorder, rtt_tracker, peer_stats, apply_rate,
                on_interval, stop_requested, start_time, text_log,
                check_workers=check_client_workers, apply_trial=apply_trial,
                client_topology=client_topology,
                minimum_rate_mbps=minimum_tuning_rate_mbps)
        else:
            run_deadline = start_time + cfg.duration_sec
            while time.time() < run_deadline and not stop_requested.is_set():
                time.sleep(constants.SAMPLE_INTERVAL_SEC)
                check_client_workers()
                totals = _aggregate_totals(counters)
                loaded = rtt_tracker.loaded_stats()
                entry = recorder.record(time.time(), totals, rtt_ms=loaded.get("avg"), phase="running")
                if on_interval:
                    on_interval(entry)

        tuning_stopped = ((tuning or {}).get("termination_reason") == "stopped")
        run_cancelled = stop_requested.is_set() or tuning_stopped
        run_status = "cancelled" if run_cancelled else "complete"
        client_host_end = _snapshot_client_endpoint(
            client_topology, tested_family=cfg.family)
        client_worker_failure = None
        local_workers_stopped = False
        measurement_ended_at = None
        if cfg.direction == "up":
            if use_mp:
                client_worker_failure = _stop_client_workers(mp_state)
            else:
                for ev in stream_stop_events.values():
                    ev.set()
                join_deadline = time.monotonic() + constants.WORKER_DRAIN_TIMEOUT_SEC
                for thread in stream_threads:
                    thread.join(timeout=max(0.0, join_deadline - time.monotonic()))
                stuck_threads = [thread.name for thread in stream_threads
                                 if thread.is_alive()]
                if stuck_threads:
                    client_worker_failure = (
                        f"client data threads did not stop: {stuck_threads}")
                if cfg.transport == "tcp":
                    for i, (sock, _dest) in streams.items():
                        retrans = tcp_info.get_tcp_retransmits(sock)
                        if retrans is not None:
                            counters[i].set_retransmits(retrans)
                        try:
                            sock.shutdown(socket.SHUT_WR)
                        except OSError:
                            pass
            measurement_ended_at = time.monotonic()
            local_workers_stopped = True
            if cfg.transport == "udp":
                time.sleep(constants.PEER_STATS_INTERVAL_SEC * 2)

        prober_stop.set()
        if measurement_ended_at is None:
            measurement_ended_at = time.monotonic()
        control.send("stop")
        measurement_elapsed_sec = max(
            0.0, measurement_ended_at - measurement_started_at)

        final_stats_received = _wait_for_final_stats(
            done_event, final_stats_event, constants.FINAL_STATS_TIMEOUT_SEC)
        if final_stats_received:
            done_event.wait(timeout=2)
        prober_stop.set()
        phase.stop()
        dispatch_thread.join(timeout=2)

        if not final_stats_received:
            if server_errors:
                raise ThroughputError(f"server failed the test: {server_errors[0]}")
            if done_event.is_set():
                raise ThroughputError("server closed the test without final peer statistics")
            raise ThroughputError(
                f"timed out after {constants.FINAL_STATS_TIMEOUT_SEC}s waiting for final peer statistics")

        if not local_workers_stopped:
            drain_timeout = (constants.TCP_RECEIVER_DRAIN_TIMEOUT_SEC
                             if cfg.transport == "tcp"
                             else constants.WORKER_DRAIN_TIMEOUT_SEC)
            if use_mp:
                client_worker_failure = _stop_client_workers(
                    mp_state, wait_for_peer=True, peer_drain_timeout_sec=drain_timeout)
            else:
                join_deadline = time.monotonic() + drain_timeout
                for thread in stream_threads:
                    thread.join(timeout=max(0.0, join_deadline - time.monotonic()))
                if any(thread.is_alive() for thread in stream_threads):
                    for ev in stream_stop_events.values():
                        ev.set()
                    join_deadline = time.monotonic() + constants.WORKER_DRAIN_TIMEOUT_SEC
                    for thread in stream_threads:
                        thread.join(timeout=max(0.0, join_deadline - time.monotonic()))
                stuck_threads = [thread.name for thread in stream_threads
                                 if thread.is_alive()]
                if stuck_threads:
                    client_worker_failure = (
                        f"client data threads did not stop: {stuck_threads}")
        if client_worker_failure:
            raise ThroughputError(f"client worker process failed:\n{client_worker_failure}")

        if not use_mp:
            for sock, _dest in streams.values():
                try:
                    sock.close()
                except OSError:
                    pass

        if cfg.transport == "udp":
            final_client_streams = [
                counter.snapshot() for counter in counters.values()]
            client_udp_backend = udp_batch.aggregate_backend_status(
                final_client_streams,
                base_status=udp_batch.backend_status(),
                include_base=not use_mp,
            )
            client_topology["udp_batch"] = copy.deepcopy(client_udp_backend)

        try:
            host_summary = _build_whole_run_host_telemetry(
                cfg.direction, client_host_baseline, client_host_end, peer_stats,
                client_topology, tested_family=cfg.family)
        except Exception as exc:
            host_summary = {
                "schema_version": _HOST_TELEMETRY_SCHEMA_VERSION,
                "tested_family": host_telemetry.normalize_ip_family(cfg.family),
                "available": False,
                "reason": "unavailable",
                "error": f"{type(exc).__name__}: {exc}",
                "topology": {"client": copy.deepcopy(client_topology),
                             "server": None},
                "client": None,
                "server": None,
            }
        try:
            socket_telemetry = _build_socket_telemetry(
                [counter.snapshot() for counter in counters.values()],
                peer_stats.latest_streams(), transport=cfg.transport)
        except Exception as exc:
            socket_telemetry = {
                "requested_buffer_bytes": (stream_worker.UDP_SOCKET_BUFFER_BYTES
                                           if cfg.transport == "udp" else None),
                "available": False,
                "error": f"{type(exc).__name__}: {exc}",
                "client": {"streams": []},
                "server": {"streams": []},
            }
        summary = _build_summary(cfg, run_id, counters, rtt_tracker, recorder, text_log,
                                 source_ports, tuning=tuning,
                                 host_telemetry_summary=host_summary,
                                 socket_telemetry=socket_telemetry,
                                 client_udp_backend=client_udp_backend,
                                 server_udp_backend=server_udp_backend,
                                 elapsed_sec=measurement_elapsed_sec,
                                 status=run_status)
        json_path, log_path = logging_store.save_result(
            constants.TEST_TYPE_THROUGHPUT, summary, text_log, base_dir=log_base_dir)
        summary["log_json"] = json_path
        summary["log_text"] = log_path
        if summary.get("aborted"):
            return summary
        try:
            _validate_stream_results(cfg, counters)
            _validate_summary_throughput(summary)
            _validate_udp_tuning_summary(cfg, summary)
            if cfg.transport == "udp":
                totals = summary.get("totals") or {}
                if totals.get("packets_sent", 0) > 0 and totals.get("packets_received", 0) == 0:
                    raise ThroughputError(
                        f"UDP data-plane failure: sent {totals['packets_sent']} packets but received none")
        except ThroughputError as exc:
            raise ThroughputError(f"{exc}; diagnostic log: {json_path}") from exc
        return summary

    finally:
        if 'preamble_resend_stop' in locals() and preamble_resend_stop is not None:
            preamble_resend_stop.set()
        if 'prober_stop' in locals():
            prober_stop.set()
        if 'phase' in locals():
            phase.stop()
        if 'stream_stop_events' in locals():
            for event in stream_stop_events.values():
                event.set()
        if 'stream_start_gate' in locals():
            stream_start_gate.set()
        # Exception paths (including tuning retries) must drain data threads
        # before closing sockets, just like the successful path does.
        if 'stream_threads' in locals():
            deadline = time.monotonic() + constants.WORKER_DRAIN_TIMEOUT_SEC
            for thread in stream_threads:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
        if 'mp_state' in locals() and mp_state is not None:
            mp_state["stop_flag"].value = 1
            for process in mp_state["procs"]:
                process.join(timeout=2)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=2)
        if 'streams' in locals() and streams:
            for sock, _dest in streams.values():
                try:
                    sock.close()
                except OSError:
                    pass
        if control is not None:
            control.close()


def run_throughput_test(cfg: ThroughputClientConfig, on_interval=None,
                         stop_requested: threading.Event = None,
                         log_base_dir: str = None) -> dict:
    stop_requested = stop_requested or threading.Event()
    if cfg.tune_udp:
        minimum_duration = _udp_tuning_measurement_budget_sec()
        if cfg.max_duration_sec < minimum_duration:
            raise ThroughputError(
                f"UDP tuning max duration must be at least {minimum_duration:.1f}s")
    attempts = constants.UDP_RETRY_ATTEMPTS if cfg.transport == "udp" else 1
    last_error = None
    for attempt in range(attempts):
        try:
            return _run_throughput_test_once(
                cfg,
                on_interval=on_interval,
                stop_requested=stop_requested,
                log_base_dir=log_base_dir,
            )
        except (ThroughputError, OSError, EOFError, TimeoutError) as exc:
            last_error = exc
            if attempt + 1 >= attempts or stop_requested.is_set():
                if isinstance(exc, ThroughputError):
                    raise
                raise _operational_throughput_error(exc) from exc
            time.sleep(constants.UDP_RETRY_BACKOFF_SEC * (2 ** attempt))
    if isinstance(last_error, ThroughputError):
        raise last_error
    raise _operational_throughput_error(last_error) from last_error


def _validate_udp_tuning_summary(cfg: ThroughputClientConfig, summary: dict) -> None:
    if cfg.tune_udp and summary.get("loss_pct", 100.0) > cfg.acceptable_loss_pct:
        tuning = summary.get("tuning") or {}
        minimum_detail = ""
        if tuning.get("termination_reason") == "minimum_rate_reached":
            minimum_detail = (
                "; loss persisted at the minimum sampled rate "
                f"{tuning.get('minimum_rate_mbps', 0):.6f} Mbps")
        raise ThroughputError(
            f"no UDP rate met the {cfg.acceptable_loss_pct:.2f}% loss target; "
            f"best effort was {summary.get('recv_mbps', 0):.2f} Mbps at "
            f"{summary.get('loss_pct', 100.0):.3f}% loss{minimum_detail}")


def _validate_summary_throughput(summary: dict) -> None:
    send_mbps = summary.get("send_mbps", 0)
    recv_mbps = summary.get("recv_mbps", 0)
    if send_mbps <= 0 or recv_mbps <= 0:
        raise ThroughputError(
            f"invalid throughput result: send={send_mbps:.2f} Mbps recv={recv_mbps:.2f} Mbps")


def _validate_stream_results(cfg: ThroughputClientConfig, counters: dict) -> None:
    missing = [index for index in range(cfg.streams) if index not in counters]
    inactive = []
    for index, counter in counters.items():
        snapshot = counter.snapshot()
        if snapshot.get("bytes_sent", 0) <= 0 or snapshot.get("bytes_received", 0) <= 0:
            inactive.append(
                f"stream {index}: sent={snapshot.get('bytes_sent', 0)} "
                f"received={snapshot.get('bytes_received', 0)} "
                f"socket_error={snapshot.get('error')}")
    if missing or inactive:
        details = []
        if missing:
            details.append(f"missing streams: {missing}")
        details.extend(inactive)
        raise ThroughputError("stream validation failed: " + "; ".join(details))


def _aggregate_totals(counters: dict) -> dict:
    totals = {"bytes_sent": 0, "packets_sent": 0, "bytes_received": 0,
              "packets_received": 0, "packets_lost": 0, "retransmits": 0}
    for c in counters.values():
        snap = c.snapshot()
        for key in totals:
            totals[key] += snap.get(key, 0)
    return totals


def _build_summary(cfg, run_id, counters, rtt_tracker, recorder, text_log, source_ports=None,
                   tuning=None, host_telemetry_summary=None,
                   socket_telemetry=None, client_udp_backend=None,
                   server_udp_backend=None, elapsed_sec=None, status=None):
    source_ports = source_ports or {}
    per_stream = []
    for c in counters.values():
        snap = c.snapshot()
        idx = snap.get("index")
        snap["source_port"] = source_ports.get(idx)
        per_stream.append(snap)
    totals = _aggregate_totals(counters)

    if elapsed_sec is None:
        actual_duration = cfg.duration_sec
    else:
        try:
            actual_duration = max(0.0, float(elapsed_sec))
        except (TypeError, ValueError, OverflowError):
            actual_duration = 0.0
        if not math.isfinite(actual_duration):
            actual_duration = 0.0
    if status is None:
        status = ("cancelled" if (tuning or {}).get("termination_reason") == "stopped"
                  else "complete")
    aborted = status in ("aborted", "cancelled")
    # Completed fixed runs retain their configured measurement window. An
    # interrupted run must use only the time during which bytes could flow.
    duration = actual_duration if aborted else cfg.duration_sec
    send_bps = (totals["bytes_sent"] / duration) if duration else 0
    recv_bps = (totals["bytes_received"] / duration) if duration else 0
    send_mbps = send_bps * 8 / 1e6
    recv_mbps = recv_bps * 8 / 1e6
    send_pps = metrics.estimate_pps(cfg.transport, send_bps,
                                    (totals["packets_sent"] / duration) if duration else 0)
    recv_pps = metrics.estimate_pps(cfg.transport, recv_bps,
                                    (totals["packets_received"] / duration) if duration else 0)

    loss_pct = None
    if cfg.transport == "udp":
        expected = totals["packets_sent"]
        lost_packets = max(totals["packets_lost"], expected - totals["packets_received"])
        loss_pct = (lost_packets / expected * 100.0) if expected else 0.0

    # For a tuning run the cumulative totals span every probed rate, so they are
    # not a meaningful "throughput". Report the best sustainable trial. When no
    # trial met the loss target, fall back to the best-effort trial (highest
    # goodput measured) so we still report the real throughput/loss instead of
    # zeros while data was clearly flowing.
    if tuning is not None:
        best = tuning.get("best") or tuning.get("best_effort")
        if best is not None:
            send_mbps = best.get("offered_mbps", 0.0)
            recv_mbps = best.get("goodput_mbps", 0.0)
            send_pps = best.get("offered_pps", 0.0)
            recv_pps = best.get("goodput_pps", 0.0)
            loss_pct = best.get("loss_pct", loss_pct)
            receiver_bps_override = recv_mbps * 1e6 / 8
        else:
            send_mbps = recv_mbps = send_pps = recv_pps = 0.0
            receiver_bps_override = 0.0
    else:
        receiver_bps_override = None

    unloaded_rtt_ms = rtt_tracker.unloaded_rtt_ms()
    loaded_stats = rtt_tracker.loaded_stats()

    receiver_bps = (receiver_bps_override if receiver_bps_override is not None
                    else ((totals["bytes_received"] / duration) if duration else 0))
    bloat = metrics.compute_bufferbloat(receiver_bps, unloaded_rtt_ms, loaded_stats.get("avg"))

    tcp_mss_bytes = stream_worker.tcp_mss_for_path_mtu(cfg.path_mtu, family=cfg.family)
    udp_datagram_bytes = stream_worker.udp_packet_size_for_path_mtu(cfg.path_mtu, family=cfg.family)

    summary = {
        "run_id": run_id,
        "server": cfg.server_host,
        "ip_version": cfg.ip_version,
        "transport": cfg.transport,
        "direction": cfg.direction,
        "streams": cfg.streams,
        "procs": cfg.procs,
        "duration_sec": duration,
        "configured_duration_sec": cfg.duration_sec,
        "elapsed_sec": actual_duration,
        "actual_duration_sec": actual_duration,
        "status": status,
        "complete": status == "complete",
        "aborted": aborted,
        "cancelled": status == "cancelled",
        "path_mtu": cfg.path_mtu,
        "jumbo_mtu": cfg.jumbo_mtu,
        "probe_max_mtu": (cfg.pmtud_info or {}).get("probe_max_mtu"),
        "probe_ceiling_reached": (cfg.pmtud_info or {}).get("probe_ceiling_reached"),
        "tcp_mss_bytes": tcp_mss_bytes,
        "udp_datagram_bytes": udp_datagram_bytes,
        "pmtud": cfg.pmtud_info,
        "source_ports": source_ports,
        "totals": totals,
        "send_mbps": send_mbps,
        "recv_mbps": recv_mbps,
        "send_pps": send_pps,
        "recv_pps": recv_pps,
        "loss_pct": loss_pct,
        "retransmits": totals["retransmits"] if cfg.transport == "tcp" else None,
        "unloaded_rtt_ms": unloaded_rtt_ms,
        "loaded_rtt": loaded_stats,
        "bufferbloat": bloat,
        "per_stream": per_stream,
        "interval_series": recorder.records,
        "tuning": tuning,
        "host_telemetry": host_telemetry_summary,
        "socket_telemetry": socket_telemetry,
    }
    if cfg.transport == "udp":
        summary.update({
            "udp_wire_version": stream_worker.UDP_WIRE_VERSION,
            "udp_batch": {
                "client": copy.deepcopy(client_udp_backend),
                "server": copy.deepcopy(server_udp_backend),
            },
        })

    text_log.append("")
    text_log.append(
        f"Status: {status}  elapsed={actual_duration:.3f}s  "
        f"configured={cfg.duration_sec:.3f}s")
    text_log.append(f"IP version: {cfg.ip_version}")
    text_log.append(f"Path MTU: {cfg.path_mtu} bytes")
    text_log.append(f"PMTUD source: {(cfg.pmtud_info or {}).get('path_mtu_source', 'unknown')}")
    text_log.append(f"PMTUD ceiling: jumbo_mtu={cfg.jumbo_mtu}  "
                    f"max_mtu={(cfg.pmtud_info or {}).get('probe_max_mtu')}  "
                    f"reached={(cfg.pmtud_info or {}).get('probe_ceiling_reached')}")
    text_log.append(f"Packet sizing: TCP MSS={tcp_mss_bytes} bytes  UDP datagram={udp_datagram_bytes} bytes")
    text_log.append(f"Aggregate throughput: send={send_mbps:.2f} Mbps  recv={recv_mbps:.2f} Mbps")
    text_log.append(f"Packets/sec: send={send_pps:.1f} pps  recv={recv_pps:.1f} pps")
    if loss_pct is not None:
        text_log.append(f"UDP packet loss: {loss_pct:.3f}%")
        text_log.append(
            "UDP batch backend: "
            f"client={(client_udp_backend or {}).get('selected')} "
            f"server={(server_udp_backend or {}).get('selected')}")
    if summary["retransmits"] is not None:
        text_log.append(f"TCP retransmits: {summary['retransmits']}")
    text_log.append(f"Unloaded RTT: {unloaded_rtt_ms} ms")
    text_log.append(f"Loaded RTT: avg={loaded_stats.get('avg')} best={loaded_stats.get('best')} "
                     f"worst={loaded_stats.get('worst')} stddev={loaded_stats.get('stddev')} "
                     f"jitter={loaded_stats.get('jitter')}")
    text_log.append(f"Bufferbloat: bdp_bytes={bloat.get('bdp_bytes')} "
                     f"buffered_bytes={bloat.get('buffered_bytes')} bloat_factor={bloat.get('bloat_factor')}")

    return summary
