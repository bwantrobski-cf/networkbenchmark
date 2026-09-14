"""Synchronized multiprocessing-shared per-stream counters."""

import functools
import multiprocessing as mp
import time


# All values use signed 64-bit slots. Sequence values are encoded as two's
# complement so the full UDP uint64 sequence space remains representable.
_F_BYTES_SENT = 0
_F_PACKETS_SENT = 1
_F_BYTES_RECV = 2
_F_PACKETS_RECV = 3
_F_MAX_SEQ = 4
_F_MAX_SEQ_VALID = 5
_F_PACKETS_LOST = 6
_F_DUPLICATES = 7
_F_OUT_OF_ORDER = 8
_F_KERNEL_RXQ_DROPS = 9
_F_SOCKET_SNDBUF = 10
_F_SOCKET_RCVBUF = 11
_F_RXQ_OVERFLOW_SUPPORTED = 12
_F_RETRANS = 13
_F_ERROR = 14

_F_TRIAL_SENT_ACTIVE = 15
_F_TRIAL_SENT_ID = 16
_F_TRIAL_SENT_BYTES = 17
_F_TRIAL_SENT_PACKETS = 18
_F_TRIAL_SENT_FIRST = 19
_F_TRIAL_SENT_FIRST_VALID = 20
_F_TRIAL_SENT_LAST = 21
_F_TRIAL_SENT_LAST_VALID = 22

_F_TRIAL_RECV_ACTIVE = 23
_F_TRIAL_RECV_ID = 24
_F_TRIAL_RECV_BYTES = 25
_F_TRIAL_RECV_PACKETS = 26
_F_TRIAL_RECV_FIRST = 27
_F_TRIAL_RECV_FIRST_VALID = 28
_F_TRIAL_RECV_LAST = 29
_F_TRIAL_RECV_LAST_VALID = 30
_F_TRIAL_RECV_LOST = 31
_F_TRIAL_RECV_DUPLICATES = 32
_F_TRIAL_RECV_OUT_OF_ORDER = 33

_F_TRIAL_SENT_FIRST_NS = 34
_F_TRIAL_SENT_LAST_NS = 35
_F_SENT_VERSION = 36
_F_RECV_VERSION = 37
_F_OBSERVED_TRIAL_ID = 38
_F_OBSERVED_TRIAL_VALID = 39
_F_UDP_BACKEND = 40
_F_UDP_RUNTIME_FALLBACK = 41
_F_TRIAL_RECV_SEQUENCE_GAPS = 42
_F_TRIAL_SENT_ACTIVATED_NS = 43
_F_TRIAL_SENT_DEACTIVATED_NS = 44
_F_TRIAL_RECV_ACTIVATED_NS = 45
_F_TRIAL_RECV_DEACTIVATED_NS = 46
_USED_FIELDS = 47
_CACHE_LINE_QWORDS = 8
_NUM_FIELDS = (
    (_USED_FIELDS + _CACHE_LINE_QWORDS - 1) // _CACHE_LINE_QWORDS
) * _CACHE_LINE_QWORDS

_UINT64_MODULUS = 1 << 64
_INT64_LIMIT = 1 << 63
_INT64_MAX = _INT64_LIMIT - 1


def _encode_u64(value):
    value = int(value)
    if value < 0 or value >= _UINT64_MODULUS:
        raise OverflowError("sequence does not fit in uint64")
    return value if value < _INT64_LIMIT else value - _UINT64_MODULUS


def _decode_u64(value):
    value = int(value)
    return value if value >= 0 else value + _UINT64_MODULUS


def _newer_trial_id(candidate, current):
    if int(candidate) == 0 and int(current) != 0:
        return False
    delta = (int(candidate) - int(current)) & 0xFFFFFFFF
    return 0 < delta < 0x80000000


def _trial_role(role):
    if role in ("sent", "sender", "trial_sent"):
        return "sent"
    if role in ("received", "receiver", "trial_received"):
        return "received"
    raise ValueError(f"unknown trial role: {role!r}")


def _synchronized(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self._lock.acquire(timeout=5.0):
            raise RuntimeError(
                f"timed out acquiring shared counter lock for stream {self.index}")
        try:
            return method(self, *args, **kwargs)
        finally:
            self._lock.release()
    return wrapper


class SharedCounters:
    """Own the padded shared array and hand out per-stream views."""

    def __init__(self, num_streams: int, ctx=None):
        self.num_streams = num_streams
        array_factory = (ctx or mp).Array
        # A stride is an integral number of 64-byte cache lines, preventing
        # workers that own adjacent streams from false-sharing stream records.
        self._arr = array_factory("q", num_streams * _NUM_FIELDS, lock=False)
        lock_factory = (ctx or mp).Lock
        self._locks = [lock_factory() for _ in range(num_streams)]

    def view(self, index: int) -> "SharedCounterView":
        return SharedCounterView(self._arr, index, self._locks[index])

    def snapshot(self, index: int) -> dict:
        return self.view(index).snapshot()


class SharedCounterView:
    """Per-stream accessor matching ``metrics.StreamCounters``."""

    def __init__(self, arr, index: int, lock):
        self._arr = arr
        self._lock = lock
        self.index = index
        self._base = index * _NUM_FIELDS

    def _begin(self, field):
        self._arr[self._base + field] += 1

    def _end(self, field):
        self._arr[self._base + field] += 1

    def _activate_trial_locked(self, trial_id, role, activated_at_ns):
        base = self._base
        trial_id = int(trial_id) & 0xFFFFFFFF
        if trial_id == 0:
            return
        role = _trial_role(role)
        if role == "sent":
            active_field = _F_TRIAL_SENT_ACTIVE
            id_field = _F_TRIAL_SENT_ID
            activated_field = _F_TRIAL_SENT_ACTIVATED_NS
            current = self._arr[base + id_field]
            if (not self._arr[base + active_field]
                    or _newer_trial_id(trial_id, current)):
                self._arr[base + active_field] = 1
                self._arr[base + id_field] = trial_id
                self._arr[base + _F_TRIAL_SENT_BYTES] = 0
                self._arr[base + _F_TRIAL_SENT_PACKETS] = 0
                self._arr[base + _F_TRIAL_SENT_FIRST_VALID] = 0
                self._arr[base + _F_TRIAL_SENT_LAST_VALID] = 0
                self._arr[base + _F_TRIAL_SENT_FIRST_NS] = 0
                self._arr[base + _F_TRIAL_SENT_LAST_NS] = 0
                self._arr[base + _F_TRIAL_SENT_DEACTIVATED_NS] = 0
                current = trial_id
        else:
            active_field = _F_TRIAL_RECV_ACTIVE
            id_field = _F_TRIAL_RECV_ID
            activated_field = _F_TRIAL_RECV_ACTIVATED_NS
            current = self._arr[base + id_field]
            if (not self._arr[base + active_field]
                    or _newer_trial_id(trial_id, current)):
                self._arr[base + active_field] = 1
                self._arr[base + id_field] = trial_id
                self._arr[base + _F_TRIAL_RECV_BYTES] = 0
                self._arr[base + _F_TRIAL_RECV_PACKETS] = 0
                self._arr[base + _F_TRIAL_RECV_FIRST_VALID] = 0
                self._arr[base + _F_TRIAL_RECV_LAST_VALID] = 0
                self._arr[base + _F_TRIAL_RECV_LOST] = 0
                self._arr[base + _F_TRIAL_RECV_DUPLICATES] = 0
                self._arr[base + _F_TRIAL_RECV_OUT_OF_ORDER] = 0
                self._arr[base + _F_TRIAL_RECV_SEQUENCE_GAPS] = 0
                self._arr[base + _F_TRIAL_RECV_DEACTIVATED_NS] = 0
                current = trial_id
        if trial_id == current:
            activated_at_ns = int(activated_at_ns)
            existing = self._arr[base + activated_field]
            if not existing or activated_at_ns < existing:
                self._arr[base + activated_field] = activated_at_ns

    @_synchronized
    def activate_trial(self, trial_id: int, role: str,
                       activated_at_ns=None):
        """Publish an exact zero-valued record at a trial boundary."""
        if activated_at_ns is None:
            activated_at_ns = time.monotonic_ns()
        self._activate_trial_locked(trial_id, role, activated_at_ns)

    @_synchronized
    def deactivate_trial(self, trial_id: int, role: str,
                         deactivated_at_ns=None):
        if deactivated_at_ns is None:
            deactivated_at_ns = time.monotonic_ns()
        base = self._base
        trial_id = int(trial_id) & 0xFFFFFFFF
        role = _trial_role(role)
        if role == "sent":
            active_field = _F_TRIAL_SENT_ACTIVE
            id_field = _F_TRIAL_SENT_ID
            boundary_field = _F_TRIAL_SENT_DEACTIVATED_NS
        else:
            active_field = _F_TRIAL_RECV_ACTIVE
            id_field = _F_TRIAL_RECV_ID
            boundary_field = _F_TRIAL_RECV_DEACTIVATED_NS
        if (trial_id != 0 and self._arr[base + active_field]
                and self._arr[base + id_field] == trial_id):
            deactivated_at_ns = int(deactivated_at_ns)
            existing = self._arr[base + boundary_field]
            if not existing or deactivated_at_ns < existing:
                self._arr[base + boundary_field] = deactivated_at_ns

    def _reconcile_peer_loss(self):
        base = self._base
        if not self._arr[base + _F_TRIAL_SENT_ACTIVE]:
            return
        self._arr[base + _F_PACKETS_LOST] = min(
            _INT64_MAX,
            max(0, self._arr[base + _F_PACKETS_SENT]
                - self._arr[base + _F_PACKETS_RECV]),
        )
        sent_packets = max(0, self._arr[base + _F_TRIAL_SENT_PACKETS])
        if (not self._arr[base + _F_TRIAL_RECV_ACTIVE]
                or self._arr[base + _F_TRIAL_RECV_ID]
                != self._arr[base + _F_TRIAL_SENT_ID]):
            return
        unique_packets = max(0, self._arr[base + _F_TRIAL_RECV_PACKETS])
        exact_loss = min(_INT64_MAX, max(0, sent_packets - unique_packets))
        self._arr[base + _F_TRIAL_RECV_LOST] = exact_loss

    @_synchronized
    def add_sent(self, nbytes: int, npackets: int = 1):
        base = self._base
        self._begin(_F_SENT_VERSION)
        self._arr[base + _F_BYTES_SENT] += int(nbytes)
        self._arr[base + _F_PACKETS_SENT] += int(npackets)
        self._end(_F_SENT_VERSION)

    @_synchronized
    def add_sent_batch(self, nbytes: int, npackets: int, trial_id: int = 0,
                       first_seq=None, last_seq=None, first_sent_ns=None,
                       last_sent_ns=None):
        base = self._base
        trial_id = int(trial_id) & 0xFFFFFFFF
        self._begin(_F_SENT_VERSION)
        self._arr[base + _F_BYTES_SENT] += int(nbytes)
        self._arr[base + _F_PACKETS_SENT] += int(npackets)

        active = bool(self._arr[base + _F_TRIAL_SENT_ACTIVE])
        current = self._arr[base + _F_TRIAL_SENT_ID]
        if not active or _newer_trial_id(trial_id, current):
            self._arr[base + _F_TRIAL_SENT_ACTIVE] = 1
            self._arr[base + _F_TRIAL_SENT_ID] = trial_id
            self._arr[base + _F_TRIAL_SENT_BYTES] = 0
            self._arr[base + _F_TRIAL_SENT_PACKETS] = 0
            self._arr[base + _F_TRIAL_SENT_FIRST_VALID] = 0
            self._arr[base + _F_TRIAL_SENT_LAST_VALID] = 0
            self._arr[base + _F_TRIAL_SENT_FIRST_NS] = 0
            self._arr[base + _F_TRIAL_SENT_LAST_NS] = 0
            self._arr[base + _F_TRIAL_SENT_ACTIVATED_NS] = 0
            self._arr[base + _F_TRIAL_SENT_DEACTIVATED_NS] = 0
            current = trial_id
        if trial_id == current:
            self._arr[base + _F_TRIAL_SENT_BYTES] += int(nbytes)
            self._arr[base + _F_TRIAL_SENT_PACKETS] += int(npackets)
            if first_seq is not None:
                encoded = _encode_u64(first_seq)
                if (not self._arr[base + _F_TRIAL_SENT_FIRST_VALID]
                        or int(first_seq) < _decode_u64(
                            self._arr[base + _F_TRIAL_SENT_FIRST])):
                    self._arr[base + _F_TRIAL_SENT_FIRST] = encoded
                    self._arr[base + _F_TRIAL_SENT_FIRST_VALID] = 1
            if last_seq is not None:
                encoded = _encode_u64(last_seq)
                if (not self._arr[base + _F_TRIAL_SENT_LAST_VALID]
                        or int(last_seq) > _decode_u64(
                            self._arr[base + _F_TRIAL_SENT_LAST])):
                    self._arr[base + _F_TRIAL_SENT_LAST] = encoded
                    self._arr[base + _F_TRIAL_SENT_LAST_VALID] = 1
            if first_sent_ns is not None:
                first_sent_ns = int(first_sent_ns)
                if (not self._arr[base + _F_TRIAL_SENT_FIRST_NS]
                        or first_sent_ns < self._arr[base + _F_TRIAL_SENT_FIRST_NS]):
                    self._arr[base + _F_TRIAL_SENT_FIRST_NS] = first_sent_ns
            if last_sent_ns is not None:
                last_sent_ns = int(last_sent_ns)
                if last_sent_ns > self._arr[base + _F_TRIAL_SENT_LAST_NS]:
                    self._arr[base + _F_TRIAL_SENT_LAST_NS] = last_sent_ns
        self._end(_F_SENT_VERSION)

    @_synchronized
    def add_received(self, nbytes: int, seq=None):
        base = self._base
        self._begin(_F_RECV_VERSION)
        self._arr[base + _F_BYTES_RECV] += int(nbytes)
        self._arr[base + _F_PACKETS_RECV] += 1
        if seq is not None:
            seq = int(seq)
            if (not self._arr[base + _F_MAX_SEQ_VALID]
                    or seq > _decode_u64(self._arr[base + _F_MAX_SEQ])):
                self._arr[base + _F_MAX_SEQ] = _encode_u64(seq)
                self._arr[base + _F_MAX_SEQ_VALID] = 1
            max_seq = _decode_u64(self._arr[base + _F_MAX_SEQ])
            self._arr[base + _F_PACKETS_LOST] = max(
                0, max_seq + 1 - self._arr[base + _F_PACKETS_RECV])
        self._end(_F_RECV_VERSION)

    @_synchronized
    def add_received_batch(
            self, nbytes: int, unique_count: int, trial_id: int = 0,
            first_seq=None, last_seq=None, duplicates: int = 0,
            reordered: int = 0, missing_count: int = 0,
            kernel_rxq_drops=None, *, out_of_order=None, trial_nbytes=None,
            trial_unique_count=None, trial_duplicates=None,
            trial_reordered=None, trial_missing_count=None):
        if out_of_order is not None:
            reordered = out_of_order
        nbytes = int(nbytes)
        unique_count = int(unique_count)
        duplicates = int(duplicates)
        reordered = int(reordered)
        missing_count = min(_INT64_MAX, max(0, int(missing_count)))
        trial_id = int(trial_id) & 0xFFFFFFFF
        trial_nbytes = nbytes if trial_nbytes is None else int(trial_nbytes)
        trial_unique_count = (
            unique_count if trial_unique_count is None else int(trial_unique_count))
        trial_duplicates = (
            duplicates if trial_duplicates is None else int(trial_duplicates))
        trial_reordered = (
            reordered if trial_reordered is None else int(trial_reordered))
        trial_missing_count = (
            missing_count if trial_missing_count is None
            else min(_INT64_MAX, max(0, int(trial_missing_count))))

        base = self._base
        self._begin(_F_RECV_VERSION)
        self._arr[base + _F_BYTES_RECV] += nbytes
        self._arr[base + _F_PACKETS_RECV] += unique_count
        self._arr[base + _F_DUPLICATES] += duplicates
        self._arr[base + _F_OUT_OF_ORDER] += reordered
        self._arr[base + _F_PACKETS_LOST] = missing_count
        for seq in (first_seq, last_seq):
            if seq is not None:
                seq = int(seq)
                if (not self._arr[base + _F_MAX_SEQ_VALID]
                        or seq > _decode_u64(self._arr[base + _F_MAX_SEQ])):
                    self._arr[base + _F_MAX_SEQ] = _encode_u64(seq)
                    self._arr[base + _F_MAX_SEQ_VALID] = 1
        if kernel_rxq_drops is not None:
            self._arr[base + _F_KERNEL_RXQ_DROPS] = max(
                self._arr[base + _F_KERNEL_RXQ_DROPS],
                int(kernel_rxq_drops))

        active = bool(self._arr[base + _F_TRIAL_RECV_ACTIVE])
        current = self._arr[base + _F_TRIAL_RECV_ID]
        if not active or _newer_trial_id(trial_id, current):
            self._arr[base + _F_TRIAL_RECV_ACTIVE] = 1
            self._arr[base + _F_TRIAL_RECV_ID] = trial_id
            self._arr[base + _F_TRIAL_RECV_BYTES] = 0
            self._arr[base + _F_TRIAL_RECV_PACKETS] = 0
            self._arr[base + _F_TRIAL_RECV_FIRST_VALID] = 0
            self._arr[base + _F_TRIAL_RECV_LAST_VALID] = 0
            self._arr[base + _F_TRIAL_RECV_LOST] = 0
            self._arr[base + _F_TRIAL_RECV_DUPLICATES] = 0
            self._arr[base + _F_TRIAL_RECV_OUT_OF_ORDER] = 0
            self._arr[base + _F_TRIAL_RECV_SEQUENCE_GAPS] = 0
            self._arr[base + _F_TRIAL_RECV_ACTIVATED_NS] = 0
            self._arr[base + _F_TRIAL_RECV_DEACTIVATED_NS] = 0
            current = trial_id
        if trial_id == current:
            self._arr[base + _F_TRIAL_RECV_BYTES] += trial_nbytes
            self._arr[base + _F_TRIAL_RECV_PACKETS] += trial_unique_count
            self._arr[base + _F_TRIAL_RECV_DUPLICATES] += trial_duplicates
            self._arr[base + _F_TRIAL_RECV_OUT_OF_ORDER] += trial_reordered
            self._arr[base + _F_TRIAL_RECV_LOST] = trial_missing_count
            self._arr[base + _F_TRIAL_RECV_SEQUENCE_GAPS] = trial_missing_count
            if first_seq is not None:
                encoded = _encode_u64(first_seq)
                if (not self._arr[base + _F_TRIAL_RECV_FIRST_VALID]
                        or int(first_seq) < _decode_u64(
                            self._arr[base + _F_TRIAL_RECV_FIRST])):
                    self._arr[base + _F_TRIAL_RECV_FIRST] = encoded
                    self._arr[base + _F_TRIAL_RECV_FIRST_VALID] = 1
            if last_seq is not None:
                encoded = _encode_u64(last_seq)
                if (not self._arr[base + _F_TRIAL_RECV_LAST_VALID]
                        or int(last_seq) > _decode_u64(
                            self._arr[base + _F_TRIAL_RECV_LAST])):
                    self._arr[base + _F_TRIAL_RECV_LAST] = encoded
                    self._arr[base + _F_TRIAL_RECV_LAST_VALID] = 1
        self._reconcile_peer_loss()
        self._end(_F_RECV_VERSION)

    @_synchronized
    def set_socket_metadata(self, socket_sndbuf_bytes=None,
                            socket_rcvbuf_bytes=None,
                            rxq_overflow_supported=None):
        base = self._base
        self._begin(_F_RECV_VERSION)
        if socket_sndbuf_bytes is not None:
            self._arr[base + _F_SOCKET_SNDBUF] = int(socket_sndbuf_bytes)
        if socket_rcvbuf_bytes is not None:
            self._arr[base + _F_SOCKET_RCVBUF] = int(socket_rcvbuf_bytes)
        if rxq_overflow_supported is not None:
            self._arr[base + _F_RXQ_OVERFLOW_SUPPORTED] = int(
                bool(rxq_overflow_supported))
        self._end(_F_RECV_VERSION)

    @_synchronized
    def set_kernel_rxq_drops(self, cumulative: int):
        base = self._base
        self._begin(_F_RECV_VERSION)
        self._arr[base + _F_KERNEL_RXQ_DROPS] = max(
            self._arr[base + _F_KERNEL_RXQ_DROPS], int(cumulative))
        self._end(_F_RECV_VERSION)

    @_synchronized
    def set_retransmits(self, value: int):
        self._arr[self._base + _F_RETRANS] = int(value)

    @_synchronized
    def set_error(self, message: str):
        self._arr[self._base + _F_ERROR] = 1

    @_synchronized
    def set_observed_trial(self, trial_id: int):
        trial_id = int(trial_id) & 0xFFFFFFFF
        self._arr[self._base + _F_OBSERVED_TRIAL_ID] = trial_id
        self._arr[self._base + _F_OBSERVED_TRIAL_VALID] = 1
        if trial_id:
            self._activate_trial_locked(
                trial_id, "sent", time.monotonic_ns())

    @_synchronized
    def set_udp_backend(self, status: dict):
        selected = (status or {}).get("selected")
        self._arr[self._base + _F_UDP_BACKEND] = (
            1 if selected == "native" else 2 if selected == "python" else 0)
        self._arr[self._base + _F_UDP_RUNTIME_FALLBACK] = int(bool(
            (status or {}).get("native_runtime_error")))

    def _stable_values(self):
        base = self._base
        return [self._arr[base + field] for field in range(_USED_FIELDS)]

    @_synchronized
    def snapshot(self) -> dict:
        values = self._stable_values()
        max_seq = (
            _decode_u64(values[_F_MAX_SEQ])
            if values[_F_MAX_SEQ_VALID] else -1)
        sent_first = (
            _decode_u64(values[_F_TRIAL_SENT_FIRST])
            if values[_F_TRIAL_SENT_FIRST_VALID] else None)
        sent_last = (
            _decode_u64(values[_F_TRIAL_SENT_LAST])
            if values[_F_TRIAL_SENT_LAST_VALID] else None)
        recv_first = (
            _decode_u64(values[_F_TRIAL_RECV_FIRST])
            if values[_F_TRIAL_RECV_FIRST_VALID] else None)
        recv_last = (
            _decode_u64(values[_F_TRIAL_RECV_LAST])
            if values[_F_TRIAL_RECV_LAST_VALID] else None)
        trial_sent = {
            "trial_id": values[_F_TRIAL_SENT_ID],
            "activated_at_ns": (
                values[_F_TRIAL_SENT_ACTIVATED_NS] or None),
            "deactivated_at_ns": (
                values[_F_TRIAL_SENT_DEACTIVATED_NS] or None),
            "bytes_sent": values[_F_TRIAL_SENT_BYTES],
            "packets_sent": values[_F_TRIAL_SENT_PACKETS],
            "first_seq": sent_first,
            "last_seq": sent_last,
            "first_sent_ns": (values[_F_TRIAL_SENT_FIRST_NS] or None),
            "last_sent_ns": (values[_F_TRIAL_SENT_LAST_NS] or None),
        }
        trial_received = {
            "trial_id": values[_F_TRIAL_RECV_ID],
            "activated_at_ns": (
                values[_F_TRIAL_RECV_ACTIVATED_NS] or None),
            "deactivated_at_ns": (
                values[_F_TRIAL_RECV_DEACTIVATED_NS] or None),
            "bytes_received": values[_F_TRIAL_RECV_BYTES],
            "packets_received": values[_F_TRIAL_RECV_PACKETS],
            "unique_packets": values[_F_TRIAL_RECV_PACKETS],
            "first_seq": recv_first,
            "last_seq": recv_last,
            "packets_lost": values[_F_TRIAL_RECV_LOST],
            "sequence_gaps": values[_F_TRIAL_RECV_SEQUENCE_GAPS],
            "duplicates": values[_F_TRIAL_RECV_DUPLICATES],
            "out_of_order": values[_F_TRIAL_RECV_OUT_OF_ORDER],
        }
        return {
            "index": self.index,
            "bytes_sent": values[_F_BYTES_SENT],
            "packets_sent": values[_F_PACKETS_SENT],
            "bytes_received": values[_F_BYTES_RECV],
            "packets_received": values[_F_PACKETS_RECV],
            "max_seq_seen": max_seq,
            "packets_lost": values[_F_PACKETS_LOST],
            "duplicates": values[_F_DUPLICATES],
            "out_of_order": values[_F_OUT_OF_ORDER],
            "kernel_rxq_drops": (
                values[_F_KERNEL_RXQ_DROPS]
                if values[_F_RXQ_OVERFLOW_SUPPORTED] else None),
            "socket_sndbuf_bytes": values[_F_SOCKET_SNDBUF],
            "socket_rcvbuf_bytes": values[_F_SOCKET_RCVBUF],
            "rxq_overflow_supported": bool(
                values[_F_RXQ_OVERFLOW_SUPPORTED]),
            "trial_sent": trial_sent,
            "trial_received": trial_received,
            "retransmits": values[_F_RETRANS],
            "error": True if values[_F_ERROR] else None,
            "observed_trial_id": (
                values[_F_OBSERVED_TRIAL_ID]
                if values[_F_OBSERVED_TRIAL_VALID] else None),
            "udp_batch": {
                "selected": {
                    1: "native", 2: "python",
                }.get(values[_F_UDP_BACKEND]),
                "native_runtime_error": (
                    True if values[_F_UDP_RUNTIME_FALLBACK] else None),
            },
        }

    @_synchronized
    def merge_peer_snapshot(self, peer: dict):
        """Merge role-specific peer counters into this shared record."""
        base = self._base
        peer_sent = int(peer.get("bytes_sent", 0) or 0)
        peer_sent_packets = int(peer.get("packets_sent", 0) or 0)
        peer_trial_sent = peer.get("trial_sent")
        if peer_sent or peer_sent_packets or isinstance(peer_trial_sent, dict):
            self._begin(_F_SENT_VERSION)
            self._arr[base + _F_BYTES_SENT] = max(
                self._arr[base + _F_BYTES_SENT], peer_sent)
            self._arr[base + _F_PACKETS_SENT] = max(
                self._arr[base + _F_PACKETS_SENT], peer_sent_packets)
            if isinstance(peer_trial_sent, dict):
                record = peer_trial_sent
                peer_id = int(record.get("trial_id", 0)) & 0xFFFFFFFF
                current_id = self._arr[base + _F_TRIAL_SENT_ID]
                should_copy = (
                    not self._arr[base + _F_TRIAL_SENT_ACTIVE]
                    or _newer_trial_id(peer_id, current_id)
                    or (peer_id == current_id
                        and int(record.get("packets_sent", 0) or 0)
                        >= self._arr[base + _F_TRIAL_SENT_PACKETS]))
            else:
                should_copy = False
            if should_copy:
                self._arr[base + _F_TRIAL_SENT_ACTIVE] = 1
                self._arr[base + _F_TRIAL_SENT_ID] = peer_id
                self._arr[base + _F_TRIAL_SENT_BYTES] = int(
                    record.get("bytes_sent", 0))
                self._arr[base + _F_TRIAL_SENT_PACKETS] = int(
                    record.get("packets_sent", 0))
                self._arr[base + _F_TRIAL_SENT_ACTIVATED_NS] = int(
                    record.get("activated_at_ns", 0) or 0)
                self._arr[base + _F_TRIAL_SENT_DEACTIVATED_NS] = int(
                    record.get("deactivated_at_ns", 0) or 0)
                self._store_optional_sequence(
                    record.get("first_seq"), _F_TRIAL_SENT_FIRST,
                    _F_TRIAL_SENT_FIRST_VALID)
                self._store_optional_sequence(
                    record.get("last_seq"), _F_TRIAL_SENT_LAST,
                    _F_TRIAL_SENT_LAST_VALID)
                self._arr[base + _F_TRIAL_SENT_FIRST_NS] = int(
                    record.get("first_sent_ns", 0) or 0)
                self._arr[base + _F_TRIAL_SENT_LAST_NS] = int(
                    record.get("last_sent_ns", 0) or 0)
            self._end(_F_SENT_VERSION)

        peer_received = int(peer.get("bytes_received", 0) or 0)
        peer_received_packets = int(peer.get("packets_received", 0) or 0)
        peer_trial_received = peer.get("trial_received")
        if (peer_received or peer_received_packets
                or isinstance(peer_trial_received, dict)
                or peer.get("duplicates") or peer.get("out_of_order")):
            self._begin(_F_RECV_VERSION)
            self._arr[base + _F_BYTES_RECV] = max(
                self._arr[base + _F_BYTES_RECV], peer_received)
            self._arr[base + _F_PACKETS_RECV] = max(
                self._arr[base + _F_PACKETS_RECV], peer_received_packets)
            if peer_received or peer_received_packets:
                self._arr[base + _F_PACKETS_LOST] = min(
                    _INT64_MAX, max(0, int(peer.get("packets_lost", 0) or 0)))
            self._arr[base + _F_DUPLICATES] = max(
                self._arr[base + _F_DUPLICATES], int(peer.get("duplicates", 0) or 0))
            self._arr[base + _F_OUT_OF_ORDER] = max(
                self._arr[base + _F_OUT_OF_ORDER], int(peer.get("out_of_order", 0) or 0))
            if isinstance(peer_trial_received, dict):
                record = peer_trial_received
                peer_id = int(record.get("trial_id", 0)) & 0xFFFFFFFF
                current_id = self._arr[base + _F_TRIAL_RECV_ID]
                peer_unique = int(record.get(
                    "unique_packets", record.get("packets_received", 0)) or 0)
                should_copy = (
                    not self._arr[base + _F_TRIAL_RECV_ACTIVE]
                    or _newer_trial_id(peer_id, current_id)
                    or (peer_id == current_id and (
                        peer_unique >= self._arr[base + _F_TRIAL_RECV_PACKETS]
                        or int(record.get("duplicates", 0) or 0)
                        > self._arr[base + _F_TRIAL_RECV_DUPLICATES])))
            else:
                should_copy = False
            if should_copy:
                self._arr[base + _F_TRIAL_RECV_ACTIVE] = 1
                self._arr[base + _F_TRIAL_RECV_ID] = peer_id
                self._arr[base + _F_TRIAL_RECV_BYTES] = int(
                    record.get("bytes_received", 0))
                self._arr[base + _F_TRIAL_RECV_PACKETS] = peer_unique
                self._arr[base + _F_TRIAL_RECV_LOST] = min(
                    _INT64_MAX, max(0, int(record.get("packets_lost", 0) or 0)))
                self._arr[base + _F_TRIAL_RECV_SEQUENCE_GAPS] = min(
                    _INT64_MAX, max(0, int(record.get(
                        "sequence_gaps", record.get("packets_lost", 0)) or 0)))
                self._arr[base + _F_TRIAL_RECV_DUPLICATES] = int(
                    record.get("duplicates", 0))
                self._arr[base + _F_TRIAL_RECV_OUT_OF_ORDER] = int(
                    record.get("out_of_order", 0))
                self._arr[base + _F_TRIAL_RECV_ACTIVATED_NS] = int(
                    record.get("activated_at_ns", 0) or 0)
                self._arr[base + _F_TRIAL_RECV_DEACTIVATED_NS] = int(
                    record.get("deactivated_at_ns", 0) or 0)
                self._store_optional_sequence(
                    record.get("first_seq"), _F_TRIAL_RECV_FIRST,
                    _F_TRIAL_RECV_FIRST_VALID)
                self._store_optional_sequence(
                    record.get("last_seq"), _F_TRIAL_RECV_LAST,
                    _F_TRIAL_RECV_LAST_VALID)
            self._end(_F_RECV_VERSION)

        max_seq = peer.get("max_seq_seen", -1)
        if max_seq is not None and int(max_seq) >= 0:
            current = (
                _decode_u64(self._arr[base + _F_MAX_SEQ])
                if self._arr[base + _F_MAX_SEQ_VALID] else -1)
            if int(max_seq) > current:
                self._arr[base + _F_MAX_SEQ] = _encode_u64(max_seq)
                self._arr[base + _F_MAX_SEQ_VALID] = 1
        peer_rxq_support = peer.get("rxq_overflow_supported")
        peer_rxq_drops = peer.get("kernel_rxq_drops")
        if (peer_rxq_support is True
                or ("rxq_overflow_supported" not in peer
                    and peer_rxq_drops is not None)):
            self._arr[base + _F_RXQ_OVERFLOW_SUPPORTED] = 1
        if (peer_rxq_drops is not None
                and (peer_rxq_support is True
                     or "rxq_overflow_supported" not in peer)):
            self._arr[base + _F_KERNEL_RXQ_DROPS] = max(
                self._arr[base + _F_KERNEL_RXQ_DROPS],
                int(peer_rxq_drops))
        if peer.get("socket_sndbuf_bytes", 0):
            self._arr[base + _F_SOCKET_SNDBUF] = int(
                peer["socket_sndbuf_bytes"])
        if peer.get("socket_rcvbuf_bytes", 0):
            self._arr[base + _F_SOCKET_RCVBUF] = int(
                peer["socket_rcvbuf_bytes"])
        if peer.get("retransmits", 0) > self._arr[base + _F_RETRANS]:
            self._arr[base + _F_RETRANS] = int(peer["retransmits"])
        if peer.get("error"):
            self._arr[base + _F_ERROR] = 1
        self._reconcile_peer_loss()

    def _store_optional_sequence(self, value, field, valid_field):
        base = self._base
        if value is None:
            self._arr[base + valid_field] = 0
        else:
            self._arr[base + field] = _encode_u64(value)
            self._arr[base + valid_field] = 1
