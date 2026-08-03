"""Multiprocessing-shared per-stream counters.

When a throughput test runs its streams across worker *processes* (see the
`--procs` option), the per-stream counters must live in shared memory so the
main process can poll them for the live interval series, peer_stats relay, and
final summary.

Design / safety:
- Backed by a single ``multiprocessing.Array('q', ...)`` (signed 64-bit ints),
  laid out as ``num_streams * _NUM_FIELDS`` slots.
- Each stream is owned end-to-end by exactly one worker process, so every
  counter *field* has a single writer:
    * a sender worker writes bytes_sent / packets_sent,
    * a receiver worker writes bytes_received / packets_received / max_seq_seen,
    * the main process writes the peer-relayed field (merge) and retransmits.
  Because writers never contend for the same slot, we deliberately use
  ``lock=False`` for speed; reads by the main process are monitoring snapshots
  and tolerate being momentarily stale.
- ``SharedCounterView`` exposes the same subset of the ``StreamCounters``
  interface the stream loops already call (``add_sent`` / ``add_received`` /
  ``set_retransmits`` / ``set_error`` / ``snapshot``), so the existing
  sender/receiver loops work unchanged whether given a thread-local
  ``StreamCounters`` or a shared view.
"""

import multiprocessing as mp

# Field offsets within each stream's slice of the shared array.
_F_BYTES_SENT = 0
_F_PACKETS_SENT = 1
_F_BYTES_RECV = 2
_F_PACKETS_RECV = 3
_F_MAX_SEQ = 4      # highest UDP sequence number seen (-1 if none)
_F_RETRANS = 5
_F_ERROR = 6        # 0 = ok, 1 = an error occurred on this stream
_NUM_FIELDS = 7


class SharedCounters:
    """Owns the shared array for all streams and hands out per-stream views."""

    def __init__(self, num_streams: int, ctx=None):
        self.num_streams = num_streams
        array_factory = (ctx or mp).Array
        self._arr = array_factory("q", num_streams * _NUM_FIELDS, lock=False)
        for i in range(num_streams):
            self._arr[i * _NUM_FIELDS + _F_MAX_SEQ] = -1

    def view(self, index: int) -> "SharedCounterView":
        return SharedCounterView(self._arr, index)

    def snapshot(self, index: int) -> dict:
        return SharedCounterView(self._arr, index).snapshot()


class SharedCounterView:
    """Per-stream accessor with the StreamCounters-compatible method subset."""

    def __init__(self, arr, index: int):
        self._arr = arr
        self.index = index
        self._base = index * _NUM_FIELDS

    def add_sent(self, nbytes: int, npackets: int = 1):
        self._arr[self._base + _F_BYTES_SENT] += nbytes
        self._arr[self._base + _F_PACKETS_SENT] += npackets

    def add_received(self, nbytes: int, seq=None):
        self._arr[self._base + _F_BYTES_RECV] += nbytes
        self._arr[self._base + _F_PACKETS_RECV] += 1
        if seq is not None and seq > self._arr[self._base + _F_MAX_SEQ]:
            self._arr[self._base + _F_MAX_SEQ] = seq

    def set_retransmits(self, value: int):
        self._arr[self._base + _F_RETRANS] = int(value)

    def set_error(self, message: str):
        # Shared memory can't hold the string cheaply; record a boolean flag.
        self._arr[self._base + _F_ERROR] = 1

    def snapshot(self) -> dict:
        base = self._base
        max_seq = self._arr[base + _F_MAX_SEQ]
        packets_received = self._arr[base + _F_PACKETS_RECV]
        packets_lost = 0
        if max_seq >= 0:
            packets_lost = max(0, (max_seq + 1) - packets_received)
        return {
            "index": self.index,
            "bytes_sent": self._arr[base + _F_BYTES_SENT],
            "packets_sent": self._arr[base + _F_PACKETS_SENT],
            "bytes_received": self._arr[base + _F_BYTES_RECV],
            "packets_received": packets_received,
            "max_seq_seen": max_seq,
            "packets_lost": packets_lost,
            "retransmits": self._arr[base + _F_RETRANS],
            "error": bool(self._arr[base + _F_ERROR]),
        }

    def merge_peer_snapshot(self, peer: dict):
        """Merge peer-relayed counters (used by the main process for the
        receiver-side totals the local worker doesn't see)."""
        base = self._base
        if peer.get("bytes_sent", 0) > self._arr[base + _F_BYTES_SENT]:
            self._arr[base + _F_BYTES_SENT] = peer["bytes_sent"]
            self._arr[base + _F_PACKETS_SENT] = peer.get("packets_sent",
                                                          self._arr[base + _F_PACKETS_SENT])
        if peer.get("bytes_received", 0) > self._arr[base + _F_BYTES_RECV]:
            self._arr[base + _F_BYTES_RECV] = peer["bytes_received"]
            self._arr[base + _F_PACKETS_RECV] = peer.get("packets_received",
                                                         self._arr[base + _F_PACKETS_RECV])
        if peer.get("max_seq_seen", -1) > self._arr[base + _F_MAX_SEQ]:
            self._arr[base + _F_MAX_SEQ] = peer["max_seq_seen"]
        if peer.get("retransmits", 0) > self._arr[base + _F_RETRANS]:
            self._arr[base + _F_RETRANS] = peer["retransmits"]
        if peer.get("error"):
            self._arr[base + _F_ERROR] = 1
