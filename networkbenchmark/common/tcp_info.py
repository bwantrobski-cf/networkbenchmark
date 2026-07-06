"""Best-effort cross-platform TCP retransmit counter.

Linux exposes `tcpi_total_retrans` via TCP_INFO with a stable field layout
for the fields we need, so we parse it precisely there. macOS/Windows lack a
straightforward, stable public API for this from Python, so we report None
("n/a") rather than risk silently-wrong numbers.
"""

import platform
import socket
import struct

_LINUX_TCP_INFO_FMT = "=6B2B23I"  # 8 bytes fixed header + 23 x u32
_LINUX_TCP_INFO_SIZE = struct.calcsize(_LINUX_TCP_INFO_FMT)
_LINUX_TOTAL_RETRANS_INDEX = 23  # 0-based index into the 24-field u32 array (24th field)


def get_tcp_retransmits(sock: socket.socket):
    """Return total retransmitted segments for this socket, or None if
    unavailable on this platform."""
    system = platform.system()
    if system != "Linux":
        return None

    try:
        raw = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, _LINUX_TCP_INFO_SIZE)
        if len(raw) < _LINUX_TCP_INFO_SIZE:
            return None
        fields = struct.unpack(_LINUX_TCP_INFO_FMT, raw)
        # fields[0:8] are the individual bytes/bitfields, fields[8:] are the u32 array
        u32_fields = fields[8:]
        return u32_fields[_LINUX_TOTAL_RETRANS_INDEX]
    except (OSError, AttributeError, struct.error):
        return None


def tcp_retransmit_support() -> bool:
    return platform.system() == "Linux"
