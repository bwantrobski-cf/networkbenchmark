"""Best-effort cross-platform TCP retransmit counter.

Linux exposes `tcpi_total_retrans` via TCP_INFO and macOS exposes
`tcpi_txretransmitpackets` via TCP_CONNECTION_INFO. Both are cumulative
per-socket counters and are sampled from the data-sending endpoint.
"""

import platform
import socket
import struct

# Linux `struct tcp_info` begins with an 8-byte fixed header (8 x u8), followed
# by a stable sequence of u32 fields. `tcpi_total_retrans` is the 24th u32
# (0-based index 23), i.e. at byte offset 8 + 23*4 = 100. We read it by offset
# rather than unpacking the whole struct, since the struct has grown across
# kernel versions and its total size is not fixed.
_LINUX_HEADER_BYTES = 8
_LINUX_TOTAL_RETRANS_U32_INDEX = 23
_LINUX_TOTAL_RETRANS_OFFSET = _LINUX_HEADER_BYTES + _LINUX_TOTAL_RETRANS_U32_INDEX * 4  # 100
_LINUX_TCP_INFO_BUFSIZE = 256
_MACOS_TCP_CONNECTION_INFO = getattr(socket, "TCP_CONNECTION_INFO", None)
_MACOS_TCP_CONNECTION_INFO_BUFSIZE = 128
_MACOS_TX_RETRANS_PACKETS_OFFSET = 104


def get_tcp_retransmits(sock: socket.socket):
    """Return total retransmitted segments for this socket, or None if
    unavailable on this platform. Never raises."""
    system = platform.system()
    try:
        if system == "Linux":
            raw = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, _LINUX_TCP_INFO_BUFSIZE)
            if len(raw) < _LINUX_TOTAL_RETRANS_OFFSET + 4:
                return None
            (total_retrans,) = struct.unpack_from("=I", raw, _LINUX_TOTAL_RETRANS_OFFSET)
            return total_retrans
        if system == "Darwin" and _MACOS_TCP_CONNECTION_INFO is not None:
            raw = sock.getsockopt(socket.IPPROTO_TCP, _MACOS_TCP_CONNECTION_INFO,
                                  _MACOS_TCP_CONNECTION_INFO_BUFSIZE)
            if len(raw) < _MACOS_TX_RETRANS_PACKETS_OFFSET + 8:
                return None
            (total_retrans,) = struct.unpack_from("=Q", raw, _MACOS_TX_RETRANS_PACKETS_OFFSET)
            return total_retrans
    except (OSError, AttributeError, struct.error):
        return None
    return None


def tcp_retransmit_support() -> bool:
    return (platform.system() == "Linux"
            or (platform.system() == "Darwin" and _MACOS_TCP_CONNECTION_INFO is not None))
