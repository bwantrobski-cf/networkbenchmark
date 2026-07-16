"""Best-effort cross-platform TCP retransmit counter.

Linux exposes `tcpi_total_retrans` via TCP_INFO with a stable field layout
for the fields we need, so we parse it precisely there. macOS/Windows lack a
straightforward, stable public API for this from Python, so we report None
("n/a") rather than risk silently-wrong numbers.
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
_LINUX_TCP_INFO_BUFSIZE = 256  # request generously; kernel returns its actual size


def get_tcp_retransmits(sock: socket.socket):
    """Return total retransmitted segments for this socket, or None if
    unavailable on this platform. Never raises."""
    if platform.system() != "Linux":
        return None

    try:
        raw = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, _LINUX_TCP_INFO_BUFSIZE)
    except (OSError, AttributeError):
        return None

    if len(raw) < _LINUX_TOTAL_RETRANS_OFFSET + 4:
        return None
    try:
        (total_retrans,) = struct.unpack_from("=I", raw, _LINUX_TOTAL_RETRANS_OFFSET)
    except struct.error:
        return None
    return total_retrans


def tcp_retransmit_support() -> bool:
    return platform.system() == "Linux"
