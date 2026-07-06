"""Privilege / raw-socket capability detection.

NetworkBenchmark tries to use raw sockets (via scapy) for MTR, PMTUD, and
ESP/GRE protocol checks. Those features require root/admin on most
platforms. Rather than failing outright, we probe capabilities at startup
and expose them so the CLI/UI can gracefully disable (grey out) whatever
isn't available while still running everything that is.
"""

import ctypes
import os
import platform
import socket

_CAPS_CACHE = None


def is_admin() -> bool:
    system = platform.system()
    try:
        if system == "Windows":
            return bool(ctypes.windll.shell32.IsUserAnAdmin())
        return hasattr(os, "geteuid") and os.geteuid() == 0
    except Exception:
        return False


def _try_raw_socket(proto: int) -> bool:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_RAW, proto)
        s.close()
        return True
    except (PermissionError, OSError):
        return False


def probe_capabilities(force: bool = False) -> dict:
    """Return a dict of capability flags. Cached after first call."""
    global _CAPS_CACHE
    if _CAPS_CACHE is not None and not force:
        return _CAPS_CACHE

    admin = is_admin()
    icmp_raw = _try_raw_socket(socket.IPPROTO_ICMP)
    tcp_raw = _try_raw_socket(socket.IPPROTO_TCP)
    esp_raw = _try_raw_socket(getattr(socket, "IPPROTO_ESP", 50))
    gre_raw = _try_raw_socket(getattr(socket, "IPPROTO_GRE", 47))

    caps = {
        "platform": platform.system(),
        "is_admin": admin,
        "icmp_raw": icmp_raw,
        "tcp_raw": tcp_raw,
        "esp_raw": esp_raw,
        "gre_raw": gre_raw,
        # Feature-level gates consumed by CLI/UI
        "mtr_icmp": icmp_raw,
        "mtr_udp": icmp_raw,
        "mtr_tcp": icmp_raw and tcp_raw,
        "pmtud": icmp_raw,
        "protocol_icmp": True,  # basic ICMP check falls back to the system ping binary
        "protocol_udp": True,
        "protocol_tcp": True,
        "protocol_esp": esp_raw,
        "protocol_esp_udp": True,  # UDP/4500 based, no raw socket required
        "protocol_gre": gre_raw,
        "throughput_tcp": True,
        "throughput_udp": True,
    }
    caps["mtr"] = caps["mtr_icmp"] or caps["mtr_udp"] or caps["mtr_tcp"]
    _CAPS_CACHE = caps
    return caps


def capability_reason(cap_name: str, caps: dict = None) -> str:
    """Human readable reason a capability is unavailable, for UI tooltips."""
    caps = caps or probe_capabilities()
    if caps.get(cap_name):
        return "available"
    if not caps.get("is_admin"):
        return "requires root/administrator privileges (raw sockets)"
    return "unavailable on this platform"
