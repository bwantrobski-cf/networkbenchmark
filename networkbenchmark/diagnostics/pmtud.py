"""Path MTU Discovery test against the server.

Sends DF-flagged UDP probes of increasing/binary-searched size to the
server's UDP echo port. A successful echo confirms that size traverses the
path un-fragmented; an ICMP "Fragmentation Needed"/"Packet Too Big" reply
(captured via a raw ICMP socket) reveals the offending hop's next-hop MTU.
Either a successful echo or a received ICMP error also confirms end-to-end
reachability to the server, satisfying the "verify connectivity" goal.

Requires a raw ICMP socket (root/administrator).
"""

import platform
import secrets
import socket
import time

from ..common import constants
from . import packetutils as pkt

MIN_PROBE_SIZE = 68
MAX_PROBE_SIZE = 9000
DEFAULT_TIMEOUT_SEC = 1.5
DEFAULT_ATTEMPTS_PER_SIZE = 2


class PmtudError(Exception):
    pass


def _set_dont_fragment(sock: socket.socket):
    system = platform.system()
    try:
        if system == "Linux":
            IP_MTU_DISCOVER = getattr(socket, "IP_MTU_DISCOVER", 10)
            IP_PMTUDISC_DO = 2
            sock.setsockopt(socket.IPPROTO_IP, IP_MTU_DISCOVER, IP_PMTUDISC_DO)
        elif system == "Darwin":
            IP_DONTFRAG = getattr(socket, "IP_DONTFRAG", 28)
            sock.setsockopt(socket.IPPROTO_IP, IP_DONTFRAG, 1)
        elif system == "Windows":
            IP_DONTFRAGMENT = getattr(socket, "IP_DONTFRAGMENT", 14)
            sock.setsockopt(socket.IPPROTO_IP, IP_DONTFRAGMENT, 1)
        else:
            raise PmtudError(f"don't-fragment option not implemented for platform {system}")
    except OSError as exc:
        raise PmtudError(f"failed to set don't-fragment socket option: {exc}")


def _probe_size(udp_sock, icmp_sock, server_ip, udp_port, size, timeout_sec):
    """Return dict: {status: 'ok'|'too_big'|'timeout', rtt_ms, next_hop_mtu}."""
    header_len = len(constants.CONNECTIVITY_MAGIC) + 8
    pad_len = max(0, size - header_len)
    payload = constants.CONNECTIVITY_MAGIC + secrets.token_bytes(8) + (b"\x00" * pad_len)

    send_time = time.time()
    try:
        udp_sock.sendto(payload, (server_ip, udp_port))
    except OSError as exc:
        # EMSGSIZE (or similar) means the packet is too big for the local interface MTU
        return {"status": "too_big", "rtt_ms": None, "next_hop_mtu": None, "error": str(exc)}

    deadline = send_time + timeout_sec
    while time.time() < deadline:
        try:
            data, _ = udp_sock.recvfrom(constants.BUFSZ)
            if data == payload:
                return {"status": "ok", "rtt_ms": (time.time() - send_time) * 1000.0, "next_hop_mtu": None}
        except socket.timeout:
            pass
        except OSError:
            pass

        try:
            raw, _ = icmp_sock.recvfrom(65535)
        except socket.timeout:
            continue
        except OSError:
            continue

        parsed = pkt.parse_icmp_packet(raw)
        if not parsed:
            continue
        if parsed["type"] == pkt.ICMP_DEST_UNREACHABLE and parsed["code"] == pkt.ICMP_FRAG_NEEDED_CODE:
            embedded_ip = parsed.get("embedded_ip")
            if embedded_ip and embedded_ip["dst"] == server_ip:
                return {"status": "too_big", "rtt_ms": None, "next_hop_mtu": parsed.get("next_hop_mtu") or None}

    return {"status": "timeout", "rtt_ms": None, "next_hop_mtu": None}


def run_pmtud(server_ip: str, udp_port: int = constants.DEFAULT_UDP_PORT,
              min_size: int = MIN_PROBE_SIZE, max_size: int = MAX_PROBE_SIZE,
              timeout_sec: float = DEFAULT_TIMEOUT_SEC,
              attempts_per_size: int = DEFAULT_ATTEMPTS_PER_SIZE) -> dict:
    try:
        icmp_sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
    except (PermissionError, OSError) as exc:
        raise PmtudError(f"unable to open raw ICMP socket (requires root/admin): {exc}")
    icmp_sock.settimeout(0.05)

    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.settimeout(0.05)
    _set_dont_fragment(udp_sock)

    trace = []
    connectivity_confirmed = False
    reported_mtu_hint = None

    def probe_with_retries(size):
        nonlocal connectivity_confirmed, reported_mtu_hint
        last = None
        for _ in range(attempts_per_size):
            last = _probe_size(udp_sock, icmp_sock, server_ip, udp_port, size, timeout_sec)
            trace.append({"size": size, **last})
            if last["status"] in ("ok", "too_big"):
                connectivity_confirmed = True
                if last.get("next_hop_mtu"):
                    reported_mtu_hint = last["next_hop_mtu"]
                return last
        return last

    try:
        low_result = probe_with_retries(min_size)
        if low_result["status"] != "ok":
            return {
                "server_ip": server_ip,
                "udp_port": udp_port,
                "success": False,
                "connectivity_confirmed": connectivity_confirmed,
                "path_mtu": None,
                "reported_next_hop_mtu": reported_mtu_hint,
                "trace": trace,
                "error": "server did not respond even at the minimum probe size; check connectivity first",
            }

        low, high = min_size, max_size
        high_result = probe_with_retries(high)
        if high_result["status"] == "ok":
            low = high
        else:
            while high - low > 1:
                mid = (low + high) // 2
                result = probe_with_retries(mid)
                if result["status"] == "ok":
                    low = mid
                else:
                    high = mid

        return {
            "server_ip": server_ip,
            "udp_port": udp_port,
            "success": True,
            "connectivity_confirmed": connectivity_confirmed,
            "path_mtu": low + 28,  # add back IP(20)+UDP(8) header bytes for a conventional MTU figure
            "path_mtu_payload_bytes": low,
            "reported_next_hop_mtu": reported_mtu_hint,
            "trace": trace,
            "error": None,
        }
    finally:
        icmp_sock.close()
        udp_sock.close()
