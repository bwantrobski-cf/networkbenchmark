"""Path MTU Discovery tests against the server.

The primary method sends DF-flagged UDP probes of increasing/binary-searched
size to the server's UDP echo port. When UDP PMTUD fails, the fallback sends
DF-flagged ICMP echo probes, starting at 1450 bytes and refining the largest
successful packet. The discovery chain ultimately uses a fixed fallback MTU
when both probe methods fail.

Requires a raw ICMP socket (root/administrator).
"""

import secrets
import socket
import struct
import time

from ..common import constants, net
from . import packetutils as pkt

MIN_PROBE_SIZE = 68
MAX_PROBE_SIZE = 9000
DEFAULT_TIMEOUT_SEC = 1.5
DEFAULT_ATTEMPTS_PER_SIZE = 2
ICMP_START_PACKET_BYTES = 1450
ICMP_STEP_BYTES = 50
ICMP_MIN_PACKET_BYTES = 576
ICMP_MAX_PACKET_BYTES = 9000


class PmtudError(Exception):
    pass


def _set_dont_fragment(sock: socket.socket, family=socket.AF_INET):
    try:
        net.configure_dont_fragment(sock)
    except OSError as exc:
        raise PmtudError(f"failed to set don't-fragment socket option: {exc}")


def _probe_size(udp_sock, icmp_sock, server_ip, udp_port, size, timeout_sec, family):
    """Return dict: {status: 'ok'|'too_big'|'timeout', rtt_ms, next_hop_mtu}."""
    header_len = len(constants.CONNECTIVITY_MAGIC) + 8
    pad_len = max(0, size - header_len)
    payload = constants.CONNECTIVITY_MAGIC + secrets.token_bytes(8) + (b"\x00" * pad_len)

    send_time = time.time()
    try:
        udp_sock.sendto(payload, net.sockaddr(server_ip, udp_port, family))
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

        parsed = pkt.parse_icmp_packet(raw, family=family)
        if not parsed:
            continue
        too_big = ((family == socket.AF_INET and parsed["type"] == pkt.ICMP_DEST_UNREACHABLE
                    and parsed["code"] == pkt.ICMP_FRAG_NEEDED_CODE)
                   or (family == socket.AF_INET6 and parsed["type"] == pkt.ICMPV6_PACKET_TOO_BIG))
        if too_big:
            embedded_ip = parsed.get("embedded_ip")
            if embedded_ip and embedded_ip["dst"] == server_ip:
                return {"status": "too_big", "rtt_ms": None, "next_hop_mtu": parsed.get("next_hop_mtu") or None}

    return {"status": "timeout", "rtt_ms": None, "next_hop_mtu": None}


def _search_packet_size(probe, start: int = ICMP_START_PACKET_BYTES,
                       step: int = ICMP_STEP_BYTES,
                       min_size: int = ICMP_MIN_PACKET_BYTES,
                       max_size: int = ICMP_MAX_PACKET_BYTES) -> dict:
    trace = []

    def attempt(size: int) -> dict:
        result = dict(probe(size))
        result["size"] = size
        trace.append(result)
        return result

    start = max(min_size, min(start, max_size))
    first = attempt(start)
    if first.get("status") == "ok":
        low = start
        high = start + step
        while high <= max_size:
            result = attempt(high)
            if result.get("status") != "ok":
                break
            low = high
            high += step
        else:
            return {"success": True, "packet_bytes": low, "trace": trace, "error": None}
    else:
        high = start
        low = start - step
        while low >= min_size:
            result = attempt(low)
            if result.get("status") == "ok":
                break
            high = low
            low -= step
        else:
            return {
                "success": False,
                "packet_bytes": None,
                "trace": trace,
                "error": "ICMP echo did not respond at the minimum probe size",
            }

    while high - low > 1:
        middle = (low + high) // 2
        result = attempt(middle)
        if result.get("status") == "ok":
            low = middle
        else:
            high = middle

    return {"success": True, "packet_bytes": low, "trace": trace, "error": None}


def _icmp_probe_size(icmp_sock, server_ip: str, family, identifier: int,
                     sequence: int, packet_size: int, timeout_sec: float) -> dict:
    ip_header_bytes = 40 if family == socket.AF_INET6 else 20
    icmp_header_bytes = 8
    payload_size = max(0, packet_size - ip_header_bytes - icmp_header_bytes)
    prefix = constants.CONNECTIVITY_MAGIC + secrets.token_bytes(8)
    payload = prefix + (b"\x00" * max(0, payload_size - len(prefix)))
    if family == socket.AF_INET6:
        source_ip = pkt.get_default_source_ip(server_ip, family=family)
        packet = pkt.build_icmpv6_echo(source_ip, server_ip, identifier, sequence, payload)
    else:
        packet = pkt.build_icmp_echo(identifier, sequence, payload)

    try:
        icmp_sock.sendto(packet, net.sockaddr(server_ip, 0, family))
    except OSError as exc:
        return {"status": "too_big", "rtt_ms": None, "next_hop_mtu": None, "error": str(exc)}

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            raw, _ = icmp_sock.recvfrom(65535)
        except socket.timeout:
            continue
        except OSError as exc:
            return {"status": "timeout", "rtt_ms": None, "next_hop_mtu": None, "error": str(exc)}

        parsed = pkt.parse_icmp_packet(raw, family=family)
        if not parsed:
            continue
        if (parsed.get("ip_src") == server_ip
                and parsed.get("id") == (identifier & 0xFFFF)
                and parsed.get("seq") == (sequence & 0xFFFF)
                and parsed.get("type") in (pkt.ICMP_ECHO_REPLY, pkt.ICMPV6_ECHO_REPLY)):
            return {"status": "ok", "rtt_ms": None, "next_hop_mtu": None}

        too_big = ((family == socket.AF_INET and parsed.get("type") == pkt.ICMP_DEST_UNREACHABLE
                    and parsed.get("code") == pkt.ICMP_FRAG_NEEDED_CODE)
                   or (family == socket.AF_INET6 and parsed.get("type") == pkt.ICMPV6_PACKET_TOO_BIG))
        embedded = parsed.get("embedded_ip")
        embedded_payload = parsed.get("embedded_payload", b"")
        if too_big and embedded and embedded.get("dst") == server_ip and len(embedded_payload) >= 8:
            _type, _code, _checksum, embedded_id, embedded_seq = struct.unpack("!BBHHH", embedded_payload[:8])
            if embedded_id == (identifier & 0xFFFF) and embedded_seq == (sequence & 0xFFFF):
                return {
                    "status": "too_big",
                    "rtt_ms": None,
                    "next_hop_mtu": parsed.get("next_hop_mtu"),
                }

    return {"status": "timeout", "rtt_ms": None, "next_hop_mtu": None}


def run_icmp_pmtud(server_ip: str, timeout_sec: float = DEFAULT_TIMEOUT_SEC,
                   attempts_per_size: int = DEFAULT_ATTEMPTS_PER_SIZE,
                   family=socket.AF_INET, ipv6=False) -> dict:
    if ipv6:
        family = socket.AF_INET6
    server_ip = net.resolve_host(server_ip, family=family)
    icmp_protocol = socket.IPPROTO_ICMPV6 if family == socket.AF_INET6 else socket.IPPROTO_ICMP
    try:
        icmp_sock = socket.socket(family, socket.SOCK_RAW, icmp_protocol)
    except (PermissionError, OSError) as exc:
        raise PmtudError(f"unable to open raw ICMP socket for ICMP PMTUD (requires root/admin): {exc}")

    try:
        icmp_sock.settimeout(0.05)
        _set_dont_fragment(icmp_sock, family=family)
    except Exception:
        icmp_sock.close()
        raise
    identifier = secrets.randbelow(0x10000)
    sequence = secrets.randbelow(0x10000)
    sequence_lock = [sequence]

    def probe(packet_size: int) -> dict:
        last_result = {"status": "timeout", "rtt_ms": None, "next_hop_mtu": None}
        for _ in range(max(1, attempts_per_size)):
            sequence_lock[0] = (sequence_lock[0] + 1) & 0xFFFF
            last_result = _icmp_probe_size(
                icmp_sock, server_ip, family, identifier, sequence_lock[0], packet_size, timeout_sec)
            if last_result.get("status") == "ok":
                return last_result
        return last_result

    try:
        search = _search_packet_size(probe)
        path_mtu = int(search["packet_bytes"]) if search["success"] else None
        return {
            "server_ip": server_ip,
            "ip_version": net.ip_version_label(family=family),
            "success": bool(search["success"]),
            "connectivity_confirmed": bool(search["success"]),
            "path_mtu": path_mtu,
            "path_mtu_payload_bytes": (
                int(search["packet_bytes"]) - (48 if family == socket.AF_INET6 else 28)
                if search["success"] else None
            ),
            "reported_next_hop_mtu": next(
                (item.get("next_hop_mtu") for item in search["trace"] if item.get("next_hop_mtu")),
                None,
            ),
            "trace": search["trace"],
            "error": search["error"],
        }
    finally:
        icmp_sock.close()


def discover_path_mtu(server_ip: str, udp_port: int = constants.DEFAULT_UDP_PORT,
                     min_size: int = MIN_PROBE_SIZE, max_size: int = MAX_PROBE_SIZE,
                     timeout_sec: float = DEFAULT_TIMEOUT_SEC,
                     attempts_per_size: int = DEFAULT_ATTEMPTS_PER_SIZE,
                     family=socket.AF_INET, ipv6=False) -> dict:
    try:
        primary = run_pmtud(
            server_ip,
            udp_port=udp_port,
            min_size=min_size,
            max_size=max_size,
            timeout_sec=timeout_sec,
            attempts_per_size=attempts_per_size,
            family=family,
            ipv6=ipv6,
        )
    except (PmtudError, OSError, ValueError) as exc:
        primary = {
            "server_ip": server_ip,
            "udp_port": udp_port,
            "ip_version": net.ip_version_label(family=family),
            "success": False,
            "connectivity_confirmed": False,
            "path_mtu": None,
            "path_mtu_payload_bytes": None,
            "reported_next_hop_mtu": None,
            "bidirectional_validated": False,
            "bidirectional_validation": "unavailable",
            "trace": [],
            "error": str(exc),
        }

    if primary.get("success") and isinstance(primary.get("path_mtu"), int):
        primary["path_mtu_source"] = "udp_pmtud"
        primary["fallback_used"] = False
        return primary

    try:
        icmp_result = run_icmp_pmtud(
            server_ip,
            timeout_sec=timeout_sec,
            attempts_per_size=attempts_per_size,
            family=family,
            ipv6=ipv6,
        )
    except (PmtudError, OSError, ValueError) as exc:
        icmp_result = {
            "server_ip": server_ip,
            "ip_version": net.ip_version_label(family=family),
            "success": False,
            "connectivity_confirmed": False,
            "path_mtu": None,
            "path_mtu_payload_bytes": None,
            "reported_next_hop_mtu": None,
            "bidirectional_validated": False,
            "bidirectional_validation": "unavailable",
            "trace": [],
            "error": str(exc),
        }

    primary["icmp_pmtud"] = icmp_result
    if icmp_result.get("success") and isinstance(icmp_result.get("path_mtu"), int):
        primary.update({
            "success": True,
            "connectivity_confirmed": True,
            "path_mtu": icmp_result["path_mtu"],
            "path_mtu_payload_bytes": icmp_result.get("path_mtu_payload_bytes"),
            "reported_next_hop_mtu": icmp_result.get("reported_next_hop_mtu"),
            "bidirectional_validated": False,
            "bidirectional_validation": "icmp_echo_only",
            "path_mtu_source": "icmp_pmtud",
            "fallback_used": True,
            "fallback_reason": "UDP PMTUD failed; ICMP echo DF PMTUD succeeded",
        })
        return primary

    primary.update({
        "path_mtu": constants.FALLBACK_PATH_MTU,
        "path_mtu_source": "fixed_fallback",
        "fallback_used": True,
        "fallback_reason": "UDP and ICMP PMTUD failed; using fixed fallback MTU",
        "bidirectional_validated": False,
        "bidirectional_validation": "fixed_fallback",
        "path_mtu_payload_bytes": None,
    })
    return primary


def run_pmtud(server_ip: str, udp_port: int = constants.DEFAULT_UDP_PORT,
              min_size: int = MIN_PROBE_SIZE, max_size: int = MAX_PROBE_SIZE,
              timeout_sec: float = DEFAULT_TIMEOUT_SEC,
              attempts_per_size: int = DEFAULT_ATTEMPTS_PER_SIZE,
              family=socket.AF_INET, ipv6=False) -> dict:
    if ipv6:
        family = socket.AF_INET6
    server_ip = net.resolve_host(server_ip, family=family)
    icmp_protocol = socket.IPPROTO_ICMPV6 if family == socket.AF_INET6 else socket.IPPROTO_ICMP
    try:
        icmp_sock = socket.socket(family, socket.SOCK_RAW, icmp_protocol)
    except (PermissionError, OSError) as exc:
        raise PmtudError(f"unable to open raw ICMP socket (requires root/admin): {exc}")
    icmp_sock.settimeout(0.05)

    udp_sock = socket.socket(family, socket.SOCK_DGRAM)
    udp_sock.settimeout(0.05)
    _set_dont_fragment(udp_sock, family=family)

    trace = []
    connectivity_confirmed = False
    reported_mtu_hint = None

    def probe_with_retries(size):
        nonlocal connectivity_confirmed, reported_mtu_hint
        last = None
        for _ in range(attempts_per_size):
            last = _probe_size(udp_sock, icmp_sock, server_ip, udp_port, size, timeout_sec, family)
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
                "ip_version": net.ip_version_label(family=family),
                "success": False,
                "connectivity_confirmed": connectivity_confirmed,
                "path_mtu": None,
                "reported_next_hop_mtu": reported_mtu_hint,
                "bidirectional_validated": False,
                "bidirectional_validation": "udp_df_echo",
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
            "ip_version": net.ip_version_label(family=family),
            "success": True,
            "connectivity_confirmed": connectivity_confirmed,
            "path_mtu": low + (48 if family == socket.AF_INET6 else 28),  # add back IP+UDP headers
            "path_mtu_payload_bytes": low,
            "reported_next_hop_mtu": reported_mtu_hint,
            "bidirectional_validated": True,
            "bidirectional_validation": "udp_df_echo",
            "trace": trace,
            "error": None,
        }
    finally:
        icmp_sock.close()
        udp_sock.close()
