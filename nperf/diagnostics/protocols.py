"""Protocol reachability matrix: basic UDP/TCP/ICMP plus IPSEC (native ESP),
IPSEC (UDP-encapsulated ESP / NAT-T), and GRE.

Native ESP and GRE require the NPERF server (run with elevated
privileges) to be listening with a matching raw-socket echo responder
(see server.engine). UDP-over-ESP just needs the server's fixed NAT-T UDP
echo listener on port 4500 (no elevated privileges required on either end).
"""

import secrets
import socket
import time

from ..common import constants, net, privileges
from . import connectivity
from . import packetutils as pkt


def _raw_proto_echo_check(server_ip: str, proto: int, label: str,
                           timeout_sec: float = 2.0, attempts: int = 3,
                           header: bytes = b"", family=socket.AF_INET, ipv6=False) -> dict:
    if ipv6:
        family = socket.AF_INET6
    marker = secrets.token_bytes(8)
    # `header` (e.g. a GRE v0 header) precedes the reachability magic so the
    # packet is a well-formed instance of the protocol on the wire. The server
    # echoes the whole body back verbatim, so the round-trip comparison holds.
    payload = header + constants.CONNECTIVITY_MAGIC + marker

    result = {"success": False, "protocol": label, "target": server_ip,
              "ip_version": net.ip_version_label(family=family), "rtt_ms": None, "error": None}

    try:
        sock = socket.socket(family, socket.SOCK_RAW, proto)
    except (PermissionError, OSError) as exc:
        result["error"] = f"requires root/administrator privileges: {exc}"
        return result

    sock.settimeout(0.2)
    try:
        for _ in range(attempts):
            send_time = time.time()
            try:
                sock.sendto(payload, net.sockaddr(server_ip, 0, family))
            except OSError as exc:
                result["error"] = str(exc)
                continue

            deadline = send_time + timeout_sec
            while time.time() < deadline:
                try:
                    raw, _from_addr = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    break

                ip = pkt.parse_ip_header(raw, family=family)
                if not ip or ip["src"] != server_ip:
                    continue
                body = raw[ip["ihl"]:]
                if body == payload:
                    result["success"] = True
                    result["rtt_ms"] = (time.time() - send_time) * 1000.0
                    result["error"] = None
                    return result

        if result["error"] is None:
            result["error"] = f"no {label} echo reply received (timeout) -- server may not be running, " \
                               "may lack privileges, or the path may block this protocol"
    finally:
        sock.close()

    return result


def esp_native_check(server_ip: str, timeout_sec: float = 2.0,
                     family=socket.AF_INET, ipv6=False) -> dict:
    return _raw_proto_echo_check(server_ip, constants.IPPROTO_ESP, "esp_native", timeout_sec,
                                 family=family, ipv6=ipv6)


def gre_check(server_ip: str, timeout_sec: float = 2.0,
              family=socket.AF_INET, ipv6=False) -> dict:
    header = constants.GRE_V0_HEADER
    if ipv6:
        header = constants.GRE_V0_HEADER[:2] + constants.GRE_V0_PROTOCOL_TYPE_IPV6.to_bytes(2, "big")
    return _raw_proto_echo_check(server_ip, constants.IPPROTO_GRE, "gre", timeout_sec,
                                  header=header, family=family, ipv6=ipv6)


def esp_udp_check(server_ip: str, timeout_sec: float = 2.0,
                  family=socket.AF_INET, ipv6=False) -> dict:
    r = connectivity.udp_check(server_ip, constants.IPSEC_NATT_PORT, timeout_sec=timeout_sec,
                                family=family, ipv6=ipv6)
    r["protocol"] = "esp_udp"
    return r


def _safe_check(label, check, family):
    try:
        result = check()
    except Exception as exc:  # noqa: BLE001 - diagnostics must report, not abort, on probe failure
        result = {"success": False, "protocol": label, "rtt_ms": None, "error": str(exc)}
    result.setdefault("protocol", label)
    result.setdefault("ip_version", net.ip_version_label(family=family))
    result.setdefault("success", False)
    result.setdefault("error", None)
    return result


def run_all(server_ip: str, udp_port: int, tcp_port: int, timeout_sec: float = 2.0,
            family=socket.AF_INET, ipv6=False) -> dict:
    if ipv6:
        family = socket.AF_INET6
    try:
        caps = privileges.probe_capabilities()
    except Exception as exc:  # noqa: BLE001 - capability probing must not crash diagnostics
        caps = {}
        capability_error = str(exc)
    else:
        capability_error = None

    results = {
        "server_ip": server_ip,
        "ip_version": net.ip_version_label(family=family),
        "capabilities": caps,
        "icmp": _safe_check("icmp", lambda: connectivity.icmp_ping(
            server_ip, timeout_sec=timeout_sec, family=family), family),
        "udp": _safe_check("udp", lambda: connectivity.udp_check(
            server_ip, udp_port, timeout_sec=timeout_sec, family=family), family),
        "tcp": _safe_check("tcp", lambda: connectivity.tcp_check(
            server_ip, tcp_port, timeout_sec=timeout_sec, family=family), family),
        "esp_udp": _safe_check("esp_udp", lambda: esp_udp_check(
            server_ip, timeout_sec=timeout_sec, family=family), family),
    }
    if capability_error:
        results["capabilities_error"] = capability_error

    if caps.get("protocol_esp") and family == socket.AF_INET:
        results["esp_native"] = _safe_check("esp_native", lambda: esp_native_check(
            server_ip, timeout_sec=timeout_sec, family=family), family)
    elif caps.get("protocol_esp_ipv6") and family == socket.AF_INET6:
        results["esp_native"] = _safe_check("esp_native", lambda: esp_native_check(
            server_ip, timeout_sec=timeout_sec, family=family, ipv6=True), family)
    else:
        capability = "protocol_esp_ipv6" if family == socket.AF_INET6 else "protocol_esp"
        error = (f"capability probing failed: {capability_error}" if capability_error
                 else privileges.capability_reason(capability, caps))
        results["esp_native"] = {"success": False, "skipped": True,
                                  "ip_version": net.ip_version_label(family=family),
                                  "error": error}

    if (caps.get("protocol_gre") and family == socket.AF_INET) or (caps.get("protocol_gre_ipv6") and family == socket.AF_INET6):
        results["gre"] = _safe_check("gre", lambda: gre_check(
            server_ip, timeout_sec=timeout_sec, family=family, ipv6=family == socket.AF_INET6), family)
    else:
        capability = "protocol_gre_ipv6" if family == socket.AF_INET6 else "protocol_gre"
        error = (f"capability probing failed: {capability_error}" if capability_error
                 else privileges.capability_reason(capability, caps))
        results["gre"] = {"success": False, "skipped": True,
                           "ip_version": net.ip_version_label(family=family),
                           "error": error}

    return results
