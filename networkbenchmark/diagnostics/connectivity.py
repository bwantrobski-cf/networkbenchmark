"""Basic ICMP / UDP / TCP connectivity checks against the server.

ICMP uses the OS `ping` binary (works without elevated privileges on every
major platform since the system ping tool already carries the necessary
capabilities/setuid bit). UDP/TCP checks use a lightweight echo protocol
against the server's configured base ports, answered by the echo responders
in server.engine.
"""

import platform
import re
import secrets
import socket
import subprocess
import time

from ..common import constants

_TIME_RE = re.compile(r"time[=<]\s*([\d.]+)\s*ms", re.IGNORECASE)
_LOSS_RE = re.compile(r"([\d.]+)%\s*(?:packet)?\s*loss", re.IGNORECASE)


def icmp_ping(target_ip: str, count: int = 4, timeout_sec: float = 2.0) -> dict:
    system = platform.system()
    if system == "Windows":
        cmd = ["ping", "-n", str(count), "-w", str(int(timeout_sec * 1000)), target_ip]
    else:
        cmd = ["ping", "-c", str(count), target_ip]

    result = {
        "success": False,
        "target": target_ip,
        "samples_ms": [],
        "loss_pct": None,
        "error": None,
        "raw_output": "",
    }

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                               timeout=timeout_sec * count + 3)
        result["raw_output"] = proc.stdout + proc.stderr
        result["samples_ms"] = [float(m) for m in _TIME_RE.findall(result["raw_output"])]
        loss_match = _LOSS_RE.search(result["raw_output"])
        if loss_match:
            result["loss_pct"] = float(loss_match.group(1))
        result["success"] = proc.returncode == 0 and len(result["samples_ms"]) > 0
    except FileNotFoundError:
        result["error"] = "system 'ping' binary not found"
    except subprocess.TimeoutExpired:
        result["error"] = "ping timed out"
        result["loss_pct"] = 100.0

    return result


def udp_check(server_ip: str, udp_port: int, timeout_sec: float = 2.0) -> dict:
    nonce = secrets.token_bytes(8)
    payload = constants.CONNECTIVITY_MAGIC + nonce

    result = {"success": False, "target": f"{server_ip}:{udp_port}", "rtt_ms": None, "error": None}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout_sec)
    try:
        send_time = time.time()
        sock.sendto(payload, (server_ip, udp_port))
        data, _ = sock.recvfrom(constants.BUFSZ)
        if data == payload:
            result["success"] = True
            result["rtt_ms"] = (time.time() - send_time) * 1000.0
        else:
            result["error"] = "unexpected echo payload"
    except socket.timeout:
        result["error"] = "timed out waiting for UDP echo"
    except OSError as exc:
        result["error"] = str(exc)
    finally:
        sock.close()

    return result


def tcp_check(server_ip: str, tcp_port: int, timeout_sec: float = 2.0) -> dict:
    nonce = secrets.token_bytes(8)
    payload = constants.CONNECTIVITY_MAGIC + nonce

    result = {"success": False, "target": f"{server_ip}:{tcp_port}", "rtt_ms": None, "error": None}
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout_sec)
    try:
        send_time = time.time()
        sock.connect((server_ip, tcp_port))
        sock.sendall(payload)
        data = sock.recv(len(payload))
        if data == payload:
            result["success"] = True
            result["rtt_ms"] = (time.time() - send_time) * 1000.0
        else:
            result["error"] = "unexpected echo payload"
    except socket.timeout:
        result["error"] = "timed out connecting/waiting for TCP echo"
    except OSError as exc:
        result["error"] = str(exc)
    finally:
        sock.close()

    return result


def run_all(server_ip: str, udp_port: int, tcp_port: int, icmp_count: int = 4,
            timeout_sec: float = 2.0) -> dict:
    return {
        "server_ip": server_ip,
        "icmp": icmp_ping(server_ip, count=icmp_count, timeout_sec=timeout_sec),
        "udp": udp_check(server_ip, udp_port, timeout_sec=timeout_sec),
        "tcp": tcp_check(server_ip, tcp_port, timeout_sec=timeout_sec),
    }
