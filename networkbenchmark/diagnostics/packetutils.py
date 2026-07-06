"""Raw packet build/parse helpers for MTR and PMTUD (ICMP/UDP/TCP probes).

Implemented with stdlib `socket`/`struct` only (no scapy dependency) to
minimize install friction across macOS/Linux/Windows. Raw sockets still
require root/administrator privileges; capability probing in
common.privileges determines whether these code paths are reachable.
"""

import socket
import struct

ICMP_ECHO_REQUEST = 8
ICMP_ECHO_REPLY = 0
ICMP_DEST_UNREACHABLE = 3
ICMP_TIME_EXCEEDED = 11
ICMP_FRAG_NEEDED_CODE = 4
ICMP_PORT_UNREACHABLE_CODE = 3


def checksum(data: bytes) -> int:
    if len(data) % 2:
        data = data + b"\x00"
    total = 0
    for i in range(0, len(data), 2):
        word = (data[i] << 8) + data[i + 1]
        total += word
    total = (total >> 16) + (total & 0xFFFF)
    total += total >> 16
    return ~total & 0xFFFF


def build_icmp_echo(identifier: int, seq: int, payload: bytes = b"") -> bytes:
    header = struct.pack("!BBHHH", ICMP_ECHO_REQUEST, 0, 0, identifier & 0xFFFF, seq & 0xFFFF)
    csum = checksum(header + payload)
    header = struct.pack("!BBHHH", ICMP_ECHO_REQUEST, 0, csum, identifier & 0xFFFF, seq & 0xFFFF)
    return header + payload


def parse_ip_header(data: bytes):
    if len(data) < 20:
        return None
    ver_ihl = data[0]
    ihl = (ver_ihl & 0x0F) * 4
    if len(data) < ihl:
        return None
    total_len, ident, flags_frag, ttl, proto = struct.unpack("!HHHBB", data[2:10])
    src = socket.inet_ntoa(data[12:16])
    dst = socket.inet_ntoa(data[16:20])
    return {
        "ihl": ihl,
        "ttl": ttl,
        "proto": proto,
        "src": src,
        "dst": dst,
        "total_len": total_len,
        "ident": ident,
    }


def parse_icmp_packet(raw: bytes):
    """Parse a raw IP packet carrying an ICMP message. Returns a dict or None."""
    ip = parse_ip_header(raw)
    if ip is None:
        return None
    icmp_data = raw[ip["ihl"]:]
    if len(icmp_data) < 8:
        return None

    icmp_type, icmp_code, _icmp_csum, field1, field2 = struct.unpack("!BBHHH", icmp_data[:8])

    result = {
        "ip_src": ip["src"],
        "ip_ttl": ip["ttl"],
        "type": icmp_type,
        "code": icmp_code,
        "id": field1,
        "seq": field2,
    }

    if icmp_type in (ICMP_DEST_UNREACHABLE, ICMP_TIME_EXCEEDED):
        embedded = icmp_data[8:]
        embedded_ip = parse_ip_header(embedded)
        if embedded_ip:
            result["embedded_ip"] = embedded_ip
            payload_start = embedded_ip["ihl"]
            result["embedded_payload"] = embedded[payload_start:payload_start + 8]
        if icmp_type == ICMP_DEST_UNREACHABLE and icmp_code == ICMP_FRAG_NEEDED_CODE:
            result["next_hop_mtu"] = field2

    return result


def build_ip_header(src_ip: str, dst_ip: str, proto: int, payload_len: int, ttl: int, ident: int) -> bytes:
    ver_ihl = (4 << 4) + 5
    tos = 0
    total_len = 20 + payload_len
    flags_frag = 0
    header_csum = 0
    src = socket.inet_aton(src_ip)
    dst = socket.inet_aton(dst_ip)
    header = struct.pack("!BBHHHBBH4s4s", ver_ihl, tos, total_len, ident & 0xFFFF,
                          flags_frag, ttl, proto, header_csum, src, dst)
    csum = checksum(header)
    header = struct.pack("!BBHHHBBH4s4s", ver_ihl, tos, total_len, ident & 0xFFFF,
                          flags_frag, ttl, proto, csum, src, dst)
    return header


def build_tcp_syn(src_ip: str, dst_ip: str, src_port: int, dst_port: int, seq_num: int) -> bytes:
    offset_res_flags = (5 << 12) | 0x02  # data offset=5 words, SYN flag set
    window = 65535
    header = struct.pack("!HHIIHHHH", src_port, dst_port, seq_num, 0,
                          offset_res_flags, window, 0, 0)
    pseudo_header = struct.pack("!4s4sBBH", socket.inet_aton(src_ip), socket.inet_aton(dst_ip),
                                 0, socket.IPPROTO_TCP, len(header))
    csum = checksum(pseudo_header + header)
    header = struct.pack("!HHIIHHHH", src_port, dst_port, seq_num, 0,
                          offset_res_flags, window, csum, 0)
    return header


def parse_tcp_header(data: bytes):
    if len(data) < 20:
        return None
    src_port, dst_port, seq, ack, offset_flags, window, csum, urgent = struct.unpack("!HHIIHHHH", data[:20])
    data_offset = (offset_flags >> 12) * 4
    flags = offset_flags & 0x3F
    return {
        "src_port": src_port,
        "dst_port": dst_port,
        "seq": seq,
        "ack": ack,
        "flags": flags,
        "syn": bool(flags & 0x02),
        "ack_flag": bool(flags & 0x10),
        "rst": bool(flags & 0x04),
        "data_offset": data_offset,
    }


def parse_tcp_header_from_ip(raw: bytes):
    """Parse a raw IP packet (as delivered by an IPPROTO_TCP raw socket)
    into (ip_info, tcp_info), or None if malformed."""
    ip_info = parse_ip_header(raw)
    if ip_info is None:
        return None
    tcp_info = parse_tcp_header(raw[ip_info["ihl"]:])
    if tcp_info is None:
        return None
    return ip_info, tcp_info


def get_default_source_ip(dst_ip: str) -> str:
    """Determine the local source IP that would be used to reach dst_ip."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((dst_ip, 80))
        return s.getsockname()[0]
    finally:
        s.close()
