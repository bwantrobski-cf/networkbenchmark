"""MTR (My TraceRoute) style hop-by-hop diagnostics.

Supports ICMP (default), UDP, and TCP (selectable port) probing. Each round
sends one probe per TTL (1..max_hops) until the target responds or
max_hops is reached; rounds repeat at the configured interval until
stopped. Per-hop latency/jitter/avg/best/worst/stddev are computed from all
samples collected so far. Reverse DNS / WHOIS / ASN lookups are performed
per unique hop IP (skipped for RFC1918/CGNAT addresses).

Requires a raw ICMP socket (root/administrator) to observe intermediate-hop
"Time Exceeded" responses regardless of probe protocol; TCP mode
additionally requires a raw TCP socket to observe the final hop's SYN-ACK/RST.
"""

import os
import random
import socket
import struct
import threading
import time

from ..common import addr, constants, net, netinfo, stats
from . import packetutils as pkt


class MtrError(Exception):
    pass


class HopRecord:
    def __init__(self, ttl: int):
        self.ttl = ttl
        self.ip = None
        self.samples_ms = []
        self.probes_sent = 0
        self.probes_lost = 0
        self.info = None  # populated lazily via netinfo

    def to_dict(self):
        latency_stats = stats.compute_latency_stats(self.samples_ms)
        loss_pct = (self.probes_lost / self.probes_sent * 100.0) if self.probes_sent else 0.0
        d = {
            "ttl": self.ttl,
            "ip": self.ip,
            "hostname": (self.info or {}).get("reverse_dns", ""),
            "asn": (self.info or {}).get("asn", ""),
            "asn_description": (self.info or {}).get("asn_description", ""),
            "whois_org": (self.info or {}).get("whois_org", ""),
            "lookup_skipped": (self.info or {}).get("lookup_skipped", False),
            "probes_sent": self.probes_sent,
            "probes_lost": self.probes_lost,
            "loss_pct": loss_pct,
        }
        d.update(latency_stats)
        return d


class MtrRunner:
    def __init__(self, target: str, protocol: str = "icmp", port: int = 33434,
                 interval_ms: int = constants.MTR_DEFAULT_INTERVAL_MS,
                 max_hops: int = constants.MTR_DEFAULT_MAX_HOPS,
                 timeout_sec: float = constants.MTR_DEFAULT_TIMEOUT_SEC,
                 ipv6: bool = False):
        protocol = protocol.lower()
        if protocol not in constants.MTR_PROTOCOLS:
            raise ValueError(f"unsupported MTR protocol: {protocol}")
        interval_ms = max(constants.MTR_MIN_INTERVAL_MS, min(constants.MTR_MAX_INTERVAL_MS, interval_ms))

        self.target_host = target
        self.ipv6 = bool(ipv6)
        self.family = net.family_for_ipv6(self.ipv6)
        self.ip_version = net.ip_version_label(family=self.family)
        self.target_ip = addr.resolve_host(target, family=self.family)
        self.protocol = protocol
        self.port = port
        self.interval_sec = interval_ms / 1000.0
        self.max_hops = max_hops
        self.timeout_sec = timeout_sec

        self._icmp_id = (os.getpid() ^ random.randint(0, 0xFFFF)) & 0xFFFF
        self._tcp_src_port = random.randint(20000, 60000)
        self._hops = {ttl: HopRecord(ttl) for ttl in range(1, max_hops + 1)}
        self._known_ips = {}
        self._rounds_completed = 0

    # -- socket setup ------------------------------------------------------

    def _open_icmp_socket(self):
        try:
            protocol = socket.IPPROTO_ICMPV6 if self.family == socket.AF_INET6 else socket.IPPROTO_ICMP
            s = socket.socket(self.family, socket.SOCK_RAW, protocol)
        except (PermissionError, OSError) as exc:
            raise MtrError(f"unable to open raw ICMP socket (requires root/admin): {exc}")
        s.settimeout(0.05)
        return s

    def _open_tcp_raw_socket(self):
        try:
            s = socket.socket(self.family, socket.SOCK_RAW, socket.IPPROTO_TCP)
        except (PermissionError, OSError) as exc:
            raise MtrError(f"unable to open raw TCP socket (requires root/admin): {exc}")
        s.settimeout(0.05)
        return s

    def _open_udp_send_socket(self):
        return socket.socket(self.family, socket.SOCK_DGRAM)

    def _open_tcp_send_socket(self, src_ip):
        try:
            s = socket.socket(self.family, socket.SOCK_RAW, socket.IPPROTO_TCP)
            if self.family == socket.AF_INET:
                s.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
        except (PermissionError, OSError) as exc:
            raise MtrError(f"unable to open raw IP socket for TCP SYN (requires root/admin): {exc}")
        return s

    # -- main loop -----------------------------------------------------

    def run(self, stop_event: threading.Event, on_round=None, max_rounds=None):
        """Run rounds until stop_event is set (or max_rounds reached).

        on_round(list_of_hop_dicts, round_number) is invoked after each round.
        """
        icmp_sock = self._open_icmp_socket()
        udp_sock = self._open_udp_send_socket() if self.protocol == "udp" else None
        src_ip = pkt.get_default_source_ip(self.target_ip, family=self.family)
        tcp_raw_sock = None
        tcp_send_sock = None
        if self.protocol == "tcp":
            tcp_raw_sock = self._open_tcp_raw_socket()
            tcp_send_sock = self._open_tcp_send_socket(src_ip)

        try:
            round_num = 0
            while not stop_event.is_set():
                round_num += 1
                self._rounds_completed = round_num
                reached = self._do_round(icmp_sock, udp_sock, tcp_send_sock, tcp_raw_sock, src_ip, stop_event)

                snapshot = [self._hops[t].to_dict() for t in sorted(self._hops) if self._hops[t].probes_sent > 0]
                if on_round:
                    on_round(snapshot, round_num)

                if max_rounds and round_num >= max_rounds:
                    break

                stop_event.wait(self.interval_sec)
        finally:
            icmp_sock.close()
            if udp_sock:
                udp_sock.close()
            if tcp_raw_sock:
                tcp_raw_sock.close()
            if tcp_send_sock:
                tcp_send_sock.close()

    def _lookup_ip_info(self, hop: HopRecord):
        if hop.ip is None:
            return
        if hop.ip not in self._known_ips:
            self._known_ips[hop.ip] = netinfo.get_ip_info(hop.ip)
        hop.info = self._known_ips[hop.ip]

    def _do_round(self, icmp_sock, udp_sock, tcp_send_sock, tcp_raw_sock, src_ip, stop_event) -> bool:
        for ttl in range(1, self.max_hops + 1):
            if stop_event.is_set():
                return False
            hop = self._hops[ttl]
            hop.probes_sent += 1

            send_time = time.time()
            probe_port = self.port if self.protocol != "udp" else (33434 + ttl)

            try:
                if self.protocol == "icmp":
                    if self.family == socket.AF_INET6:
                        icmp_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_UNICAST_HOPS, ttl)
                        packet = pkt.build_icmpv6_echo(src_ip, self.target_ip, self._icmp_id, ttl,
                                                       b"nperf-mtr")
                    else:
                        icmp_sock.setsockopt(socket.SOL_IP, socket.IP_TTL, ttl)
                        packet = pkt.build_icmp_echo(self._icmp_id, ttl, b"nperf-mtr")
                    icmp_sock.sendto(packet, net.sockaddr(self.target_ip, 0, self.family))
                elif self.protocol == "udp":
                    if self.family == socket.AF_INET6:
                        udp_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_UNICAST_HOPS, ttl)
                    else:
                        udp_sock.setsockopt(socket.SOL_IP, socket.IP_TTL, ttl)
                    udp_sock.sendto(b"nperf-mtr", net.sockaddr(self.target_ip, probe_port, self.family))
                else:  # tcp
                    tcp_header = (pkt.build_tcp_syn_ipv6(src_ip, self.target_ip, self._tcp_src_port,
                                                         self.port, seq_num=random.randint(0, 0xFFFFFFF))
                                  if self.family == socket.AF_INET6 else
                                  pkt.build_tcp_syn(src_ip, self.target_ip, self._tcp_src_port,
                                                    self.port, seq_num=random.randint(0, 0xFFFFFFF)))
                    if self.family == socket.AF_INET6:
                        tcp_send_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_UNICAST_HOPS, ttl)
                        tcp_send_sock.sendto(tcp_header, net.sockaddr(self.target_ip, 0, self.family))
                    else:
                        ip_header = pkt.build_ip_header(src_ip, self.target_ip, socket.IPPROTO_TCP,
                                                         20, ttl, ident=ttl + 1000)
                        tcp_send_sock.sendto(ip_header + tcp_header, (self.target_ip, 0))
            except OSError:
                hop.probes_lost += 1
                continue

            hop_ip, rtt_ms, is_final = self._wait_for_response(
                icmp_sock, tcp_raw_sock, ttl, send_time, probe_port)

            if hop_ip is None:
                hop.probes_lost += 1
            else:
                hop.ip = hop_ip
                hop.samples_ms.append(rtt_ms)
                self._lookup_ip_info(hop)

            if is_final:
                return True

        return False

    def _wait_for_response(self, icmp_sock, tcp_raw_sock, ttl, send_time, probe_port):
        deadline = send_time + self.timeout_sec
        while time.time() < deadline:
            remaining = deadline - time.time()

            if self.protocol == "tcp" and tcp_raw_sock is not None:
                try:
                    raw, from_addr = tcp_raw_sock.recvfrom(65535)
                    tcp_seg = pkt.parse_tcp_header_from_ip(raw)
                except socket.timeout:
                    tcp_seg = None
                except OSError:
                    tcp_seg = None
                if tcp_seg:
                    ip_info, tcp_info = tcp_seg
                    if (tcp_info["dst_port"] == self._tcp_src_port
                            and tcp_info["src_port"] == self.port
                            and ip_info["src"] == self.target_ip
                            and (tcp_info["syn"] or tcp_info["rst"])):
                        rtt_ms = (time.time() - send_time) * 1000.0
                        return ip_info["src"], rtt_ms, True

            try:
                raw, from_addr = icmp_sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                continue

            parsed = pkt.parse_icmp_packet(raw, family=self.family)
            if not parsed:
                continue

            rtt_ms = (time.time() - send_time) * 1000.0

            if self.protocol == "icmp":
                echo_reply = (pkt.ICMPV6_ECHO_REPLY if self.family == socket.AF_INET6 else pkt.ICMP_ECHO_REPLY)
                if parsed["type"] == echo_reply and parsed["id"] == self._icmp_id and parsed["seq"] == ttl:
                    return parsed["ip_src"], rtt_ms, (parsed["ip_src"] == self.target_ip)
                time_exceeded = (pkt.ICMPV6_TIME_EXCEEDED if self.family == socket.AF_INET6 else pkt.ICMP_TIME_EXCEEDED)
                if parsed["type"] == time_exceeded:
                    embedded = parsed.get("embedded_payload", b"")
                    if len(embedded) >= 8:
                        e_id, e_seq = struct.unpack("!HH", embedded[4:8])
                        if e_id == self._icmp_id and e_seq == ttl:
                            return parsed["ip_src"], rtt_ms, False

            elif self.protocol == "udp":
                time_exceeded = (pkt.ICMPV6_TIME_EXCEEDED if self.family == socket.AF_INET6 else pkt.ICMP_TIME_EXCEEDED)
                if parsed["type"] == time_exceeded:
                    embedded_ip = parsed.get("embedded_ip")
                    embedded = parsed.get("embedded_payload", b"")
                    if embedded_ip and embedded_ip["dst"] == self.target_ip and len(embedded) >= 4:
                        _sport, dport = struct.unpack("!HH", embedded[0:4])
                        if dport == probe_port:
                            return parsed["ip_src"], rtt_ms, False
                unreachable_type = (pkt.ICMPV6_DEST_UNREACHABLE if self.family == socket.AF_INET6
                                    else pkt.ICMP_DEST_UNREACHABLE)
                unreachable_code = (pkt.ICMPV6_PORT_UNREACHABLE_CODE if self.family == socket.AF_INET6
                                    else pkt.ICMP_PORT_UNREACHABLE_CODE)
                if (parsed["type"] == unreachable_type
                        and parsed["code"] == unreachable_code):
                    embedded_ip = parsed.get("embedded_ip")
                    embedded = parsed.get("embedded_payload", b"")
                    if embedded_ip and embedded_ip["dst"] == self.target_ip and len(embedded) >= 4:
                        _sport, dport = struct.unpack("!HH", embedded[0:4])
                        if dport == probe_port:
                            return parsed["ip_src"], rtt_ms, True

            else:  # tcp: intermediate hops still show up as ICMP time-exceeded
                time_exceeded = (pkt.ICMPV6_TIME_EXCEEDED if self.family == socket.AF_INET6 else pkt.ICMP_TIME_EXCEEDED)
                if parsed["type"] == time_exceeded:
                    embedded_ip = parsed.get("embedded_ip")
                    embedded = parsed.get("embedded_payload", b"")
                    if embedded_ip and embedded_ip["dst"] == self.target_ip and len(embedded) >= 4:
                        sport, _dport = struct.unpack("!HH", embedded[0:4])
                        if sport == self._tcp_src_port:
                            return parsed["ip_src"], rtt_ms, False

        return None, None, False
