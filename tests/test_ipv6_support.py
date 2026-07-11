import socket
import unittest

from networkbenchmark.cli import build_parser
from networkbenchmark.client.engine import ThroughputClientConfig
from networkbenchmark.common import net
from networkbenchmark.diagnostics import packetutils
from networkbenchmark.transport import stream_worker


class IPv6SupportTests(unittest.TestCase):
    def test_family_helpers_and_socket_addresses(self):
        self.assertEqual(net.family_for_ipv6(False), socket.AF_INET)
        self.assertEqual(net.family_for_ipv6(True), socket.AF_INET6)
        self.assertEqual(net.ip_version_label(family=socket.AF_INET6), "IPv6")
        self.assertEqual(net.sockaddr("2001:db8::1", 443, socket.AF_INET6),
                         ("2001:db8::1", 443, 0, 0))
        self.assertEqual(net.bind_targets("0.0.0.0", 443, socket.SOCK_STREAM), [
            (socket.AF_INET, ("0.0.0.0", 443)),
            (socket.AF_INET6, ("::", 443, 0, 0)),
        ])

    def test_ipv6_packet_helpers(self):
        header = packetutils.build_ipv6_header(
            "2001:db8::1", "2001:db8::2", socket.IPPROTO_UDP, 12, 64)
        parsed = packetutils.parse_ip_header(header, family=socket.AF_INET6)
        self.assertEqual(parsed["version"], 6)
        self.assertEqual(parsed["header_len"], 40)
        self.assertEqual(parsed["src"], "2001:db8::1")
        self.assertEqual(parsed["dst"], "2001:db8::2")
        self.assertEqual(parsed["proto"], socket.IPPROTO_UDP)

    def test_dual_stack_listener_is_best_effort(self):
        logs = []
        sockets = net.open_server_sockets(
            "0.0.0.0", 0, socket.SOCK_STREAM, listener=True, log=logs.append, service="test")
        try:
            self.assertTrue(sockets)
            self.assertTrue(all(sock.getsockname()[1] > 0 for sock in sockets))
        finally:
            for sock in sockets:
                sock.close()

    def test_ipv6_packet_sizing(self):
        self.assertEqual(stream_worker.udp_packet_size_for_path_mtu(1500), 1472)
        self.assertEqual(stream_worker.udp_packet_size_for_path_mtu(1500, socket.AF_INET6), 1452)
        self.assertEqual(stream_worker.tcp_mss_for_path_mtu(1500), 1460)
        self.assertEqual(stream_worker.tcp_mss_for_path_mtu(1500, socket.AF_INET6), 1440)

    def test_ipv6_cli_opt_in(self):
        default_config = ThroughputClientConfig("198.51.100.5")
        ipv6_config = ThroughputClientConfig("2001:db8::5", ipv6=True)
        self.assertFalse(default_config.ipv6)
        self.assertEqual(default_config.ip_version, "IPv4")
        self.assertTrue(ipv6_config.ipv6)
        self.assertEqual(ipv6_config.ip_version, "IPv6")

        parser = build_parser()
        for command, argv in (
            ("throughput", ["throughput", "--server-ip", "2001:db8::5", "-v6"]),
            ("fullbench", ["fullbench", "--server-ip", "2001:db8::5", "--ipv6"]),
            ("connectivity", ["connectivity", "--server-ip", "2001:db8::5", "-v6"]),
            ("pmtud", ["pmtud", "--server-ip", "2001:db8::5", "--ipv6"]),
            ("protocols", ["protocols", "--server-ip", "2001:db8::5", "-v6"]),
            ("mtr", ["mtr", "2001:db8::5", "--ipv6"]),
        ):
            args = parser.parse_args(argv)
            self.assertEqual(args.command, command)
            self.assertTrue(args.ipv6)


if __name__ == "__main__":
    unittest.main()
