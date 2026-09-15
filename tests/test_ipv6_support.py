import io
import socket
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from nperf import cli
from nperf.cli import build_parser
from nperf.client.engine import ThroughputClientConfig
from nperf.common import net
from nperf.diagnostics import packetutils
from nperf.server import engine as server_engine
from nperf.transport import stream_worker


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

    def test_default_server_bind_opens_ipv4_and_ipv6_listeners(self):
        logs = []
        sockets = net.open_server_sockets(
            "0.0.0.0", 0, socket.SOCK_STREAM, listener=True, log=logs.append, service="test")
        try:
            self.assertIn(socket.AF_INET, {sock.family for sock in sockets})
            if socket.has_ipv6:
                self.assertIn(socket.AF_INET6, {sock.family for sock in sockets}, "\n".join(logs))
            self.assertTrue(all(sock.getsockname()[1] > 0 for sock in sockets))
            for sock in sockets:
                if sock.family == socket.AF_INET6:
                    self.assertEqual(
                        sock.getsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY), 1)
        finally:
            for sock in sockets:
                sock.close()

    def test_default_server_listeners_are_reachable_over_both_families(self):
        for socktype, listener in ((socket.SOCK_STREAM, True), (socket.SOCK_DGRAM, False)):
            sockets = net.open_server_sockets("0.0.0.0", 0, socktype, listener=listener)
            try:
                for server_sock in sockets:
                    server_sock.settimeout(1.0)
                    loopback = "::1" if server_sock.family == socket.AF_INET6 else "127.0.0.1"
                    client_sock = socket.socket(server_sock.family, socktype)
                    try:
                        client_sock.settimeout(1.0)
                        destination = net.sockaddr(
                            loopback, server_sock.getsockname()[1], server_sock.family)
                        if socktype == socket.SOCK_STREAM:
                            client_sock.connect(destination)
                            accepted, _ = server_sock.accept()
                            accepted.close()
                        else:
                            client_sock.sendto(b"ipv6-bind-test", destination)
                            self.assertEqual(server_sock.recvfrom(64)[0], b"ipv6-bind-test")
                    finally:
                        client_sock.close()
            finally:
                for server_sock in sockets:
                    server_sock.close()

    def test_server_cli_describes_default_bind_as_dual_stack(self):
        args = build_parser().parse_args(["server"])
        output = io.StringIO()
        with patch.object(server_engine, "run_server") as run_server, redirect_stdout(output):
            cli.cmd_server(args)

        self.assertIn("all IPv4 and IPv6 interfaces", output.getvalue())
        config = run_server.call_args.args[0]
        self.assertEqual(config.bind, "0.0.0.0")

    def test_ipv6_packet_sizing(self):
        self.assertEqual(stream_worker.udp_packet_size_for_path_mtu(1500), 1472)
        self.assertEqual(stream_worker.udp_packet_size_for_path_mtu(1500, socket.AF_INET6), 1452)
        self.assertEqual(stream_worker.tcp_mss_for_path_mtu(1500), 1460)
        self.assertEqual(stream_worker.tcp_mss_for_path_mtu(1500, socket.AF_INET6), 1440)
        self.assertEqual(stream_worker.path_mtu_for_packet_size(1472, "udp"), 1500)
        self.assertEqual(stream_worker.path_mtu_for_packet_size(1452, "udp", socket.AF_INET6), 1500)
        self.assertEqual(stream_worker.path_mtu_for_packet_size(1460, "tcp"), 1500)
        self.assertEqual(stream_worker.path_mtu_for_packet_size(1440, "tcp", socket.AF_INET6), 1500)

    def test_tput_manual_packet_length_sets_equivalent_path_mtu(self):
        parser = build_parser()
        cases = (
            (["tput", "--server-ip", "198.51.100.5", "--udp", "--packet-length", "1472"], 1500),
            (["tput", "--server-ip", "198.51.100.5", "--tcp", "--packet-length", "1460"], 1500),
            (["tput", "--server-ip", "2001:db8::5", "-v6", "--udp", "--packet-length", "1452"], 1500),
            (["tput", "--server-ip", "2001:db8::5", "-v6", "--tcp", "--packet-length", "1440"], 1500),
        )
        for argv, expected_path_mtu in cases:
            with self.subTest(argv=argv):
                args = parser.parse_args(argv)
                with patch.object(cli.signal, "signal"), \
                     patch.object(cli, "run_throughput_test", return_value={}) as run_test, \
                     patch.object(cli, "_print_throughput_summary"):
                    cli.cmd_throughput(args)
                cfg = run_test.call_args.args[0]
                self.assertEqual(cfg.path_mtu, expected_path_mtu)
                self.assertIsNone(cfg.pmtud_info)

    def test_tput_rejects_invalid_manual_packet_lengths(self):
        parser = build_parser()
        cases = (
            ["tput", "--server-ip", "198.51.100.5", "--udp", "--packet-length", "16"],
            ["tput", "--server-ip", "198.51.100.5", "--tcp", "--packet-length", "63"],
            ["tput", "--server-ip", "2001:db8::5", "-v6", "--udp", "--packet-length", "65488"],
            ["tput", "--server-ip", "2001:db8::5", "-v6", "--tcp", "--packet-length", "65476"],
        )
        for argv in cases:
            with self.subTest(argv=argv):
                args = parser.parse_args(argv)
                with patch.object(cli, "run_throughput_test") as run_test:
                    cli.cmd_throughput(args)
                run_test.assert_not_called()

    def test_tput_cli_converts_unexpected_failures_without_tracebacks(self):
        args = build_parser().parse_args([
            "tput", "--server-ip", "2001:db8::5", "--ipv6", "--tcp",
            "--packet-length", "1440",
        ])
        output = io.StringIO()
        with patch.object(cli.signal, "signal"), \
             patch.object(cli, "run_throughput_test", side_effect=RuntimeError("unexpected failure")), \
             redirect_stdout(output), self.assertRaises(SystemExit) as raised:
            cli.cmd_throughput(args)

        self.assertEqual(raised.exception.code, 1)
        self.assertIn("ERROR: throughput test failed unexpectedly", output.getvalue())
        self.assertIn("unexpected failure", output.getvalue())
        self.assertNotIn("Traceback", output.getvalue())

    def test_ipv6_cli_opt_in(self):
        default_config = ThroughputClientConfig("198.51.100.5")
        ipv6_config = ThroughputClientConfig("2001:db8::5", ipv6=True)
        self.assertFalse(default_config.ipv6)
        self.assertEqual(default_config.ip_version, "IPv4")
        self.assertTrue(ipv6_config.ipv6)
        self.assertEqual(ipv6_config.ip_version, "IPv6")

        parser = build_parser()
        self.assertEqual(parser.prog, "nperf")
        fullbench_args = parser.parse_args(["fullbench", "--server-ip", "198.51.100.5"])
        self.assertEqual(fullbench_args.test_type, "nperf")
        for command, argv in (
            ("tput", ["tput", "--server-ip", "2001:db8::5", "-v6"]),
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
