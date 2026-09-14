import io
import socket
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch

from nperf import cli
from nperf.common import constants, net
from nperf.diagnostics import packetutils, pmtud


class _FakeSocket:
    def settimeout(self, _timeout):
        pass

    def close(self):
        pass


class PmtudTests(unittest.TestCase):
    def _run_udp_search(self, family, usable_mtu=None,
                        max_mtu=constants.PMTUD_DEFAULT_MAX_MTU):
        payload_sizes = []
        overhead = 48 if family == socket.AF_INET6 else 28

        def probe(_udp_sock, _icmp_sock, _server_ip, _udp_port, payload_size,
                  _timeout_sec, probe_family):
            self.assertEqual(probe_family, family)
            payload_sizes.append(payload_size)
            status = "ok" if usable_mtu is None or payload_size + overhead <= usable_mtu else "too_big"
            return {"status": status, "rtt_ms": None, "next_hop_mtu": None}

        with patch.object(net, "resolve_host", side_effect=lambda host, family: host), \
             patch.object(pmtud.socket, "socket", side_effect=[_FakeSocket(), _FakeSocket()]), \
             patch.object(pmtud, "_set_dont_fragment"), \
             patch.object(pmtud, "_probe_size", side_effect=probe):
            result = pmtud.run_pmtud(
                "198.51.100.10",
                min_mtu=constants.PMTUD_MIN_MTU,
                max_mtu=max_mtu,
                attempts_per_size=1,
                family=family,
            )

        return result, payload_sizes

    def test_icmp_search_steps_down_then_binary_refines(self):
        def probe(size):
            return {"status": "ok" if size <= 1420 else "too_big"}

        result = pmtud._search_packet_size(probe)

        self.assertTrue(result["success"])
        self.assertEqual(result["packet_bytes"], 1420)
        self.assertEqual([entry["size"] for entry in result["trace"][:3]], [1450, 1400, 1425])

    def test_icmp_search_steps_up_then_binary_refines(self):
        def probe(size):
            return {"status": "ok" if size <= 1510 else "too_big"}

        result = pmtud._search_packet_size(probe)

        self.assertTrue(result["success"])
        self.assertEqual(result["packet_bytes"], 1510)
        self.assertEqual([entry["size"] for entry in result["trace"][:4]], [1450, 1500, 1550, 1525])

    def test_dont_fragment_configuration_uses_linux_ipv4_option(self):
        calls = []

        class FakeSocket:
            family = socket.AF_INET

            def setsockopt(self, level, option, value):
                calls.append((level, option, value))

        with patch.object(net.platform, "system", return_value="Linux"), \
             patch.object(net.socket, "IP_MTU_DISCOVER", 10, create=True):
            net.configure_dont_fragment(FakeSocket())

        self.assertEqual(calls, [(socket.IPPROTO_IP, 10, 2)])

    def test_ipv4_default_mtu_bounds_are_converted_to_udp_payload(self):
        result, payload_sizes = self._run_udp_search(socket.AF_INET)

        self.assertEqual(payload_sizes, [40, 1472])
        self.assertEqual([entry["size"] for entry in result["trace"]], [68, 1500])
        self.assertEqual(result["path_mtu"], 1500)
        self.assertEqual(result["path_mtu_payload_bytes"], 1472)

    def test_ipv6_default_mtu_bounds_are_converted_to_udp_payload(self):
        result, payload_sizes = self._run_udp_search(socket.AF_INET6)

        self.assertEqual(payload_sizes, [20, 1452])
        self.assertEqual([entry["size"] for entry in result["trace"]], [68, 1500])
        self.assertEqual(result["path_mtu"], 1500)
        self.assertEqual(result["path_mtu_payload_bytes"], 1452)

    def test_jumbo_ceiling_uses_total_mtu_for_both_ip_versions(self):
        expected_payload = {socket.AF_INET: 8972, socket.AF_INET6: 8952}

        for family, payload_size in expected_payload.items():
            with self.subTest(family=family):
                result, payload_sizes = self._run_udp_search(
                    family, max_mtu=constants.PMTUD_JUMBO_MAX_MTU)

                self.assertEqual(payload_sizes[-1], payload_size)
                self.assertEqual(result["path_mtu"], 9000)
                self.assertEqual(result["probe_max_mtu"], 9000)
                self.assertTrue(result["probe_ceiling_reached"])

    def test_mtu_bounds_are_validated_before_discovery(self):
        invalid_bounds = (
            (67, 1500, "min_mtu"),
            (1501, 1500, "less than or equal"),
            (68, 9001, "9000"),
            (68.0, 1500, "integers"),
        )

        with patch.object(pmtud, "run_pmtud") as run_udp:
            for min_mtu, max_mtu, message in invalid_bounds:
                with self.subTest(min_mtu=min_mtu, max_mtu=max_mtu), \
                     self.assertRaisesRegex(ValueError, message):
                    pmtud.discover_path_mtu(
                        "198.51.100.10", min_mtu=min_mtu, max_mtu=max_mtu)

        run_udp.assert_not_called()

    def test_udp_search_still_discovers_mtu_below_default_ceiling(self):
        result, payload_sizes = self._run_udp_search(socket.AF_INET, usable_mtu=1280)

        self.assertIn(1252, payload_sizes)
        self.assertEqual(result["path_mtu"], 1280)
        self.assertEqual(result["path_mtu_payload_bytes"], 1252)
        self.assertEqual(result["probe_max_mtu"], 1500)
        self.assertFalse(result["probe_ceiling_reached"])

    def test_default_ceiling_metadata_marks_a_bounded_result(self):
        result, _ = self._run_udp_search(socket.AF_INET)

        self.assertEqual(result["probe_max_mtu"], 1500)
        self.assertTrue(result["probe_ceiling_reached"])

    def test_cli_defaults_to_1500_and_jumbo_opt_in_uses_9000(self):
        parser = cli.build_parser()
        for extra_args, expected_max_mtu, expected_jumbo in (
                ([], constants.PMTUD_DEFAULT_MAX_MTU, False),
                (["--jumbo-mtu"], constants.PMTUD_JUMBO_MAX_MTU, True)):
            with self.subTest(extra_args=extra_args):
                args = parser.parse_args([
                    "pmtud", "--server-ip", "198.51.100.10", *extra_args])
                result = {
                    "success": True,
                    "path_mtu": 1280,
                    "probe_max_mtu": expected_max_mtu,
                    "probe_ceiling_reached": False,
                }
                with patch.object(cli.privileges, "probe_capabilities", return_value={"pmtud": True}), \
                     patch.object(cli, "discover_path_mtu", return_value=result) as discover, \
                     patch.object(cli.logging_store, "save_result", return_value=("pmtud.json", "pmtud.log")):
                    cli.cmd_pmtud(args)

                self.assertEqual(discover.call_args.kwargs["min_mtu"], constants.PMTUD_MIN_MTU)
                self.assertEqual(discover.call_args.kwargs["max_mtu"], expected_max_mtu)
                self.assertEqual(result["jumbo_mtu"], expected_jumbo)

    def test_cli_legacy_payload_bounds_are_converted_and_deprecated(self):
        parser = cli.build_parser()
        for family_args, expected_min, expected_max in (
                ([], 604, 2028),
                (["--ipv6"], 624, 2048)):
            with self.subTest(family_args=family_args):
                legacy_args = parser.parse_args([
                    "pmtud", "--server-ip", "198.51.100.10", "--jumbo-mtu",
                    "--min-size", "576", "--max-size", "2000", *family_args,
                ])
                result = {
                    "success": True,
                    "path_mtu": expected_max,
                    "probe_max_mtu": expected_max,
                    "probe_ceiling_reached": True,
                }
                warning = io.StringIO()
                capability = "pmtud_ipv6" if family_args else "pmtud"
                with patch.object(cli.privileges, "probe_capabilities", return_value={capability: True}), \
                     patch.object(cli, "discover_path_mtu", return_value=result) as discover, \
                     patch.object(cli.logging_store, "save_result", return_value=("pmtud.json", "pmtud.log")), \
                     redirect_stderr(warning):
                    cli.cmd_pmtud(legacy_args)

                self.assertEqual(discover.call_args.kwargs["min_mtu"], expected_min)
                self.assertEqual(discover.call_args.kwargs["max_mtu"], expected_max)
                self.assertIn("deprecated UDP-payload-byte", warning.getvalue())

    def test_cli_total_mtu_bounds_remain_unconverted_and_mode_capped(self):
        parser = cli.build_parser()
        total_args = parser.parse_args([
            "pmtud", "--server-ip", "198.51.100.10", "--jumbo-mtu",
            "--min-mtu", "576", "--max-mtu", "2000",
        ])
        result = {
            "success": True,
            "path_mtu": 2000,
            "probe_max_mtu": 2000,
            "probe_ceiling_reached": True,
        }
        with patch.object(cli.privileges, "probe_capabilities", return_value={"pmtud": True}), \
             patch.object(cli, "discover_path_mtu", return_value=result) as discover, \
             patch.object(cli.logging_store, "save_result", return_value=("pmtud.json", "pmtud.log")):
            cli.cmd_pmtud(total_args)

        self.assertEqual(discover.call_args.kwargs["min_mtu"], 576)
        self.assertEqual(discover.call_args.kwargs["max_mtu"], 2000)

        capped_args = parser.parse_args([
            "pmtud", "--server-ip", "198.51.100.10", "--max-mtu", "2000",
        ])
        with patch.object(cli, "discover_path_mtu") as discover, \
             self.assertRaises(SystemExit) as raised:
            cli.cmd_pmtud(capped_args)

        self.assertEqual(raised.exception.code, 2)
        discover.assert_not_called()

        reversed_args = parser.parse_args([
            "pmtud", "--server-ip", "198.51.100.10",
            "--min-mtu", "1400", "--max-mtu", "1300",
        ])
        with patch.object(cli.privileges, "probe_capabilities") as capabilities, \
             self.assertRaises(SystemExit) as raised:
            cli.cmd_pmtud(reversed_args)

        self.assertEqual(raised.exception.code, 2)
        capabilities.assert_not_called()

    def test_cli_packet_length_respects_total_path_mtu_cap(self):
        parser = cli.build_parser()
        cases = (
            ("udp", [], 8972, 8973),
            ("tcp", [], 8960, 8961),
            ("udp", ["--ipv6"], 8952, 8953),
            ("tcp", ["--ipv6"], 8940, 8941),
        )
        for transport, family_args, accepted, rejected in cases:
            with self.subTest(transport=transport, family_args=family_args):
                accepted_args = parser.parse_args([
                    "tput", "--server-ip", "198.51.100.10", f"--{transport}",
                    "--packet-length", str(accepted), *family_args,
                ])
                with patch.object(cli.signal, "signal"), \
                     patch.object(cli, "run_throughput_test", return_value={}) as run_test, \
                     patch.object(cli, "_print_throughput_summary"):
                    cli.cmd_throughput(accepted_args)
                self.assertEqual(run_test.call_args.args[0].path_mtu, 9000)

                rejected_args = parser.parse_args([
                    "tput", "--server-ip", "198.51.100.10", f"--{transport}",
                    "--packet-length", str(rejected), *family_args,
                ])
                with patch.object(cli, "run_throughput_test") as run_test:
                    cli.cmd_throughput(rejected_args)
                run_test.assert_not_called()

    def test_throughput_and_fullbench_jumbo_flags_are_opt_in(self):
        parser = cli.build_parser()
        for command in ("tput", "fullbench"):
            default_args = parser.parse_args([
                command, "--server-ip", "198.51.100.10"])
            jumbo_args = parser.parse_args([
                command, "--server-ip", "198.51.100.10", "--jumbo-mtu"])
            self.assertFalse(default_args.jumbo_mtu)
            self.assertTrue(jumbo_args.jumbo_mtu)

    def test_udp_pmtud_success_is_bidirectionally_validated(self):
        udp_result = {
            "success": True,
            "path_mtu": 1446,
            "path_mtu_payload_bytes": 1418,
            "bidirectional_validated": True,
            "bidirectional_validation": "udp_df_echo",
            "trace": [],
        }
        with patch.object(pmtud, "run_pmtud", return_value=udp_result):
            result = pmtud.discover_path_mtu("198.51.100.10", family=socket.AF_INET)

        self.assertEqual(result["path_mtu"], 1446)
        self.assertTrue(result["bidirectional_validated"])
        self.assertEqual(result["bidirectional_validation"], "udp_df_echo")

    def test_discovery_uses_icmp_after_udp_failure(self):
        udp_result = {"success": False, "path_mtu": None, "trace": []}
        icmp_result = {
            "success": True,
            "path_mtu": 1420,
            "path_mtu_payload_bytes": 1392,
            "trace": [],
        }
        with patch.object(pmtud, "run_pmtud", return_value=udp_result), \
             patch.object(pmtud, "run_icmp_pmtud", return_value=icmp_result):
            result = pmtud.discover_path_mtu("198.51.100.10", family=socket.AF_INET)

        self.assertEqual(result["path_mtu"], 1420)
        self.assertEqual(result["path_mtu_source"], "icmp_pmtud")
        self.assertTrue(result["fallback_used"])

    def test_discovery_propagates_jumbo_ceiling_to_icmp_fallback(self):
        udp_result = {"success": False, "path_mtu": None, "trace": []}
        icmp_result = {
            "success": True,
            "path_mtu": 9000,
            "path_mtu_payload_bytes": 8972,
            "trace": [],
        }
        with patch.object(pmtud, "run_pmtud", return_value=udp_result) as run_udp, \
             patch.object(pmtud, "run_icmp_pmtud", return_value=icmp_result) as run_icmp:
            result = pmtud.discover_path_mtu(
                "198.51.100.10", max_mtu=constants.PMTUD_JUMBO_MAX_MTU,
                family=socket.AF_INET)

        self.assertEqual(run_udp.call_args.kwargs["max_mtu"], 9000)
        self.assertEqual(run_icmp.call_args.kwargs["max_mtu"], 9000)
        self.assertEqual(result["probe_max_mtu"], 9000)
        self.assertTrue(result["probe_ceiling_reached"])

    def test_discovery_uses_1300_after_both_methods_fail(self):
        udp_result = {"success": False, "path_mtu": None, "trace": []}
        icmp_result = {"success": False, "path_mtu": None, "trace": []}
        with patch.object(pmtud, "run_pmtud", return_value=udp_result), \
             patch.object(pmtud, "run_icmp_pmtud", return_value=icmp_result):
            result = pmtud.discover_path_mtu("198.51.100.10", family=socket.AF_INET)

        self.assertEqual(result["path_mtu"], 1300)
        self.assertEqual(result["path_mtu_source"], "fixed_fallback")
        self.assertTrue(result["fallback_used"])
        self.assertEqual(constants.FALLBACK_PATH_MTU, 1300)

    def test_fixed_fallback_stays_within_requested_bounds(self):
        failed = {"success": False, "path_mtu": None, "trace": []}
        with patch.object(pmtud, "run_pmtud",
                          side_effect=lambda *_args, **_kwargs: dict(failed)), \
             patch.object(pmtud, "run_icmp_pmtud",
                          side_effect=lambda *_args, **_kwargs: dict(failed)):
            below = pmtud.discover_path_mtu(
                "198.51.100.10", max_mtu=1000, family=socket.AF_INET)
            above = pmtud.discover_path_mtu(
                "198.51.100.10", min_mtu=1400, max_mtu=1500,
                family=socket.AF_INET)

        self.assertEqual(below["path_mtu"], 1000)
        self.assertEqual(above["path_mtu"], 1400)

    def test_payload_only_icmpv6_response_uses_recvfrom_source(self):
        raw = bytes([packetutils.ICMPV6_ECHO_REPLY, 0, 0, 0, 0, 7, 0, 9])

        parsed = packetutils.parse_icmp_packet(
            raw, family=socket.AF_INET6, source_ip="2001:db8::1")

        self.assertEqual(parsed["ip_src"], "2001:db8::1")
        self.assertEqual(parsed["type"], packetutils.ICMPV6_ECHO_REPLY)
        self.assertEqual(parsed["id"], 7)
        self.assertEqual(parsed["seq"], 9)


if __name__ == "__main__":
    unittest.main()
