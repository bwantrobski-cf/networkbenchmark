import socket
import unittest
from unittest.mock import patch

from nperf.common import constants, net
from nperf.diagnostics import pmtud


class PmtudTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
