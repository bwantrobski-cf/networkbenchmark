import socket
import unittest
from unittest.mock import patch

from networkbenchmark.diagnostics import connectivity, protocols


class ProtocolChecksTests(unittest.TestCase):
    def test_macos_ipv4_ping_does_not_use_unsupported_family_flag(self):
        completed = type("Completed", (), {"stdout": "", "stderr": "", "returncode": 1})()
        with patch.object(connectivity.platform, "system", return_value="Darwin"), \
             patch.object(connectivity.subprocess, "run", return_value=completed) as run:
            connectivity.icmp_ping("198.51.100.10", family=socket.AF_INET)

        command = run.call_args.args[0]
        self.assertEqual(command[:3], ["ping", "-c", "4"])
        self.assertNotIn("-4", command)

    def test_protocol_matrix_reports_probe_exception(self):
        with patch.object(protocols.privileges, "probe_capabilities", return_value={}), \
             patch.object(protocols.connectivity, "udp_check", side_effect=OSError("socket unavailable")):
            result = protocols.run_all("198.51.100.10", 10000, 11000)

        self.assertFalse(result["udp"]["success"])
        self.assertEqual(result["udp"]["error"], "socket unavailable")
        self.assertIn("tcp", result)
        self.assertIn("esp_native", result)
        self.assertIn("gre", result)

    def test_safe_check_converts_unexpected_probe_failure(self):
        result = protocols._safe_check("test", lambda: (_ for _ in ()).throw(RuntimeError("boom")), socket.AF_INET)

        self.assertFalse(result["success"])
        self.assertEqual(result["protocol"], "test")
        self.assertEqual(result["error"], "boom")


if __name__ == "__main__":
    unittest.main()
