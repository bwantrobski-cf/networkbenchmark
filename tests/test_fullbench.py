import threading
import unittest
from unittest.mock import patch

from nperf.client import fullbench
from nperf.client.fullbench import FullBenchConfig
from nperf.client.engine import ThroughputClientConfig, ThroughputError, _should_use_multiprocess


class FullbenchTests(unittest.TestCase):
    def test_build_plan_contains_selected_parallel_stream_counts(self):
        config = FullBenchConfig(
            "198.51.100.10",
            transports=["tcp", "udp"],
            directions=["up", "down"],
            streams=[1, 8],
            procs=[2],
            udp_port=21000,
            tcp_port=22000,
        )

        plan = config.build_plan()

        self.assertEqual(len(plan), 8)
        self.assertEqual({spec["streams"] for spec in plan}, {1, 8})
        self.assertTrue(all(spec["procs"] == 2 for spec in plan))
        self.assertEqual(config.udp_port, 21000)
        self.assertEqual(config.tcp_port, 22000)

    def test_multiple_streams_are_concurrent_without_multiple_processes(self):
        config = ThroughputClientConfig("198.51.100.10", streams=8, procs=1)

        self.assertFalse(_should_use_multiprocess(config))
        self.assertEqual(config.streams, 8)
        self.assertEqual(config.procs, 1)

    def test_failed_test_is_exported_as_failure_result(self):
        config = FullBenchConfig(
            "198.51.100.10",
            transports=["tcp"],
            directions=["up"],
            streams=[1],
            procs=[1],
            export_url="https://example.test/nperf-results",
        )
        exported_payloads = []

        def capture_export(_url, payload, **_kwargs):
            exported_payloads.append(payload)
            return {"ok": True, "status": 200, "error": None}

        with patch.object(fullbench, "discover_path_mtu", return_value={
                "success": True,
                "path_mtu": 1400,
                "path_mtu_source": "test",
                "fallback_used": False,
            }), patch.object(fullbench.logging_store, "save_result", return_value=("result.json", "result.log")), \
             patch.object(fullbench, "run_throughput_test", side_effect=ThroughputError("data plane failed")), \
             patch.object(fullbench, "export_result", side_effect=capture_export):
            manifest = fullbench.run_fullbench(
                config, stop_requested=threading.Event(), log_base_dir="/tmp")

        self.assertFalse(manifest["tests"][0]["ok"])
        self.assertEqual(manifest["tests"][0]["error"], "data plane failed")
        self.assertTrue(manifest["tests"][0]["export"]["ok"])
        self.assertEqual(len(exported_payloads), 1)
        self.assertFalse(exported_payloads[0]["ok"])
        self.assertEqual(exported_payloads[0]["error"], "data plane failed")
        self.assertIsNone(exported_payloads[0]["result"])

    def test_exported_tests_pause_between_fullbench_cases(self):
        config = FullBenchConfig(
            "198.51.100.10",
            transports=["tcp"],
            directions=["up"],
            streams=[1, 8],
            procs=[1],
            export_url="https://example.test/nperf-results",
        )

        with patch.object(fullbench, "discover_path_mtu", return_value={
                "success": True,
                "path_mtu": 1400,
                "path_mtu_source": "test",
                "fallback_used": False,
            }), patch.object(fullbench.logging_store, "save_result", return_value=("result.json", "result.log")), \
             patch.object(fullbench, "run_throughput_test", side_effect=ThroughputError("data plane failed")), \
             patch.object(fullbench, "export_result", return_value={"ok": True, "status": 200, "error": None}), \
             patch.object(fullbench.time, "sleep") as sleep:
            fullbench.run_fullbench(config, stop_requested=threading.Event(), log_base_dir="/tmp")

        sleep.assert_called_once_with(5)


if __name__ == "__main__":
    unittest.main()
