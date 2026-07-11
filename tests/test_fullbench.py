import unittest

from networkbenchmark.client.fullbench import FullBenchConfig
from networkbenchmark.client.engine import ThroughputClientConfig, _should_use_multiprocess


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


if __name__ == "__main__":
    unittest.main()
