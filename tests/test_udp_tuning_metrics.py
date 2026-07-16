import unittest

from nperf.client.engine import _PeerStatsTracker, _udp_trial_metrics


class UdpTuningMetricsTests(unittest.TestCase):
    def test_upload_uses_peer_timestamp_for_goodput(self):
        local_start = {"bytes_sent": 0, "packets_sent": 0, "bytes_received": 0, "packets_received": 0}
        local_end = {"bytes_sent": 84_375_000, "packets_sent": 60_000, "bytes_received": 0, "packets_received": 0}
        peer_start = {"bytes_received": 0, "packets_received": 0, "packets_lost": 0}
        peer_end = {"bytes_received": 142_000_000, "packets_received": 100_000, "packets_lost": 0}

        result = _udp_trial_metrics("up", local_start, local_end, peer_start, peer_end, 2.0, 4.0)

        self.assertAlmostEqual(result["offered_mbps"], 337.5)
        self.assertAlmostEqual(result["goodput_mbps"], 284.0)
        self.assertNotAlmostEqual(result["goodput_mbps"], 568.0)
        self.assertEqual(result["loss_pct"], 0.0)

    def test_peer_tracker_aggregates_timestamped_stream_snapshots(self):
        tracker = _PeerStatsTracker()
        tracker.update([
            {"index": 0, "bytes_sent": 100, "bytes_received": 80, "packets_sent": 2,
             "packets_received": 1, "packets_lost": 1},
            {"index": 1, "bytes_sent": 200, "bytes_received": 160, "packets_sent": 4,
             "packets_received": 2, "packets_lost": 2},
        ], timestamp=10.0)

        snapshot = tracker.snapshot()

        self.assertEqual(snapshot["timestamp"], 10.0)
        self.assertEqual(snapshot["bytes_sent"], 300)
        self.assertEqual(snapshot["bytes_received"], 240)
        self.assertEqual(snapshot["packets_lost"], 3)


if __name__ == "__main__":
    unittest.main()
