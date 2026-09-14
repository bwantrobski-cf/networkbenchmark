import unittest

from nperf.client.engine import (
    _PeerStatsTracker,
    ThroughputError,
    _exact_udp_trial_metrics,
    _udp_trial_metrics,
)
from nperf.transport import metrics


class UdpTuningMetricsTests(unittest.TestCase):
    def test_upload_uses_each_sides_active_trial_duration(self):
        local_start = {"bytes_sent": 0, "packets_sent": 0, "bytes_received": 0, "packets_received": 0}
        local_end = {"bytes_sent": 84_375_000, "packets_sent": 60_000, "bytes_received": 0, "packets_received": 0}
        peer_start = {"bytes_received": 0, "packets_received": 0, "packets_lost": 0}
        peer_end = {"bytes_received": 168_750_000, "packets_received": 120_000, "packets_lost": 0}

        result = _udp_trial_metrics("up", local_start, local_end, peer_start, peer_end, 2.0, 4.0)

        self.assertAlmostEqual(result["offered_mbps"], 337.5)
        self.assertAlmostEqual(result["goodput_mbps"], 337.5)
        self.assertEqual(result["loss_pct"], 0.0)

    def test_loss_uses_receiver_window_when_peer_telemetry_is_wider(self):
        local_start = {"bytes_sent": 0, "packets_sent": 0}
        local_end = {"bytes_sent": 100_000, "packets_sent": 100}
        peer_start = {"bytes_received": 0, "packets_received": 0, "packets_lost": 0}
        peer_end = {"bytes_received": 120_000, "packets_received": 120, "packets_lost": 10}

        result = _udp_trial_metrics(
            "up", local_start, local_end, peer_start, peer_end, 5.0, 6.0)

        self.assertAlmostEqual(result["loss_pct"], 10 / 130 * 100)

    def test_loss_includes_trailing_packets_missing_from_receiver_sequence_gaps(self):
        local_start = {"bytes_sent": 0, "packets_sent": 0}
        local_end = {"bytes_sent": 100_000, "packets_sent": 100}
        peer_start = {"bytes_received": 0, "packets_received": 0, "packets_lost": 0}
        peer_end = {"bytes_received": 1_000, "packets_received": 1, "packets_lost": 0}

        result = _udp_trial_metrics(
            "up", local_start, local_end, peer_start, peer_end, 5.0, 5.0)

        self.assertEqual(result["loss_pct"], 99.0)

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

    def test_exact_upload_aggregates_same_trial_and_excludes_duplicates(self):
        local_streams = [
            {"index": 0, "trial_sent": {
                "trial_id": 7, "bytes_sent": 1000, "packets_sent": 10,
                "first_seq": 10, "last_seq": 19,
            }},
            {"index": 1, "trial_sent": {
                "trial_id": 7, "bytes_sent": 2000, "packets_sent": 20,
                "first_seq": 100, "last_seq": 119,
            }},
        ]
        peer = _PeerStatsTracker()
        peer.update([
            {"index": 0, "trial_received": {
                "trial_id": 7, "bytes_received": 800,
                "packets_received": 8, "unique_packets": 8,
                "duplicates": 2, "out_of_order": 1, "packets_lost": 1,
                "first_seq": 10, "last_seq": 18,
            }},
            {"index": 1, "trial_received": {
                "trial_id": 7, "bytes_received": 1900,
                "packets_received": 19, "unique_packets": 19,
                "duplicates": 0, "out_of_order": 2, "packets_lost": 1,
                "first_seq": 100, "last_seq": 119,
            }},
        ])

        result = _exact_udp_trial_metrics(
            "up", 7, local_streams, peer, 2.0, peer_after_generation=0)

        self.assertEqual(result["sent_packets"], 30)
        self.assertEqual(result["unique_received_packets"], 27)
        self.assertEqual(result["duplicates"], 2)
        self.assertEqual(result["out_of_order"], 3)
        self.assertEqual(result["missing_packets"], 3)
        self.assertEqual(result["loss_pct"], 10.0)
        self.assertEqual(result["sequence_diagnostics"]["sender_first_seq"], 10)
        self.assertEqual(result["sequence_diagnostics"]["receiver_last_seq"], 119)

    def test_exact_download_uses_peer_sender_records(self):
        peer = _PeerStatsTracker()
        peer.update([{"index": 0, "trial_sent": {
            "trial_id": 9, "bytes_sent": 1000, "packets_sent": 10,
            "first_seq": 50, "last_seq": 59,
        }}])
        local_streams = [{"index": 0, "trial_received": {
            "trial_id": 9, "bytes_received": 800,
            "packets_received": 8, "unique_packets": 8,
            "duplicates": 3, "out_of_order": 1, "packets_lost": 2,
            "first_seq": 50, "last_seq": 59,
        }}]

        result = _exact_udp_trial_metrics(
            "down", 9, local_streams, peer, 1.0, peer_after_generation=0)

        self.assertTrue(result["peer_trial_data"])
        self.assertEqual(result["sent_packets"], 10)
        self.assertEqual(result["received_packets"], 8)
        self.assertEqual(result["duplicates"], 3)
        self.assertEqual(result["loss_pct"], 20.0)

    def test_exact_total_blackout_is_one_hundred_percent_loss(self):
        peer = _PeerStatsTracker()
        peer.update([{"index": 0, "trial_received": {
            "trial_id": 0, "bytes_received": 100, "unique_packets": 1,
        }}])
        local_streams = [{"index": 0, "trial_sent": {
            "trial_id": 11, "bytes_sent": 5000, "packets_sent": 50,
            "first_seq": 1000, "last_seq": 1049,
        }}]

        result = _exact_udp_trial_metrics(
            "up", 11, local_streams, peer, 5.0, peer_after_generation=0)

        self.assertFalse(result["peer_trial_data"])
        self.assertEqual(result["received_packets"], 0)
        self.assertEqual(result["missing_packets"], 50)
        self.assertEqual(result["packets_lost"], 50)
        self.assertEqual(result["loss_pct"], 100.0)

    def test_stream_packet_loss_reconciles_exact_trial_counts(self):
        counter = metrics.StreamCounters(0)
        counter.add_sent_batch(
            1000, 10, trial_id=21, first_seq=100, last_seq=109)
        counter.merge_peer_snapshot({
            "bytes_received": 800,
            "packets_received": 8,
            "packets_lost": 0,
            "trial_received": {
                "trial_id": 21,
                "bytes_received": 800,
                "packets_received": 8,
                "unique_packets": 8,
                "first_seq": 102,
                "last_seq": 109,
                "packets_lost": 0,
                "duplicates": 0,
                "out_of_order": 0,
            },
        })

        snapshot = counter.snapshot()

        self.assertEqual(snapshot["packets_lost"], 2)
        self.assertEqual(snapshot["trial_received"]["packets_lost"], 2)
        self.assertEqual(snapshot["trial_received"]["sequence_gaps"], 0)

    def test_stream_packet_loss_counts_complete_blackout(self):
        counter = metrics.StreamCounters(0)
        counter.add_sent_batch(
            5000, 50, trial_id=22, first_seq=1000, last_seq=1049)
        counter.merge_peer_snapshot({
            "bytes_received": 0,
            "packets_received": 0,
            "trial_received": {
                "trial_id": 0,
                "bytes_received": 0,
                "packets_received": 0,
                "unique_packets": 0,
                "packets_lost": 0,
            },
        })

        self.assertEqual(counter.snapshot()["packets_lost"], 50)

    def test_peer_tracker_retains_prior_trial_and_final_host_sample(self):
        tracker = _PeerStatsTracker()
        tracker.update([{"index": 0, "trial_sent": {
            "trial_id": 3, "bytes_sent": 300, "packets_sent": 3,
        }}], host_sample={"available": True, "marker": "first"})
        tracker.update([{"index": 0, "trial_sent": {
            "trial_id": 4, "bytes_sent": 400, "packets_sent": 4,
        }}], host_sample={"available": True, "marker": "final"}, final=True)

        self.assertEqual(tracker.aggregate_trial(3, "sent")["packets_sent"], 3)
        self.assertEqual(tracker.aggregate_trial(4, "sent")["packets_sent"], 4)
        self.assertEqual(
            tracker.final_boundary()["host_sample"]["marker"], "final")

    def test_peer_tracker_bounds_periodic_host_history(self):
        tracker = _PeerStatsTracker()
        for generation in range(30):
            tracker.update(
                [{"index": 0, "bytes_sent": generation}],
                host_sample={"marker": generation},
                final=generation == 29,
            )

        self.assertLessEqual(len(tracker._boundaries), 8)
        self.assertEqual(
            tracker.first_host_boundary()["host_sample"]["marker"], 0)
        self.assertEqual(
            tracker.latest_host_boundary()["host_sample"]["marker"], 29)

    def test_exact_trial_requires_every_sender_stream(self):
        local_streams = [{"index": 0, "trial_sent": {
            "trial_id": 15, "bytes_sent": 1000, "packets_sent": 10,
        }}]

        with self.assertRaisesRegex(ThroughputError, "missing_streams=\\[1\\]"):
            _exact_udp_trial_metrics(
                "up", 15, local_streams, _PeerStatsTracker(), 5.0,
                expected_stream_count=2)

    def test_exact_trial_uses_trial_boundary_interval(self):
        local_streams = [{"index": 0, "trial_sent": {
            "trial_id": 16,
            "bytes_sent": 625_000_000,
            "packets_sent": 500_000,
            "first_sent_ns": 1_000_000_000,
            "last_sent_ns": 6_000_000_000,
        }}]
        peer = _PeerStatsTracker()
        peer.update([{"index": 0, "trial_received": {
            "trial_id": 16,
            "bytes_received": 625_000_000,
            "unique_packets": 500_000,
        }}])

        result = _exact_udp_trial_metrics(
            "up", 16, local_streams, peer, 5.0,
            peer_after_generation=0, expected_stream_count=1)

        self.assertEqual(result["measurement_clock"], "trial_boundary_monotonic")
        self.assertEqual(result["measurement_sec"], 5.0)
        self.assertEqual(result["offered_mbps"], 1000.0)

    def test_exact_trial_accepts_sparse_packets_across_full_active_window(self):
        local_streams = [{"index": 0, "trial_sent": {
            "trial_id": 18,
            "bytes_sent": 1200,
            "packets_sent": 1,
            "first_seq": 9,
            "last_seq": 9,
            "first_sent_ns": 2_000_000_000,
            "last_sent_ns": 2_000_000_000,
        }}]
        peer = _PeerStatsTracker()
        peer.update([{"index": 0, "trial_received": {
            "trial_id": 18,
            "bytes_received": 1200,
            "unique_packets": 1,
            "first_seq": 9,
            "last_seq": 9,
        }}])

        result = _exact_udp_trial_metrics(
            "up", 18, local_streams, peer, 5.0,
            peer_after_generation=0, expected_stream_count=1)

        self.assertEqual(result["sent_packets"], 1)
        self.assertEqual(result["received_packets"], 1)
        self.assertEqual(result["measurement_sec"], 5.0)
        self.assertAlmostEqual(result["offered_pps"], 0.2)

    def test_exact_trial_rejects_short_activation_window(self):
        local_streams = [{"index": 0, "trial_sent": {
            "trial_id": 17,
            "bytes_sent": 125_000_000,
            "packets_sent": 100_000,
            "first_sent_ns": 1_000_000_000,
            "last_sent_ns": 2_000_000_000,
        }}]

        with self.assertRaisesRegex(ThroughputError, "shorter"):
            _exact_udp_trial_metrics(
                "up", 17, local_streams, _PeerStatsTracker(), 1.0,
                expected_stream_count=1)


if __name__ == "__main__":
    unittest.main()
