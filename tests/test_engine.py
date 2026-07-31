import threading
import unittest
from unittest.mock import Mock, patch

from nperf.client import engine
from nperf.common import constants, tcp_info
from nperf.transport import metrics, run_mode


class EngineTests(unittest.TestCase):
    def test_udp_down_trial_counts_offered_packets_as_loss(self):
        local_start = {"bytes_received": 0, "packets_received": 0, "packets_lost": 0}
        local_end = {"bytes_received": 0, "packets_received": 0, "packets_lost": 0}
        peer_start = {"bytes_sent": 0, "packets_sent": 0}
        peer_end = {"bytes_sent": 1000, "packets_sent": 10}

        result = engine._udp_trial_metrics(
            "down", local_start, local_end, peer_start, peer_end, 1.0, 1.0)

        self.assertEqual(result["offered_pps"], 10.0)
        self.assertEqual(result["goodput_pps"], 0.0)
        self.assertEqual(result["loss_pct"], 100.0)

    def test_udp_retry_wrapper_uses_exponential_backoff(self):
        cfg = engine.ThroughputClientConfig("198.51.100.10", transport="udp")
        result = {"run_id": "ok"}
        with patch.object(engine, "_run_throughput_test_once",
                          side_effect=[engine.ThroughputError("first"), result]), \
             patch.object(engine.time, "sleep") as sleep:
            self.assertEqual(engine.run_throughput_test(cfg), result)

        sleep.assert_called_once_with(constants.UDP_RETRY_BACKOFF_SEC)

    def test_dispatch_marks_final_stats_after_merging_peer_counters(self):
        control = Mock()
        control.recv.side_effect = [
            {"type": "final_stats", "streams": [{"index": 0, "bytes_sent": 1234}]},
            {"type": "done"},
        ]
        phase = run_mode.RunPhase()
        counters = {0: metrics.StreamCounters(0)}
        done_event = threading.Event()
        final_stats_event = threading.Event()

        engine._client_dispatch_loop(
            control,
            phase,
            metrics.RttTracker(),
            counters,
            done_event,
            final_stats_event=final_stats_event,
        )

        self.assertTrue(done_event.is_set())
        self.assertTrue(final_stats_event.is_set())
        self.assertEqual(counters[0].snapshot()["bytes_sent"], 1234)

    def test_wait_for_final_stats_rejects_done_without_final_stats(self):
        done_event = threading.Event()
        done_event.set()

        self.assertFalse(engine._wait_for_final_stats(done_event, threading.Event(), 1.0))

    def test_linux_tcp_retransmit_offset_is_decoded(self):
        raw = bytearray(tcp_info._LINUX_TOTAL_RETRANS_OFFSET + 4)
        raw[tcp_info._LINUX_TOTAL_RETRANS_OFFSET:tcp_info._LINUX_TOTAL_RETRANS_OFFSET + 4] = (7).to_bytes(4, "little")
        sock = type("Sock", (), {
            "getsockopt": lambda self, level, option, size: bytes(raw),
        })()
        with patch.object(tcp_info.platform, "system", return_value="Linux"), \
             patch.object(tcp_info.socket, "TCP_INFO", 11, create=True):
            self.assertEqual(tcp_info.get_tcp_retransmits(sock), 7)

    def test_macos_tcp_retransmit_offset_is_decoded(self):
        raw = bytearray(tcp_info._MACOS_TX_RETRANS_PACKETS_OFFSET + 8)
        raw[tcp_info._MACOS_TX_RETRANS_PACKETS_OFFSET:tcp_info._MACOS_TX_RETRANS_PACKETS_OFFSET + 8] = (9).to_bytes(8, "little")
        sock = type("Sock", (), {
            "getsockopt": lambda self, level, option, size: bytes(raw),
        })()
        with patch.object(tcp_info.platform, "system", return_value="Darwin"):
            self.assertEqual(tcp_info.get_tcp_retransmits(sock), 9)


if __name__ == "__main__":
    unittest.main()
