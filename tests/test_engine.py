import queue
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from nperf.client import engine
from nperf.common import constants, tcp_info
from nperf.server import throughput_session
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
        control.send.assert_called_once_with("final_stats_ack")

    def test_wait_for_final_stats_rejects_done_without_final_stats(self):
        done_event = threading.Event()
        done_event.set()

        self.assertFalse(engine._wait_for_final_stats(done_event, threading.Event(), 1.0))

    def test_worker_process_failure_reports_nonzero_exit(self):
        process = SimpleNamespace(
            name="worker-1", pid=123, exitcode=7, is_alive=lambda: False)

        failure = engine._worker_process_failure([process], queue.Queue())

        self.assertIn("worker-1 pid=123 exited with code 7", failure)

    def test_worker_process_failure_reports_early_clean_exit(self):
        process = SimpleNamespace(
            name="worker-1", pid=123, exitcode=0, is_alive=lambda: False)

        failure = engine._worker_process_failure(
            [process], queue.Queue(), expect_running=True)

        self.assertIn("exited before the test stopped", failure)

    def test_stop_client_workers_signals_before_joining(self):
        stop_flag = SimpleNamespace(value=0)
        joined_after_stop = []
        process = SimpleNamespace(
            name="worker-1",
            pid=123,
            exitcode=0,
            is_alive=lambda: False,
            join=lambda timeout: joined_after_stop.append((stop_flag.value, timeout)),
        )
        state = {
            "stop_flag": stop_flag,
            "procs": [process],
            "worker_status_q": queue.Queue(),
        }

        self.assertIsNone(engine._stop_client_workers(state))

        self.assertEqual(stop_flag.value, 1)
        self.assertEqual(len(joined_after_stop), 1)
        self.assertEqual(joined_after_stop[0][0], 1)
        self.assertLessEqual(joined_after_stop[0][1], constants.WORKER_DRAIN_TIMEOUT_SEC)

    def test_stream_validation_rejects_inactive_stream(self):
        cfg = engine.ThroughputClientConfig("198.51.100.10", streams=1)
        counters = {0: metrics.StreamCounters(0)}
        counters[0].add_sent(1024)

        with self.assertRaisesRegex(engine.ThroughputError, "stream 0.*received=0"):
            engine._validate_stream_results(cfg, counters)

    def test_server_cleanup_termination_is_not_reported_as_crash(self):
        process = SimpleNamespace(
            name="server-worker-1", pid=456, exitcode=-15, is_alive=lambda: False)
        session = SimpleNamespace(
            mp_enabled=True,
            worker_status_q=queue.Queue(),
            worker_procs=[process],
            cleanup_terminated_pids={456},
        )

        self.assertIsNone(throughput_session._server_worker_failure(session))

    def test_server_worker_failure_reports_nonzero_exit(self):
        process = SimpleNamespace(
            name="server-worker-1", pid=456, exitcode=9, is_alive=lambda: False)
        session = SimpleNamespace(
            mp_enabled=True, worker_status_q=queue.Queue(), worker_procs=[process])

        failure = throughput_session._server_worker_failure(session)

        self.assertIn("server-worker-1 pid=456 exited with code 9", failure)

    def test_peer_stream_error_is_retained_without_rejecting_completed_stream(self):
        cfg = engine.ThroughputClientConfig("198.51.100.10", streams=1)
        counters = {0: metrics.StreamCounters(0)}
        counters[0].add_sent(1024)
        counters[0].add_received(1024)
        counters[0].merge_peer_snapshot({"error": "peer stream closed"})

        engine._validate_stream_results(cfg, counters)

        self.assertEqual(counters[0].snapshot()["error"], "peer stream closed")

    def test_server_final_stats_ack_wait_ignores_queued_probe(self):
        control = Mock()
        control.recv.side_effect = [
            {"type": "probe", "t_send": 1.0},
            {"type": "final_stats_ack"},
        ]

        with patch.object(throughput_session.latency_probe, "handle_probe_message") as handle_probe:
            self.assertTrue(throughput_session._wait_for_final_stats_ack(control, 1.0))

        handle_probe.assert_called_once_with(control, {"type": "probe", "t_send": 1.0})

    def test_dispatch_surfaces_server_worker_error(self):
        control = Mock()
        control.recv.return_value = {"type": "error", "error": "worker exited with code 1"}
        done_event = threading.Event()
        server_errors = []

        engine._client_dispatch_loop(
            control,
            run_mode.RunPhase(),
            metrics.RttTracker(),
            {0: metrics.StreamCounters(0)},
            done_event,
            server_errors=server_errors,
        )

        self.assertTrue(done_event.is_set())
        self.assertEqual(server_errors, ["worker exited with code 1"])

    def test_udp_tuning_rejects_best_effort_above_loss_target(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", acceptable_loss_pct=1.0)

        with self.assertRaisesRegex(engine.ThroughputError, "99.466% loss"):
            engine._validate_udp_tuning_summary(
                cfg, {"recv_mbps": 5.5, "loss_pct": 99.466})

    def test_zero_reported_throughput_is_rejected(self):
        with self.assertRaisesRegex(engine.ThroughputError, "send=250.00 Mbps recv=0.00 Mbps"):
            engine._validate_summary_throughput({"send_mbps": 250.0, "recv_mbps": 0.0})

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
