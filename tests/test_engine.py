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
    def test_provided_path_mtu_skips_discovery(self):
        cfg = engine.ThroughputClientConfig("198.51.100.10", path_mtu=1500)
        control_sock = Mock()
        control_sock.connect.side_effect = OSError("stop after sizing")

        with patch.object(engine.addr, "resolve_host", return_value="198.51.100.10"), \
             patch.object(engine.socket, "socket", return_value=control_sock), \
             patch.object(engine, "_discover_path_mtu") as discover, \
             self.assertRaisesRegex(OSError, "stop after sizing"):
            engine._run_throughput_test_once(cfg)

        discover.assert_not_called()
        self.assertEqual(cfg.pmtud_info["path_mtu_source"], "provided")

    def test_tuned_udp_initial_rate_is_paced_per_stream(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", streams=8, acceptable_loss_pct=1.0)

        self.assertEqual(cfg.initial_rate_per_stream_bps, 3_906_250.0)

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

    def test_udp_trial_loss_uses_consistent_receiver_window(self):
        local_start = {"bytes_sent": 0, "packets_sent": 0}
        local_end = {"bytes_sent": 10_000, "packets_sent": 100}
        peer_start = {"bytes_received": 0, "packets_received": 0, "packets_lost": 0}
        peer_end = {"bytes_received": 9000, "packets_received": 90, "packets_lost": 10}

        result = engine._udp_trial_metrics(
            "up", local_start, local_end, peer_start, peer_end, 1.0, 1.0)

        self.assertEqual(result["loss_pct"], 10.0)

    def test_udp_tuning_tests_lower_rate_after_step_reaches_minimum(self):
        cfg = SimpleNamespace(
            acceptable_loss_pct=1.0,
            max_duration_sec=10.0,
            streams=1,
            direction="up",
        )
        counter = Mock()
        counter.snapshot.return_value = {
            "bytes_sent": 0, "packets_sent": 0, "bytes_received": 0,
            "packets_received": 0, "packets_lost": 0, "retransmits": 0,
        }

        class PeerStats:
            def snapshot(self):
                return {"generation": 1, "timestamp": 1.0}

            def wait_for_update(self, after_generation, timeout_sec):
                return {"generation": after_generation + 1, "timestamp": 2.0}

        stop_requested = threading.Event()
        measurements = iter([
            {"offered_mbps": 250.0, "goodput_mbps": 100.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 10.0},
            {"offered_mbps": 187.5, "goodput_mbps": 110.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 5.0},
            {"offered_mbps": 140.625, "goodput_mbps": 120.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 2.0},
            {"offered_mbps": 105.46875, "goodput_mbps": 100.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.5},
        ])
        calls = []

        def measure(*args):
            result = next(measurements)
            if result["offered_mbps"] == 105.46875:
                stop_requested.set()
            return result

        with patch.object(engine, "_udp_trial_metrics", side_effect=measure), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            tuning = engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), PeerStats(), calls.append,
                None, stop_requested, engine.time.time(), [])

        aggregate_rates = []
        for rate in calls:
            aggregate_rate = rate * 8 / 1e6
            if aggregate_rate >= 1.0 and (not aggregate_rates or aggregate_rate != aggregate_rates[-1]):
                aggregate_rates.append(aggregate_rate)
        self.assertEqual(aggregate_rates[:4], [250.0, 187.5, 140.625, 105.46875])
        self.assertEqual(tuning["best"]["target_mbps"], 105.46875)

    def test_udp_tuning_does_not_select_saturated_trial_as_best(self):
        cfg = SimpleNamespace(
            acceptable_loss_pct=1.0, max_duration_sec=10.0, streams=1, direction="up")
        counter = Mock()
        counter.snapshot.return_value = {
            "bytes_sent": 0, "packets_sent": 0, "bytes_received": 0,
            "packets_received": 0, "packets_lost": 0, "retransmits": 0,
        }

        class PeerStats:
            def snapshot(self):
                return {"generation": 1, "timestamp": 1.0}

            def wait_for_update(self, after_generation, timeout_sec):
                return {"generation": after_generation + 1, "timestamp": 2.0}

        measurements = iter([
            {"offered_mbps": 250.0, "goodput_mbps": 100.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 350.0, "goodput_mbps": 104.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
        ])

        with patch.object(engine, "_udp_trial_metrics", side_effect=lambda *args: next(measurements)), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            tuning = engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), PeerStats(), Mock(),
                None, threading.Event(), engine.time.time(), [])

        self.assertEqual(tuning["best"]["target_mbps"], 250.0)
        self.assertEqual(tuning["trials"][0]["target_mbps"], 350.0)

    def test_udp_retry_wrapper_uses_exponential_backoff(self):
        cfg = engine.ThroughputClientConfig("198.51.100.10", transport="udp")
        result = {"run_id": "ok"}
        with patch.object(engine, "_run_throughput_test_once",
                          side_effect=[engine.ThroughputError("first"), result]), \
             patch.object(engine.time, "sleep") as sleep:
            self.assertEqual(engine.run_throughput_test(cfg), result)

        sleep.assert_called_once_with(constants.UDP_RETRY_BACKOFF_SEC)

    def test_udp_tuning_rejects_short_budget_before_starting(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", acceptable_loss_pct=1.0,
            max_duration_sec=1.0)

        with patch.object(engine, "_run_throughput_test_once") as run_once, \
             self.assertRaisesRegex(engine.ThroughputError, "must be at least"):
            engine.run_throughput_test(cfg)

        run_once.assert_not_called()

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
