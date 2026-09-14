import queue
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from nperf.client import engine
from nperf.common import constants, tcp_info
from nperf.server import throughput_session
from nperf.transport import metrics, run_mode, shared_counters


class _FreshPeerStats:
    def __init__(self):
        self.current_generation = 0

    def generation(self):
        return self.current_generation

    def snapshot(self):
        return {"generation": self.current_generation, "timestamp": 1.0}

    def wait_for_update(self, after_generation, timeout_sec):
        self.current_generation = max(self.current_generation, after_generation) + 1
        return {"generation": self.current_generation, "timestamp": 2.0}

    def latest_boundary(self):
        return {
            "generation": self.current_generation,
            "timestamp": 2.0,
            "streams": [],
            "host_sample": None,
        }


class EngineTests(unittest.TestCase):
    def test_config_rejects_nonfinite_or_excessive_durations(self):
        invalid = (0, -1, float("nan"), float("inf"),
                   constants.MAX_RUN_TIME_FAILSAFE_SEC + 1)
        for value in invalid:
            with self.subTest(duration=value), \
                 self.assertRaisesRegex(ValueError, "duration.*finite"):
                engine.ThroughputClientConfig(
                    "198.51.100.10", duration_sec=value)
            with self.subTest(max_duration=value), \
                 self.assertRaisesRegex(ValueError, "maximum duration.*finite"):
                engine.ThroughputClientConfig(
                    "198.51.100.10", max_duration_sec=value)

    def test_mutated_path_mtu_revalidates_converted_packet_length(self):
        cfg = engine.ThroughputClientConfig("198.51.100.10", transport="udp")
        cfg.path_mtu = engine.stream_worker.path_mtu_for_packet_size(
            constants.PMTUD_JUMBO_MAX_MTU - 28, "udp")

        self.assertEqual(cfg.path_mtu, constants.PMTUD_JUMBO_MAX_MTU)
        with self.assertRaisesRegex(ValueError, "path_mtu"):
            cfg.path_mtu = engine.stream_worker.path_mtu_for_packet_size(
                constants.PMTUD_JUMBO_MAX_MTU, "udp")

    def test_udp_tuning_step_budget_covers_warmup_measurement_and_peer_stats(self):
        self.assertEqual(engine._udp_tuning_measurement_budget_sec(), 10.0)

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
        self.assertIsNone(cfg.pmtud_info["probe_max_mtu"])
        self.assertFalse(cfg.pmtud_info["probe_ceiling_reached"])

    def test_udp_tuning_rejects_rate_below_one_packet_per_stream_window(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", streams=64,
            acceptable_loss_pct=1.0, max_duration_sec=10.0,
            udp_start_mbps=0.01, path_mtu=1500)

        with patch.object(
                engine.addr, "resolve_host", return_value="198.51.100.10"), \
             patch.object(engine.socket, "socket") as socket_factory, \
             self.assertRaisesRegex(
                 engine.ThroughputError,
                 r"0\.01 Mbps.*64 streams.*1472-byte datagrams.*"
                 r"5-second trial window.*minimum aggregate rate is "
                 r"0\.150733 Mbps"):
            engine._run_throughput_test_once(cfg)

        socket_factory.assert_not_called()

    def test_udp_tuning_keeps_sampled_low_rate_valid(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", streams=1,
            acceptable_loss_pct=1.0, max_duration_sec=10.0,
            udp_start_mbps=0.01, path_mtu=1500)
        control_sock = Mock()
        control_sock.connect.side_effect = OSError("validated")

        with patch.object(
                engine.addr, "resolve_host", return_value="198.51.100.10"), \
             patch.object(engine.socket, "socket", return_value=control_sock), \
             self.assertRaisesRegex(OSError, "validated"):
            engine._run_throughput_test_once(cfg)

    def test_discovery_uses_selected_total_mtu_ceiling(self):
        for jumbo_mtu, expected_max_mtu in (
                (False, constants.PMTUD_DEFAULT_MAX_MTU),
                (True, constants.PMTUD_JUMBO_MAX_MTU)):
            with self.subTest(jumbo_mtu=jumbo_mtu):
                cfg = engine.ThroughputClientConfig(
                    "198.51.100.10", jumbo_mtu=jumbo_mtu)
                result = {
                    "success": True,
                    "path_mtu": 1280,
                    "path_mtu_source": "udp_pmtud",
                    "probe_max_mtu": expected_max_mtu,
                    "probe_ceiling_reached": False,
                }
                with patch.object(engine, "discover_path_mtu", return_value=result) as discover, \
                     patch.object(engine.logging_store, "save_result", return_value=("pmtud.json", "pmtud.log")):
                    path_mtu, info = engine._discover_path_mtu(
                        cfg, "198.51.100.10")

                self.assertEqual(discover.call_args.kwargs["max_mtu"], expected_max_mtu)
                self.assertEqual(path_mtu, 1280)
                self.assertEqual(info["probe_max_mtu"], expected_max_mtu)
                self.assertFalse(info["probe_ceiling_reached"])
                self.assertEqual(info["jumbo_mtu"], jumbo_mtu)

    def test_summary_promotes_pmtud_ceiling_metadata(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10",
            path_mtu=9000,
            jumbo_mtu=True,
            pmtud_info={
                "path_mtu_source": "udp_pmtud",
                "probe_max_mtu": 9000,
                "probe_ceiling_reached": True,
            },
        )
        rtt_tracker = Mock()
        rtt_tracker.unloaded_rtt_ms.return_value = None
        rtt_tracker.loaded_stats.return_value = {}

        summary = engine._build_summary(
            cfg, "run-id", {}, rtt_tracker, SimpleNamespace(records=[]), [])

        self.assertTrue(summary["jumbo_mtu"])
        self.assertEqual(summary["probe_max_mtu"], 9000)
        self.assertTrue(summary["probe_ceiling_reached"])

    def test_cancelled_fixed_summary_uses_actual_elapsed_duration(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", duration_sec=10.0, transport="tcp")
        counter = metrics.StreamCounters(0)
        counter.add_sent(2_000_000)
        counter.add_received(1_000_000)
        rtt_tracker = Mock()
        rtt_tracker.unloaded_rtt_ms.return_value = None
        rtt_tracker.loaded_stats.return_value = {}

        cancelled = engine._build_summary(
            cfg, "cancelled", {0: counter}, rtt_tracker,
            SimpleNamespace(records=[]), [], elapsed_sec=2.0,
            status="cancelled")
        complete = engine._build_summary(
            cfg, "complete", {0: counter}, rtt_tracker,
            SimpleNamespace(records=[]), [], elapsed_sec=2.0,
            status="complete")

        self.assertEqual(cancelled["status"], "cancelled")
        self.assertTrue(cancelled["aborted"])
        self.assertFalse(cancelled["complete"])
        self.assertEqual(cancelled["duration_sec"], 2.0)
        self.assertEqual(cancelled["configured_duration_sec"], 10.0)
        self.assertEqual(cancelled["actual_duration_sec"], 2.0)
        self.assertEqual(cancelled["send_mbps"], 8.0)
        self.assertEqual(complete["duration_sec"], 10.0)
        self.assertEqual(complete["send_mbps"], 1.6)

    def test_shared_summary_loss_agrees_with_stream_and_total_counts(self):
        shared = shared_counters.SharedCounters(2)
        counters = {index: shared.view(index) for index in range(2)}
        for index, counter in counters.items():
            counter.add_sent_batch(
                10_000, 10, trial_id=3,
                first_seq=index * 100, last_seq=index * 100 + 9)
            received = 8 if index == 0 else 7
            counter.merge_peer_snapshot({
                "bytes_received": received * 1000,
                "packets_received": received,
                "trial_received": {
                    "trial_id": 3,
                    "bytes_received": received * 1000,
                    "packets_received": received,
                    "unique_packets": received,
                    "packets_lost": 0,
                },
            })
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", duration_sec=1.0, transport="udp")
        rtt_tracker = Mock()
        rtt_tracker.unloaded_rtt_ms.return_value = None
        rtt_tracker.loaded_stats.return_value = {}

        summary = engine._build_summary(
            cfg, "run-id", counters, rtt_tracker,
            SimpleNamespace(records=[]), [])

        self.assertEqual(summary["totals"]["packets_sent"], 20)
        self.assertEqual(summary["totals"]["packets_received"], 15)
        self.assertEqual(summary["totals"]["packets_lost"], 5)
        self.assertEqual(
            sum(stream["packets_lost"] for stream in summary["per_stream"]), 5)
        self.assertEqual(summary["loss_pct"], 25.0)

    def test_tail_receive_after_final_peer_merge_preserves_exact_loss(self):
        shared = shared_counters.SharedCounters(1)
        implementations = (
            ("threaded", metrics.StreamCounters(0)),
            ("shared", shared.view(0)),
        )
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", duration_sec=1.0,
            transport="udp", path_mtu=1500)

        for name, counter in implementations:
            with self.subTest(implementation=name):
                counter.add_received_batch(
                    800, 8, trial_id=7, first_seq=0, last_seq=7,
                    missing_count=0, trial_missing_count=0)
                counter.merge_peer_snapshot({
                    "bytes_sent": 1000,
                    "packets_sent": 10,
                    "trial_sent": {
                        "trial_id": 7,
                        "bytes_sent": 1000,
                        "packets_sent": 10,
                        "first_seq": 0,
                        "last_seq": 9,
                    },
                })
                self.assertEqual(counter.snapshot()["packets_lost"], 2)

                counter.add_received_batch(
                    100, 1, trial_id=7, first_seq=8, last_seq=8,
                    missing_count=0, trial_missing_count=0)
                snapshot = counter.snapshot()
                self.assertEqual(snapshot["packets_sent"], 10)
                self.assertEqual(snapshot["packets_received"], 9)
                self.assertEqual(snapshot["packets_lost"], 1)
                self.assertEqual(
                    snapshot["trial_received"]["packets_lost"], 1)

                rtt_tracker = Mock()
                rtt_tracker.unloaded_rtt_ms.return_value = None
                rtt_tracker.loaded_stats.return_value = {}
                summary = engine._build_summary(
                    cfg, f"{name}-run", {0: counter}, rtt_tracker,
                    SimpleNamespace(records=[]), [])
                self.assertEqual(summary["totals"]["packets_sent"], 10)
                self.assertEqual(summary["totals"]["packets_received"], 9)
                self.assertEqual(summary["totals"]["packets_lost"], 1)
                self.assertEqual(summary["per_stream"][0]["packets_lost"], 1)
                self.assertEqual(summary["loss_pct"], 10.0)

    def test_tuned_udp_initial_rate_is_paced_per_stream(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", streams=8, acceptable_loss_pct=1.0)

        self.assertEqual(cfg.initial_rate_per_stream_bps, 3_906_250.0)
        self.assertEqual(cfg.sender_duration_sec,
                         constants.DEFAULT_MAX_DURATION_SEC + constants.UDP_TUNE_CONTROL_GRACE_SEC)

    def test_tuned_udp_custom_initial_rate_is_paced_per_stream(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", streams=4, acceptable_loss_pct=1.0,
            udp_start_mbps=1000.0)

        self.assertEqual(cfg.initial_rate_per_stream_bps, 31_250_000.0)

    def test_tuned_udp_rejects_nonpositive_initial_rate(self):
        with self.assertRaisesRegex(ValueError, "start rate"):
            engine.ThroughputClientConfig(
                "198.51.100.10", transport="udp", acceptable_loss_pct=1.0,
                udp_start_mbps=0)

    def test_tuned_udp_rejects_nonfinite_inputs(self):
        for value in (float("nan"), float("inf")):
            with self.subTest(start_mbps=value), \
                 self.assertRaisesRegex(ValueError, "no more than"):
                engine.ThroughputClientConfig(
                    "198.51.100.10", transport="udp", acceptable_loss_pct=1.0,
                    udp_start_mbps=value)
            with self.subTest(loss=value), \
                 self.assertRaisesRegex(ValueError, "finite percentage"):
                engine.ThroughputClientConfig(
                    "198.51.100.10", transport="udp", acceptable_loss_pct=value)
        with self.assertRaisesRegex(ValueError, "no more than"):
            engine.ThroughputClientConfig(
                "198.51.100.10", transport="udp", acceptable_loss_pct=1.0,
                udp_start_mbps=1e308)

    def test_udp_tuning_reports_when_passing_probe_reaches_rate_cap(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", acceptable_loss_pct=1.0,
            max_duration_sec=10.0, udp_start_mbps=constants.UDP_TUNE_MAX_RATE_MBPS)
        counter = Mock()
        counter.snapshot.return_value = {
            "bytes_sent": 0, "packets_sent": 0, "bytes_received": 0,
            "packets_received": 0, "packets_lost": 0, "retransmits": 0,
        }
        measurement = {
            "offered_mbps": constants.UDP_TUNE_MAX_RATE_MBPS,
            "goodput_mbps": constants.UDP_TUNE_MAX_RATE_MBPS,
            "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0,
        }

        with patch.object(engine, "_exact_udp_trial_metrics", return_value=measurement), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            tuning = engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), _FreshPeerStats(),
                Mock(), None, threading.Event(), engine.time.time(), [])

        self.assertEqual(tuning["termination_reason"], "rate_cap_reached")
        self.assertFalse(tuning["search_complete"])
        self.assertEqual(tuning["best"]["target_mbps"], constants.UDP_TUNE_MAX_RATE_MBPS)

    def test_udp_tuning_holds_one_rate_through_warmup_and_measurement(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", acceptable_loss_pct=1.0,
            max_duration_sec=10.0, udp_start_mbps=250.0)
        counter = Mock()
        counter.snapshot.return_value = {
            "bytes_sent": 0, "packets_sent": 0, "bytes_received": 0,
            "packets_received": 0, "packets_lost": 0, "retransmits": 0,
        }
        stop_requested = threading.Event()
        apply_rate = Mock()

        def finish_first_measurement(*args):
            stop_requested.set()
            return {
                "offered_mbps": 250.0, "goodput_mbps": 250.0,
                "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0,
            }

        with patch.object(engine, "_exact_udp_trial_metrics", side_effect=finish_first_measurement), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), _FreshPeerStats(),
                apply_rate, None, stop_requested, engine.time.time(), [])

        target_rate = (250.0 * 1e6 / 8.0)
        self.assertEqual(apply_rate.call_args_list[:2], [unittest.mock.call(target_rate),
                                                         unittest.mock.call(1.0)])

    def test_udp_tuning_transitions_trial_zero_to_nonzero_and_back(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", acceptable_loss_pct=1.0,
            max_duration_sec=10.0,
            udp_start_mbps=constants.UDP_TUNE_MAX_RATE_MBPS)
        counter = Mock()
        counter.snapshot.return_value = {}
        trial_ids = []
        measurement = {
            "offered_mbps": constants.UDP_TUNE_MAX_RATE_MBPS,
            "goodput_mbps": constants.UDP_TUNE_MAX_RATE_MBPS,
            "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0,
        }

        with patch.object(engine, "_exact_udp_trial_metrics", return_value=measurement), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), _FreshPeerStats(), Mock(),
                None, threading.Event(), engine.time.time(), [],
                apply_trial=trial_ids.append)

        self.assertEqual(trial_ids[:3], [0, 1, 0])
        self.assertTrue(all(trial_id == 0 for trial_id in trial_ids[3:]))

    def test_udp_tuning_activates_zero_packet_records_for_64_streams(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", streams=64,
            acceptable_loss_pct=1.0, max_duration_sec=10.0,
            udp_start_mbps=0.01)
        counters = {index: metrics.StreamCounters(index) for index in range(64)}
        stop_requested = threading.Event()

        def inspect_records(_direction, trial_id, local_streams, *_args):
            self.assertEqual(len(local_streams), 64)
            for stream in local_streams:
                trial = stream["trial_sent"]
                self.assertEqual(trial["trial_id"], trial_id)
                self.assertEqual(trial["packets_sent"], 0)
                self.assertIsNotNone(trial["activated_at_ns"])
                self.assertIsNotNone(trial["deactivated_at_ns"])
            stop_requested.set()
            return {
                "offered_mbps": 0.0, "goodput_mbps": 0.0,
                "offered_pps": 0.0, "goodput_pps": 0.0,
                "loss_pct": 0.0,
            }

        with patch.object(
                engine, "_exact_udp_trial_metrics",
                side_effect=inspect_records), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            tuning = engine._run_udp_tuning(
                cfg, counters, Mock(), Mock(), _FreshPeerStats(), Mock(),
                None, stop_requested, engine.time.time(), [])

        self.assertEqual(tuning["termination_reason"], "stopped")

    def test_udp_loss_limit_uses_nested_loss_causality(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", streams=1,
            acceptable_loss_pct=1.0, max_duration_sec=10.0,
            udp_start_mbps=100.0)
        counter = metrics.StreamCounters(0)
        stop_requested = threading.Event()

        measurement_count = 0

        def measurement(*_args):
            nonlocal measurement_count
            measurement_count += 1
            if measurement_count == 2:
                stop_requested.set()
            return {
                "offered_mbps": 100.0, "goodput_mbps": 98.0,
                "offered_pps": 100.0, "goodput_pps": 98.0,
                "loss_pct": 2.0,
            }

        classification = {
            "primary": "none", "scope": "none", "confidence": "high",
            "target_attained": True, "target_attainment_pct": 100.0,
            "sender_attainment": {
                "primary": "none", "scope": "none", "confidence": "high",
            },
            "loss_causality": {
                "primary": "socket_overflow", "scope": "receiver",
                "confidence": "high",
            },
        }
        with patch.object(
                engine, "_exact_udp_trial_metrics", side_effect=measurement), \
             patch.object(engine, "_classify_trial", return_value=classification), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            tuning = engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), _FreshPeerStats(), Mock(),
                None, stop_requested, engine.time.time(), [])

        self.assertEqual(tuning["limiting_factor"], "socket_overflow")
        self.assertEqual(tuning["limiting_scope"], "receiver")
        self.assertEqual(tuning["limiting_confidence"], "high")

    def test_udp_target_miss_stops_search_and_preserves_prior_best(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", acceptable_loss_pct=1.0,
            max_duration_sec=10.0, udp_start_mbps=100.0)
        counter = Mock()
        counter.snapshot.return_value = {}
        clean_endpoint = {
            "available": True,
            "complete": True,
            "host_cpu": {
                "available": True,
                "host": {"busy_pct": 10.0, "softirq_pct": 1.0},
                "cores": {},
            },
            "process_cpu": {"available": True, "cpu_pct": 10.0},
            "udp": {"available": True, "total": {}},
        }
        measurements = [
            {"offered_mbps": 100.0, "goodput_mbps": 100.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 100.0, "goodput_mbps": 100.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
        ]

        with patch.object(engine, "_exact_udp_trial_metrics", side_effect=measurements), \
             patch.object(engine, "_trial_host_telemetry", return_value=(
                 {"client": {}, "server": {}}, clean_endpoint,
                 clean_endpoint, {"sender": 0, "receiver": 0})), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            tuning = engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), _FreshPeerStats(), Mock(),
                None, threading.Event(), engine.time.time(), [])

        self.assertEqual(tuning["termination_reason"], "target_not_attained")
        self.assertFalse(tuning["search_complete"])
        self.assertEqual(tuning["limiting_factor"], "unknown")
        self.assertEqual(tuning["best"]["target_mbps"], 100.0)
        self.assertEqual(tuning["limiting_trial"]["target_mbps"], 125.0)

    def test_udp_hello_ack_rejects_wire_mismatch_or_missing_version(self):
        for selected in (None, 1, True):
            with self.subTest(selected=selected), \
                 self.assertRaisesRegex(engine.ThroughputError, "wire version mismatch"):
                engine._validate_udp_hello_ack({"udp_wire_version": selected})

        self.assertEqual(engine._validate_udp_hello_ack({
            "udp_wire_version": engine.stream_worker.UDP_WIRE_VERSION,
        }), engine.stream_worker.UDP_WIRE_VERSION)

    def test_udp_tuning_stops_before_an_initial_probe_when_cancelled(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", acceptable_loss_pct=1.0,
            max_duration_sec=10.0)
        stop_requested = threading.Event()
        stop_requested.set()
        applied_rates = []

        tuning = engine._run_udp_tuning(
            cfg, {}, Mock(), Mock(), Mock(), applied_rates.append,
            None, stop_requested, engine.time.time(), [])

        self.assertEqual(tuning["termination_reason"], "stopped")
        self.assertFalse(tuning["search_complete"])
        self.assertIsNone(tuning["probe"])
        self.assertIsNone(tuning["best"])

    def test_udp_tuning_rejects_measurement_without_peer_boundaries(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", acceptable_loss_pct=1.0,
            max_duration_sec=10.0)
        counter = Mock()
        counter.snapshot.return_value = {
            "bytes_sent": 1000, "packets_sent": 10, "bytes_received": 0,
            "packets_received": 0, "packets_lost": 0, "retransmits": 0,
        }

        with patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0), \
             self.assertRaisesRegex(engine.ThroughputError, "fresh peer trial statistics"):
            engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), Mock(snapshot=Mock(return_value=None)),
                Mock(), None, threading.Event(), engine.time.time(), [])

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

    def test_udp_tuning_backs_off_by_percentage_until_loss_passes(self):
        cfg = SimpleNamespace(
            acceptable_loss_pct=1.0,
            max_duration_sec=10.0,
            streams=1,
            direction="up",
            udp_start_mbps=250.0,
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
            {"offered_mbps": 121.8, "goodput_mbps": 110.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.5},
        ])
        calls = []

        def measure(*args):
            result = next(measurements)
            if result["offered_mbps"] == 121.8:
                stop_requested.set()
            return result

        with patch.object(engine, "_exact_udp_trial_metrics", side_effect=measure), \
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

    def test_udp_tuning_repeated_loss_stops_at_sampling_floor(self):
        cfg = engine.ThroughputClientConfig(
            "198.51.100.10", transport="udp", streams=64,
            acceptable_loss_pct=1.0, max_duration_sec=10.0,
            udp_start_mbps=0.2, path_mtu=1500)
        minimum_mbps = engine._minimum_udp_tuning_rate_mbps(cfg)
        counter = metrics.StreamCounters(0)
        applied_rates = []
        measurements = [
            {"offered_mbps": 0.2, "goodput_mbps": 0.18,
             "offered_pps": 10.0, "goodput_pps": 9.0, "loss_pct": 10.0},
            {"offered_mbps": minimum_mbps, "goodput_mbps": 0.13,
             "offered_pps": 8.0, "goodput_pps": 7.0, "loss_pct": 10.0},
        ]
        classification = {
            "primary": "none", "scope": "none", "confidence": "high",
            "target_attained": True, "target_attainment_pct": 100.0,
            "sender_attainment": {
                "primary": "none", "scope": "none", "confidence": "high",
            },
            "loss_causality": {
                "primary": "network_or_remote", "scope": "path",
                "confidence": "low",
            },
        }

        with patch.object(
                engine, "_exact_udp_trial_metrics", side_effect=measurements) as exact, \
             patch.object(engine, "_classify_trial", return_value=classification), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            tuning = engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), _FreshPeerStats(),
                applied_rates.append, None, threading.Event(),
                engine.time.time(), [], minimum_rate_mbps=minimum_mbps)

        candidate_mbps = [
            rate * cfg.streams * 8 / 1e6
            for rate in applied_rates
            if rate != 1.0
        ]
        self.assertEqual(exact.call_count, 2)
        self.assertEqual(len(candidate_mbps), 2)
        self.assertAlmostEqual(candidate_mbps[0], 0.2)
        self.assertAlmostEqual(candidate_mbps[1], minimum_mbps)
        self.assertTrue(all(rate >= minimum_mbps for rate in candidate_mbps))
        self.assertEqual(tuning["termination_reason"], "minimum_rate_reached")
        self.assertEqual(tuning["result_kind"], "no_passing_rate")
        self.assertFalse(tuning["search_complete"])
        self.assertIsNone(tuning["best"])
        self.assertEqual(tuning["best_effort"]["target_mbps"], 0.2)
        self.assertAlmostEqual(
            tuning["limiting_trial"]["target_mbps"], minimum_mbps)
        self.assertEqual(tuning["limiting_factor"], "network_or_remote")
        self.assertEqual(tuning["limiting_scope"], "path")
        with self.assertRaisesRegex(
                engine.ThroughputError,
                r"loss persisted at the minimum sampled rate"):
            engine._validate_udp_tuning_summary(cfg, {
                "recv_mbps": tuning["best_effort"]["goodput_mbps"],
                "loss_pct": tuning["best_effort"]["loss_pct"],
                "tuning": tuning,
            })

    def test_udp_tuning_increases_by_percentage_while_loss_passes(self):
        cfg = SimpleNamespace(
            acceptable_loss_pct=1.0, max_duration_sec=10.0, streams=1, direction="up",
            udp_start_mbps=250.0)
        counter = Mock()
        counter.snapshot.return_value = {
            "bytes_sent": 0, "packets_sent": 0, "bytes_received": 0,
            "packets_received": 0, "packets_lost": 0, "retransmits": 0,
        }
        peer_stats = _FreshPeerStats()
        stop_requested = threading.Event()
        measurements = iter([
            {"offered_mbps": 250.0, "goodput_mbps": 250.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 312.5, "goodput_mbps": 312.5,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 468.75, "goodput_mbps": 468.75,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 937.5, "goodput_mbps": 937.5,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 1875.0, "goodput_mbps": 1875.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
        ])
        calls = []

        def measure(*args):
            result = next(measurements)
            if result["offered_mbps"] == 1875.0:
                stop_requested.set()
            return result

        with patch.object(engine, "_exact_udp_trial_metrics", side_effect=measure), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            tuning = engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), peer_stats, calls.append,
                None, stop_requested, engine.time.time(), [])

        aggregate_rates = []
        for rate in calls:
            aggregate_rate = rate * 8 / 1e6
            if aggregate_rate >= 1.0 and (not aggregate_rates or aggregate_rate != aggregate_rates[-1]):
                aggregate_rates.append(aggregate_rate)
        self.assertEqual(aggregate_rates[:4], [250.0, 312.5, 468.75, 937.5])
        self.assertEqual(tuning["termination_reason"], "stopped")
        self.assertFalse(tuning["search_complete"])

    def test_udp_tuning_refines_loss_bracket_by_percentage(self):
        cfg = SimpleNamespace(
            acceptable_loss_pct=1.0, max_duration_sec=10.0, streams=1, direction="up",
            udp_start_mbps=250.0)
        counter = Mock()
        counter.snapshot.return_value = {
            "bytes_sent": 0, "packets_sent": 0, "bytes_received": 0,
            "packets_received": 0, "packets_lost": 0, "retransmits": 0,
        }
        peer_stats = _FreshPeerStats()
        stop_requested = threading.Event()
        measurements = iter([
            {"offered_mbps": 250.0, "goodput_mbps": 250.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 500.0, "goodput_mbps": 500.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 1000.0, "goodput_mbps": 900.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 2.0},
            {"offered_mbps": 707.1, "goodput_mbps": 707.1,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.5},
            {"offered_mbps": 840.9, "goodput_mbps": 800.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.5},
        ])
        calls = []

        def measure(*args):
            result = next(measurements)
            if result["offered_mbps"] == 840.9:
                stop_requested.set()
            return result

        with patch.object(engine, "_exact_udp_trial_metrics", side_effect=measure), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_INITIAL_INCREASE_PCT", 100.0), \
             patch.object(constants, "UDP_TUNE_MAX_INCREASE_PCT", 100.0):
            tuning = engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), peer_stats, calls.append,
                None, stop_requested, engine.time.time(), [])

        self.assertAlmostEqual(tuning["trials"][-1]["target_mbps"], 707.10678, places=4)

    def test_udp_tuning_converges_after_passing_refinement(self):
        cfg = SimpleNamespace(
            acceptable_loss_pct=1.0, max_duration_sec=10.0, streams=1, direction="up",
            udp_start_mbps=250.0)
        counter = Mock()
        counter.snapshot.return_value = {
            "bytes_sent": 0, "packets_sent": 0, "bytes_received": 0,
            "packets_received": 0, "packets_lost": 0, "retransmits": 0,
        }
        peer_stats = _FreshPeerStats()
        measurements = iter([
            {"offered_mbps": 250.0, "goodput_mbps": 250.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 500.0, "goodput_mbps": 450.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 2.0},
            {"offered_mbps": 353.6, "goodput_mbps": 353.6,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 420.4, "goodput_mbps": 420.4,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 458.5, "goodput_mbps": 440.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 2.0},
            {"offered_mbps": 439.1, "goodput_mbps": 439.1,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
        ])

        with patch.object(engine, "_exact_udp_trial_metrics", side_effect=lambda *args: next(measurements)), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_INITIAL_INCREASE_PCT", 100.0), \
             patch.object(constants, "UDP_TUNE_MAX_INCREASE_PCT", 100.0):
            tuning = engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), peer_stats, Mock(),
                None, threading.Event(), engine.time.time(), [])

        self.assertEqual(tuning["termination_reason"], "converged")
        self.assertTrue(tuning["search_complete"])
        self.assertAlmostEqual(tuning["best"]["target_mbps"], 439.063, places=2)

    def test_udp_tuning_does_not_select_saturated_trial_as_best(self):
        cfg = SimpleNamespace(
            acceptable_loss_pct=1.0, max_duration_sec=10.0, streams=1, direction="up",
            udp_start_mbps=250.0)
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
            {"offered_mbps": 500.0, "goodput_mbps": 104.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
            {"offered_mbps": 600.0, "goodput_mbps": 105.0,
             "offered_pps": 1.0, "goodput_pps": 1.0, "loss_pct": 0.0},
        ])

        with patch.object(engine, "_exact_udp_trial_metrics", side_effect=lambda *args: next(measurements)), \
             patch.object(constants, "UDP_TUNE_SETTLE_SEC", 0.0), \
             patch.object(constants, "UDP_TUNE_TRIAL_SEC", 0.0), \
             patch.object(constants, "PEER_STATS_INTERVAL_SEC", 0.0):
            tuning = engine._run_udp_tuning(
                cfg, {0: counter}, Mock(), Mock(), PeerStats(), Mock(),
                None, threading.Event(), engine.time.time(), [])

        self.assertEqual(tuning["best"]["target_mbps"], 468.75)
        self.assertEqual(tuning["trials"][0]["target_mbps"], 312.5)
        self.assertEqual(tuning["termination_reason"], "plateau")
        self.assertTrue(tuning["search_complete"])

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
