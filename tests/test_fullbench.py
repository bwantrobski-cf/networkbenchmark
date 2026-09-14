import io
import threading
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from nperf import cli
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
        self.assertFalse(config.jumbo_mtu)

    def test_stream_counts_are_validated_against_protocol_cap(self):
        config = FullBenchConfig("198.51.100.10", streams=[64])
        self.assertEqual(config.streams, [64])

        with self.assertRaisesRegex(ValueError, "1 to 64"):
            FullBenchConfig("198.51.100.10", streams=[65])

    def test_pmtud_ceiling_mode_is_propagated_and_recorded(self):
        for jumbo_mtu, expected_max_mtu in ((False, 1500), (True, 9000)):
            with self.subTest(jumbo_mtu=jumbo_mtu):
                config = FullBenchConfig(
                    "198.51.100.10",
                    transports=["tcp"],
                    directions=["up"],
                    streams=[1],
                    procs=[1],
                    jumbo_mtu=jumbo_mtu,
                )
                pmtud_result = {
                    "success": True,
                    "path_mtu": expected_max_mtu,
                    "path_mtu_source": "udp_pmtud",
                    "fallback_used": False,
                    "probe_max_mtu": expected_max_mtu,
                    "probe_ceiling_reached": True,
                }
                with patch.object(fullbench, "discover_path_mtu", return_value=pmtud_result) as discover, \
                     patch.object(fullbench.logging_store, "save_result", return_value=("result.json", "result.log")), \
                     patch.object(fullbench, "run_throughput_test", return_value={"run_id": "run-id"}) as run_test:
                    manifest = fullbench.run_fullbench(config)

                self.assertEqual(discover.call_args.kwargs["max_mtu"], expected_max_mtu)
                self.assertEqual(run_test.call_args.args[0].jumbo_mtu, jumbo_mtu)
                self.assertEqual(manifest["jumbo_mtu"], jumbo_mtu)
                self.assertEqual(manifest["probe_max_mtu"], expected_max_mtu)
                self.assertTrue(manifest["probe_ceiling_reached"])
                self.assertEqual(manifest["config"]["jumbo_mtu"], jumbo_mtu)
                self.assertEqual(manifest["tests"][0]["probe_max_mtu"], expected_max_mtu)
                self.assertTrue(manifest["tests"][0]["probe_ceiling_reached"])

    def test_custom_udp_tuning_start_rate_is_retained(self):
        config = FullBenchConfig("198.51.100.10", udp_start_mbps=5000)

        self.assertEqual(config.udp_start_mbps, 5000.0)

    def test_udp_tuning_rejects_nonfinite_inputs(self):
        with self.assertRaisesRegex(ValueError, "no more than"):
            FullBenchConfig("198.51.100.10", udp_start_mbps=float("nan"))
        with self.assertRaisesRegex(ValueError, "finite percentage"):
            FullBenchConfig("198.51.100.10", acceptable_loss_pct=float("inf"))
        with self.assertRaisesRegex(ValueError, "no more than"):
            FullBenchConfig("198.51.100.10", udp_start_mbps=1e308)

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

    def test_per_case_config_error_is_recorded_without_aborting_matrix(self):
        config = FullBenchConfig(
            "198.51.100.10",
            transports=["tcp"],
            directions=["up"],
            streams=[1, 8],
            procs=[1],
        )
        real_config = fullbench.ThroughputClientConfig

        def build_case(**kwargs):
            if kwargs["streams"] == 1:
                raise ValueError("invalid first case")
            return real_config(**kwargs)

        with patch.object(fullbench, "discover_path_mtu", return_value={
                "success": True,
                "path_mtu": 1400,
                "path_mtu_source": "test",
                "fallback_used": False,
            }), patch.object(fullbench.logging_store, "save_result", return_value=("result.json", "result.log")), \
             patch.object(fullbench, "ThroughputClientConfig", side_effect=build_case), \
             patch.object(fullbench, "run_throughput_test", return_value={"run_id": "run-id"}) as run_test:
            manifest = fullbench.run_fullbench(config)

        self.assertEqual(manifest["completed"], 2)
        self.assertEqual(manifest["status"], "complete")
        self.assertFalse(manifest["tests"][0]["ok"])
        self.assertEqual(manifest["tests"][0]["error"], "invalid first case")
        self.assertTrue(manifest["tests"][1]["ok"])
        run_test.assert_called_once()

    def test_stopped_udp_tuning_is_recorded_as_failure(self):
        config = FullBenchConfig(
            "198.51.100.10",
            transports=["udp"],
            directions=["up"],
            streams=[1],
            procs=[1],
            acceptable_loss_pct=1.0,
        )
        summary = {
            "run_id": "run-id",
            "tuning": {"search_complete": False, "termination_reason": "stopped"},
        }

        with patch.object(fullbench, "discover_path_mtu", return_value={
                "success": True,
                "path_mtu": 1400,
                "path_mtu_source": "test",
                "fallback_used": False,
            }), patch.object(fullbench.logging_store, "save_result", return_value=("result.json", "result.log")), \
             patch.object(fullbench, "run_throughput_test", return_value=summary):
            manifest = fullbench.run_fullbench(
                config, stop_requested=threading.Event(), log_base_dir="/tmp")

        self.assertFalse(manifest["tests"][0]["ok"])
        self.assertEqual(manifest["tests"][0]["error"], "stopped by request")
        self.assertEqual(manifest["test_failures"], 1)

    def test_unattained_udp_target_is_not_recorded_as_network_ceiling(self):
        config = FullBenchConfig(
            "198.51.100.10",
            transports=["udp"],
            directions=["up"],
            streams=[1],
            procs=[1],
            acceptable_loss_pct=1.0,
        )
        summary = {
            "run_id": "run-id",
            "send_mbps": 700.0,
            "recv_mbps": 690.0,
            "tuning": {
                "search_complete": False,
                "ceiling_confirmed": False,
                "result_kind": "endpoint_limited",
                "termination_reason": "target_not_attained",
                "limiting_factor": "cpu",
                "limiting_scope": "sender",
                "limiting_confidence": "high",
            },
        }

        with patch.object(fullbench, "discover_path_mtu", return_value={
                "success": True,
                "path_mtu": 1400,
                "path_mtu_source": "test",
                "fallback_used": False,
            }), patch.object(fullbench.logging_store, "save_result", return_value=("result.json", "result.log")), \
             patch.object(fullbench, "run_throughput_test", return_value=summary):
            manifest = fullbench.run_fullbench(
                config, stop_requested=threading.Event(), log_base_dir="/tmp")

        record = manifest["tests"][0]
        self.assertTrue(record["ok"])
        self.assertIsNone(record["error"])
        self.assertFalse(record["tuning_ceiling_confirmed"])
        self.assertEqual(record["tuning_result_kind"], "endpoint_limited")
        self.assertEqual(record["tuning_limiting_factor"], "cpu")
        self.assertEqual(record["tuning_limiting_scope"], "sender")
        self.assertEqual(manifest["test_failures"], 0)

        output = io.StringIO()
        with redirect_stdout(output):
            cli._print_fullbench_summary(manifest)
        self.assertIn("TARGET NOT ATTAINED", output.getvalue())

    def test_cancellation_between_cases_marks_manifest_aborted(self):
        config = FullBenchConfig(
            "198.51.100.10",
            transports=["tcp"],
            directions=["up"],
            streams=[1, 8],
            procs=[1],
        )
        stop_requested = threading.Event()

        def finish_first_case(*_args, **_kwargs):
            stop_requested.set()
            return {"run_id": "run-id"}

        with patch.object(fullbench, "discover_path_mtu", return_value={
                "success": True,
                "path_mtu": 1400,
                "path_mtu_source": "test",
                "fallback_used": False,
            }), patch.object(fullbench.logging_store, "save_result", return_value=("result.json", "result.log")), \
             patch.object(fullbench, "run_throughput_test", side_effect=finish_first_case):
            manifest = fullbench.run_fullbench(
                config, stop_requested=stop_requested, log_base_dir="/tmp")

        self.assertEqual(manifest["completed"], 1)
        self.assertEqual(manifest["test_count"], 2)
        self.assertEqual(manifest["status"], "aborted")
        self.assertFalse(manifest["complete"])
        self.assertTrue(manifest["aborted"])

    def test_final_or_only_cancelled_case_takes_aborted_precedence(self):
        cancelled = {
            "run_id": "cancelled-run",
            "status": "cancelled",
            "complete": False,
            "aborted": True,
            "cancelled": True,
        }
        cases = (
            ([1], [cancelled]),
            ([1, 8], [{"run_id": "complete-run", "status": "complete"},
                      cancelled]),
        )
        for streams, summaries in cases:
            with self.subTest(streams=streams):
                config = FullBenchConfig(
                    "198.51.100.10",
                    transports=["tcp"], directions=["up"],
                    streams=streams, procs=[1],
                )
                with patch.object(fullbench, "discover_path_mtu", return_value={
                        "success": True,
                        "path_mtu": 1400,
                        "path_mtu_source": "test",
                        "fallback_used": False,
                    }), patch.object(
                        fullbench.logging_store, "save_result",
                        return_value=("result.json", "result.log")), \
                     patch.object(
                        fullbench, "run_throughput_test",
                        side_effect=summaries):
                    manifest = fullbench.run_fullbench(
                        config, stop_requested=threading.Event(),
                        log_base_dir="/tmp")

                self.assertEqual(manifest["completed"], len(streams))
                self.assertEqual(manifest["test_count"], len(streams))
                self.assertEqual(manifest["status"], "aborted")
                self.assertTrue(manifest["aborted"])
                self.assertFalse(manifest["complete"])
                self.assertFalse(manifest["tests"][-1]["ok"])
                self.assertTrue(manifest["tests"][-1]["aborted"])

    def test_cli_exits_nonzero_for_incomplete_matrix(self):
        args = cli.build_parser().parse_args([
            "fullbench", "--server-ip", "198.51.100.10",
            "--transports", "tcp", "--directions", "up", "--streams", "1,8",
        ])
        manifest = {
            "benchmark_id": "benchmark-id",
            "completed": 1,
            "test_count": 2,
            "status": "aborted",
            "tests": [{"ok": True}],
        }
        output = io.StringIO()

        with patch.object(cli.signal, "signal"), \
             patch.object(cli.fullbench_mod, "run_fullbench", return_value=manifest), \
             patch.object(cli, "_print_fullbench_summary"), \
             redirect_stdout(output), self.assertRaises(SystemExit) as raised:
            cli.cmd_fullbench(args)

        self.assertEqual(raised.exception.code, 1)
        self.assertIn("matrix did not complete", output.getvalue())

    def test_cli_exits_nonzero_for_aborted_single_case_matrix(self):
        args = cli.build_parser().parse_args([
            "fullbench", "--server-ip", "198.51.100.10",
            "--transports", "tcp", "--directions", "up", "--streams", "1",
        ])
        manifest = {
            "benchmark_id": "benchmark-id",
            "completed": 1,
            "test_count": 1,
            "status": "aborted",
            "tests": [{"ok": False, "aborted": True}],
        }

        with patch.object(cli.signal, "signal"), \
             patch.object(
                 cli.fullbench_mod, "run_fullbench", return_value=manifest), \
             patch.object(cli, "_print_fullbench_summary"), \
             redirect_stdout(io.StringIO()), \
             self.assertRaises(SystemExit) as raised:
            cli.cmd_fullbench(args)

        self.assertEqual(raised.exception.code, 1)

    def test_throughput_cli_exits_nonzero_for_cancelled_fixed_run(self):
        args = cli.build_parser().parse_args([
            "tput", "--server-ip", "198.51.100.10", "--tcp",
        ])
        summary = {
            "status": "cancelled",
            "aborted": True,
            "tuning": None,
        }

        with patch.object(cli.signal, "signal"), \
             patch.object(cli, "run_throughput_test", return_value=summary), \
             patch.object(cli, "_print_throughput_summary"), \
             self.assertRaises(SystemExit) as raised:
            cli.cmd_throughput(args)

        self.assertEqual(raised.exception.code, 1)

    def test_export_preserves_user_label_and_adds_case_label(self):
        config = FullBenchConfig(
            "198.51.100.10",
            transports=["tcp"], directions=["up"], streams=[1], procs=[1],
            label="aes-256-gcm",
            export_url="https://example.test/nperf-results",
            export_headers={"Authorization": "secret"},
        )
        exported = []

        def capture_export(_url, payload, **_kwargs):
            exported.append(payload)
            return {"ok": True, "status": 200, "error": None}

        with patch.object(fullbench, "discover_path_mtu", return_value={
                "success": True,
                "path_mtu": 1400,
                "path_mtu_source": "test",
                "fallback_used": False,
            }), patch.object(
                fullbench.logging_store, "save_result",
                return_value=("result.json", "result.log")), \
             patch.object(
                fullbench, "run_throughput_test",
                return_value={"run_id": "run-id", "status": "complete"}), \
             patch.object(fullbench, "export_result", side_effect=capture_export):
            manifest = fullbench.run_fullbench(
                config, stop_requested=threading.Event(), log_base_dir="/tmp")

        expected_case = "tcp/up/1s/1p/fixed"
        self.assertEqual(manifest["tests"][0]["label"], "aes-256-gcm")
        self.assertEqual(manifest["tests"][0]["case_label"], expected_case)
        self.assertEqual(exported[0]["label"], "aes-256-gcm")
        self.assertEqual(exported[0]["case_label"], expected_case)
        self.assertNotIn("export_headers", exported[0])
        self.assertNotIn("Authorization", exported[0])

    def test_tuning_trial_labels_unattained_target_instead_of_ok(self):
        output = io.StringIO()
        tuning = {
            "acceptable_loss_pct": 1.0,
            "trials": [{
                "target_mbps": 1000.0,
                "goodput_mbps": 700.0,
                "goodput_pps": 1.0,
                "loss_pct": 0.1,
                "target_attained": False,
            }],
            "termination_reason": "target_not_attained",
            "search_complete": False,
        }

        with redirect_stdout(output):
            cli._print_tuning_summary(tuning)

        self.assertIn("[TARGET NOT ATTAINED]", output.getvalue())
        self.assertNotIn("[ok]", output.getvalue())

    def test_cli_rxq_drops_are_udp_only_and_support_aware(self):
        base_summary = {
            "send_mbps": 1.0,
            "recv_mbps": 1.0,
            "socket_telemetry": {
                "client": {
                    "kernel_rxq_drops": 0,
                    "streams": [{"rxq_overflow_supported": True}],
                },
                "server": {
                    "kernel_rxq_drops": 0,
                    "streams": [{"rxq_overflow_supported": None}],
                },
            },
        }
        tcp_output = io.StringIO()
        with redirect_stdout(tcp_output):
            cli._print_throughput_summary({**base_summary, "transport": "tcp"})
        self.assertNotIn("RX queue drops", tcp_output.getvalue())

        udp_output = io.StringIO()
        with redirect_stdout(udp_output):
            cli._print_throughput_summary({**base_summary, "transport": "udp"})
        self.assertIn("client=0", udp_output.getvalue())
        self.assertIn("server=unavailable", udp_output.getvalue())

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

    def test_export_failure_is_counted_and_written_to_manifest_log(self):
        config = FullBenchConfig(
            "198.51.100.10",
            transports=["tcp"],
            directions=["up"],
            streams=[1],
            procs=[1],
            export_url="https://example.test/nperf-results",
        )
        saved_text = []

        def capture_result(_test_type, _payload, text_lines, **_kwargs):
            saved_text.extend(text_lines)
            return "result.json", "result.log"

        summary = {
            "run_id": "run-id",
            "send_mbps": 100.0,
            "recv_mbps": 99.0,
            "send_pps": 1.0,
            "recv_pps": 1.0,
            "loss_pct": None,
            "retransmits": 1,
            "path_mtu": 1400,
            "tcp_mss_bytes": 1360,
            "udp_datagram_bytes": 1372,
            "log_json": "test.json",
        }
        with patch.object(fullbench, "discover_path_mtu", return_value={
                "success": True,
                "path_mtu": 1400,
                "path_mtu_source": "test",
                "fallback_used": False,
            }), patch.object(fullbench.logging_store, "save_result", side_effect=capture_result), \
             patch.object(fullbench, "run_throughput_test", return_value=summary), \
             patch.object(fullbench, "export_result", return_value={
                 "ok": False, "status": None, "error": "DNS failed",
             }):
            manifest = fullbench.run_fullbench(
                config, stop_requested=threading.Event(), log_base_dir="/tmp")

        self.assertEqual(manifest["test_failures"], 0)
        self.assertEqual(manifest["export_failures"], 1)
        self.assertTrue(any("export=FAIL(DNS failed)" in line for line in saved_text))


if __name__ == "__main__":
    unittest.main()
