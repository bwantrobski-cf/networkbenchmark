import queue
import socket
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from nperf.client import engine
from nperf.transport import metrics, run_mode


class ClientTelemetryTests(unittest.TestCase):
    def test_dispatch_retains_final_host_sample_and_socket_identity(self):
        control = Mock()
        control.recv.side_effect = [
            {
                "type": "peer_stats",
                "t": 1.0,
                "streams": [{
                    "index": 0,
                    "bytes_received": 100,
                    "socket_sndbuf_bytes": 999,
                    "socket_rcvbuf_bytes": 888,
                    "kernel_rxq_drops": 7,
                }],
                "host_sample": {"available": True, "marker": "periodic"},
            },
            {
                "type": "final_stats",
                "streams": [{
                    "index": 0,
                    "bytes_received": 200,
                    "socket_sndbuf_bytes": 1000,
                    "socket_rcvbuf_bytes": 900,
                    "kernel_rxq_drops": 9,
                }],
                "host_sample": {"available": True, "marker": "final"},
            },
            {"type": "done"},
        ]
        counter = metrics.StreamCounters(0)
        counter.set_socket_metadata(
            socket_sndbuf_bytes=111,
            socket_rcvbuf_bytes=222,
            rxq_overflow_supported=True,
        )
        tracker = engine._PeerStatsTracker()

        engine._client_dispatch_loop(
            control, run_mode.RunPhase(), metrics.RttTracker(), {0: counter},
            threading.Event(), peer_stats=tracker,
            final_stats_event=threading.Event())

        local = counter.snapshot()
        server = tracker.latest_streams()[0]
        self.assertEqual(local["socket_sndbuf_bytes"], 111)
        self.assertEqual(local["socket_rcvbuf_bytes"], 222)
        self.assertEqual(local["kernel_rxq_drops"], 0)
        self.assertEqual(server["socket_sndbuf_bytes"], 1000)
        self.assertEqual(server["kernel_rxq_drops"], 9)
        self.assertEqual(
            tracker.final_boundary()["host_sample"]["marker"], "final")

    def test_client_worker_propagates_shared_uint32_trial_holder(self):
        fake_socket = Mock()
        fake_socket.family = socket.AF_INET
        fake_socket.getsockname.return_value = ("127.0.0.1", 40000)
        connected = queue.Queue()
        trial = SimpleNamespace(value=17)
        rate = SimpleNamespace(value=1000.0)
        counter = metrics.StreamCounters(0)
        shared = SimpleNamespace(view=Mock(return_value=counter))
        conn = {
            "transport": "udp",
            "family": socket.AF_INET,
            "bind": "0.0.0.0",
            "server_ip": "127.0.0.1",
            "duration_sec": 1.0,
            "path_mtu": 1500,
        }

        with patch.object(engine.socket, "socket", return_value=fake_socket), \
             patch.object(engine.net, "configure_dont_fragment"), \
             patch.object(engine.net, "sockaddr", side_effect=lambda host, port, family: (host, port)), \
             patch.object(engine.stream_worker, "run_stream_group") as run_group:
            engine._client_worker_proc_inner(
                conn, "run-id", [(0, 9000)], "send", shared,
                SimpleNamespace(value=1), SimpleNamespace(value=0), connected,
                rate_value=rate, trial_value=trial)

        self.assertEqual(connected.get_nowait(), ("ok", {0: 40000}))
        self.assertIs(run_group.call_args.kwargs["rate_value"], rate)
        self.assertIs(run_group.call_args.kwargs["trial_value"], trial)

    def test_host_telemetry_failures_are_nonfatal(self):
        topology = {"controller_pid": 10, "workers": []}
        with patch.object(engine.host_telemetry, "snapshot_endpoint",
                          side_effect=RuntimeError("procfs failed")):
            sample = engine._snapshot_client_endpoint(topology)

        summary = engine._build_whole_run_host_telemetry(
            "up", sample, sample, engine._PeerStatsTracker(), topology)

        self.assertFalse(sample["available"])
        self.assertIn("procfs failed", sample["error"])
        self.assertEqual(sample["topology"], topology)
        self.assertFalse(summary["server"]["delta"]["available"])
        self.assertIsNone(summary["server"]["final_sample"])

    def test_compact_trial_telemetry_keeps_cpu_and_udp_error_deltas(self):
        compact = engine._compact_endpoint_delta({
            "available": True,
            "complete": True,
            "elapsed_sec": 5.0,
            "host_cpu": {"host": {
                "user_pct": 20.0,
                "system_pct": 10.0,
                "softirq_pct": 3.0,
                "busy_pct": 33.0,
            }},
            "process_cpu": {"user_pct": 5.0, "system_pct": 2.0, "cpu_pct": 7.0},
            "udp": {"total": {"InErrors": 4, "RcvbufErrors": 3, "SndbufErrors": 2}},
        })

        self.assertEqual(compact["cpu"]["user_pct"], 20.0)
        self.assertEqual(compact["cpu"]["system_pct"], 10.0)
        self.assertEqual(compact["cpu"]["softirq_pct"], 3.0)
        self.assertEqual(compact["udp_errors"]["InErrors"], 4)
        self.assertEqual(compact["udp_errors"]["RcvbufErrors"], 3)
        self.assertEqual(compact["udp_errors"]["SndbufErrors"], 2)

    def test_compact_trial_telemetry_uses_only_tested_udp_family(self):
        compact = engine._compact_endpoint_delta({
            "available": True,
            "complete": True,
            "tested_family": "ipv6",
            "udp": {
                "total": {"InErrors": 11, "RcvbufErrors": 13},
                "ipv4": {"available": True, "counters": {
                    "InErrors": 10, "RcvbufErrors": 12,
                }},
                "ipv6": {"available": True, "counters": {
                    "InErrors": 1, "RcvbufErrors": 1,
                }},
            },
        })

        self.assertEqual(compact["tested_family"], "ipv6")
        self.assertEqual(compact["udp_errors"]["InErrors"], 1)
        self.assertEqual(compact["udp_errors"]["RcvbufErrors"], 1)

    def test_socket_summary_keeps_client_and_server_actuals_separate(self):
        summary = engine._build_socket_telemetry(
            [{"index": 0, "socket_sndbuf_bytes": 111,
              "socket_rcvbuf_bytes": 222, "kernel_rxq_drops": 1}],
            [{"index": 0, "socket_sndbuf_bytes": 333,
              "socket_rcvbuf_bytes": 444, "kernel_rxq_drops": 2}],
        )

        client = summary["client"]["streams"][0]
        server = summary["server"]["streams"][0]
        self.assertEqual(client["actual_sndbuf_bytes"], 111)
        self.assertEqual(server["actual_sndbuf_bytes"], 333)
        self.assertEqual(client["kernel_rxq_drops"], 1)
        self.assertEqual(server["kernel_rxq_drops"], 2)
        self.assertEqual(
            client["requested_sndbuf_bytes"], summary["requested_buffer_bytes"])

    def test_unsupported_rxq_overflow_is_not_serialized_as_zero(self):
        unsupported = metrics.StreamCounters(0)
        unsupported.set_socket_metadata(rxq_overflow_supported=False)
        supported = metrics.StreamCounters(1)
        supported.set_socket_metadata(rxq_overflow_supported=True)

        self.assertIsNone(unsupported.snapshot()["kernel_rxq_drops"])
        self.assertEqual(supported.snapshot()["kernel_rxq_drops"], 0)

    def test_rxq_aggregate_rejects_partial_support_or_stream_coverage(self):
        mixed = engine._build_socket_telemetry(
            [
                {"index": 0, "rxq_overflow_supported": True,
                 "kernel_rxq_drops": 0},
                {"index": 1, "rxq_overflow_supported": False,
                 "kernel_rxq_drops": None},
            ],
            [
                {"index": 0, "rxq_overflow_supported": True,
                 "kernel_rxq_drops": 0},
                {"index": 1, "rxq_overflow_supported": True,
                 "kernel_rxq_drops": 0},
            ],
        )
        partial = engine._build_socket_telemetry(
            [
                {"index": 0, "rxq_overflow_supported": True,
                 "kernel_rxq_drops": 0},
                {"index": 1, "rxq_overflow_supported": True,
                 "kernel_rxq_drops": 0},
            ],
            [{"index": 0, "rxq_overflow_supported": True,
              "kernel_rxq_drops": 0}],
        )

        self.assertIsNone(mixed["client"]["rxq_overflow_supported"])
        self.assertIsNone(mixed["client"]["kernel_rxq_drops"])
        self.assertFalse(mixed["client"]["available"])
        self.assertIsNone(partial["server"]["rxq_overflow_supported"])
        self.assertIsNone(partial["server"]["kernel_rxq_drops"])
        self.assertFalse(partial["server"]["available"])

    def test_rxq_delta_rejects_partial_stream_coverage(self):
        before = [
            {"index": 0, "rxq_overflow_supported": True,
             "kernel_rxq_drops": 0},
            {"index": 1, "rxq_overflow_supported": True,
             "kernel_rxq_drops": 0},
        ]
        after = [{
            "index": 0, "rxq_overflow_supported": True,
            "kernel_rxq_drops": 2,
        }]

        delta = engine._socket_overflow_delta(before, after)

        self.assertFalse(delta["available"])
        self.assertIsNone(delta["rxq_overflow_supported"])
        self.assertIsNone(delta["total"])

    def test_rxq_delta_preserves_unknown_support(self):
        delta = engine._socket_overflow_delta(
            [{"index": 0, "rxq_overflow_supported": True,
              "kernel_rxq_drops": 0}],
            [{"index": 0, "rxq_overflow_supported": None,
              "kernel_rxq_drops": None}],
        )

        self.assertFalse(delta["available"])
        self.assertIsNone(delta["rxq_overflow_supported"])
        self.assertIsNone(delta["total"])

    def test_loss_breach_with_clean_telemetry_is_classified_as_path(self):
        endpoint = {
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

        result = engine._classify_trial(
            100.0, 100.0, 2.0, 1.0, endpoint, endpoint,
            {"sender": 0, "receiver": 0})

        self.assertTrue(result["target_attained"])
        self.assertEqual(result["primary"], "none")
        self.assertEqual(result["sender_attainment"]["primary"], "none")
        self.assertEqual(result["loss_causality"]["primary"], "network_or_remote")
        self.assertEqual(result["loss_causality"]["scope"], "path")
        self.assertIn("loss_target_breached", result["evidence"])


if __name__ == "__main__":
    unittest.main()
