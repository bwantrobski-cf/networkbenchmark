import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from nperf.common import host_telemetry


CPU_BEFORE = """\
cpu 100 20 30 400 50 10 5 0 0 0
cpu0 60 10 20 200 20 5 3 0 0 0
cpu1 40 10 10 200 30 5 2 0 0 0
intr 123
"""

CPU_AFTER = """\
cpu 200 20 80 600 50 20 45 0 0 0
cpu0 110 10 45 300 20 10 23 0 0 0
cpu1 90 10 35 300 30 10 24 0 0 0
intr 456
"""

SNMP_BEFORE = """\
Ip: Forwarding DefaultTTL
Ip: 2 64
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors
Udp: 100 1 2 120 2 0
"""

SNMP_AFTER = """\
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors
Udp: 150 2 8 200 7 1
"""

SNMP6_BEFORE = """\
Ip6InReceives 10
Udp6InDatagrams 20
Udp6NoPorts 0
Udp6InErrors 1
Udp6OutDatagrams 30
Udp6RcvbufErrors 1
Udp6SndbufErrors 0
"""

SNMP6_AFTER = """\
Udp6InDatagrams 35
Udp6NoPorts 1
Udp6InErrors 4
Udp6OutDatagrams 50
Udp6RcvbufErrors 3
Udp6SndbufErrors 0
"""


def pid_stat(pid, comm, user_ticks, system_ticks, starttime_ticks):
    remainder = ["R"] + ["0"] * 49
    remainder[11] = str(user_ticks)
    remainder[12] = str(system_ticks)
    remainder[13] = "3"
    remainder[14] = "4"
    remainder[19] = str(starttime_ticks)
    return "{} ({}) {}\n".format(pid, comm, " ".join(remainder))


def pid_snapshot(pid, captured_at, user_ticks, system_ticks=0):
    return {
        "available": True,
        "captured_at": captured_at,
        "pid": pid,
        "comm": "worker-{}".format(pid),
        "starttime_ticks": pid * 100,
        "clock_ticks_per_sec": 100,
        "user_ticks": user_ticks,
        "system_ticks": system_ticks,
        "children_user_ticks": 0,
        "children_system_ticks": 0,
    }


def write_proc_fixture(root, stat_text, snmp_text, snmp6_text, pid=123,
                       user_ticks=100, system_ticks=50, starttime_ticks=9000):
    root = Path(root)
    (root / "net").mkdir(parents=True)
    (root / str(pid)).mkdir(parents=True)
    (root / "stat").write_text(stat_text, encoding="ascii")
    (root / "net" / "snmp").write_text(snmp_text, encoding="ascii")
    (root / "net" / "snmp6").write_text(snmp6_text, encoding="ascii")
    (root / str(pid) / "stat").write_text(
        pid_stat(pid, "worker ) pool", user_ticks, system_ticks, starttime_ticks),
        encoding="ascii",
    )


class HostCpuTests(unittest.TestCase):
    def test_proc_stat_snapshot_and_delta_include_host_and_cores(self):
        with tempfile.TemporaryDirectory() as before_root, tempfile.TemporaryDirectory() as after_root:
            Path(before_root, "stat").write_text(CPU_BEFORE, encoding="ascii")
            Path(after_root, "stat").write_text(CPU_AFTER, encoding="ascii")
            before = host_telemetry.snapshot_host_cpu(
                before_root, platform_name="Linux", captured_at=10.0)
            after = host_telemetry.snapshot_host_cpu(
                after_root, platform_name="Linux", captured_at=12.0)

        delta = host_telemetry.diff_host_cpu(before, after)

        self.assertTrue(delta["available"])
        self.assertEqual(delta["elapsed_sec"], 2.0)
        self.assertEqual(set(delta["cores"]), {"0", "1"})
        self.assertAlmostEqual(delta["host"]["user_pct"], 25.0)
        self.assertAlmostEqual(delta["host"]["system_pct"], 15.0)
        self.assertAlmostEqual(delta["host"]["softirq_pct"], 10.0)
        self.assertAlmostEqual(delta["host"]["busy_pct"], 50.0)

    def test_unsupported_or_missing_procfs_is_fail_open(self):
        unsupported = host_telemetry.snapshot_host_cpu(
            "/does/not/exist", platform_name="Darwin")
        missing = host_telemetry.snapshot_host_cpu(
            "/does/not/exist", platform_name="Linux")

        self.assertFalse(unsupported["available"])
        self.assertEqual(unsupported["reason"], "unsupported_platform")
        self.assertIsNone(unsupported["host"])
        self.assertFalse(missing["available"])
        self.assertEqual(missing["reason"], "not_found")


class UdpCounterTests(unittest.TestCase):
    def test_snmp_parsers_and_combined_delta(self):
        self.assertEqual(host_telemetry.parse_proc_net_snmp(SNMP_BEFORE)["InDatagrams"], 100)
        self.assertEqual(host_telemetry.parse_proc_net_snmp6(SNMP6_BEFORE)["InDatagrams"], 20)

        with tempfile.TemporaryDirectory() as before_root, tempfile.TemporaryDirectory() as after_root:
            Path(before_root, "net").mkdir()
            Path(after_root, "net").mkdir()
            Path(before_root, "net", "snmp").write_text(SNMP_BEFORE, encoding="ascii")
            Path(before_root, "net", "snmp6").write_text(SNMP6_BEFORE, encoding="ascii")
            Path(after_root, "net", "snmp").write_text(SNMP_AFTER, encoding="ascii")
            Path(after_root, "net", "snmp6").write_text(SNMP6_AFTER, encoding="ascii")
            before = host_telemetry.snapshot_udp_counters(
                before_root, platform_name="Linux", captured_at=4.0)
            after = host_telemetry.snapshot_udp_counters(
                after_root, platform_name="Linux", captured_at=6.0)

        delta = host_telemetry.diff_udp_counters(before, after)

        self.assertTrue(delta["complete"])
        self.assertEqual(delta["ipv4"]["counters"]["RcvbufErrors"], 5)
        self.assertEqual(delta["ipv6"]["counters"]["RcvbufErrors"], 2)
        self.assertEqual(delta["total"]["RcvbufErrors"], 7)
        self.assertEqual(delta["total"]["InDatagrams"], 65)
        self.assertEqual(delta["active_family"], "ipv4")

    def test_counter_reset_is_null_instead_of_negative(self):
        before = {
            "ipv4": {"available": True, "counters": {"InDatagrams": 10}},
            "ipv6": {"available": False, "counters": None},
        }
        after = {
            "ipv4": {"available": True, "counters": {"InDatagrams": 2}},
            "ipv6": {"available": False, "counters": None},
        }

        delta = host_telemetry.diff_udp_counters(before, after)

        self.assertFalse(delta["available"])
        self.assertIsNone(delta["ipv4"]["counters"]["InDatagrams"])
        self.assertEqual(delta["ipv4"]["reason"], "counter_reset")


class PidCpuTests(unittest.TestCase):
    def test_comm_parentheses_and_starttime_matched_delta(self):
        parsed = host_telemetry.parse_proc_pid_stat(
            pid_stat(123, "worker ) pool", 100, 50, 9000))
        self.assertEqual(parsed["comm"], "worker ) pool")
        self.assertEqual(parsed["starttime_ticks"], 9000)

        with tempfile.TemporaryDirectory() as before_root, tempfile.TemporaryDirectory() as after_root:
            Path(before_root, "123").mkdir()
            Path(after_root, "123").mkdir()
            Path(before_root, "123", "stat").write_text(
                pid_stat(123, "worker ) pool", 100, 50, 9000), encoding="ascii")
            Path(after_root, "123", "stat").write_text(
                pid_stat(123, "worker ) pool", 200, 150, 9000), encoding="ascii")
            with patch.object(host_telemetry.os, "sysconf", return_value=100):
                before = host_telemetry.snapshot_pid_cpu(
                    123, before_root, platform_name="Linux", captured_at=10.0)
                after = host_telemetry.snapshot_pid_cpu(
                    123, after_root, platform_name="Linux", captured_at=12.0)

        delta = host_telemetry.diff_pid_cpu(before, after)

        self.assertTrue(delta["complete"])
        self.assertEqual(delta["cpu_ticks"], 200)
        self.assertAlmostEqual(delta["user_pct"], 50.0)
        self.assertAlmostEqual(delta["system_pct"], 50.0)
        self.assertAlmostEqual(delta["cpu_pct"], 100.0)

        after["starttime_ticks"] += 1
        reused = host_telemetry.diff_pid_cpu(before, after)
        self.assertFalse(reused["available"])
        self.assertEqual(reused["reason"], "pid_reused")

    def test_omitted_pid_uses_current_pid(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, "123").mkdir()
            Path(root, "123", "stat").write_text(
                pid_stat(123, "current", 1, 2, 10), encoding="ascii")
            with patch.object(host_telemetry.os, "getpid", return_value=123):
                snapshot = host_telemetry.snapshot_pid_cpu(
                    proc_root=root, platform_name="Linux")

        self.assertTrue(snapshot["available"])
        self.assertEqual(snapshot["pid"], 123)


class EndpointTests(unittest.TestCase):
    def test_endpoint_snapshot_and_diff_compose_all_components(self):
        with tempfile.TemporaryDirectory() as before_root, tempfile.TemporaryDirectory() as after_root:
            write_proc_fixture(
                before_root, CPU_BEFORE, SNMP_BEFORE, SNMP6_BEFORE,
                user_ticks=100, system_ticks=50)
            write_proc_fixture(
                after_root, CPU_AFTER, SNMP_AFTER, SNMP6_AFTER,
                user_ticks=200, system_ticks=150)
            with patch.object(host_telemetry.os, "sysconf", return_value=100):
                before = host_telemetry.snapshot_endpoint(
                    123, before_root, platform_name="Linux", captured_at=20.0)
                after = host_telemetry.snapshot_endpoint(
                    123, after_root, platform_name="Linux", captured_at=22.0)

        delta = host_telemetry.diff_endpoint(before, after)

        self.assertTrue(before["complete"])
        self.assertTrue(delta["complete"])
        self.assertEqual(delta["elapsed_sec"], 2.0)
        self.assertEqual(delta["pid"], 123)
        self.assertEqual(delta["udp"]["total"]["RcvbufErrors"], 7)
        self.assertAlmostEqual(delta["host_cpu"]["host"]["busy_pct"], 50.0)

    def test_endpoint_is_unavailable_without_throwing_off_linux(self):
        snapshot = host_telemetry.snapshot_endpoint(
            os.getpid(), "/does/not/exist", platform_name="Darwin")
        delta = host_telemetry.diff_endpoint(snapshot, snapshot)

        self.assertFalse(snapshot["available"])
        self.assertEqual(snapshot["reason"], "unsupported_platform")
        self.assertFalse(delta["available"])

    def test_multi_process_cpu_reports_peak_and_aggregate_separately(self):
        before = {
            "captured_at": 10.0,
            "host_cpu": {"available": False},
            "process_cpus": [
                pid_snapshot(101, 10.0, 10),
                pid_snapshot(102, 10.0, 20),
            ],
            "udp": {},
        }
        after = {
            "captured_at": 12.0,
            "host_cpu": {"available": False},
            "process_cpus": [
                pid_snapshot(101, 12.0, 120),
                pid_snapshot(102, 12.0, 130),
            ],
            "udp": {},
        }

        delta = host_telemetry.diff_endpoint(before, after)
        result = host_telemetry.classify_bottleneck(
            1000, 700, delta, ClassificationTests.endpoint(), {})

        self.assertAlmostEqual(delta["process_cpu"]["cpu_pct"], 55.0)
        self.assertAlmostEqual(delta["process_cpu"]["max_cpu_pct"], 55.0)
        self.assertAlmostEqual(delta["process_cpu"]["aggregate_cpu_pct"], 110.0)
        self.assertEqual(delta["process_cpu"]["process_count"], 2)
        self.assertEqual(result["primary"], "unknown")
        self.assertNotIn("sender_process_cpu_high", result["evidence"])


class ClassificationTests(unittest.TestCase):
    def test_normalize_ip_family_accepts_platform_socket_values(self):
        self.assertEqual(host_telemetry.normalize_ip_family(2), "ipv4")
        for value in (10, 23, 30):
            self.assertEqual(host_telemetry.normalize_ip_family(value), "ipv6")

    @staticmethod
    def endpoint(busy_pct=10.0, process_pct=10.0, udp=None):
        return {
            "available": True,
            "complete": True,
            "host_cpu": {
                "available": True,
                "host": {"busy_pct": busy_pct, "softirq_pct": 1.0},
                "cores": {"0": {"busy_pct": busy_pct, "softirq_pct": 1.0}},
            },
            "process_cpu": {"available": True, "cpu_pct": process_pct},
            "udp": {"available": True, "total": udp or {}},
        }

    def test_target_attainment_uses_95_percent_threshold(self):
        result = host_telemetry.classify_bottleneck(
            1000, 950, self.endpoint(), self.endpoint(), {})

        self.assertTrue(result["target_attained"])
        self.assertEqual(result["primary"], "none")
        self.assertEqual(result["scope"], "none")
        self.assertEqual(result["confidence"], "high")

    def test_receiver_socket_overflow_is_high_confidence(self):
        result = host_telemetry.classify_bottleneck(
            1000, 700, self.endpoint(), self.endpoint(), {"receiver": 4})

        self.assertFalse(result["target_attained"])
        self.assertEqual(result["primary"], "unknown")
        self.assertEqual(result["scope"], "sender")
        self.assertEqual(result["loss_causality"]["primary"], "socket_overflow")
        self.assertEqual(result["loss_causality"]["scope"], "receiver")
        self.assertEqual(result["loss_causality"]["confidence"], "high")
        self.assertIn("receiver_socket_overflow", result["evidence"])

    def test_sender_receive_queue_drops_are_not_a_sender_limit(self):
        result = host_telemetry.classify_bottleneck(
            1000, 700, self.endpoint(), self.endpoint(), {"sender": 4})

        self.assertEqual(result["primary"], "unknown")
        self.assertEqual(result["scope"], "sender")
        self.assertIn("sender_rxq_overflow_noncausal", result["evidence"])

    def test_partial_endpoint_telemetry_does_not_clear_host_pressure(self):
        partial = self.endpoint()
        partial["complete"] = False
        partial["udp"] = {"available": False}

        result = host_telemetry.classify_bottleneck(
            1000, 700, partial, self.endpoint(), {})

        self.assertEqual(result["primary"], "unknown")
        self.assertIn("endpoint_telemetry_partial", result["evidence"])

    def test_host_udp_overflow_and_sender_cpu_have_stable_scopes(self):
        udp_result = host_telemetry.classify_bottleneck(
            1000, 700, self.endpoint(),
            self.endpoint(udp={"RcvbufErrors": 2, "InErrors": 2}), {},
            loss_pct=2.0)
        cpu_result = host_telemetry.classify_bottleneck(
            1000, 700, self.endpoint(process_pct=99.0), self.endpoint(), {})

        self.assertEqual(
            (udp_result["primary"], udp_result["scope"], udp_result["confidence"]),
            ("unknown", "sender", "low"),
        )
        self.assertEqual(
            (udp_result["loss_causality"]["primary"],
             udp_result["loss_causality"]["scope"],
             udp_result["loss_causality"]["confidence"]),
            ("socket_overflow", "receiver", "medium"),
        )
        self.assertEqual(
            (cpu_result["primary"], cpu_result["scope"], cpu_result["confidence"]),
            ("cpu", "sender", "high"),
        )
        self.assertIn("sender_process_cpu_high", cpu_result["evidence"])

    def test_clean_endpoints_leave_path_as_low_confidence(self):
        result = host_telemetry.classify_bottleneck(
            1000, 700, self.endpoint(), self.endpoint(), {}, loss_pct=1.0)

        self.assertEqual(result["primary"], "unknown")
        self.assertEqual(result["scope"], "sender")
        self.assertEqual(result["loss_causality"]["primary"], "network_or_remote")
        self.assertEqual(result["loss_causality"]["scope"], "path")
        self.assertEqual(result["loss_causality"]["confidence"], "low")
        self.assertIn("no_endpoint_pressure", result["evidence"])

    def test_udp_errors_use_tested_family_and_require_benchmark_loss(self):
        receiver = self.endpoint()
        receiver["tested_family"] = "ipv4"
        receiver["udp"] = {
            "available": True,
            "total": {"InErrors": 9, "RcvbufErrors": 9},
            "ipv4": {"available": True, "counters": {
                "InDatagrams": 100, "InErrors": 0, "RcvbufErrors": 0,
            }},
            "ipv6": {"available": True, "counters": {
                "InDatagrams": 0, "InErrors": 9, "RcvbufErrors": 9,
            }},
        }

        unused_family = host_telemetry.classify_bottleneck(
            1000, 1000, self.endpoint(), receiver, {}, loss_pct=5.0)
        receiver["udp"]["ipv4"]["counters"]["RcvbufErrors"] = 2
        no_loss = host_telemetry.classify_bottleneck(
            1000, 1000, self.endpoint(), receiver, {}, loss_pct=0.0)
        correlated = host_telemetry.classify_bottleneck(
            1000, 1000, self.endpoint(), receiver, {}, loss_pct=5.0)

        self.assertEqual(unused_family["tested_family"], "ipv4")
        self.assertEqual(
            unused_family["loss_causality"]["primary"], "network_or_remote")
        self.assertEqual(no_loss["loss_causality"]["primary"], "none")
        self.assertIn(
            "receiver_host_udp_errors_uncorrelated",
            no_loss["loss_causality"]["evidence"],
        )
        self.assertEqual(
            correlated["loss_causality"]["primary"], "socket_overflow")

    def test_sender_host_udp_errors_also_require_loss_correlation(self):
        sender = self.endpoint(udp={"SndbufErrors": 2})

        no_loss = host_telemetry.classify_bottleneck(
            1000, 700, sender, self.endpoint(), {}, loss_pct=0.0)
        correlated = host_telemetry.classify_bottleneck(
            1000, 700, sender, self.endpoint(), {}, loss_pct=1.0)

        self.assertEqual(no_loss["primary"], "unknown")
        self.assertIn(
            "sender_host_udp_errors_uncorrelated",
            no_loss["sender_attainment"]["evidence"],
        )
        self.assertEqual(correlated["primary"], "socket_overflow")

    def test_unavailable_inputs_return_unknown_without_throwing(self):
        result = host_telemetry.classify_bottleneck(None, "bad", None, None, None)

        self.assertIsNone(result["target_attained"])
        self.assertEqual(result["primary"], "unknown")
        self.assertEqual(result["evidence"], ["throughput_unavailable"])


if __name__ == "__main__":
    unittest.main()
