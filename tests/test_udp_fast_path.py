import multiprocessing
import socket
import threading
import time
import unittest
from unittest.mock import patch

from nperf.transport import metrics, shared_counters, stream_worker


def _frame(stream_index, sequence, trial_id, flag=stream_worker.UDP_FLAG_DATA,
           size=64, session_cookie=0):
    packet = bytearray(max(size, stream_worker.UDP_HEADER_SIZE))
    stream_worker.UDP_HEADER_STRUCT.pack_into(
        packet, 0, stream_index, sequence, session_cookie, trial_id, flag)
    return packet


def _increment_shared_counter(shared, count):
    counters = shared.view(0)
    for _ in range(count):
        counters.add_sent(1)


class _FakeSendSocket:
    family = socket.AF_INET

    def __init__(self):
        self.connected_to = None
        self.blocking = None

    def setsockopt(self, _level, _option, _value):
        pass

    def getsockopt(self, _level, option):
        return 111 if option == socket.SO_SNDBUF else 222

    def connect(self, address):
        self.connected_to = address

    def setblocking(self, value):
        self.blocking = value


class _RecordingCounters(metrics.StreamCounters):
    def __init__(self, index):
        super().__init__(index)
        self.sent_publications = 0

    def add_sent_batch(self, *args, **kwargs):
        self.sent_publications += 1
        return super().add_sent_batch(*args, **kwargs)


class UdpFastPathTests(unittest.TestCase):
    def test_v3_header_round_trip_uses_uint64_sequence_and_cookie(self):
        sequence = (1 << 48) + 123
        packet = _frame(7, sequence, 41)

        self.assertEqual(stream_worker.UDP_WIRE_VERSION, 3)
        self.assertEqual(stream_worker.UDP_HEADER_SIZE, 25)
        self.assertEqual(
            stream_worker.UDP_HEADER_STRUCT.unpack_from(packet),
            (7, sequence, 0, 41, stream_worker.UDP_FLAG_DATA),
        )

    def test_feed_rejects_malformed_wrong_stream_and_unknown_flag(self):
        counters = metrics.StreamCounters(3)

        self.assertFalse(stream_worker.feed_udp_data_frame(counters, b"short"))
        self.assertFalse(stream_worker.feed_udp_data_frame(
            counters, _frame(4, 0, 1)))
        self.assertFalse(stream_worker.feed_udp_data_frame(
            counters, _frame(3, 0, 1, flag=99)))
        self.assertEqual(counters.snapshot()["packets_received"], 0)

        self.assertTrue(stream_worker.feed_udp_data_frame(
            counters, _frame(3, 1 << 40, 9)))
        snapshot = counters.snapshot()
        self.assertEqual(snapshot["packets_received"], 1)
        self.assertEqual(snapshot["max_seq_seen"], 1 << 40)
        self.assertEqual(snapshot["trial_received"]["trial_id"], 9)

    def test_feed_rejects_a_stale_session_cookie(self):
        counters = metrics.StreamCounters(0)
        current_cookie = stream_worker.udp_session_cookie("current-run")

        self.assertFalse(stream_worker.feed_udp_data_frame(
            counters, _frame(0, 0, 1, session_cookie=current_cookie + 1),
            session_cookie=current_cookie))
        self.assertTrue(stream_worker.feed_udp_data_frame(
            counters, _frame(0, 0, 1, session_cookie=current_cookie),
            session_cookie=current_cookie))
        self.assertEqual(counters.snapshot()["packets_received"], 1)

    def test_sequence_tracker_handles_ranges_duplicates_and_reordering(self):
        base = (1 << 40) + 100
        tracker = stream_worker.SequenceTracker()

        statuses = [
            tracker.observe(base),
            tracker.observe(base + 4),
            tracker.observe(base + 2),
            tracker.observe(base + 2),
            tracker.observe(base + 1),
            tracker.observe(base + 3),
        ]

        self.assertEqual(statuses, [
            tracker.UNIQUE,
            tracker.UNIQUE,
            tracker.REORDERED,
            tracker.DUPLICATE,
            tracker.REORDERED,
            tracker.REORDERED,
        ])
        self.assertEqual(tracker.unique_count, 5)
        self.assertEqual(tracker.duplicates, 1)
        self.assertEqual(tracker.out_of_order, 3)
        self.assertEqual(tracker.missing_count, 0)
        self.assertEqual(tracker.missing_ranges, [])

        tracker.reset(trial_id=8)
        self.assertEqual(tracker.observe((1 << 63) + 9), tracker.UNIQUE)
        self.assertEqual(tracker.missing_count, 0)
        self.assertEqual(tracker.trial_id, 8)

    def test_sequence_tracker_bounds_reorder_ranges(self):
        tracker = stream_worker.SequenceTracker()
        tracker.observe(0)
        for sequence in range(2, (1 << 17) + 2, 2):
            tracker.observe(sequence)

        self.assertLessEqual(
            len(tracker.missing_ranges), stream_worker._SEQUENCE_REORDER_WINDOW)
        self.assertGreater(tracker.missing_count, 0)

    def test_udp_preamble_reader_ignores_unrelated_datagrams(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        receiver.bind(("127.0.0.1", 0))
        sender.sendto(b"unrelated", receiver.getsockname())
        sender.sendto(
            stream_worker.build_preamble(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", 3),
            receiver.getsockname())
        sender.sendto(
            stream_worker.build_preamble(
                "12345678-1234-1234-1234-123456789abc", 2),
            receiver.getsockname())
        sender.sendto(
            stream_worker.build_preamble(
                "12345678-1234-1234-1234-123456789abc", 3),
            receiver.getsockname())

        result = stream_worker.read_udp_preamble(
            receiver, timeout_sec=1.0,
            expected_run_id="12345678-1234-1234-1234-123456789abc",
            expected_stream_index=3)

        self.assertEqual(result[:2], (
            "12345678-1234-1234-1234-123456789abc", 3))

    def test_udp_preamble_reader_times_out_after_only_foreign_preambles(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        receiver.bind(("127.0.0.1", 0))
        sender.sendto(
            stream_worker.build_preamble(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", 7),
            receiver.getsockname())

        result = stream_worker.read_udp_preamble(
            receiver, timeout_sec=0.02,
            expected_run_id="12345678-1234-1234-1234-123456789abc",
            expected_stream_index=7)

        self.assertIsNone(result)

    def test_threaded_and_shared_batch_counter_snapshots_match(self):
        threaded = metrics.StreamCounters(0)
        shared = shared_counters.SharedCounters(1).view(0)

        for counters in (threaded, shared):
            counters.add_sent_batch(
                300, 3, trial_id=12, first_seq=(1 << 63) + 1,
                last_seq=(1 << 63) + 3)
            counters.add_received_batch(
                200, 2, trial_id=12, first_seq=(1 << 63) + 1,
                last_seq=(1 << 63) + 4, duplicates=1, reordered=1,
                missing_count=2, kernel_rxq_drops=7,
                trial_missing_count=2)
            counters.set_socket_metadata(4096, 8192, True)

        self.assertEqual(threaded.snapshot(), shared.snapshot())
        self.assertEqual(shared_counters._NUM_FIELDS * 8 % 64, 0)

    def test_threaded_and_shared_publish_zero_packet_trial_boundaries(self):
        threaded = metrics.StreamCounters(0)
        shared = shared_counters.SharedCounters(1).view(0)

        for counters in (threaded, shared):
            counters.activate_trial(23, "sent", activated_at_ns=100)
            counters.set_observed_trial(23)
            counters.deactivate_trial(23, "sent", deactivated_at_ns=200)
            counters.activate_trial(23, "received", activated_at_ns=100)
            counters.deactivate_trial(23, "received", deactivated_at_ns=200)

        self.assertEqual(threaded.snapshot(), shared.snapshot())
        snapshot = threaded.snapshot()
        for role in ("trial_sent", "trial_received"):
            self.assertEqual(snapshot[role]["trial_id"], 23)
            self.assertEqual(snapshot[role]["activated_at_ns"], 100)
            self.assertEqual(snapshot[role]["deactivated_at_ns"], 200)
        self.assertEqual(snapshot["trial_sent"]["packets_sent"], 0)
        self.assertEqual(snapshot["trial_received"]["unique_packets"], 0)

    def test_threaded_and_shared_keep_cumulative_and_latest_trial_loss(self):
        threaded = metrics.StreamCounters(0)
        shared = shared_counters.SharedCounters(1).view(0)

        for counters in (threaded, shared):
            counters.add_sent_batch(
                1000, 10, trial_id=1, first_seq=0, last_seq=9)
            counters.merge_peer_snapshot({
                "bytes_received": 800,
                "packets_received": 8,
                "trial_received": {
                    "trial_id": 1, "bytes_received": 800,
                    "packets_received": 8, "unique_packets": 8,
                    "packets_lost": 0,
                },
            })
            counters.add_sent_batch(
                1000, 10, trial_id=2, first_seq=10, last_seq=19)
            counters.merge_peer_snapshot({
                "bytes_received": 1700,
                "packets_received": 17,
                "trial_received": {
                    "trial_id": 2, "bytes_received": 900,
                    "packets_received": 9, "unique_packets": 9,
                    "packets_lost": 0,
                },
            })

        for snapshot in (threaded.snapshot(), shared.snapshot()):
            self.assertEqual(snapshot["packets_sent"], 20)
            self.assertEqual(snapshot["packets_received"], 17)
            self.assertEqual(snapshot["packets_lost"], 3)
            self.assertEqual(snapshot["trial_received"]["packets_lost"], 1)

    def test_zero_packet_latest_trial_still_reconciles_cumulative_loss(self):
        threaded = metrics.StreamCounters(0)
        shared = shared_counters.SharedCounters(1).view(0)

        for counters in (threaded, shared):
            counters.add_sent_batch(
                1000, 10, trial_id=1, first_seq=0, last_seq=9)
            counters.merge_peer_snapshot({
                "bytes_received": 800,
                "packets_received": 8,
                "trial_received": {
                    "trial_id": 1, "bytes_received": 800,
                    "packets_received": 8, "unique_packets": 8,
                },
            })
            counters.activate_trial(2, "sent", activated_at_ns=100)
            counters.merge_peer_snapshot({
                "bytes_received": 900,
                "packets_received": 9,
                "trial_received": {
                    "trial_id": 2, "bytes_received": 0,
                    "packets_received": 0, "unique_packets": 0,
                },
            })

        for snapshot in (threaded.snapshot(), shared.snapshot()):
            self.assertEqual(snapshot["trial_sent"]["packets_sent"], 0)
            self.assertEqual(snapshot["trial_received"]["packets_lost"], 0)
            self.assertEqual(snapshot["packets_lost"], 1)

    @unittest.skipUnless(
        "fork" in multiprocessing.get_all_start_methods(),
        "requires fork multiprocessing",
    )
    def test_shared_counter_updates_are_coherent_across_processes(self):
        ctx = multiprocessing.get_context("fork")
        shared = shared_counters.SharedCounters(1, ctx=ctx)
        counters = shared.view(0)
        count = 20_000
        process = ctx.Process(
            target=_increment_shared_counter, args=(shared, count))
        process.start()
        while process.is_alive():
            counters.merge_peer_snapshot({
                "bytes_received": 1,
                "packets_received": 1,
            })
            counters.snapshot()
        process.join(timeout=5)

        self.assertEqual(process.exitcode, 0)
        snapshot = counters.snapshot()
        self.assertEqual(snapshot["bytes_sent"], count)
        self.assertEqual(snapshot["packets_sent"], count)

    def test_partial_batch_send_does_not_skip_sequences(self):
        sock = _FakeSendSocket()
        counters = _RecordingCounters(5)
        stop = threading.Event()
        calls = []
        data_call_count = 0

        def sendmmsg(_sock, buffers, address=None, flags=0):
            nonlocal data_call_count
            decoded = [
                stream_worker.UDP_HEADER_STRUCT.unpack_from(buffer)
                for buffer in buffers
            ]
            calls.append((decoded, address, flags))
            if decoded[0][4] == stream_worker.UDP_FLAG_STOP:
                return 1
            data_call_count += 1
            if data_call_count == 1:
                return 2
            stop.set()
            return 3

        with patch.object(stream_worker.udp_batch, "sendmmsg", side_effect=sendmmsg):
            stream_worker.udp_sender_loop(
                sock, ("127.0.0.1", 9000), 5, counters, stop,
                duration_sec=5.0, start_time=time.time(),
                trial_value=stream_worker.TrialHolder(77),
            )

        sent_sequences = (
            [item[1] for item in calls[0][0][:2]]
            + [item[1] for item in calls[1][0][:3]])
        self.assertEqual(sent_sequences, [0, 1, 2, 3, 4])
        self.assertTrue(all(call[1] is None for call in calls))
        self.assertEqual(calls[-1][0][0][1], 5)
        self.assertEqual(counters.sent_publications, 2)
        snapshot = counters.snapshot()
        self.assertEqual(snapshot["observed_trial_id"], 77)
        self.assertEqual(snapshot["packets_sent"], 5)
        self.assertEqual(snapshot["trial_sent"]["first_seq"], 0)
        self.assertEqual(snapshot["trial_sent"]["last_seq"], 4)

    def test_python_backend_loopback_batches_are_decoded(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        receiver.bind(("127.0.0.1", 0))
        sender.bind(("127.0.0.1", 0))
        counters = metrics.StreamCounters(2)
        stop = threading.Event()
        thread = threading.Thread(
            target=stream_worker.udp_receiver_loop,
            args=(receiver, counters, stop),
            kwargs={"known_peer_addr": sender.getsockname(), "stream_index": 2},
        )

        with patch.object(stream_worker.udp_batch, "BACKEND", "python"):
            thread.start()
            sender.connect(receiver.getsockname())
            sender.setblocking(False)
            first_sequence = (1 << 32) + 20
            packets = [
                _frame(2, first_sequence + offset, 6)
                for offset in range(8)
            ]
            packets.append(_frame(
                2, first_sequence + 8, 6, stream_worker.UDP_FLAG_STOP))
            stream_worker.udp_batch.sendmmsg(sender, packets)
            thread.join(2.0)

        if thread.is_alive():
            stop.set()
            thread.join(2.0)
        self.assertFalse(thread.is_alive())
        snapshot = counters.snapshot()
        self.assertEqual(snapshot["packets_received"], 8)
        self.assertEqual(snapshot["packets_lost"], 0)
        self.assertEqual(snapshot["trial_received"]["trial_id"], 6)

    def test_stop_in_middle_of_batch_does_not_discard_tail_data(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.addCleanup(receiver.close)
        self.addCleanup(sender.close)
        receiver.bind(("127.0.0.1", 0))
        sender.bind(("127.0.0.1", 0))
        counters = metrics.StreamCounters(0)
        stop = threading.Event()
        thread = threading.Thread(
            target=stream_worker.udp_receiver_loop,
            args=(receiver, counters, stop),
            kwargs={"known_peer_addr": sender.getsockname(), "stream_index": 0},
        )
        packets = [
            _frame(0, 0, 4),
            _frame(0, 3, 4, stream_worker.UDP_FLAG_STOP),
            _frame(0, 1, 4),
            _frame(0, 2, 4),
        ]

        with patch.object(stream_worker.udp_batch, "BACKEND", "python"):
            thread.start()
            sender.connect(receiver.getsockname())
            sender.setblocking(False)
            stream_worker.udp_batch.sendmmsg(sender, packets)
            thread.join(2.0)

        if thread.is_alive():
            stop.set()
            thread.join(2.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(counters.snapshot()["packets_received"], 3)

    def test_foreign_truncated_udp_datagrams_do_not_poison_stream(self):
        current_cookie = stream_worker.udp_session_cookie("current-run")
        packets = [
            _frame(0, 1, 4, session_cookie=current_cookie + 1),
            _frame(1, 2, 4, session_cookie=current_cookie),
        ]
        counters = metrics.StreamCounters(0)
        stop = threading.Event()

        def receive_batch(_sock, buffers):
            for buffer, packet in zip(buffers, packets):
                header = packet[:stream_worker.UDP_HEADER_SIZE]
                buffer[:len(header)] = header
            stop.set()
            return [
                (len(packet), None, socket.MSG_TRUNC, None)
                for packet in packets
            ]

        with patch.object(stream_worker, "_prepare_udp_socket",
                          return_value=(None, True)), \
             patch.object(stream_worker.udp_batch, "recvmmsg_into",
                          side_effect=receive_batch), \
             patch.object(stream_worker, "_publish_udp_backend"):
            stream_worker.udp_receiver_loop(
                object(), counters, stop, stream_index=0, path_mtu=68,
                session_cookie=current_cookie)

        self.assertIsNone(counters.snapshot()["error"])
        self.assertEqual(counters.snapshot()["packets_received"], 0)

    def test_current_run_truncated_udp_datagram_sets_stream_error(self):
        current_cookie = stream_worker.udp_session_cookie("current-run")
        packet = _frame(0, 1, 4, session_cookie=current_cookie)
        counters = metrics.StreamCounters(0)
        stop = threading.Event()

        def receive_batch(_sock, buffers):
            header = packet[:stream_worker.UDP_HEADER_SIZE]
            buffers[0][:len(header)] = header
            stop.set()
            return [(len(packet), None, socket.MSG_TRUNC, None)]

        with patch.object(stream_worker, "_prepare_udp_socket",
                          return_value=(None, True)), \
             patch.object(stream_worker.udp_batch, "recvmmsg_into",
                          side_effect=receive_batch), \
             patch.object(stream_worker, "_publish_udp_backend"):
            stream_worker.udp_receiver_loop(
                object(), counters, stop, stream_index=0, path_mtu=68,
                session_cookie=current_cookie)

        self.assertEqual(counters.snapshot()["error"],
                         "truncated UDP datagram")
        self.assertEqual(counters.snapshot()["packets_received"], 0)

    def test_socket_metadata_records_actual_buffer_values(self):
        sock = _FakeSendSocket()
        counters = metrics.StreamCounters(0)

        metadata = stream_worker.configure_udp_socket(sock, counters)

        self.assertEqual(metadata["socket_sndbuf_bytes"], 111)
        self.assertEqual(metadata["socket_rcvbuf_bytes"], 222)
        snapshot = counters.snapshot()
        self.assertEqual(snapshot["socket_sndbuf_bytes"], 111)
        self.assertEqual(snapshot["socket_rcvbuf_bytes"], 222)


if __name__ == "__main__":
    unittest.main()
