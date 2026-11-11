"""Deterministic sender-side RTT sampling and timer tests (RFC 6298).

Covers sample selection (original, Karn, delayed, duplicate, cumulative,
stale and colliding echoes) and the timer lifecycle: backoff and re-arm,
restart on new data, and the 3 s data-phase RTO after a retried handshake.
"""

import time
import types
import unittest

from transport import sender as sender_module
from transport.packet import SYN
from transport.rtt import encode_timestamp as original_encode_timestamp

from tests.helpers import SenderTestBase


def parse_details(row):
    """Parse key-value details from a sender log row."""
    values = {}
    for token in row["details"].split():
        key, _, value = token.partition("=")
        try:
            values[key] = float(value)
        except ValueError:
            values[key] = value
    return values


class RttIntegrationTests(SenderTestBase):
    def samples(self):
        return self.events("RTT_SAMPLE")

    def test_original_transmission_acknowledged_produces_a_sample(self):
        self.open_window()
        ts0 = self.peer.timestamps[0]
        self.peer.send_ack(1, ts_echo=ts0)
        samples = self.wait_for_event("RTT_SAMPLE", timeout=1.0)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]["seq"], "0")
        values = parse_details(samples[0])
        self.assertGreaterEqual(values["rtt"], 0.0)
        self.assertLess(values["rtt"], 0.5)
        self.assertAlmostEqual(values["srtt"], values["rtt"], places=3)
        self.assertAlmostEqual(values["rttvar"], values["rtt"] / 2, places=3)
        self.assertGreaterEqual(values["rto"], 1.0)
        self.finish_transfer()
        self.assertEqual(len(self.samples()), 1)

    def test_retransmitted_packet_is_not_sampled_karns_algorithm(self):
        self.open_window()
        ts0 = self.peer.timestamps[0]
        for _ in range(3):
            self.peer.send_ack(0)
        retransmission = self.peer.recv_data(timeout=0.6)
        self.assertIsNotNone(retransmission)
        self.assertEqual(retransmission.seq, 0)
        self.peer.send_ack(1, ts_echo=ts0)
        self.peer.send_ack(2, ts_echo=self.peer.timestamps[1])
        samples = self.wait_for_event("RTT_SAMPLE", timeout=1.0)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]["seq"], "1")
        self.finish_transfer()
        self.assertEqual([row["seq"] for row in self.samples()], ["1"])

    def test_delayed_ack_samples_the_full_delay(self):
        self.open_window()
        ts0 = self.peer.timestamps[0]
        time.sleep(0.25)
        self.peer.send_ack(1, ts_echo=ts0)
        samples = self.wait_for_event("RTT_SAMPLE", timeout=1.0)
        self.assertEqual(len(samples), 1)
        values = parse_details(samples[0])
        self.assertGreaterEqual(values["rtt"], 0.2)
        self.finish_transfer()

    def test_duplicate_ack_samples_the_identified_packet_once(self):
        self.open_window()
        ts4 = self.peer.timestamps[4]
        for _ in range(3):
            self.peer.send_ack(0, ts_echo=ts4)
        time.sleep(0.15)
        samples = self.samples()
        self.assertEqual(len(samples), 1, samples)
        self.assertEqual(samples[0]["seq"], "4")
        self.finish_transfer()
        self.assertEqual(len(self.samples()), 1)

    def test_out_of_order_echo_produces_a_sample_for_the_identified_packet(self):
        self.open_window()
        ts4 = self.peer.timestamps[4]
        self.peer.send_ack(2, ts_echo=ts4)
        time.sleep(0.15)
        samples = self.samples()
        self.assertEqual(len(samples), 1, samples)
        self.assertEqual(samples[0]["seq"], "4")
        self.finish_transfer()

    def test_retransmitted_ooo_packet_echo_is_not_sampled(self):
        self.open_window()
        ts0 = self.peer.timestamps[0]
        for _ in range(3):
            self.peer.send_ack(0)
        retransmission = self.peer.recv_data(timeout=0.6)
        self.assertIsNotNone(retransmission)
        self.assertEqual(retransmission.seq, 0)
        self.peer.send_ack(0, ts_echo=ts0)
        time.sleep(0.15)
        self.assertEqual(self.samples(), [])
        self.finish_transfer()

    def test_stale_echo_of_an_acknowledged_packet_is_not_sampled(self):
        self.open_window()
        self.peer.send_ack(2, ts_echo=self.peer.timestamps[1])
        time.sleep(0.1)
        self.assertEqual(len(self.samples()), 1)
        self.peer.send_ack(3, ts_echo=self.peer.timestamps[0])
        time.sleep(0.15)
        samples = self.samples()
        self.assertEqual(len(samples), 1, samples)
        self.assertEqual(samples[0]["seq"], "1")
        self.finish_transfer()

    def test_zero_stamp_echo_is_conservatively_skipped(self):
        original = sender_module.encode_timestamp
        sender_module.encode_timestamp = lambda now: 0
        self.addCleanup(setattr, sender_module, "encode_timestamp", original)
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(len(self.CHUNKS)), list(range(len(self.CHUNKS))))
        self.peer.send_ack(1, ts_echo=0)
        time.sleep(0.15)
        self.assertEqual(self.samples(), [])
        self.finish_transfer()

    def test_cumulative_ack_takes_exactly_one_sample(self):
        self.open_window()
        ts3 = self.peer.timestamps[3]
        self.peer.send_ack(5, ts_echo=ts3)
        samples = self.wait_for_event("RTT_SAMPLE", timeout=1.0)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]["seq"], "3")
        self.finish_transfer()
        self.assertEqual(len(self.samples()), 1)

    def test_timeout_backs_off_rto_and_rearms_the_timer(self):
        self.open_window()
        first = self.peer.recv_data(timeout=1.6)
        self.assertIsNotNone(first)
        self.assertEqual(first.seq, 0)
        backoffs = self.wait_for_event("RTO_BACKOFF", timeout=1.0)
        self.assertEqual(len(backoffs), 1)
        self.assertAlmostEqual(parse_details(backoffs[0])["rto"], 2.0, places=3)
        self.assertIsNone(
            self.peer.expect_no_data(1.2),
            "the backed-off timer fired too early",
        )
        second = self.peer.recv_data(timeout=1.2)
        self.assertIsNotNone(second, "the backed-off timer never fired")
        self.assertEqual(second.seq, 0)
        self.finish_transfer()

    def test_timer_restarts_when_new_data_is_acknowledged(self):
        self.open_window()
        time.sleep(0.6)
        self.peer.send_ack(1)
        time.sleep(0.1)
        self.assertIsNone(
            self.peer.expect_no_data(0.5),
            "timer was not restarted by the new ACK",
        )
        retransmission = self.peer.recv_data(timeout=0.9)
        self.assertIsNotNone(retransmission)
        self.assertEqual(retransmission.seq, 1)
        self.finish_transfer()

    def test_fast_retransmit_does_not_restart_running_timer(self):
        self.open_window()
        time.sleep(0.5)
        for _ in range(3):
            self.peer.send_ack(0)
        fast_retx = self.peer.recv_data(timeout=0.6)
        self.assertIsNotNone(fast_retx)
        self.assertEqual(fast_retx.seq, 0)
        second_retx = self.peer.recv_data(timeout=0.7)
        self.assertIsNotNone(second_retx)
        self.assertEqual(second_retx.seq, 0)
        self.finish_transfer()

    def test_retried_handshake_starts_data_phase_with_three_second_rto(self):
        packet, addr = self.peer.recv_any(timeout=2.0)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, SYN)
        self.peer.sender_addr = addr
        packet, _ = self.peer.recv_any(timeout=3.5)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, SYN)
        self.peer.send_ack(0)
        initial = self.wait_for_event("RTO_INITIAL", timeout=2.0)
        self.assertEqual(len(initial), 1)
        self.assertAlmostEqual(
            parse_details(initial[0])["rto"], 3.0, places=3
        )
        self.assertEqual(
            self.peer.collect_data(len(self.CHUNKS)),
            list(range(len(self.CHUNKS))),
        )
        self.finish_transfer()


class TimestampCollisionTests(SenderTestBase):
    def samples(self):
        return self.events("RTT_SAMPLE")

    @staticmethod
    def _quantized_stamp(now):
        return (int(now * 1_000_000) // 10_000) * 10_000 & 0xFFFFFFFF

    def _patch_stamp_encoder(self):
        sender_module.encode_timestamp = self._quantized_stamp
        self.addCleanup(
            setattr,
            sender_module,
            "encode_timestamp",
            original_encode_timestamp,
        )

    def _patch_first_two_stamps_shared(self, shared):
        calls = {"count": 0}

        def encode(now):
            calls["count"] += 1
            if calls["count"] <= 2:
                return shared
            return int(now * 1_000_000) & 0xFFFFFFFF

        sender_module.encode_timestamp = encode
        self.addCleanup(
            setattr,
            sender_module,
            "encode_timestamp",
            original_encode_timestamp,
        )

    def test_colliding_fresh_stamps_are_ambiguous_and_skipped(self):
        self._patch_stamp_encoder()
        self.open_window()
        shared = self.peer.timestamps[0]
        self.assertEqual(self.peer.timestamps[4], shared)
        self.peer.send_ack(3, ts_echo=shared)
        time.sleep(0.15)
        self.assertEqual(self.samples(), [])
        self.finish_transfer()
        self.assertEqual(self.samples(), [])

    def test_collision_with_retransmitted_packet_is_skipped(self):
        self._patch_stamp_encoder()
        self.open_window()
        shared = self.peer.timestamps[0]
        for _ in range(3):
            self.peer.send_ack(0)
        retransmission = self.peer.recv_data(timeout=0.6)
        self.assertIsNotNone(retransmission)
        self.assertEqual(retransmission.seq, 0)
        self.peer.send_ack(2, ts_echo=shared)
        time.sleep(0.15)
        self.assertEqual(self.samples(), [])
        self.finish_transfer()
        self.assertEqual(self.samples(), [])

    def test_delayed_ack_of_an_ambiguous_stamp_is_not_sampled(self):
        # Packets 0 and 1 share a stamp.  The first ACK proves the stamp
        # ambiguous (it matches both); a later duplicate ACK echoing the
        # same stamp matches only packet 1 but must still be skipped,
        # since the echo cannot be associated with one packet.
        self._patch_first_two_stamps_shared(424242)
        self.open_window()
        self.peer.send_ack(1, ts_echo=424242)
        time.sleep(0.15)
        self.assertEqual(self.samples(), [])

        self.peer.send_ack(1, ts_echo=424242)
        time.sleep(0.15)
        self.assertEqual(self.samples(), [])
        self.finish_transfer()
        self.assertEqual(self.samples(), [])


class SynHandshakeTests(SenderTestBase):
    def test_single_syn_loss_starts_data_phase_at_three_seconds(self):
        packet, addr = self.peer.recv_any(timeout=2.0)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, SYN)
        self.peer.sender_addr = addr
        packet, _ = self.peer.recv_any(timeout=3.5)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, SYN)
        self.peer.send_ack(0)
        initial = self.wait_for_event("RTO_INITIAL", timeout=2.0)
        self.assertEqual(len(initial), 1)
        self.assertAlmostEqual(
            parse_details(initial[0])["rto"], 3.0, places=3
        )
        self.assertEqual(
            self.peer.collect_data(len(self.CHUNKS)),
            list(range(len(self.CHUNKS))),
        )
        self.finish_transfer()

    def test_double_syn_loss_keeps_the_backed_off_four_second_rto(self):
        packet, addr = self.peer.recv_any(timeout=2.0)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, SYN)
        self.peer.sender_addr = addr
        packet, _ = self.peer.recv_any(timeout=3.5)
        self.assertIsNotNone(packet)
        packet, _ = self.peer.recv_any(timeout=4.0)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, SYN)
        self.peer.send_ack(0)
        initial = self.wait_for_event("RTO_INITIAL", timeout=2.0)
        self.assertAlmostEqual(
            parse_details(initial[0])["rto"], 4.0, places=3
        )
        self.assertEqual(
            self.peer.collect_data(len(self.CHUNKS)),
            list(range(len(self.CHUNKS))),
        )
        self.finish_transfer()

    def test_triple_syn_loss_backs_off_to_four_second_wait_and_eight_second_rto(self):
        packet, addr = self.peer.recv_any(timeout=2.0)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, SYN)
        self.peer.sender_addr = addr
        packet, _ = self.peer.recv_any(timeout=3.5)
        self.assertIsNotNone(packet)
        packet, _ = self.peer.recv_any(timeout=4.0)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, SYN)
        third = time.monotonic()
        packet, _ = self.peer.recv_any(timeout=6.0)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, SYN)
        gap = time.monotonic() - third
        self.assertGreaterEqual(gap, 3.9)
        self.assertLess(gap, 5.5)
        self.peer.send_ack(0)
        initial = self.wait_for_event("RTO_INITIAL", timeout=2.0)
        self.assertAlmostEqual(
            parse_details(initial[0])["rto"], 8.0, places=3
        )
        self.assertEqual(
            self.peer.collect_data(len(self.CHUNKS)),
            list(range(len(self.CHUNKS))),
        )
        self.finish_transfer()


class BackoffCollapseTests(SenderTestBase):
    CHUNKS = [bytes([65 + index % 26]) * 1000 for index in range(11)]
    PEER_RWND = 6000

    def test_recovery_ack_without_a_sample_keeps_backoff_until_a_sample(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(6), [0, 1, 2, 3, 4, 5])
        self.peer.send_ack(1, ts_echo=self.peer.timestamps[0])
        self.wait_for_event("RTT_SAMPLE", timeout=1.0)
        self.assertEqual(self.peer.collect_data(1, timeout=1.5), [6])
        retransmission = self.peer.recv_data(timeout=1.6)
        self.assertIsNotNone(retransmission)
        self.assertEqual(retransmission.seq, 1)
        backoffs = self.wait_for_event("RTO_BACKOFF", timeout=1.0)
        self.assertAlmostEqual(
            parse_details(backoffs[-1])["rto"], 2.0, places=3
        )
        self.peer.send_ack(2)
        time.sleep(0.15)
        self.assertEqual(self.events("RTO_RECOMPUTE"), [])
        self.peer.send_ack(7, ts_echo=self.peer.timestamps[6])
        samples = self.wait_for_event("RTT_SAMPLE", timeout=1.0)
        self.assertLess(parse_details(samples[-1])["rto"], 2.0)
        for ack in range(8, len(self.CHUNKS) + 1):
            self.peer.send_ack(ack)
        self.finish_transfer()

    def test_new_sample_collapses_the_backoff(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(6), [0, 1, 2, 3, 4, 5])
        first = self.peer.recv_data(timeout=1.6)
        self.assertIsNotNone(first)
        self.assertEqual(first.seq, 0)
        backoffs = self.wait_for_event("RTO_BACKOFF", timeout=1.0)
        self.assertEqual(len(backoffs), 1)
        self.assertAlmostEqual(
            parse_details(backoffs[0])["rto"], 2.0, places=3
        )
        self.peer.send_ack(1)
        # Post-timeout cwnd is 1, so the outstanding window 1..5 must
        # drain before packet 6 can be sent.
        for ack in range(2, 7):
            self.peer.send_ack(ack)
        self.assertEqual(self.peer.collect_data(1, timeout=2.0), [6])
        self.peer.send_ack(7, ts_echo=self.peer.timestamps[6])
        samples = self.wait_for_event("RTT_SAMPLE", timeout=1.0)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]["seq"], "6")
        values = parse_details(samples[0])
        self.assertLess(values["rtt"], 0.5)
        self.assertLess(values["rto"], 2.0)
        self.finish_transfer()


class TimerLifecycleTests(SenderTestBase):
    def test_timer_covers_final_packet_after_partial_cumulative_ack(self):
        self.open_window()
        self.peer.send_ack(4)
        time.sleep(0.1)
        retransmission = self.peer.recv_data(timeout=1.6)
        self.assertIsNotNone(retransmission, "no timeout for the final packet")
        self.assertEqual(retransmission.seq, 4)
        self.finish_transfer()


class TimerRestartWithContinuationTests(SenderTestBase):
    CHUNKS = [bytes([65 + index % 26]) * 1000 for index in range(11)]
    PEER_RWND = 6000

    def test_timer_starts_for_new_data_when_nothing_is_outstanding(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(6), [0, 1, 2, 3, 4, 5])
        self.peer.send_ack(6)
        self.assertEqual(
            self.peer.collect_data(5, timeout=2.0), [6, 7, 8, 9, 10]
        )
        retransmission = self.peer.recv_data(timeout=1.6)
        self.assertIsNotNone(retransmission, "no timeout after the second window")
        self.assertEqual(retransmission.seq, 6)
        self.finish_transfer()


class ReceiveAckArrivalTests(unittest.TestCase):
    def test_arrival_timestamp_is_captured_before_unpacking(self):
        from transport.packet import Packet, ACK

        datagram = Packet(packet_type=ACK, ack=7).pack()
        unpacked_at = {}

        class SlowStubPacket:
            @staticmethod
            def unpack(data):
                time.sleep(0.15)
                unpacked_at["time"] = time.monotonic()
                return types.SimpleNamespace(
                    packet_type=ACK, ack=7, rwnd=64000, ts_echo=0
                )

        class FakeSock:
            def recvfrom(self, size):
                return datagram, ("127.0.0.1", 9999)

        real_packet_cls = sender_module.Packet
        real_logger = sender_module.logger
        sender_module.Packet = SlowStubPacket
        sender_module.logger = types.SimpleNamespace(
            log=lambda *args, **kwargs: None
        )
        try:
            result = sender_module.receive_ack(
                FakeSock(), ("127.0.0.1", 9999)
            )
        finally:
            sender_module.Packet = real_packet_cls
            sender_module.logger = real_logger
        self.assertIsNotNone(result)
        packet, arrival = result
        self.assertEqual(packet.ack, 7)
        self.assertLess(arrival, unpacked_at["time"])
        self.assertGreaterEqual(unpacked_at["time"] - arrival, 0.10)


if __name__ == "__main__":
    unittest.main()