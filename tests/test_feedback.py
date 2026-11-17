import unittest

from transport.feedback import (
    CE_MODULUS,
    MAX_Q_DELAY_US,
    FeedbackTracker,
    stamp_forwarded_datagram,
)
from transport.packet import CE, DATA, SYN, Packet


class FeedbackTrackerTests(unittest.TestCase):
    def test_first_echo_reports_every_mark_so_far(self):
        tracker = FeedbackTracker()

        self.assertEqual(tracker.observe(3, 100), 3)
        self.assertEqual(tracker.ce_total, 3)
        self.assertEqual(tracker.q_delay_us, 100)

    def test_each_echo_reports_only_its_increment(self):
        tracker = FeedbackTracker()

        tracker.observe(2, 10)
        self.assertEqual(tracker.observe(5, 20), 3)
        self.assertEqual(tracker.ce_total, 5)

    def test_lost_acks_are_recovered_by_the_next_echo(self):
        tracker = FeedbackTracker()

        tracker.observe(1, 10)
        self.assertEqual(tracker.observe(4, 40), 3)
        self.assertEqual(tracker.ce_total, 4)

    def test_duplicate_echo_adds_nothing_but_refreshes_delay(self):
        tracker = FeedbackTracker()

        tracker.observe(4, 10)
        self.assertEqual(tracker.observe(4, 50), 0)
        self.assertEqual(tracker.ce_total, 4)
        self.assertEqual(tracker.q_delay_us, 50)

    def test_reordered_older_echo_is_ignored(self):
        tracker = FeedbackTracker()

        tracker.observe(5, 10)
        self.assertIsNone(tracker.observe(3, 99))
        self.assertEqual(tracker.ce_total, 5)
        self.assertEqual(tracker.q_delay_us, 10)

    def test_counter_wrap_is_counted_modulo_the_field_size(self):
        tracker = FeedbackTracker()

        tracker.observe(30000, 0)
        tracker.observe(60000, 0)
        self.assertEqual(tracker.observe(CE_MODULUS - 2, 0), 5534)
        self.assertEqual(tracker.observe(2, 0), 4)
        self.assertEqual(tracker.ce_total, CE_MODULUS + 2)

    def test_reordered_echo_across_the_wrap_is_ignored(self):
        tracker = FeedbackTracker()

        tracker.observe(2, 0)
        self.assertIsNone(tracker.observe(CE_MODULUS - 2, 0))
        self.assertEqual(tracker.ce_total, 2)


class StampForwardedDatagramTests(unittest.TestCase):
    def _data(self, payload=b"hello", **fields):
        return Packet(
            packet_type=DATA, seq=7, ack=3, rwnd=640, ts_send=99,
            payload=payload, **fields
        ).pack()

    def test_unmarked_data_gets_queue_delay_in_microseconds(self):
        datagram, ce_applied = stamp_forwarded_datagram(
            self._data(), 0.0123456, marked=False
        )
        received = Packet.unpack(datagram)

        self.assertEqual(received.q_delay_us, 12346)
        self.assertEqual(received.flags & CE, 0)
        self.assertFalse(ce_applied)

    def test_marked_data_sets_ce_and_reports_it_applied(self):
        datagram, ce_applied = stamp_forwarded_datagram(
            self._data(), 0.002, marked=True
        )
        received = Packet.unpack(datagram)

        self.assertTrue(received.flags & CE)
        self.assertEqual(received.q_delay_us, 2000)
        self.assertTrue(ce_applied)

    def test_stamping_keeps_every_other_header_field_and_the_payload(self):
        original = self._data(payload=b"payload-bytes")
        datagram, _ = stamp_forwarded_datagram(original, 0.001, marked=True)
        before = Packet.unpack(original)
        after = Packet.unpack(datagram)

        self.assertEqual(after.payload, before.payload)
        self.assertEqual(after.seq, before.seq)
        self.assertEqual(after.ack, before.ack)
        self.assertEqual(after.rwnd, before.rwnd)
        self.assertEqual(after.ts_send, before.ts_send)
        self.assertEqual(after.packet_type, DATA)
        self.assertEqual(len(datagram), len(original))

    def test_queue_delay_is_clamped_to_the_field_size(self):
        datagram, _ = stamp_forwarded_datagram(self._data(), 1e9, marked=False)

        self.assertEqual(Packet.unpack(datagram).q_delay_us, MAX_Q_DELAY_US)

    def test_control_packets_pass_through_unchanged(self):
        syn = Packet(packet_type=SYN, seq=0).pack()

        datagram, ce_applied = stamp_forwarded_datagram(
            syn, 0.05, marked=True
        )

        self.assertEqual(datagram, syn)
        self.assertFalse(ce_applied)

    def test_opaque_bytes_pass_through_unchanged(self):
        opaque = bytes([1]) * 1000

        datagram, ce_applied = stamp_forwarded_datagram(
            opaque, 0.05, marked=True
        )

        self.assertEqual(datagram, opaque)
        self.assertFalse(ce_applied)


if __name__ == "__main__":
    unittest.main()
