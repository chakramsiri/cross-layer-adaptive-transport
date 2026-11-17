"""Router feedback over real loopback sockets: CE on marked DATA packets and
queue delay written into every forwarded DATA header."""

import unittest

from router.queues.codel import CoDel
from router.queues.droptail import DropTail
from router.queues.red import RED
from transport.packet import CE, DATA, SYN, Packet

from tests.udp_router.test_forwarder import (
    ForwarderFixture,
    read_events,
    recv_all,
    wait_until,
)


def data_datagram(seq, payload_size=1000):
    return Packet(
        packet_type=DATA,
        seq=seq,
        payload=bytes([seq % 256]) * payload_size,
    ).pack()


class FeedbackForwardingTests(unittest.TestCase):
    def _send_data(self, fixture, count):
        for seq in range(count):
            fixture.sender.sendto(data_datagram(seq), fixture.sender_face)

    def _stop(self, fixture):
        fixture.forwarder.stop()
        fixture.thread.join(timeout=5.0)

    def test_red_mark_mode_sets_ce_on_each_marked_data_packet(self):
        queue = RED(capacity=64, w_q=1.0, min_th=1.0, max_th=2.0,
                    max_p=1.0, mode="mark")
        fixture = ForwarderFixture(self, rate_bps=50_000, capacity=64,
                                   queue=queue)
        count = 20
        self._send_data(fixture, count)

        counters = fixture.forwarder.counters
        self.assertTrue(wait_until(lambda: counters["forwarded"] >= count))
        received = recv_all(fixture.receiver, count, timeout=10.0)
        self._stop(fixture)

        packets = [Packet.unpack(data) for data, _addr in received]
        marked = sum(1 for packet in packets if packet.flags & CE)

        self.assertGreaterEqual(counters["marks"], 5)
        self.assertEqual(counters["dropped"], 0)
        self.assertEqual(marked, counters["ce_marked"])
        self.assertEqual(counters["ce_marked"], counters["marks"])
        self.assertEqual(
            [packet.payload for packet in packets],
            [bytes([seq]) * 1000 for seq in range(count)],
        )

    def test_codel_mark_mode_sets_ce_when_packets_leave_the_queue(self):
        queue = CoDel(capacity=64, target_seconds=0.005,
                      interval_seconds=0.02, mode="mark")
        fixture = ForwarderFixture(self, rate_bps=100_000, capacity=64,
                                   queue=queue)
        count = 30
        self._send_data(fixture, count)

        counters = fixture.forwarder.counters
        self.assertTrue(
            wait_until(lambda: counters["forwarded"] >= count, timeout=20.0)
        )
        received = recv_all(fixture.receiver, count, timeout=10.0)
        self._stop(fixture)

        marked = sum(
            1 for data, _addr in received if Packet.unpack(data).flags & CE
        )
        self.assertGreaterEqual(counters["ce_marked"], 1)
        self.assertEqual(marked, counters["ce_marked"])
        self.assertEqual(counters["ce_marked"], counters["marks"])
        self.assertEqual(counters["dropped"], 0)

    def test_queue_delay_stamps_match_forward_telemetry_sojourns(self):
        fixture = ForwarderFixture(self, rate_bps=50_000, capacity=64,
                                   queue=DropTail(64))
        count = 12
        self._send_data(fixture, count)

        counters = fixture.forwarder.counters
        self.assertTrue(wait_until(lambda: counters["forwarded"] >= count))
        received = recv_all(fixture.receiver, count, timeout=10.0)
        self._stop(fixture)

        forwards = [
            row
            for row in read_events(fixture.tmp / "router_telemetry.csv")
            if row["event"] == "forward"
        ]
        self.assertEqual(len(forwards), count)

        for (data, _addr), row in zip(received, forwards):
            stamped = Packet.unpack(data)
            expected_us = round(float(row["sojourn_ms"]) * 1000)
            self.assertLessEqual(abs(stamped.q_delay_us - expected_us), 1)

        self.assertGreater(Packet.unpack(received[-1][0]).q_delay_us, 0)
        self.assertEqual(counters["ce_marked"], 0)
        self.assertEqual(counters["marks"], 0)

    def test_control_packets_pass_through_even_when_the_queue_marks_them(self):
        queue = RED(capacity=64, w_q=1.0, min_th=1.0, max_th=2.0,
                    max_p=1.0, mode="mark")
        fixture = ForwarderFixture(self, rate_bps=50_000, capacity=64,
                                   queue=queue)
        self._send_data(fixture, 10)
        syn = Packet(packet_type=SYN, seq=0).pack()
        fixture.sender.sendto(syn, fixture.sender_face)

        counters = fixture.forwarder.counters
        self.assertTrue(wait_until(lambda: counters["forwarded"] >= 11))
        received = recv_all(fixture.receiver, 11, timeout=10.0)
        self._stop(fixture)

        self.assertEqual(received[-1][0], syn)
        marked_data = sum(
            1 for data, _addr in received[:-1]
            if Packet.unpack(data).flags & CE
        )
        self.assertEqual(counters["ce_marked"], marked_data)
        self.assertEqual(counters["marks"], counters["ce_marked"] + 1)


if __name__ == "__main__":
    unittest.main()
