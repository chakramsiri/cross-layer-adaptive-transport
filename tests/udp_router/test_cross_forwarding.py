"""Cross-traffic datagrams share the bottleneck but must never become the
destination of reverse-path ACKs for the transport flow."""

import socket
import time
import unittest

from transport.packet import CROSS, DATA, Packet

from tests.udp_router.test_forwarder import (
    ForwarderFixture,
    read_events,
    recv_all,
    wait_until,
)


def cross_datagram(seq=0, payload_size=1000):
    return Packet(packet_type=CROSS, seq=seq,
                  payload=b"\x00" * payload_size).pack()


def data_datagram(seq=0, payload_size=1000):
    return Packet(packet_type=DATA, seq=seq,
                  payload=b"\x01" * payload_size).pack()


class CrossTelemetryKindTests(unittest.TestCase):
    def test_telemetry_labels_each_forwarded_datagram_by_kind(self):
        fixture = ForwarderFixture(self)
        cross = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cross.bind(("127.0.0.1", 0))
        self.addCleanup(cross.close)

        fixture.sender.sendto(data_datagram(0), fixture.sender_face)
        cross.sendto(cross_datagram(0), fixture.sender_face)
        counters = fixture.forwarder.counters
        self.assertTrue(wait_until(lambda: counters["forwarded"] >= 2))

        rows = read_events(fixture.tmp / "router_telemetry.csv")
        forwards = [row for row in rows if row["event"] == "forward"]
        self.assertEqual([row["kind"] for row in forwards], ["data", "cross"])


class CrossTrafficForwardingTests(unittest.TestCase):
    def _cross_source(self, test):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        test.addCleanup(sock.close)
        return sock

    def test_ack_still_reaches_the_transport_sender_after_cross_traffic(self):
        fixture = ForwarderFixture(self)
        cross = self._cross_source(self)

        fixture.sender.sendto(data_datagram(0), fixture.sender_face)
        recv_all(fixture.receiver, 1)

        cross.sendto(cross_datagram(0), fixture.sender_face)
        counters = fixture.forwarder.counters
        self.assertTrue(wait_until(lambda: counters["forwarded"] >= 2))
        forwarded_cross = recv_all(fixture.receiver, 1)[0][0]
        self.assertEqual(Packet.unpack(forwarded_cross).packet_type, CROSS)

        fixture.receiver.sendto(b"ACK-for-transport", fixture.receiver_face)
        data, _addr = recv_all(fixture.sender, 1)[0]

        self.assertEqual(data, b"ACK-for-transport")
        cross.settimeout(0.2)
        with self.assertRaises(socket.timeout):
            cross.recvfrom(2048)

    def test_cross_datagrams_alone_leave_no_reverse_destination(self):
        fixture = ForwarderFixture(self)
        cross = self._cross_source(self)

        cross.sendto(cross_datagram(0), fixture.sender_face)
        recv_all(fixture.receiver, 1)
        fixture.receiver.sendto(b"orphan-ack", fixture.receiver_face)

        time.sleep(0.2)
        self.assertIsNone(fixture.forwarder.sender_addr)
        self.assertEqual(fixture.forwarder.counters["acks_forwarded"], 0)

    def test_transport_sender_remains_the_reverse_destination_later(self):
        fixture = ForwarderFixture(self)
        cross = self._cross_source(self)

        fixture.sender.sendto(data_datagram(0), fixture.sender_face)
        recv_all(fixture.receiver, 1)
        cross.sendto(cross_datagram(1), fixture.sender_face)
        cross.sendto(cross_datagram(2), fixture.sender_face)
        fixture.sender.sendto(data_datagram(1), fixture.sender_face)
        recv_all(fixture.receiver, 3)

        self.assertEqual(
            fixture.forwarder.sender_addr, fixture.sender.getsockname()
        )


if __name__ == "__main__":
    unittest.main()
