import unittest

from transport.packet import (
    Packet, DATA, ACK, SYN, FIN, CE, ECE,
    HEADER_SIZE
)


class TestPacket(unittest.TestCase):

    def test_pack_unpack_data(self):
        original = Packet(
            packet_type=DATA,
            seq=100,
            ack=50,
            rwnd=4096,
            ts_send=123456,
            ts_echo=654321,
            q_delay_us=5000,
            ce_count=2,
            payload=b"Hello World",
            flags=CE
        )

        packed = original.pack()
        received = Packet.unpack(packed)

        self.assertEqual(received.packet_type, original.packet_type)
        self.assertEqual(received.seq, original.seq)
        self.assertEqual(received.ack, original.ack)
        self.assertEqual(received.rwnd, original.rwnd)
        self.assertEqual(received.ts_send, original.ts_send)
        self.assertEqual(received.ts_echo, original.ts_echo)
        self.assertEqual(received.q_delay_us, original.q_delay_us)
        self.assertEqual(received.ce_count, original.ce_count)
        self.assertEqual(received.payload, original.payload)
        self.assertEqual(received.flags, original.flags)

    def test_control_packets(self):
        for packet_type in (ACK, SYN, FIN):
            original = Packet(packet_type=packet_type)

            received = Packet.unpack(original.pack())

            self.assertEqual(
                received.packet_type,
                packet_type
            )

    def test_header_size(self):
        self.assertEqual(HEADER_SIZE, 29)

    def test_invalid_packet(self):
        with self.assertRaises(ValueError):
            Packet.unpack(b"abc")

    def test_payload_length_mismatch(self):
        packet = Packet(DATA, payload=b"hello")
        packed = packet.pack()

        corrupted = packed[:-1]

        with self.assertRaises(ValueError):
            Packet.unpack(corrupted)


if __name__ == "__main__":
    unittest.main()