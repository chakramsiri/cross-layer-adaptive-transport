"""Deterministic sender-side flow-control tests (Phase 2 receiver window).

The sender enforces `in_flight_bytes + next_chunk <= advertised rwnd` for
NEW data only; retransmissions of the earliest unacked packet are exempt,
which is what lets a zero-window receiver recover.
"""

from tests.helpers import DEFAULT_RWND, SenderTestBase


class FlowControlTests(SenderTestBase):
    def test_syn_ack_window_is_enforced_byte_exactly(self):
        # The peer advertises room for exactly three packets.
        self.peer.rwnd = 3000
        self.peer.accept_syn()

        seqs = self.peer.collect_data(3)
        self.assertEqual(seqs, [0, 1, 2])
        self.assertIsNone(
            self.peer.expect_no_data(0.3),
            "sender exceeded the advertised window",
        )

        # A window update of 2000 bytes allows exactly two more packets.
        self.peer.send_ack(3, rwnd=2000)
        seqs = self.peer.collect_data(2)
        self.assertEqual(seqs, [3, 4])
        self.assertIsNone(
            self.peer.expect_no_data(0.3),
            "sender sent beyond the updated window",
        )

        self.finish_transfer()

    def test_zero_window_blocks_new_data_until_an_update(self):
        self.peer.rwnd = 3000
        self.peer.accept_syn()
        # Sender fills the 3000-byte window: packets 0, 1, 2.
        self.assertEqual(self.peer.collect_data(3), [0, 1, 2])

        # Advance base and close the window: ACK(1, rwnd=0).
        self.peer.send_ack(1, rwnd=0)
        self.assertIsNone(
            self.peer.expect_no_data(0.5),
            "sender transmitted despite a zero window",
        )

        # An advancing ACK with a larger window reopens it.
        self.peer.send_ack(3, rwnd=2000)
        seqs = self.peer.collect_data(2, timeout=2.0)
        self.assertEqual(seqs, [3, 4])

        self.finish_transfer()

    def test_zero_window_with_outstanding_data_recovers_via_retransmission(self):
        self.peer.rwnd = 3000
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(3), [0, 1, 2])

        # Acknowledge one packet, then slam the window shut.
        self.peer.send_ack(1, rwnd=0)

        # No NEW data may be sent while rwnd is zero...
        self.assertIsNone(
            self.peer.expect_no_data(0.6),
            "sender sent new data into a zero window",
        )

        # ...but the retransmission timer must still fire for the earliest
        # unacknowledged packet; retransmissions are exempt from rwnd.
        retransmission = self.peer.recv_data(timeout=1.2)
        self.assertIsNotNone(
            retransmission, "no timeout retransmission under a zero window"
        )
        self.assertEqual(retransmission.seq, 1)

        # Receiving it frees buffer space; the peer reopens the window.
        self.peer.send_ack(3, rwnd=3000)
        self.assertEqual(self.peer.collect_data(2), [3, 4])

        self.finish_transfer()

    def test_partial_ack_frees_exactly_the_acknowledged_bytes(self):
        self.peer.rwnd = 3000
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(3), [0, 1, 2])

        # Acking two packets frees 2000 bytes; packet 2 is still in flight.
        self.peer.send_ack(2, rwnd=3000)
        self.assertEqual(self.peer.collect_data(2), [3, 4])

        self.finish_transfer()


class PacketCapTests(SenderTestBase):
    """The static Phase 2 packet cap (Phase 3 will supply cwnd)."""

    CHUNKS = [bytes([65 + index % 26]) * 1000 for index in range(40)]
    PEER_RWND = 64000

    def test_packet_window_cap_is_enforced_with_large_rwnd(self):
        self.peer.accept_syn()

        # With a generous rwnd the 32-packet cap is the binding limit.
        self.assertEqual(self.peer.collect_data(32), list(range(32)))
        self.assertIsNone(
            self.peer.expect_no_data(0.3),
            "sender exceeded the 32-packet window cap",
        )

        # Advancing the base frees window slots for the rest of the file.
        self.peer.send_ack(32)
        self.assertEqual(
            self.peer.collect_data(8, timeout=2.0), list(range(32, 40))
        )

        self.finish_transfer()


class ZeroInitialWindowTests(SenderTestBase):
    """A zero initial rwnd can never make progress: fail cleanly."""

    PEER_RWND = 0

    def test_zero_initial_rwnd_fails_cleanly(self):
        self.peer.accept_syn()

        # No DATA may be sent into a zero window.
        self.assertIsNone(self.peer.expect_no_data(0.3))

        self.thread.join(timeout=5.0)
        self.assertFalse(self.thread.is_alive(), "sender hung on rwnd=0")
        self.assertIsNone(self.sender_error)
        data_sent = [
            row for row in self.events("PACKET_SENT")
            if row["packet_type"] == "DATA"
        ]
        self.assertEqual(len(data_sent), 0)
        self.assertEqual(len(self.events("TRANSFER_FAILED")), 1)
        self.assertEqual(len(self.events("TRANSFER_COMPLETED")), 0)


class SmallInitialWindowTests(SenderTestBase):
    """An initial window below one payload is accommodated by re-chunking."""

    PEER_RWND = 600

    def test_initial_window_smaller_than_chunk_size_rechunks(self):
        payload = b"".join(self.CHUNKS)
        total = -(-len(payload) // self.PEER_RWND)  # ceil division

        self.peer.accept_syn()

        received = {}
        for seq in range(total):
            packet = self.peer.recv_data(timeout=2.0)
            self.assertIsNotNone(packet, f"packet {seq} never arrived")
            self.assertEqual(packet.seq, seq)
            self.assertLessEqual(len(packet.payload), self.PEER_RWND)
            received[seq] = packet.payload
            self.peer.send_ack(seq + 1, rwnd=self.PEER_RWND)

        self.peer.send_ack(total)
        self.assertTrue(self.peer.wait_for_fin(), "FIN handshake failed")
        self.thread.join(timeout=5.0)
        self.assertFalse(self.thread.is_alive())
        self.assertIsNone(self.sender_error)

        # Re-chunking must preserve the file byte for byte.
        self.assertEqual(
            b"".join(received[seq] for seq in range(total)), payload
        )


class ValidSmallWindowTests(SenderTestBase):
    """A small but usable window limits packets in flight, then completes."""

    PEER_RWND = 2000

    def test_valid_small_window_limits_then_completes(self):
        self.peer.accept_syn()

        self.assertEqual(self.peer.collect_data(2), [0, 1])
        self.assertIsNone(
            self.peer.expect_no_data(0.3), "sender exceeded rwnd=2000"
        )

        self.peer.send_ack(2, rwnd=self.PEER_RWND)
        self.assertEqual(self.peer.collect_data(2), [2, 3])

        self.peer.send_ack(4, rwnd=self.PEER_RWND)
        self.assertEqual(self.peer.collect_data(1), [4])

        self.finish_transfer()


if __name__ == "__main__":
    import unittest

    unittest.main()
